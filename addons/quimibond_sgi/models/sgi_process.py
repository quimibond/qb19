# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError


class SgiProcess(models.Model):
    _name = 'sgi.process'
    _description = "Proceso SGI"
    # mail.activity.mixin es indispensable: el aviso de «eslabón atorado» se
    # agenda SOBRE el proceso destino (activity_schedule) — sin el mixin el
    # cron de medición tronaba completo en producción. mail.thread da además
    # el chatter que a la ficha le faltaba.
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _parent_name = 'parent_id'
    _parent_store = True
    _order = 'process_type, code'

    code = fields.Char(string="Clave", required=True, index=True)
    name = fields.Char(string="Nombre", required=True)
    process_type = fields.Selection([
        ('cop', "COP (Operativo cliente)"),
        ('estrategico', "Estratégico"),
        ('soporte', "Soporte"),
    ], string="Tipo", default='cop', required=True)
    parent_id = fields.Many2one('sgi.process', string="Macroproceso", ondelete='restrict', index=True)
    parent_path = fields.Char(index=True)
    child_ids = fields.One2many('sgi.process', 'parent_id', string="Subprocesos")
    owner_id = fields.Many2one('hr.employee', string="Dueño del proceso")
    department_id = fields.Many2one('hr.department', string="Departamento")
    job_ids = fields.Many2many('hr.job', string="Puestos")
    active = fields.Boolean(default=True)

    purpose = fields.Text(
        string="Objetivo del proceso",
        help="Para qué existe el proceso (de la caracterización/SIPOC).")

    in_flow_ids = fields.One2many('sgi.process.flow', 'to_process_id', string="Entradas")
    out_flow_ids = fields.One2many('sgi.process.flow', 'from_process_id', string="Salidas")

    # Ficha del proceso: todo lo ligado, navegable desde un solo lugar.
    linked_document_ids = fields.One2many(
        'documents.document', 'sgi_process_id', string="Documentos del proceso")
    procedure_ids = fields.One2many(
        'documents.document', 'sgi_process_id', string="Procedimientos e instructivos",
        domain=[('sgi_doc_type', 'in', ('procedimiento', 'instructivo')),
                ('sgi_state', '=', 'vigente')])
    indicator_ids = fields.One2many('sgi.indicator', 'process_id', string="Indicadores")
    risk_ids = fields.One2many('sgi.risk', 'process_id', string="Riesgos y oportunidades")
    odoo_model_ids = fields.Many2many(
        'ir.model', string="Módulos de Odoo conectados",
        compute='_compute_odoo_models',
        help="Modelos donde viven los registros reales de las entradas/salidas.")

    nc_count = fields.Integer(string="NC abiertas", compute='_compute_health')
    overdue_action_count = fields.Integer(string="Acciones vencidas", compute='_compute_health')
    red_kpi_count = fields.Integer(string="KPIs en rojo", compute='_compute_health')
    open_high_risk_count = fields.Integer(string="Riesgos altos abiertos",
                                          compute='_compute_health')
    health = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Salud del proceso", compute='_compute_health')
    document_count = fields.Integer(string="# Documentos", compute='_compute_counts')
    indicator_count = fields.Integer(string="# Indicadores", compute='_compute_counts')
    risk_count = fields.Integer(string="# Riesgos", compute='_compute_counts')

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de proceso debe ser única.",
    )

    @api.constrains('parent_id')
    def _check_parent_recursion(self):
        if self._has_cycle():
            raise ValidationError("No puede crear una recursión de macroprocesos.")

    def _compute_health(self):
        """Salud del proceso por agregación (semáforo), sin datos nuevos:

        - verde: nada abierto (0 NC del SGI, 0 acciones vencidas de sus
          orígenes, 0 KPI en rojo, 0 riesgo de atención máxima abierto);
        - rojo: hay un riesgo de atención máxima abierto, o coinciden NC abierta
          y KPI en rojo (síntoma sistémico: falla + evidencia de que no baja);
        - amarillo: hay algo abierto pero no alcanza el umbral rojo.

        Todo POR LOTE (una query por métrica para el recordset completo): la
        versión por registro disparaba 4+ queries por proceso y el mapa
        completo (~21 procesos) costaba ~150 queries por render.
        """
        Alert = self.env['quality.alert']
        ActionLine = self.env['sgi.action.line']
        Risk = self.env['sgi.risk']
        Measure = self.env['sgi.indicator.measure']
        processes = self.filtered('id')
        for process in (self - processes):
            process.nc_count = process.overdue_action_count = 0
            process.red_kpi_count = process.open_high_risk_count = 0
            process.health = 'verde'
        if not processes:
            return
        ids = processes.ids
        nc_counts = {p.id: count for p, count in Alert._read_group(
            [('sgi_process_id', 'in', ids),
             ('stage_id.sgi_is_closing_stage', '=', False),
             ('stage_id.sgi_is_cancel_stage', '=', False)],
            ['sgi_process_id'], ['__count'])}
        # Acciones vencidas cuyos orígenes (NC/riesgo/incidente/AMEF) apuntan
        # al proceso: una search para todos y el bucket en Python (una acción
        # tiene exactamente UN origen — constraint XOR).
        overdue_counts = {}
        overdue = ActionLine.search([
            ('state', '=', 'vencida'),
            '|', '|', '|',
            ('alert_id.sgi_process_id', 'in', ids),
            ('risk_id.process_id', 'in', ids),
            ('incident_id.process_id', 'in', ids),
            ('fmea_line_id.fmea_id.process_id', 'in', ids),
        ])
        for line in overdue:
            pid = (line.alert_id.sgi_process_id.id
                   or line.risk_id.process_id.id
                   or line.incident_id.process_id.id
                   or line.fmea_line_id.fmea_id.process_id.id)
            if pid:
                overdue_counts[pid] = overdue_counts.get(pid, 0) + 1
        # KPIs en rojo: indicadores del proceso cuya última medición VALIDADA
        # está en rojo — una sola search ordenada y se toma la primera por
        # indicador.
        red_counts = {}
        indicators = processes.indicator_ids
        if indicators:
            seen = set()
            for measure in Measure.search(
                    [('indicator_id', 'in', indicators.ids),
                     ('state', '=', 'validado')],
                    order='indicator_id, period_date desc, id desc'):
                ind = measure.indicator_id
                if ind.id in seen:
                    continue
                seen.add(ind.id)
                if measure.semaphore == 'rojo':
                    pid = ind.process_id.id
                    red_counts[pid] = red_counts.get(pid, 0) + 1
        risk_counts = {p.id: count for p, count in Risk._read_group(
            [('process_id', 'in', ids),
             ('attention_level', 'in', ('inmediata', 'alto')),
             ('state', '!=', 'cerrado')],
            ['process_id'], ['__count'])}
        for process in processes:
            process.nc_count = nc_counts.get(process.id, 0)
            process.overdue_action_count = overdue_counts.get(process.id, 0)
            process.red_kpi_count = red_counts.get(process.id, 0)
            process.open_high_risk_count = risk_counts.get(process.id, 0)
            if process.open_high_risk_count or (process.nc_count and process.red_kpi_count):
                process.health = 'rojo'
            elif process.nc_count or process.overdue_action_count or process.red_kpi_count:
                process.health = 'amarillo'
            else:
                process.health = 'verde'

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for process in self:
            process.display_name = "%s - %s" % (process.code, process.name) if process.code else process.name

    def _compute_odoo_models(self):
        for process in self:
            flows = process.in_flow_ids | process.out_flow_ids
            process.odoo_model_ids = flows.mapped('odoo_model_id')

    def _compute_counts(self):
        """Conteos por lote (una _read_group por métrica, no 3 queries por
        proceso)."""
        Doc = self.env['documents.document']
        Indicator = self.env['sgi.indicator']
        Risk = self.env['sgi.risk']
        processes = self.filtered('id')
        for process in (self - processes):
            process.document_count = process.indicator_count = process.risk_count = 0
        if not processes:
            return
        ids = processes.ids
        doc_counts = {p.id: count for p, count in Doc._read_group(
            [('sgi_process_id', 'in', ids)], ['sgi_process_id'], ['__count'])}
        ind_counts = {p.id: count for p, count in Indicator._read_group(
            [('process_id', 'in', ids)], ['process_id'], ['__count'])}
        risk_counts = {p.id: count for p, count in Risk._read_group(
            [('process_id', 'in', ids)], ['process_id'], ['__count'])}
        for process in processes:
            process.document_count = doc_counts.get(process.id, 0)
            process.indicator_count = ind_counts.get(process.id, 0)
            process.risk_count = risk_counts.get(process.id, 0)

    def action_open_documents(self):
        self.ensure_one()
        list_view = self.env.ref('quimibond_sgi.sgi_document_view_list',
                                 raise_if_not_found=False)
        form_view = self.env.ref('quimibond_sgi.sgi_document_view_form',
                                 raise_if_not_found=False)
        return {
            'type': 'ir.actions.act_window',
            'name': "Documentos — %s" % self.name,
            'res_model': 'documents.document',
            'view_mode': 'list,form',
            # Fija la ficha SGI (clave/estado/tipo/familia); sin ella Odoo abre
            # el formulario mínimo de captura de URL de la app Documentos.
            'views': [
                (list_view.id if list_view else False, 'list'),
                (form_view.id if form_view else False, 'form'),
            ],
            'domain': [('sgi_process_id', '=', self.id)],
            'context': {'default_sgi_process_id': self.id},
        }

    def action_open_indicators(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Indicadores — %s" % self.name,
            'res_model': 'sgi.indicator',
            'view_mode': 'list,form',
            'domain': [('process_id', '=', self.id)],
            'context': {'default_process_id': self.id},
        }

    def action_open_risks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Riesgos — %s" % self.name,
            'res_model': 'sgi.risk',
            'view_mode': 'list,form',
            'domain': [('process_id', '=', self.id)],
            'context': {'default_process_id': self.id},
        }

    def action_open_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidades — %s" % self.name,
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('sgi_process_id', '=', self.id)],
            'context': {'default_sgi_process_id': self.id},
        }

    def action_open_overdue_actions(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Acciones vencidas — %s" % self.name,
            'res_model': 'sgi.action.line',
            'view_mode': 'list,form',
            'domain': [
                ('state', '=', 'vencida'),
                '|', '|', '|',
                ('alert_id.sgi_process_id', '=', self.id),
                ('risk_id.process_id', '=', self.id),
                ('incident_id.process_id', '=', self.id),
                ('fmea_line_id.fmea_id.process_id', '=', self.id),
            ],
        }

    def action_open_red_kpis(self):
        self.ensure_one()
        red_ids = [
            indicator.id for indicator in self.indicator_ids
            if indicator.measure_ids.filtered(lambda m: m.state == 'validado')
            .sorted('period_date', reverse=True)[:1].semaphore == 'rojo'
        ]
        return {
            'type': 'ir.actions.act_window',
            'name': "KPIs en rojo — %s" % self.name,
            'res_model': 'sgi.indicator',
            'view_mode': 'list,form',
            'domain': [('id', 'in', red_ids)],
        }

    def action_open_high_risks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Riesgos de atención máxima — %s" % self.name,
            'res_model': 'sgi.risk',
            'view_mode': 'list,form',
            'domain': [
                ('process_id', '=', self.id),
                ('attention_level', 'in', ('inmediata', 'alto')),
                ('state', '!=', 'cerrado'),
            ],
        }


class SgiProcessFlow(models.Model):
    _name = 'sgi.process.flow'
    _description = "Flujo entre procesos SGI"
    _order = 'from_process_id, name'

    name = fields.Char(string="Entregable", required=True)
    from_process_id = fields.Many2one('sgi.process', string="Proceso origen", required=True, ondelete='cascade')
    to_process_id = fields.Many2one('sgi.process', string="Proceso destino", required=True, ondelete='cascade')
    document_id = fields.Many2one('documents.document', string="Formato de entrega")
    acceptance_criteria = fields.Text(string="Criterio de aceptación")
    odoo_model_id = fields.Many2one('ir.model', string="Modelo Odoo que lo materializa")
    odoo_model_name = fields.Char(related='odoo_model_id.model', string="Modelo técnico")

    @api.constrains('from_process_id', 'to_process_id')
    def _check_from_to(self):
        for flow in self:
            if flow.from_process_id == flow.to_process_id:
                raise ValidationError("El proceso origen y destino de un flujo no pueden ser el mismo.")

    def _sgi_records_domain(self):
        """Domain razonable para navegar los registros vivos del flujo."""
        self.ensure_one()
        model = self.odoo_model_id.model
        if model == 'stock.picking':
            # Sin acotar por tipo (un flujo puede ser recepción o entrega); el
            # usuario filtra en la vista. Se deja el domain vacío a propósito.
            return []
        return []

    def action_view_records(self):
        """Abre los registros vivos del modelo Odoo que materializa el flujo."""
        self.ensure_one()
        if not self.odoo_model_id:
            raise UserError(
                "El flujo «%s» no tiene un modelo de Odoo ligado (es un entregable "
                "documental)." % self.name)
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — registros" % self.name,
            'res_model': self.odoo_model_id.model,
            'view_mode': 'list,form',
            'domain': self._sgi_records_domain(),
        }
