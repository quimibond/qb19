# -*- coding: utf-8 -*-
import logging

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

from .sgi_risk import SGI_HIGH_ATTENTION

_logger = logging.getLogger(__name__)


class SgiProcess(models.Model):
    """Proceso del SGI: dueño, etapas, actividades, entradas y salidas, documentos, indicadores,
    riesgos y semáforo. Es dato: se captura o se carga, no viene en el módulo."""
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
    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)
    process_type = fields.Selection([
        ('cop', "Cadena de valor (COP)"),
        ('estrategico', "Estratégico"),
        ('soporte', "Soporte"),
    ], string="Tipo", default='cop', required=True,
        group_expand='_group_expand_process_type')
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

    # Ficha del proceso: dónde empieza, dónde termina, qué entra y qué sale.
    start_trigger = fields.Text(
        string="Disparador de inicio",
        help="Qué hace que el proceso arranque (ej. llega un pedido).")
    end_trigger = fields.Text(
        string="Termina cuando",
        help="Qué marca el fin del proceso (ej. la factura queda cobrada).")
    inputs = fields.Text(string="Entradas")
    outputs = fields.Text(string="Salidas")
    # C-001 (19.0.56.31.0, decisión 3 de Jose): el DOCUMENTO manda
    # (documents.document.sgi_replaced_by_process_id, ondelete restrict); aquí
    # solo se lee su inverso. La tabla M2M vieja sgi_process_replaced_doc_rel
    # quedó respaldada en un adjunto JSON por proceso (pre-migrate 56.31.0).
    replaced_document_ids = fields.One2many(
        'documents.document', 'sgi_replaced_by_process_id',
        string="Procedimientos que sustituye", readonly=True,
        help="Procedimientos del Dropbox que este proceso sustituye. Se captura "
             "en la ficha de cada procedimiento («Lo sustituye el proceso»). "
             "Siguen vigentes mientras el proceso esté en borrador o piloto; al "
             "entrar en vigor el proceso pasan a obsoletos y a «Baja "
             "tramitada».")
    # PR-1 (53.1.0): quién sustituyó a este proceso al archivarlo por «replaces».
    # Una carga futura mueve al sucesor lo que se quede colgado aquí.
    replaced_by_id = fields.Many2one(
        'sgi.process', string="Sustituido por", copy=False, index=True,
        domain="[('id', '!=', id), ('active', '=', True)]",
        help="Proceso que tomó el lugar de este al archivarlo. La carga lo "
             "llena con «replaces»; en un proceso archivado sin sucesor se "
             "captura a mano y la siguiente carga mueve al sucesor lo que "
             "quede colgado (indicadores, riesgos abiertos, documentos vigentes).",
        ondelete='restrict')
    owner_valid = fields.Boolean(
        string="Dueño válido", compute='_compute_owner_valid',
        help="El dueño es un empleado activo con usuario de Odoo. Sin eso "
             "nadie recibe los avisos del proceso y la salud se pinta en rojo.")

    in_flow_ids = fields.One2many('sgi.process.flow', 'to_process_id', string="Flujos de entrada")
    out_flow_ids = fields.One2many('sgi.process.flow', 'from_process_id', string="Flujos de salida")

    # Ficha del proceso: todo lo ligado, navegable desde un solo lugar.
    linked_document_ids = fields.One2many(
        'documents.document', 'sgi_process_id', string="Documentos del proceso")
    procedure_ids = fields.One2many(
        'documents.document', 'sgi_process_id', string="Procedimientos e instructivos",
        domain=[('sgi_doc_type_id.code', 'in', ('procedimiento', 'instructivo')),
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
    flow_count = fields.Integer(string="# Conexiones", compute='_compute_flow_count',
                                help="Entregables que recibe de otros procesos más los que entrega.")

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        "La clave de proceso debe ser única por empresa.",
    )

    def _group_expand_process_type(self, values, domain):
        """El kanban muestra siempre las tres columnas (estratégico, cadena de
        valor, soporte), aunque alguna esté vacía."""
        return ['estrategico', 'cop', 'soporte']

    @api.depends('owner_id.active', 'owner_id.user_id')
    def _compute_owner_valid(self):
        for process in self:
            owner = process.owner_id
            process.owner_valid = bool(owner and owner.active and owner.user_id)

    @api.onchange('owner_id')
    def _onchange_owner_id_approver(self):
        if self.owner_id.user_id and not self.doc_approver_id:
            self.doc_approver_id = self.owner_id.user_id

    @api.model
    def _sgi_default_vobo_user(self):
        """Vo.Bo. por omisión de los procedimientos: parámetro
        quimibond_sgi.vobo_user_id (Jefe de MAST y SGI)."""
        value = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.vobo_user_id')
        try:
            return self.env['res.users'].sudo().browse(int(value)).exists()
        except (TypeError, ValueError):
            return self.env['res.users']

    @api.model_create_multi
    def create(self, vals_list):
        vobo = self._sgi_default_vobo_user()
        Employee = self.env['hr.employee'].sudo()
        for vals in vals_list:
            if not vals.get('doc_approver_id') and vals.get('owner_id'):
                user = Employee.browse(vals['owner_id']).user_id
                if user:
                    vals['doc_approver_id'] = user.id
            if not vals.get('doc_vobo_id') and vobo:
                vals['doc_vobo_id'] = vobo.id
        records = super().create(vals_list)
        if any(vals.get('owner_id') for vals in vals_list):
            self.sudo()._sgi_sync_process_owner_group()
        return records

    # --- Punto 5 (45.0.0) + L-005 (56.31.0): al poner vigente el proceso, lo
    # que sustituye queda obsoleto y con la baja tramitada. Movido desde
    # sgi_cleanup.py (B-004).
    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') == 'vigente':
            self._sgi_obsolete_replaced_documents()
        if {'owner_id', 'active', 'company_id'} & set(vals):
            self.sudo()._sgi_sync_process_owner_group()
        return res

    # --- 57.0.0 (entrega 6, decisión de Jose): «Dueño de proceso (SGI)» ---
    @api.model
    def _sgi_sync_process_owner_group(self):
        """Sincroniza ``quimibond_sgi.group_sgi_process_owner`` con los datos:
        entran los usuarios activos de ``owner_id`` (es un ``hr.employee``: se
        toma su ``user_id``) de los procesos activos de la empresa del SGI
        (``sgi.config._sgi_company()``); salen los que ya no son dueños de
        ningún proceso activo. Toca solo la membresía directa de ese grupo;
        idempotente (sin cambios no escribe). Lo llaman ``create``/``write``
        de ``sgi.process`` (``owner_id``, ``active``, ``company_id``), el
        post-migrate de 19.0.57.0.0 y el cron diario de respaldo (que además
        recoge los cambios de ``hr.employee.user_id``).

        Verificado en producción por MCP (solo lectura, 2026-09-29, empresa
        1): ``aggregate_records sgi.process groupby [owner_id] domain
        [company_id = 1, active = True]`` → 14 procesos activos, todos con
        dueño, 11 empleados distintos; 10 con usuario activo interno (ids 6,
        15, 22, 33, 35, 68, 88, 128, 135, 152) y 1 sin usuario (Francisco
        González, empleado 564, que no entra). Esperado la primera vez: 10
        dueños en el grupo."""
        group = self.env.ref('quimibond_sgi.group_sgi_process_owner', raise_if_not_found=False)
        if not group:
            return {'added': [], 'removed': []}
        group = group.sudo()
        company = self.env['sgi.config']._sgi_company()
        processes = self.env['sgi.process'].sudo().with_context(active_test=True).search([
            ('company_id', '=', company.id), ('owner_id', '!=', False)])
        owners = processes.mapped('owner_id.user_id').filtered(lambda u: u.active and not u.share)
        current = group.with_context(active_test=False).user_ids
        to_add = owners - current
        to_remove = current - owners
        commands = [(4, user.id) for user in to_add] + [(3, user.id) for user in to_remove]
        if commands:
            group.write({'user_ids': commands})
            _logger.info("SGI: grupo «Dueño de proceso» sincronizado: +%s −%s (quedan %d).",
                         to_add.ids, to_remove.ids, len(owners))
        return {'added': to_add.ids, 'removed': to_remove.ids}

    def _sgi_obsolete_replaced_documents(self):
        """Los procedimientos que sustituye un proceso vigente (los que tienen
        a este proceso en «Lo sustituye el proceso») pasan a obsoletos, con
        fecha y motivo, y su migración a «Baja tramitada». Idempotente: solo
        toca los vigentes, y completa la baja de los ya obsoletos."""
        for process in self.filtered(lambda p: p.state == 'vigente'):
            replaced = process.replaced_document_ids
            docs = replaced.filtered(lambda d: d.sgi_state == 'vigente')
            reason = "Lo sustituye el proceso %s, que entró en vigor." % process.display_name
            for doc in docs:
                doc.sudo().write({'sgi_state': 'obsoleto', 'sgi_obsolete_reason': reason,
                                  'sgi_migration_state': 'baja'})
                doc.message_post(body=(
                    "Obsoleto: lo sustituye el proceso %s, que entró en vigor." % process.display_name))
            pending = replaced.filtered(
                lambda d: d.sgi_state == 'obsoleto' and d.sgi_migration_state != 'baja')
            if pending:
                pending.sudo().write({'sgi_migration_state': 'baja'})
            if not docs:
                continue
            process.message_post(body=(
                "Al entrar en vigor quedaron obsoletos, con la baja tramitada, %d "
                "documento(s) sustituido(s): %s." % (
                    len(docs), ", ".join(docs.mapped(lambda d: d.sgi_code or d.name)))))
            process._sgi_warn_foreign_procedure_refs(docs)
            _logger.info("SGI: %s vigente → %d documento(s) sustituido(s) obsoleto(s) y en baja.",
                         process.code, len(docs))
        return True

    def _sgi_warn_foreign_procedure_refs(self, docs):
        """57.16.0 (H-016): actividades activas de OTRO proceso que citan como
        «Procedimiento relacionado» un documento que este proceso acaba de
        obsoletar. No se cambia nada (la fuente de verdad es el documento,
        decisión 3): se avisa en el chatter de los dos procesos y al dueño del
        otro proceso (o al Jefe MAST) para que liguen el procedimiento
        vigente. Devuelve las actividades encontradas."""
        self.ensure_one()
        Activity = self.env['sgi.process.activity'].sudo()
        refs = Activity.search([
            ('related_procedure_id', 'in', docs.ids),
            ('process_id', '!=', self.id), ('process_id', '!=', False)])
        if not refs:
            return refs
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        lines = []
        for other in refs.process_id:
            acts = refs.filtered(lambda a, o=other: a.process_id == o)
            items = ", ".join("%s (cita %s)" % (
                a.number or a.name,
                a.related_procedure_id.sgi_code or a.related_procedure_id.name) for a in acts)
            state = dict(other._fields['state'].selection).get(other.state, other.state)
            lines.append("%s [%s]: %s" % (other.display_name, state, items))
            note = ("El proceso %s entró en vigor y obsoletó procedimientos que estas "
                    "actividades de %s todavía citan: %s. Liga el procedimiento vigente "
                    "(o quítalo) en cada actividad." % (
                        self.display_name, other.display_name, items))
            other.sudo().message_post(body=note)
            user_id = other.owner_id.user_id.id or manager_id
            if user_id:
                Cron._sgi_step(
                    "aviso de procedimiento obsoleto citado en %s" % other.display_name,
                    lambda o=other, n=note, u=user_id: Cron._sgi_schedule(
                        o, "Actividades citan un procedimiento obsoleto (%s)" % self.code,
                        n, u, key='procedimiento_obsoleto_citado:%d' % self.id))
        self.message_post(body=(
            "Aviso: actividades de otros procesos citan un procedimiento que quedó "
            "obsoleto: %s." % "; ".join(lines)))
        _logger.info("SGI: %s vigente → %d actividad(es) de otros procesos citan un "
                     "procedimiento obsoleto.", self.code, len(refs))
        return refs

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
             ('attention_level', 'in', SGI_HIGH_ATTENTION),
             ('state', '!=', 'cerrado')],
            ['process_id'], ['__count'])}
        for process in processes:
            process.nc_count = nc_counts.get(process.id, 0)
            process.overdue_action_count = overdue_counts.get(process.id, 0)
            process.red_kpi_count = red_counts.get(process.id, 0)
            process.open_high_risk_count = risk_counts.get(process.id, 0)
            # Un proceso sin dueño que reciba avisos no se gobierna: rojo.
            if (not process.owner_valid or process.open_high_risk_count
                    or (process.nc_count and process.red_kpi_count)):
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

    @api.depends('in_flow_ids', 'out_flow_ids')
    def _compute_flow_count(self):
        for process in self:
            process.flow_count = len(process.in_flow_ids) + len(process.out_flow_ids)

    def action_open_flows(self):
        """Botón «Conexiones»: los flujos que entran y salen del proceso."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Conexiones — %s" % (self.code or self.name),
            'res_model': 'sgi.process.flow',
            'view_mode': 'list,form',
            'domain': ['|', ('from_process_id', '=', self.id), ('to_process_id', '=', self.id)],
            'context': {'default_from_process_id': self.id},
        }

    def _sgi_master_list_documents(self):
        """Documentos controlados del proceso para la lista maestra: vigentes
        y en piloto, por tipo y clave."""
        self.ensure_one()
        return self.env['documents.document'].sudo().search(
            [('sgi_is_controlled', '=', True), ('sgi_process_id', '=', self.id),
             ('sgi_state', 'in', ('vigente', 'piloto'))],
            order='sgi_doc_type, sgi_code, name')

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
                ('attention_level', 'in', SGI_HIGH_ATTENTION),
                ('state', '!=', 'cerrado'),
            ],
        }


class SgiProcessFlow(models.Model):
    """Flujo entre dos procesos: qué pasa de uno a otro y, si es un documento de Odoo, de qué
    modelo."""
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
    company_id = fields.Many2one(
        related='from_process_id.company_id', string="Empresa", store=True,
        index=True)

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
