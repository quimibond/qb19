# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError


class SgiProcess(models.Model):
    _name = 'sgi.process'
    _description = "Proceso SGI"
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
        Alert = self.env['quality.alert']
        ActionLine = self.env['sgi.action.line']
        for process in self:
            if process.id:
                process.nc_count = Alert.search_count([
                    ('sgi_process_id', '=', process.id),
                    ('stage_id.sgi_is_closing_stage', '=', False),
                    ('stage_id.sgi_is_cancel_stage', '=', False),
                ])
                process.overdue_action_count = ActionLine.search_count([
                    ('alert_id.sgi_process_id', '=', process.id),
                    ('state', '=', 'vencida'),
                ])
            else:
                process.nc_count = 0
                process.overdue_action_count = 0

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for process in self:
            process.display_name = "%s - %s" % (process.code, process.name) if process.code else process.name

    def _compute_odoo_models(self):
        for process in self:
            flows = process.in_flow_ids | process.out_flow_ids
            process.odoo_model_ids = flows.mapped('odoo_model_id')

    def _compute_counts(self):
        Doc = self.env['documents.document']
        Indicator = self.env['sgi.indicator']
        Risk = self.env['sgi.risk']
        for process in self:
            if process.id:
                process.document_count = Doc.search_count([('sgi_process_id', '=', process.id)])
                process.indicator_count = Indicator.search_count([('process_id', '=', process.id)])
                process.risk_count = Risk.search_count([('process_id', '=', process.id)])
            else:
                process.document_count = process.indicator_count = process.risk_count = 0

    def action_open_documents(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Documentos — %s" % self.name,
            'res_model': 'documents.document',
            'view_mode': 'list,kanban,form',
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
