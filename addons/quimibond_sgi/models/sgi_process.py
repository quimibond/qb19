# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError


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
    document_ids = fields.Many2many('documents.document', string="Documentos aplicables")
    active = fields.Boolean(default=True)

    in_flow_ids = fields.One2many('sgi.process.flow', 'to_process_id', string="Entradas")
    out_flow_ids = fields.One2many('sgi.process.flow', 'from_process_id', string="Salidas")

    nc_count = fields.Integer(string="NC abiertas", compute='_compute_health')
    overdue_action_count = fields.Integer(string="Acciones vencidas", compute='_compute_health')

    _sql_constraints = [
        ('code_uniq', 'unique(code)', "La clave de proceso debe ser única."),
    ]

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

    def _compute_display_name(self):
        for process in self:
            process.display_name = "%s - %s" % (process.code, process.name) if process.code else process.name

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

    @api.constrains('from_process_id', 'to_process_id')
    def _check_from_to(self):
        for flow in self:
            if flow.from_process_id == flow.to_process_id:
                raise ValidationError("El proceso origen y destino de un flujo no pueden ser el mismo.")
