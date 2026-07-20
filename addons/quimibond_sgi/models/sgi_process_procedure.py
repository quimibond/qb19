# -*- coding: utf-8 -*-
"""Procedimiento vivo: el Desarrollo del procedimiento como datos del proceso.

El procedimiento deja de ser un PDF adjunto — su alcance, responsabilidades y
actividades viven como datos estructurados del proceso, y el PDF se genera
desde Odoo con el layout del F-P-G01-02 (ver report/report_procedure.xml).

Son LÍNEAS (no evidencia), así que NO usan el mixin de folio/inmutabilidad.
"""
from odoo import models, fields, api


class SgiProcessProcedure(models.Model):
    """Extiende el proceso con el cuerpo del procedimiento controlado."""
    _inherit = 'sgi.process'

    scope = fields.Text(
        string="Alcance",
        help="A qué áreas/actividades aplica el procedimiento (sección 2).")
    env_aspects = fields.Text(
        string="Descripción de aspectos ambientales",
        help="Aspectos ambientales del proceso (sección 5 del procedimiento).")
    norm_ids = fields.Many2many(
        'sgi.norm', 'sgi_process_norm_rel', 'process_id', 'norm_id',
        string="Marco normativo",
        help="Normas ISO que rigen el proceso (sección 7).")
    job_responsibility_ids = fields.One2many(
        'sgi.process.responsibility', 'process_id',
        string="Responsabilidades de las áreas")
    activity_ids = fields.One2many(
        'sgi.process.activity', 'process_id',
        string="Actividades del procedimiento")
    activity_count = fields.Integer(
        string="# Actividades", compute='_compute_activity_count')

    @api.depends('activity_ids')
    def _compute_activity_count(self):
        for process in self:
            process.activity_count = len(process.activity_ids)


class SgiProcessResponsibility(models.Model):
    """Responsabilidad de un rol/puesto dentro del procedimiento (sección 3)."""
    _name = 'sgi.process.responsibility'
    _description = "Responsabilidad de área en el procedimiento"
    _order = 'process_id, sequence, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade',
        index=True)
    sequence = fields.Integer(string="Secuencia", default=10)
    job_id = fields.Many2one(
        'hr.job', string="Puesto", required=True,
        help="Puesto de hr.job al que corresponde el rol.")
    name = fields.Char(
        string="Rol en el procedimiento", required=True,
        help="Nombre del rol tal como aparece en el procedimiento "
             "(no siempre mapea 1:1 al puesto de hr.job).")
    responsibilities = fields.Text(string="Responsabilidades")

    @api.onchange('job_id')
    def _onchange_job_id(self):
        if self.job_id and not self.name:
            self.name = self.job_id.name


class SgiProcessActivity(models.Model):
    """Actividad (numeral) del Desarrollo del procedimiento (sección 4)."""
    _name = 'sgi.process.activity'
    _description = "Actividad del procedimiento"
    _order = 'process_id, sequence, number, id'

    process_id = fields.Many2one(
        'sgi.process', string="Proceso", required=True, ondelete='cascade',
        index=True)
    sequence = fields.Integer(string="Secuencia", default=10)
    number = fields.Char(string="Numeral", help="Ej. 4.2.3.1")
    block = fields.Selection([
        ('inicial', "Actividades iniciales"),
        ('desarrollo', "Desarrollo"),
        ('final', "Actividades finales"),
    ], string="Bloque", default='desarrollo', required=True)
    section = fields.Char(
        string="Sección", help="Título del apartado, ej. '4.2.3 Cotización de productos'.")
    name = fields.Char(string="Resumen", help="Resumen corto de la actividad.")
    description = fields.Text(
        string="Descripción", help="Texto completo del numeral del procedimiento.")
    responsible_job_ids = fields.Many2many(
        'hr.job', 'sgi_activity_job_rel', 'activity_id', 'job_id',
        string="Puestos responsables")
    responsible_role = fields.Char(
        string="Rol responsable",
        help="Nombre del rol en negritas del procedimiento (no siempre mapea a "
             "un puesto de hr.job).")
    format_document_ids = fields.Many2many(
        'documents.document', 'sgi_activity_format_rel', 'activity_id', 'document_id',
        string="Formatos referenciados",
        domain=[('sgi_is_controlled', '=', True)],
        help="Claves de formato en rojo que la actividad genera o usa.")
    related_procedure_id = fields.Many2one(
        'documents.document', string="Procedimiento relacionado",
        domain=[('sgi_doc_type', '=', 'procedimiento')],
        help="Otro procedimiento que rige esta actividad (ej. marca P-A22).")
    odoo_ref = fields.Char(
        string="Dónde se ejecuta en Odoo",
        help="Ej. 'Ventas > Pedidos', 'Helpdesk Servicio Técnico'.")
    note = fields.Text(
        string="Nota", help="Notas resaltadas del procedimiento.")

    @api.depends('number', 'name', 'section')
    def _compute_display_name(self):
        for activity in self:
            label = activity.name or activity.section or ''
            activity.display_name = (
                "%s %s" % (activity.number, label)).strip() if activity.number else label
