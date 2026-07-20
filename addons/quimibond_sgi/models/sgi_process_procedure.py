# -*- coding: utf-8 -*-
"""Procedimiento vivo: el Desarrollo del procedimiento como datos del proceso.

El procedimiento deja de ser un PDF adjunto — su alcance, responsabilidades y
actividades viven como datos estructurados del proceso, y el PDF se genera
desde Odoo con el layout del F-P-G01-02 (ver report/report_procedure.xml).

Son LÍNEAS (no evidencia), así que NO usan el mixin de folio/inmutabilidad.
"""
from odoo import models, fields, api
from odoo.exceptions import UserError


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

    # Firmas del procedimiento (bloque del F-P-G01-02). Se imprimen como
    # nombre + cargo; el PDF generado es copia NO controlada, sin imagen de firma.
    doc_owner_id = fields.Many2one(
        'res.users', string="Responsable del documento",
        help="Elabora / es dueño del procedimiento (bloque de firmas).")
    doc_approver_id = fields.Many2one(
        'res.users', string="Aprueba")
    doc_vobo_id = fields.Many2one(
        'res.users', string="Vo.Bo.")

    @api.depends('activity_ids')
    def _compute_activity_count(self):
        for process in self:
            process.activity_count = len(process.activity_ids)

    # --- Helpers del reporte "Imprimir procedimiento (F-P-G01-02)" -----------
    def _sgi_procedure_document(self):
        """Documento controlado vigente (P-Xnn) que encabeza el procedimiento.
        La clave, fecha de emisión, área y revisión del encabezado se leen EN
        VIVO de aquí (única fuente de verdad)."""
        self.ensure_one()
        docs = self.procedure_ids.filtered(
            lambda d: d.sgi_doc_type == 'procedimiento' and d.sgi_state == 'vigente')
        return docs[:1]

    def _sgi_format_revision(self, code):
        """Revisión viva del documento controlado vigente con esa clave (para el
        pie F-P-G01-02), o False."""
        return self.env['sgi.format.map'].sudo()._revision_of(code)

    def _sgi_document_by_code(self, code):
        """Documento vigente con esa clave (ref. a F-P-S01-01, etc.)."""
        return self.env['documents.document'].sudo().search([
            ('sgi_code', '=', code), ('sgi_state', '=', 'vigente'),
        ], limit=1)

    def _sgi_env_risks(self):
        """Riesgos ambientales ligados al proceso (sección 5)."""
        self.ensure_one()
        return self.risk_ids.filtered(lambda r: r.instrument == 'ambiental')

    def _sgi_iper_risks(self):
        """Riesgos IPER (SST) ligados al proceso (sección 6)."""
        self.ensure_one()
        return self.risk_ids.filtered(lambda r: r.instrument == 'iper')

    def _sgi_documented_info(self):
        """Sección 8, GENERADA: unión sin duplicados de los formatos referenciados
        en las actividades + la familia FK del procedimiento + las referencias
        cruzadas, ordenada por clave."""
        self.ensure_one()
        docs = self.activity_ids.mapped('format_document_ids')
        proc = self._sgi_procedure_document()
        if proc:
            docs |= proc.sgi_family_document_ids
            docs |= proc.sgi_reference_ids
        return docs.sorted(lambda d: (d.sgi_code or '￿', d.name or ''))

    def action_print_procedure(self):
        self.ensure_one()
        return self.env.ref(
            'quimibond_sgi.action_report_procedure').report_action(self)


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
    odoo_menu_id = fields.Many2one(
        'ir.ui.menu', string="Menú de Odoo",
        help="Menú real donde se ejecuta la actividad; el texto impreso se toma "
             "de la ruta.")
    note = fields.Text(
        string="Nota", help="Notas resaltadas del procedimiento.")

    @api.depends('number', 'name', 'section')
    def _compute_display_name(self):
        for activity in self:
            label = activity.name or activity.section or ''
            activity.display_name = (
                "%s %s" % (activity.number, label)).strip() if activity.number else label

    @api.onchange('odoo_menu_id')
    def _onchange_odoo_menu_id(self):
        """Si hay menú y el texto está vacío, toma la ruta legible del menú."""
        if self.odoo_menu_id and not self.odoo_ref:
            self.odoo_ref = self.odoo_menu_id.complete_name

    def action_open_odoo(self):
        """Abre el menú real de Odoo donde se ejecuta la actividad, respetando el
        dominio/contexto/vistas de su acción original."""
        self.ensure_one()
        action = self.odoo_menu_id.action if self.odoo_menu_id else False
        if not action or action._name != 'ir.actions.act_window':
            raise UserError("Esta actividad no tiene menú de Odoo ligado.")
        return action.read()[0]
