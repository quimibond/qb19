# -*- coding: utf-8 -*-
"""Matriz de aspectos e impactos ambientales (ISO 14001 §6.1.2, E2.23).

Un renglón por aspecto de una actividad: qué se hace, qué aspecto ambiental
tiene (emisión, residuo, consumo…), qué impacto causa y en qué condición
(normal, anormal o emergencia). La significancia sale de tres criterios:
severidad × frecuencia (1 a 5 cada uno) da el nivel (bajo, moderado, severo o
crítico, como los nombra P-E01) y un requisito legal aplicable lo vuelve
significativo aunque el nivel sea bajo. Los aspectos significativos llevan su
control operacional (E2.34) antes de quedar evaluados.

No duplica ``sgi.risk``: la evaluación del aspecto vive aquí, y si hace falta
tratarlo con acciones (CAPA) se crea o liga un riesgo del instrumento
«Aspecto ambiental», que ya tiene acciones, candado de cierre y semáforo.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

SCALE_1_5 = [('1', "1"), ('2', "2"), ('3', "3"), ('4', "4"), ('5', "5")]

ASPECT_LEVELS = [
    ('bajo', "Bajo"),
    ('moderado', "Moderado"),
    ('severo', "Severo"),
    ('critico', "Crítico"),
]
# Niveles que vuelven significativo al aspecto (P-E01: «los aspectos
# moderados, severos y críticos» llevan control operacional).
SIGNIFICANT_LEVELS = ('moderado', 'severo', 'critico')


class SgiEnvAspect(models.Model):
    _name = 'sgi.env.aspect'
    _description = "Aspecto e impacto ambiental (ISO 14001 6.1.2)"
    _inherit = ['sgi.base.mixin', 'sgi.format.mixin']
    _order = 'significant desc, score desc, folio desc'
    _sgi_sequence_code = 'sgi.env.aspect'

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un aspecto ambiental con ese folio.")

    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)
    active = fields.Boolean(default=True)
    process_id = fields.Many2one('sgi.process', string="Proceso", ondelete='restrict',
                                 tracking=True)
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI", ondelete='restrict')
    activity = fields.Char(string="Actividad u operación", required=True, tracking=True,
                           help="Qué se hace: «Lavado de tambos», «Carga de caldera».")
    aspect_type = fields.Selection([
        ('emision', "Emisión a la atmósfera"),
        ('descarga', "Descarga de agua residual"),
        ('residuo_peligroso', "Residuo peligroso"),
        ('residuo_manejo_especial', "Residuo de manejo especial"),
        ('residuo_urbano', "Residuo sólido urbano"),
        ('energia', "Consumo de energía"),
        ('agua', "Consumo de agua"),
        ('materiales', "Consumo de materiales o químicos"),
        ('ruido', "Ruido"),
        ('suelo', "Afectación al suelo"),
        ('otro', "Otro"),
    ], string="Tipo de aspecto", required=True, default='residuo_peligroso', tracking=True)
    name = fields.Char(string="Aspecto ambiental", required=True, tracking=True,
                       help="El elemento de la actividad que interactúa con el ambiente: "
                            "«Generación de estopas impregnadas de aceite».")
    impact = fields.Text(string="Impacto ambiental", required=True,
                         help="El cambio en el ambiente que causa: «Contaminación del suelo».")
    condition = fields.Selection([
        ('normal', "Normal"),
        ('anormal', "Anormal"),
        ('emergencia', "Emergencia"),
    ], string="Condición", required=True, default='normal', tracking=True,
        help="Normal: operación diaria. Anormal: arranques, paros, mantenimiento. "
             "Emergencia: derrame, fuga, incendio.")

    # --- Criterios de significancia --------------------------------------
    severity = fields.Selection(SCALE_1_5, string="Severidad", tracking=True,
                                help="1 = sin efecto apreciable … 5 = daño grave o irreversible.")
    frequency = fields.Selection(SCALE_1_5, string="Frecuencia", tracking=True,
                                 help="1 = rara vez … 5 = continuo o diario.")
    legal_requirement_ids = fields.Many2many(
        'sgi.legal.requirement', 'sgi_env_aspect_legal_rel', 'aspect_id', 'requirement_id',
        string="Requisitos legales aplicables")
    legal_requirement = fields.Boolean(
        string="Tiene requisito legal", compute='_compute_legal_requirement', store=True,
        readonly=False, tracking=True,
        help="Un requisito legal aplicable vuelve significativo el aspecto. Se marca solo "
             "al ligar un requisito legal; también se puede marcar a mano.")
    score = fields.Integer(string="Puntaje", compute='_compute_significance', store=True,
                           help="Severidad × frecuencia.")
    level = fields.Selection(ASPECT_LEVELS, string="Nivel", compute='_compute_significance',
                             store=True)
    significant = fields.Boolean(
        string="Significativo", compute='_compute_significance', store=True,
        help="Nivel moderado o mayor, o con requisito legal aplicable.")

    # --- Control operacional y tratamiento -------------------------------
    control_description = fields.Text(
        string="Control operacional",
        help="Cómo se controla el aspecto: procedimiento, instructivo, equipo, "
             "almacén temporal, contratista autorizado…")
    operational_control_id = fields.Many2one(
        'documents.document', string="Documento del control",
        domain=[('sgi_is_controlled', '=', True)])
    risk_id = fields.Many2one(
        'sgi.risk', string="Riesgo ambiental (tratamiento)", ondelete='set null',
        domain=[('instrument', '=', 'ambiental')],
        help="Riesgo del instrumento «Aspecto ambiental» donde viven las acciones "
             "de tratamiento, si hacen falta.")
    responsible_id = fields.Many2one('res.users', string="Responsable del área", tracking=True)

    # --- Revisión periódica ----------------------------------------------
    review_frequency_months = fields.Integer(string="Frecuencia de revisión (meses)",
                                             default=12)
    last_review_date = fields.Date(string="Última revisión", readonly=True, copy=False,
                                   tracking=True)
    next_review_date = fields.Date(string="Próxima revisión", copy=False, tracking=True)
    state = fields.Selection([
        ('borrador', "En evaluación"),
        ('evaluado', "Evaluado"),
        ('obsoleto', "Ya no aplica"),
    ], string="Estado", default='borrador', required=True, tracking=True)

    @api.depends('legal_requirement_ids')
    def _compute_legal_requirement(self):
        for aspect in self:
            if aspect.legal_requirement_ids:
                aspect.legal_requirement = True
            elif not aspect.legal_requirement:
                aspect.legal_requirement = False

    @api.model
    def _sgi_aspect_level(self, score):
        """Nivel por puntaje. Umbrales en parámetros del sistema (como los de
        riesgos): moderado desde 5, severo desde 10, crítico desde 16."""
        if not score:
            return False
        Param = self.env['ir.config_parameter'].sudo()
        critical = int(Param.get_param('quimibond_sgi.aspect_critico', 16))
        severe = int(Param.get_param('quimibond_sgi.aspect_severo', 10))
        moderate = int(Param.get_param('quimibond_sgi.aspect_moderado', 5))
        if score >= critical:
            return 'critico'
        if score >= severe:
            return 'severo'
        if score >= moderate:
            return 'moderado'
        return 'bajo'

    @api.depends('severity', 'frequency', 'legal_requirement')
    def _compute_significance(self):
        for aspect in self:
            score = int(aspect.severity) * int(aspect.frequency) \
                if aspect.severity and aspect.frequency else 0
            aspect.score = score
            aspect.level = self._sgi_aspect_level(score)
            aspect.significant = bool(
                aspect.level in SIGNIFICANT_LEVELS or aspect.legal_requirement)

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for aspect in self:
            aspect.display_name = ("%s - %s" % (aspect.folio, aspect.name)
                                   if aspect.folio else aspect.name)

    # ------------------------------------------------------------------
    # Evaluación y revisión
    # ------------------------------------------------------------------
    def _sgi_check_can_evaluate(self):
        for aspect in self:
            problems = []
            if not (aspect.severity and aspect.frequency):
                problems.append("• Capture la severidad y la frecuencia.")
            if aspect.significant and not (
                    (aspect.control_description or '').strip() or aspect.operational_control_id):
                problems.append(
                    "• Es un aspecto significativo: describa su control operacional o "
                    "ligue el documento del control.")
            if problems:
                raise UserError("No se puede dar por evaluado el aspecto %s:\n%s" % (
                    aspect.folio or aspect.name, "\n".join(problems)))

    def action_evaluate(self):
        """Sella la revisión de hoy, programa la siguiente y deja el aspecto
        evaluado. Sirve igual para la evaluación inicial y para la revisión
        anual o por cambio (P-E01)."""
        today = fields.Date.context_today(self)
        self._sgi_check_can_evaluate()
        for aspect in self:
            months = aspect.review_frequency_months or 12
            aspect.write({
                'state': 'evaluado',
                'last_review_date': today,
                'next_review_date': today + relativedelta(months=months),
            })
            aspect.message_post(body="Evaluación registrada: severidad %s × frecuencia %s = %d "
                                     "(%s)%s. Próxima revisión: %s." % (
                                         aspect.severity, aspect.frequency, aspect.score,
                                         dict(ASPECT_LEVELS).get(aspect.level, '-'),
                                         ", significativo" if aspect.significant else "",
                                         aspect.next_review_date))
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        self.write({'state': 'obsoleto', 'active': False})
        return True

    def action_create_risk(self):
        """Crea (o abre) el riesgo ambiental donde viven las acciones de
        tratamiento. Probabilidad = frecuencia, impacto = severidad."""
        self.ensure_one()
        if not self.risk_id:
            self.risk_id = self.env['sgi.risk'].create({
                'name': self.name,
                'instrument': 'ambiental',
                'kind': 'riesgo',
                'consequence': self.impact,
                'process_id': self.process_id.id,
                'sgi_area_id': self.sgi_area_id.id,
                'existing_controls': self.control_description or False,
                'operational_control_id': self.operational_control_id.id,
                'eval_probability': self.frequency or False,
                'eval_impact': self.severity or False,
            })
            self.message_post(body="Se creó el riesgo ambiental %s para su tratamiento." % (
                self.risk_id.display_name))
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sgi.risk',
            'res_id': self.risk_id.id,
            'view_mode': 'form',
        }
