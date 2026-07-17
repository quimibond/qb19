# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError

SCALE_1_5 = [('1', "1"), ('2', "2"), ('3', "3"), ('4', "4"), ('5', "5")]

ATTENTION_LEVELS = [
    ('baja', "Baja"),
    ('intermedia', "Intermedia"),
    ('media', "Media"),
    ('inmediata', "Inmediata"),
    ('bajo', "Bajo"),
    ('medio', "Medio"),
    ('alto', "Alto"),
]


class SgiRiskCategory(models.Model):
    _name = 'sgi.risk.category'
    _description = "Categoría de riesgo/oportunidad"
    _order = 'name'

    name = fields.Char(string="Categoría", required=True)
    active = fields.Boolean(default=True)


class SgiRisk(models.Model):
    _name = 'sgi.risk'
    _description = "Riesgo / Oportunidad SGI"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'folio desc'

    folio = fields.Char(string="Folio", readonly=True, copy=False, index=True, tracking=True)
    name = fields.Char(string="Aspecto / Peligro / Situación", required=True, tracking=True)
    consequence = fields.Text(string="Consecuencia")
    instrument = fields.Selection([
        ('ryo', "Riesgos y oportunidades"),
        ('iper', "IPER (SST)"),
        ('ambiental', "Aspecto ambiental"),
        ('patrimonial', "Patrimonial"),
        ('foda', "FODA"),
    ], string="Instrumento", default='ryo', required=True, tracking=True)
    kind = fields.Selection([
        ('riesgo', "Riesgo"),
        ('oportunidad', "Oportunidad"),
    ], string="Tipo", default='riesgo', required=True)
    category_id = fields.Many2one('sgi.risk.category', string="Categoría")
    source = fields.Selection([
        ('interno', "Interno"),
        ('externo', "Externo"),
    ], string="Origen", default='interno')
    process_id = fields.Many2one('sgi.process', string="Proceso")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI")
    job_id = fields.Many2one('hr.job', string="Puesto")
    existing_controls = fields.Text(string="Controles existentes")
    operational_control_id = fields.Many2one('documents.document',
                                             string="Control operacional (ambiental)")
    condition = fields.Selection([
        ('rutinaria', "Rutinaria"),
        ('no_rutinaria', "No rutinaria"),
        ('emergencia', "Emergencia"),
    ], string="Condición (IPER)")
    foda_type = fields.Selection([
        ('fortaleza', "Fortaleza"),
        ('oportunidad', "Oportunidad"),
        ('debilidad', "Debilidad"),
        ('amenaza', "Amenaza"),
    ], string="Tipo FODA")

    action_line_ids = fields.One2many('sgi.action.line', 'risk_id', string="Acciones")
    next_review_date = fields.Date(string="Próxima revisión")
    state = fields.Selection([
        ('identificado', "Identificado"),
        ('en_tratamiento', "En tratamiento"),
        ('controlado', "Controlado"),
        ('cerrado', "Cerrado"),
    ], string="Estado", default='identificado', required=True, tracking=True)
    active = fields.Boolean(default=True)

    # Evaluación inicial
    eval_probability = fields.Selection(SCALE_1_5, string="Probabilidad")
    eval_impact = fields.Selection(SCALE_1_5, string="Impacto / Severidad")
    score = fields.Integer(string="Nivel de riesgo", compute='_compute_score', store=True)
    attention_level = fields.Selection(ATTENTION_LEVELS, string="Nivel de atención",
                                       compute='_compute_score', store=True)

    # Evaluación residual
    residual_probability = fields.Selection(SCALE_1_5, string="Probabilidad residual")
    residual_impact = fields.Selection(SCALE_1_5, string="Impacto residual")
    residual_score = fields.Integer(string="Riesgo residual", compute='_compute_residual', store=True)
    residual_level = fields.Selection(ATTENTION_LEVELS, string="Nivel residual",
                                      compute='_compute_residual', store=True)
    has_finished_actions = fields.Boolean(string="Acciones terminadas",
                                          compute='_compute_has_finished_actions')

    @api.model_create_multi
    def create(self, vals_list):
        seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('folio'):
                vals['folio'] = seq.next_by_code('sgi.risk') or '/'
        return super().create(vals_list)

    # ------------------------------------------------------------------
    # Escalas por instrumento
    # ------------------------------------------------------------------
    def _sgi_level(self, instrument, score):
        """Devuelve la clave del nivel de atención según el instrumento."""
        if not score:
            return False
        if instrument in ('ryo', 'ambiental'):
            Param = self.env['ir.config_parameter'].sudo()
            inm = int(Param.get_param('quimibond_sgi.risk_ryo_inmediata', 16))
            med = int(Param.get_param('quimibond_sgi.risk_ryo_media', 9))
            inter = int(Param.get_param('quimibond_sgi.risk_ryo_intermedia', 4))
            if score >= inm:
                return 'inmediata'
            if score >= med:
                return 'media'
            if score >= inter:
                return 'intermedia'
            return 'baja'
        if instrument == 'iper':
            if score >= 6:
                return 'alto'
            if score >= 3:
                return 'medio'
            return 'bajo'
        if instrument == 'patrimonial':
            if score >= 15:
                return 'alto'
            if score >= 6:
                return 'medio'
            return 'bajo'
        return False

    @api.constrains('instrument', 'eval_probability', 'eval_impact')
    def _check_iper_scale(self):
        for risk in self:
            if risk.instrument != 'iper':
                continue
            for value in (risk.eval_probability, risk.eval_impact,
                          risk.residual_probability, risk.residual_impact):
                if value and int(value) > 3:
                    raise ValidationError(
                        "En IPER la probabilidad y la consecuencia van de 1 a 3.")

    @api.constrains('instrument', 'foda_type')
    def _check_foda_type(self):
        for risk in self:
            if risk.instrument == 'foda' and not risk.foda_type:
                raise ValidationError("Un registro FODA requiere su tipo (F/O/D/A).")

    @api.depends('instrument', 'eval_probability', 'eval_impact')
    def _compute_score(self):
        for risk in self:
            if risk.instrument == 'foda' or not risk.eval_probability or not risk.eval_impact:
                risk.score = 0
                risk.attention_level = False
                continue
            risk.score = int(risk.eval_probability) * int(risk.eval_impact)
            risk.attention_level = risk._sgi_level(risk.instrument, risk.score)

    @api.depends('instrument', 'residual_probability', 'residual_impact')
    def _compute_residual(self):
        for risk in self:
            if risk.instrument == 'foda' or not risk.residual_probability \
                    or not risk.residual_impact:
                risk.residual_score = 0
                risk.residual_level = False
                continue
            risk.residual_score = int(risk.residual_probability) * int(risk.residual_impact)
            risk.residual_level = risk._sgi_level(risk.instrument, risk.residual_score)

    @api.depends('action_line_ids.date_done')
    def _compute_has_finished_actions(self):
        for risk in self:
            risk.has_finished_actions = any(
                line.date_done for line in risk.action_line_ids)

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for risk in self:
            risk.display_name = "%s - %s" % (risk.folio, risk.name) \
                if risk.folio else risk.name
