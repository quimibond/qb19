# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

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

# Nivel máximo de cada instrumento (ryo/ambiental → 'inmediata';
# iper/patrimonial → 'alto'). Fuente ÚNICA para el código Python (deuda
# D.29: el par se repetía a mano en la salud del proceso, la RxD y el
# candado H11). Los dominios XML lo repiten literal por necesidad; si algún
# día se normaliza la escala, se migra desde aquí. La escala doble en sí es
# deliberada: cada instrumento conserva su vocabulario del formato original.
SGI_HIGH_ATTENTION = ('inmediata', 'alto')


class SgiRiskCategory(models.Model):
    _name = 'sgi.risk.category'
    _description = "Categoría de riesgo/oportunidad"
    _order = 'name'

    name = fields.Char(string="Categoría", required=True)
    active = fields.Boolean(default=True)


class SgiRisk(models.Model):
    _name = 'sgi.risk'
    _description = "Riesgo / Oportunidad SGI"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.risk'
    _sgi_locked_states = ('cerrado',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un riesgo/oportunidad con ese folio.",
    )

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
    # Ligas inversas (H7): NCs del SGI que apuntan a este riesgo.
    sgi_nc_ids = fields.Many2many(
        'quality.alert', 'sgi_alert_risk_rel', 'risk_id', 'alert_id',
        string="NCs ligadas")
    sgi_nc_count = fields.Integer(string="# NCs ligadas",
                                  compute='_compute_sgi_nc_count')
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
    residual_note = fields.Text(
        string="Justificación del riesgo residual",
        help="Obligatoria para controlar/cerrar un riesgo de atención máxima si "
             "el riesgo residual no baja respecto al inicial.")
    has_finished_actions = fields.Boolean(string="Acciones terminadas",
                                          compute='_compute_has_finished_actions')

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

    @api.constrains('instrument', 'eval_probability', 'eval_impact',
                    'residual_probability', 'residual_impact')
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

    @api.depends('sgi_nc_ids')
    def _compute_sgi_nc_count(self):
        for risk in self:
            risk.sgi_nc_count = len(risk.sgi_nc_ids)

    def action_view_sgi_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "NCs ligadas",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('id', 'in', self.sgi_nc_ids.ids)],
        }

    # ------------------------------------------------------------------
    # Candado de riesgo alto (H11)
    # ------------------------------------------------------------------
    # Nivel más alto de cada instrumento — fuente única a nivel de módulo.
    _SGI_HIGH_ATTENTION = SGI_HIGH_ATTENTION
    _SGI_CLOSING_STATES = ('controlado', 'cerrado')

    def _sgi_check_can_close(self):
        """Un riesgo de atención máxima no se controla/cierra sin evidencia."""
        for risk in self:
            if risk.attention_level not in self._SGI_HIGH_ATTENTION:
                continue
            problems = []
            if not any(line.date_done for line in risk.action_line_ids):
                problems.append(
                    "• No hay ninguna acción de tratamiento TERMINADA.")
            residual_lower = (risk.residual_score and risk.score
                              and risk.residual_score < risk.score)
            if not (residual_lower or risk.residual_note):
                problems.append(
                    "• Falta re-evaluar el riesgo residual (que baje respecto "
                    "al inicial) o justificarlo en «Justificación del riesgo "
                    "residual».")
            if problems:
                level = dict(self._fields['attention_level'].selection).get(
                    risk.attention_level, risk.attention_level)
                raise UserError(
                    "No se puede controlar/cerrar el riesgo de atención %s "
                    "%s:\n%s" % (level, risk.folio or risk.name,
                                 "\n".join(problems)))

    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') in self._SGI_CLOSING_STATES:
            self.filtered(
                lambda r: r.state in self._SGI_CLOSING_STATES
            )._sgi_check_can_close()
        return res

    # Botones explícitos de transición (consistencia con el resto del SGI:
    # antes solo se podía avanzar clicando el statusbar). El candado H11 vive
    # en write(), así que aplica igual por cualquiera de las dos vías.
    def action_set_en_tratamiento(self):
        self.write({'state': 'en_tratamiento'})
        return True

    def action_set_controlado(self):
        self.write({'state': 'controlado'})
        return True

    def action_set_cerrado(self):
        self.write({'state': 'cerrado'})
        return True

    def action_set_identificado(self):
        self.write({'state': 'identificado'})
        return True
