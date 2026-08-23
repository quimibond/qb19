# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError

SCALE_1_10 = [(str(i), str(i)) for i in range(1, 11)]


class SgiFmea(models.Model):
    _name = 'sgi.fmea'
    _description = "AMEF - Análisis de Modo y Efecto de Falla (P-C10)"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.fmea'

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un AMEF con ese folio.",
    )

    name = fields.Char(string="Nombre", required=True, tracking=True)
    fmea_type = fields.Selection([
        ('proceso', "Proceso (PFMEA)"),
        ('diseno', "Diseño (DFMEA)"),
    ], string="Tipo", default='proceso', required=True, tracking=True)
    product_tmpl_id = fields.Many2one('product.template', string="Producto")
    process_id = fields.Many2one('sgi.process', string="Proceso")
    revision = fields.Char(string="Revisión", default="00", tracking=True)
    date = fields.Date(string="Fecha", default=fields.Date.context_today)
    team_ids = fields.Many2many('res.users', string="Equipo AMEF")
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    line_ids = fields.One2many('sgi.fmea.line', 'fmea_id', string="Modos de falla")
    max_npr = fields.Integer(string="NPR máximo", compute='_compute_max_npr', store=True)
    # Ligas inversas (H7): NCs del SGI que apuntan a este AMEF.
    sgi_nc_ids = fields.One2many('quality.alert', 'sgi_fmea_id', string="NCs ligadas")
    sgi_nc_count = fields.Integer(string="# NCs ligadas",
                                  compute='_compute_sgi_nc_count')

    @api.depends('line_ids.npr')
    def _compute_max_npr(self):
        for fmea in self:
            fmea.max_npr = max(fmea.line_ids.mapped('npr') or [0])

    @api.depends('sgi_nc_ids')
    def _compute_sgi_nc_count(self):
        for fmea in self:
            fmea.sgi_nc_count = len(fmea.sgi_nc_ids)

    def action_view_sgi_ncs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "NCs ligadas",
            'res_model': 'quality.alert',
            'view_mode': 'list,form',
            'domain': [('id', 'in', self.sgi_nc_ids.ids)],
        }

    def action_set_vigente(self):
        for fmea in self:
            pending = fmea.line_ids.filtered(
                lambda l: l.requires_action and not l.action_line_ids)
            if pending:
                raise UserError(
                    "El AMEF %s no puede pasar a Vigente: hay %d modo(s) de falla con "
                    "NPR alto sin acción registrada." % (fmea.folio or fmea.name, len(pending)))
            # IATF: la acción del modo de falla con NPR alto debe estar
            # TERMINADA, no solo registrada (deuda declarada en Fase 3).
            unfinished = fmea.line_ids.filtered(
                lambda l: l.requires_action and l.action_line_ids
                and not l.action_line_ids.filtered('date_done'))
            if unfinished:
                raise UserError(
                    "El AMEF %s no puede pasar a Vigente: hay %d modo(s) de falla con "
                    "NPR alto cuya acción no está TERMINADA (fecha de terminación)."
                    % (fmea.folio or fmea.name, len(unfinished)))
            # AIAG: la acción terminada debe demostrarse con la re-evaluación
            # (S/O/D post). Sin NPR post, "la acción funcionó" es una
            # afirmación sin dato.
            no_reeval = fmea.line_ids.filtered(
                lambda l: l.requires_action and not l.npr_post)
            if no_reeval:
                raise UserError(
                    "El AMEF %s no puede pasar a Vigente: hay %d modo(s) de falla con "
                    "NPR alto sin la re-evaluación post-acción (S/O/D post)."
                    % (fmea.folio or fmea.name, len(no_reeval)))
            # Mismo patrón que el residual de riesgos: el NPR post debe bajar,
            # o quedar la justificación escrita de por qué no baja.
            not_improved = fmea.line_ids.filtered(
                lambda l: l.requires_action and l.npr_post >= l.npr
                and not (l.post_note or '').strip())
            if not_improved:
                raise UserError(
                    "El AMEF %s no puede pasar a Vigente: hay %d modo(s) de falla "
                    "cuyo NPR post NO bajó respecto al inicial y no tienen "
                    "justificación escrita (campo «Justificación NPR post»)."
                    % (fmea.folio or fmea.name, len(not_improved)))
            fmea.state = 'vigente'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        for fmea in self:
            fmea.state = 'obsoleto'
            # Paridad con el plan de control: al obsoletar se avisa al Jefe
            # MAST para revisar si el proceso queda sin AMEF vigente.
            if manager_id:
                Cron._sgi_schedule(
                    fmea,
                    "Revisar cobertura del AMEF obsoleto %s" % (fmea.folio or fmea.name),
                    "El AMEF pasó a obsoleto. Verifique que el proceso/producto "
                    "tenga un AMEF vigente que lo cubra.",
                    manager_id)
        return True

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for fmea in self:
            fmea.display_name = "%s - %s" % (fmea.folio, fmea.name) if fmea.folio else fmea.name


class SgiFmeaLine(models.Model):
    _name = 'sgi.fmea.line'
    _description = "Línea de AMEF"
    _order = 'fmea_id, sequence, id'

    fmea_id = fields.Many2one('sgi.fmea', string="AMEF", required=True, ondelete='cascade')
    sequence = fields.Integer(string="Secuencia", default=10)
    step = fields.Char(string="Paso / Función", required=True)
    failure_mode = fields.Char(string="Modo de falla")
    effect = fields.Char(string="Efecto")
    severity = fields.Selection(SCALE_1_10, string="Severidad (S)")
    cause = fields.Char(string="Causa")
    occurrence = fields.Selection(SCALE_1_10, string="Ocurrencia (O)")
    current_controls = fields.Char(string="Controles actuales")
    detection = fields.Selection(SCALE_1_10, string="Detección (D)")
    npr = fields.Integer(string="NPR", compute='_compute_npr', store=True)
    requires_action = fields.Boolean(string="Requiere acción", compute='_compute_npr', store=True)
    action_line_ids = fields.One2many('sgi.action.line', 'fmea_line_id', string="Acciones")

    # Re-evaluación posterior
    severity_post = fields.Selection(SCALE_1_10, string="Severidad post")
    occurrence_post = fields.Selection(SCALE_1_10, string="Ocurrencia post")
    detection_post = fields.Selection(SCALE_1_10, string="Detección post")
    npr_post = fields.Integer(string="NPR post", compute='_compute_npr_post', store=True)
    post_note = fields.Char(
        string="Justificación NPR post",
        help="Obligatoria para marcar el AMEF vigente cuando el NPR post de "
             "un modo de falla con NPR alto no baja respecto al inicial "
             "(p. ej. la severidad no puede reducirse por diseño).")

    def _sgi_npr_threshold(self):
        return int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.fmea_npr_action', 100))

    @api.depends('severity', 'occurrence', 'detection')
    def _compute_npr(self):
        threshold = self._sgi_npr_threshold()
        for line in self:
            if line.severity and line.occurrence and line.detection:
                line.npr = int(line.severity) * int(line.occurrence) * int(line.detection)
            else:
                line.npr = 0
            line.requires_action = bool(line.npr) and line.npr >= threshold

    @api.depends('severity_post', 'occurrence_post', 'detection_post')
    def _compute_npr_post(self):
        for line in self:
            if line.severity_post and line.occurrence_post and line.detection_post:
                line.npr_post = int(line.severity_post) * int(line.occurrence_post) * \
                    int(line.detection_post)
            else:
                line.npr_post = 0

    # Un AMEF vigente/obsoleto es evidencia: sus líneas no se borran (salvo MAST).
    # En borrador el equipo edita/elimina libremente; el candado protege lo publicado.
    _SGI_LOCKED_PARENT_STATES = ('vigente', 'obsoleto')

    def unlink(self):
        if not self.env.su and not self.env.user.has_group(
                'quimibond_sgi.group_sgi_manager'):
            locked = self.filtered(
                lambda l: l.fmea_id.state in self._SGI_LOCKED_PARENT_STATES)
            if locked:
                raise UserError(
                    "No se puede borrar una línea de un AMEF vigente u obsoleto "
                    "(es evidencia). Pide al Jefe de MAST regresarlo a borrador.\n\n"
                    "AMEF: %s" % ", ".join(locked.mapped('fmea_id.display_name')))
        return super().unlink()
