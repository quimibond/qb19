# -*- coding: utf-8 -*-
"""Permiso de trabajo de alto riesgo (ISO 45001 8.1, E2.18; P-A19 y P-A24).

Sustituye al permiso único F-P-A14-03 (citado en las rutinas, sin documento
en el Dropbox). Un permiso por trabajo: quién lo pide, dónde, qué tipo
(alturas, espacio confinado, trabajo en caliente, eléctrico), peligros, EPP y
las verificaciones antes de empezar. Lo autorizan el jefe del área y
Seguridad (Jefe MAST); cada autorización queda sellada con usuario y hora.
Vale entre su inicio y su fin, y se cierra con las condiciones en que quedó
el área.

Flujo: borrador → solicitado → autorizado → cerrado (o cancelado). Un
permiso cerrado o cancelado es evidencia: solo el Jefe MAST lo reabre.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

WORK_TYPES = [
    ('alturas', "Trabajo en alturas"),
    ('espacio_confinado', "Espacio confinado"),
    ('caliente', "Trabajo en caliente (soldadura, corte)"),
    ('electrico', "Trabajo eléctrico"),
    ('otro', "Otro trabajo de alto riesgo"),
]

# Verificaciones y EPP sugeridos por tipo (NOM-009, NOM-033, NOM-027 y
# NOM-029 de la STPS). Son la semilla del permiso: se pueden quitar las que
# no apliquen o agregar otras antes de solicitarlo.
_COMMON_CHECKS = [
    ('verificacion', "El personal está capacitado y autorizado para este trabajo"),
    ('verificacion', "El área está delimitada y señalizada"),
    ('verificacion', "Se revisaron las herramientas y el equipo antes de usarlos"),
    ('verificacion', "Hay extintor y botiquín a la mano"),
]
WORK_TYPE_CHECKS = {
    'alturas': [
        ('verificacion', "Andamio o escalera revisados con su checklist"),
        ('verificacion', "Punto de anclaje verificado"),
        ('verificacion', "Hay quien vigile desde abajo"),
        ('epp', "Arnés de cuerpo completo con línea de vida"),
        ('epp', "Casco con barbiquejo"),
    ],
    'espacio_confinado': [
        ('verificacion', "Atmósfera medida (oxígeno, explosividad, tóxicos) antes de entrar"),
        ('verificacion', "Energías del equipo bloqueadas y etiquetadas"),
        ('verificacion', "Vigía afuera durante todo el trabajo"),
        ('verificacion', "Plan de rescate y medio de comunicación listos"),
        ('epp', "Arnés con línea de rescate"),
        ('epp', "Equipo de protección respiratoria"),
    ],
    'caliente': [
        ('verificacion', "Materiales combustibles retirados o cubiertos (radio de 10 m)"),
        ('verificacion', "Vigilancia contra incendio durante y 30 min después"),
        ('verificacion', "Equipo de soldadura y mangueras en buen estado"),
        ('epp', "Careta de soldar y lentes"),
        ('epp', "Guantes y mandil de carnaza"),
    ],
    'electrico': [
        ('verificacion', "Circuito desenergizado, bloqueado y etiquetado"),
        ('verificacion', "Ausencia de tensión comprobada con probador"),
        ('verificacion', "Puesta a tierra temporal colocada"),
        ('epp', "Guantes dieléctricos"),
        ('epp', "Calzado dieléctrico y casco clase E"),
    ],
    'otro': [],
}


class SgiWorkPermit(models.Model):
    _name = 'sgi.work.permit'
    _description = "Permiso de trabajo de alto riesgo"
    # 57.67.0: ``hr.mixin`` (Odoo 19): escribir un Many2many a hr.employee
    # exige leer hr.employee, y solo RH lo lee. Sin él, un Usuario SGI no
    # podía poner a los ejecutores del permiso (AccessError en staging).
    _inherit = ['sgi.base.mixin', 'sgi.format.mixin', 'hr.mixin']
    _order = 'date_start desc, folio desc'
    _sgi_sequence_code = 'sgi.work.permit'
    _sgi_locked_states = ('cerrado', 'cancelado')

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un permiso de trabajo con ese folio.")

    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env.company)
    name = fields.Char(string="Trabajo a realizar", required=True, tracking=True)
    work_type = fields.Selection(WORK_TYPES, string="Tipo de trabajo", required=True,
                                 default='alturas', tracking=True)
    requester_id = fields.Many2one('res.users', string="Solicitante", required=True,
                                   default=lambda self: self.env.user, tracking=True)
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI", ondelete='restrict')
    location = fields.Char(string="Lugar exacto", required=True)
    equipment_id = fields.Many2one('maintenance.equipment', string="Equipo")
    maintenance_request_id = fields.Many2one('maintenance.request', string="Orden de mantenimiento")
    executor_ids = fields.Many2many(
        'hr.employee', 'sgi_work_permit_employee_rel', 'permit_id', 'employee_id',
        string="Personal que ejecuta")
    contractor_id = fields.Many2one('res.partner', string="Contratista",
                                    help="Si el trabajo lo hace un contratista.")
    hazards = fields.Text(string="Peligros identificados",
                          help="Qué puede salir mal: caída, atmósfera peligrosa, incendio, "
                               "choque eléctrico…")
    epp_notes = fields.Text(string="EPP adicional")
    check_ids = fields.One2many('sgi.work.permit.check', 'permit_id', string="Verificaciones y EPP")
    date_start = fields.Datetime(string="Vigente desde", required=True,
                                 default=fields.Datetime.now, tracking=True)
    date_end = fields.Datetime(string="Vigente hasta", required=True, tracking=True)
    # 57.96.0 (N-06): guardado e indexado para buscar y avisar. Al guardar se
    # compara con la hora de ese momento; el paso del tiempo lo pone la acción
    # planificada «SGI: Permisos de trabajo vencidos (cada hora)».
    expired = fields.Boolean(string="Vencido", compute='_compute_expired', store=True, index=True,
                             help="Autorizado y pasada su hora de fin. Se revisa cada hora.")
    loto_ids = fields.One2many('sgi.loto', 'work_permit_id', string="Bloqueos (LOTO)",
                               help="Bloqueos de energía ligados al permiso. El permiso no se "
                                    "cierra mientras uno siga aplicado.")
    # 57.96.0 (N-06, 45001 8.1.4): evaluación SST del contratista.
    sgi_contractor_eval_ok = fields.Boolean(
        string="Contratista con evaluación SST vigente", compute='_compute_sgi_contractor_eval_ok',
        help="La evaluación SST del contratista cubre hasta el fin del permiso.")

    # --- Autorizaciones (quién y cuándo, sellado) -------------------------
    area_manager_id = fields.Many2one(
        'res.users', string="Jefe del área que autoriza", tracking=True,
        help="Autoriza por el área. Seguridad (Jefe MAST) autoriza aparte.")
    area_approved_by_id = fields.Many2one('res.users', string="Autorizó el área",
                                          readonly=True, copy=False)
    area_approved_date = fields.Datetime(string="Fecha de autorización del área",
                                         readonly=True, copy=False)
    sst_approved_by_id = fields.Many2one('res.users', string="Autorizó Seguridad",
                                         readonly=True, copy=False)
    sst_approved_date = fields.Datetime(string="Fecha de autorización de Seguridad",
                                        readonly=True, copy=False)

    # --- Cierre ------------------------------------------------------------
    close_note = fields.Text(string="Condiciones al cierre",
                             help="Cómo quedó el área: limpia, sin fuentes de ignición, "
                                  "energías restablecidas, guardas colocadas…")
    closed_by_id = fields.Many2one('res.users', string="Cerró", readonly=True, copy=False)
    closed_date = fields.Datetime(string="Fecha de cierre", readonly=True, copy=False)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('solicitado', "Solicitado"),
        ('autorizado', "Autorizado"),
        ('cerrado', "Cerrado"),
        ('cancelado', "Cancelado"),
    ], string="Estado", default='borrador', required=True, tracking=True)

    @api.constrains('date_start', 'date_end')
    def _check_dates(self):
        for permit in self:
            if permit.date_start and permit.date_end and permit.date_end <= permit.date_start:
                raise ValidationError("El permiso %s debe vencer después de su inicio." % (
                    permit.folio or permit.name))

    @api.depends('contractor_id.commercial_partner_id.sgi_sst_eval_valid_until', 'date_end')
    def _compute_sgi_contractor_eval_ok(self):
        for permit in self:
            partner = permit.contractor_id.commercial_partner_id.sudo()
            until = permit.date_end.date() if permit.date_end else fields.Date.context_today(permit)
            permit.sgi_contractor_eval_ok = bool(
                not partner or (partner.sgi_sst_eval_valid_until
                                and partner.sgi_sst_eval_valid_until >= until))

    def _sgi_people_problems(self):
        """57.96.0 (N-06, 45001 7.2 y 8.1.4): competencias exigidas por tipo
        de trabajo (sin configuración no exige nada) y evaluación SST del
        contratista (bloquea solo con el parámetro encendido). sudo: hr.employee
        y sus competencias solo los lee RH."""
        self.ensure_one()
        problems = []
        until = (self.date_end or fields.Datetime.now()).date()
        rules = self.env['sgi.work.permit.skill'].sudo().search([('work_type', '=', self.work_type)])
        if rules:
            Skill = self.env['hr.employee.skill'].sudo()
            for employee in self.sudo().executor_ids:
                missing = rules.filtered(lambda r: not Skill.search_count([
                    ('employee_id', '=', employee.id), ('skill_id', '=', r.skill_id.id),
                    '|', ('valid_to', '=', False), ('valid_to', '>=', until)]))
                if missing:
                    problems.append("• %s no tiene vigente hasta el %s: %s." % (
                        employee.name, until, ", ".join(missing.mapped('skill_id.name'))))
        required = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.permit_contractor_eval_required', '0') in ('1', 'True', 'true')
        if required and self.contractor_id and not self.sgi_contractor_eval_ok:
            problems.append(
                "• El contratista %s no tiene evaluación SST vigente hasta el fin del permiso (%s). "
                "La registra el Jefe MAST en el contacto, pestaña SGI."
                % (self.contractor_id.commercial_partner_id.sudo().display_name, until))
        return problems

    def _sgi_expired_on(self, now):
        self.ensure_one()
        return bool(self.state == 'autorizado' and self.date_end and self.date_end < now)

    @api.depends('date_end', 'state')
    def _compute_expired(self):
        now = fields.Datetime.now()
        for permit in self:
            permit.expired = permit._sgi_expired_on(now)

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for permit in self:
            permit.display_name = ("%s - %s" % (permit.folio, permit.name)
                                   if permit.folio else permit.name)

    @api.model_create_multi
    def create(self, vals_list):
        permits = super().create(vals_list)
        permits.filtered(lambda p: not p.check_ids)._sgi_load_checks()
        return permits

    def write(self, vals):
        # 57.96.0 (N-06): con energía bloqueada el permiso no se cierra (por
        # el botón o por escritura directa; tampoco el Jefe MAST).
        if vals.get('state') == 'cerrado':
            self.filtered(lambda p: p.state != 'cerrado')._sgi_check_loto_released()
        return super().write(vals)

    def _sgi_check_loto_released(self):
        Loto = self.env['sgi.loto'].sudo()
        for permit in self:
            applied = Loto.search([('work_permit_id', '=', permit.id), ('state', '=', 'bloqueado')])
            if applied:
                raise UserError(
                    "No se puede cerrar el permiso %s: el bloqueo %s sigue aplicado. Cada "
                    "trabajador retira su candado y se retira el bloqueo antes de cerrar el permiso."
                    % (permit.folio or permit.name, ", ".join(applied.mapped('display_name'))))

    def _sgi_load_checks(self):
        """Carga las verificaciones y el EPP sugeridos para el tipo de trabajo,
        sin repetir los que ya tiene."""
        for permit in self:
            existing = set(permit.check_ids.mapped('name'))
            rows = _COMMON_CHECKS + WORK_TYPE_CHECKS.get(permit.work_type, [])
            commands = [(0, 0, {'sequence': index, 'category': category, 'name': text})
                        for index, (category, text) in enumerate(rows, start=1)
                        if text not in existing]
            if commands:
                permit.check_ids = commands

    def action_load_checks(self):
        self._sgi_load_checks()
        return True

    # ------------------------------------------------------------------
    # Flujo
    # ------------------------------------------------------------------
    def _sgi_is_sst(self):
        return self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')

    def action_submit(self):
        for permit in self:
            if permit.state != 'borrador':
                raise UserError("Solo se solicita un permiso en borrador.")
            problems = []
            if not (permit.hazards or '').strip():
                problems.append("• Describa los peligros identificados.")
            if not permit.executor_ids and not permit.contractor_id:
                problems.append("• Indique el personal que ejecuta o el contratista.")
            if not permit.area_manager_id:
                problems.append("• Indique el jefe del área que autoriza.")
            if not permit.check_ids:
                problems.append("• El permiso no tiene verificaciones.")
            elif permit.check_ids.filtered(lambda c: not c.answer):
                problems.append("• Conteste todas las verificaciones y el EPP (Sí, No o No aplica).")
            if permit.check_ids.filtered(lambda c: c.answer == 'no'):
                problems.append("• Hay verificaciones o EPP en «No»: corríjalos antes de solicitar.")
            problems += permit._sgi_people_problems()
            if problems:
                raise UserError("No se puede solicitar el permiso %s:\n%s" % (
                    permit.folio or permit.name, "\n".join(problems)))
            permit.state = 'solicitado'
            if permit.area_manager_id:
                permit._sgi_schedule_activity(
                    permit.area_manager_id, "Autorizar permiso de trabajo %s" % permit.folio)
        return True

    def _sgi_check_can_authorize(self):
        for permit in self:
            if permit.state != 'solicitado':
                raise UserError("Solo se autoriza un permiso solicitado.")
            failing = permit.check_ids.filtered(lambda c: c.answer == 'no')
            if failing:
                raise UserError(
                    "No se puede autorizar el permiso %s: hay verificaciones o EPP en «No». "
                    "Corríjalos antes de empezar el trabajo:\n%s" % (
                        permit.folio, "\n".join("• %s" % c.name for c in failing)))
            people = permit._sgi_people_problems()
            if people:
                raise UserError("No se puede autorizar el permiso %s:\n%s" % (
                    permit.folio, "\n".join(people)))

    def _sgi_maybe_authorized(self):
        for permit in self:
            if permit.area_approved_by_id and permit.sst_approved_by_id:
                permit.state = 'autorizado'
                permit.message_post(body="Permiso autorizado por el área (%s) y por Seguridad (%s)." % (
                    permit.area_approved_by_id.name, permit.sst_approved_by_id.name))

    def action_approve_area(self):
        self._sgi_check_can_authorize()
        for permit in self:
            if not (self.env.user == permit.area_manager_id or self._sgi_is_sst()):
                raise UserError("Autoriza por el área el jefe indicado en el permiso (%s) "
                                "o el Jefe MAST." % (permit.area_manager_id.name or "sin indicar"))
            if permit.sst_approved_by_id == self.env.user:
                raise UserError("Las dos autorizaciones del permiso las dan dos personas distintas.")
            permit.write({'area_approved_by_id': self.env.user.id,
                          'area_approved_date': fields.Datetime.now()})
            permit.activity_ids.filtered(
                lambda a: a.user_id == self.env.user).action_feedback(feedback="Autorizado por el área.")
        self._sgi_maybe_authorized()
        return True

    def action_approve_sst(self):
        self._sgi_check_can_authorize()
        if not self._sgi_is_sst():
            raise UserError("La autorización de Seguridad la da el Jefe MAST.")
        for permit in self:
            if permit.area_approved_by_id == self.env.user:
                raise UserError("Las dos autorizaciones del permiso las dan dos personas distintas.")
            permit.write({'sst_approved_by_id': self.env.user.id,
                          'sst_approved_date': fields.Datetime.now()})
        self._sgi_maybe_authorized()
        return True

    def action_close(self):
        for permit in self:
            if permit.state != 'autorizado':
                raise UserError("Solo se cierra un permiso autorizado.")
            if not (permit.close_note or '').strip():
                raise UserError("Anote las condiciones en que quedó el área antes de cerrar el "
                                "permiso %s." % permit.folio)
            permit.write({'state': 'cerrado', 'closed_by_id': self.env.user.id,
                          'closed_date': fields.Datetime.now()})
        return True

    def action_cancel(self):
        self.write({'state': 'cancelado'})
        return True

    def action_reset(self):
        """Regresa a borrador y borra las autorizaciones (hay que volver a
        pedirlas). Desde cerrado o cancelado pasa por el candado de evidencia:
        solo el Jefe MAST."""
        self.write({'state': 'borrador', 'area_approved_by_id': False, 'area_approved_date': False,
                    'sst_approved_by_id': False, 'sst_approved_date': False,
                    'closed_by_id': False, 'closed_date': False})
        return True


class SgiWorkPermitCheck(models.Model):
    _name = 'sgi.work.permit.check'
    _description = "Verificación o EPP del permiso de trabajo"
    _order = 'permit_id, category desc, sequence, id'

    permit_id = fields.Many2one('sgi.work.permit', string="Permiso", required=True,
                                ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    category = fields.Selection([
        ('verificacion', "Verificación"),
        ('epp', "EPP"),
    ], string="Tipo", required=True, default='verificacion')
    name = fields.Char(string="Punto a verificar", required=True)
    answer = fields.Selection([
        ('si', "Sí"),
        ('no', "No"),
        ('na', "No aplica"),
    ], string="Respuesta")
    note = fields.Char(string="Observación")

    def _sgi_check_parent_open(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        locked = self.permit_id.filtered(lambda p: p.state != 'borrador')
        if locked:
            raise UserError("Las verificaciones se contestan en borrador; ya solicitado el permiso "
                            "no se modifican (regréselo a borrador para corregirlas): %s."
                            % ", ".join(locked.mapped('display_name')))

    @api.model_create_multi
    def create(self, vals_list):
        checks = super().create(vals_list)
        checks._sgi_check_parent_open()
        return checks

    def write(self, vals):
        self._sgi_check_parent_open()
        return super().write(vals)

    def unlink(self):
        self._sgi_check_parent_open()
        return super().unlink()


class SgiWorkPermitSkill(models.Model):
    """57.96.0 (N-06, 45001 7.2): competencia exigida por tipo de permiso."""
    _name = 'sgi.work.permit.skill'
    _description = "Competencia requerida por tipo de permiso de trabajo"
    _order = 'work_type, id'

    _work_type_skill_uniq = models.Constraint(
        'unique(work_type, skill_id)', "Esa competencia ya se exige para ese tipo de trabajo.")

    work_type = fields.Selection(WORK_TYPES, string="Tipo de trabajo", required=True)
    skill_id = fields.Many2one('hr.skill', string="Competencia", required=True, ondelete='restrict',
                               help="Competencia que cada persona que ejecuta debe tener vigente "
                                    "hasta el fin del permiso.")
    skill_type_id = fields.Many2one(related='skill_id.skill_type_id', string="Tipo de competencia")
    note = fields.Char(string="Por qué se exige", help="NOM o procedimiento: NOM-009-STPS, DC-3…")
    active = fields.Boolean(default=True)


class ResPartnerSstContractor(models.Model):
    """57.96.0 (N-06, 45001 8.1.4): evaluación SST del contratista."""
    _inherit = 'res.partner'

    sgi_sst_eval_valid_until = fields.Date(
        string="Evaluación SST vigente hasta", tracking=True,
        help="Hasta cuándo vale la evaluación de seguridad y salud del contratista. El permiso de "
             "trabajo la revisa. La registra el Jefe MAST.")
    sgi_sst_eval_note = fields.Text(
        string="Qué se revisó (SST)",
        help="REPSE, SUA, constancias DC-3, inducción de seguridad, seguro…")

    sgi_user_can_eval_sst = fields.Boolean(
        compute='_compute_sgi_user_can_eval_sst',
        help="Usted es Jefe MAST: puede registrar la evaluación SST del contratista.")

    _SGI_SST_EVAL_FIELDS = ('sgi_sst_eval_valid_until', 'sgi_sst_eval_note')

    @api.depends_context('uid')
    def _compute_sgi_user_can_eval_sst(self):
        can = self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        for partner in self:
            partner.sgi_user_can_eval_sst = can

    def _sgi_check_sst_eval(self, vals_list):
        if any(f in vals for vals in vals_list for f in self._SGI_SST_EVAL_FIELDS) and not (
                self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("La evaluación SST del contratista la registra el Jefe MAST.")

    @api.model_create_multi
    def create(self, vals_list):
        # También al crear (importación o RPC), no solo al editar.
        self._sgi_check_sst_eval(vals_list)
        return super().create(vals_list)

    def write(self, vals):
        self._sgi_check_sst_eval([vals])
        return super().write(vals)
