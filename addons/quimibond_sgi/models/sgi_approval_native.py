# -*- coding: utf-8 -*-
"""56.5.0 / 56.6.0: el rol «Aprueba» ligado a una aprobación NATIVA de Odoo.

Cada renglón «Aprueba» de una actividad dice cómo se aprueba: con una
solicitud en Aprobaciones (decisiones sin documento), con una firma en Sign,
o bloqueando el botón de un documento de Odoo (modelo, botón y, si aplica,
condición estructurada: «total > 50,000»). Un cron nocturno mantiene a los
aprobadores al día cuando cambian las personas del puesto. Las aprobaciones
por dar entran a «Mis pendientes».

57.9.0 (A-010, D-10): la regla de aprobación nativa del botón
(`studio.approval.rule`, de Studio) vive en el satélite
``quimibond_sgi_studio`` (``auto_install`` con ``web_studio``). El núcleo ya
no depende de Studio: aquí quedan el tipo, el documento, el botón, la
condición y las solicitudes y firmas; los ganchos ``_sgi_button_*`` son los
que el satélite completa. Sin el satélite, un rol «Botón de Odoo» queda
«Por sincronizar» y «Sincronizar» avisa que falta el módulo.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .sgi_catalog import SGI_RECORD_RELATIVES

_logger = logging.getLogger(__name__)

# Botón que se aprueba por omisión según el documento.
APPROVAL_METHOD_BY_MODEL = {
    'purchase.order': 'button_confirm',
    'sale.order': 'action_confirm',
    'account.move': 'action_post',
    'account.payment': 'action_post',
    'stock.picking': 'button_validate',
    'stock.quant': 'action_apply_inventory',
    'mrp.production': 'button_mark_done',
    'hr.payslip.run': 'action_validate',
    'hr.expense.sheet': 'action_approve_expense_sheets',
}
CONDITION_OPERATORS = [
    ('>', "mayor que"),
    ('>=', "mayor o igual que"),
    ('<', "menor que"),
    ('<=', "menor o igual que"),
    ('=', "igual a"),
    ('!=', "distinto de"),
]
CONDITION_FIELD_TYPES = ('float', 'monetary', 'integer', 'char', 'selection', 'boolean')
APPROVAL_STATES = [
    ('sin_configurar', "Sin configurar"),
    ('sin_aprobadores', "Sin personas en el puesto"),
    ('conflicto', "Ya hay otra regla en el botón"),
    ('por_sincronizar', "Por sincronizar"),
    ('activa', "Activa en Odoo"),
    # 57.13.0: solicitante, quien detecta o área responsable, o el jefe del
    # que pide en un botón o firma: no hay aprobador fijo que sincronizar.
    ('relativo', "Depende de cada registro"),
]
# 56.6.0: no toda aprobación es un botón. Las decisiones sin documento van a
# una solicitud de Aprobaciones y lo que hoy se firma en papel, a Sign.
APPROVAL_KINDS = [
    ('boton', "Botón de Odoo"),
    ('solicitud', "Solicitud en Aprobaciones"),
    ('firma', "Firma en Sign"),
]




class ApprovalCategorySgiRole(models.Model):
    _inherit = 'approval.category'

    sgi_role_id = fields.Many2one(
        'sgi.activity.role', string="Rol SGI que aprueba", index='btree_not_null', ondelete='set null',
        help="Categoría creada para un renglón «Aprueba»: sus aprobadores siguen a las personas del puesto.")


class SgiActivityRoleApproval(models.Model):
    _inherit = 'sgi.activity.role'

    approval_kind = fields.Selection(
        APPROVAL_KINDS, string="Cómo se aprueba", default='boton',
        help="Botón de Odoo: la regla nativa bloquea el botón del documento. Solicitud: una "
             "categoría de Aprobaciones para decisiones sin documento. Firma: una plantilla de Sign.")
    approval_category_id = fields.Many2one(
        'approval.category', string="Categoría de Aprobaciones", ondelete='set null',
        help="Vacía: «Sincronizar» crea una categoría propia con las personas del puesto.")
    approval_sign_template_id = fields.Many2one(
        'sign.template', string="Plantilla de Sign", ondelete='set null')

    approval_model_id = fields.Many2one(
        'ir.model', string="Documento que se aprueba", ondelete='set null',
        compute='_compute_approval_model_id', store=True, readonly=False,
        help="Modelo de Odoo cuyo botón queda bloqueado hasta que el puesto aprueba. "
             "Se sugiere el modelo que materializa la actividad.")
    approval_method = fields.Char(
        string="Botón que se aprueba", compute='_compute_approval_method', store=True, readonly=False,
        help="Método del botón (button_confirm, action_post, button_validate…).")
    condition_field_id = fields.Many2one(
        'ir.model.fields', string="Campo de la condición", ondelete='set null',
        domain="[('model_id', '=', approval_model_id), ('ttype', 'in', %s), ('store', '=', True)]"
               % (list(CONDITION_FIELD_TYPES),),
        help="Solo cuando la aprobación aplica bajo una condición (ej. total de la orden).")
    condition_operator = fields.Selection(CONDITION_OPERATORS, string="Operador", default='>')
    condition_value = fields.Char(string="Valor", help="Número, texto o True/False.")
    approval_domain = fields.Char(string="Condición en Odoo", compute='_compute_approval_domain', store=True)
    approval_user_ids = fields.Many2many(
        'res.users', string="Personas que aprueban", compute='_compute_approval_users')
    approval_state = fields.Selection(
        APPROVAL_STATES, string="Aprobación en Odoo", compute='_compute_approval_state')
    approval_entry_count = fields.Integer(string="Aprobaciones dadas", compute='_compute_approval_entries')
    approval_last_date = fields.Datetime(string="Última aprobación", compute='_compute_approval_entries')

    # ------------------------------------------------------------------
    @api.depends('role', 'activity_id.measure_model_id')
    def _compute_approval_model_id(self):
        for role in self:
            # Siempre se asigna (campo guardado); solo se sugiere si está vacío.
            suggested = role.activity_id.measure_model_id if role.role == 'aprueba' else False
            role.approval_model_id = role.approval_model_id or suggested

    @api.depends('approval_model_id')
    def _compute_approval_method(self):
        for role in self:
            suggested = APPROVAL_METHOD_BY_MODEL.get(role.approval_model_id.model, False) \
                if role.approval_model_id else False
            role.approval_method = role.approval_method or suggested

    @api.depends('condition_field_id', 'condition_operator', 'condition_value')
    def _compute_approval_domain(self):
        for role in self:
            role.approval_domain = repr(role._sgi_condition_domain()) if role.condition_field_id else False

    def _sgi_condition_domain(self):
        """[(campo, operador, valor)] con el valor convertido al tipo del campo."""
        self.ensure_one()
        field = self.condition_field_id
        if not field:
            return []
        raw = (self.condition_value or '').strip()
        if field.ttype in ('float', 'monetary', 'integer'):
            try:
                number = float(raw.replace(',', ''))
            except ValueError:
                raise ValidationError("El valor de la condición de «%s» debe ser un número (ej. 50000)."
                                      % (self.activity_id.display_name,))
            value = int(number) if field.ttype == 'integer' else number
        elif field.ttype == 'boolean':
            value = raw.lower() in ('1', 'true', 'verdadero', 'si', 'sí')
        else:
            value = raw
        return [(field.name, self.condition_operator or '=', value)]

    @api.constrains('condition_field_id', 'condition_value', 'approval_model_id')
    def _check_condition(self):
        for role in self.filtered('condition_field_id'):
            if role.condition_field_id.model_id != role.approval_model_id:
                raise ValidationError("El campo de la condición debe ser del documento que se aprueba.")
            role._sgi_condition_domain()

    def _sgi_approver_users(self):
        """Usuarios activos de las personas que aprueban: las del puesto, las
        de la familia o el dueño del proceso (57.13.0: con la regla aprobador
        ≠ ejecutor, ver ``sgi_relative_roles``). Los relativos que dependen
        del registro (solicitante, jefe del que pide…) no tienen personas
        fijas: vacío."""
        self.ensure_one()
        employees, _note = self._sgi_target_employees()
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    def _sgi_category_manager_approval(self):
        """57.13.0: «Jefe del área que pide» en una solicitud de Aprobaciones
        es nativo: la categoría pide la aprobación del jefe del empleado que
        hace la solicitud (``manager_approval``)."""
        self.ensure_one()
        return 'required' if (self.target_type == 'relative'
                              and self.relative_role == 'jefe_del_solicitante') else False

    def _sgi_record_dependent(self):
        """El aprobador depende de cada registro y no hay forma nativa de
        fijarlo en el catálogo (57.13.0)."""
        self.ensure_one()
        return self.target_type == 'relative' and self.relative_role in SGI_RECORD_RELATIVES \
            and not (self.approval_kind == 'solicitud' and self._sgi_category_manager_approval())

    def _compute_approval_users(self):
        for role in self:
            role.approval_user_ids = role._sgi_approver_users() if role.role == 'aprueba' else False

    def _compute_approval_state(self):
        for role in self:
            if role.role != 'aprueba':
                role.approval_state = False
            elif role._sgi_record_dependent():
                role.approval_state = 'relativo'
            elif role.approval_kind == 'firma':
                role.approval_state = 'activa' if role.approval_sign_template_id else 'sin_configurar'
            elif role.approval_kind == 'solicitud':
                category = role.approval_category_id.sudo()
                manager = role._sgi_category_manager_approval()
                if not role.approval_user_ids and not manager and not (category and category.approver_ids):
                    role.approval_state = 'sin_aprobadores'
                elif not category or (category.sgi_role_id == role and (set(
                        category.approver_ids.user_id.ids) != set(role.approval_user_ids.ids)
                        or (category.manager_approval or False) != manager)):
                    role.approval_state = 'por_sincronizar'
                else:
                    role.approval_state = 'activa'
            elif not role.approval_model_id or not role.approval_method:
                role.approval_state = 'sin_configurar'
            elif not role.approval_user_ids:
                role.approval_state = 'sin_aprobadores'
            else:
                role.approval_state = role._sgi_button_state()

    def _compute_approval_entries(self):
        Request = self.env['approval.request'].sudo()
        Sign = self.env['sign.request'].sudo()
        for role in self:
            if role.approval_kind == 'solicitud' and role.approval_category_id:
                done = Request.search([('category_id', '=', role.approval_category_id.id),
                                       ('request_status', '=', 'approved')], order='write_date desc')
                role.approval_entry_count = len(done)
                role.approval_last_date = done[:1].write_date
            elif role.approval_kind == 'firma' and role.approval_sign_template_id:
                done = Sign.search([('template_id', '=', role.approval_sign_template_id.id),
                                    ('state', '=', 'signed')], order='write_date desc')
                role.approval_entry_count = len(done)
                role.approval_last_date = done[:1].write_date
            else:
                role.approval_entry_count, role.approval_last_date = role._sgi_button_entries()

    # ------------------------------------------------------------------
    # Ganchos del botón de Odoo (los completa quimibond_sgi_studio, A-010).
    # ------------------------------------------------------------------
    def _sgi_button_supported(self):
        """¿Hay quien mantenga la regla nativa del botón? Solo con el satélite."""
        return False

    def _sgi_button_state(self):
        """Estado de un rol «Botón de Odoo» ya configurado y con personas."""
        self.ensure_one()
        return 'por_sincronizar'

    def _sgi_button_entries(self):
        """(aprobaciones dadas, fecha de la última) del botón."""
        self.ensure_one()
        return 0, False

    def _sgi_sync_button(self):
        """Crea o pone al día la regla nativa del botón (satélite)."""
        return True

    def _sgi_release_button(self):
        """Archiva la regla nativa del botón, si la hay (satélite)."""
        return True

    def _sgi_has_native_approval(self):
        """El rol mantiene algo vivo en Odoo: su categoría de Aprobaciones (o,
        con el satélite, su regla de Studio)."""
        self.ensure_one()
        return self.approval_category_id.sgi_role_id == self

    @api.model
    def _sgi_native_approval_domain(self):
        """Roles que el cron nocturno revisa."""
        return [('approval_category_id.sgi_role_id', '!=', False)]

    # ------------------------------------------------------------------
    def _sgi_sync_category(self):
        """Solicitud en Aprobaciones: la categoría propia del rol (se crea si
        no hay) con las personas del puesto como aprobadores. Una categoría
        elegida que no es del rol no se toca."""
        Category = self.env['approval.category'].sudo()
        for role in self:
            users = role._sgi_approver_users()
            category = role.approval_category_id.sudo()
            if not role.activity_id.active or role.role != 'aprueba':
                if category.sgi_role_id == role:
                    category.active = False
                continue
            manager = role._sgi_category_manager_approval()
            if not category:
                if not users and not manager:
                    continue
                activity = role.activity_id.sudo()
                number = activity.number or activity.legacy_number or ''
                category = Category.create({
                    'name': " ".join(("SGI %s %s" % (number, activity.name or '')).split()),
                    'description': "Aprobación del procedimiento: %s" % (role.condition or "siempre"),
                    'sgi_role_id': role.id, 'approval_minimum': 1,
                    'manager_approval': manager,
                })
                role.approval_category_id = category
            if category.sgi_role_id == role:
                current = category.approver_ids
                keep = current.filtered(lambda a: a.user_id in users)
                (current - keep).unlink()
                missing = users - keep.user_id
                if missing:
                    category.write({'approver_ids': [(0, 0, {'user_id': u.id, 'required': False})
                                                     for u in missing]})
                if (category.manager_approval or False) != manager:
                    category.manager_approval = manager
                category.active = True

    def _sgi_sync_approval_rule(self):
        """Pone al día la aprobación del rol según su tipo."""
        for role in self:
            if role.approval_kind != 'boton':
                role._sgi_release_button()
                if role.approval_kind == 'solicitud':
                    role._sgi_sync_category()
                continue
            role._sgi_sync_button()
        return True

    def action_sgi_sync_approval(self):
        buttons = self.filtered(lambda r: r.role == 'aprueba' and r.approval_kind == 'boton')
        if buttons and not self._sgi_button_supported():
            raise UserError(
                "La aprobación por botón de Odoo la mantiene el módulo «Quimibond SGI - Aprobaciones "
                "de Studio» (quimibond_sgi_studio), que no está instalado. Instálalo o cambia el renglón "
                "a «Solicitud en Aprobaciones» o «Firma en Sign».")
        self._sgi_sync_approval_rule()
        return True

    def write(self, vals):
        res = super().write(vals)
        if {'role', 'job_id', 'family_id', 'relative_role', 'target_type', 'approval_kind'} & set(vals):
            self.filtered(lambda r: r._sgi_has_native_approval())._sgi_sync_approval_rule()
        return res

    def unlink(self):
        self._sgi_release_button()
        self.approval_category_id.sudo().filtered(lambda c: c.sgi_role_id in self).write({'active': False})
        return super().unlink()

    @api.model
    def cron_sgi_sync_approvals(self):
        """Cada noche: aprobadores al día (cambian las personas de los
        puestos) y reglas archivadas si la actividad o el rol ya no existen."""
        roles = self.sudo().search(self._sgi_native_approval_domain())
        for role in roles:
            try:
                with self.env.cr.savepoint():
                    role._sgi_sync_approval_rule()
            except Exception:  # noqa: BLE001 — un rol mal configurado no detiene a los demás
                _logger.exception("SGI: no se pudo sincronizar la aprobación del rol %s", role.id)
        return len(roles)


class SgiProcessActivityApproval(models.Model):
    _inherit = 'sgi.process.activity'

    def write(self, vals):
        res = super().write(vals)
        if 'active' in vals:
            self.role_ids.filtered(lambda r: r._sgi_has_native_approval())._sgi_sync_approval_rule()
        return res
