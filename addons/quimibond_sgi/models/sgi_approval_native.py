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
import ast
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .sgi_catalog import SGI_RECORD_RELATIVES
from .sgi_guard import sgi_require_system

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



# ----------------------------------------------------------------------
# 57.89.0: un dominio de aprobación nunca lleva un campo que el documento no
# tiene. Producción, 2026-09-30: `[('company_id', '=', 1)]` escrito a mano en
# `approval_domain` de roles cuyo documento no tiene `company_id`
# (sgi.audit.program, sgi.ppap, sgi.control.plan) llegó a las reglas de
# Studio, y `studio.approval.rule._get_approval_spec` reventaba
# («Invalid field … in condition») al abrir cualquier ficha de esos modelos.
# ----------------------------------------------------------------------
_DOMAIN_ARITY = {'!': 1, '&': 2, '|': 2}


def _sgi_field_path_exists(model, path):
    """¿Existe la ruta ``a.b.c`` en ``model``? (``model`` es un recordset)."""
    names = path.split('.')
    for i, name in enumerate(names):
        field = model._fields.get(name)
        if field is None:
            return False
        if i < len(names) - 1:
            if not field.relational or field.comodel_name not in model.env:
                return False
            model = model.env[field.comodel_name]
    return True


def sgi_sanitize_domain(env, model_name, domain):
    """Quita de ``domain`` las hojas cuyo campo no existe en ``model_name``.

    ``domain`` es el texto guardado (``"[('a', '=', 1)]"``) o una lista.
    Devuelve ``(limpio, quitadas)``: si nada sobra, ``limpio`` es ``domain``
    tal cual (mismo texto, mismo formato); si sobra algo, el texto del
    dominio sin esas hojas, o ``False`` si no queda ninguna (sin condición).
    Las demás hojas y los operadores que las unen no se tocan; un ``&`` o
    ``|`` que pierde una rama queda en la otra, un ``!`` que la pierde
    desaparece. Un dominio que no es literal (``uid``, ``context_today()``…)
    o de un modelo que no existe se devuelve sin cambio.
    """
    if not domain or not model_name or model_name not in env:
        return domain, []
    try:
        items = ast.literal_eval(domain) if isinstance(domain, str) else domain
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        return domain, []
    if not isinstance(items, (list, tuple)):
        return domain, []
    model = env[model_name]
    removed = []
    items = list(items)
    pos = 0

    def parse():
        nonlocal pos
        if pos >= len(items):
            raise ValueError("dominio incompleto")
        token = items[pos]
        pos += 1
        if isinstance(token, str):
            if token not in _DOMAIN_ARITY:
                raise ValueError("operador desconocido %r" % (token,))
            children = [parse() for _i in range(_DOMAIN_ARITY[token])]
            kept = [c for c in children if c is not None]
            if len(kept) == len(children):
                return [token] + [t for c in children for t in c]
            return kept[0] if (kept and token != '!') else None
        if isinstance(token, (list, tuple)) and len(token) == 3:
            left = token[0]
            if isinstance(left, str) and not _sgi_field_path_exists(model, left):
                removed.append(token)
                return None
            return [token]
        raise ValueError("hoja inválida %r" % (token,))

    try:
        expressions = []
        while pos < len(items):
            expressions.append(parse())
    except ValueError:
        return domain, []
    if not removed:
        return domain, []
    clean = [t for e in expressions if e is not None for t in e]
    return (repr(clean) if clean else False), removed


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
        'sign.template', string="Plantilla de Sign", ondelete='set null',
        help="Plantilla de Sign que se firma para aprobar, cuando la aprobación es por firma.")

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
    condition_operator = fields.Selection(CONDITION_OPERATORS, string="Operador", default='>',
                                          help="Cómo se compara el campo con el valor para que la aprobación "
                                               "aplique.")
    condition_value = fields.Char(string="Valor", help="Número, texto o True/False.")
    approval_domain = fields.Char(string="Condición en Odoo", compute='_compute_approval_domain', store=True)
    approval_user_ids = fields.Many2many(
        'res.users', string="Personas que aprueban", compute='_compute_approval_users',
        help="Personas que hoy aprueban: las del puesto o la familia y las del suplente nombrado, "
             "sin quien también ejecuta la actividad. Al aprobar un registro, quien lo pidió tampoco.")
    approval_state = fields.Selection(
        APPROVAL_STATES, string="Aprobación en Odoo", compute='_compute_approval_state',
        help="Si la aprobación ya funciona en Odoo o qué le falta (configurarla, personas en el puesto, otra "
             "regla en el mismo botón).")
    approval_entry_count = fields.Integer(string="Aprobaciones dadas", compute='_compute_approval_entries')
    approval_last_date = fields.Datetime(string="Última aprobación", compute='_compute_approval_entries',
                                         help="Fecha de la última aprobación dada con esta regla.")

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

    def _sgi_clean_approval_domain(self):
        """57.89.0: el dominio de aprobación sin hojas de campos que el
        documento no tiene (ver ``sgi_sanitize_domain``)."""
        self.ensure_one()
        return sgi_sanitize_domain(self.env, self.approval_model_id.model, self.approval_domain or False)[0] or False

    def _sgi_sanitize_approval_domains(self):
        """Limpia ``approval_domain`` guardado; devuelve [(rol, antes, después)]."""
        changes = []
        for role in self.filtered('approval_domain'):
            clean = role._sgi_clean_approval_domain()
            if clean != role.approval_domain:
                changes.append((role, role.approval_domain, clean))
                _logger.warning("SGI: rol %s (%s): condición de aprobación %s -> %s (campos que %s no tiene)",
                                role.id, role.activity_id.display_name, role.approval_domain, clean,
                                role.approval_model_id.model)
                # Sin pasar por write(): el valor ya está limpio.
                super(SgiActivityRoleApproval, role).write({'approval_domain': clean})
        return changes

    @api.model_create_multi
    def create(self, vals_list):
        roles = super().create(vals_list)
        roles._sgi_sanitize_approval_domains()
        return roles

    # 57.13.0: antes se llamaba _check_condition, el mismo nombre que la
    # restricción de sgi_catalog («la condición solo va en quien aprueba, se
    # entera o escala»); al heredar, esta la reemplazaba y aquella dejó de
    # correr desde 56.5.0.
    @api.constrains('condition_field_id', 'condition_value', 'approval_model_id')
    def _check_approval_condition(self):
        for role in self.filtered('condition_field_id'):
            if role.condition_field_id.model_id != role.approval_model_id:
                raise ValidationError("El campo de la condición debe ser del documento que se aprueba.")
            role._sgi_condition_domain()

    def _sgi_approver_users(self, record=None):
        """Usuarios activos de las personas que aprueban: las del puesto, las
        de la familia o el dueño del proceso, más las del suplente nombrado
        (57.13.0 / 57.143.0: con la regla aprobador ≠ quien ejecuta o pide,
        sin subir por jerarquía; ver ``sgi_relative_roles``). Con ``record``
        (una solicitud de Aprobaciones) también se quita a quien la pidió. Los
        relativos que dependen del registro (solicitante, jefe del que pide…)
        no tienen personas fijas: vacío."""
        self.ensure_one()
        employees, _note = self._sgi_target_employees(record, log=record is None)
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

    # 57.13.0: sin dependencias el estado se quedaba en caché: «Sincronizar»
    # creaba la categoría y, en la misma transacción, el rol seguía «Por
    # sincronizar». Las personas del puesto no se pueden declarar aquí (el
    # formulario las vuelve a leer en cada petición).
    @api.depends('role', 'target_type', 'relative_role', 'job_id', 'family_id', 'substitute_job_id',
                 'approval_kind', 'approval_sign_template_id', 'approval_model_id',
                 'approval_method', 'approval_category_id.sgi_role_id',
                 'approval_category_id.approver_ids.user_id',
                 'approval_category_id.manager_approval')
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

    @api.model
    def _sgi_studio_installed(self):
        """57.131.0: el satélite quimibond_sgi_studio está instalado (o por
        instalar / actualizar) aunque todavía no esté en el registro: pasa en
        las migraciones del SGI, que corren antes de cargarlo."""
        return bool(self.env['ir.module.module'].sudo().search_count(
            [('name', '=', 'quimibond_sgi_studio'), ('state', 'in', ('installed', 'to upgrade', 'to install'))]))

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
                "de Studio» (quimibond_sgi_studio), que no está instalado. Instálelo o cambie el renglón "
                "a «Solicitud en Aprobaciones» o «Firma en Sign».")
        self._sgi_sync_approval_rule()
        return True

    def write(self, vals):
        res = super().write(vals)
        if {'approval_domain', 'approval_model_id', 'condition_field_id'} & set(vals):
            self._sgi_sanitize_approval_domains()
        if {'role', 'job_id', 'family_id', 'relative_role', 'target_type', 'approval_kind',
                'substitute_job_id'} & set(vals):
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
        sgi_require_system(self.env)  # 57.91.0 (K-06)
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
