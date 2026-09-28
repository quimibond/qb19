# -*- coding: utf-8 -*-
"""56.5.0: el rol «Aprueba» ligado a una aprobación NATIVA de Odoo.

Cada renglón «Aprueba» de una actividad dice qué documento de Odoo se aprueba
(modelo), en qué botón (método: confirmar la compra, validar el traslado,
publicar el asiento…) y, si aplica, con qué condición estructurada (campo,
operador y valor: «total > 50,000»). «Sincronizar» crea o actualiza la regla
de aprobación nativa de Odoo (`studio.approval.rule`) con las personas del
puesto que aprueba: el botón queda bloqueado hasta que una de ellas aprueba,
y Odoo guarda quién aprobó y cuándo (`studio.approval.entry`). Un cron
nocturno mantiene a los aprobadores al día cuando cambian las personas del
puesto. Las aprobaciones por dar entran a «Mis pendientes».
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

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
    ('por_sincronizar', "Por sincronizar"),
    ('activa', "Activa en Odoo"),
]


class StudioApprovalRuleSgi(models.Model):
    _inherit = 'studio.approval.rule'

    sgi_role_id = fields.Many2one(
        'sgi.activity.role', string="Rol SGI que aprueba", index='btree_not_null', ondelete='set null',
        help="Renglón «Aprueba» de la actividad del procedimiento que mantiene esta regla.")


class SgiActivityRoleApproval(models.Model):
    _inherit = 'sgi.activity.role'

    approval_model_id = fields.Many2one(
        'ir.model', string="Documento que se aprueba", ondelete='set null',
        compute='_compute_approval_model_id', store=True, readonly=False,
        help="Modelo de Odoo cuyo botón queda bloqueado hasta que el puesto aprueba. "
             "Se sugiere el modelo que materializa la actividad.")
    approval_model_name = fields.Char(related='approval_model_id.model', string="Modelo técnico")
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
    approval_rule_id = fields.Many2one(
        'studio.approval.rule', string="Regla nativa", readonly=True, copy=False, ondelete='set null')
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
        """Usuarios activos de las personas del puesto (o de la familia, o el
        dueño del proceso) que aprueban."""
        self.ensure_one()
        Employee = self.env['hr.employee'].sudo()
        if self.target_type == 'job' and self.job_id:
            employees = Employee.search([]).filtered(lambda e: e.job_id == self.job_id)
        elif self.target_type == 'family' and self.family_id:
            jobs = self.family_id.sudo().job_ids
            employees = Employee.search([]).filtered(lambda e: e.job_id in jobs)
        elif self.relative_role == 'dueno_proceso':
            employees = self.activity_id.process_id.sudo().owner_id
        else:
            employees = Employee
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    def _compute_approval_users(self):
        for role in self:
            role.approval_user_ids = role._sgi_approver_users() if role.role == 'aprueba' else False

    def _compute_approval_state(self):
        for role in self:
            if role.role != 'aprueba':
                role.approval_state = False
            elif not role.approval_model_id or not role.approval_method:
                role.approval_state = 'sin_configurar'
            elif not role.approval_user_ids:
                role.approval_state = 'sin_aprobadores'
            elif not role.approval_rule_id.active or not role._sgi_rule_in_sync():
                role.approval_state = 'por_sincronizar'
            else:
                role.approval_state = 'activa'

    def _compute_approval_entries(self):
        Entry = self.env['studio.approval.entry'].sudo()
        for role in self:
            entries = Entry.search([('rule_id', '=', role.approval_rule_id.id), ('approved', '=', True)],
                                   order='create_date desc') if role.approval_rule_id else Entry
            role.approval_entry_count = len(entries)
            role.approval_last_date = entries[:1].create_date

    # ------------------------------------------------------------------
    def _sgi_rule_vals(self):
        self.ensure_one()
        activity = self.activity_id.sudo()
        number = activity.number or activity.legacy_number or ''
        label = " ".join(("SGI %s %s" % (number, activity.name or '')).split())
        return {
            'name': "%s — aprueba %s" % (label, self._sgi_target_label()),
            'model_id': self.approval_model_id.id,
            'method': self.approval_method,
            'domain': self.approval_domain or False,
            'approver_ids': [(6, 0, self._sgi_approver_users().ids)],
            'message': label,
            'sgi_role_id': self.id,
            'active': True,
        }

    def _sgi_rule_in_sync(self):
        self.ensure_one()
        rule = self.approval_rule_id.sudo()
        return bool(rule) and rule.model_id == self.approval_model_id and rule.method == self.approval_method \
            and (rule.domain or False) == (self.approval_domain or False) \
            and set(rule.approver_ids.ids) == set(self._sgi_approver_users().ids)

    def _sgi_sync_approval_rule(self):
        """Crea, actualiza o archiva la regla nativa. Devuelve el estado."""
        Rule = self.env['studio.approval.rule'].sudo()
        for role in self:
            rule = role.approval_rule_id.sudo()
            wanted = (role.role == 'aprueba' and role.activity_id.active and role.approval_model_id
                      and role.approval_method and role._sgi_approver_users())
            if not wanted:
                if rule.active:
                    rule.active = False
                continue
            model = self.env.get(role.approval_model_id.model)
            if model is None or not callable(getattr(model, role.approval_method, None)):
                raise UserError("«%s» no tiene el botón «%s»: revisa el documento y el botón que se aprueba "
                                "(%s)." % (role.approval_model_id.name, role.approval_method,
                                           role.activity_id.display_name))
            vals = role._sgi_rule_vals()
            if rule and rule.entry_ids and (rule.model_id.id != vals['model_id'] or rule.method != vals['method']
                                            or (rule.domain or False) != vals['domain']):
                # Una regla con aprobaciones registradas no cambia de botón ni
                # de condición: se archiva (queda la evidencia) y nace otra.
                rule.active = False
                rule = Rule
            if rule:
                rule.write(vals)
            else:
                role.approval_rule_id = Rule.create(vals)
        return True

    def action_sgi_sync_approval(self):
        self._sgi_sync_approval_rule()
        return True

    def action_sgi_open_approval_rule(self):
        self.ensure_one()
        if not self.approval_rule_id:
            raise UserError("Aún no hay regla: pulsa «Sincronizar».")
        return {'type': 'ir.actions.act_window', 'res_model': 'studio.approval.rule',
                'res_id': self.approval_rule_id.id, 'view_mode': 'form'}

    def action_sgi_open_approval_entries(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Aprobaciones — %s" % self.activity_id.display_name,
                'res_model': 'studio.approval.entry', 'view_mode': 'list,form',
                'domain': [('rule_id', '=', self.approval_rule_id.id)], 'context': {'create': False}}

    def write(self, vals):
        res = super().write(vals)
        if {'role', 'job_id', 'family_id', 'relative_role', 'target_type'} & set(vals):
            self.filtered('approval_rule_id')._sgi_sync_approval_rule()
        return res

    def unlink(self):
        self.approval_rule_id.sudo().write({'active': False})
        return super().unlink()

    @api.model
    def cron_sgi_sync_approvals(self):
        """Cada noche: aprobadores al día (cambian las personas de los
        puestos) y reglas archivadas si la actividad o el rol ya no existen."""
        roles = self.sudo().search([('approval_rule_id', '!=', False)])
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
            self.role_ids.filtered('approval_rule_id')._sgi_sync_approval_rule()
        return res
