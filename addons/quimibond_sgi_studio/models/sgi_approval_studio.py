# -*- coding: utf-8 -*-
"""El rol «Aprueba» de tipo «Botón de Odoo»: la regla de aprobación nativa de
Studio (`studio.approval.rule`) con las personas del puesto. El botón queda
bloqueado hasta que una de ellas aprueba y Odoo guarda quién aprobó y cuándo
(`studio.approval.entry`).

Vivía en ``quimibond_sgi/models/sgi_approval_native.py`` hasta 57.8.0; el
núcleo deja ganchos (``_sgi_button_*``) que aquí se completan.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

from odoo.addons.quimibond_sgi.models.sgi_approval_native import sgi_sanitize_domain

_logger = logging.getLogger(__name__)


class StudioApprovalRuleSgi(models.Model):
    _inherit = 'studio.approval.rule'

    sgi_role_id = fields.Many2one(
        'sgi.activity.role', string="Rol SGI que aprueba", index='btree_not_null', ondelete='set null',
        help="Renglón «Aprueba» de la actividad del procedimiento que mantiene esta regla.")

    # 1.0.3: ninguna regla de aprobación guarda una condición sobre un campo
    # que su documento no tiene. Studio la evalúa con filtered_domain al abrir
    # cada ficha del modelo, y un campo inexistente revienta la ficha
    # («Invalid field sgi.audit.program.company_id», 2026-09-30).
    @api.model
    def _sgi_clean_domain(self, model_id, domain):
        model = self.env['ir.model'].browse(model_id).model if model_id else False
        clean, removed = sgi_sanitize_domain(self.env, model, domain)
        if removed:
            _logger.warning("SGI: regla de aprobación de %s: se quitan de la condición %s campos que el "
                            "documento no tiene: %s", model, domain, removed)
        return clean

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('domain') and vals.get('model_id'):
                vals['domain'] = self._sgi_clean_domain(vals['model_id'], vals['domain'])
        return super().create(vals_list)

    def write(self, vals):
        if not vals.get('domain'):
            return super().write(vals)
        if vals.get('model_id'):
            return super().write(dict(vals, domain=self._sgi_clean_domain(vals['model_id'], vals['domain'])))
        for model in self.model_id:
            rules = self.filtered(lambda r, m=model: r.model_id == m)
            super(StudioApprovalRuleSgi, rules).write(
                dict(vals, domain=self._sgi_clean_domain(model.id, vals['domain'])))
        rest = self.filtered(lambda r: not r.model_id)
        if rest:
            super(StudioApprovalRuleSgi, rest).write(vals)
        return True

    def _sgi_sanitize_domains(self):
        """Quita de la condición guardada las hojas de campos que el documento
        no tiene; las demás hojas no se tocan. Devuelve [(regla, antes,
        después)]. Lo usa la migración 19.0.1.0.3."""
        changes = []
        for rule in self.filtered('domain'):
            clean, removed = sgi_sanitize_domain(self.env, rule.model_id.model, rule.domain)
            if removed:
                changes.append((rule, rule.domain, clean))
                rule.write({'domain': clean})
        return changes


class SgiActivityRoleStudio(models.Model):
    _inherit = 'sgi.activity.role'

    approval_conflict_rule_ids = fields.Many2many(
        'studio.approval.rule', string="Otras reglas en el botón", compute='_compute_approval_conflicts',
        help="Reglas de aprobación activas en el mismo botón que no mantiene este rol: el documento "
             "pediría dos aprobaciones. Adóptala o quítala antes de sincronizar.")
    approval_rule_id = fields.Many2one(
        'studio.approval.rule', string="Regla nativa", readonly=True, copy=False, ondelete='set null',
        help="Regla de aprobación de Odoo que el SGI creó para este rol.")

    @api.depends('approval_kind', 'approval_model_id', 'approval_method', 'approval_rule_id')
    def _compute_approval_conflicts(self):
        Rule = self.env['studio.approval.rule'].sudo()
        for role in self:
            if role.role != 'aprueba' or role.approval_kind != 'boton' or not role.approval_model_id \
                    or not role.approval_method:
                role.approval_conflict_rule_ids = False
                continue
            rules = Rule.search([('model_id', '=', role.approval_model_id.id),
                                 ('method', '=', role.approval_method), ('active', '=', True)])
            # Reglas de otros roles SGI en el mismo botón son niveles válidos
            # (cada uno con su condición); solo cuentan las que nadie mantiene.
            role.approval_conflict_rule_ids = rules.filtered(
                lambda r: not r.sgi_role_id and r != role.approval_rule_id)

    # ------------------------------------------------------------------
    # Ganchos del núcleo
    # ------------------------------------------------------------------
    def _sgi_button_supported(self):
        return True

    def _sgi_button_state(self):
        self.ensure_one()
        if self.approval_conflict_rule_ids:
            return 'conflicto'
        if not self.approval_rule_id.active or not self._sgi_rule_in_sync():
            return 'por_sincronizar'
        return 'activa'

    def _sgi_button_entries(self):
        self.ensure_one()
        if not self.approval_rule_id:
            return 0, False
        entries = self.env['studio.approval.entry'].sudo().search(
            [('rule_id', '=', self.approval_rule_id.id), ('approved', '=', True)], order='create_date desc')
        return len(entries), entries[:1].create_date

    def _sgi_has_native_approval(self):
        self.ensure_one()
        return bool(self.approval_rule_id) or super()._sgi_has_native_approval()

    @api.model
    def _sgi_native_approval_domain(self):
        return ['|', ('approval_rule_id', '!=', False)] + super()._sgi_native_approval_domain()

    def _sgi_release_button(self):
        self.approval_rule_id.sudo().filtered('active').write({'active': False})
        return super()._sgi_release_button()

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
            # 1.0.3: la condición limpia (sin campos que el documento no tiene).
            'domain': self._sgi_clean_approval_domain(),
            'approver_ids': [(6, 0, self._sgi_approver_users().ids)],
            'message': label,
            'sgi_role_id': self.id,
            'active': True,
        }

    def _sgi_rule_in_sync(self):
        self.ensure_one()
        rule = self.approval_rule_id.sudo()
        return bool(rule) and rule.model_id == self.approval_model_id and rule.method == self.approval_method \
            and (rule.domain or False) == self._sgi_clean_approval_domain() \
            and set(rule.approver_ids.ids) == set(self._sgi_approver_users().ids)

    def _sgi_sync_button(self):
        """Crea o pone al día la regla nativa del botón."""
        Rule = self.env['studio.approval.rule'].sudo()
        for role in self:
            rule = role.approval_rule_id.sudo()
            wanted = (role.role == 'aprueba' and role.activity_id.active and role.approval_model_id
                      and role.approval_method and role._sgi_approver_users())
            if not wanted:
                if rule.active:
                    rule.active = False
                continue
            if role.approval_conflict_rule_ids:
                raise UserError(
                    "El botón «%s» de «%s» ya tiene otra regla de aprobación (%s): el documento pediría "
                    "dos aprobaciones. Usa «Adoptar regla» para que el SGI la mantenga, o quítala en "
                    "Studio." % (role.approval_method, role.approval_model_id.name,
                                 ", ".join(role.approval_conflict_rule_ids.mapped('display_name'))))
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

    def action_sgi_adopt_rule(self):
        """Adopta la regla manual que ya existe en el botón: la liga al rol y
        la deja con las personas del puesto y la condición del rol."""
        self.ensure_one()
        rule = self.approval_conflict_rule_ids[:1].sudo()
        if not rule:
            raise UserError("No hay otra regla en este botón.")
        rule.sgi_role_id = self
        self.approval_rule_id = rule
        self.invalidate_recordset(['approval_conflict_rule_ids'])
        self._sgi_sync_approval_rule()
        return True

    def action_sgi_open_approval_entries(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Aprobaciones — %s" % self.activity_id.display_name,
                'res_model': 'studio.approval.entry', 'view_mode': 'list,form',
                'domain': [('rule_id', '=', self.approval_rule_id.id)], 'context': {'create': False}}


class StudioApprovalEntrySgi(models.Model):
    """1.0.6 (Dirección General 2026-10-09): nadie aprueba lo que él mismo
    pidió. Studio registra cada aprobación como una entrada; si la regla es
    de un rol «Aprueba» del SGI y quien aprueba es quien pidió el registro
    (campos explícitos de ``SGI_REQUESTER_FIELDS``: «Solicitó», «Elaboró»,
    quien mandó a aprobar…), la entrada no se crea y el mensaje dice a quién
    le toca (el titular o el suplente que no lo pidió)."""
    _inherit = 'studio.approval.entry'

    @api.model_create_multi
    def create(self, vals_list):
        Rule = self.env['studio.approval.rule'].sudo()
        Users = self.env['res.users'].sudo()
        for vals in vals_list:
            if not vals.get('approved') or not vals.get('rule_id') or not vals.get('res_id'):
                continue
            rule = Rule.browse(vals['rule_id'])
            role = rule.sgi_role_id
            if not role or not rule.model_name or rule.model_name not in self.env:
                continue
            record = self.env[rule.model_name].sudo().browse(vals['res_id']).exists()
            user = Users.browse(vals.get('user_id') or self.env.uid)
            conflict = record and role._sgi_requester_conflict(record, user)
            if conflict:
                raise UserError(conflict)
        return super().create(vals_list)
