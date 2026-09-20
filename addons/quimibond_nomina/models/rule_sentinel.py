# -*- coding: utf-8 -*-
"""Centinela de reglas salariales.

``SUBSIDY`` e ``INT_DAY_WAGE`` de la estructura «Paga regular» no son copias
propias: son registros del módulo de Odoo con el código de Quimibond escrito
encima. Ya pasó una vez (16-sep-2026) que quince reglas volvieron solas al
código de fábrica a media corrida. El daño no es que truene: es que no truena,
la nómina sigue calculando con la lógica de Odoo y nadie se entera.

El centinela guarda un SHA-256 de la condición y la fórmula, revisa a diario
por cron y avisa. **No revierte solo, a propósito**: esas reglas se editan
deliberadamente y revertir por detrás sería peor que el problema. Cuando el
cambio es querido, se acepta la huella nueva desde el formulario."""
import hashlib
import json
import logging

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)

# Reglas con las que se siembra el centinela al instalar, y la estructura
# donde viven (id 4, «Paga regular»).
SEED_CODES = ('SUBSIDY', 'INT_DAY_WAGE')
SEED_STRUCT_CODE = 'MX_REGULAR'

# Lo que entra a la huella: todo lo que decide si la regla aplica y cuánto vale.
FINGERPRINT_FIELDS = (
    'code', 'active',
    'condition_select', 'condition_python',
    'amount_select', 'amount_python_compute', 'amount_fix', 'amount_percentage', 'quantity',
)

PARAM_EXTRA_EMAILS = 'quimibond_nomina.sentinel_email'


class QbNominaRuleSentinel(models.Model):
    _name = 'qb.nomina.rule.sentinel'
    _description = 'Centinela de reglas salariales'
    _inherit = ['mail.thread']
    _order = 'changed desc, code, id'

    rule_id = fields.Many2one(
        'hr.salary.rule', string='Regla', required=True, ondelete='cascade', index=True,
        context={'active_test': False})
    code = fields.Char(related='rule_id.code', store=True)
    rule_name = fields.Char(related='rule_id.name', string='Nombre de la regla')
    struct_id = fields.Many2one(related='rule_id.struct_id', store=True, string='Estructura')
    rule_active = fields.Boolean(related='rule_id.active', string='Regla activa')
    fingerprint_accepted = fields.Char(string='Huella aceptada', readonly=True, copy=False)
    fingerprint_current = fields.Char(string='Huella actual', compute='_compute_fingerprint_current')
    changed = fields.Boolean(string='Cambió', readonly=True, copy=False, tracking=True,
                             help="La condición o la fórmula ya no coinciden con la huella aceptada.")
    accepted_at = fields.Datetime(string='Aceptada el', readonly=True, copy=False)
    accepted_uid = fields.Many2one('res.users', string='Aceptada por', readonly=True, copy=False)
    checked_at = fields.Datetime(string='Última revisión', readonly=True, copy=False)
    changed_at = fields.Datetime(string='Cambio detectado el', readonly=True, copy=False)
    note = fields.Text(string='Notas')

    _rule_uniq = models.Constraint(
        'unique(rule_id)',
        "Ya hay un centinela para esa regla.",
    )

    # ------------------------------------------------------------------
    # Huella
    # ------------------------------------------------------------------
    @api.model
    def _fingerprint(self, rule):
        rule = rule.sudo().with_context(active_test=False)
        data = {}
        for name in FINGERPRINT_FIELDS:
            value = rule[name]
            if isinstance(value, str):
                # El editor web cambia \r\n y espacios al final; eso no es un cambio.
                value = value.replace('\r\n', '\n').strip()
            data[name] = value
        raw = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    @api.depends('rule_id', 'rule_id.condition_select', 'rule_id.condition_python',
                 'rule_id.amount_select', 'rule_id.amount_python_compute', 'rule_id.amount_fix',
                 'rule_id.amount_percentage', 'rule_id.quantity', 'rule_id.active', 'rule_id.code')
    def _compute_fingerprint_current(self):
        for rec in self:
            rec.fingerprint_current = self._fingerprint(rec.rule_id) if rec.rule_id else False

    @api.depends('code', 'rule_id.name')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = '%s · %s' % (rec.code or '?', rec.rule_id.name or '')

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        recs = super().create(vals_list)
        # La primera línea base es lo que esté vivo al crear el centinela.
        now = fields.Datetime.now()
        for rec in recs.filtered(lambda r: not r.fingerprint_accepted):
            rec.write({
                'fingerprint_accepted': self._fingerprint(rec.rule_id),
                'accepted_at': now,
                'accepted_uid': self.env.uid,
                'checked_at': now,
            })
        return recs

    def action_accept(self):
        """La condición y la fórmula de hoy pasan a ser la línea base."""
        now = fields.Datetime.now()
        for rec in self:
            rec.write({
                'fingerprint_accepted': self._fingerprint(rec.rule_id),
                'changed': False,
                'changed_at': False,
                'accepted_at': now,
                'accepted_uid': self.env.uid,
                'checked_at': now,
            })
            rec.message_post(body=_('Huella aceptada: la condición y la fórmula actuales son la línea base.'))
        return True

    def action_check_now(self):
        newly = self._check()
        if newly:
            self._notify(newly)
        return True

    def _check(self):
        """Compara cada regla con su huella aceptada y marca. Devuelve los
        centinelas que ACABAN de cambiar (los que ya estaban marcados no se
        vuelven a avisar)."""
        now = fields.Datetime.now()
        newly = self.browse()
        back = self.browse()
        for rec in self:
            current = self._fingerprint(rec.rule_id)
            vals = {'checked_at': now}
            if current != rec.fingerprint_accepted:
                if not rec.changed:
                    vals.update({'changed': True, 'changed_at': now})
                    newly |= rec
            elif rec.changed:
                vals.update({'changed': False, 'changed_at': False})
                back |= rec
            rec.write(vals)
        for rec in newly:
            rec.message_post(body=_(
                'La condición o la fórmula de la regla %s ya no coinciden con la huella aceptada. '
                'No se revirtió nada: revisa la regla y, si el cambio es deliberado, acepta la huella.'
            ) % rec.code)
            _logger.warning('quimibond_nomina: la regla %s (id %s) cambió respecto a la huella aceptada',
                            rec.code, rec.rule_id.id)
        for rec in back:
            rec.message_post(body=_('La regla %s volvió a coincidir con la huella aceptada.') % rec.code)
        return newly

    @api.model
    def _cron_check(self):
        sentinels = self.search([])
        newly = sentinels._check()
        if newly:
            self._notify(newly)
        return True

    # ------------------------------------------------------------------
    # Aviso
    # ------------------------------------------------------------------
    @api.model
    def _recipients(self):
        group = self.env.ref('hr_payroll.group_hr_payroll_manager', raise_if_not_found=False)
        users = self.env['res.users']
        if group:
            # Odoo 19 trae all_user_ids (con los grupos implicados); antes sólo user_ids.
            users = group.all_user_ids if 'all_user_ids' in group._fields else group.user_ids
        emails = [u.email for u in users.sudo() if u.active and u.email and not u.share]
        extra = self.env['ir.config_parameter'].sudo().get_param(PARAM_EXTRA_EMAILS) or ''
        emails += [e.strip() for e in extra.replace(';', ',').split(',') if e.strip()]
        return sorted(set(emails))

    @api.model
    def _notify(self, sentinels):
        recipients = self._recipients()
        if not recipients:
            _logger.warning('quimibond_nomina: %d regla(s) cambiaron y no hay a quién avisar '
                            '(sin responsables de nómina con correo ni %s)', len(sentinels), PARAM_EXTRA_EMAILS)
            return False
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url') or ''
        rows = ''.join(
            '<tr><td><a href="%s/odoo/action-quimibond_nomina.action_qb_nomina_rule_sentinel/%s">%s</a></td>'
            '<td>%s</td><td>%s</td><td>%s</td></tr>' % (
                base_url, rec.id, rec.code, rec.rule_id.name, rec.struct_id.name or '',
                fields.Datetime.context_timestamp(rec, rec.changed_at or fields.Datetime.now()).strftime('%Y-%m-%d %H:%M'))
            for rec in sentinels)
        body = (
            '<p>El centinela de reglas de nómina detectó que la condición o la fórmula de estas reglas '
            'ya no coinciden con la huella aceptada:</p>'
            '<table border="1" cellpadding="4" cellspacing="0">'
            '<tr><th>Código</th><th>Regla</th><th>Estructura</th><th>Detectado</th></tr>%s</table>'
            '<p><b>No se revirtió nada.</b> Ya pasó una vez (16-sep-2026) que reglas de Odoo con código '
            'propio volvieron solas a la fórmula de fábrica y la nómina siguió calculando sin avisar. '
            'Revisa la regla contra la última nómina cuadrada; si el cambio es deliberado, acepta la '
            'huella en Nómina → Configuración → Centinela de reglas.</p>'
        ) % rows
        subject = _('Nómina: %d regla(s) salarial(es) cambiaron (%s)') % (
            len(sentinels), ', '.join(sentinels.mapped('code')))
        mail = self.env['mail.mail'].sudo().create({
            'subject': subject,
            'email_to': ', '.join(recipients),
            'body_html': body,
            'auto_delete': False,
        })
        mail.send()
        return mail

    # ------------------------------------------------------------------
    # Menú
    # ------------------------------------------------------------------
    MENU_XMLID = 'quimibond_nomina.menu_qb_nomina_rule_sentinel'
    PAYROLL_APP_ICON = 'hr_payroll,static/description/icon.png'
    CONFIG_MENU_NAMES = ('Configuración', 'Configuration', 'Ajustes', 'Settings')

    @api.model
    def qb_nomina_colgar_menu(self):
        """Cuelga el menú del centinela de Nómina → Configuración y lo activa.
        Se llama desde views/rule_sentinel_views.xml en cada instalación y
        actualización (``<function>``).

        Por qué no va en el XML: el xmlid del menú de configuración de Nómina
        cambió en Odoo 19 (``hr_payroll.menu_hr_payroll_configuration`` ya no
        existe) y un ``parent`` que no resuelve tumba la instalación del módulo
        entero. Aquí se busca la app de Nómina por su icono (no depende del
        idioma ni del xmlid) y dentro el menú Configuración; si no está, el
        centinela cuelga de la raíz de Nómina; si ni eso, queda inactivo y se
        avisa (la acción sigue existiendo)."""
        menu = self.env.ref(self.MENU_XMLID, raise_if_not_found=False)
        if not menu:
            return False
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        root = Menu.search([('parent_id', '=', False), ('web_icon', '=', self.PAYROLL_APP_ICON)], limit=1)
        if not root:
            _logger.warning('quimibond_nomina: no hay app de Nómina (icono %s); el menú del centinela '
                            'queda inactivo', self.PAYROLL_APP_ICON)
            menu.sudo().write({'active': False})
            return False
        parent = Menu.search([('parent_id', '=', root.id), ('action', '=', False),
                              ('name', 'in', list(self.CONFIG_MENU_NAMES))], limit=1)
        if not parent:
            # Segundo intento en todos los idiomas instalados.
            for lang in self.env['res.lang'].get_installed():
                parent = Menu.with_context(lang=lang[0]).search(
                    [('parent_id', '=', root.id), ('action', '=', False),
                     ('name', 'in', list(self.CONFIG_MENU_NAMES))], limit=1)
                if parent:
                    break
        if not parent:
            _logger.warning('quimibond_nomina: la app de Nómina no tiene menú Configuración; '
                            'el centinela cuelga de la raíz')
            parent = root
        menu.sudo().write({'parent_id': parent.id, 'active': True})
        return True

    # ------------------------------------------------------------------
    # Siembra
    # ------------------------------------------------------------------
    @api.model
    def _seed(self, codes=SEED_CODES, struct_code=SEED_STRUCT_CODE):
        """Crea (si falta) un centinela por cada regla activa con esos códigos
        en la estructura indicada. Idempotente."""
        Rule = self.env['hr.salary.rule'].sudo()
        rules = Rule.search([('code', 'in', list(codes)), ('struct_id.code', '=', struct_code)])
        if not rules:
            _logger.warning('quimibond_nomina: no hay reglas %s en la estructura %s; el centinela queda vacío',
                            ', '.join(codes), struct_code)
            return self.browse()
        watched = self.with_context(active_test=False).search([('rule_id', 'in', rules.ids)]).mapped('rule_id')
        return self.create([{'rule_id': rule.id} for rule in rules - watched])
