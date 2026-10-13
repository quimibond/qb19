# -*- coding: utf-8 -*-
"""57.150.0 (auditoría UX 2026-10-13, fase 2): correo diario por persona con
lo que hoy pide su «Mis pendientes» (atrasado y por vencer), para que la
bandeja sea el único lugar a revisar. Va junto con el ajuste «No repetir en
la campana…» (``sgi.cron._sgi_skip_covered``), que deja de crear como
actividad lo que ya es renglón propio de la bandeja.

- Misma fuente que la bandeja (``sgi.my.pending._sgi_pending_values``) y
  mismo patrón que el correo semanal de atrasados (D-14).
- Cada usuario lo apaga en sus Preferencias
  (``res.users.sgi_daily_pending_mail``, encendida por default).
- El cron ``sgi_cron_daily_pending_mail`` sale APAGADO: Jose lo enciende a
  mano (mandar correo a toda la planta es una decisión, no un despliegue).
"""
import logging

from odoo import api, fields, models

from .sgi_guard import sgi_require_system
from .sgi_weekly_overdue import MAIL_ROW_LIMIT

_logger = logging.getLogger(__name__)

DAILY_STATES = ('atrasada', 'por_vencer')


class ResUsersDailyPending(models.Model):
    _inherit = 'res.users'

    sgi_daily_pending_mail = fields.Boolean(
        string="Correo diario de mis pendientes (SGI)", default=True,
        help="Cada mañana, un correo con lo atrasado y lo que vence pronto en «Mis pendientes» del "
             "SGI. Solo llega si hay algo. Apáguelo si no lo quiere.")

    @property
    def SELF_READABLE_FIELDS(self):
        return super().SELF_READABLE_FIELDS + ['sgi_daily_pending_mail']

    @property
    def SELF_WRITEABLE_FIELDS(self):
        return super().SELF_WRITEABLE_FIELDS + ['sgi_daily_pending_mail']


class SgiCronDailyPending(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_daily_pending_users(self):
        company = self.env['sgi.config']._sgi_company()
        return self.env['res.users'].sudo().search([
            ('share', '=', False), ('sgi_daily_pending_mail', '=', True),
            ('email', '!=', False), ('company_ids', 'in', company.ids)])

    @api.model
    def _sgi_daily_pending_rows(self, rows):
        """Renglones atrasados y por vencer listos para la plantilla, en el
        orden de la bandeja (lo atrasado primero)."""
        Pending = self.env['sgi.my.pending']
        kinds = dict(Pending._fields['kind'].selection)
        states = dict(Pending._fields['state'].selection)
        processes = self.env['sgi.process'].sudo().with_context(active_test=False)
        out = []
        for row in rows:
            if row.get('state') not in DAILY_STATES:
                continue
            process = processes.browse(row['process_id']) if row.get('process_id') else processes
            out.append({
                'state': states.get(row['state'], row['state']),
                'late': row['state'] == 'atrasada',
                'kind': kinds.get(row.get('kind'), row.get('kind') or ''),
                'name': row.get('name') or '',
                'date_due': row['date_due'].strftime('%d/%m/%Y') if row.get('date_due') else '',
                'process': (process.code or '') if process else '',
            })
        return out

    @api.model
    def cron_daily_pending_mail(self):
        """Cron diario: a cada persona con algo atrasado o por vencer en Mis
        pendientes, un correo con esa lista. Un envío que falla no detiene a
        los demás. Devuelve los ids de usuario a los que se mandó."""
        sgi_require_system(self.env)
        template = self.env.ref('quimibond_sgi.mail_template_sgi_daily_pending', raise_if_not_found=False)
        users = self._sgi_daily_pending_users()
        if not template or not users:
            return []
        values = self.env['sgi.my.pending']._sgi_pending_values(users)
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url') or ''
        link = "%s/odoo/action-quimibond_sgi.sgi_my_pending_action_mine" % base_url.rstrip('/')
        sent = []
        for user in users:
            rows = self._sgi_daily_pending_rows(values.get(user.id, []))
            if not rows:
                continue

            def _send(user=user, rows=rows):
                template.sudo().with_context(
                    sgi_rows=rows[:MAIL_ROW_LIMIT],
                    sgi_more=max(len(rows) - MAIL_ROW_LIMIT, 0),
                    sgi_total=len(rows),
                    sgi_late=sum(1 for r in rows if r['late']),
                    sgi_link=link,
                ).send_mail(user.id, email_layout_xmlid='mail.mail_notification_light')
            if self._sgi_step("correo diario de pendientes a %s" % user.login, _send):
                sent.append(user.id)
        _logger.info("SGI: correo diario de Mis pendientes a %d persona(s).", len(sent))
        return sent
