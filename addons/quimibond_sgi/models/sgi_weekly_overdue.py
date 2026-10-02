# -*- coding: utf-8 -*-
"""D-14 (57.7.0): correo semanal por persona con lo ATRASADO de su «Mis
pendientes», en lugar del resumen semanal viejo a MAST y Dirección (B-017,
apagado desde el 24-ago-2026).

- Cada usuario lo puede apagar en sus Preferencias (casilla encendida por
  default: ``res.users.sgi_weekly_overdue_mail``).
- Solo se manda a quien tiene al menos un renglón «atrasada» (misma fuente
  que Mis pendientes: ``sgi.my.pending._sgi_pending_values``).
- Plantilla del módulo ``mail_template_sgi_weekly_overdue``.
- El cron ``sgi_cron_weekly_digest`` (mismo registro que el resumen viejo)
  sale apagado; Jose lo enciende a mano.
"""
import logging

from odoo import api, fields, models

from .sgi_guard import sgi_require_system

_logger = logging.getLogger(__name__)

# Renglones que se listan en el correo; el resto se resume en «y N más».
MAIL_ROW_LIMIT = 40


class ResUsersWeeklyOverdue(models.Model):
    _inherit = 'res.users'

    sgi_weekly_overdue_mail = fields.Boolean(
        string="Correo semanal de mis pendientes atrasados (SGI)", default=True,
        help="Cada semana, un correo con lo atrasado de «Mis pendientes» del SGI. "
             "Solo llega si hay algo atrasado. Apáguelo si no lo quiere.")

    @property
    def SELF_READABLE_FIELDS(self):
        return super().SELF_READABLE_FIELDS + ['sgi_weekly_overdue_mail']

    @property
    def SELF_WRITEABLE_FIELDS(self):
        return super().SELF_WRITEABLE_FIELDS + ['sgi_weekly_overdue_mail']


class SgiCronWeeklyOverdue(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_weekly_overdue_users(self):
        """Usuarios internos activos de la empresa del SGI que no apagaron el
        correo y tienen correo electrónico."""
        company = self.env['sgi.config']._sgi_company()
        return self.env['res.users'].sudo().search([
            ('share', '=', False), ('sgi_weekly_overdue_mail', '=', True),
            ('email', '!=', False), ('company_ids', 'in', company.ids)])

    @api.model
    def _sgi_weekly_overdue_rows(self, rows):
        """Renglones atrasados listos para la plantilla (texto plano)."""
        Pending = self.env['sgi.my.pending']
        kinds = dict(Pending._fields['kind'].selection)
        processes = self.env['sgi.process'].sudo().with_context(active_test=False)
        out = []
        for row in rows:
            if row.get('state') != 'atrasada':
                continue
            process = processes.browse(row['process_id']) if row.get('process_id') else processes
            out.append({
                'kind': kinds.get(row.get('kind'), row.get('kind') or ''),
                'name': row.get('name') or '',
                'date_due': row['date_due'].strftime('%d/%m/%Y') if row.get('date_due') else '',
                'process': (process.code or '') if process else '',
            })
        return out

    @api.model
    def cron_weekly_overdue_mail(self):
        """Cron semanal (D-14): a cada persona con algo atrasado en Mis
        pendientes, un correo con esa lista. Cada envío va en su savepoint:
        un correo que falla no detiene a los demás. Devuelve la lista de
        usuarios a los que se mandó."""
        sgi_require_system(self.env)  # F-008
        template = self.env.ref('quimibond_sgi.mail_template_sgi_weekly_overdue',
                                raise_if_not_found=False)
        users = self._sgi_weekly_overdue_users()
        if not template or not users:
            return []
        values = self.env['sgi.my.pending']._sgi_pending_values(users)
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url') or ''
        link = "%s/odoo/action-quimibond_sgi.sgi_my_pending_action_mine" % base_url.rstrip('/')
        sent = []
        for user in users:
            late = self._sgi_weekly_overdue_rows(values.get(user.id, []))
            if not late:
                continue

            def _send(user=user, late=late):
                template.sudo().with_context(
                    sgi_rows=late[:MAIL_ROW_LIMIT],
                    sgi_more=max(len(late) - MAIL_ROW_LIMIT, 0),
                    sgi_total=len(late), sgi_link=link,
                ).send_mail(user.id, email_layout_xmlid='mail.mail_notification_light')
            if self._sgi_step("correo semanal de atrasados a %s" % user.login, _send):
                sent.append(user.id)
        _logger.info("SGI D-14: correo semanal de pendientes atrasados a %d persona(s).", len(sent))
        return sent
