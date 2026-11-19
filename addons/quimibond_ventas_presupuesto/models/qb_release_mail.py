# -*- coding: utf-8 -*-
"""Release por correo con aprobación de Ventas (19.0.3.30.0, Jose 19-nov-2026).

«Haz el 1 y 2 pero necesito que Jessica apruebe cada release nuevo en algún lugar, no que se integre
automático, para comprobar que se haga correctamente y que ella tenga el control».

- **Buzón** ``releases@`` (alias de Odoo sobre ``qb.release``): lo que llega (del cliente o reenviado por
  Ventas) se reconoce por perfil (dominios del remitente, asunto y nombre del archivo, ``release/correo.py``),
  se crea el release con el archivo y se **lee**. Nunca se aplica solo.
- **Aprobación**: el release leído deja una actividad «Aprobar release» al Responsable en Ventas del perfil.
  «Marcar revisado» y «Aplicar al pronóstico» solo los hace ese responsable (o un administrador).
- Lo que no se reconoce (varios perfiles posibles o ninguno) queda «Por revisar» sin perfil para que Ventas
  lo elija; un correo sin archivo de release queda descartado. Un archivo que ya llegó no se duplica.
- **Pedidos**: al aplicar, si hay semanas sin pedido dentro de «Semanas a pedido» del perfil, se deja la
  actividad «Crear pedidos» con el botón «Semanas por pedir» del release (abre Pronóstico vigente filtrado,
  donde está «Crear pedido»).
"""
import base64
import hashlib
import logging
from collections import Counter
from datetime import timedelta
from email.message import Message

from odoo import api, fields, models
from odoo.exceptions import UserError
from odoo.tools import html2plaintext

from ..release import correo

_logger = logging.getLogger(__name__)

APPROVE = "Aprobar release"
ORDERS = "Crear pedidos"


class QbReleaseProfileMail(models.Model):
    _inherit = 'qb.release.profile'

    email_subject = fields.Char(
        string="Asunto del correo contiene", tracking=True,
        help="Separados por coma; basta uno (sin distinguir mayúsculas). Vacío = cualquier asunto. "
             "Sirve para separar dos plantas del mismo cliente (Saltillo: «saltillo lamination release»; "
             "León: «leon lamination»).")
    email_file = fields.Char(
        string="Archivo del release", tracking=True,
        help="Nombre del archivo con comodines, separados por coma (FXI: «SUM Quimibond*.xlsx»; Saltillo: "
             "«* D.pdf»). Vacío = cualquier PDF, Excel, CSV, TXT o EML. Los demás adjuntos (firmas, PPAP, "
             "órdenes de compra) no crean release.")

    def _qb_mail_rules(self):
        return [{'id': p.id, 'domains': correo.lista(p.sender_domains),
                 'subject': correo.lista(p.email_subject), 'files': correo.lista(p.email_file)} for p in self]


class QbReleaseMail(models.Model):
    _inherit = 'qb.release'

    # 3.30.0: un release que llegó por correo y no se reconoció queda sin perfil hasta que Ventas lo elige.
    profile_id = fields.Many2one(required=False)
    by_email = fields.Boolean(string="Llegó por correo", readonly=True, copy=False)
    email_from = fields.Char(string="Remitente", readonly=True, copy=False)
    approver_id = fields.Many2one('res.users', string="Aprueba", related='profile_id.sales_user_id')
    to_order_count = fields.Integer(string="Semanas por pedir", compute='_compute_to_order_count')

    # --- Correo ----------------------------------------------------------------------
    @api.model
    def _qb_mail_files(self, msg_dict):
        """(nombre, bytes) de los adjuntos del correo."""
        files = []
        for att in msg_dict.get('attachments') or []:
            name, content = att[0], att[1]
            if isinstance(content, Message):
                content = content.as_bytes()
            elif isinstance(content, str):
                content = content.encode('utf-8')
            if content:
                files.append((name or 'adjunto', content))
        return files

    @api.model
    def _qb_mail_duplicate(self, profile, content):
        """Un release vigente del mismo perfil con el mismo archivo."""
        checksum = hashlib.sha1(content).hexdigest()
        atts = self.env['ir.attachment'].sudo().search([
            ('res_model', '=', 'qb.release'), ('res_field', '=', 'file'), ('checksum', '=', checksum)])
        return self.search([('id', 'in', atts.mapped('res_id')), ('profile_id', '=', profile.id),
                            ('state', 'not in', ('descartado',))], limit=1)

    @api.model
    def _qb_default_approver(self):
        """El Responsable en Ventas más común de los perfiles (para lo que llega sin perfil)."""
        profiles = self.env['qb.release.profile'].search([])
        counts = Counter(p.sales_user_id.id for p in profiles if p.sales_user_id)
        return self.env['res.users'].browse(counts.most_common(1)[0][0] if counts else [])

    @api.model
    def message_new(self, msg_dict, custom_values=None):
        """Un release por archivo reconocido; se lee y espera la aprobación de Ventas."""
        Profile = self.env['qb.release.profile']
        profiles = Profile.search([])
        rules = profiles._qb_mail_rules()
        email_from = msg_dict.get('email_from') or ''
        subject = msg_dict.get('subject') or ''
        body = html2plaintext(msg_dict.get('body') or '')
        files = self._qb_mail_files(msg_dict)
        base = dict(custom_values or {}, by_email=True, email_from=email_from)
        releases = self.browse()
        loose = []
        for name, content in files:
            pid, tied = correo.elegir(rules, email_from, subject, body, name)
            if not pid:
                if tied or correo.es_archivo_release(name):
                    loose.append((name, content, Profile.browse(tied)))
                continue
            profile = Profile.browse(pid)
            dup = self._qb_mail_duplicate(profile, content)
            if dup:
                dup.message_post(body="El mismo archivo llegó otra vez por correo (%s, «%s»); no se creó otro "
                                      "release." % (email_from, subject))
                releases |= dup
                continue
            release = self.create(dict(base, profile_id=pid, filename=name,
                                       file=base64.b64encode(content)))
            release._qb_mail_read()
            releases |= release
        if not releases:
            for name, content, tied in loose[:1]:
                why = ("le pueden tocar varios perfiles (%s)" % ", ".join(tied.mapped('name'))) if tied else \
                    "no se reconoció el cliente (revise los dominios, el asunto y el archivo de sus perfiles)"
                releases |= self.create(dict(
                    base, filename=name, file=base64.b64encode(content), state='por_revisar',
                    error_reason="Llegó por correo y %s: elija el perfil y dé «Leer»." % why))
        if not releases:
            names = ", ".join(n for n, _c in files) or "sin adjuntos"
            return self.create(dict(base, state='descartado',
                                    error_reason="El correo «%s» no trae archivo de release (%s)." % (subject, names)))
        for release in releases:
            release._qb_ask_approval()
            if release != releases[0]:
                release.message_post(body="Llegó por correo de %s: «%s»." % (email_from, subject))
        return releases[0]

    def _qb_mail_read(self):
        """Lee sin detener el correo: cualquier error deja el release «por revisar» con el motivo."""
        for release in self:
            try:
                with self.env.cr.savepoint():
                    release.action_read()
            except Exception as exc:  # noqa: BLE001 — el correo nunca se rebota por un archivo ilegible
                _logger.info("Release %s por correo no se pudo leer: %s", release.id, exc)
                release.write({'state': 'por_revisar', 'error_reason': "No se pudo leer: %s" % exc})

    def _qb_ask_approval(self):
        todo = self.env.ref('mail.mail_activity_data_todo', raise_if_not_found=False)
        for release in self:
            user = release.profile_id.sales_user_id or self._qb_default_approver()
            if not user or release.state not in ('recibido', 'por_revisar', 'leido'):
                continue
            if release.state == 'leido':
                note = ("Se leyó solo. Revise las partes y las semanas; si está bien, «Marcar revisado» y "
                        "«Aplicar al pronóstico». No se aplica sin su aprobación.")
            else:
                note = "No se pudo leer solo: %s" % (release.error_reason or "elija el perfil y dé «Leer».")
            release.activity_schedule(
                activity_type_id=todo.id if todo else False, user_id=user.id,
                summary="%s %s" % (APPROVE, release.name), note=note)

    # --- Aprobación --------------------------------------------------------------------
    def _qb_check_approver(self):
        user = self.env.user
        if self.env.su or user.has_group('base.group_system'):
            return
        for release in self:
            approver = release.profile_id.sales_user_id
            if approver and approver != user:
                raise UserError("El release %s lo aprueba %s (Responsable en Ventas del perfil)." % (
                    release.name, approver.name))

    def action_mark_reviewed(self):
        self._qb_check_approver()
        return super().action_mark_reviewed()

    def action_apply(self):
        self._qb_check_approver()
        res = super().action_apply()
        self._qb_after_apply()
        return res

    def action_discard(self):
        res = super().action_discard()
        self.activity_ids.filtered(lambda a: (a.summary or '').startswith(APPROVE)).action_feedback(
            feedback="Descartado")
        return res

    # --- Semanas por pedir ----------------------------------------------------------------
    def _qb_to_order_domain(self):
        self.ensure_one()
        profile = self.profile_id
        if not profile:
            return [('id', '=', 0)]
        today = fields.Date.context_today(self)
        current = today - timedelta(days=today.weekday())
        horizon = current + timedelta(weeks=max(profile.so_weeks, 1))
        domain = [('company_id', '=', profile.company_id.id),
                  ('partner_id', '=', profile.partner_id.commercial_partner_id.id),
                  ('week', '>=', current), ('week', '<', horizon), ('qty_neta', '>', 0)]
        if profile.ship_to_id:
            domain.append(('ship_to_id', '=', profile.ship_to_id.id))
        elif profile.plant:
            domain.append(('plant', '=', profile.plant))
        return domain

    def _compute_to_order_count(self):
        Line = self.env['qb.forecast.line']
        for release in self:
            release.to_order_count = Line.search_count(release._qb_to_order_domain()) \
                if release.state == 'aplicado' else 0

    def action_qb_weeks_to_order(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id(
            'quimibond_ventas_presupuesto.qb_forecast_line_action')
        action.update(name="Por pedir: %s" % self.profile_id.name, domain=self._qb_to_order_domain(),
                      context={'search_default_group_week': 1})
        return action

    def _qb_after_apply(self):
        todo = self.env.ref('mail.mail_activity_data_todo', raise_if_not_found=False)
        for release in self:
            release.activity_ids.filtered(lambda a: (a.summary or '').startswith(APPROVE)).action_feedback(
                feedback="Aplicado por %s" % self.env.user.name)
            user = release.profile_id.sales_user_id
            count = self.env['qb.forecast.line'].search_count(release._qb_to_order_domain())
            if not user or not count:
                continue
            release.activity_schedule(
                activity_type_id=todo.id if todo else False, user_id=user.id,
                summary="%s %s" % (ORDERS, release.profile_id.name),
                note="%d semana(s) de las próximas %d sin pedido. Use «Semanas por pedir» en este release y "
                     "«Crear pedido» en el Pronóstico vigente." % (count, max(release.profile_id.so_weeks, 1)))
