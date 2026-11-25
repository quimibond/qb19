# -*- coding: utf-8 -*-
"""Avisos de complementos de pago (1.32.0).

- **Proveedores** (día ``quimibond_sat.rep_dia_proveedores``, 10 por omisión):
  una actividad por proveedor con sus facturas pagadas en Odoo sin
  complemento en el SAT (hallazgo ``sin_complemento`` de ``sat.pago.compare``)
  y, en el contacto, la acción «Pedir complementos de pago» abre el correo ya
  escrito con la plantilla ``mail_template_pedir_rep``. Sin el complemento,
  el IVA de ese pago no se acredita.
- **Clientes** (día ``quimibond_sat.rep_dia_clientes``, 1 por omisión): correo
  a ``quimibond_sat.alert_email`` con los cobros del mes anterior (y los
  vencidos) que no tienen complemento ni en el SAT ni timbrado en Odoo, antes
  de que venza el plazo del día 5.

La actividad es para ``quimibond_sat.rep_responsable`` (logins, coma); si no
está, para los usuarios cuyo correo está en ``quimibond_sat.alert_email``.
"""
import logging
from datetime import date

from markupsafe import Markup, escape

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

RESUMEN_PROVEEDOR = 'Pedir complementos de pago'


def _money(value):
    return '{:,.2f}'.format(value or 0.0)


class ResPartner(models.Model):
    _inherit = 'res.partner'

    def _sat_rep_faltantes(self):
        """Facturas de este proveedor pagadas en Odoo sin complemento en el SAT."""
        self.ensure_one()
        return self.env['sat.pago.compare'].sudo().search([
            ('partner_id', 'child_of', self.commercial_partner_id.id),
            ('direction', '=', 'received'),
            ('issue', '=', 'sin_complemento'),
        ], order='fecha, id')

    def _sat_rep_tabla(self, rows=None):
        """Tabla HTML de las facturas sin complemento (para la actividad y el correo)."""
        rows = self._sat_rep_faltantes() if rows is None else rows
        cuerpo = Markup('').join(
            Markup('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td>'
                   '<td style="text-align:right">%s</td><td style="text-align:right">%s</td></tr>') % (
                ' '.join(filter(None, (r.cfdi_id.serie, r.cfdi_id.folio))) or '—',
                r.cfdi_id.uuid or '', r.fecha or '', r.fecha_pago_odoo or '',
                '%s %s' % (_money(r.total), r.moneda or ''),
                '%s %s' % (_money(r.pagado_odoo - r.pagado_sat), r.moneda or ''))
            for r in rows)
        return Markup(
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Serie y folio</th>'
            '<th>Folio fiscal (UUID)</th><th>Fecha</th><th>Pagada el</th><th>Total</th>'
            '<th>Pago sin complemento</th></tr>%s</table>') % cuerpo

    def _sat_action_pedir_rep(self):
        """Abre el correo al proveedor con la lista de complementos que faltan."""
        self.ensure_one()
        partner = self.commercial_partner_id
        if not partner._sat_rep_faltantes():
            raise UserError(_('%s no tiene facturas pagadas sin complemento de pago.') % partner.display_name)
        template = self.env.ref('quimibond_sat.mail_template_pedir_rep')
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mail.compose.message',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_model': 'res.partner',
                'default_res_ids': partner.ids,
                'default_template_id': template.id,
                'default_composition_mode': 'comment',
                'default_email_layout_xmlid': 'mail.mail_notification_light',
            },
        }


class SatPagoCompare(models.Model):
    _inherit = 'sat.pago.compare'

    # ------------------------------------------------------------ comunes
    @api.model
    def _rep_dia(self, key, default):
        raw = self.env['ir.config_parameter'].sudo().get_param('quimibond_sat.%s' % key)
        try:
            return int(raw) if raw else default
        except ValueError:
            return default

    @api.model
    def _rep_responsables(self):
        ICP = self.env['ir.config_parameter'].sudo()
        Users = self.env['res.users'].sudo()
        logins = [x.strip() for x in (ICP.get_param('quimibond_sat.rep_responsable') or '').split(',') if x.strip()]
        if logins:
            users = Users.search([('login', 'in', logins)])
        else:
            correos = [c.lower() for c in self.env['sat.cfdi']._alert_recipients()]
            users = Users.search([('share', '=', False)]).filtered(
                lambda u: (u.email or '').lower() in correos or (u.login or '').lower() in correos) if correos else Users
        return users.filtered(lambda u: not u.share)

    @api.model
    def _cron_avisos_rep(self, today=None):
        """Diario: corre cada aviso solo en su día del mes."""
        today = today or fields.Date.context_today(self)
        hecho = {}
        if today.day == self._rep_dia('rep_dia_proveedores', 10):
            hecho['proveedores'] = self._aviso_rep_proveedores()
        if today.day == self._rep_dia('rep_dia_clientes', 1):
            hecho['clientes'] = self._aviso_rep_clientes(today=today)
        return hecho

    # -------------------------------------------------------- proveedores
    @api.model
    def _aviso_rep_proveedores(self):
        """Una actividad por proveedor con sus facturas sin complemento. Si ya
        tiene una abierta, la actualiza en lugar de duplicarla. Devuelve las
        actividades creadas o actualizadas."""
        users = self._rep_responsables()
        if not users:
            _logger.info('sat.pago.compare: sin quimibond_sat.rep_responsable ni usuarios en alert_email; '
                         'no se crean actividades de complementos de proveedores')
            return self.env['mail.activity']
        companies = self.env['res.company'].sudo().search([('sat_sync_enabled', '=', True)])
        rows = self.sudo().search([('company_id', 'in', companies.ids), ('direction', '=', 'received'),
                                   ('issue', '=', 'sin_complemento'), ('partner_id', '!=', False)],
                                  order='fecha, id')
        por_partner = {}
        for row in rows:
            por_partner.setdefault(row.partner_id.commercial_partner_id, self.browse())
            por_partner[row.partner_id.commercial_partner_id] |= row
        Activity = self.env['mail.activity'].sudo()
        todo = self.env.ref('mail.mail_activity_data_todo')
        model_id = self.env['ir.model']._get_id('res.partner')
        deadline = fields.Date.context_today(self)
        hechas = Activity
        for partner, prows in por_partner.items():
            total_mxn = sum(prows.mapped(lambda r: r.pagado_odoo_mxn - r.pagado_sat_mxn))
            note = Markup(
                '<p>%s factura(s) pagada(s) en Odoo sin complemento de pago en el SAT '
                '(%s MXN). Sin el complemento, el IVA de esos pagos no se acredita.</p>'
                '<p>En este contacto, menú <b>Acción → Pedir complementos de pago</b> abre el correo ya '
                'escrito para el proveedor%s.</p>%s') % (
                len(prows), _money(total_mxn),
                '' if partner.email else Markup(' <b>(el contacto no tiene correo: captúrelo antes)</b>'),
                partner._sat_rep_tabla(prows))
            summary = '%s (%s)' % (RESUMEN_PROVEEDOR, len(prows))
            existing = Activity.search([('res_model', '=', 'res.partner'), ('res_id', '=', partner.id),
                                        ('summary', '=like', RESUMEN_PROVEEDOR + '%')], limit=1)
            if existing:
                existing.write({'summary': summary, 'note': note})
                hechas |= existing
                continue
            hechas |= Activity.create({
                'res_model_id': model_id,
                'res_id': partner.id,
                'activity_type_id': todo.id,
                'summary': summary,
                'note': note,
                'user_id': users[0].id,
                'date_deadline': deadline,
            })
        _logger.info('sat.pago.compare: %s actividades de complementos de proveedores', len(hechas))
        return hechas

    # ----------------------------------------------------------- clientes
    @api.model
    def _rep_clientes_pendientes(self):
        """Cobros sin complemento: (renglón, pagos de Odoo sin timbrar, vencido).

        Un renglón cuyo pago ya tiene folio fiscal en Odoo (el complemento se
        timbró y el SAT todavía no lo entrega) no se avisa."""
        companies = self.env['res.company'].sudo().search([('sat_sync_enabled', '=', True)])
        rows = self.sudo().search([('company_id', 'in', companies.ids), ('direction', '=', 'issued'),
                                   ('issue', 'in', ('rep_en_plazo', 'sin_complemento'))],
                                  order='fecha_pago_odoo, id')
        Payment = self.env['account.payment']
        con_uuid = 'l10n_mx_edi_cfdi_uuid' in Payment._fields
        out = []
        for row in rows:
            pagos = Payment
            if row.move_id:
                pagos = (row.move_id._get_reconciled_payments() | row.move_id.matched_payment_ids).filtered(
                    lambda p: p.state != 'canceled')
            sin_timbrar = pagos.filtered(lambda p: not p.l10n_mx_edi_cfdi_uuid) if con_uuid else pagos
            if con_uuid and pagos and not sin_timbrar:
                continue
            out.append((row, sin_timbrar, row.issue == 'sin_complemento'))
        return out

    @api.model
    def _aviso_rep_clientes(self, today=None):
        """Correo a alert_email con los cobros sin complemento. Sin
        destinatarios o sin pendientes no manda nada."""
        recipients = self.env['sat.cfdi']._alert_recipients()
        if not recipients:
            return False
        pendientes = self._rep_clientes_pendientes()
        if not pendientes:
            return False
        today = today or fields.Date.context_today(self)
        vence = date(today.year, today.month, 5)
        filas = Markup('').join(
            Markup('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>'
                   '<td style="text-align:right">%s</td><td>%s</td></tr>') % (
                row.counterparty_name or '', row.move_name or '', row.fecha_pago_odoo or '',
                ', '.join(pagos.mapped('name')) or '—',
                '%s %s' % (_money(row.pagado_odoo - row.pagado_sat), row.moneda or ''),
                _money(row.pagado_odoo_mxn - row.pagado_sat_mxn),
                Markup('<b>Vencido</b>') if vencido else escape(_('Vence el %s') % vence.strftime('%d/%m/%Y')))
            for row, pagos, vencido in pendientes)
        n_vencidos = sum(1 for p in pendientes if p[2])
        body = Markup(
            '<p>Cobros registrados en Odoo cuyo complemento de pago no está en el SAT ni timbrado en Odoo. '
            'El complemento se emite a más tardar el día 5 del mes siguiente al cobro.</p>'
            '<p>Para timbrarlo: abra el pago en Odoo y use <b>Timbrar</b> (o «Enviar e imprimir»).</p>'
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Cliente</th><th>Factura</th>'
            '<th>Cobrada el</th><th>Pago en Odoo</th><th>Sin complemento</th><th>MXN</th><th>Plazo</th></tr>'
            '%s</table>'
            '<p>Detalle: Contabilidad → SAT (Syntage) → Pagos (REP), filtros «Emitidas (cobros)» y '
            '«Con hallazgo».</p>') % filas
        subject = _('Complementos de pago a clientes: %(n)s sin timbrar (%(v)s vencidos)') % {
            'n': len(pendientes), 'v': n_vencidos}
        mail = self.env['mail.mail'].sudo().create({
            'subject': subject,
            'email_to': ', '.join(recipients),
            'body_html': body,
            'auto_delete': False,
        })
        mail.send()
        return mail
