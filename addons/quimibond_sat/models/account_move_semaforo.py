# -*- coding: utf-8 -*-
"""Semáforo de las facturas de proveedor que nacen del SAT.

* **Verde:** cuadra con el SAT al centavo, misma moneda, sin 69-B, sin
  anticipo abierto, y cada línea viene de una orden de compra con el mismo
  precio y sin facturar más de lo recibido, o de una regla de proveedor
  confirmada con el monto en su rango. Se publica en lote desde
  *Compras por revisar*.
* **Amarillo:** falta algo que Contabilidad completa (regla propuesta o sin
  regla, precio distinto a la orden, anticipo abierto para fijar el tipo de
  cambio ponderado, monto fuera de rango).
* **Rojo:** no cuadra con el SAT, otra moneda, 69-B, CFDI cancelado o sin
  proveedor: no se publica en lote.

El semáforo se calcula al crear la factura y con *Revisar de nuevo*; publicar
en lote lo recalcula antes de publicar.
"""
from datetime import timedelta

from odoo import _, api, fields, models

from .res_partner import RIESGO_69B

DEFAULT_ADVANCE_ACCOUNTS = '120.01.01,120.01.02'
TOTAL_TOLERANCE = 0.05


class AccountMoveSemaforo(models.Model):
    _inherit = 'account.move'

    sat_auto_bill = fields.Boolean(string='Creada desde el SAT', copy=False, index=True, readonly=True)
    sat_origen = fields.Char(string='Cómo se armó', copy=False, readonly=True,
                             help='Orden de compra encontrada o regla del proveedor que se usó.')
    sat_semaforo = fields.Selection([
        ('verde', 'Verde: lista para publicar'),
        ('amarillo', 'Amarillo: completar'),
        ('rojo', 'Rojo: no publicar'),
    ], string='Semáforo', copy=False, index=True, readonly=True)
    sat_semaforo_motivo = fields.Text(string='Qué revisar', copy=False, readonly=True)

    # ── reglas del semáforo ─────────────────────────────────────────

    @api.model
    def _sat_advance_account_codes(self):
        raw = self.env['ir.config_parameter'].sudo().get_param('quimibond_sat.cuentas_anticipo_proveedor')
        raw = raw or DEFAULT_ADVANCE_ACCOUNTS
        return [c.strip() for c in raw.replace(';', ',').split(',') if c.strip()]

    def _sat_open_advance(self):
        """Saldo de anticipos abiertos del proveedor (en moneda de la compañía)."""
        self.ensure_one()
        codes = self._sat_advance_account_codes()
        partner = self.commercial_partner_id
        if not codes or not partner:
            return 0.0
        lines = self.env['account.move.line'].sudo().search([
            ('company_id', '=', self.company_id.id),
            ('partner_id', 'child_of', partner.id),
            ('parent_state', '=', 'posted'),
            ('account_id.code', 'in', codes),
            ('reconciled', '=', False),
        ])
        return sum(lines.mapped('amount_residual'))

    def _sat_semaforo_check(self):
        """(color, [motivos]) de esta factura de proveedor."""
        self.ensure_one()
        rojo, amarillo = [], []
        cfdis = self.sat_cfdi_ids.filtered(lambda c: c.tipo in ('I', 'E'))
        vigentes = cfdis.filtered(lambda c: c.estado_sat != 'cancelado')
        partner = self.commercial_partner_id
        if not partner:
            rojo.append(_('Sin proveedor: dé de alta el contacto con el RFC del CFDI.'))
        if not vigentes:
            rojo.append(_('El CFDI está cancelado en el SAT o no está ligado.') if cfdis
                        else _('Sin CFDI del SAT ligado.'))
        else:
            currency = self.currency_id.name
            if any((c.moneda or 'MXN').upper() != currency for c in vigentes):
                rojo.append(_('El CFDI está en otra moneda que la factura.'))
            else:
                total_sat = sum(c.total if c.tipo == 'I' else -c.total for c in vigentes)
                total_sat = abs(total_sat)
                if abs(abs(self.amount_total) - total_sat) > TOTAL_TOLERANCE:
                    rojo.append(_('No cuadra con el SAT: la factura da %(odoo)s y el CFDI %(sat)s.') % {
                        'odoo': '{:,.2f}'.format(abs(self.amount_total)), 'sat': '{:,.2f}'.format(total_sat)})
        if partner.sat_69b_status in RIESGO_69B:
            rojo.append(_('El proveedor está en la lista 69-B del SAT.'))

        advance = self._sat_open_advance() if partner else 0.0
        if advance > 0.005:
            amarillo.append(_('Anticipo abierto del proveedor por %s: aplíquelo y fije el tipo de cambio '
                              'ponderado de lo que cubre antes de publicar.') % '{:,.2f}'.format(advance))

        lines = self.invoice_line_ids.filtered(lambda l: l.display_type == 'product')
        has_po = 'purchase_line_id' in lines._fields
        for line in lines:
            label = (line.name or '').split('\n')[0][:60]
            po_line = has_po and line.purchase_line_id
            if po_line:
                price = po_line.currency_id._convert(po_line.price_unit, self.currency_id, self.company_id,
                                                     self.invoice_date or fields.Date.today(), round=False)
                net = line.price_unit * (1 - (line.discount or 0.0) / 100.0)
                if abs(net - price) > max(0.01, 0.01 * abs(price)):
                    amarillo.append(_('%(line)s: el precio (%(bill)s) no es el de la orden %(po)s (%(order)s).') % {
                        'line': label, 'bill': line.price_unit, 'po': po_line.order_id.name, 'order': price})
                if po_line.qty_to_invoice < -0.001:
                    amarillo.append(_('%(line)s: se factura más de lo recibido en %(po)s.') % {
                        'line': label, 'po': po_line.order_id.name})
                continue
            if 'is_storable' in line.product_id._fields and line.product_id.is_storable:
                amarillo.append(_('%s: producto de inventario sin orden de compra. Pida la orden a Compras, '
                                  'que Almacén la reciba y oprima «Revisar de nuevo».') % label)
            rule = line.sat_bill_rule_id
            if not rule:
                amarillo.append(_('%s: sin regla de cuenta para este proveedor; elija la cuenta.') % label)
            elif rule.state != 'confirmada':
                amarillo.append(_('%(line)s: la regla «%(rule)s» está por confirmar.') % {
                    'line': label, 'rule': rule.display_name})
        if self.move_type == 'in_invoice' and partner and has_po and lines and not lines.purchase_line_id:
            last_order = self._sat_recent_purchase_order()
            if last_order:
                amarillo.append(_('Este proveedor trabaja con orden de compra (última: %s) y esta factura no '
                                  'trae orden. Pida la orden a Compras y oprima «Revisar de nuevo», o publíquela '
                                  'así si es un gasto sin orden.') % last_order.name)
        rules = lines.sat_bill_rule_id
        for rule in rules.filtered(lambda r: not r.clave_sat and not r.concepto):
            if rule._monto_fuera_de_rango(self.amount_untaxed):
                amarillo.append(_('El subtotal %(amount)s está fuera de lo normal para este proveedor '
                                  '(%(min)s a %(max)s).') % {
                    'amount': '{:,.2f}'.format(abs(self.amount_untaxed)),
                    'min': '{:,.2f}'.format(rule.monto_min), 'max': '{:,.2f}'.format(rule.monto_max)})
        if rojo:
            return 'rojo', rojo + amarillo
        if amarillo:
            return 'amarillo', amarillo
        return 'verde', []

    def _sat_recent_purchase_order(self, days=180):
        """Última orden de compra confirmada del proveedor en `days` días."""
        self.ensure_one()
        if 'purchase.order' not in self.env:
            return False
        since = fields.Datetime.now() - timedelta(days=days)
        return self.env['purchase.order'].sudo().search([
            ('company_id', '=', self.company_id.id),
            ('partner_id', 'child_of', self.commercial_partner_id.id),
            ('state', 'in', ('purchase', 'done')),
            ('date_approve', '>=', since),
        ], order='date_approve desc', limit=1)

    def _sat_semaforo_update(self):
        for move in self.filtered(lambda m: m.sat_auto_bill and m.state == 'draft'):
            color, reasons = move._sat_semaforo_check()
            move.write({'sat_semaforo': color, 'sat_semaforo_motivo': '\n'.join(reasons) or False})

    def action_sat_semaforo(self):
        """Botón «Revisar de nuevo»: si la factura nació sin orden y Compras
        ya hizo la orden (recibida), la rehace desde la orden."""
        for move in self.filtered(lambda m: m.sat_auto_bill and m.state == 'draft'):
            cfdi = move.sat_cfdi_ids.filtered(lambda c: c.tipo == 'I' and c.estado_sat != 'cancelado')[:1]
            if cfdi:
                cfdi._rebuild_bill_from_purchase(move)
        self._sat_semaforo_update()
        return True

    def action_sat_post_green(self):
        """Publica las facturas verdes de la selección (recalcula antes).
        Las que no quedan en verde se quedan en borrador."""
        moves = self.filtered(lambda m: m.sat_auto_bill and m.state == 'draft')
        moves._sat_semaforo_update()
        green = moves.filtered(lambda m: m.sat_semaforo == 'verde')
        posted, failed = self.browse(), []
        for move in green:
            try:
                with self.env.cr.savepoint():
                    move.action_post()
                posted |= move
            except Exception as exc:  # noqa: BLE001 — una factura mala no detiene a las demás
                failed.append('%s: %s' % (move.ref or move.partner_id.name, str(exc)[:120]))
        message = _('%(posted)s publicadas; %(left)s siguen en borrador (amarillo o rojo).') % {
            'posted': len(posted), 'left': len(moves) - len(posted)}
        if failed:
            message += '\n' + _('No se pudieron publicar: %s') % '; '.join(failed)
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': _('Publicar las verdes'), 'message': message,
                       'type': 'warning' if failed else 'success', 'sticky': bool(failed),
                       'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'}},
        }

    def _post(self, soft=True):
        res = super()._post(soft=soft)
        learned = self.filtered(lambda m: m.sat_auto_bill and m.state == 'posted')
        if learned:
            learned.write({'sat_semaforo': False, 'sat_semaforo_motivo': False})
            self.env['sat.bill.rule']._learn_from_posted(learned)
        return res


class AccountMoveLineSemaforo(models.Model):
    _inherit = 'account.move.line'

    sat_clave = fields.Char(string='Clave del SAT', copy=False,
                            help='ClaveProdServ del concepto del CFDI que dio origen a esta línea.')
    sat_bill_rule_id = fields.Many2one('sat.bill.rule', string='Regla de cuenta', copy=False,
                                       ondelete='set null', index='btree_not_null')
    sat_cuenta_propuesta_id = fields.Many2one(
        'account.account', string='Cuenta que propuso el SAT', copy=False, ondelete='set null',
        help='Cuenta con la que nació la línea desde el CFDI (orden de compra o regla). Si al publicar '
             'es otra, Contabilidad la corrigió: sirve para medir qué tan bien acierta.')

    def _sat_remember_account(self):
        for line in self.filtered(lambda l: l.display_type == 'product'):
            line.sat_cuenta_propuesta_id = line.account_id
