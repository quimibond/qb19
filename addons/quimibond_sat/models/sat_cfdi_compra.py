# -*- coding: utf-8 -*-
"""Factura de proveedor desde el SAT, con orden de compra o con la regla del
proveedor.

Antes de importar el XML se busca la orden de compra del CFDI (``raw_json``
de Syntage ya trae los conceptos: cantidad, precio, descripción y clave del
SAT). Si hay una, la factura se arma **desde la orden**, con lo recibido por
facturar, como lo hace Compras con el botón de la orden: queda ligada a la
recepción, con la cuenta de la categoría del producto, los impuestos y la
analítica de la orden. Si no hay orden, el XML entra por el importador de Odoo
(que a veces encuentra la orden por su cuenta) y a las líneas que quedan sin
orden se les aplica la regla del proveedor (``sat.bill.rule``).

Orden por orden de preferencia:

1. el CFDI menciona la orden (nombre o «Referencia de proveedor» de Compras);
2. una sola orden cuyo subtotal por facturar cuadra con el subtotal del CFDI
   (a $1 o 0.5 %);
3. hasta tres órdenes del proveedor que juntas cuadran;
4. concepto por concepto: cada concepto con una línea de orden del mismo
   precio unitario y con cantidad por facturar suficiente.

Todo nace en borrador con su semáforo (``account.move.sat_semaforo``).
"""
import itertools
import logging

import pytz

from odoo import Command, _, api, fields, models
from odoo.exceptions import UserError

from .sat_bill_rule import cfdi_items, pair_lines_with_items, same_price

_logger = logging.getLogger(__name__)

MAX_ORDERS_FOR_COMBOS = 12


class SatCfdiCompra(models.Model):
    _inherit = 'sat.cfdi'

    # ── datos del CFDI ──────────────────────────────────────────────

    def _cfdi_currency(self):
        self.ensure_one()
        name = (self.moneda or 'MXN').upper()
        return self.env['res.currency'].with_context(active_test=False).search([('name', '=', name)], limit=1)

    def _cfdi_local_date(self):
        """Fecha de emisión en la hora de México (fecha_emision es UTC)."""
        self.ensure_one()
        if not self.fecha_emision:
            return fields.Date.context_today(self)
        dt = pytz.utc.localize(fields.Datetime.to_datetime(self.fecha_emision))
        return dt.astimezone(pytz.timezone('America/Mexico_City')).date()

    def _cfdi_subtotal(self):
        self.ensure_one()
        return (self.subtotal or 0.0) - (self.descuento or 0.0)

    def _cfdi_text(self):
        """Lo que el CFDI dice en texto libre: conceptos y referencia."""
        self.ensure_one()
        raw = self.raw_json or {}
        parts = [item['description'] for item in cfdi_items(self)]
        parts += [str(raw.get(k) or '') for k in ('reference', 'internalIdentifier', 'paymentTermsRaw')]
        return ' '.join(parts).upper()

    # ── orden de compra ─────────────────────────────────────────────

    def _open_purchase_lines(self):
        """Líneas de orden confirmadas del proveedor con algo por facturar,
        en la moneda del CFDI."""
        self.ensure_one()
        if 'purchase.order.line' not in self.env or self.tipo != 'I' or not self.partner_id:
            return None
        partner = self.partner_id.commercial_partner_id
        lines = self.env['purchase.order.line'].sudo().search([
            ('company_id', '=', self.company_id.id),
            ('order_id.partner_id', 'child_of', partner.id),
            ('order_id.state', 'in', ('purchase', 'done')),
            ('display_type', '=', False),
            ('qty_to_invoice', '>', 0),
        ])
        currency = self._cfdi_currency()
        return lines.filtered(lambda l: l.order_id.currency_id == currency)

    @staticmethod
    def _amount_to_invoice(lines):
        return sum(l.qty_to_invoice * l.price_unit * (1 - (l.discount or 0.0) / 100.0) for l in lines)

    def _match_purchase(self):
        """Busca lo que este CFDI factura de las órdenes de compra.

        Devuelve un dict ``{'lines', 'quantities', 'how'}``: las líneas de
        orden, la cantidad a facturar de cada una (id → cantidad; las que no
        están van con lo que tienen por facturar) y cómo se encontró. Sin
        orden: ``lines`` vacío."""
        self.ensure_one()
        empty = {'lines': None, 'quantities': {}, 'how': False}
        lines = self._open_purchase_lines()
        if not lines:
            return empty
        subtotal = self._cfdi_subtotal()
        tol = max(1.0, 0.005 * abs(subtotal))
        orders = lines.order_id
        text = self._cfdi_text()
        named = orders.filtered(lambda o: (o.name and o.name.upper() in text)
                                or (o.partner_ref and len(o.partner_ref) > 3 and o.partner_ref.upper() in text))
        if named:
            orders = named

        def order_lines(recs):
            return lines.filtered(lambda l: l.order_id in recs)

        # 2. Una sola orden que cuadra completa.
        hits = [o for o in orders if abs(self._amount_to_invoice(order_lines(o)) - subtotal) <= tol]
        if len(hits) == 1:
            how = _('orden %s (lo pendiente cuadra con el CFDI)') % hits[0].name
            return {'lines': order_lines(hits[0]), 'quantities': {}, 'how': how}
        # 3. Hasta tres órdenes que juntas cuadran (una sola combinación).
        if not hits and 1 < len(orders) <= MAX_ORDERS_FOR_COMBOS:
            combos = []
            for size in (2, 3):
                for combo in itertools.combinations(orders, size):
                    recs = self.env['purchase.order'].union(*combo)
                    if abs(self._amount_to_invoice(order_lines(recs)) - subtotal) <= tol:
                        combos.append(recs)
                if combos:
                    break
            if len(combos) == 1:
                how = _('órdenes %s (juntas cuadran con el CFDI)') % ', '.join(combos[0].mapped('name'))
                return {'lines': order_lines(combos[0]), 'quantities': {}, 'how': how}
        # 4. Concepto por concepto, por precio unitario.
        items = cfdi_items(self)
        if not items:
            return empty
        pool = order_lines(orders).sorted(lambda l: (l.order_id.date_approve or l.order_id.create_date, l.id))
        quantities = {}
        for item in items:
            match = next((l for l in pool if l.id not in quantities and same_price(l.price_unit, item['price'])
                          and item['qty'] <= l.qty_to_invoice + 0.001), None)
            if not match:
                return empty
            quantities[match.id] = item['qty']
        matched = pool.filtered(lambda l: l.id in quantities)
        how = _('concepto por concepto en %s') % ', '.join(matched.order_id.mapped('name'))
        return {'lines': matched, 'quantities': quantities, 'how': how}

    def _bill_ref(self, orders):
        """Referencia como la captura Contabilidad: método de pago / folio /
        referencia de proveedor de la orden («PPD/F-21733/R-686-26»)."""
        self.ensure_one()
        parts = [self.metodo_pago or '']
        folio = self.folio or ''
        parts.append('F-%s' % folio if folio else (self.uuid or '')[:8].upper())
        refs = [r for r in orders.mapped('partner_ref') if r]
        if refs:
            parts.append(', '.join(refs))
        return '/'.join(p for p in parts if p)

    def _create_bill_from_purchase(self, match, journal):
        """Factura en borrador desde las líneas de orden de `match`."""
        self.ensure_one()
        po_lines = match['lines']
        quantities = match['quantities']
        orders = po_lines.order_id
        first = orders.sorted('id')[:1].with_company(self.company_id)
        vals = first.with_context(default_move_type='in_invoice')._prepare_invoice()
        vals.update({
            'journal_id': journal.id,
            'invoice_date': self._cfdi_local_date(),
            'invoice_origin': ', '.join(orders.mapped('name')),
            'ref': self._bill_ref(orders),
            'invoice_line_ids': [],
        })
        sequence = 10
        for line in po_lines.sorted(lambda l: (l.order_id.id, l.sequence, l.id)):
            line_vals = line.with_company(self.company_id)._prepare_account_move_line()
            if line.id in quantities:
                line_vals['quantity'] = quantities[line.id]
            line_vals['sequence'] = sequence
            sequence += 1
            vals['invoice_line_ids'].append(Command.create(line_vals))
        move = self.env['account.move'].with_company(self.company_id).with_context(
            default_move_type='in_invoice').create(vals)
        # Clave del SAT de cada línea (para el semáforo y para aprender).
        pairs = pair_lines_with_items(move.invoice_line_ids.filtered(lambda l: l.display_type == 'product'),
                                      cfdi_items(self))
        for line in move.invoice_line_ids:
            item = pairs.get(line.id)
            if item and item['clave']:
                line.sat_clave = item['clave']
        return move

    def _link_new_bill(self, move, how):
        """Liga este CFDI a la factura recién creada, con folio fiscal y XML."""
        self.ensure_one()
        self.write({'move_id': move.id, 'match_method': 'conciliado', 'match_status': 'matched'})
        if self._has_mx_edi():
            self._write_fiscal_folio(move)
        else:
            self._attach_xml_to_move(move)
        note = _('Factura creada en borrador desde el XML del SAT: %s') % how
        self.write({'auto_bill_state': 'created', 'auto_bill_note': note[:250], 'note': note[:250]})
        move.message_post(
            body=_('Factura creada en borrador desde el CFDI del SAT (Syntage) %(uuid)s, total %(total)s %(cur)s, '
                   'con %(how)s. Revísela y publíquela.') % {
                'uuid': (self.uuid or '').upper(), 'total': '{:,.2f}'.format(self.total or 0.0),
                'cur': self.moneda or 'MXN', 'how': how},
            message_type='comment', subtype_xmlid='mail.mt_note')

    # ── regla del proveedor ─────────────────────────────────────────

    def _apply_bill_rules(self, move):
        """Cuenta (y analítica) de la regla del proveedor en las líneas sin
        orden de compra. Devuelve cuántas líneas tomaron regla."""
        self.ensure_one()
        Rule = self.env['sat.bill.rule']
        rules = Rule._rules_for(self.partner_id or move.partner_id, self.company_id)
        lines = move.invoice_line_ids.filtered(lambda l: l.display_type == 'product')
        if 'purchase_line_id' in lines._fields:
            lines = lines.filtered(lambda l: not l.purchase_line_id)
        pairs = pair_lines_with_items(lines, cfdi_items(self))
        applied = 0
        for line in lines:
            item = pairs.get(line.id) or {}
            clave = item.get('clave') or False
            rule = Rule._pick(rules, clave, item.get('description') or line.name)
            vals = {'sat_clave': clave}
            if rule:
                vals.update({'account_id': rule.account_id.id, 'sat_bill_rule_id': rule.id})
                if rule.analytic_distribution:
                    vals['analytic_distribution'] = rule.analytic_distribution
                applied += 1
            line.write(vals)
        return applied

    # ── creación ────────────────────────────────────────────────────

    def _create_bill_from_xml(self, journal):
        """Con orden de compra, la factura se arma desde la orden; si no, el
        XML entra por el importador de Odoo y se aplica la regla del
        proveedor. En los dos casos queda en borrador con su semáforo."""
        self.ensure_one()
        blocker = self._auto_bill_blocker()
        if blocker:
            raise UserError(_('No se crea la factura del CFDI %(uuid)s: %(why)s.') % {
                'uuid': self.uuid, 'why': blocker})
        match = self._match_purchase()
        if match['lines']:
            with self.env.cr.savepoint():
                move = self._create_bill_from_purchase(match, journal)
                self._link_new_bill(move, match['how'])
            how = _('Orden de compra: %s') % match['how']
        else:
            rules = self.env['sat.bill.rule']._rules_for(self.partner_id, self.company_id)
            rule_journal = rules.filtered(lambda r: not r.clave_sat and not r.concepto and r.journal_id)[:1].journal_id
            move = super()._create_bill_from_xml(rule_journal or journal)
            if 'purchase_line_id' in move.invoice_line_ids._fields and move.invoice_line_ids.purchase_line_id:
                how = _('Orden de compra encontrada por Odoo al leer el XML (%s)') % ', '.join(
                    move.invoice_line_ids.purchase_line_id.order_id.mapped('name'))
            else:
                how = _('Sin orden de compra: regla del proveedor')
            self._apply_bill_rules(move)
        move.write({'sat_auto_bill': True, 'sat_origen': how[:250]})
        move._sat_semaforo_update()
        return move

    @api.model
    def _cron_seed_bill_rules(self):
        """Propone reglas nuevas con lo que se publicó (semanal)."""
        return self.env['sat.bill.rule']._seed_from_history()
