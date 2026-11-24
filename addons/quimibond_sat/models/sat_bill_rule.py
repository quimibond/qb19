# -*- coding: utf-8 -*-
"""Regla de cuenta para las facturas de proveedor que nacen del SAT sin orden
de compra.

Con orden de compra la cuenta sale de la categoría del producto, como cuando
se crea la factura desde la orden. Sin orden, el importador de XML de Odoo
pone la cuenta del producto que adivina (un genérico como «REEMBOLSO» cae en
la categoría «All» y termina en costo primo). La regla dice qué cuenta,
analítica y diario usa ese proveedor; con clave del SAT (ClaveProdServ), solo
para los conceptos con esa clave, para los proveedores que usan varias
cuentas (Si Vale, Edenred, rentas).

Las reglas nacen **propuestas** del historial (las últimas facturas
publicadas del proveedor usan siempre la misma cuenta) o de lo que Contabilidad
publicó; una factura con regla propuesta llega en amarillo. Solo una regla
**confirmada** deja una factura en verde.
"""
import html
from collections import Counter, defaultdict
from datetime import timedelta

from odoo import _, api, fields, models


def cfdi_items(cfdi):
    """Conceptos del CFDI tal como los manda Syntage (``raw_json.items``)."""
    out = []
    for item in (cfdi.raw_json or {}).get('items') or []:
        if not isinstance(item, dict):
            continue
        out.append({
            'qty': float(item.get('quantity') or 0.0),
            'price': float(item.get('unitAmount') or 0.0),
            'amount': float(item.get('totalAmount') or 0.0),
            'discount': float(item.get('discountAmount') or 0.0),
            'description': html.unescape(item.get('description') or ''),
            'clave': (item.get('productIdentification') or '').strip(),
            'code': (item.get('identificationNumber') or '').strip(),
        })
    return out


def same_price(a, b):
    """Precios unitarios iguales: a un centavo o 0.5 %."""
    return abs((a or 0.0) - (b or 0.0)) <= max(0.01, 0.005 * abs(b or 0.0))


def pair_lines_with_items(lines, items):
    """Empareja líneas de factura con conceptos del CFDI por precio unitario y
    cantidad (y, a falta de eso, por orden cuando son tantas como conceptos).
    Devuelve {línea.id: concepto}."""
    pairs = {}
    free = list(range(len(items)))
    for line in lines:
        for idx in free:
            item = items[idx]
            if same_price(line.price_unit, item['price']) and abs(line.quantity - item['qty']) <= 0.001:
                pairs[line.id] = item
                free.remove(idx)
                break
    if not pairs and len(lines) == len(items):
        pairs = {line.id: item for line, item in zip(lines, items)}
    return pairs


class SatBillRule(models.Model):
    _name = 'sat.bill.rule'
    _inherit = ['analytic.mixin']
    _description = 'Regla de cuenta para facturas de proveedor desde el SAT'
    _order = 'partner_id, clave_sat, id'

    company_id = fields.Many2one('res.company', string='Compañía', required=True, index=True,
                                 default=lambda self: self.env.company)
    partner_id = fields.Many2one('res.partner', string='Proveedor', required=True, index=True,
                                 domain="[('is_company', '=', True)]")
    clave_sat = fields.Char(
        string='Clave del SAT',
        help='ClaveProdServ del concepto del CFDI (p. ej. 15101514 gasolina). Vacío: todos los conceptos '
             'del proveedor que no tengan una regla con su clave.')
    concepto = fields.Char(
        string='El concepto dice',
        help='Texto que debe aparecer en la descripción del concepto del CFDI (sin importar mayúsculas), '
             'p. ej. «ADMON DE FLOTAS». Vacío: cualquier concepto.')
    account_id = fields.Many2one('account.account', string='Cuenta', required=True,
                                 domain="[('active', '=', True)]")
    journal_id = fields.Many2one('account.journal', string='Diario',
                                 domain="[('type', '=', 'purchase'), ('company_id', '=', company_id)]",
                                 help='Diario donde nace la factura (p. ej. GSTVAR para gastos chicos). '
                                      'Vacío: el de la compañía.')
    state = fields.Selection([
        ('propuesta', 'Propuesta'),
        ('confirmada', 'Confirmada'),
    ], string='Estado', default='propuesta', required=True, index=True,
        help='Solo una regla confirmada deja la factura en verde; con una propuesta llega en amarillo.')
    origen = fields.Char(string='De dónde salió', readonly=True)
    facturas = fields.Integer(string='Facturas en el historial', readonly=True)
    monto_min = fields.Float(string='Subtotal mínimo', digits=(16, 2),
                             help='Subtotal más chico visto en el historial. Fuera del rango (−30 % / +30 %) '
                                  'la factura llega en amarillo. En cero: sin rango.')
    monto_max = fields.Float(string='Subtotal máximo', digits=(16, 2))
    active = fields.Boolean(default=True)

    @api.depends('partner_id', 'clave_sat', 'concepto', 'account_id')
    def _compute_display_name(self):
        for rule in self:
            rule.display_name = '%s%s%s → %s' % (
                rule.partner_id.name or '', ' [%s]' % rule.clave_sat if rule.clave_sat else '',
                ' «%s»' % rule.concepto if rule.concepto else '', rule.account_id.display_name or '')

    # ── uso ─────────────────────────────────────────────────────────

    @api.model
    def _rules_for(self, partner, company):
        partner = partner.commercial_partner_id
        if not partner:
            return self.browse()
        return self.sudo().search([('company_id', '=', company.id), ('partner_id', '=', partner.id)])

    @api.model
    def _pick(self, rules, clave, description=''):
        """La regla más específica que aplica al concepto: clave y texto, solo
        texto, solo clave y, al final, la del proveedor sin condiciones."""
        clave = (clave or '').strip()
        text = (description or '').upper()

        def fits(rule):
            if rule.clave_sat and rule.clave_sat != clave:
                return False
            if rule.concepto and rule.concepto.upper() not in text:
                return False
            return True
        candidates = rules.filtered(fits)
        return candidates.sorted(key=lambda r: (not (r.clave_sat and r.concepto), not r.concepto,
                                                not r.clave_sat, r.id))[:1]

    def _monto_fuera_de_rango(self, subtotal):
        self.ensure_one()
        if not self.monto_max:
            return False
        return abs(subtotal) < 0.7 * self.monto_min or abs(subtotal) > 1.3 * self.monto_max

    def action_confirm(self):
        self.write({'state': 'confirmada'})

    def action_propose(self):
        self.write({'state': 'propuesta'})

    # ── propuestas desde el historial ───────────────────────────────

    @api.model
    def _seed_from_history(self, months=6, min_bills=3):
        """Propone reglas con las facturas de proveedor publicadas sin orden de
        compra de los últimos `months` meses. No toca las reglas que ya existen.
        Un proveedor con `min_bills` facturas o más que usa siempre la misma
        cuenta → regla sin clave; si usa varias, una regla por clave del SAT
        que siempre fue a la misma cuenta (dos veces o más). Devuelve cuántas
        creó."""
        created = 0
        for company in self.env['res.company'].sudo().search([('sat_sync_enabled', '=', True)]):
            created += self._seed_company(company, months, min_bills)
        return created

    @api.model
    def action_seed_from_history(self):
        """Botón de la lista de reglas."""
        created = self._seed_from_history()
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': _('Reglas de facturas desde el SAT'),
                       'message': _('%s reglas propuestas desde el historial.') % created,
                       'type': 'success', 'sticky': False,
                       'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'}},
        }

    @api.model
    def _seed_company(self, company, months, min_bills):
        Line = self.env['account.move.line'].sudo()
        since = fields.Date.today() - timedelta(days=int(months * 30.5))
        domain = [('company_id', '=', company.id), ('move_id.move_type', '=', 'in_invoice'),
                  ('parent_state', '=', 'posted'), ('display_type', '=', 'product'),
                  ('date', '>=', since), ('journal_id.type', '=', 'purchase')]
        if 'purchase_line_id' in Line._fields:
            domain.append(('purchase_line_id', '=', False))
        lines = Line.search(domain, order='date desc, move_id, sequence, id')
        by_partner = defaultdict(lambda: self.env['account.move.line'])
        for line in lines:
            partner = line.move_id.commercial_partner_id
            if partner.sat_cfdi_policy != 'factura':
                continue
            by_partner[partner] |= line
        existing = {(r.partner_id.id, r.clave_sat or '') for r in self.sudo().with_context(active_test=False).search(
            [('company_id', '=', company.id)])}
        vals_list = []
        for partner, plines in by_partner.items():
            moves = plines.move_id
            if len(moves) < min_bills:
                continue
            accounts = Counter(plines.mapped('account_id').ids)
            journals = Counter(moves.mapped('journal_id').ids)
            subtotals = [abs(m.amount_untaxed) for m in moves]
            if len(accounts) == 1:
                if (partner.id, '') in existing:
                    continue
                vals = {
                    'company_id': company.id, 'partner_id': partner.id,
                    'account_id': next(iter(accounts)),
                    'journal_id': journals.most_common(1)[0][0],
                    'facturas': len(moves), 'monto_min': min(subtotals), 'monto_max': max(subtotals),
                    'origen': _('Historial: %s facturas, siempre la misma cuenta') % len(moves),
                }
                distributions = Counter(str(sorted((l.analytic_distribution or {}).items())) for l in plines)
                top, count = distributions.most_common(1)[0]
                if count >= 0.8 * len(plines):
                    sample = plines.filtered(lambda l: str(sorted((l.analytic_distribution or {}).items())) == top)[:1]
                    vals['analytic_distribution'] = sample.analytic_distribution or False
                vals_list.append(vals)
                continue
            # Varias cuentas: por la clave del SAT de cada concepto.
            per_clave = defaultdict(list)
            for move in moves:
                cfdis = move.sat_cfdi_ids.filtered(lambda c: c.tipo == 'I' and c.estado_sat != 'cancelado')
                if len(cfdis) != 1:
                    continue
                mlines = plines.filtered(lambda l: l.move_id == move)
                pairs = pair_lines_with_items(mlines, cfdi_items(cfdis))
                for line in mlines:
                    item = pairs.get(line.id)
                    if item and item['clave']:
                        per_clave[item['clave']].append(line.account_id.id)
            for clave, accs in per_clave.items():
                if len(accs) < 2 or len(set(accs)) != 1 or (partner.id, clave) in existing:
                    continue
                vals_list.append({
                    'company_id': company.id, 'partner_id': partner.id, 'clave_sat': clave,
                    'account_id': accs[0], 'facturas': len(accs),
                    'origen': _('Historial: %s conceptos con esta clave, siempre la misma cuenta') % len(accs),
                })
        if vals_list:
            self.sudo().create(vals_list)
        return len(vals_list)

    @api.model
    def _learn_from_posted(self, moves):
        """Al publicar una factura que nació del SAT: los conceptos sin regla
        dejan una regla propuesta con la cuenta que Contabilidad eligió."""
        vals_list = []
        seen = set()
        for move in moves:
            partner = move.commercial_partner_id
            rules = self._rules_for(partner, move.company_id)
            for line in move.invoice_line_ids.filtered(lambda l: l.display_type == 'product'):
                if 'purchase_line_id' in line._fields and line.purchase_line_id:
                    continue
                if self._pick(rules, line.sat_clave, line.name):
                    continue
                key = (move.company_id.id, partner.id, line.sat_clave or '')
                if key in seen:
                    continue
                seen.add(key)
                vals_list.append({
                    'company_id': move.company_id.id, 'partner_id': partner.id,
                    'clave_sat': line.sat_clave or False, 'account_id': line.account_id.id,
                    'journal_id': move.journal_id.id if not line.sat_clave else False,
                    'analytic_distribution': line.analytic_distribution or False,
                    'facturas': 1, 'origen': _('Publicada: %s') % move.name,
                })
        if vals_list:
            self.sudo().create(vals_list)
        return len(vals_list)
