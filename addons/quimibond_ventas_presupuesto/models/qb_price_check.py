# -*- coding: utf-8 -*-
"""Comparación de precios del forecast contra las listas de Odoo (diseño v3, 4.4; 19.0.3.4.0).

La hoja PRECIOS del ``forecast.xlsx`` trae, por cliente y código, el precio por metro o por kilo con el
que Ventas pronostica. Antes de armar el presupuesto hay que ver dónde no coincide con la lista de precios
del cliente en Odoo (la que usan los pedidos). Cada carga reemplaza la comparación anterior de la compañía.

Estados:
- igual: la diferencia es menor a la tolerancia (0.5 % por omisión);
- diferente: hay regla y no coincide (con el %);
- sin_regla: la lista del cliente no tiene regla para el producto (Odoo caería al precio de venta, que no
  se usa) o el cliente no tiene lista;
- sin_producto: el código no existe en Odoo en la unidad del precio;
- sin_cliente: el nombre del forecast no se pudo ligar a un cliente.
La moneda no viene en el Excel: el precio se compara en la moneda de la lista del cliente.
"""
import base64
import io

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..release import forecast_xlsx

STATES = [
    ('igual', "Igual"),
    ('diferente', "Diferente"),
    ('sin_regla', "Sin regla en la lista"),
    ('sin_producto', "Sin producto"),
    ('sin_cliente', "Sin cliente"),
]
TOLERANCE_PCT = 0.5


class QbPriceCheck(models.Model):
    _name = 'qb.price.check'
    _description = "Comparación de precios del forecast contra la lista de Odoo"
    _order = 'state, client_excel, code'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True,
                                 default=lambda self: self.env.company)
    check_date = fields.Datetime(string="Comparado el", default=fields.Datetime.now, readonly=True)
    client_excel = fields.Char(string="Cliente en el forecast", readonly=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", readonly=True, index=True)
    code = fields.Char(string="Código en el forecast", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", readonly=True)
    basis = fields.Selection([('m', "Por metro"), ('kg', "Por kilo")], string="Base", readonly=True)
    price_excel = fields.Float(string="Precio del forecast", digits=(16, 4), readonly=True)
    price_odoo = fields.Float(string="Precio en Odoo", digits=(16, 4), readonly=True)
    currency_id = fields.Many2one('res.currency', string="Moneda de la lista", readonly=True)
    diff = fields.Float(string="Diferencia", digits=(16, 4), readonly=True,
                        help="Precio del forecast menos el de Odoo, en la moneda de la lista.")
    diff_pct = fields.Float(string="Diferencia (%)", digits=(16, 2), readonly=True)
    pricelist_id = fields.Many2one('product.pricelist', string="Lista", readonly=True)
    rule_id = fields.Many2one('product.pricelist.item', string="Regla", readonly=True)
    state = fields.Selection(STATES, string="Resultado", required=True, readonly=True, index=True)
    note = fields.Char(string="Nota", readonly=True)
    excel_row = fields.Integer(string="Renglón del Excel", readonly=True)

    @api.model
    def action_open_wizard(self):
        return {'type': 'ir.actions.act_window', 'name': "Comparar precios del forecast",
                'res_model': 'qb.price.check.wizard', 'view_mode': 'form', 'target': 'new'}

    # --- Precio de la lista --------------------------------------------------
    @api.model
    def _qb_list_price(self, partner, product):
        """(precio en la moneda de la lista, regla, lista, problema). La regla tiene que ser del producto:
        sin regla, o con una regla global sobre el precio de venta, Odoo caería al precio de venta (que no
        es precio de cliente)."""
        pricelist = partner.property_product_pricelist
        if not pricelist:
            return None, None, pricelist, "el cliente no tiene lista de precios"
        price, rule_id = pricelist._get_product_price_rule(
            product, 1.0, uom=product.uom_id, date=fields.Date.context_today(self))
        rule = self.env['product.pricelist.item'].browse(rule_id) if rule_id else None
        if not rule or (rule.applied_on == '3_global' and rule.base == 'list_price'
                        and rule.compute_price != 'fixed'):
            return None, None, pricelist, "la lista «%s» no tiene regla para %s" % (
                pricelist.name, product.default_code or product.name)
        return price, rule, pricelist, None

    @api.model
    def _qb_compare_row(self, row, partner, products):
        """Valores del renglón de la comparación para un renglón de PRECIOS."""
        vals = {'client_excel': row['client'], 'code': row['code'], 'excel_row': row['row'],
                'partner_id': partner.id if partner else False}
        basis = 'm' if row['price_m'] else 'kg'
        excel = row['price_m'] if basis == 'm' else row['price_kg']
        vals.update(basis=basis, price_excel=excel)
        if not partner:
            return dict(vals, state='sin_cliente', note="Ligue el cliente en Hojas del forecast.")
        code = row['code']
        # El forecast escribe el código en metros (WJ…) o el de kilos (IWJ…); la «I» al frente es kilos.
        base_code = code[1:] if code.startswith('I') and code[1:] in products else code
        product_m = products.get(base_code)
        product_kg = products.get('I' + base_code) or (products.get(code) if code != base_code else None)
        product = product_m if basis == 'm' else product_kg
        derived = False
        if basis == 'm' and not product and product_kg:
            product, derived = product_kg, True  # se compara el kilo entre el rendimiento
        if not product:
            return dict(vals, state='sin_producto',
                        note="No existe %s en Odoo." % (base_code if basis == 'm' else 'I' + base_code))
        price, rule, pricelist, problem = self._qb_list_price(partner, product)
        vals.update(product_id=product.id, pricelist_id=pricelist.id if pricelist else False,
                    currency_id=pricelist.currency_id.id if pricelist else False)
        if problem:
            return dict(vals, state='sin_regla', note=problem)
        note = False
        if derived:
            m_por_kg = product.qb_m_por_kg or row.get('yield_m_kg')
            if not m_por_kg:
                return dict(vals, state='sin_regla', rule_id=rule.id,
                            note="Solo hay precio por kilo y falta el rendimiento para pasarlo a metro.")
            price = price / m_por_kg
            note = "Precio por kilo de %s entre %.4f m/kg." % (product.default_code, m_por_kg)
        diff = excel - price
        pct = diff / price * 100.0 if price else 0.0
        state = 'igual' if abs(pct) < TOLERANCE_PCT else 'diferente'
        return dict(vals, price_odoo=price, diff=diff, diff_pct=pct, rule_id=rule.id, state=state, note=note)


class QbPriceCheckWizard(models.TransientModel):
    _name = 'qb.price.check.wizard'
    _description = "Comparar precios del forecast contra las listas de Odoo"

    file = fields.Binary(string="Archivo forecast.xlsx", required=True)
    filename = fields.Char(string="Nombre del archivo")

    def _qb_rows(self):
        try:
            import openpyxl
        except ImportError as exc:
            raise UserError("Falta openpyxl en el servidor para leer el Excel.") from exc
        try:
            workbook = openpyxl.load_workbook(io.BytesIO(base64.b64decode(self.file)), data_only=True,
                                              read_only=True)
        except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
            raise UserError("No se pudo abrir el archivo como Excel (.xlsx): %s" % exc) from exc
        sheet = next((ws for ws in workbook.worksheets
                      if forecast_xlsx.norm(ws.title) == forecast_xlsx.PRICE_SHEET), None)
        if not sheet:
            raise UserError("El archivo no tiene la hoja PRECIOS.")
        rows = forecast_xlsx.parse_prices(sheet.iter_rows(values_only=True))
        if not rows:
            raise UserError("La hoja PRECIOS no trae renglones con precio (encabezado con CLIENTE y «precio / m»).")
        return rows

    def _qb_partner(self, client, cache):
        if client in cache:
            return cache[client]
        key = forecast_xlsx.norm(client)
        sheets = self.env['qb.forecast.sheet'].search([('partner_id', '!=', False)])
        partner = next((s.partner_id for s in sheets if forecast_xlsx.norm(s.name) == key), None)
        if not partner:
            partner = self.env['qb.forecast.import']._qb_guess_partner(client, None) or None
        cache[client] = partner.commercial_partner_id if partner else None
        return cache[client]

    def action_compare(self):
        self.ensure_one()
        company = self.env.company
        rows = self._qb_rows()
        codes = set()
        for row in rows:
            code = row['code']
            codes |= {code, 'I' + code, code[1:] if code.startswith('I') else code}
        products = {}
        for product in self.env['product.product'].search([('default_code', 'in', list(codes))]):
            key = (product.default_code or '').upper().replace(' ', '')
            current = products.get(key)
            if not current or (current.qb_conv_origen == 'sin_dato' and product.qb_conv_origen != 'sin_dato'):
                products[key] = product
        Check = self.env['qb.price.check']
        Check.search([('company_id', '=', company.id)]).unlink()
        cache = {}
        Check.create([dict(Check._qb_compare_row(row, self._qb_partner(row['client'], cache), products),
                           company_id=company.id) for row in rows])
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_ventas_presupuesto.qb_price_check_action')
        action['target'] = 'main'
        return action
