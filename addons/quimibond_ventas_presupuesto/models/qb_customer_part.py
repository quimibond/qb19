# -*- coding: utf-8 -*-
"""Catálogo de partes del cliente: parte del cliente → producto de Quimibond.

Cada release trae la parte con que el cliente conoce el material
(L002790184NCPAA, 4002749, 290530…) en SU unidad (MT, LY, M2). Este catálogo
dice qué producto es y cómo se convierte la cantidad. Una parte sin producto
confirmado detiene la aplicación de esa línea del release (nunca se adivina).

El producto se SUGIERE con ``release/part_match.py`` (Python puro) a partir de
lo que ya está en Odoo: la referencia escrita en la descripción del cliente,
la nota de factura que trae la parte debajo de la línea del producto (así la
escribe hoy Ventas), los pedidos que mencionan la parte o su PO, lo que se le
ha vendido al cliente y el gramaje/ancho de la ficha (``qb.producto.ficha`` si
``qb_capacidad_costeo`` está instalado). Ventas confirma.
"""
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from ..release import part_match, units

_HISTORY_DAYS = 730
_LENGTH_UOM_XMLID = 'uom.product_uom_meter'
_WEIGHT_UOM_XMLID = 'uom.product_uom_kgm'


class QbCustomerPart(models.Model):
    _name = 'qb.customer.part'
    _description = "Parte del cliente → producto"
    _inherit = ['mail.thread']
    _order = 'partner_id, customer_part'
    _rec_name = 'customer_part'

    partner_id = fields.Many2one(
        'res.partner', string="Cliente", required=True, index=True,
        domain="[('is_company', '=', True)]", tracking=True,
        help="Empresa (comercial) del cliente.")
    ship_to_id = fields.Many2one(
        'res.partner', string="Planta / ship-to",
        domain="[('id', 'child_of', partner_id)]",
        help="Vacío = la parte vale para todas las plantas del cliente.")
    customer_part = fields.Char(string="Parte del cliente", required=True,
                                index=True, tracking=True)
    customer_description = fields.Char(string="Descripción del cliente")
    customer_po = fields.Char(string="PO del cliente",
                              help="PO con que el cliente pide esta parte (si la hay).")
    customer_uom = fields.Char(
        string="Unidad del cliente", tracking=True,
        help="Como la escribe el cliente: MT, M, LY, YD, KG, M2…")
    product_id = fields.Many2one('product.product', string="Producto",
                                 index=True, tracking=True)
    conversion = fields.Selection([
        ('auto', "Por unidad (y rendimiento m/kg de la ficha)"),
        ('fija', "Factor fijo"),
    ], string="Conversión", default='auto', required=True, tracking=True)
    factor = fields.Float(
        string="Factor fijo", digits=(16, 6), tracking=True,
        help="Unidades del producto por unidad del cliente. Solo con conversión "
             "'Factor fijo'.")
    state = fields.Selection([
        ('sin_producto', "Sin producto"),
        ('sugerido', "Sugerido"),
        ('confirmado', "Confirmado"),
    ], string="Estado", default='sin_producto', required=True, tracking=True,
        help="Solo una parte confirmada se usa al aplicar un release.")
    match_confidence = fields.Selection([
        ('alta', "Alta"), ('media', "Media"), ('sin_match', "Sin candidato"),
    ], string="Confianza de la sugerencia", readonly=True)
    match_score = fields.Integer(string="Puntaje", readonly=True)
    match_reasons = fields.Text(string="Por qué", readonly=True)
    match_alternatives = fields.Text(string="Otros candidatos", readonly=True)
    confirmed_by_id = fields.Many2one('res.users', string="Confirmó", readonly=True)
    confirmed_date = fields.Datetime(string="Fecha de confirmación", readonly=True)
    company_id = fields.Many2one('res.company', string="Compañía", required=True,
                                 default=lambda self: self.env.company)
    active = fields.Boolean(default=True)

    _part_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, ship_to_id, customer_part)',
        "Esa parte ya está en el catálogo para ese cliente y planta.")

    @api.depends('customer_part', 'partner_id', 'product_id')
    def _compute_display_name(self):
        for part in self:
            name = part.customer_part or ''
            if part.product_id:
                name = "%s → %s" % (name, part.product_id.default_code
                                    or part.product_id.name)
            part.display_name = name

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('product_id') and not vals.get('state'):
                vals['state'] = 'sugerido'
        return super().create(vals_list)

    def write(self, vals):
        # Cambiar el producto o la conversión a mano quita la confirmación:
        # hay que volver a confirmar (salvo que el mismo write confirme).
        if ({'product_id', 'conversion', 'factor', 'customer_uom'} & set(vals)
                and 'state' not in vals):
            has_product = vals['product_id'] if 'product_id' in vals else True
            vals = dict(vals, state='sugerido' if has_product else 'sin_producto')
        return super().write(vals)

    # --- Sugerencia de producto ----------------------------------------------
    def _qb_ficha_by_product(self, products):
        """{product_id: (gramaje, ancho_m, rendimiento_m_kg)} de la ficha."""
        if 'qb.producto.ficha' not in self.env or not products:
            return {}
        fichas = self.env['qb.producto.ficha'].sudo().search_read(
            [('product_id', 'in', products.ids)],
            ['product_id', 'gramaje_g_m2', 'ancho_m', 'rendimiento_m_kg'])
        return {f['product_id'][0]: (f['gramaje_g_m2'], f['ancho_m'],
                                     f['rendimiento_m_kg']) for f in fichas}

    def _qb_candidates(self):
        """Candidatos (dicts para part_match) con la evidencia de Odoo."""
        self.ensure_one()
        SOL = self.env['sale.order.line'].sudo()
        Product = self.env['product.product'].sudo()
        partner = self.partner_id.commercial_partner_id
        since = fields.Datetime.now() - timedelta(days=_HISTORY_DAYS)
        base_domain = [
            ('order_id.state', 'in', ('sale', 'done')),
            ('company_id', '=', self.company_id.id),
            ('product_id', '!=', False),
        ]
        # Lo que se le ha vendido al cliente (pedidos distintos por producto).
        sold = {}
        for sol in SOL.search_read(
                base_domain + [('order_id.partner_id', 'child_of', partner.id),
                               ('order_id.date_order', '>=', since)],
                ['product_id', 'order_id']):
            sold.setdefault(sol['product_id'][0], set()).add(sol['order_id'][0])
        # Pedidos que mencionan la parte (línea o nota).
        part_text = (self.customer_part or '').strip()
        mentions = set()
        if len(part_text) >= 4:
            orders = self.env['sale.order'].sudo().search([
                ('partner_id', 'child_of', partner.id),
                ('company_id', '=', self.company_id.id),
                '|', ('order_line.name', 'ilike', part_text),
                ('note', 'ilike', part_text)])
            mentions = set(orders.order_line.filtered('product_id').product_id.ids)
        # Facturas cuya nota trae la parte: la liga que ya escribe Ventas
        # («IWJ045Q22JNT160 / NÚMERO DE PARTE L002790184NCPAA»). El producto es
        # el de la línea inmediata anterior a la nota.
        invoice_notes = {}
        if len(part_text) >= 4:
            notes = self.env['account.move.line'].sudo().search([
                ('display_type', '=', 'line_note'),
                ('name', 'ilike', part_text),
                ('move_id.move_type', 'in', ('out_invoice', 'out_refund')),
                ('parent_state', '=', 'posted'),
                ('move_id.commercial_partner_id', '=', partner.id),
                ('company_id', '=', self.company_id.id),
            ], limit=500)
            for move in notes.move_id:
                lines = move.invoice_line_ids.sorted(lambda ln: (ln.sequence, ln.id))
                last_product = False
                for line in lines:
                    if line.display_type == 'product' and line.product_id:
                        last_product = line.product_id.id
                    elif line in notes and last_product:
                        invoice_notes[last_product] = invoice_notes.get(last_product, 0) + 1
                        break
        # Pedidos con la PO del cliente.
        with_po = set()
        po = (self.customer_po or '').strip()
        if len(po) >= 3:
            orders = self.env['sale.order'].sudo().search([
                ('partner_id', 'child_of', partner.id),
                ('company_id', '=', self.company_id.id),
                ('state', 'in', ('sale', 'done')),
                ('client_order_ref', 'ilike', po)])
            with_po = set(orders.order_line.filtered('product_id').product_id.ids)
        # Referencias nuestras escritas en la descripción del cliente.
        info = part_match.parse_customer_part(self.customer_part,
                                              self.customer_description)
        embedded = set()
        for code in info['codes']:
            base = part_match.base_code(code)
            embedded |= set(Product.search([
                ('default_code', 'ilike', base), ('sale_ok', '=', True)], limit=10).ids)

        ids = set(sold) | set(invoice_notes) | mentions | with_po | embedded
        products = Product.browse(sorted(ids))
        ficha = self._qb_ficha_by_product(products)
        candidates = []
        for product in products:
            grammage, width_m, _yield = ficha.get(product.id, (0.0, 0.0, 0.0))
            candidates.append({
                'product_id': product.id,
                'default_code': product.default_code or '',
                'grammage': grammage or None,
                'width_m': width_m or None,
                'sold_to_customer': product.id in sold,
                'n_orders': len(sold.get(product.id, ())),
                'invoice_notes': invoice_notes.get(product.id, 0),
                'mentions_part': product.id in mentions,
                'with_po': product.id in with_po,
            })
        return candidates

    def action_suggest_product(self):
        """Sugiere el producto. Nunca pisa una parte confirmada."""
        for part in self:
            if part.state == 'confirmado':
                continue
            ranked = part_match.rank({
                'part': part.customer_part,
                'description': part.customer_description,
                'po': part.customer_po,
            }, part._qb_candidates())
            confidence = part_match.classify(ranked)
            vals = {
                'match_confidence': confidence,
                'match_score': ranked[0]['score'] if ranked else 0,
                'match_reasons': "\n".join(ranked[0]['reasons']) if ranked else
                "Ningún producto con evidencia: captura el producto a mano.",
                'match_alternatives': "\n".join(
                    "%s (%d): %s" % (r['default_code'], r['score'], "; ".join(r['reasons']))
                    for r in ranked[1:4]) or False,
            }
            if confidence != 'sin_match':
                vals.update(product_id=ranked[0]['product_id'], state='sugerido')
            super(QbCustomerPart, part).write(vals)
        return True

    def action_confirm(self):
        for part in self:
            if not part.product_id:
                raise UserError("La parte %s no tiene producto: sugiérelo o "
                                "captúralo antes de confirmar." % part.customer_part)
            if part.conversion == 'fija' and not part.factor:
                raise UserError("La parte %s usa factor fijo pero el factor está "
                                "en 0." % part.customer_part)
        self.write({'state': 'confirmado', 'confirmed_by_id': self.env.user.id,
                    'confirmed_date': fields.Datetime.now()})
        return True

    def action_reset(self):
        self.write({'state': 'sugerido', 'confirmed_by_id': False,
                    'confirmed_date': False})
        return True

    # --- Conversión ------------------------------------------------------------
    def _qb_product_dimension(self):
        """(dimensión, factor a la base) de la unidad de venta del producto."""
        self.ensure_one()
        uom = self.product_id.uom_id
        for xmlid, dim in ((_LENGTH_UOM_XMLID, 'length'),
                           (_WEIGHT_UOM_XMLID, 'weight')):
            ref = self.env.ref(xmlid, raise_if_not_found=False)
            if ref and uom._has_common_reference(ref):
                # Cuántos metros (o kilos) vale una unidad del producto.
                return dim, uom._compute_quantity(1.0, ref, round=False)
        return None, None

    def to_product_qty(self, qty):
        """Cantidad del cliente → unidad de venta del producto, o None si no se
        puede (sin producto, unidad desconocida o sin rendimiento en la ficha)."""
        self.ensure_one()
        if not self.product_id:
            return None
        if self.conversion == 'fija':
            return units.convert(qty, self.customer_uom, None,
                                 fixed_factor=self.factor or None)
        dim, factor = self._qb_product_dimension()
        if not dim:
            return None
        grammage, width_m, yield_m_kg = self._qb_ficha_by_product(
            self.product_id).get(self.product_id.id, (0.0, 0.0, 0.0))
        return units.convert(qty, self.customer_uom, dim, product_factor=factor,
                             yield_m_kg=yield_m_kg or None, width_m=width_m or None)
