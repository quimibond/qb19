# -*- coding: utf-8 -*-
"""Validación de datos maestros: recetas, precios de componentes y pesos.

Un dato dudoso rompe una cotización completa sin que nadie lo note (una
receta con 0.632 kg de colorante por kg de tela en vez de 0.0632 duplicó la
MP de una tela de 300 g). Cada regla deja una fila por producto; mientras
esté abierta el costo del producto es «dudoso», el cotizador lo muestra y el
cierre del período la cuenta en los productos principales.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

REGLAS = [
    ('precio_cero', 'Componente con precio 0 en receta activa'),
    ('precio_fuera_banda', 'Costo promedio lejos de la última compra'),
    ('receta_implausible', 'Receta fuera de rango (colorante / auxiliares)'),
]

ESTADOS = [
    ('abierta', 'Abierta'),
    ('corregida', 'Corregida'),
    ('aceptada', 'Aceptada con motivo'),
]


class QbProductoValidacion(models.Model):
    _name = 'qb.producto.validacion'
    _description = 'Validación de datos del producto'
    _order = 'estado, product_id, regla'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    regla = fields.Selection(REGLAS, required=True)
    componente_id = fields.Many2one(
        'product.product', string='Componente',
        help='El componente de la receta que dispara la regla, si aplica.')
    valor = fields.Float(digits=(16, 4))
    detalle = fields.Char()
    estado = fields.Selection(ESTADOS, default='abierta', required=True)
    motivo = fields.Char(
        help='Por qué se acepta el dato tal cual (obligatorio para aceptar).')
    detectada_el = fields.Datetime(default=fields.Datetime.now, readonly=True)
    resuelta_el = fields.Datetime(readonly=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)

    def action_aceptar(self):
        for rec in self:
            if not (rec.motivo or '').strip():
                raise UserError('Escriba el motivo para aceptar el dato.')
            rec.write({'estado': 'aceptada',
                       'resuelta_el': fields.Datetime.now()})
        return True

    def action_reabrir(self):
        self.write({'estado': 'abierta', 'resuelta_el': False})
        return True

    # ------------------------------------------------------------------
    # Motor
    # ------------------------------------------------------------------
    @api.model
    def revisar(self, products=None):
        """Corre todas las reglas sobre `products` (default: todo producto
        con receta activa). Abre, actualiza o cierra filas; devuelve las
        abiertas tras la corrida.

        Las recetas se explotan: un componente que a su vez tiene receta
        (teñido, crudo, preparación de color) no se valida por su precio,
        porque su costo sale de su receta; se validan las hojas compradas.
        La regla de colorantes mide la preparación de color (categoría
        «Cocina») por kg de tela terminada, que es lo que captura tintorería.
        """
        P = self.env['qb.parametro']
        company = self.env.company
        Bom = self.env['mrp.bom']
        if products is None:
            boms = Bom.search([('active', '=', True),
                               ('company_id', 'in', (False, company.id))])
            products = (boms.mapped('product_id')
                        | boms.mapped('product_tmpl_id.product_variant_ids'))
        hallazgos = {}  # (product_id, regla, componente_id) → (valor, detalle)
        excluir = set(P.get_list('validacion_excluir_codigos', 'AGUA'))
        cat_excluir = [t.upper() for t in
                       P.get_list('validacion_excluir_categorias', 'Maquila')]
        banda = P.get_float('precio_banda_pct', 30.0)
        cat_tokens = [t.upper() for t in
                      P.get_list('validacion_categorias_colorante', 'Cocina')]
        max_linea = P.get_float('colorante_max_pct', 30.0)
        max_total = P.get_float('colorante_total_max_pct', 40.0)
        ultimo_precio = self._ultimo_precio_compra()

        def categoria(prod):
            return (prod.categ_id.complete_name or '').upper()

        def es(prod, tokens):
            return any(t in categoria(prod) for t in tokens)

        for product in products:
            if es(product, cat_excluir) or es(product, cat_tokens):
                continue
            bom = Bom._bom_find(product, company_id=company.id).get(product)
            if not bom:
                continue
            kg_bom = self._kg(bom.product_qty, bom.product_uom_id)
            total_colorante = 0.0
            for line in bom.bom_line_ids:
                comp = line.product_id
                if comp.default_code in excluir or comp.type == 'service':
                    continue
                kg = self._kg(line.product_qty, line.product_uom_id)
                tiene_receta = bool(Bom._bom_find(
                    comp, company_id=company.id).get(comp))
                # Preparación de color por kg de tela.
                if es(comp, cat_tokens) and kg is not None and kg_bom:
                    pct = 100.0 * kg / kg_bom
                    total_colorante += pct
                    if pct > max_linea:
                        hallazgos[(product.id, 'receta_implausible', comp.id)] = (
                            pct, '%s: %.1f %% del peso de la tela (máximo '
                            '%.0f %%)' % (comp.display_name, pct, max_linea))
                if tiene_receta:
                    continue   # su costo sale de su receta, no de su precio
                # precio_cero
                if comp.standard_price == 0 and kg is not None:
                    hallazgos[(product.id, 'precio_cero', comp.id)] = (
                        0.0, '%s cuesta 0 y se consume %.4f'
                        % (comp.display_name, line.product_qty))
                # precio_fuera_banda
                ult = ultimo_precio.get(comp.id)
                if ult and comp.standard_price:
                    desv = abs(comp.standard_price - ult) / ult * 100.0
                    if desv > banda:
                        hallazgos[(product.id, 'precio_fuera_banda', comp.id)] = (
                            desv, '%s: costo promedio %.2f vs última compra '
                            '%.2f (%.0f %%)' % (comp.display_name,
                                                comp.standard_price, ult, desv))
            if total_colorante > max_total:
                hallazgos[(product.id, 'receta_implausible', False)] = (
                    total_colorante, 'Preparaciones de color suman %.1f %% '
                    'del peso de la tela (máximo %.0f %%)'
                    % (total_colorante, max_total))

        return self._aplicar(products, hallazgos)

    def _aplicar(self, products, hallazgos):
        """Concilia los hallazgos con las filas existentes."""
        existentes = self.search([('product_id', 'in', products.ids)])
        por_clave = {(r.product_id.id, r.regla, r.componente_id.id or False): r
                     for r in existentes}
        ahora = fields.Datetime.now()
        abiertas = self.browse()
        for clave, (valor, detalle) in hallazgos.items():
            rec = por_clave.pop(clave, None)
            if rec:
                vals = {'valor': valor, 'detalle': detalle}
                if rec.estado == 'corregida':
                    vals.update(estado='abierta', resuelta_el=False,
                                detectada_el=ahora)
                elif rec.estado == 'aceptada' and rec.valor and \
                        abs(valor - rec.valor) > 0.1 * abs(rec.valor):
                    vals.update(estado='abierta', resuelta_el=False,
                                detectada_el=ahora,
                                motivo='(cambió el valor; se reabre) '
                                       + (rec.motivo or ''))
                rec.write(vals)
            else:
                rec = self.create({
                    'product_id': clave[0], 'regla': clave[1],
                    'componente_id': clave[2] or False,
                    'valor': valor, 'detalle': detalle,
                })
            if rec.estado == 'abierta':
                abiertas |= rec
        # Lo que ya no dispara, se cierra solo.
        for rec in por_clave.values():
            if rec.estado == 'abierta':
                rec.write({'estado': 'corregida', 'resuelta_el': ahora})
        return abiertas

    @api.model
    def _kg(self, qty, uom):
        """Cantidad en kg si la unidad es de peso; None si no."""
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not uom or not kg or not uom._has_common_reference(kg):
            return None
        return uom._compute_quantity(qty, kg, round=False)

    @api.model
    def _ultimo_precio_compra(self):
        """{product_id: precio de la última compra confirmada, en moneda de
        la compañía y en la unidad del producto}. `currency_rate` del pedido
        es compañía → divisa, así que se divide; la unidad de compra se
        convierte a la del producto (un kilo comprado por litro o por caja
        no es un kilo)."""
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT DISTINCT ON (pol.product_id) pol.product_id,
                   pol.price_unit, COALESCE(NULLIF(po.currency_rate, 0), 1),
                   pol.product_uom_id
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done')
              AND po.company_id = %s AND pol.product_qty > 0
            ORDER BY pol.product_id, po.date_approve DESC NULLS LAST, pol.id DESC
        """, (self.env.company.id,))
        filas = self.env.cr.fetchall()
        Uom = self.env['uom.uom']
        Prod = self.env['product.product']
        out = {}
        for pid, precio, rate, uom_id in filas:
            if not precio:
                continue
            precio_mxn = precio / rate
            prod = Prod.browse(pid)
            uom = Uom.browse(uom_id) if uom_id else prod.uom_id
            if uom and prod.uom_id and uom != prod.uom_id and \
                    uom._has_common_reference(prod.uom_id):
                precio_mxn = uom._compute_price(precio_mxn, prod.uom_id)
            out[pid] = precio_mxn
        return out

    @api.model
    def cron_revisar(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).revisar()

    def action_revisar_todo(self):
        self.revisar()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Validaciones abiertas',
            'res_model': 'qb.producto.validacion',
            'view_mode': 'list,form',
            'domain': [('estado', '=', 'abierta')],
        }
