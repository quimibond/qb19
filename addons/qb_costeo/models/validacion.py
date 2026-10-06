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
        abiertas tras la corrida."""
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
        banda = P.get_float('precio_banda_pct', 30.0)
        cat_tokens = [t.upper() for t in
                      P.get_list('validacion_categorias_colorante', 'Cocina')]
        max_linea = P.get_float('colorante_max_pct', 12.0)
        max_total = P.get_float('colorante_total_max_pct', 25.0)

        ultimo_precio = self._ultimo_precio_compra()

        for product in products:
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
                es_colorante = any(t in (comp.categ_id.complete_name or '').upper()
                                   for t in cat_tokens)
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
                # receta_implausible por línea
                if es_colorante and kg is not None and kg_bom:
                    pct = 100.0 * kg / kg_bom
                    total_colorante += pct
                    if pct > max_linea:
                        hallazgos[(product.id, 'receta_implausible', comp.id)] = (
                            pct, '%s: %.1f %% del peso de la tela (máximo '
                            '%.0f %%)' % (comp.display_name, pct, max_linea))
            if total_colorante > max_total:
                hallazgos[(product.id, 'receta_implausible', False)] = (
                    total_colorante, 'Colorantes y auxiliares suman %.1f %% '
                    'del peso (máximo %.0f %%)' % (total_colorante, max_total))

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
        """{product_id: precio unitario de la última compra confirmada, en
        moneda de la compañía}."""
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT DISTINCT ON (pol.product_id) pol.product_id,
                   pol.price_unit * COALESCE(po.currency_rate, 1)
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done')
              AND po.company_id = %s AND pol.product_qty > 0
            ORDER BY pol.product_id, po.date_approve DESC NULLS LAST, pol.id DESC
        """, (self.env.company.id,))
        return {pid: p for pid, p in self.env.cr.fetchall() if p}

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
