# -*- coding: utf-8 -*-
"""Capa de absorción: la conversión que le falta al inventario a fin de mes.

Solo los circulares de Tejido absorben conversión en Odoo (costo por hora en
el centro de trabajo). Tintorería, Rama, Acabado, Inspección y los demás
centros están en $0, así que el costo unitario de Odoo trae la materia prima,
el tejido y lo que haya metido una revaluación, pero no la conversión
completa. El inventario queda por debajo de su costo de producción (IAS 2).

Por producto en existencia al último día del período:

    valor_completo  = existencia × costo de producción del módulo (MP + fabricación)
    faltante        = valor_completo − valor de Odoo
    tope_conversion = existencia × fabricación del módulo
    capitalizable   = faltante acotado entre 0 y el tope

El tope evita que una diferencia de precio de la materia prima (el módulo
explota la receta a precio de compra, Odoo usa el promedio real) se
capitalice como conversión. Si Odoo ya vale más que el módulo, no se
capitaliza nada: la valuación la iguala el cierre de inventario de Odoo.

El total capitalizable es el saldo que debe tener la cuenta de conversión
capitalizada al cierre del mes. El asiento lo registra Contabilidad; aquí
solo se calcula.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.fields import Domain

from .costo import CALIDAD_SEL

ESTADOS = [
    ('capitaliza', 'Se capitaliza'),
    ('cubierto', 'Odoo ya lo cubre'),
    ('sin_costo', 'Sin costo del módulo'),
]


class QbCapaAbsorcion(models.Model):
    _name = 'qb.capa.absorcion'
    _description = 'Capa de absorción por producto'
    _order = 'capitalizable desc, id'
    _rec_name = 'product_id'

    periodo_id = fields.Many2one('qb.periodo', required=True, index=True,
                                 ondelete='cascade')
    company_id = fields.Many2one(related='periodo_id.company_id', store=True)
    product_id = fields.Many2one('product.product', string='Producto',
                                 required=True, index=True)
    categ_id = fields.Many2one('product.category', string='Categoría',
                               index=True)
    cuenta_id = fields.Many2one(
        'account.account', string='Cuenta de inventario',
        help='Cuenta de valuación de la categoría del producto.')
    uom_name = fields.Char(string='Unidad')
    qty = fields.Float(string='Existencia', digits=(16, 4),
                       help='Existencia valuada al último día del período.')
    valor_odoo = fields.Float(
        string='Valor en Odoo', digits=(16, 2),
        help='Valor de la existencia en Odoo al último día del período.')
    costo_odoo_unit = fields.Float(string='Costo Odoo $/u', digits=(16, 4))
    mp_unit = fields.Float(string='MP módulo $/u', digits=(16, 4))
    fabricacion_unit = fields.Float(string='Fabricación módulo $/u',
                                    digits=(16, 4))
    costo_produccion = fields.Float(string='Producción módulo $/u',
                                    digits=(16, 4))
    calidad = fields.Selection(CALIDAD_SEL, string='Calidad del costo')
    valor_completo = fields.Float(
        string='Valor a costo completo', digits=(16, 2),
        help='Existencia × costo de producción del módulo.')
    faltante = fields.Float(
        string='Diferencia contra Odoo', digits=(16, 2),
        help='Valor a costo completo − valor en Odoo. Negativo: Odoo vale '
             'más que el módulo.')
    tope_conversion = fields.Float(
        string='Conversión del módulo', digits=(16, 2),
        help='Existencia × fabricación del módulo: lo más que puede faltar '
             'de conversión.')
    capitalizable = fields.Float(
        string='Conversión a capitalizar', digits=(16, 2),
        help='Diferencia contra Odoo acotada entre 0 y la conversión del '
             'módulo.')
    estado = fields.Selection(ESTADOS, string='Resultado', required=True)

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    @api.model
    def _categorias(self):
        """Categorías que entran: las que empiezan con alguna de
        `capa_categorias`, menos las que contienen alguna de
        `capa_excluir_categorias` (lo comprado no lleva conversión)."""
        P = self.env['qb.parametro']
        incluir = P.get_list('capa_categorias',
                             'Producto Terminado,Producto en Proceso')
        excluir = [t.upper() for t in P.get_list(
            'capa_excluir_categorias', 'Importado,Importación,Subproducto')]
        if not incluir:
            return self.env['product.category']
        dominio = Domain.OR([[('complete_name', '=like', '%s%%' % c)]
                             for c in incluir])
        cats = self.env['product.category'].search(dominio)
        return cats.filtered(lambda c: not any(
            t in (c.complete_name or '').upper() for t in excluir))

    @api.model
    def calcular_periodo(self, periodo):
        """Reescribe las filas del período y devuelve las nuevas."""
        company = periodo.company_id
        self = self.with_company(company).with_context(
            allowed_company_ids=company.ids)
        self.search([('periodo_id', '=', periodo.id)]).unlink()
        ultimo_dia = periodo.date_to - relativedelta(days=1)
        cats = self._categorias()
        products = self.env['product.product'].with_context(
            active_test=False).search([('categ_id', 'in', cats.ids),
                                       ('is_storable', '=', True)])
        if not products:
            return self.browse()
        hasta = company._to_datetime_upper_bound(ultimo_dia)
        con_fecha = products.with_context(to_date=ultimo_dia)
        existencias = products._with_valuation_context().with_context(
            to_date=hasta)
        qty_por_producto = {p.id: p.qty_available for p in existencias}
        costos = {c.product_id.id: c for c in self.env['qb.costo.unitario'].search(
            [('periodo_id', '=', periodo.id),
             ('product_id', 'in', products.ids)])}
        vals_list = []
        for p in con_fecha:
            qty = qty_por_producto.get(p.id, 0.0)
            if p.uom_id.is_zero(qty):
                continue
            valor = p.total_value
            costo = costos.get(p.id)
            mp = costo.mp_unit if costo else 0.0
            fab = costo.fabricacion_unit if costo else 0.0
            produccion = costo.costo_produccion if costo else 0.0
            if not costo or produccion <= 0:
                completo = faltante = tope = capitalizable = 0.0
                estado = 'sin_costo'
            else:
                completo = qty * produccion
                faltante = completo - valor
                tope = max(qty * fab, 0.0)
                capitalizable = min(max(faltante, 0.0), tope)
                estado = 'capitaliza' if capitalizable > 0 else 'cubierto'
            vals_list.append({
                'periodo_id': periodo.id,
                'product_id': p.id,
                'categ_id': p.categ_id.id,
                'cuenta_id': p.categ_id.property_stock_valuation_account_id.id,
                'uom_name': p.uom_id.name,
                'qty': qty,
                'valor_odoo': valor,
                'costo_odoo_unit': valor / qty if qty else 0.0,
                'mp_unit': mp,
                'fabricacion_unit': fab,
                'costo_produccion': produccion,
                'calidad': costo.calidad if costo else False,
                'valor_completo': completo,
                'faltante': faltante,
                'tope_conversion': tope,
                'capitalizable': capitalizable,
                'estado': estado,
            })
        return self.create(vals_list)
