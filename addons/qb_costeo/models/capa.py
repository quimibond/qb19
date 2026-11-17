# -*- coding: utf-8 -*-
"""Capa de absorción: la conversión que le falta al inventario a fin de mes.

Solo los circulares de Tejido absorben conversión en Odoo (costo por hora en
el centro de trabajo). Tintorería, Rama, Acabado, Inspección y los demás
centros están en $0, así que el costo unitario de Odoo trae la materia prima,
el tejido y lo que haya metido una revaluación, pero no la conversión
completa. El inventario puede quedar por debajo de su costo de producción
(IAS 2).

Por producto en existencia al último día del período:

    costo_vendible  = costo de producción del módulo ÷ rendimiento
                      (la merma la paga la unidad buena, como en Odoo)
    valor_completo  = existencia × costo_vendible
    faltante        = valor_completo − valor de Odoo
    tope_conversion = existencia × fabricación del módulo ÷ rendimiento

1.11.1: la capa se mide **en neto por categoría**. Dentro de una categoría,
lo que les falta a unos productos se compensa con lo que otros ya traen de
más en Odoo (revaluaciones, costos de Odoo más altos que el estándar):

    capitalizable_categoria = Σ faltante acotado entre 0 y Σ tope_conversion

Así una diferencia de precio de la materia prima no se capitaliza como
conversión, y no se suma solo lo que falta ignorando lo que sobra.

Los productos sin fabricación del módulo (no tiene horas para ellos: hoy los
no tejidos) no se pueden medir y no entran al neto: se listan con su valor
en Odoo para saber cuánto inventario quedó sin medir.

El total capitalizable es el saldo que debe tener la cuenta de conversión
capitalizada al cierre del mes. El asiento lo registra Contabilidad; aquí
solo se calcula.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.fields import Domain

from .costo import CALIDAD_SEL

ESTADOS = [
    ('falta', 'Le falta conversión'),
    ('cubierto', 'Odoo ya lo cubre'),
    ('sin_fabricacion', 'Sin fabricación del módulo'),
    ('sin_costo', 'Sin costo del módulo'),
]
MEDIBLES = ('falta', 'cubierto')


class QbCapaAbsorcion(models.Model):
    _name = 'qb.capa.absorcion'
    _description = 'Capa de absorción por producto'
    _order = 'faltante desc, id'
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
    rendimiento = fields.Float(string='Rendimiento', digits=(6, 4))
    costo_vendible = fields.Float(
        string='Vendible módulo $/u', digits=(16, 4),
        help='Producción ÷ rendimiento: lo que cuesta la unidad buena, con su '
             'merma.')
    calidad = fields.Selection(CALIDAD_SEL, string='Calidad del costo')
    valor_completo = fields.Float(
        string='Valor a costo completo', digits=(16, 2),
        help='Existencia × costo vendible del módulo.')
    faltante = fields.Float(
        string='Diferencia contra Odoo', digits=(16, 2),
        help='Valor a costo completo − valor en Odoo. Negativo: Odoo vale '
             'más que el módulo.')
    tope_conversion = fields.Float(
        string='Conversión del módulo', digits=(16, 2),
        help='Existencia × fabricación del módulo ÷ rendimiento: lo más que '
             'puede faltar de conversión.')
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
        """Reescribe las filas del período (producto y resumen por
        categoría) y devuelve las filas por producto."""
        company = periodo.company_id
        self = self.with_company(company).with_context(
            allowed_company_ids=company.ids)
        self.search([('periodo_id', '=', periodo.id)]).unlink()
        Resumen = self.env['qb.capa.absorcion.categoria']
        Resumen.search([('periodo_id', '=', periodo.id)]).unlink()
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
            rend = (costo.rendimiento or 1.0) if costo else 1.0
            rend = min(max(rend, 0.05), 1.0)
            vendible = (costo.costo_vendible or produccion / rend) if costo else 0.0
            completo = faltante = tope = 0.0
            if not costo or produccion <= 0:
                estado = 'sin_costo'
            elif fab <= 0:
                estado = 'sin_fabricacion'
            else:
                completo = qty * vendible
                faltante = completo - valor
                tope = max(qty * fab / rend, 0.0)
                estado = 'falta' if faltante > 0 else 'cubierto'
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
                'rendimiento': rend,
                'costo_vendible': vendible,
                'calidad': costo.calidad if costo else False,
                'valor_completo': completo,
                'faltante': faltante,
                'tope_conversion': tope,
                'estado': estado,
            })
        filas = self.create(vals_list)
        Resumen.resumir(periodo, filas)
        return filas


class QbCapaAbsorcionCategoria(models.Model):
    """1.11.1: la capa en neto por categoría. Es el número que se capitaliza."""
    _name = 'qb.capa.absorcion.categoria'
    _description = 'Capa de absorción por categoría'
    _order = 'capitalizable desc, id'
    _rec_name = 'categ_id'

    periodo_id = fields.Many2one('qb.periodo', required=True, index=True,
                                 ondelete='cascade')
    company_id = fields.Many2one(related='periodo_id.company_id', store=True)
    categ_id = fields.Many2one('product.category', string='Categoría',
                               required=True)
    cuenta_id = fields.Many2one('account.account',
                                string='Cuenta de inventario')
    n_medidos = fields.Integer(string='Productos medidos')
    n_sin_medir = fields.Integer(string='Productos sin medir')
    valor_odoo = fields.Float(
        string='Valor en Odoo (medido)', digits=(16, 2),
        help='Valor en Odoo de los productos que el módulo puede medir.')
    valor_completo = fields.Float(string='Valor a costo completo',
                                  digits=(16, 2))
    faltante_neto = fields.Float(
        string='Diferencia neta', digits=(16, 2),
        help='Σ (valor a costo completo − valor en Odoo) de los productos '
             'medidos. Negativo: en conjunto Odoo ya vale más.')
    tope_conversion = fields.Float(string='Conversión del módulo',
                                   digits=(16, 2))
    capitalizable = fields.Float(
        string='Conversión a capitalizar', digits=(16, 2),
        help='Diferencia neta acotada entre 0 y la conversión del módulo.')
    valor_sin_medir = fields.Float(
        string='Valor en Odoo sin medir', digits=(16, 2),
        help='Productos sin costo o sin fabricación del módulo.')

    @api.model
    def resumir(self, periodo, filas):
        por_cat = {}
        for f in filas:
            por_cat.setdefault(f.categ_id, self.env['qb.capa.absorcion'])
            por_cat[f.categ_id] |= f
        vals_list = []
        for categ, lineas in por_cat.items():
            medidas = lineas.filtered(lambda f: f.estado in MEDIBLES)
            sin_medir = lineas - medidas
            neto = sum(medidas.mapped('faltante'))
            tope = sum(medidas.mapped('tope_conversion'))
            vals_list.append({
                'periodo_id': periodo.id,
                'categ_id': categ.id,
                'cuenta_id': categ.property_stock_valuation_account_id.id,
                'n_medidos': len(medidas),
                'n_sin_medir': len(sin_medir),
                'valor_odoo': sum(medidas.mapped('valor_odoo')),
                'valor_completo': sum(medidas.mapped('valor_completo')),
                'faltante_neto': neto,
                'tope_conversion': tope,
                'capitalizable': min(max(neto, 0.0), tope),
                'valor_sin_medir': sum(sin_medir.mapped('valor_odoo')),
            })
        return self.create(vals_list)
