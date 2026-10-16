# -*- coding: utf-8 -*-
"""Rentabilidad por cliente y por producto, 12 meses (1.6.0).

Sustituyen a `qb.cliente.rentabilidad` y `qb.producto.rentabilidad` del
módulo anterior (`qb_capacidad_costeo`) con el costo de este módulo:

    contribución = facturado − cantidad × costo variable
    margen bruto = facturado − cantidad × costo vendible (producción ÷ rendimiento)
    margen neto  = margen bruto − facturado × op %

Las facturas son las mismas que lee el costeo (`ventas_por_producto`):
clientes, publicadas, cuentas de ingreso, compañía activa, cantidad sin el
triplete lista/descuento/neta y el importe en pesos (`balance`). Cada mes
usa el costo de su período si ese mes se costeó; si no, el del último
período costeado del producto (`mismo_mes_pct` dice cuánto de la venta
llevó el costo de su mes). Lo vendido sin costo no tiene margen:
`cobertura_pct` lo mide y, abajo de 80 %, el semáforo dice «sin costo» en
vez de inventar un verde.

Vistas SQL de solo lectura: se leen en vivo, no hay nada que recalcular.
Sin caracteres de porcentaje en el SQL (`_table_query` pasa por formateo).
"""
from odoo import api, fields, models

from .costo import QTY_DEDUP_SQL

# Cobertura mínima de costo para dar semáforo (debajo: «sin costo»).
COBERTURA_MIN = 80.0
SEMAFORO_SEL = [('rojo', 'Pierde'), ('ambar', 'Apenas'), ('verde', 'Sano'),
                ('sin_costo', 'Sin costo suficiente')]


def _base_sql(company_id):
    """CTE `joined`: una fila por cliente × producto × mes con venta,
    cantidad y el costo que le toca."""
    return """
        WITH lines AS (
            SELECT am.commercial_partner_id AS partner_id, aml.product_id,
                   date_trunc('month', am.invoice_date)::date AS mes,
                   am.invoice_date, aml.move_id, aml.quantity, aml.balance,
                   am.move_type, am.currency_id
            FROM account_move_line aml
            JOIN account_move am ON am.id = aml.move_id
            JOIN account_account aa ON aa.id = aml.account_id
            WHERE am.move_type IN ('out_invoice', 'out_refund')
              AND am.state = 'posted'
              AND aml.display_type = 'product'
              AND aml.product_id IS NOT NULL
              AND aa.account_type = 'income'
              AND aml.company_id = {cid}
              AND am.invoice_date >= (date_trunc('month', CURRENT_DATE)
                                      - INTERVAL '12 months')
        ),
        qty_dedup AS (
            SELECT d.*, l2.partner_id, l2.mes, l2.invoice_date
            FROM ({dedup}) d
            JOIN LATERAL (
                SELECT partner_id, mes, invoice_date FROM lines l
                WHERE l.move_id = d.move_id LIMIT 1) l2 ON TRUE
        ),
        qty AS (
            SELECT partner_id, product_id, mes,
                   SUM(CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END) AS qty,
                   MAX(invoice_date) AS ultima
            FROM qty_dedup GROUP BY 1, 2, 3
        ),
        revenue AS (
            SELECT partner_id, product_id, mes, SUM(-balance) AS rev
            FROM lines GROUP BY 1, 2, 3
        ),
        costos AS (
            SELECT cu.product_id, cu.period, cu.costo_variable,
                   cu.costo_vendible, cu.op_pct, cu.calidad
            FROM qb_costo_unitario cu
            WHERE cu.company_id = {cid} AND cu.costo_vendible > 0
        ),
        ultimo AS (
            SELECT DISTINCT ON (product_id) *
            FROM costos ORDER BY product_id, period DESC
        ),
        joined AS (
            SELECT r.partner_id, r.product_id, r.mes, r.rev,
                   COALESCE(q.qty, 0) AS qty, q.ultima,
                   COALESCE(c.costo_variable, u.costo_variable) AS costo_variable,
                   COALESCE(c.costo_vendible, u.costo_vendible) AS costo_vendible,
                   COALESCE(c.op_pct, u.op_pct, 0) AS op_pct,
                   COALESCE(c.calidad, u.calidad) AS calidad,
                   (c.product_id IS NOT NULL) AS mismo_mes
            FROM revenue r
            LEFT JOIN qty q ON q.partner_id = r.partner_id
                 AND q.product_id = r.product_id AND q.mes = r.mes
            LEFT JOIN costos c ON c.product_id = r.product_id AND c.period = r.mes
            LEFT JOIN ultimo u ON u.product_id = r.product_id
        )
    """.format(cid=int(company_id), dedup=QTY_DEDUP_SQL)


# Agregados comunes (sobre `joined j`): solo cuentan en el margen las filas
# con costo; la venta sin costo baja la cobertura.
_AGREGADOS = """
    SUM(j.rev) AS revenue_12m,
    SUM(j.qty) AS qty_12m,
    SUM(CASE WHEN j.costo_vendible IS NOT NULL
             THEN j.rev - j.qty * j.costo_variable END) AS contrib_12m,
    SUM(CASE WHEN j.costo_vendible IS NOT NULL
             THEN j.rev - j.qty * j.costo_vendible END) AS margen_bruto_12m,
    SUM(CASE WHEN j.costo_vendible IS NOT NULL
             THEN j.rev * (1 - j.op_pct) - j.qty * j.costo_vendible END) AS margen_neto_12m,
    SUM(CASE WHEN j.costo_vendible IS NOT NULL THEN j.rev END) AS rev_con_costo,
    SUM(CASE WHEN j.mismo_mes THEN j.rev END) AS rev_mismo_mes,
    SUM(CASE WHEN j.calidad = 'alta' THEN j.rev END) AS rev_alta,
    COUNT(DISTINCT j.mes) AS meses_activo
"""

_PCTS = """
    t.revenue_12m, t.qty_12m,
    COALESCE(t.contrib_12m, 0) AS contrib_12m,
    COALESCE(t.margen_bruto_12m, 0) AS margen_bruto_12m,
    COALESCE(t.margen_neto_12m, 0) AS margen_neto_12m,
    CASE WHEN t.rev_con_costo > 0 THEN 100.0 * t.contrib_12m / t.rev_con_costo
         ELSE 0 END AS contrib_pct,
    CASE WHEN t.rev_con_costo > 0 THEN 100.0 * t.margen_bruto_12m / t.rev_con_costo
         ELSE 0 END AS margen_bruto_pct,
    CASE WHEN t.rev_con_costo > 0 THEN 100.0 * t.margen_neto_12m / t.rev_con_costo
         ELSE 0 END AS margen_neto_pct,
    CASE WHEN t.revenue_12m > 0 THEN 100.0 * COALESCE(t.rev_con_costo, 0) / t.revenue_12m
         ELSE 0 END AS cobertura_pct,
    CASE WHEN t.revenue_12m > 0 THEN 100.0 * COALESCE(t.rev_mismo_mes, 0) / t.revenue_12m
         ELSE 0 END AS mismo_mes_pct,
    CASE WHEN t.revenue_12m > 0 THEN 100.0 * COALESCE(t.rev_alta, 0) / t.revenue_12m
         ELSE 0 END AS calidad_alta_pct,
    CASE WHEN t.revenue_12m <= 0
              OR 100.0 * COALESCE(t.rev_con_costo, 0) / t.revenue_12m < {cob}
              THEN 'sin_costo'
         WHEN t.margen_neto_12m < 0 THEN 'rojo'
         WHEN 100.0 * t.margen_neto_12m / t.rev_con_costo < 5 THEN 'ambar'
         ELSE 'verde' END AS semaforo,
    t.meses_activo
""".format(cob=COBERTURA_MIN)


class QbRentabilidadMixin(models.AbstractModel):
    _name = 'qb.rentabilidad.mixin'
    _description = 'Campos comunes de rentabilidad (12 meses)'

    company_id = fields.Many2one('res.company', readonly=True)
    company_currency_id = fields.Many2one(
        'res.currency', related='company_id.currency_id', string='Moneda')
    revenue_12m = fields.Monetary(string='Ventas 12m', readonly=True,
                                  currency_field='company_currency_id')
    qty_12m = fields.Float(string='Volumen 12m', readonly=True, digits=(16, 2))
    contrib_12m = fields.Monetary(
        string='Contribución 12m', readonly=True, currency_field='company_currency_id',
        help='Facturado − cantidad × costo variable (MP + energía), solo lo vendido con costo.')
    contrib_pct = fields.Float(string='Contribución %', readonly=True, digits=(6, 1))
    margen_bruto_12m = fields.Monetary(
        string='Margen bruto 12m', readonly=True, currency_field='company_currency_id',
        help='Facturado − cantidad × costo vendible (producción ÷ rendimiento: la merma ya pagada).')
    margen_bruto_pct = fields.Float(string='Margen bruto %', readonly=True, digits=(6, 1))
    margen_neto_12m = fields.Monetary(
        string='Margen neto 12m', readonly=True, currency_field='company_currency_id',
        help='Margen bruto − facturado × op % del período (administración y ventas).')
    margen_neto_pct = fields.Float(string='Margen neto %', readonly=True, digits=(6, 1))
    cobertura_pct = fields.Float(
        string='Cobertura de costo %', readonly=True, digits=(6, 1),
        help='Parte de la venta de productos con costo calculado. Los márgenes solo cuentan esa parte; '
             'abajo de 80 % el semáforo dice «sin costo».')
    mismo_mes_pct = fields.Float(
        string='Costo del mismo mes %', readonly=True, digits=(6, 1),
        help='Parte de la venta que llevó el costo del período de su mes. El resto lleva el del último '
             'período costeado del producto.')
    calidad_alta_pct = fields.Float(
        string='Costo de calidad alta %', readonly=True, digits=(6, 1),
        help='Parte de la venta cuyo costo es de calidad alta (horas medidas o estándar).')
    semaforo = fields.Selection(SEMAFORO_SEL, readonly=True)
    meses_activo = fields.Integer(string='Meses con venta', readonly=True)
    veredicto = fields.Char(compute='_compute_veredicto')

    @api.depends('semaforo', 'margen_neto_pct', 'margen_neto_12m', 'contrib_pct', 'cobertura_pct')
    def _compute_veredicto(self):
        for rec in self:
            if rec.semaforo == 'sin_costo':
                rec.veredicto = 'Solo %.0f %% de la venta tiene costo calculado: sin veredicto.' % rec.cobertura_pct
            elif rec.semaforo == 'rojo':
                rec.veredicto = ('Pierde $%.2f de cada $100 vendidos (%s en 12 meses) después de todos los costos; '
                                 'contribución %.0f %%.' % (-rec.margen_neto_pct, '{:,.0f}'.format(
                                     rec.margen_neto_12m), rec.contrib_pct))
            elif rec.semaforo == 'ambar':
                rec.veredicto = 'Deja %.1f %% neto: cubre costos con margen mínimo.' % rec.margen_neto_pct
            else:
                rec.veredicto = 'Deja %.1f %% neto (%s en 12 meses).' % (
                    rec.margen_neto_pct, '{:,.0f}'.format(rec.margen_neto_12m))


class QbRentabilidadCliente(models.Model):
    _name = 'qb.rentabilidad.cliente'
    _inherit = ['qb.rentabilidad.mixin', 'qb.costeo.vista.sql']
    _description = 'Rentabilidad por cliente (12 meses)'
    _auto = False
    _order = 'margen_neto_12m'
    _rec_name = 'partner_id'

    partner_id = fields.Many2one('res.partner', string='Cliente', readonly=True)
    n_productos = fields.Integer(string='Productos distintos', readonly=True)
    ultima_compra = fields.Date(string='Última compra', readonly=True)

    @property
    def _table_query(self):
        cid = self.env.company.id
        return _base_sql(cid) + """
            SELECT t.partner_id AS id, t.partner_id, {cid} AS company_id,
                   t.n_productos, t.ultima_compra, {pcts}
            FROM (
                SELECT j.partner_id, {agg},
                       COUNT(DISTINCT j.product_id) AS n_productos,
                       MAX(j.ultima) AS ultima_compra
                FROM joined j GROUP BY j.partner_id
            ) t
        """.format(cid=int(cid), agg=_AGREGADOS, pcts=_PCTS)


class QbRentabilidadProducto(models.Model):
    _name = 'qb.rentabilidad.producto'
    _inherit = ['qb.rentabilidad.mixin', 'qb.costeo.vista.sql']
    _description = 'Rentabilidad por producto (12 meses)'
    _auto = False
    _order = 'margen_neto_12m'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', string='Producto', readonly=True)
    precio_prom = fields.Float(string='Precio prom. $/u', readonly=True, digits=(16, 4),
                               help='Ventas ÷ volumen: lo realmente cobrado, todos los clientes.')
    n_clientes = fields.Integer(string='Clientes distintos', readonly=True)
    ultima_venta = fields.Date(string='Última venta', readonly=True)

    @property
    def _table_query(self):
        cid = self.env.company.id
        return _base_sql(cid) + """
            SELECT t.product_id AS id, t.product_id, {cid} AS company_id,
                   CASE WHEN t.qty_12m > 0 THEN t.revenue_12m / t.qty_12m ELSE 0 END AS precio_prom,
                   t.n_clientes, t.ultima_venta, {pcts}
            FROM (
                SELECT j.product_id, {agg},
                       COUNT(DISTINCT j.partner_id) AS n_clientes,
                       MAX(j.ultima) AS ultima_venta
                FROM joined j GROUP BY j.product_id
            ) t
        """.format(cid=int(cid), agg=_AGREGADOS, pcts=_PCTS)
