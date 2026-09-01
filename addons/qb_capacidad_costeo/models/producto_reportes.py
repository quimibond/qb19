# -*- coding: utf-8 -*-
"""Reportes por producto (vistas SQL read-only, 12 meses) + auditoría de pesos.

Tres preguntas que la rentabilidad por cliente no responde:

- qb.producto.rentabilidad — "¿qué producto me deja y cuál me cuesta?":
  márgenes reales por producto (facturado vs costo del período).
- qb.producto.cliente — "¿a quién le vendo cada producto y a qué precio?":
  la matriz producto × cliente con la dispersión de precio (quién compra
  barato contra el promedio del producto).
- qb.producto.mensual — "¿cómo corre el programa?": volúmenes y márgenes
  por producto por mes; en pivot (meses en columnas) es el programa anual.

Mismo esqueleto probado que qb.cliente.rentabilidad: líneas de factura
(dedup del triplete) × qb.costo.producto del MISMO período, solo cuentas
income, solo la compañía activa. Ningún caracter de porcentaje en el SQL
(_table_query pasa por formateo estilo printf).

qb.peso.auditoria es tabla normal regenerable: compara el peso que usa el
motor contra el teórico de la nomenclatura y marca qué revisar — el
verificador que faltaba cuando la familia m² traía +50 por ciento de peso.
"""
import re

from odoo import api, fields, models
from odoo.tools import html_escape

# Formato compartido de las fichas 360 y el panel. El signo va ANTES del
# símbolo (−$947,106, no $-947,106) y los meses en español — strftime usa
# el locale C del servidor y pintaba «Aug 2025» en un reporte en español.
MESES_ES = ('ene', 'feb', 'mar', 'abr', 'may', 'jun',
            'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


def money(v, dec=0):
    return ('-$' if v < 0 else '$') + '{:,.{d}f}'.format(abs(v), d=dec)


def mes_es(d):
    return '%s %s' % (MESES_ES[d.month - 1], d.year)


def fecha_es(d):
    if not d:
        return ''
    return '%s %s %s' % (d.day, MESES_ES[d.month - 1], d.year)


# CTEs compartidos por los tres reportes: líneas income deduplicadas con
# revenue en MXN (balance) y qty neta, cruzadas con el costo del período.
_BASE_SQL = """
    WITH lines AS (
        SELECT am.commercial_partner_id AS partner_id,
               aml.product_id, aml.id AS line_id,
               date_trunc('month', am.invoice_date)::date AS mes,
               am.invoice_date,
               aml.move_id, aml.quantity, aml.balance,
               am.move_type, aml.company_id
        FROM account_move_line aml
        JOIN account_move am ON am.id = aml.move_id
        JOIN account_account aa ON aa.id = aml.account_id
        WHERE am.move_type IN ('out_invoice', 'out_refund')
          AND am.state = 'posted'
          AND aml.display_type = 'product'
          AND aa.account_type = 'income'
          AND aml.product_id IS NOT NULL
          AND aml.company_id = {company_id}
          AND am.invoice_date >= (date_trunc('month', CURRENT_DATE)
                                  - INTERVAL '12 months')
    ),
    qty_dedup AS (
        SELECT DISTINCT ON (move_id, product_id, ABS(quantity))
               partner_id, product_id, mes, invoice_date, move_id, line_id,
               CASE WHEN move_type = 'out_refund'
                    THEN -quantity ELSE quantity END AS qty,
               company_id
        FROM lines
        ORDER BY move_id, product_id, ABS(quantity)
    ),
    revenue AS (
        SELECT partner_id, product_id, mes, SUM(-balance) AS rev
        FROM lines GROUP BY 1, 2, 3
    ),
    qty AS (
        SELECT partner_id, product_id, mes, company_id,
               SUM(qty) AS qty, MAX(invoice_date) AS ultima,
               MIN(line_id) AS line_id
        FROM qty_dedup GROUP BY 1, 2, 3, 4
    ),
    joined AS (
        SELECT q.partner_id, q.product_id, q.mes, q.qty, q.company_id,
               q.ultima, q.line_id, r.rev,
               cp.costo_variable, cp.fab_unit,
               COALESCE(NULLIF(cp.rendimiento, 0), 1) AS rendimiento,
               COALESCE(f.op_pct, 0) AS op_pct,
               CASE WHEN cp.contrib_hora_maquina > 0
                    THEN cp.margen_contribucion / cp.contrib_hora_maquina
                    ELSE 0 END AS horas_por_unidad
        FROM qty q
        JOIN revenue r ON r.partner_id = q.partner_id
                     AND r.product_id = q.product_id AND r.mes = q.mes
        LEFT JOIN qb_costo_producto cp
               ON cp.product_id = q.product_id AND cp.period = q.mes
              AND cp.company_id = {company_id}
        LEFT JOIN qb_costo_factores f ON f.id = cp.factores_id
    )
"""


class QbProductoRentabilidad(models.Model):
    _name = 'qb.producto.rentabilidad'
    _inherit = 'qb.sql.view'
    _description = 'Rentabilidad por producto (12 meses)'
    _auto = False
    _order = 'revenue_12m DESC'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', readonly=True,
                                 string='Producto')
    revenue_12m = fields.Float(string='Ventas 12m (MXN)', readonly=True)
    qty_12m = fields.Float(string='Volumen 12m', readonly=True)
    precio_prom = fields.Float(
        string='Precio prom $/u', readonly=True,
        help='Ventas ÷ volumen: el precio realmente cobrado, todas las '
             'facturas y clientes.')
    costo_unit = fields.Float(
        string='Costo $/u', readonly=True,
        help='Costo completo promedio por unidad en la ventana: variable + '
             'fabricación + operación (op = porcentaje del período aplicado '
             'al precio real de cada mes). Comparable directo contra el '
             'precio promedio: la resta es el margen neto por unidad.')
    contrib_12m = fields.Float(string='Contribución 12m (MXN)', readonly=True)
    contrib_pct = fields.Float(string='Contribución %', readonly=True)
    margen_bruto_12m = fields.Float(string='Margen bruto 12m (MXN)',
                                    readonly=True)
    margen_bruto_pct = fields.Float(string='Margen bruto %', readonly=True)
    margen_neto_12m = fields.Float(
        string='Margen neto 12m (MXN)', readonly=True,
        help='Bruto − operación (op del período × facturado). Lo que el '
             'producto deja después de TODOS los costos asignables.')
    margen_neto_pct = fields.Float(string='Margen neto %', readonly=True)
    margen_neto_real_12m = fields.Float(
        string='Margen real 12m (MXN)', readonly=True,
        help='Margen neto con el costo GROSS-UP por rendimiento: el metro '
             'de primera paga también la fracción que salió FE/segunda/'
             'desperdicio. El que manda para decidir precios (A7).')
    margen_neto_real_pct = fields.Float(
        string='Margen real %', readonly=True)
    rendimiento_pct = fields.Float(
        string='Rendimiento %', readonly=True,
        help='Promedio ponderado por volumen del rendimiento vendible de '
             'los meses de la ventana. 100 = sin merma clasificada.')
    costo_cobertura_pct = fields.Float(
        string='Cobertura de costo %', readonly=True,
        help='Parte de las ventas cuyo mes SÍ tenía costo calculado. '
             '<100 = contribución inflada por costo cero.')
    horas_cuello_12m = fields.Float(string='Horas-máquina 12m', readonly=True)
    contrib_por_hora = fields.Float(string='Contribución $/hora',
                                    readonly=True)
    n_clientes = fields.Integer(string='Clientes distintos', readonly=True)
    meses_activo = fields.Integer(string='Meses con venta', readonly=True)
    ultima_venta = fields.Date(string='Última venta', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)
    company_currency_id = fields.Many2one(
        'res.currency', related='company_id.currency_id', string='Moneda')

    # ------------------------------------------------------------------
    # Situación completa del producto: veredicto en semáforo y pestañas
    # calculadas al abrir — la misma gramática visual que la ficha del
    # cliente (el juicio primero, cada número al lado).
    # ------------------------------------------------------------------
    semaforo = fields.Selection(
        [('rojo', 'Pierde'), ('ambar', 'Apenas'), ('verde', 'Sano')],
        compute='_compute_semaforo', string='Semáforo')
    veredicto = fields.Char(compute='_compute_semaforo')

    _SEM = {'rojo': '🔴', 'ambar': '🟡', 'verde': '🟢'}

    @api.depends('margen_neto_real_pct', 'margen_neto_real_12m',
                 'margen_neto_pct', 'precio_prom', 'contrib_pct',
                 'rendimiento_pct')
    def _compute_semaforo(self):
        # A7 (memo 31-ago): el semáforo usa el margen REAL — con la merma
        # pagada. El caso WC090: verde con "10.3% neto" que con su 10.5%
        # de no conforme era cero o negativo, y se estaba usando para
        # decidir precios.
        for rec in self:
            pct = rec.margen_neto_real_pct
            merma_nota = ''
            if rec.rendimiento_pct and rec.rendimiento_pct < 99.5:
                merma_nota = (' (ya pagada la merma: rendimiento '
                              '{r:.1f}%)'.format(r=rec.rendimiento_pct))
            if pct < 0:
                rec.semaforo = 'rojo'
                rec.veredicto = (
                    'A {precio} por unidad promedio, pierde ${p:.2f} de '
                    'cada $100 vendidos ({neto} en 12 meses) después de '
                    'todos los costos{m}. Contribución {c:.0f}%: el precio '
                    'no cubre lo que absorbe.'.format(
                        precio=money(rec.precio_prom, 2), p=-pct,
                        neto=money(rec.margen_neto_real_12m),
                        m=merma_nota, c=rec.contrib_pct))
            elif pct < 5:
                rec.semaforo = 'ambar'
                rec.veredicto = (
                    'Deja {p:.1f}% neto real a {precio} promedio{m} — al '
                    'filo: una subida de hilo lo manda a rojo.'.format(
                        p=pct, precio=money(rec.precio_prom, 2),
                        m=merma_nota))
            else:
                rec.semaforo = 'verde'
                rec.veredicto = (
                    'Deja {p:.1f}% neto real ({neto} en 12 meses) a '
                    '{precio} promedio{m}.'.format(
                        p=pct, neto=money(rec.margen_neto_real_12m),
                        precio=money(rec.precio_prom, 2), m=merma_nota))

    clientes_html = fields.Html(
        compute='_compute_clientes_html', sanitize=False,
        string='Quién lo compra')
    tendencia_html = fields.Html(
        compute='_compute_tendencia_html', sanitize=False,
        string='Tendencia 12 meses')
    cotizaciones_html = fields.Html(
        compute='_compute_cotizaciones_html', sanitize=False,
        string='Cotizaciones')

    def _compute_clientes_html(self):
        Pareja = self.env['qb.producto.cliente']
        for rec in self:
            filas = Pareja.search([('product_id', '=', rec.product_id.id)])
            if not filas:
                rec.clientes_html = ('<p class="text-muted">Sin ventas en '
                                     'los últimos 12 meses.</p>')
                continue
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Cliente</th>'
                    '<th class="text-end">Volumen</th>'
                    '<th class="text-end">Precio $/u</th>'
                    '<th class="text-end">Δ vs promedio</th>'
                    '<th class="text-end">Margen neto</th>'
                    '<th class="text-end">Neto</th>'
                    '<th>Última</th></tr></thead><tbody>')
            for f in filas:
                sem = ('rojo' if f.margen_neto_pct < 0
                       else 'ambar' if f.margen_neto_pct < 5 else 'verde')
                if len(filas) <= 1:
                    # Δ contra el promedio de UN solo comprador es 0 por
                    # construcción — decir «+0.0» confunde; el dato útil es
                    # que no hay mercado interno contra qué comparar.
                    delta = '<span class="text-muted">único comprador</span>'
                elif f.delta_precio_pct < -10:
                    delta = ('<span style="color:#b02a37;font-weight:bold;">'
                             '{:+.1f}&#37;</span>').format(f.delta_precio_pct)
                else:
                    delta = '{:+.1f}&#37;'.format(f.delta_precio_pct)
                celdas = (
                    self._SEM[sem] + ' '
                    + html_escape(f.partner_id.name or ''),
                    '{:,.0f}'.format(f.qty_12m),
                    money(f.precio_prom, 2),
                    delta,
                    money(f.margen_neto_12m),
                    '{:.1f}&#37;'.format(f.margen_neto_pct),
                    fecha_es(f.ultima_compra),
                )
                html += ('<tr><td>%s</td><td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td>%s</td></tr>' % celdas)
            html += ('</tbody></table><p style="font-size:11px;" '
                     'class="text-muted">Δ en rojo = ese cliente paga este '
                     'producto más de 10 abajo del promedio de todos — el '
                     'candidato a renegociar.</p>')
            rec.clientes_html = html

    def _compute_tendencia_html(self):
        company_id = int(self.env.company.id)
        for rec in self:
            self.env.cr.execute(
                _BASE_SQL.format(company_id=company_id) + """
                SELECT j.mes, SUM(j.qty) AS qty, SUM(j.rev) AS rev,
                       SUM(j.rev * (1 - j.op_pct)
                           - j.qty * (COALESCE(j.costo_variable, 0)
                                      + COALESCE(j.fab_unit, 0))) AS neto
                FROM joined j
                WHERE j.product_id = %s
                GROUP BY j.mes ORDER BY j.mes
                """, (rec.product_id.id,))
            meses = self.env.cr.fetchall()
            if not meses:
                rec.tendencia_html = ('<p class="text-muted">Sin ventas en '
                                      'la ventana.</p>')
                continue
            tope = max(abs(r[2]) for r in meses) or 1.0
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Mes</th>'
                    '<th class="text-end">Volumen</th>'
                    '<th class="text-end">Precio $/u</th>'
                    '<th class="text-end">Ventas</th><th style="width:25%">'
                    '</th><th class="text-end">Neto sobre venta</th></tr>'
                    '</thead><tbody>')
            for mes, qty, rev, neto in meses:
                ancho = 100.0 * abs(rev) / tope
                precio = rev / qty if qty else 0.0
                pct = 100.0 * neto / rev if rev else 0.0
                color = ('#b02a37' if pct < 0
                         else '#997404' if pct < 5 else '#146c43')
                celdas = (
                    mes_es(mes),
                    '{:,.0f}'.format(qty),
                    money(precio, 2),
                    money(rev),
                    '{:.0f}'.format(ancho),
                    color,
                    '{:+.1f}'.format(pct),
                )
                html += ('<tr><td>%s</td><td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td><div style="background:#0d6efd33;height:10px;'
                         'width:%s&#37;;"></div></td>'
                         '<td class="text-end" style="color:%s">%s&#37;'
                         '</td></tr>' % celdas)
            html += ('</tbody></table><p style="font-size:11px;" '
                     'class="text-muted">Precio y margen con el costo '
                     'vigente en cada mes: si el precio no se mueve y el '
                     'costo sube, aquí se ve la erosión.</p>')
            rec.tendencia_html = html

    def _compute_cotizaciones_html(self):
        Cot = self.env['qb.cotizacion']
        estados = dict(
            Cot._fields['state']._description_selection(self.env))
        for rec in self:
            cots = Cot.search([('product_id', '=', rec.product_id.id)],
                              order='create_date desc', limit=15)
            if not cots:
                rec.cotizaciones_html = (
                    '<p class="text-muted">Sin cotizaciones de este '
                    'producto.</p>')
                continue
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Cliente</th><th>Rev.</th>'
                    '<th class="text-end">Precio evaluado</th>'
                    '<th class="text-end">Margen neto</th>'
                    '<th>Estado</th><th>Fecha</th></tr></thead><tbody>')
            for c in cots:
                celdas = (
                    html_escape(c.partner_id.name or '(sin cliente)'),
                    c.revision,
                    money(c.precio_evaluado, 2),
                    '{:.1f}&#37;'.format(c.margen_neto_pct),
                    self._SEM.get(c.semaforo, '') + ' '
                    + html_escape(estados.get(c.state, c.state or '')),
                    fecha_es(c.create_date.date() if c.create_date else None),
                )
                html += ('<tr><td>%s</td><td>%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td>%s</td><td>%s</td></tr>' % celdas)
            html += '</tbody></table>'
            rec.cotizaciones_html = html

    # ------------------------------------------------------------------
    # Drill-down: desde el renglón del producto, todo lo suyo a un clic.
    # El id de la vista ES el product_id, así que la navegación es directa.
    # ------------------------------------------------------------------
    def _accion(self, nombre, res_model, domain, view_mode='list'):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': nombre,
            'res_model': res_model,
            'view_mode': view_mode,
            'domain': domain,
            'target': 'current',
        }

    def action_ver_clientes(self):
        return self._accion(
            'Clientes de %s' % self.product_id.display_name,
            'qb.producto.cliente',
            [('product_id', '=', self.product_id.id)])

    def action_ver_programa(self):
        return self._accion(
            'Programa mensual: %s' % self.product_id.display_name,
            'qb.producto.mensual',
            [('product_id', '=', self.product_id.id)],
            view_mode='list,graph,pivot')

    def action_ver_costos(self):
        return self._accion(
            'Costos por período: %s' % self.product_id.display_name,
            'qb.costo.producto',
            [('product_id', '=', self.product_id.id)])

    def action_ver_cotizaciones(self):
        return self._accion(
            'Cotizaciones: %s' % self.product_id.display_name,
            'qb.cotizacion',
            [('product_id', '=', self.product_id.id)])

    def action_abrir_producto(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'product.product',
            'res_id': self.product_id.id,
            'view_mode': 'form',
            'target': 'current',
        }

    @property
    def _table_query(self):
        company_id = int(self.env.company.id)
        return _BASE_SQL.format(company_id=company_id) + """
            SELECT
                j.product_id AS id,
                j.product_id,
                SUM(j.rev) AS revenue_12m,
                SUM(j.qty) AS qty_12m,
                CASE WHEN SUM(j.qty) > 0 THEN SUM(j.rev) / SUM(j.qty)
                     ELSE 0 END AS precio_prom,
                CASE WHEN SUM(j.qty) > 0 THEN
                    SUM(j.qty * (COALESCE(j.costo_variable, 0)
                                 + COALESCE(j.fab_unit, 0))
                        + j.rev * j.op_pct) / SUM(j.qty)
                     ELSE 0 END AS costo_unit,
                SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0))
                    AS contrib_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0))
                          / SUM(j.rev) ELSE 0 END AS contrib_pct,
                SUM(j.rev - j.qty * (COALESCE(j.costo_variable, 0)
                                     + COALESCE(j.fab_unit, 0)))
                    AS margen_bruto_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev - j.qty * (COALESCE(j.costo_variable, 0)
                                                       + COALESCE(j.fab_unit, 0)))
                          / SUM(j.rev) ELSE 0 END AS margen_bruto_pct,
                SUM(j.rev * (1 - j.op_pct)
                    - j.qty * (COALESCE(j.costo_variable, 0)
                               + COALESCE(j.fab_unit, 0)))
                    AS margen_neto_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev * (1 - j.op_pct)
                                      - j.qty * (COALESCE(j.costo_variable, 0)
                                                 + COALESCE(j.fab_unit, 0)))
                          / SUM(j.rev) ELSE 0 END AS margen_neto_pct,
                SUM(j.rev * (1 - j.op_pct)
                    - j.qty * (COALESCE(j.costo_variable, 0)
                               + COALESCE(j.fab_unit, 0)) / j.rendimiento)
                    AS margen_neto_real_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev * (1 - j.op_pct)
                                      - j.qty * (COALESCE(j.costo_variable, 0)
                                                 + COALESCE(j.fab_unit, 0))
                                        / j.rendimiento)
                          / SUM(j.rev) ELSE 0 END AS margen_neto_real_pct,
                CASE WHEN SUM(j.qty) > 0
                     THEN 100.0 * SUM(j.qty * j.rendimiento) / SUM(j.qty)
                     ELSE 0 END AS rendimiento_pct,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(CASE WHEN j.costo_variable IS NOT NULL
                                           THEN j.rev ELSE 0 END) / SUM(j.rev)
                     ELSE 0 END AS costo_cobertura_pct,
                SUM(j.qty * j.horas_por_unidad) AS horas_cuello_12m,
                CASE WHEN SUM(j.qty * j.horas_por_unidad) > 0
                     THEN SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0))
                          / SUM(j.qty * j.horas_por_unidad)
                     ELSE 0 END AS contrib_por_hora,
                COUNT(DISTINCT j.partner_id) AS n_clientes,
                COUNT(DISTINCT j.mes) AS meses_activo,
                MAX(j.ultima) AS ultima_venta,
                MIN(j.company_id) AS company_id
            FROM joined j
            GROUP BY j.product_id
        """


class QbProductoCliente(models.Model):
    _name = 'qb.producto.cliente'
    _inherit = 'qb.sql.view'
    _description = 'Producto × Cliente (12 meses)'
    _auto = False
    _order = 'revenue_12m DESC'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', readonly=True,
                                 string='Producto')
    partner_id = fields.Many2one('res.partner', readonly=True,
                                 string='Cliente')
    revenue_12m = fields.Float(string='Ventas 12m (MXN)', readonly=True)
    qty_12m = fields.Float(string='Volumen 12m', readonly=True)
    precio_prom = fields.Float(
        string='Precio a este cliente $/u', readonly=True)
    precio_prom_producto = fields.Float(
        string='Precio prom del producto $/u', readonly=True,
        help='El promedio de TODOS los clientes en el mismo producto.')
    delta_precio_pct = fields.Float(
        string='Δ vs promedio %', readonly=True,
        help='Precio de este cliente vs el promedio del producto. Negativo '
             '= este cliente compra más barato que el resto.')
    margen_neto_12m = fields.Float(string='Margen neto 12m (MXN)',
                                   readonly=True)
    margen_neto_pct = fields.Float(string='Margen neto %', readonly=True)
    meses_activo = fields.Integer(string='Meses con venta', readonly=True)
    ultima_compra = fields.Date(string='Última compra', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        company_id = int(self.env.company.id)
        return _BASE_SQL.format(company_id=company_id) + """
            , grouped AS (
                SELECT
                    MIN(j.line_id) AS id,
                    j.product_id, j.partner_id,
                    SUM(j.rev) AS revenue_12m,
                    SUM(j.qty) AS qty_12m,
                    SUM(j.rev * (1 - j.op_pct)
                        - j.qty * (COALESCE(j.costo_variable, 0)
                                   + COALESCE(j.fab_unit, 0)))
                        AS margen_neto_12m,
                    COUNT(DISTINCT j.mes) AS meses_activo,
                    MAX(j.ultima) AS ultima_compra,
                    MIN(j.company_id) AS company_id
                FROM joined j
                GROUP BY j.product_id, j.partner_id
            )
            SELECT g.id, g.product_id, g.partner_id, g.revenue_12m,
                   g.qty_12m, g.margen_neto_12m, g.meses_activo,
                   g.ultima_compra, g.company_id,
                   CASE WHEN g.qty_12m > 0
                        THEN g.revenue_12m / g.qty_12m ELSE 0 END
                       AS precio_prom,
                   CASE WHEN SUM(g.qty_12m) OVER (PARTITION BY g.product_id) > 0
                        THEN SUM(g.revenue_12m) OVER (PARTITION BY g.product_id)
                             / SUM(g.qty_12m) OVER (PARTITION BY g.product_id)
                        ELSE 0 END AS precio_prom_producto,
                   CASE WHEN g.qty_12m > 0
                         AND SUM(g.qty_12m) OVER (PARTITION BY g.product_id) > 0
                         AND SUM(g.revenue_12m) OVER (PARTITION BY g.product_id) > 0
                        THEN 100.0 * ((g.revenue_12m / g.qty_12m)
                             / (SUM(g.revenue_12m) OVER (PARTITION BY g.product_id)
                                / SUM(g.qty_12m) OVER (PARTITION BY g.product_id))
                             - 1)
                        ELSE 0 END AS delta_precio_pct,
                   CASE WHEN g.revenue_12m > 0
                        THEN 100.0 * g.margen_neto_12m / g.revenue_12m
                        ELSE 0 END AS margen_neto_pct
            FROM grouped g
        """


class QbProductoMensual(models.Model):
    _name = 'qb.producto.mensual'
    _inherit = 'qb.sql.view'
    _description = 'Programa mensual por producto (12 meses)'
    _auto = False
    _order = 'mes DESC, revenue DESC'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', readonly=True,
                                 string='Producto')
    mes = fields.Date(string='Mes', readonly=True)
    qty = fields.Float(string='Volumen', readonly=True)
    revenue = fields.Float(string='Ventas (MXN)', readonly=True)
    precio_prom = fields.Float(string='Precio prom $/u', readonly=True)
    costo_unit = fields.Float(
        string='Costo $/u', readonly=True,
        help='Costo completo por unidad del mes: variable + fabricación + '
             'operación. Contra el precio promedio da el margen neto '
             'unitario — y mes a mes muestra si lo que merma el resultado '
             'es el costo subiendo o el precio bajando.')
    margen_neto = fields.Float(string='Margen neto (MXN)', readonly=True)
    n_clientes = fields.Integer(string='Clientes', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        company_id = int(self.env.company.id)
        return _BASE_SQL.format(company_id=company_id) + """
            SELECT
                MIN(j.line_id) AS id,
                j.product_id, j.mes,
                SUM(j.qty) AS qty,
                SUM(j.rev) AS revenue,
                CASE WHEN SUM(j.qty) > 0 THEN SUM(j.rev) / SUM(j.qty)
                     ELSE 0 END AS precio_prom,
                CASE WHEN SUM(j.qty) > 0 THEN
                    SUM(j.qty * (COALESCE(j.costo_variable, 0)
                                 + COALESCE(j.fab_unit, 0))
                        + j.rev * j.op_pct) / SUM(j.qty)
                     ELSE 0 END AS costo_unit,
                SUM(j.rev * (1 - j.op_pct)
                    - j.qty * (COALESCE(j.costo_variable, 0)
                               + COALESCE(j.fab_unit, 0))) AS margen_neto,
                COUNT(DISTINCT j.partner_id) AS n_clientes,
                MIN(j.company_id) AS company_id
            FROM joined j
            GROUP BY j.product_id, j.mes
        """


class QbPesoAuditoria(models.Model):
    """Auditoría de pesos: el verificador que faltaba.

    Compara el peso que USA el motor (resolve_kg_per_unit) contra el
    teórico de la nomenclatura, para cada producto VENDIDO en los últimos
    12 meses, ponderado por su venta. Así el peso inflado de la familia m²
    (+50 por ciento) o un 0.1 de Odoo weight se ven en una lista ordenada
    por dinero en riesgo, en lugar de descubrirse cliente por cliente.

    Tabla normal (no vista): se regenera con el botón — resolver el peso
    cruza el maestro, la nomenclatura y el gemelo nacional en Python, que
    no cabe en un SQL de vista.
    """
    _name = 'qb.peso.auditoria'
    _description = 'Auditoría de pesos por producto'
    _order = 'revenue_12m DESC'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', readonly=True,
                                 required=True, ondelete='cascade')
    default_code = fields.Char(string='Referencia', readonly=True)
    uom_name = fields.Char(string='UoM', readonly=True)
    kg_motor = fields.Float(
        string='kg/u que usa el motor', digits=(16, 6), readonly=True)
    kg_teorico = fields.Float(
        string='kg/u teórico', digits=(16, 6), readonly=True,
        help='De la nomenclatura: gramaje × ancho por metro; gramaje/1000 '
             'por m²; 1 por kg. Cero = la clave no trae gramaje parseable.')
    fuente = fields.Char(string='Fuente del peso', readonly=True)
    desviacion_pct = fields.Float(
        string='Desviación %', readonly=True,
        help='Motor vs teórico. Grande = alguien tiene que medir o '
             'capturar el peso en el maestro.')
    revenue_12m = fields.Float(string='Ventas 12m (MXN)', readonly=True)
    estado = fields.Selection([
        ('ok', 'OK'),
        ('revisar', 'Revisar'),
        ('critico', 'Crítico'),
        ('sin_peso', 'Sin peso'),
    ], readonly=True)

    _UMBRAL_REVISAR = 25.0
    _UMBRAL_CRITICO = 60.0

    @api.model
    def _estado_para(self, kg_motor, kg_teorico, fuente):
        """Clasifica una fila. Separado para poderse probar sin ventas."""
        if not kg_motor:
            return 'sin_peso', 0.0
        if not kg_teorico:
            # Sin teórico contra qué comparar: estimado = a revisar.
            Peso = self.env['qb.producto.peso']
            if fuente in Peso.PESO_SOURCES_ESTIMADAS or fuente == 'sin_peso':
                return 'revisar', 0.0
            return 'ok', 0.0
        desv = 100.0 * (kg_motor / kg_teorico - 1.0)
        if abs(desv) >= self._UMBRAL_CRITICO:
            return 'critico', desv
        if abs(desv) >= self._UMBRAL_REVISAR:
            return 'revisar', desv
        return 'ok', desv

    @api.model
    def _kg_teorico(self, product):
        ref = product.default_code or ''
        uom = (product.uom_id.name or '').lower()
        if uom in ('kg', 'kgs', 'kilogramo', 'kilogramos'):
            return 1.0
        m = re.match(r'^[A-Za-z]+(\d{3})(?!\d)', ref)
        if not m:
            return 0.0
        gramaje = int(m.group(1)) / 1000.0
        if uom in ('m2', 'm²'):
            return gramaje
        return self.env['qb.producto.peso']._gramaje_from_ref(ref)

    def action_generar(self):
        """Regenera la auditoría para los productos vendidos en 12 meses.
        Importados y subproductos quedan fuera: no cargan energía ni
        fabricación por peso, su kg no mueve ningún costo."""
        self.search([]).unlink()
        self.env.cr.execute("""
            SELECT aml.product_id, SUM(-aml.balance) AS rev
            FROM account_move_line aml
            JOIN account_move am ON am.id = aml.move_id
            JOIN account_account aa ON aa.id = aml.account_id
            WHERE am.move_type IN ('out_invoice', 'out_refund')
              AND am.state = 'posted'
              AND aml.display_type = 'product'
              AND aa.account_type = 'income'
              AND aml.product_id IS NOT NULL
              AND aml.company_id = %s
              AND am.invoice_date >= (date_trunc('month', CURRENT_DATE)
                                      - INTERVAL '12 months')
            GROUP BY aml.product_id
            ORDER BY 2 DESC
        """, (self.env.company.id,))
        ventas = self.env.cr.fetchall()
        Peso = self.env['qb.producto.peso']
        Ruteo = self.env['qb.producto.ruteo']
        Product = self.env['product.product']
        vals_list = []
        for product_id, rev in ventas:
            product = Product.browse(product_id).exists()
            if not product:
                continue
            bucket, _centros = Ruteo.resolve(product)
            if bucket in ('importado', 'subproducto'):
                continue
            kg, fuente = Peso._resolve_kg_source(product)
            teorico = self._kg_teorico(product)
            estado, desv = self._estado_para(kg, teorico, fuente)
            vals_list.append({
                'product_id': product.id,
                'default_code': product.default_code,
                'uom_name': product.uom_id.name,
                'kg_motor': kg,
                'kg_teorico': teorico,
                'fuente': fuente,
                'desviacion_pct': desv,
                'revenue_12m': rev or 0.0,
                'estado': estado,
            })
        self.create(vals_list)
        return True
