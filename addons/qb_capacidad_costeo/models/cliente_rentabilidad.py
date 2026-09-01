# -*- coding: utf-8 -*-
"""Rentabilidad por cliente (vista SQL read-only, 12 meses).

La pregunta que responde: "¿me conviene pelear este cliente?" — sus tres
márgenes REALES (precio facturado vs costos del modelo, mes a mes),
cuántas horas del cuello de botella ocupa y con qué mezcla:

- Contribución = facturado − costo variable (MP + energía).
- Margen bruto = facturado − costo de producción (variable + fabricación).
- Margen neto  = bruto − operación (op% del período × facturado).

Cruce: líneas de factura (dedup del triplete) × qb.costo.producto del
MISMO período — así cada margen usa el costo vigente en el mes en que se
facturó, no el de hoy. Solo cuentas de tipo 'income' (ventas/rebajas de
producto): ventas de activo fijo y anticipos NO cuentan como venta.
"""
from odoo import api, fields, models
from odoo.tools import html_escape

from .producto_reportes import _BASE_SQL, fecha_es, mes_es, money


class QbClienteRentabilidad(models.Model):
    _name = 'qb.cliente.rentabilidad'
    _inherit = 'qb.sql.view'
    _description = 'Rentabilidad por cliente (12 meses)'
    _auto = False
    _order = 'contrib_12m DESC'
    _rec_name = 'partner_id'

    partner_id = fields.Many2one('res.partner', readonly=True,
                                 string='Cliente')
    revenue_12m = fields.Float(string='Ventas 12m (MXN)', readonly=True)
    contrib_12m = fields.Float(
        string='Contribución 12m (MXN)', readonly=True,
        help='Σ (facturado − qty × costo variable del período). Lo que este '
             'cliente aportó a fijos en 12 meses.')
    contrib_pct = fields.Float(
        string='Contribución %', readonly=True,
        help='Contribución ÷ ventas. Lo que este cliente deja para pagar '
             'fijos por cada peso vendido — todavía no es utilidad.')
    margen_bruto_12m = fields.Float(
        string='Margen bruto 12m (MXN)', readonly=True,
        help='Σ (facturado − qty × costo de producción del período). '
             'Utilidad después de fabricar, ANTES de administración y ventas.')
    margen_bruto_pct = fields.Float(string='Margen bruto %', readonly=True)
    margen_neto_12m = fields.Float(
        string='Margen neto 12m (MXN)', readonly=True,
        help='Margen bruto − operación (admin y ventas como % de las ventas, '
             'con el op% vigente en cada mes). Lo que este cliente deja de '
             'verdad después de TODOS los costos asignables.')
    margen_neto_pct = fields.Float(string='Margen neto %', readonly=True)
    costo_cobertura_pct = fields.Float(
        string='Cobertura de costo %', readonly=True,
        help='% de las ventas del cliente cuyo mes SÍ tenía costo calculado. '
             'Si es <100%, parte de la contribución está inflada (se tomó '
             'costo cero por falta de cálculo): corre "Recalcular costeo (año '
             'en curso)" para completar los meses.')
    horas_cuello_12m = fields.Float(
        string='Horas-máquina 12m', readonly=True,
        help='Horas del centro más lento de cada producto consumidas por lo '
             'que este cliente compró. Su "renta" del cuello de botella.')
    contrib_por_hora = fields.Float(
        string='Contribución $/hora', readonly=True,
        help='Contribución 12m ÷ horas-máquina 12m: qué tan bien paga este '
             'cliente el uso de tu cuello. Compararlo entre clientes.')
    n_productos = fields.Integer(string='Productos distintos', readonly=True)
    meses_activo = fields.Integer(string='Meses con compra', readonly=True)
    ultima_compra = fields.Date(string='Última compra', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    # ------------------------------------------------------------------
    # Situación completa: el veredicto en un semáforo y tres pestañas
    # calculadas al abrir (qué compra, tendencia, cotizaciones) — la misma
    # gramática visual del resto del módulo: alerta arriba, tablas HTML
    # compactas, cada juicio con su número al lado.
    # ------------------------------------------------------------------
    semaforo = fields.Selection(
        [('rojo', 'Pierde'), ('ambar', 'Apenas'), ('verde', 'Sano')],
        compute='_compute_semaforo', string='Semáforo')
    veredicto = fields.Char(compute='_compute_semaforo')
    company_currency_id = fields.Many2one(
        'res.currency', related='company_id.currency_id', string='Moneda')

    @api.depends('margen_neto_pct', 'margen_neto_12m', 'contrib_pct',
                 'contrib_12m')
    def _compute_semaforo(self):
        for rec in self:
            pct = rec.margen_neto_pct
            if pct < 0:
                rec.semaforo = 'rojo'
                rec.veredicto = (
                    'Pierde ${p:.2f} de cada $100 vendidos ({neto} en 12 '
                    'meses) después de TODOS los costos. Contribución '
                    '{c:.0f}%: aporta {contrib} a fijos — el problema es '
                    'precio, no el cliente.'.format(
                        p=-pct, neto=money(rec.margen_neto_12m),
                        c=rec.contrib_pct,
                        contrib=money(rec.contrib_12m)))
            elif pct < 5:
                rec.semaforo = 'ambar'
                rec.veredicto = (
                    'Deja {p:.1f}% neto — cubre costos con margen mínimo. '
                    'Un aumento de costo de hilo lo manda a rojo.'.format(
                        p=pct))
            else:
                rec.semaforo = 'verde'
                rec.veredicto = (
                    'Deja {p:.1f}% neto ({neto} en 12 meses) después de '
                    'todos los costos.'.format(
                        p=pct, neto=money(rec.margen_neto_12m)))

    productos_html = fields.Html(
        compute='_compute_productos_html', sanitize=False,
        string='Qué compra')
    tendencia_html = fields.Html(
        compute='_compute_tendencia_html', sanitize=False,
        string='Tendencia 12 meses')
    cotizaciones_html = fields.Html(
        compute='_compute_cotizaciones_html', sanitize=False,
        string='Cotizaciones')

    _SEM = {'rojo': '🔴', 'ambar': '🟡', 'verde': '🟢'}

    def _compute_productos_html(self):
        Pareja = self.env['qb.producto.cliente']
        Prod = self.env['qb.producto.rentabilidad']
        for rec in self:
            filas = Pareja.search([('partner_id', '=', rec.partner_id.id)])
            if not filas:
                rec.productos_html = ('<p class="text-muted">Sin compras '
                                      'en los últimos 12 meses.</p>')
                continue
            # ¿En cuáles de estos productos este cliente es el ÚNICO
            # comprador? Ahí el Δ contra el promedio es 0 por construcción
            # y decir «+0.0» confunde (caso BLANCOS MILENIUM).
            n_clientes = {p.id: p.n_clientes for p in Prod.search(
                [('id', 'in', filas.mapped('product_id').ids)])}
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Producto</th>'
                    '<th class="text-end">Volumen</th>'
                    '<th class="text-end">Precio $/u</th>'
                    '<th class="text-end">Δ vs prom. del producto</th>'
                    '<th class="text-end">Margen neto</th>'
                    '<th class="text-end">Neto</th>'
                    '<th>Última</th></tr></thead><tbody>')
            for f in filas:
                sem = ('rojo' if f.margen_neto_pct < 0
                       else 'ambar' if f.margen_neto_pct < 5 else 'verde')
                if n_clientes.get(f.product_id.id, 0) <= 1:
                    delta = ('<span class="text-muted">único comprador'
                             '</span>')
                elif f.delta_precio_pct < -10:
                    delta = ('<span style="color:#b02a37;font-weight:bold;">'
                             '{:+.1f}&#37;</span>').format(f.delta_precio_pct)
                else:
                    delta = '{:+.1f}&#37;'.format(f.delta_precio_pct)
                celdas = (
                    self._SEM[sem] + ' '
                    + html_escape(f.product_id.display_name or ''),
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
                     'class="text-muted">Δ en rojo = este cliente compra '
                     'ese producto más de 10 abajo del promedio de todos '
                     'los clientes — el candidato a renegociar.</p>')
            rec.productos_html = html

    def _compute_tendencia_html(self):
        company_id = int(self.env.company.id)
        for rec in self:
            self.env.cr.execute(
                _BASE_SQL.format(company_id=company_id) + """
                SELECT j.mes, SUM(j.rev) AS rev,
                       SUM(j.rev * (1 - j.op_pct)
                           - j.qty * (COALESCE(j.costo_variable, 0)
                                      + COALESCE(j.fab_unit, 0))) AS neto
                FROM joined j
                WHERE j.partner_id = %s
                GROUP BY j.mes ORDER BY j.mes
                """, (rec.partner_id.id,))
            meses = self.env.cr.fetchall()
            if not meses:
                rec.tendencia_html = ('<p class="text-muted">Sin ventas en '
                                      'la ventana.</p>')
                continue
            tope = max(abs(r[1]) for r in meses) or 1.0
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Mes</th>'
                    '<th class="text-end">Ventas</th><th style="width:30%">'
                    '</th><th class="text-end">Neto</th>'
                    '<th class="text-end">Neto sobre venta</th></tr>'
                    '</thead><tbody>')
            for mes, rev, neto in meses:
                ancho = 100.0 * abs(rev) / tope
                pct = 100.0 * neto / rev if rev else 0.0
                color = ('#b02a37' if pct < 0
                         else '#997404' if pct < 5 else '#146c43')
                celdas = (
                    mes_es(mes),
                    money(rev),
                    '{:.0f}'.format(ancho),
                    color, money(neto),
                    color, '{:+.1f}'.format(pct),
                )
                html += ('<tr><td>%s</td><td class="text-end">%s</td>'
                         '<td><div style="background:#0d6efd33;height:10px;'
                         'width:%s&#37;;"></div></td>'
                         '<td class="text-end" style="color:%s">%s</td>'
                         '<td class="text-end" style="color:%s">%s&#37;'
                         '</td></tr>' % celdas)
            html += ('</tbody></table><p style="font-size:11px;" '
                     'class="text-muted">El neto usa el costo vigente en el '
                     'mes de cada factura: si el precio está congelado y el '
                     'costo sube, aquí se ve el margen erosionarse mes a '
                     'mes.</p>')
            rec.tendencia_html = html

    def _compute_cotizaciones_html(self):
        Cot = self.env['qb.cotizacion']
        estados = dict(
            Cot._fields['state']._description_selection(self.env))
        for rec in self:
            cots = Cot.search([('partner_id', '=', rec.partner_id.id)],
                              order='create_date desc', limit=15)
            if not cots:
                rec.cotizaciones_html = (
                    '<p class="text-muted">Sin cotizaciones. Desde '
                    '«Productos que compra» se ve qué conviene recotizar '
                    'con el costo de hoy.</p>')
                continue
            html = ('<table class="table table-sm" style="font-size:12px;">'
                    '<thead><tr><th>Cotización</th><th>Rev.</th>'
                    '<th class="text-end">Precio evaluado</th>'
                    '<th class="text-end">Margen neto</th>'
                    '<th>Estado</th><th>Fecha</th></tr></thead><tbody>')
            for c in cots:
                celdas = (
                    html_escape(c.name or ''), c.revision,
                    money(c.precio_evaluado, 2),
                    '{:.1f}&#37;'.format(c.margen_neto_pct),
                    self._SEM.get(c.semaforo, '') + ' '
                    + html_escape(estados.get(c.state, c.state or '')),
                    fecha_es(c.create_date.date() if c.create_date
                             else None),
                )
                html += ('<tr><td>%s</td><td>%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td class="text-end">%s</td>'
                         '<td>%s</td><td>%s</td></tr>' % celdas)
            html += '</tbody></table>'
            rec.cotizaciones_html = html

    # ------------------------------------------------------------------
    # Drill-down: desde el renglón del cliente, todo lo suyo a un clic.
    # El id de la vista ES el partner_id, así que la navegación es directa.
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

    def action_ver_productos(self):
        """Qué le vendo, a qué precio y con qué margen — la matriz
        producto × cliente filtrada a esta cuenta."""
        return self._accion(
            'Productos de %s' % self.partner_id.name,
            'qb.producto.cliente',
            [('partner_id', '=', self.partner_id.id)])

    def action_ver_cotizaciones(self):
        return self._accion(
            'Cotizaciones: %s' % self.partner_id.name,
            'qb.cotizacion',
            [('partner_id', '=', self.partner_id.id)])

    def action_ver_facturas(self):
        return self._accion(
            'Facturas: %s' % self.partner_id.name,
            'account.move',
            [('commercial_partner_id', '=', self.partner_id.id),
             ('move_type', 'in', ('out_invoice', 'out_refund'))])

    def action_abrir_cliente(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'res_id': self.partner_id.id,
            'view_mode': 'form',
            'target': 'current',
        }

    @property
    def _table_query(self):
        # Solo la COMPAÑIA ACTIVA, como el motor de costos: sin este filtro,
        # en multicompañía las facturas de las otras empresas del grupo
        # metían a QUIMIBOND y a las hermanas (p.ej. ENTRETELAS BRINCO)
        # como "clientes" con cobertura de costo 0 — pura facturación
        # intercompañía. _table_query es una property: se re-evalúa en cada
        # lectura con la compañía activa del usuario. Ningún caracter de
        # porcentaje en el SQL (pasa por formateo estilo printf).
        company_id = int(self.env.company.id)
        return f"""
            WITH lines AS (
                SELECT am.commercial_partner_id AS partner_id,
                       aml.product_id,
                       date_trunc('month', am.invoice_date)::date AS mes,
                       am.invoice_date,
                       aml.move_id, aml.quantity, aml.balance,
                       am.move_type, aml.company_id
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                -- Solo cuentas de tipo 'income' (ventas y rebajas de
                -- producto): la utilidad en venta de activo fijo
                -- (income_other) y los anticipos de clientes (liability)
                -- no son venta de producto y contaminaban al cliente.
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
                       partner_id, product_id, mes, invoice_date, move_id,
                       CASE WHEN move_type = 'out_refund'
                            THEN -quantity ELSE quantity END AS qty,
                       company_id
                FROM lines
                ORDER BY move_id, product_id, ABS(quantity)
            ),
            revenue AS (
                -- Revenue en MXN desde balance (moneda de la compañía), NO
                -- price_subtotal (moneda del documento): un cliente facturado
                -- en USD entra con su valor real en pesos. SUM(-balance) suma
                -- ventas y resta devoluciones por el signo contable, y el
                -- triplete lista/descuento/neta cancela igual que con subtotal.
                SELECT partner_id, product_id, mes,
                       SUM(-balance) AS rev
                FROM lines GROUP BY 1, 2, 3
            ),
            qty AS (
                SELECT partner_id, product_id, mes, company_id,
                       SUM(qty) AS qty, MAX(invoice_date) AS ultima
                FROM qty_dedup GROUP BY 1, 2, 3, 4
            ),
            joined AS (
                SELECT q.partner_id, q.product_id, q.mes, q.qty, q.company_id,
                       q.ultima, r.rev,
                       cp.costo_variable,
                       cp.fab_unit,
                       -- Operacion del periodo (op_pct de qb_costo_factores):
                       -- el margen neto del cliente usa SU facturado
                       -- (rev x op_pct), no el op_unit del producto (que va
                       -- sobre el precio promedio de TODOS los clientes).
                       -- OJO: nada de caracteres de porcentaje en este SQL —
                       -- _table_query pasa por formateo estilo printf de
                       -- Python y un porcentaje literal lo revienta.
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
            SELECT
                j.partner_id AS id,
                j.partner_id,
                SUM(j.rev) AS revenue_12m,
                SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0)) AS contrib_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0))
                          / SUM(j.rev)
                     ELSE 0 END AS contrib_pct,
                SUM(j.rev - j.qty * (COALESCE(j.costo_variable, 0)
                                     + COALESCE(j.fab_unit, 0)))
                    AS margen_bruto_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev - j.qty * (COALESCE(j.costo_variable, 0)
                                                       + COALESCE(j.fab_unit, 0)))
                          / SUM(j.rev)
                     ELSE 0 END AS margen_bruto_pct,
                SUM(j.rev * (1 - j.op_pct)
                    - j.qty * (COALESCE(j.costo_variable, 0)
                               + COALESCE(j.fab_unit, 0)))
                    AS margen_neto_12m,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(j.rev * (1 - j.op_pct)
                                      - j.qty * (COALESCE(j.costo_variable, 0)
                                                 + COALESCE(j.fab_unit, 0)))
                          / SUM(j.rev)
                     ELSE 0 END AS margen_neto_pct,
                CASE WHEN SUM(j.rev) > 0
                     THEN 100.0 * SUM(CASE WHEN j.costo_variable IS NOT NULL
                                           THEN j.rev ELSE 0 END) / SUM(j.rev)
                     ELSE 0 END AS costo_cobertura_pct,
                SUM(j.qty * j.horas_por_unidad) AS horas_cuello_12m,
                CASE WHEN SUM(j.qty * j.horas_por_unidad) > 0
                     THEN SUM(j.rev - j.qty * COALESCE(j.costo_variable, 0))
                          / SUM(j.qty * j.horas_por_unidad)
                     ELSE 0 END AS contrib_por_hora,
                COUNT(DISTINCT j.product_id) AS n_productos,
                COUNT(DISTINCT j.mes) AS meses_activo,
                MAX(j.ultima) AS ultima_compra,
                MIN(j.company_id) AS company_id
            FROM joined j
            GROUP BY j.partner_id
        """
