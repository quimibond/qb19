# -*- coding: utf-8 -*-
"""Rentabilidad por cliente (vista SQL read-only, 12 meses).

La pregunta que responde: "¿me conviene pelear este cliente?" — su
contribución REAL (precio facturado − costo variable del modelo, mes a
mes), cuántas horas del cuello de botella ocupa y con qué mezcla.

Cruce: líneas de factura (dedup del triplete) × qb.costo.producto del
MISMO período — así la contribución usa el costo variable vigente en el
mes en que se facturó, no el de hoy.
"""
from odoo import fields, models


class QbClienteRentabilidad(models.Model):
    _name = 'qb.cliente.rentabilidad'
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
    contrib_pct = fields.Float(string='Contribución %', readonly=True)
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

    @property
    def _table_query(self):
        return """
            WITH lines AS (
                SELECT am.commercial_partner_id AS partner_id,
                       aml.product_id,
                       date_trunc('month', am.invoice_date)::date AS mes,
                       am.invoice_date,
                       aml.move_id, aml.quantity, aml.balance,
                       am.move_type, aml.company_id
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                WHERE am.move_type IN ('out_invoice', 'out_refund')
                  AND am.state = 'posted'
                  AND aml.display_type = 'product'
                  AND aml.product_id IS NOT NULL
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
                       CASE WHEN cp.contrib_hora_maquina > 0
                            THEN cp.margen_contribucion / cp.contrib_hora_maquina
                            ELSE 0 END AS horas_por_unidad
                FROM qty q
                JOIN revenue r ON r.partner_id = q.partner_id
                             AND r.product_id = q.product_id AND r.mes = q.mes
                LEFT JOIN qb_costo_producto cp
                       ON cp.product_id = q.product_id AND cp.period = q.mes
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
