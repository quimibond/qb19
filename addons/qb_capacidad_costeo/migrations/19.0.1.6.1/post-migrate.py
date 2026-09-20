# -*- coding: utf-8 -*-
"""Borrar filas huérfanas de qb_costo_producto.

El filtro de cuentas 'income' (19.0.1.6.0) sacó del costeo las ventas que
no son de producto (activo fijo, anticipos), pero el recálculo solo hacía
upsert: la fila vieja de un producto excluido sobrevivía cada corrida
(la rama Icomatex de $11.3M en 2026-03 y la línea de corte de $1.97M en
2026-06). El recálculo ya elimina huérfanas de aquí en adelante; esta
migración limpia las existentes: filas con qty vendida cuyo producto NO
tiene ninguna línea de factura contra cuenta 'income' en su período.
Idempotente."""


def migrate(cr, version):
    cr.execute("""
        DELETE FROM qb_costo_producto cp
        WHERE cp.qty_vendida != 0
          AND NOT EXISTS (
              SELECT 1
              FROM account_move_line aml
              JOIN account_move am ON am.id = aml.move_id
              JOIN account_account aa ON aa.id = aml.account_id
              WHERE am.move_type IN ('out_invoice', 'out_refund')
                AND am.state = 'posted'
                AND aml.display_type = 'product'
                AND aa.account_type = 'income'
                AND aml.product_id = cp.product_id
                AND aml.company_id = cp.company_id
                AND am.invoice_date >= cp.period
                AND am.invoice_date < (cp.period + INTERVAL '1 month')
          )
    """)
