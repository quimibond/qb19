# -*- coding: utf-8 -*-
{
    'name': "Quimibond - Presupuesto y pronóstico de ventas",
    'summary': "Presupuesto de ventas anual por mercado y pronóstico semanal por cliente (P-A28), en la app Ventas",
    'description': """
Presupuesto y pronóstico de ventas de Quimibond (procedimiento P-A28).

- Presupuesto anual por mercado (equipo de ventas) o por cliente, mensual,
  con precio de lista, revisiones congeladas y aprobación de Dirección
  (formato F-P-A28-18); pronóstico semanal por cliente (F-P-A28-13).
- Captura en matriz (grid), importación desde Excel, foto de lo facturado y
  lo pedido, cobertura del pronóstico, control de precios (lista vs
  facturado) y análisis por mercado, cliente, producto y global.
- Avisos: cierre de mes por debajo del umbral y justificación del
  incumplimiento, cobertura semanal del pronóstico y revaluación del S2.
- Liga del presupuesto de gastos (Presupuestos, budget.analytic) con el de
  ventas.
- Releases de clientes: catálogo de partes del cliente → producto (con
  sugerencia), perfiles, lectores de Lear (AIAG) y FXI (SUM), aplicación al
  pronóstico por año y demanda total al MPS.

Salió de quimibond_sgi en 57.11.0 (auditoría A-016, A-013, decisión 5): es
planeación comercial; el SGI solo lo mide (KPI VE-02, «presupuesto_ventas»,
que lo lee si este módulo está). Los modelos conservan su nombre técnico
(``sgi.sales.budget``, ``sgi.sales.budget.line``) y sus tablas; en una base
que venía de una versión anterior, el update del SGI instala este módulo y le
pasa los registros existentes sin borrar ni recrear nada.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Sales/Sales',
    'version': '19.0.1.2.0',
    'license': 'LGPL-3',
    'depends': [
        'quimibond_sgi',  # sgi.base.mixin, sgi.format.mixin, grupos, crons y KPI
        'sale_stock',  # pedidos y entregas del pronóstico
        'web_grid',  # matriz de captura de cantidades
        'account_budget',  # budget.analytic.sgi_sales_budget_id (P-3)
    ],
    'data': [
        'security/ir.model.access.csv',
        'security/security.xml',
        'data/sgi_sales_budget_data.xml',
        'data/sgi_sales_budget_parameters.xml',
        'views/sgi_sales_budget_views.xml',
        'views/sgi_budget_analytic_views.xml',
        'views/qb_customer_part_views.xml',
        'views/qb_release_views.xml',
        'views/res_config_settings_views.xml',
        'report/report_sales_budget.xml',
        'views/menus.xml',
    ],
    'auto_install': True,
    'installable': True,
}
