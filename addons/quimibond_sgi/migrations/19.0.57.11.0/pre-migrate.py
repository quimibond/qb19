# -*- coding: utf-8 -*-
"""57.11.0 (entrega 2, e2-sale-presupuesto-ventas; A-016, A-013, E-014).
El presupuesto y el pronóstico de ventas pasan al módulo nuevo
``quimibond_ventas_presupuesto``. Solo SQL sobre ``ir_model_data`` (y la
columna ``module`` de ``ir_model_constraint``/``ir_model_relation``) con el
procedimiento de ``migrations/mudanza.py``, idempotente y sin borrar ni
recrear nada: los modelos conservan su nombre técnico y sus tablas, y menús,
acciones, crons, secuencia y vistas conservan su ``res_id`` (E-002).

Producción el 2026-09-29 (MCP, solo lectura, empresa 1):
- ``sgi.sales.budget`` = 4 (ids 4, 5, 7, 137; los 4 en borrador, 2026) y
  ``sgi.sales.budget.line`` = 1,291 (915 + 12 + 39 + 325).
- Menús 2434 «Presupuestos» (acción 3886) y 2435 «Pronósticos» (acción
  3887) bajo 2438 «Ventas/Presupuesto y pronóstico», más 2449 «Análisis» y
  2444–2447 (acciones 3891–3894).
- Crons 184 (revaluación del S2, mensual) y 185 (cobertura del pronóstico,
  semanal), activos, con los mismos valores que su XML: al instalarse el
  módulo nuevo (Odoo sobrescribe incluso lo ``noupdate``) quedan igual; su
  ``nextcall`` no está en el XML y no cambia.
- ``format_map_sales_budget`` (sgi.format.map 9): mismos valores que el XML;
  ``document_id`` (3882) no está en el XML y se conserva.
- Secuencia 796 ``sgi_seq_sales_budget``: folios PPV-2026-004…137, mismo
  prefijo y relleno que el XML (el número siguiente no está en el XML).
- 50 XML IDs de ``quimibond_sgi`` sobre estos registros (sin contar campos):
  todos los declara el módulo nuevo.

Los 9 parámetros (``quimibond_sgi.sales_budget_alert_pct``, ``budget_*``,
``price_*``, ``forecast_*``) no tienen XML ID ni cambian de clave: los
siembra el módulo nuevo solo si faltan.

El módulo nuevo se marca para instalar en este mismo update (dependencias en
producción: ``sale_stock``, ``web_grid`` y ``account_budget``, instaladas).

Reversa: ``UPDATE ir_model_data SET module = 'quimibond_sgi' WHERE module =
'quimibond_ventas_presupuesto'`` (y lo mismo en ``ir_model_constraint`` e
``ir_model_relation`` por id de módulo) con el módulo nuevo sin instalar y el
código de 57.10.0.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'mudanza.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_mudanza_57_11', _path)
mudanza = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mudanza)

DESTINO = 'quimibond_ventas_presupuesto'
PRESUPUESTO = {
    'modelos': ['sgi.sales.budget', 'sgi.sales.budget.line', 'sgi.sales.budget.import',
                'budget.analytic'],
    'campos': ['res.config.settings.%s' % name for name in (
        'sgi_sales_budget_alert_pct', 'sgi_budget_planning_rate', 'sgi_price_gap_tolerance_pct',
        'sgi_price_gap_grave_pct', 'sgi_forecast_over_tolerance_pct',
        'sgi_forecast_capture_horizon_weeks', 'sgi_budget_fulfillment_min',
        'sgi_price_min_plausible', 'sgi_budget_pricelist_id')],
    'nombres': [
        # Menús (E-002: 2438, 2434, 2435, 2449, 2444-2447).
        'menu_sale_sgi_planning', 'menu_sale_sgi_sales_budget', 'menu_sale_sgi_sales_forecast',
        'menu_sale_sgi_sales_analysis', 'menu_sale_sgi_analysis_mercado',
        'menu_sale_sgi_analysis_cliente', 'menu_sale_sgi_analysis_producto',
        'menu_sale_sgi_analysis_global',
        # Crons (modelo sgi.cron, que se queda) y su acción de servidor.
        'sgi_cron_budget_revaluation', 'sgi_cron_budget_revaluation_ir_actions_server',
        'sgi_cron_forecast_coverage', 'sgi_cron_forecast_coverage_ir_actions_server',
        # Secuencia, pie de formato y plantillas QWeb del reporte.
        'sgi_seq_sales_budget', 'format_map_sales_budget',
        'report_sales_budget_document', 'report_sales_forecast_page',
    ],
}


def migrate(cr, version):
    if not version:
        return
    tag = "SGI 57.11.0 (presupuesto de ventas)"
    mudanza.mover(cr, DESTINO, tag, **PRESUPUESTO)
    mudanza.faltantes(cr, DESTINO, PRESUPUESTO['nombres'], tag)
    mudanza.instalar(cr, DESTINO, ['sale_stock', 'web_grid', 'account_budget'], tag,
                     obligatorio=bool(mudanza.contar(cr, 'sgi_sales_budget')))
