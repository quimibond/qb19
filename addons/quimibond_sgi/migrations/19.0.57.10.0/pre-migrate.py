# -*- coding: utf-8 -*-
"""57.10.0 (entrega 2, e2-ganchos-satelites; A-019, A-020). Solo SQL sobre
``ir_model_data`` (procedimiento de ``migrations/mudanza.py``), idempotente y
sin borrar nada.

- **quimibond_sgi_revisado** (A-019): el valor ``calidad_pq`` del modo de
  cálculo (``selection__sgi_indicator__calc_mode__calidad_pq``) pasa al
  satélite, que ahora lo registra con ``selection_add`` y trae el cálculo.
  Producción el 2026-09-29 (MCP): MA-03 (id 16) en ``calidad_pq``, activo,
  última medición 08/2026 = 70.88; ``quimibond_sgi_revisado`` instalado.
  Sin mudar el XML ID, el ``_process_end`` del núcleo borraría el valor de la
  selección y Odoo pasaría MA-03 a su default.
- **quimibond_sgi_pesaje** (A-020): el campo de Ajustes
  ``res.config.settings.sgi_pesaje_tolerance_kg``. El parámetro
  ``quimibond_sgi.pesaje_tolerance_kg`` no cambia de clave ni de valor (en
  producción además tiene el XML ID ``quimibond_sgi_pesaje.sgi_param_pesaje_tolerance``,
  ``noupdate``, de una versión vieja del satélite).

Si ``mrp_revisado_telas`` no estuviera instalado (no es el caso de
producción), los indicadores en ``calidad_pq`` no tendrían quién los calcule:
pasan a ``manual`` con aviso en el log (antes daban «sin dato»).
"""
import importlib.util
import logging
import os

_logger = logging.getLogger(__name__)

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'mudanza.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_mudanza_57_10', _path)
mudanza = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mudanza)

REVISADO = {'nombres': ['selection__sgi_indicator__calc_mode__calidad_pq']}
PESAJE = {'campos': ['res.config.settings.sgi_pesaje_tolerance_kg']}


def _module_state(cr, name):
    cr.execute("SELECT state FROM ir_module_module WHERE name = %s", (name,))
    row = cr.fetchone()
    return row and row[0]


def migrate(cr, version):
    if not version:
        return
    tag = "SGI 57.10.0"
    mudanza.mover(cr, 'quimibond_sgi_revisado', tag + " (revisado)", **REVISADO)
    mudanza.mover(cr, 'quimibond_sgi_pesaje', tag + " (pesaje)", **PESAJE)
    mudanza.instalar(cr, 'quimibond_sgi_revisado', ['mrp_revisado_telas'], tag)
    mudanza.instalar(cr, 'quimibond_sgi_pesaje', ['pesaje_rollos_tejido'], tag)
    if _module_state(cr, 'quimibond_sgi_revisado') not in ('installed', 'to upgrade', 'to install'):
        cr.execute("UPDATE sgi_indicator SET calc_mode = 'manual' "
                   "WHERE calc_mode = 'calidad_pq' RETURNING code")
        codes = [r[0] for r in cr.fetchall()]
        if codes:
            _logger.warning("%s: sin quimibond_sgi_revisado, %s pasan de calidad_pq a manual.", tag, codes)
