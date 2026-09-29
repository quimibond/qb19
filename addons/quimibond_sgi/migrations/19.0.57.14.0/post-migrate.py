# -*- coding: utf-8 -*-
"""57.14.0 (indicadores 2 y decisiones de Jose del 2026-09-29).

1. ``sgi.indicator._sgi_update_ind2_fichas()``: en S1-05 la fuente dice
   «precio de la orden de compra» en vez de «lista de precios del proveedor»
   (solo esa frase) y su fórmula pasa a «|pagado − acordado| × cantidad ÷
   compras del mes × 100»; C4-01 toma su definición nueva (órdenes
   confirmadas, en proceso o por cerrar vencidas más de 48 h, foto al cierre
   de la semana; sentido «más bajo es mejor», metas 10 / 20 si seguía en «más
   alto es mejor») y ``_sgi_c4_01_trajectory()`` le carga los escalones
   trimestrales 40 / 25 / 10 % (dic-2026, mar-2027, jun-2027) sin tocar los
   que ya existan.
2. ``sgi.indicator._sgi_adopt_offboarding_plan()``: el plan existente «Baja
   de personal» (id 5) toma los tipos propios en «Desactivar usuario de Odoo,
   correo y accesos» (Retirar accesos) y «Recuperar EPP…» (Recuperar EPP) y
   gana el renglón «Recoger equipo de cómputo» (Recoger equipo). Si no
   encuentra el plan o un renglón, avisa en el log y sigue.
3. ``sgi.indicator._sgi_activate_ind2()``: S2-01, S1-05, C4-01, C1-04, RH-01,
   S4-01 y S6-02 pasan a su modo si siguen activos, en «manual» y sin
   términos de fórmula (criterio de E1-02 en 57.6.0). S6-02 sin «Medir desde»
   se mide desde el mes siguiente.

Todo es idempotente, nada se borra y el valor anterior queda en el log (y en
el chatter del indicador).

Esperado en producción (MCP, solo lectura, 2026-09-29): los siete en prueba,
manuales, sin términos y sin «Medir desde»: S2-01 (id 121), S1-05 (136),
C4-01 (142), C1-04 (151), RH-01 (23), S4-01 (162) y S6-02 (167). Plan 5 con
los renglones 19 (accesos, «Por hacer», Mariano Dominguez) y 18 (EPP,
«Por hacer»).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Indicator = env['sgi.indicator']
    fichas = Indicator._sgi_update_ind2_fichas()
    _logger.info("SGI 57.14.0: fichas actualizadas: %s (esperado: S1-05 136, C4-01 142).",
                 fichas)
    steps = Indicator._sgi_c4_01_trajectory()
    _logger.info("SGI 57.14.0: escalones de C4-01 creados: %s (esperado: 3).", steps)
    plan = Indicator._sgi_adopt_offboarding_plan()
    _logger.info("SGI 57.14.0: plan de salida: %s (esperado: renglones 19 y 18 con tipo "
                 "nuevo y un renglón creado).", plan)
    done = Indicator._sgi_activate_ind2()
    _logger.info("SGI 57.14.0: indicadores 2 activados: %s (esperado en producción: "
                 "S2-01 121, S1-05 136, C4-01 142, C1-04 151, RH-01 23, S4-01 162, "
                 "S6-02 167).", done)
