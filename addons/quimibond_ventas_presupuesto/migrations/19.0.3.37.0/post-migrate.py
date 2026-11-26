# -*- coding: utf-8 -*-
"""3.37.0: el mes de cada línea del pronóstico sale de la fecha de entrega del cliente; sin ella, del lunes
de la semana. Cambiar el ``depends`` no recalcula lo guardado, así que se recalcula aquí. Los presupuestos
del mes ya armados no se tocan: se vuelven a armar desde su ficha (en borrador)."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("""
        UPDATE qb_forecast_line
           SET month = date_trunc('month', delivery_date)::date
         WHERE delivery_date IS NOT NULL
           AND month IS DISTINCT FROM date_trunc('month', delivery_date)::date""")
    _logger.info("Pronóstico: %d línea(s) cambian de mes por su fecha de entrega", cr.rowcount)
