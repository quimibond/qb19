# -*- coding: utf-8 -*-
"""Inspección de importados y nómina de Diseño — el recálculo vive en la
migración más nueva.

Regla de la cadena: SOLO la migración más nueva recalcula. El recálculo
(año corriente síncrono + históricos al cron) vive hoy en la 19.0.1.42.0.
En producción esta versión SÍ corrió con recálculo el 31-ago-2026; este
no-op existe para que las cadenas que saltan varias versiones no paguen
el mismo build dos veces.
"""


def migrate(cr, version):
    pass
