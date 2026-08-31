# -*- coding: utf-8 -*-
"""MP al precio de la época — el recálculo vive en la migración más nueva.

Regla de la cadena: SOLO la migración más nueva recalcula. El recálculo
(año corriente síncrono + históricos al cron) vive hoy en la 19.0.1.41.0;
recalcular también aquí pagaría el mismo build dos veces para tirar la
primera.
"""


def migrate(cr, version):
    pass
