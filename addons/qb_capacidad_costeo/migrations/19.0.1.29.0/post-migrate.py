# -*- coding: utf-8 -*-
"""El recálculo de la cadena se parte: año corriente síncrono, historia a cron.

La migración más nueva recalcula todos los períodos guardados — regla de la
cadena desde la 19.0.1.20.0. Con 8 períodos eran ~80 segundos de build. Pero
al cargar 2024 y 2025 los períodos son 32, y ese único recálculo pasó a 5-6
minutos que TODO build de migración paga, más los ~24 tests que invocan el
motor completo sobre una copia de producción. Por eso «antes era mucho más
rápido»: antes había una cuarta parte de la historia y dos tercios de los
tests.

El reparto ahora:

  · SÍNCRONO en la migración: los períodos del año corriente — los que se
    usan para cotizar y decidir. Con 8 períodos, ~80 s, como antes.
  · DIFERIDO: los años anteriores quedan en el parámetro
    `recalculo_pendiente` y el cron «Recálculo diferido de históricos» los
    vacía por lotes de 6 cada 10 minutos, y se apaga solo al terminar. La
    historia converge en menos de una hora sin bloquear el despliegue.

El orden dentro del diferido va del más reciente al más viejo, para que lo
que más probablemente se consulte converja primero.
"""
def migrate(cr, version):
    # Regla de la cadena: SOLO la migración más nueva recalcula. El
    # recálculo (año corriente síncrono + históricos al cron) vive hoy en
    # la 19.0.1.31.0; recalcular también aquí en un salto de varias
    # versiones pagaría el mismo build dos veces para tirar la primera.
    pass
