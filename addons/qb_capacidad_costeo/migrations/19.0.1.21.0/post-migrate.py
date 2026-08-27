# -*- coding: utf-8 -*-
"""Los totales de margen se derivan del ingreso, no de `precio × qty`.

Una fila cuya cantidad NETA del período es ≤ 0 —las devoluciones superaron a
las ventas— se trata como «sin ventas»: sin eso el precio promedio saldría
negativo y envenenaría los márgenes unitarios y las alertas. Pero
`ventas_total` sí conserva el ingreso negativo, porque es el hecho contable.

El resultado era una fila con ventas de −$242,363 y margen de $0, y con ella
la identidad `ventas − costo = margen` dejaba de cumplirse. La conciliación
contra el mayor lo veía como gasto sin explicar: 11 filas metieron $561,866
de residuo entre enero y julio de 2026 que no correspondía a ninguna causa
real.

Derivar el total del ingreso (`revenue − costo × qty_efectiva`) es
algebraicamente idéntico en toda fila con precio válido —revenue es
precio × qty— y arregla justo las filas de devolución neta. El guard del
precio se conserva: esas filas siguen sin precio ni margen unitario.

El recálculo de los períodos lo hace la migración MÁS NUEVA de la cadena,
una sola vez y con todos los cambios de datos ya aplicados: recalcular en
cada una dejaba ~130,600 recálculos de producto por build para un
resultado que la siguiente migración pisaba enseguida.
"""


def migrate(cr, version):
    """No-op: este cambio es de MOTOR, no de datos.

    El archivo existe para dejar registro de qué cambió en esta versión y
    para que la cadena de migraciones no tenga huecos. El recálculo que
    aplica el cambio lo hace la migración más nueva, una sola vez.
    """
