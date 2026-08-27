# -*- coding: utf-8 -*-
"""Sacar el precio del costo: la operación se reparte sobre producción.

19.0.1.14.0 cambia el driver del costo REPORTADO de operación. Antes era
`op = op_pct × precio`: el costo dependía del precio, así que el mismo
producto vendido a la mitad «costaba» la mitad de operación y su margen se
veía sano. Y un producto sin ventas en el mes tenía precio 0 y por lo tanto
operación 0 — justo los que hay que evaluar para decidir si vale la pena
empujarlos.

Ahora la operación se reparte sobre el costo de producción, que no se mueve
con el descuento del vendedor. El parámetro `op_driver` lo revierte a
"ventas" si se prefiere el reparto anterior.

El piso de precio del cotizador NO cambia: ahí `op_pct` sobre la venta sigue
siendo lo correcto, porque el piso a planta llena resuelve qué precio deja
cubierta una operación que es porcentaje de la venta. La circularidad ahí es
la fórmula, no un error.

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
