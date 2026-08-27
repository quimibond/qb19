# -*- coding: utf-8 -*-
"""Recalcular tras dos correcciones de precisión.

19.0.1.15.0 arregla dos cosas que cambian números ya calculados:

1. El dedup de cantidades colapsaba CUALQUIER repetición de (factura,
   producto, cantidad). Dos rollos iguales en una misma factura se contaban
   como uno: la cantidad se partía a la mitad y el precio promedio salía al
   doble. Ahora colapsa solo grupos de tres o más líneas, que es la firma del
   triplete de facturación (lista / descuento / neta).

   Medido sobre ene–ago 2026 el dedup viejo no llegó a descartar nada —la
   diferencia entre la cantidad del mayor y la del modelo era exactamente la
   de las notas de crédito, mes por mes— así que este arreglo quita un riesgo
   latente sin mover el histórico reciente.

2. `_explode_bom` recorría todas las líneas de la receta sin filtrar por
   variante. En una receta con atributos, el producto cargaba componentes que
   no consume: su MP salía inflada por todo lo de sus variantes hermanas.

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
