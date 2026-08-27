# -*- coding: utf-8 -*-
"""Simetría de la renta de entretelas + medir el camino al costeo por ruta.

19.0.1.16.0 corrige una asimetría que dejó el arreglo de la renta
(19.0.1.10.0): la renta contractual se sumaba al pool solo para los centros
NO-entretela, pero el pool de entretelas —que sí incluye su renta— se restaba
completo del pool de tela. Tela terminaba pagando una renta que nunca se le
sumó, y su factor $/kg salía bajo.

Ahora la renta contractual entra al total de TODOS los centros fabriles y lo
que entretelas se lleva sale después con su pool propio. El overhead extra
capturado a mano para entretelas dejó de restarse de tela: nunca estuvo en la
bolsa común.

Agrega además `fab_pool_con_centro_pct`, que no cambia ningún número: mide qué
parte del gasto fabril tiene centro de costo asignado. Es el prerrequisito
para costear por ruta real, y hasta que suba, la fabricación solo se puede
repartir a nivel planta.

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
