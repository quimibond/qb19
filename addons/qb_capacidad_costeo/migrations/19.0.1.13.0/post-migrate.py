# -*- coding: utf-8 -*-
"""Recalcular con capacidad normal en el denominador (costeo normal, IAS 2).

19.0.1.13.0 deja de dividir el pool fijo entre la producción real del mes y
lo divide entre la capacidad NORMAL del centro. Dividir entre producción le
carga la ociosidad al producto: un mes flojo lo encarece, y el modelo
entonces recomienda subir el precio justo cuando lo que hace falta es vender
más. El README del módulo ya prometía IAS 2 y la vista `qb.ociosidad` ya lo
hacía así — era el motor el que no estaba de acuerdo con las otras dos
mitades.

La capacidad normal se lee de `qb.ociosidad`, que la deriva igual que el
campo promete: `capacidad_normal` capturada, o calendario real × throughput
nominal. Un centro sin capacidad derivable cae a su producción real, así que
el cambio degrada con gracia.

De paso corrige el denominador de la ENERGÍA, que es variable y por lo tanto
va sobre los kilos realmente producidos: con capacidad normal, un mes al 60%
de utilización habría dado una energía por kilo 40% baja.

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
