# -*- coding: utf-8 -*-
"""Homologar las ventanas de los promedios.

Dos correcciones a cómo se promedian los pools.

**1. El denominador son los meses de la VENTANA, no los meses con factura.**
La renta y la energía se registran al pagarse, no al devengarse: la renta
oscila entre $506k y $1,490k al mes contra un contrato de ~$1,065k, y la
energía entre $53k y $173k según cuándo llegó el recibo. Dividir entre los
meses en que la cuenta tuvo movimiento daba el cargo por recibo, no el costo
mensual. Los meses negativos —el reverso del cierre anual, que en diciembre
2025 metió +$163M de débito a cuentas de ingreso— salen ahora de los DOS
lados de la división: dejarlos solo en el denominador subvaluaba el promedio
tanto como dejarlos en el numerador lo hundía.

**2. La ventana del pool fabril arranca en el corte de absorción.** Con un
centro migrando a absorción por workcenter, promediar doce meses mezcla
regímenes: los meses anteriores al corte llevan el gasto del centro completo y
los posteriores no. El factor de septiembre tiene que describir a septiembre.
Sale ruidoso el primer mes —el panel lo avisa— y se estabiliza solo.

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
