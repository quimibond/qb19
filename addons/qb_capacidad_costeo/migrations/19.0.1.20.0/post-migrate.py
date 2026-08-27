# -*- coding: utf-8 -*-
"""Restar de cada pool solo lo que ese pool contiene del centro absorbido.

La resta del abono a «costos fabriles aplicados a producción» se hacía
COMPLETA contra el pool fabril, pero el pool ya no traía al centro entero:
dos exclusiones anteriores le habían quitado su parte.

  · `excluir_centros` sacó de los buckets fabriles las cuentas etiquetadas al
    centro absorbido. En TEJIDO son ENERGETICOS (504.01.0002, $119k/mes) y
    AGUJADOS (504.01.0007, $60k/mes), las dos clasificadas con su centro.
  · `renta_centros` dejó fuera su renta contractual ($284,269/mes). La renta
    del GL (504.01.0008) está en `no_costeo` y nunca estuvo en el pool.

La tarifa por hora, en cambio, capitaliza el costo COMPLETO del centro —esas
tres partidas incluidas—, así que restar el abono entero las quitaba por
segunda vez: ~$463k/mes con la tarifa de sep-2026, cerca del 12% del pool que
se reparte a Acabado, Tintorería y Entretelas. El `max(…, 0)` no lo detectaba
porque el pool seguía siendo positivo.

Ahora se resta el REMANENTE: bruto − lo que las exclusiones ya quitaron. Lo
que queda es lo que el centro aportaba por cuentas SIN etiquetar (nómina de
501.06, indirectos genéricos de 504.01, depreciación de 504.08), que es lo
único que ninguna exclusión podía sacar. Las dos mitades quedan guardadas en
el período (`absorcion_bruta_month`, `absorcion_ya_fuera_month`) y el panel
avisa si la resta neta cae a 0 teniendo bruto.

De paso, `renta_by_month` también excluye a los centros absorbidos: se resta
de `fab_by_month`, que ya los excluía, y sin eso una cuenta de renta
etiquetada a un centro absorbido se restaba de un pool que ya no la traía.

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
