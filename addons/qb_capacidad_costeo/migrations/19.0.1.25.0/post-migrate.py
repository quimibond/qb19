# -*- coding: utf-8 -*-
"""Solo la merma es costo; los ajustes de cantidad no.

`501.01.02 COSTO POR AJUSTES A CANTIDAD` mezcla naturalezas distintas:

  SP/10758              scrap de Odoo  -> merma real, SÍ es costo
  TL/EMB/04840          embarque       -> ajuste, NO
  TL/ENC//00103         encogimiento   -> ajuste, NO
  TVAR/ENT-REF/00471    refacciones    -> ajuste, NO

Entraba entera al bucket `mp`, que alimenta el ajuste de MP. Un asiento de
regularización de diciembre de 2025 —$5,822,686, «Merma no contabilizada
(1,136 scraps sin asiento)»— subió `mp_ajuste` de 0.781 a 0.903: +15.6% de
costo de materia prima en TODOS los productos, con los ajustes adentro.

`filtro_etiqueta` en la clasificación deja pasar solo las líneas cuyo
concepto contenga el texto. Es un filtro de LÍNEA: la clasificación sigue
siendo por cuenta y esto acota qué parte de ella entra. Vacío = toda la
cuenta, que es lo normal y el default.

La clasificación específica de `501.01.02%` con `SP/` viene en los seeds y
gana por patrón más largo sobre `501.01%`.

Se recalculan los períodos abiertos. Los cerrados se respetan.
"""
def migrate(cr, version):
    """No-op: este cambio es de MOTOR y de un campo, no de datos.

    La clasificación de `501.01.02%` con `SP/` la crea el seed. El recálculo
    lo hace la migración más nueva de la cadena, una sola vez.
    """
