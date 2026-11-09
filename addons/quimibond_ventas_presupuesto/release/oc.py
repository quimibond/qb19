# -*- coding: utf-8 -*-
"""OC del cliente tal como llega al pronóstico (19.0.3.15.0, E3).

El campo «OC» del pronóstico trae lo que escribía el forecast de Excel: la OC sola («12033»),
la OC con el pedido de Odoo que ya la cubre («6700/PV17173», «12033/PV17172»), el pedido al
revés («PO/14792») o el encabezado de la columna en lugar de un valor («PO», «No.parte
cliente»). Aquí se separa la OC del pedido de Odoo y se descarta lo que no es OC.

Python puro: lo prueban ``tests_puros/ventas_presupuesto/test_oc.py`` sin Odoo.
"""
import re

_PV = re.compile(r'^PV\d+$', re.IGNORECASE)
_PREFIX = re.compile(r'^(PO|OC|P\.O\.?)\s*[#:.-]?\s*', re.IGNORECASE)


def separa_oc(texto):
    """(oc, pedido_odoo) del texto del pronóstico. ``oc`` vacío si no hay una OC de verdad: una
    OC lleva al menos un dígito. ``pedido_odoo`` es el «PV…» que la acompaña, si viene."""
    oc, pedido = '', ''
    for parte in re.split(r'[/,;|]', texto or ''):
        parte = parte.strip()
        if not parte:
            continue
        if _PV.match(parte):
            pedido = pedido or parte.upper()
            continue
        parte = _PREFIX.sub('', parte).strip()
        if parte and any(c.isdigit() for c in parte) and not oc:
            oc = parte
    return oc, pedido
