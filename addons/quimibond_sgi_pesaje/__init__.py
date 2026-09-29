# -*- coding: utf-8 -*-
from . import models

TOLERANCE_KEY = 'quimibond_sgi.pesaje_tolerance_kg'


def post_init_hook(env):
    """Siembra la tolerancia de peso de rollo si no existe (idempotente, sin
    XML ID). Hasta quimibond_sgi 57.9.0 la sembraba el núcleo; desde 57.10.0
    (A-020) es de este módulo. La clave no cambia. En producción el módulo ya
    estaba instalado y el parámetro ya existe: esto no corre."""
    from .models.mrp_weigh_wizard import DEFAULT_TOLERANCE_KG
    Param = env['ir.config_parameter'].sudo()
    if Param.get_param(TOLERANCE_KEY) is False:
        Param.set_param(TOLERANCE_KEY, str(DEFAULT_TOLERANCE_KG))
