# -*- coding: utf-8 -*-
from . import models


def post_init_hook(env):
    Cot = env['qb.cotizador.cotizacion']
    Cot._qb_sgi_apuntar_entregables()
    Cot._qb_sgi_ligar_legadas()
