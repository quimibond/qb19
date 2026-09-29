# -*- coding: utf-8 -*-
from . import models


def post_init_hook(env):
    """Base nueva: MA-03 (sembrado en «manual» por el núcleo desde 57.10.0)
    pasa al modo automático «calidad_pq», como lo sembraba el núcleo. Solo si
    sigue en manual y sin mediciones: nunca pisa lo que MAST decidió
    (B-001). En producción el módulo ya estaba instalado y esto no corre."""
    indicator = env.ref('quimibond_sgi.sgi_ind_calidad_pq', raise_if_not_found=False)
    if indicator:
        indicator._sgi_seed_calidad_pq()
