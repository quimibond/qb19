# -*- coding: utf-8 -*-
from . import models
from . import wizard


def post_init_hook(env):
    """Siembra el centinela con las reglas que sabemos que son registros de
    Odoo con código propio encima (SUBSIDY, INT_DAY_WAGE). La línea base es lo
    que esté vivo al instalar: el estado ya verificado contra el despacho."""
    env['qb.nomina.rule.sentinel']._seed()
    env['hr.payroll.structure.type']._qb_sembrar_dias_aguinaldo()
