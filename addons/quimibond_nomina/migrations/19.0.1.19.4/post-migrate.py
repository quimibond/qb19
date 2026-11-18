# -*- coding: utf-8 -*-
"""1.19.4: el centinela vigila también las reglas del fondo de ahorro (FAP,
FAE, FA_EMPRESA), que desde el 18-nov-2026 excluyen a los empleados con la
etiqueta «SIN FONDO DE AHORRO». La línea base es la condición viva."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['qb.nomina.rule.sentinel']._seed(codes=('FAP', 'FAE', 'FA_EMPRESA'))
