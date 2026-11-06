# -*- coding: utf-8 -*-
"""1.18.0: días de aguinaldo por grupo de nómina (25 la planta semanal, 16 los
quincenales). Siembra el campo nuevo solo donde quedó en cero."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['hr.payroll.structure.type']._qb_sembrar_dias_aguinaldo()
