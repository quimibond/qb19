# -*- coding: utf-8 -*-
from odoo import models


class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def button_start(self, *args, **kwargs):
        # Ninguna operación (Preparación/Devanado incluida) inicia sin la programación de Tintorería.
        # *args/**kwargs: Enterprise (mrp_workorder) agrega parámetros a button_start y
        # sus datos demo y Shop Floor los usan; se pasan sin tocar.
        self.production_id._check_tintoreria_programada()
        return super().button_start(*args, **kwargs)
