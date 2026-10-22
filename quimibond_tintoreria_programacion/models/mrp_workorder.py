# -*- coding: utf-8 -*-
from odoo import models


class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def button_start(self, raise_on_invalid_state=False):
        # Ninguna operación (Preparación/Devanado incluida) inicia sin la programación de Tintorería.
        self.production_id._check_tintoreria_programada()
        return super().button_start(raise_on_invalid_state=raise_on_invalid_state)
