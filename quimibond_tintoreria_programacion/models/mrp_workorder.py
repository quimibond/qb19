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

    def _ft_maquina_fija(self):
        # 1.6.0: el jet que asignó la Programación de Tintorería es a mano; el planificador de las fichas no lo
        # cambia por otro jet aunque esté en montaje o el artículo no tenga proceso ahí (queda solo el aviso).
        fija = super()._ft_maquina_fija()
        produccion = self.production_id
        return fija or bool(produccion.tintoreria_programada and self == produccion._workorder_tintoreria())
