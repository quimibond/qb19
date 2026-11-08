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

    def button_finish(self):
        # 1.8.2 (prueba de punta a punta en qbtesting, 2026-11-08): terminar una operación sin haberla iniciado
        # (botón de terminar directo, Shop Floor, RPC) se saltaba la regla de arriba y dejaba la orden «iniciada»
        # sin programación, ya sin poder programarse. Se exige lo mismo que para iniciar. Las operaciones que ya
        # están hechas o canceladas no se revisan (el cierre nativo de la orden vuelve a llamar este método).
        self.filtered(lambda wo: wo.state not in ('done', 'cancel')).production_id._check_tintoreria_programada()
        return super().button_finish()

    def _ft_maquina_fija(self):
        # 1.6.0: el jet que asignó la Programación de Tintorería es a mano; el planificador de las fichas no lo
        # cambia por otro jet aunque esté en montaje o el artículo no tenga proceso ahí (queda solo el aviso).
        fija = super()._ft_maquina_fija()
        produccion = self.production_id
        return fija or bool(produccion.tintoreria_programada and self == produccion._workorder_tintoreria())
