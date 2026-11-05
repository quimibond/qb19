# -*- coding: utf-8 -*-
from odoo import fields, models


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    rollos_alerta_metros_insp = fields.Float(
        'Alerta 1: diferencia de metros contra Inspección (m)', digits=(12, 2),
        config_parameter='quimibond_rollos_bitacora.alerta_metros_insp', default=1.0)
    rollos_alerta_diferencia_pct = fields.Float(
        'Alerta 2: diferencia de metros contra el teórico (%)', digits=(12, 2),
        config_parameter='quimibond_rollos_bitacora.alerta_diferencia_pct', default=3.0)
    rollos_etiqueta_rama_ancho = fields.Integer(
        'Etiqueta de Rama: ancho (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_rama_ancho', default=544)
    rollos_etiqueta_prelim_ancho = fields.Integer(
        'Etiqueta preliminar: ancho (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_prelim_ancho', default=544)
    rollos_etiqueta_prelim_alto = fields.Integer(
        'Etiqueta preliminar: alto (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_prelim_alto', default=304)
    rollos_etiqueta_rama_alto = fields.Integer(
        'Etiqueta de Rama: alto (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_rama_alto', default=304)
    rollos_etiqueta_final_ancho = fields.Integer(
        'Etiqueta final: ancho (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_final_ancho', default=608)
    rollos_etiqueta_final_alto = fields.Integer(
        'Etiqueta final: alto (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_final_alto', default=816)
    rollos_inspeccion_imprime = fields.Selection(
        [('preliminar', 'Etiqueta preliminar (68 x 38 mm)'), ('final', 'Etiqueta final (76 x 102 mm)')],
        'Etiqueta que imprime la revisadora al confirmar',
        config_parameter='quimibond_rollos_bitacora.inspeccion_imprime', default='preliminar')
