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
        config_parameter='quimibond_rollos_bitacora.etiqueta_rama_ancho', default=406)
    rollos_etiqueta_prelim_ancho = fields.Integer(
        'Etiqueta preliminar: ancho (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_prelim_ancho', default=812)
    rollos_etiqueta_prelim_alto = fields.Integer(
        'Etiqueta preliminar: alto (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_prelim_alto', default=609)
    rollos_etiqueta_rama_alto = fields.Integer(
        'Etiqueta de Rama: alto (puntos ZPL)',
        config_parameter='quimibond_rollos_bitacora.etiqueta_rama_alto', default=203)
