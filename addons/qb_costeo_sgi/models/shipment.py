# -*- coding: utf-8 -*-
"""1.3.0 (Jose 2026-10-08, 5.6): la aprobación de la muestra que Ventas registra en el envío
(`sgi.dev.shipment`, respuesta «aprueba») es la aprobación del cliente de la cotización del
desarrollo: se copia a sus cotizaciones vivas y, si alguna ya está ganada, el precio va a la
tarifa del cliente (qb_cotizador). Nada se captura dos veces."""
from odoo import models


class SgiDevShipmentCotizador(models.Model):
    _inherit = 'sgi.dev.shipment'

    def action_register_response(self):
        res = super().action_register_response()
        for ship in self.filtered(lambda s: s.response == 'aprueba' and s.project_id):
            ship.project_id._qb_sgi_cliente_aprobo(
                ship.response_medium, ship.response_date, contact=ship.response_contact_id,
                ref=ship.response_ref, evidence=ship.response_file, filename=ship.response_filename)
        return res
