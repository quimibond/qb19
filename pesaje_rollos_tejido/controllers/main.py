# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request

class IotScaleBridgeController(http.Controller):

    @http.route('/quimibond/scale/read_weight', type='jsonrpc', auth='user', cors='*')
    def read_local_scale_weight(self, **kwargs):
        """
        Ruta inactiva en el servidor. La lectura se realiza de forma directa
        en el cliente (JavaScript frontend) para evitar restricciones de red.
        """
        return {'status': 'inactive', 'weight': 0.0}