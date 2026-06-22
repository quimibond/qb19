# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
import requests
import json
import logging

_logger = logging.getLogger(__name__)

class IotScaleBridgeController(http.Controller):

    @http.route('/quimibond/scale/read_weight', type='jsonrpc', auth='user', cors='*')
    def read_local_scale_weight(self, **kwargs):
        """
        Puente en Python (Servidor Odoo SH) para consultar el Virtual IoT Box local
        sin restricciones de CORS de Google Chrome.
        """
        # Reemplazar la IP privada por el subdominio HTTPS que Odoo IoT te asigna:
        url = "https://192-168-100-30.3991e8c5.odoo-iot.com:8069/hw_proxy/perform_action"
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "params": {
                "action": "read_scale"
            }
        }
        
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=1.5)
            if response.status_code == 200:
                res_data = response.json()
                if 'result' in res_data:
                    result = res_data['result']
                    # Extractor robusto por si Odoo lo manda en weight o value
                    weight = result.get('weight') if result.get('weight') is not None else result.get('value', 0.0)
                    return {'status': 'success', 'weight': float(weight)}
            return {'status': 'error', 'weight': 0.0}
        except Exception as e:
            # En caso de desconexión del Virtual IoT Box, retorna 0 de forma segura sin romper la pantalla
            _logger.warning("Virtual IoT Box no disponible en loopback: %s", str(e))
            return {'status': 'timeout', 'weight': 0.0}