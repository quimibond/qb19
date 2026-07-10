# -*- coding: utf-8 -*-
from odoo import fields, models


class IotDevice(models.Model):
    _inherit = 'iot.device'

    # Antes esta URL estaba quemada, repetida, en tres archivos JS distintos
    # (mrp_revisado_telas y pesaje_rollos_tejido). Cualquier báscula nueva o
    # cambio de IP obligaba a tocar código. Ahora se configura una sola vez
    # aquí, por dispositivo, y los wizards la leen dinámicamente vía el
    # campo relacionado 'scale_read_url' del mixin (scale_wizard_mixin.py).
    scale_read_url = fields.Char(
        string="URL de Lectura (Báscula)",
        help=(
            "Endpoint HTTP(S) completo que responde al método 'scale_read' "
            "de esta báscula. Ejemplo:\n"
            "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read\n\n"
            "Debe ser accesible desde el navegador del usuario (misma red "
            "local o túnel de la IoT Box), no desde el servidor de Odoo."
        ),
    )
