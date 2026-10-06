# -*- coding: utf-8 -*-
"""D-22: ``delete_record`` sobre un modelo ``sgi.*`` se rechaza con un mensaje
que dice qué hacer (archivar), no con el genérico de ``mcp_server``.

La puerta ``check_model_operation_enabled`` (``mcp_enabled_model.py``) ya
niega ``unlink`` en ``sgi.*`` para todas las vías (herramientas, XML-RPC,
``call_model_method`` mapeado a ``unlink``). Aquí solo se cambia el mensaje de
la herramienta: se revisa antes de ``super()``, así no depende de que el MCP
esté encendido ni de la casilla.
"""
from odoo import models
from odoo.exceptions import AccessError

from . import politica


class McpMixin(models.AbstractModel):
    _inherit = 'mcp.mixin'

    def _check_op(self, model, operation):
        if politica.es_sin_borrado(model, operation):
            raise AccessError(politica.MENSAJE_SIN_BORRADO)
        return super()._check_op(model, operation)
