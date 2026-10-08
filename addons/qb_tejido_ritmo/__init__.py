# -*- coding: utf-8 -*-
from . import models
from . import wizards


def post_init_hook(env):
    """Expone los modelos por MCP si el servidor del repo está instalado."""
    if 'mcp.enabled.model' not in env:
        return
    Enabled = env['mcp.enabled.model'].sudo()
    for model in ('qb.tejido.descanso', 'qb.tejido.intervalo',
                  'qb.tejido.paro.general', 'qb.tejido.ritmo'):
        ir_model = env['ir.model']._get(model)
        if not ir_model or Enabled.with_context(active_test=False).search(
                [('model_id', '=', ir_model.id)], limit=1):
            continue
        Enabled.create({
            'model_id': ir_model.id, 'active': True, 'allow_read': True,
            'allow_create': True, 'allow_write': True,
            'allow_unlink': False, 'allow_method_calls': True,
            'notes': 'qb_tejido_ritmo: habilitado al instalar.'})
