# -*- coding: utf-8 -*-
from . import models
from . import wizard


def post_init_hook(env):
    """Al instalar: puestos por omisión (si existen con ese nombre), modelos
    en MCP e importación de las cotizaciones del módulo anterior (solo lectura
    de `qb.cotizacion`; nunca se modifica)."""
    env['qb.cotizador.cotizacion']._configurar_puestos_por_omision()
    _habilitar_mcp(env)
    env['qb.cotizador.cotizacion'].importar_legadas()


def _habilitar_mcp(env):
    """Expone los modelos por MCP si el servidor del repo está instalado."""
    if 'mcp.enabled.model' not in env:
        return
    Enabled = env['mcp.enabled.model'].sudo()
    for model in ('qb.cotizador.cotizacion', 'qb.cotizador.tramo',
                  'qb.cotizador.motivo'):
        ir_model = env['ir.model']._get(model)
        if not ir_model or Enabled.with_context(active_test=False).search(
                [('model_id', '=', ir_model.id)], limit=1):
            continue
        Enabled.create({
            'model_id': ir_model.id, 'active': True, 'allow_read': True,
            'allow_create': True, 'allow_write': True,
            'allow_unlink': False, 'allow_method_calls': True,
            'notes': 'qb_cotizador: habilitado al instalar.'})
