# -*- coding: utf-8 -*-
from . import models


def post_init_hook(env):
    """Si la base trae la clasificación de cuentas del módulo anterior
    (`qb_capacidad_costeo`), la importa para no clasificar 400 cuentas a mano
    otra vez. Sin ese módulo no hace nada."""
    env['qb.cuenta.clase'].importar_clasificacion_legada()
    _habilitar_mcp(env)


def _habilitar_mcp(env):
    """Si el servidor MCP del repo está instalado, expone los modelos del
    módulo para configurarlos y operarlos por MCP (lectura, alta, cambio y
    métodos). Sin `mcp_server` no hace nada."""
    if 'mcp.enabled.model' not in env:
        return
    Enabled = env['mcp.enabled.model'].sudo()
    for model in ('qb.centro', 'qb.cuenta.clase', 'qb.parametro',
                  'qb.periodo', 'qb.tarifa', 'qb.producto.validacion'):
        ir_model = env['ir.model']._get(model)
        if not ir_model or Enabled.with_context(active_test=False).search(
                [('model_id', '=', ir_model.id)], limit=1):
            continue
        Enabled.create({
            'model_id': ir_model.id, 'active': True, 'allow_read': True,
            'allow_create': True, 'allow_write': True,
            'allow_unlink': False, 'allow_method_calls': True,
            'notes': 'qb_costeo: habilitado al instalar.'})
