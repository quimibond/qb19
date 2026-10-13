# -*- coding: utf-8 -*-
"""19.0.2.4.0 — Ficha de la máquina para cualquier tipo de máquina.

Dirección General 2026-10-13: tipos de máquina con sus parámetros y un valor
por parámetro en cada configuración. Este paso solo expone al MCP los modelos
de la ficha de la máquina (tipos, parámetros, configuraciones y valores) con
lectura, alta y edición, para cargar las fichas desde Claude. Si un modelo ya
estaba habilitado, solo abre lo que falte (nunca quita un permiso dado a
mano). No asigna tipos a las máquinas ni crea configuraciones: eso se captura.
En producción no había configuraciones (2026-10-13). Idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MCP_MODELS = ('ficha.tecnica.maquina.tipo', 'ficha.tecnica.maquina.parametro',
              'ficha.tecnica.maquina.config', 'ficha.tecnica.maquina.valor')
WANTED = ('active', 'allow_read', 'allow_create', 'allow_write')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    if 'mcp.enabled.model' not in env:
        return
    Enabled = env['mcp.enabled.model'].sudo().with_context(active_test=False)
    wanted = [name for name in WANTED if name in Enabled._fields]
    for model_name in MCP_MODELS:
        model = env['ir.model'].sudo().search([('model', '=', model_name)], limit=1)
        if not model:
            continue
        existing = Enabled.search([('model_id', '=', model.id)], limit=1)
        if existing:
            vals = {name: True for name in wanted if not existing[name]}
            if vals:
                existing.write(vals)
                _logger.info("ficha_tecnica_tela 2.4.0: MCP %s, se abrió %s.", model_name, sorted(vals))
            continue
        vals = dict({name: True for name in wanted}, model_id=model.id)
        if 'notes' in Enabled._fields:
            vals['notes'] = "quimibond_ficha_tecnica_tela 2.4.0: ficha de la máquina."
        Enabled.create(vals)
        _logger.info("ficha_tecnica_tela 2.4.0: MCP %s habilitado.", model_name)
