# -*- coding: utf-8 -*-
"""56.22.0 (2026-09-28):

- Eficiencias de personal: los jefes de departamento (usuario del jefe en
  hr.department) entran al grupo «Captura de eficiencias». Los supervisores
  que no son jefe de departamento en Odoo se agregan a mano.
- C5.19: el equipo de venta «Industrial» responde reclamaciones en 20 días
  hábiles; los demás quedan en 5 (valor por omisión del campo).
- MCP: los modelos de seguridad y de vistas (ir.model.access, ir.rule,
  ir.ui.view, ir.model.data) quedan de solo lectura por MCP. Es configuración
  del módulo mcp_server (de terceros, no se toca su código); si no está
  instalado, no hace nada.
"""
import logging

from odoo import SUPERUSER_ID, api

MCP_READ_ONLY = ('ir.model.access', 'ir.rule', 'ir.ui.view', 'ir.model.data')

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    group = env.ref('quimibond_sgi.group_sgi_efficiency_capture', raise_if_not_found=False)
    if group:
        managers = env['hr.department'].search([('manager_id.user_id', '!=', False)]).manager_id.user_id
        managers = managers.filtered(lambda u: u.active and not u.share) - group.user_ids
        if managers:
            group.write({'user_ids': [(4, u.id) for u in managers]})
        _logger.info("SGI 56.22.0: %d jefe(s) de departamento en «Captura de eficiencias»: %s",
                     len(managers), ", ".join(managers.mapped('name')))
    teams = env['crm.team'].with_context(active_test=False).search([('name', '=ilike', 'industrial')])
    teams.write({'sgi_complaint_response_days': 20})
    _logger.info("SGI 56.22.0: %d equipo(s) Industrial con 20 días hábiles para responder reclamaciones.", len(teams))

    if 'mcp.enabled.model' in env:
        enabled = env['mcp.enabled.model'].with_context(active_test=False).search(
            [('model_name', 'in', MCP_READ_ONLY)])
        enabled.write({'allow_read': True, 'allow_create': False, 'allow_write': False,
                       'allow_unlink': False, 'allow_method_calls': False})
        _logger.info("SGI 56.22.0: MCP de solo lectura en %s.", ", ".join(enabled.mapped('model_name')))
