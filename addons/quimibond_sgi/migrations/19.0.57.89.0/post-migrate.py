# -*- coding: utf-8 -*-
"""57.89.0: quita de ``sgi.activity.role.approval_domain`` las hojas de campos
que el documento que se aprueba no tiene.

El 2026-09-29 se escribió a mano ``('company_id', '=', 1)`` en la condición
de seis roles «Aprueba» (D-03, «solo la empresa del SGI») sin revisar el
modelo. Tres documentos no tienen ``company_id`` (sgi.audit.program,
sgi.ppap, sgi.control.plan); la sincronización con Studio pasó esa condición
a sus reglas de aprobación y ``studio.approval.rule._get_approval_spec``
reventaba al abrir cualquier ficha de esos modelos («Invalid field
sgi.audit.program.company_id in condition»).

Solo se quitan las hojas inválidas; las demás quedan igual y un dominio que
se queda sin hojas queda vacío (la aprobación aplica siempre, como antes del
error en una base de una sola empresa). Registra antes → después de cada rol.
Idempotente. Producción (lectura por MCP, 2026-10-01): se esperan 3 roles
(1225 plan de control, 1230 PPAP, 1653 programa de auditorías) que pasan de
``[('company_id', '=', 1)]`` a vacío; 1604 (budget.analytic), 1150 y 690
(account.move) sí tienen ``company_id`` y no cambian. Las reglas de Studio
las limpia ``quimibond_sgi_studio`` 19.0.1.0.3.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    roles = env['sgi.activity.role'].with_context(active_test=False).search([('approval_domain', '!=', False)])
    changes = roles._sgi_sanitize_approval_domains()
    for role, before, after in changes:
        _logger.info("SGI 57.89.0: rol %s: %s -> %s", role.id, before, after)
    _logger.info("SGI 57.89.0: %s de %s condiciones de aprobación limpiadas", len(changes), len(roles))
