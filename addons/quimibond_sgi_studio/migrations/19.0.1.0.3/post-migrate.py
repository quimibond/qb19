# -*- coding: utf-8 -*-
"""1.0.3: quita de la condición de las reglas de aprobación de Studio las
hojas de campos que su documento no tiene.

Desde el 2026-09-30 las reglas 65 (sgi.audit.program, action_approve), 66
(sgi.ppap, action_mark_enviado) y 67 (sgi.control.plan, action_set_vigente)
tenían ``[('company_id', '=', 1)]``, que la sincronización copió de su rol
SGI; esos modelos no tienen ``company_id`` y Studio reventaba al abrir sus
fichas (``_get_approval_spec`` → ``filtered_domain``). Se revisan TODAS las
reglas (activas y archivadas, del SGI o hechas a mano): solo se quitan las
hojas inválidas, nunca se borra una regla ni se cambia otra hoja; una regla
que se queda sin hojas queda sin condición. Registra antes → después.
Idempotente. Producción: se esperan 65, 66 y 67 → sin condición; 64
(budget.analytic sí tiene ``company_id``) y las demás no cambian.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    rules = env['studio.approval.rule'].with_context(active_test=False).search([('domain', '!=', False)])
    changed = 0
    for rule in rules:
        try:
            with cr.savepoint():
                for _rule, before, after in rule._sgi_sanitize_domains():
                    changed += 1
                    _logger.info("SGI Studio 1.0.3: regla %s (%s): %s -> %s",
                                 rule.id, rule.model_id.model, before, after)
        except Exception:  # noqa: BLE001 — una regla que no se deja escribir no detiene a las demás
            _logger.exception("SGI Studio 1.0.3: no se pudo limpiar la condición de la regla %s", rule.id)
    env.registry.clear_cache()
    _logger.info("SGI Studio 1.0.3: %s de %s reglas con condición limpiadas", changed, len(rules))
