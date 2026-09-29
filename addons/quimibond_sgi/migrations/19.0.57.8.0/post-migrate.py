# -*- coding: utf-8 -*-
"""57.8.0 (entrega 2 de la auditoría, e2-codigo-muerto; B-006).

``sgi.employer.obligation`` se retiró en 56.15.0, pero quedaron **15 filas de
``ir_model_data``** de OTROS módulos (``ai``, ``calendar``, ``mail``,
``mail_mobile``, ``mass_mailing``, ``portal``, ``rating``, ``sms``,
``snailmail``, ``web_studio``, ``whatsapp``) que apuntan a la herencia del
modelo (``model_inherit__sgi_employer_obligation__*``, ``ir.model.inherit``) y
a opciones de selección (``selection__sgi_employer_obligation__*``,
``ir.model.fields.selection``).

Aquí se borran **solo esas filas de ``ir_model_data``**: no se toca ningún
dato de negocio, ni la tabla ``sgi_employer_obligation``, ni los registros de
``ir.model.inherit`` o ``ir.model.fields.selection`` a los que apuntaban.
Idempotente (la segunda corrida borra 0).

Producción el 2026-09-29 (MCP, solo lectura): 15 filas (10
``ir.model.inherit`` con res_id 1050/1051 y 5 ``ir.model.fields.selection``
con res_id 18139-18143), ninguna del módulo ``quimibond_sgi``.
"""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute(r"""
        DELETE FROM ir_model_data
         WHERE module <> 'quimibond_sgi'
           AND model IN ('ir.model.inherit', 'ir.model.fields.selection')
           AND (name LIKE 'model\_inherit\_\_sgi\_employer\_obligation\_\_%'
                OR name LIKE 'selection\_\_sgi\_employer\_obligation\_\_%')
     RETURNING module, name""")
    rows = cr.fetchall()
    _logger.info("SGI 57.8.0 (B-006): %d fila(s) de ir_model_data de sgi.employer.obligation "
                 "borradas (esperado en producción: 15): %s", len(rows), rows)
