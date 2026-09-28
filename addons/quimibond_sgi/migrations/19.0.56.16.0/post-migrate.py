# -*- coding: utf-8 -*-
"""56.16.0 (2026-09-28): líneas de negocio («Aplica a»).

Primeras etiquetas en «C2 Pedido a entrega», que mezcla pedido industrial,
de confección y exportación:

- C2.04 (pedido industrial con release) → Industrial.
- C2.08 (pedido de confección con precio de lista) → Confección.
- Etapa «E. Exportación» (C2.28 a C2.32) → Exportación.

Y las líneas de dos puestos cuyo nombre no deja duda:

- ATENCION A CLIENTES Y VENDEDORES CONFECCION → Confección + Nacional.
- COORDINADOR DE VENTAS INDUSTRIAL → Industrial.

Solo agrega: una actividad o un puesto que ya tenga líneas no se toca, no se
borra nada y se puede correr dos veces. Lo demás lo etiqueta MAST."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

ACTIVITY_SCOPES = {
    'C2.04': ['sgi_scope_industrial'],
    'C2.08': ['sgi_scope_confeccion'],
}
STAGE_SCOPES = {('C2', 'E'): ['sgi_scope_exportacion']}
JOB_SCOPES = {
    'ATENCION A CLIENTES Y VENDEDORES CONFECCION': ['sgi_scope_confeccion', 'sgi_scope_nacional'],
    'COORDINADOR DE VENTAS INDUSTRIAL': ['sgi_scope_industrial'],
}


def _scopes(env, names):
    scopes = env['sgi.activity.scope']
    for name in names:
        scopes |= env.ref('quimibond_sgi.%s' % name, raise_if_not_found=False) or scopes.browse()
    return scopes


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    Activity = env['sgi.process.activity']
    tagged = 0
    for number, names in ACTIVITY_SCOPES.items():
        scopes = _scopes(env, names)
        for activity in Activity.search([('number', '=', number), ('scope_ids', '=', False)]):
            activity.scope_ids = scopes
            tagged += 1
    for (process_code, stage_code), names in STAGE_SCOPES.items():
        scopes = _scopes(env, names)
        for activity in Activity.search([('process_id.code', '=', process_code),
                                         ('stage_id.code', '=', stage_code),
                                         ('scope_ids', '=', False)]):
            activity.scope_ids = scopes
            tagged += 1
    jobs = 0
    for job_name, names in JOB_SCOPES.items():
        for job in env['hr.job'].search([('name', '=', job_name), ('sgi_scope_ids', '=', False)]):
            job.sgi_scope_ids = _scopes(env, names)
            jobs += 1
    _logger.info("SGI 56.16.0: %d actividades y %d puestos con línea de negocio.", tagged, jobs)
