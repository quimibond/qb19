# -*- coding: utf-8 -*-
"""57.88.0: recalcula el «Mi procedimiento» guardado de todos los empleados.

Hasta la 57.87.0, ``hr.employee._compute_sgi_mp_roles_stored`` asignaba
``.ids`` a los cuatro many2many guardados (``sgi_mp_role_ids``,
``sgi_mp_received_role_ids``, ``sgi_mp_short_role_ids``,
``sgi_mp_process_ids``). En Odoo 19 una lista vacía es una lista de comandos
vacía: no cambia nada. Así, la lista que debía quedar vacía (se archivó la
última actividad o el proceso, el puesto perdió su último escalamiento o su
último «participa», el empleado cambió a un puesto sin roles) conservaba lo
de antes. Desde 57.88.0 se asignan recordsets; esta migración recalcula a
todos (activos y archivados) y registra cuántos cambiaron, lista por lista.

Idempotente: una segunda corrida no cambia nada. Producción (lectura por MCP,
2026-10-01): ningún empleado con un rol de actividad archivada ni con un
proceso archivado guardados, y los 83 puestos de los 129 empleados con roles
tienen lista no vacía; se espera 0 o muy pocos cambios (escalamientos o
«participa» que se quedaron sin reemplazo).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_FIELDS = ('sgi_mp_role_ids', 'sgi_mp_received_role_ids', 'sgi_mp_short_role_ids', 'sgi_mp_process_ids')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Employee = env['hr.employee']
    # Se lee sin active_test (empleados y procesos archivados incluidos); el
    # cálculo corre en el entorno normal, como en el uso diario.
    employees = Employee.with_context(active_test=False).search([])
    before = {f: {e.id: set(e[f].ids) for e in employees} for f in _FIELDS}
    for fname in _FIELDS:
        env.add_to_compute(Employee._fields[fname], employees)
    env.flush_all()
    env.invalidate_all()
    employees = Employee.with_context(active_test=False).browse(employees.ids)
    changed = set()
    for fname in _FIELDS:
        diffs = [(e.id, len(before[fname][e.id]), len(e[fname]))
                 for e in employees if set(e[fname].ids) != before[fname][e.id]]
        changed.update(d[0] for d in diffs)
        _logger.info("SGI 57.88.0: %s cambió en %d empleados%s", fname, len(diffs),
                     (" (id: antes→después) " + ", ".join("%s: %s→%s" % d for d in diffs[:50]))
                     if diffs else "")
    _logger.info("SGI 57.88.0: «Mi procedimiento» guardado recalculado para %d empleados; "
                 "%d con alguna lista distinta.", len(employees), len(changed))
