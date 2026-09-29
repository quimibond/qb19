# -*- coding: utf-8 -*-
"""56.1.1: pantallas «Mi procedimiento» guardadas con empleado y sin puesto
(abiertas desde Mi equipo) reciben el puesto del empleado. Idempotente."""


def migrate(cr, version):
    cr.execute("""
        UPDATE sgi_my_procedure w
           SET job_id = v.job_id
          FROM hr_employee e
          JOIN hr_version v ON v.id = e.current_version_id
         WHERE w.employee_id = e.id AND w.job_id IS NULL AND v.job_id IS NOT NULL
    """)
