# -*- coding: utf-8 -*-
"""3.2.0: el espejo en actividades nativas se apaga por default
(res.company.obligation_activity_mirror = False, decisión del CEO 2026-09-24).
Se quitan las actividades de las obligaciones abiertas para que desaparezcan
de la ficha del cliente y de los documentos en este mismo update. Va por SQL,
no por ORM: el unlink de mail.activity descartaría la obligación."""


def migrate(cr, version):
    cr.execute("""
        SELECT o.activity_id
        FROM qb_obligation o
        JOIN res_company c ON c.id = o.company_id
        WHERE o.activity_id IS NOT NULL AND o.state IN ('candidate', 'confirmed')
          AND COALESCE(c.obligation_activity_mirror, false) = false
    """)
    ids = tuple(r[0] for r in cr.fetchall())
    if not ids:
        return
    cr.execute("UPDATE qb_obligation SET activity_id = NULL WHERE activity_id IN %s", (ids,))
    cr.execute("DELETE FROM mail_activity WHERE id IN %s", (ids,))
