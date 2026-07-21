# -*- coding: utf-8 -*-
"""Mini-fase 5.5 (P-A28 Rev.15): el pronóstico es un documento vivo y ya no se
aprueba, su estado terminal es 'revisado'. Los pronósticos existentes que
quedaron en 'aprobado' pasan a 'revisado' (SQL directo: no dispara constraints ni
el candado del ORM; el valor de columna es válido de inmediato)."""


def migrate(cr, version):
    cr.execute(
        "UPDATE sgi_sales_budget SET state = 'revisado' "
        "WHERE kind = 'pronostico' AND state = 'aprobado'")
