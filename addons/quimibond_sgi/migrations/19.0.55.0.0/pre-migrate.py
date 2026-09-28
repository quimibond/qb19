# -*- coding: utf-8 -*-
"""55.0.0: varios términos con el mismo papel se suman; la restricción SQL
«un numerador y un denominador por indicador» ya no existe en el código y
Odoo no siempre la retira al actualizar. Idempotente."""


def migrate(cr, version):
    cr.execute("ALTER TABLE sgi_indicator_term DROP CONSTRAINT IF EXISTS sgi_indicator_term_role_uniq")
