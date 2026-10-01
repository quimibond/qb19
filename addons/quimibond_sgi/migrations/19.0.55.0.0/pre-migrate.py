# -*- coding: utf-8 -*-
"""55.0.0: varios términos con el mismo papel se suman; la restricción SQL
«un numerador y un denominador por indicador» ya no existe en el código y
Odoo no siempre la retira al actualizar. Idempotente."""


def migrate(cr, version):
    cr.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'sgi_indicator_term'
            ) THEN
                ALTER TABLE sgi_indicator_term DROP CONSTRAINT IF EXISTS sgi_indicator_term_role_uniq;
            END IF;
        END $$;
    """)
