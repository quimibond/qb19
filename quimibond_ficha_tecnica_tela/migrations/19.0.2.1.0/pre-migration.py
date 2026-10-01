# -*- coding: utf-8 -*-
"""Antes de que el ORM convierta Char -> Many2one, aparta las columnas de
texto (maquina_tejido, jefe_manufactura, auxiliar_procesos) para que Odoo
no las convierta a entero (ej. '31' -> id 31 inexistente => FK violation).
Los valores se re-vinculan en post-migration.py."""

COLUMNS = ('maquina_tejido', 'jefe_manufactura', 'auxiliar_procesos')
TABLE = 'ficha_tecnica_tejido'


def migrate(cr, version):
    for col in COLUMNS:
        cr.execute("""
            SELECT data_type FROM information_schema.columns
             WHERE table_name = %s AND column_name = %s
        """, (TABLE, col))
        row = cr.fetchone()
        if not row or row[0] not in ('character varying', 'text'):
            continue  # no existe o ya es entero: nada que hacer
        cr.execute('ALTER TABLE "%s" DROP COLUMN IF EXISTS "%s_old"' % (TABLE, col))
        cr.execute('ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s_old"' % (TABLE, col, col))
