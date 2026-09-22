# -*- coding: utf-8 -*-
"""Fase 1 del catálogo — antes de cargar los modelos nuevos.

1. La revisión del documento pasa de texto a entero. Odoo no convierte
   char → integer: renombraría la columna vieja y dejaría la nueva vacía. Se
   convierte aquí; lo que no sea un número («A») se guarda en
   sgi_revision_legacy para capturarlo a mano.
2. La nueva revisión de la solicitud de cambio documental, igual.
3. La clave del proceso deja de ser única global y pasa a ser única por
   empresa: se quita la restricción vieja (Odoo no borra restricciones que
   desaparecen del código).
4. «Puestos responsables» pasa a calcularse desde los roles, sobre la misma
   tabla de relación. Se respalda antes: si Odoo la recalculara al cargar el
   modelo (con los roles aún vacíos) se perdería lo que el post-migrate
   convierte en roles.
"""
import logging

_logger = logging.getLogger(__name__)

_NUMERIC = r"^\s*\d+\s*$"


def _column_type(cr, table, column):
    cr.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, column))
    row = cr.fetchone()
    return row and row[0]


def _to_integer(cr, table, column, legacy_column=None):
    data_type = _column_type(cr, table, column)
    if not data_type or data_type == 'integer':
        return
    if legacy_column:
        cr.execute('ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" varchar'
                   % (table, legacy_column))
        cr.execute('UPDATE "{t}" SET "{l}" = "{c}" WHERE "{c}" IS NOT NULL '
                   'AND trim("{c}") <> \'\' AND "{c}" !~ %s'.format(
                       t=table, c=column, l=legacy_column), (_NUMERIC,))
        if cr.rowcount:
            _logger.warning(
                "SGI fase 1: %d %s.%s no numéricos quedaron en %s.",
                cr.rowcount, table, column, legacy_column)
    cr.execute(
        'ALTER TABLE "{t}" ALTER COLUMN "{c}" TYPE integer USING '
        '(CASE WHEN "{c}" ~ %s THEN trim("{c}")::integer END)'.format(
            t=table, c=column), (_NUMERIC,))
    _logger.info("SGI fase 1: %s.%s convertido a entero.", table, column)


def migrate(cr, version):
    if not version:
        return
    _to_integer(cr, 'documents_document', 'sgi_revision', 'sgi_revision_legacy')
    _to_integer(cr, 'approval_request', 'sgi_new_revision')
    cr.execute("ALTER TABLE sgi_process DROP CONSTRAINT IF EXISTS sgi_process_code_uniq")
    cr.execute("SELECT to_regclass('sgi_activity_job_rel')")
    if cr.fetchone()[0]:
        cr.execute("""
            CREATE TABLE IF NOT EXISTS sgi_activity_job_rel_pre29 AS
            SELECT activity_id, job_id FROM sgi_activity_job_rel
        """)
