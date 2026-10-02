# -*- coding: utf-8 -*-
"""57.95.0 (K-08): columna técnica ``mail_activity.sgi_cron_kind``.

Crea la columna antes de que el ORM cargue el campo calculado guardado y la
llena con SQL: la parte de ``sgi_cron_key`` antes del primer «:» (vacía →
NULL). Así el ORM no recalcula en Python todas las actividades (7,260 en
producción el 2026-10-02, 80 con clave) ni les cambia ``write_date``. El
índice parcial lo crea el ORM después.

Solo escribe esa columna, en lotes de 5,000 por id, y solo en las filas cuya
clase no coincide (idempotente: una segunda corrida no escribe nada). No toca
datos de negocio ni borra nada. Registra cuántas filas llenó."""
import logging

_logger = logging.getLogger(__name__)
BATCH = 5000


def migrate(cr, version):
    if not version:
        return
    cr.execute("ALTER TABLE mail_activity ADD COLUMN IF NOT EXISTS sgi_cron_kind varchar")
    total = 0
    while True:
        cr.execute("""
            UPDATE mail_activity
               SET sgi_cron_kind = NULLIF(split_part(sgi_cron_key, ':', 1), '')
             WHERE id IN (
                   SELECT id FROM mail_activity
                    WHERE sgi_cron_key IS NOT NULL
                      AND sgi_cron_kind IS DISTINCT FROM NULLIF(split_part(sgi_cron_key, ':', 1), '')
                    ORDER BY id
                    LIMIT %s)
        """, (BATCH,))
        if not cr.rowcount:
            break
        total += cr.rowcount
    cr.execute("SELECT count(*) FROM mail_activity WHERE sgi_cron_kind IS NOT NULL")
    _logger.info("SGI 57.95.0: sgi_cron_kind llenado en %d actividades (%d con clase en total).",
                 total, cr.fetchone()[0])
