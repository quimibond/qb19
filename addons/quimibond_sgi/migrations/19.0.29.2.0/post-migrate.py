# -*- coding: utf-8 -*-
"""Estructura en vez de texto (19.0.29.2.0).

- Paso entero por actividad, en el orden en que ya estaban (secuencia, id);
  el numeral se recalcula como clave del proceso + paso.
- Etapas del proceso a partir del texto de «sección» («4.2.3 Cotización de
  productos» → etapa 4.2.3 · Cotización de productos).
"""
import logging
import re

_logger = logging.getLogger(__name__)
RE_STAGE = re.compile(r'^\s*([A-Za-z0-9]+(?:\.[0-9]+)*)[\.\)\-:]?\s+(.+?)\s*$')


def migrate(cr, version):
    if not version:
        return
    cr.execute("""
        UPDATE sgi_process_activity a SET step = r.rn
        FROM (SELECT id, row_number() OVER (PARTITION BY process_id ORDER BY sequence, id) AS rn
                FROM sgi_process_activity) r
        WHERE a.id = r.id AND a.step IS NULL
    """)
    cr.execute("""
        UPDATE sgi_process_activity a
           SET number = p.code || '.' || lpad(a.step::text, 2, '0')
          FROM sgi_process p
         WHERE a.process_id = p.id AND a.step IS NOT NULL
    """)
    _logger.info("SGI 29.2: %d actividad(es) con paso y numeral calculado.", cr.rowcount)

    cr.execute("""
        SELECT process_id, section, min(sequence)
          FROM sgi_process_activity
         WHERE section IS NOT NULL AND trim(section) <> '' AND stage_id IS NULL
         GROUP BY process_id, section
         ORDER BY process_id, min(sequence)
    """)
    made = 0
    for process_id, section, seq in cr.fetchall():
        text = ' '.join(section.split())
        match = RE_STAGE.match(text)
        code, name = (match.group(1), match.group(2)) if match else (None, text)
        cr.execute("""
            SELECT id FROM sgi_process_stage WHERE process_id = %s AND name = %s
        """, (process_id, name))
        row = cr.fetchone()
        if row:
            stage_id = row[0]
        else:
            cr.execute("""
                INSERT INTO sgi_process_stage (process_id, sequence, code, name, company_id,
                                               create_uid, write_uid, create_date, write_date)
                SELECT %s, %s, %s, %s, p.company_id, 1, 1,
                       now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
                  FROM sgi_process p WHERE p.id = %s
                RETURNING id
            """, (process_id, seq or 10, code, name, process_id))
            stage_id = cr.fetchone()[0]
            made += 1
        cr.execute("""
            UPDATE sgi_process_activity
               SET stage_id = %s,
                   section = CASE WHEN %s IS NULL THEN %s ELSE %s || '. ' || %s END
             WHERE process_id = %s AND section = %s AND stage_id IS NULL
        """, (stage_id, code, name, code, name, process_id, section))
    _logger.info("SGI 29.2: %d etapa(s) creadas desde el texto de sección.", made)
