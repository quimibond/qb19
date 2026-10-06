# -*- coding: utf-8 -*-
"""57.4.0 (entrega 2 de la auditoría, e2-procesos-viejos; B-002): respaldo en
CSV de ``sgi.process.responsibility``.

El piloto P-VEN (``seed_procedure_ventas``) sale del código en esta versión.
Lo único que dejó en la base son 7 responsabilidades, todas del proceso
archivado 6 «P-VEN - Ventas» (MCP, solo lectura, 2026-09-29; ids 8-14). El
consolidado propone borrarlas después del respaldo, pero la regla de la casa
es que nada se borra, y el modelo no tiene ``active``: **se quedan como
están**. Esta migración solo deja el respaldo, un adjunto CSV por proceso
(``res_model = 'sgi.process'``), y lo anota en el log.

Idempotente: si el proceso ya tiene el adjunto con ese nombre, no se repite.
"""
import base64
import csv
import io
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

NAME = 'responsabilidades_respaldo_57.4.0_%s.csv'
COLUMNS = ('id', 'process_id', 'process_code', 'process_name', 'sequence', 'job_id',
           'job_name', 'name', 'responsibilities', 'company_id', 'create_date',
           'write_date')


def migrate(cr, version):
    cr.execute("SELECT to_regclass('sgi_process_responsibility')")
    if not cr.fetchone()[0]:
        return
    cr.execute("""
        SELECT r.id, r.process_id, p.code, p.name, r.sequence, r.job_id, j.name,
               r.name, r.responsibilities, r.company_id, r.create_date, r.write_date
          FROM sgi_process_responsibility r
          JOIN sgi_process p ON p.id = r.process_id
          LEFT JOIN hr_job j ON j.id = r.job_id
         ORDER BY r.process_id, r.sequence, r.id""")
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI 57.4.0: sin responsabilidades de proceso que respaldar.")
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Attachment = env['ir.attachment']
    by_process = {}
    for row in rows:
        by_process.setdefault((row[1], row[2]), []).append(row)
    created = 0
    for (process_id, code), prows in by_process.items():
        name = NAME % (code or process_id)
        if Attachment.search_count([('res_model', '=', 'sgi.process'),
                                    ('res_id', '=', process_id), ('name', '=', name)]):
            continue
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(COLUMNS)
        for row in prows:
            writer.writerow([_text(v) for v in row])
        Attachment.create({
            'name': name,
            'res_model': 'sgi.process',
            'res_id': process_id,
            'mimetype': 'text/csv',
            'datas': base64.b64encode(buf.getvalue().encode('utf-8')),
        })
        created += 1
    _logger.info(
        "SGI 57.4.0: %d responsabilidad(es) de proceso en %d proceso(s); %d respaldo(s) CSV "
        "nuevo(s). Las filas se quedan: el modelo no tiene «active» y nada se borra.",
        len(rows), len(by_process), created)


def _text(value):
    if value is None:
        return ''
    if isinstance(value, dict):  # campos traducibles (jsonb)
        return value.get('es_MX') or value.get('en_US') or next(iter(value.values()), '')
    return str(value)
