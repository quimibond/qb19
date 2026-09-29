# -*- coding: utf-8 -*-
"""56.30.0 (auditoría 2026-09, C-006, entrega 3 «documentos», bloque 1): liga
cada ``sgi.format.map`` a su documento controlado por la clave con la que se
sembró, ANTES de que la clave del Dropbox pase a clave anterior
(decisión 4 de Jose: primero C-006, después la migración de clave).

Corre después de cargar los datos, así que también liga los 13 mapeos por
referencia (``format_ref_*``) que crea esta versión.

Verificado en producción por MCP (solo lectura, 2026-09-29):
``search_records sgi.format.map`` (9) y ``search_records documents.document
[sgi_code in <claves>]``. Resuelven 8 de 9 mapeos por modelo (en 9 de 11
claves: sale.order 3878 y 5059, purchase.order 3712, stock.picking 3768,
mrp.production 4758, maintenance.request 3658, quality.alert 4086, stock.lot
3931, sgi.management.review 3762, sgi.sales.budget 3882) y **no resuelve
F-P-A28-13** (pronóstico, clave alternativa de sgi.sales.budget): no hay
documento con esa clave; el mapeo sigue imprimiendo la clave de texto hasta
que MAST ligue el documento. Los 13 por referencia resuelven todos (3851,
3852, 3806, 4761, 3958, 3968, 3972, 3973, 3503, 5067, 4067, 3909, 3910), un
solo documento por clave, todos vigentes.

Idempotente: solo toca mapeos con el documento vacío. Por SQL, sin chatter.
"""
import logging

_logger = logging.getLogger(__name__)

_PICK = """
    SELECT d.id FROM documents_document d
     WHERE d.sgi_is_controlled IS TRUE AND d.sgi_code = %s
     ORDER BY (d.sgi_state = 'vigente') DESC, d.active DESC,
              d.sgi_revision DESC NULLS LAST, d.id DESC
     LIMIT 1
"""


def _link(cr, column, code_column):
    cr.execute("""
        SELECT id, btrim({code}) FROM sgi_format_map
         WHERE {col} IS NULL AND coalesce(btrim({code}), '') <> ''
         ORDER BY id
    """.format(col=column, code=code_column))
    linked, missing = [], []
    for map_id, code in cr.fetchall():
        cr.execute(_PICK, (code,))
        row = cr.fetchone()
        if not row:
            missing.append((map_id, code))
            continue
        cr.execute("UPDATE sgi_format_map SET {col} = %s WHERE id = %s AND {col} IS NULL"
                   .format(col=column), (row[0], map_id))
        linked.append((map_id, code, row[0]))
    return linked, missing


def migrate(cr, version):
    linked, missing = _link(cr, 'document_id', 'sgi_code')
    alt_linked, alt_missing = _link(cr, 'document_alt_id', 'sgi_code_alt')
    _logger.info("SGI 56.30.0 (C-006): %d mapeo(s) ligados a su documento y %d a su "
                 "documento alternativo: %s", len(linked), len(alt_linked),
                 ", ".join("%s→%s" % (c, d) for _m, c, d in linked + alt_linked))
    if missing or alt_missing:
        _logger.warning(
            "SGI 56.30.0 (C-006): sin documento controlado con la clave (siguen "
            "imprimiendo la clave de texto; MAST los liga en SGI > Configuración > "
            "Formatos en documentos de Odoo): %s",
            ", ".join("mapeo %s: %s" % (m, c) for m, c in missing + alt_missing))
