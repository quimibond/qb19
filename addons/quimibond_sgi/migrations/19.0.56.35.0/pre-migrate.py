# -*- coding: utf-8 -*-
"""56.35.0 (entrega 3 de la auditoría, e3-quimibond-sgi-mapa). Solo SQL, sin
importar código del módulo, idempotente y sin borrar nada.

1. A-004 / D-01: los 10 términos de fórmula de ``sgi_indicator_formula_data.xml``
   (ubicaciones, categorías, tipo de operación y proveedor con IDs de
   producción) salen del núcleo: el archivo deja el manifest y sus XML IDs
   pasan a ``__export__`` con el prefijo ``quimibond_sgi_legado_`` (patrón del
   anexo A de 01-arquitectura). Los registros se quedan como están; el mapa
   (``quimibond_sgi_mapa``) los trae con referencias portables.
2. H-014: la categoría «Proponer cambio a mi procedimiento (SGI)» (la que
   tiene ``sgi_is_mp_change``; en producción, la 11) pasa al núcleo con XML ID
   ``quimibond_sgi.sgi_approval_category_mp_change`` y ``noupdate``. Aquí se
   ADOPTA la que ya existe (se le crea el XML ID) para que el XML nuevo no
   cree una segunda. En una base sin ninguna, el XML la crea.
"""
import logging

_logger = logging.getLogger(__name__)

PREFIX = 'quimibond_sgi_legado_'
MP_XMLID = 'sgi_approval_category_mp_change'


def _move_terms(cr):
    cr.execute("""
        UPDATE ir_model_data d
           SET module = '__export__', name = %s || d.name, noupdate = true
         WHERE d.module = 'quimibond_sgi' AND d.model = 'sgi.indicator.term'
           AND NOT EXISTS (SELECT 1 FROM ir_model_data x
                            WHERE x.module = '__export__' AND x.name = %s || d.name)""",
               (PREFIX, PREFIX))
    _logger.info("SGI 56.35.0: %d XML ID(s) de términos de fórmula pasan a __export__ "
                 "(los registros se quedan).", cr.rowcount)


def _adopt_mp_category(cr):
    cr.execute("SELECT res_id FROM ir_model_data WHERE module = 'quimibond_sgi' AND name = %s",
               (MP_XMLID,))
    if cr.fetchone():
        return
    cr.execute("""SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'approval_category' AND column_name = 'sgi_is_mp_change'""")
    if not cr.fetchone():
        return
    cr.execute("""SELECT id FROM approval_category
                   WHERE sgi_is_mp_change IS TRUE ORDER BY active DESC, id LIMIT 1""")
    row = cr.fetchone()
    if not row:
        _logger.info("SGI 56.35.0: no hay categoría «Proponer cambio»; el XML la crea.")
        return
    cr.execute("""
        INSERT INTO ir_model_data (module, name, model, res_id, noupdate, create_date, write_date)
        VALUES ('quimibond_sgi', %s, 'approval.category', %s, true,
                now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')""", (MP_XMLID, row[0]))
    _logger.info("SGI 56.35.0: la categoría %s («Proponer cambio a mi procedimiento») queda "
                 "con XML ID quimibond_sgi.%s (noupdate).", row[0], MP_XMLID)


def migrate(cr, version):
    _move_terms(cr)
    _adopt_mp_category(cr)
