# -*- coding: utf-8 -*-
"""57.97.0 (N-05): cláusulas de tercer nivel con xmlid.

``data/sgi_norms_tercer_nivel.xml`` (noupdate) crea 13 cláusulas nuevas en
ISO 9001, 14001 y 45001. ``sgi.norm.clause`` no tiene restricción única de
norma + numeral: si el Jefe MAST ya capturó a mano una cláusula con el mismo
numeral en la misma norma, el XML crearía otra igual. Antes de cargar los
datos, este script solo LIGA el xmlid a la que ya existe (una fila nueva en
``ir_model_data``, ``noupdate``): es metadato, no dato de negocio; la
cláusula conserva su nombre, sus actividades, NC y hallazgos.

- Sin cláusula con ese numeral en esa norma: no hace nada (el XML la crea).
- Con varias: liga la de id menor y avisa (WARNING) de las demás.
- xmlid ya existente: no hace nada (idempotente).
- Las normas sin xmlid (requisitos de clientes CLI-xx, ISO 31000) no se tocan
  (A-029). En producción, el 2026-10-02, ninguna de las 13 existía.

``NEW_CLAUSES`` es la única tabla de las cláusulas nuevas: la lee también
``tests/test_clausulas_revision.py``. Debe coincidir con el XML."""
import logging

_logger = logging.getLogger(__name__)

MODULE = 'quimibond_sgi'

# (xmlid, xmlid de la norma, numeral, requisito)
NEW_CLAUSES = [
    ('c_9001_7_1_5', 'sgi_norm_9001', '7.1.5', "Recursos de seguimiento y medición"),
    ('c_9001_9_1_2', 'sgi_norm_9001', '9.1.2', "Satisfacción del cliente"),
    ('c_14001_6_1_2', 'sgi_norm_14001', '6.1.2', "Aspectos ambientales"),
    ('c_14001_6_1_3', 'sgi_norm_14001', '6.1.3', "Requisitos legales y otros requisitos"),
    ('c_14001_6_1_4', 'sgi_norm_14001', '6.1.4', "Planificación de acciones"),
    ('c_14001_9_1_2', 'sgi_norm_14001', '9.1.2', "Evaluación del cumplimiento"),
    ('c_45001_6_1_2', 'sgi_norm_45001', '6.1.2',
     "Identificación de peligros y evaluación de los riesgos y oportunidades"),
    ('c_45001_6_1_3', 'sgi_norm_45001', '6.1.3',
     "Determinación de los requisitos legales y otros requisitos"),
    ('c_45001_6_1_4', 'sgi_norm_45001', '6.1.4', "Planificación de acciones"),
    ('c_45001_8_1_2', 'sgi_norm_45001', '8.1.2',
     "Eliminar peligros y reducir riesgos para la SST (jerarquía de controles)"),
    ('c_45001_8_1_3', 'sgi_norm_45001', '8.1.3', "Gestión del cambio"),
    ('c_45001_8_1_4', 'sgi_norm_45001', '8.1.4', "Compras (contratistas y contratación externa)"),
    ('c_45001_9_1_2', 'sgi_norm_45001', '9.1.2', "Evaluación del cumplimiento"),
]


def _res_id(cr, name, model):
    cr.execute("SELECT res_id FROM ir_model_data WHERE module = %s AND name = %s AND model = %s",
               (MODULE, name, model))
    row = cr.fetchone()
    return row[0] if row else None


def migrate(cr, version):
    if not version:
        return
    bound = []
    for name, norm_xmlid, code, _label in NEW_CLAUSES:
        if _res_id(cr, name, 'sgi.norm.clause'):
            continue
        norm_id = _res_id(cr, norm_xmlid, 'sgi.norm')
        if not norm_id:
            continue
        cr.execute("SELECT id FROM sgi_norm_clause WHERE norm_id = %s AND trim(code) = %s ORDER BY id",
                   (norm_id, code))
        ids = [row[0] for row in cr.fetchall()]
        if not ids:
            continue
        cr.execute("""
            INSERT INTO ir_model_data (module, name, model, res_id, noupdate,
                                       create_uid, write_uid, create_date, write_date)
            VALUES (%s, %s, 'sgi.norm.clause', %s, true, 1, 1,
                    now() at time zone 'UTC', now() at time zone 'UTC')
            ON CONFLICT DO NOTHING
        """, (MODULE, name, ids[0]))
        bound.append("%s → %s" % (name, ids[0]))
        if len(ids) > 1:
            _logger.warning(
                "SGI 57.97.0: la norma %s tiene %d cláusulas con el numeral %s (ids %s); se ligó "
                "la %s a %s.%s. Revise y una las demás a mano.",
                norm_xmlid, len(ids), code, ids, ids[0], MODULE, name)
    _logger.info("SGI 57.97.0: %d cláusula(s) existente(s) ligadas a su xmlid nuevo: %s",
                 len(bound), ", ".join(bound) or "ninguna")
