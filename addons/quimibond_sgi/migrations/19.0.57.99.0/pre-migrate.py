# -*- coding: utf-8 -*-
"""57.99.0 «Salud del SGI»: los diez indicadores con xmlid.

``data/sgi_health_indicators.xml`` (noupdate) crea SG-01 a SG-10. La clave
del indicador es única (``_code_uniq``): si alguien ya capturó a mano un
indicador con una de esas claves, el XML chocaría y tumbaría el update. Antes
de cargar los datos, este script solo LIGA el xmlid al indicador que ya
existe (una fila nueva en ``ir_model_data``, ``noupdate``): es metadato, no
dato de negocio; el indicador conserva su modo, metas y mediciones.

- Sin indicador con esa clave: no hace nada (el XML lo crea).
- xmlid ya existente: no hace nada (idempotente).
- En producción, el 2026-10-02, ninguna clave SG-xx existía.

``HEALTH_INDICATORS`` debe coincidir con el XML."""
import logging

_logger = logging.getLogger(__name__)

MODULE = 'quimibond_sgi'

# (xmlid, clave)
HEALTH_INDICATORS = [
    ('sgi_ind_salud_procesos', 'SG-01'),
    ('sgi_ind_salud_personas', 'SG-02'),
    ('sgi_ind_salud_planta', 'SG-03'),
    ('sgi_ind_salud_acuses', 'SG-04'),
    ('sgi_ind_salud_validacion', 'SG-05'),
    ('sgi_ind_salud_rojos', 'SG-06'),
    ('sgi_ind_salud_nc', 'SG-07'),
    ('sgi_ind_salud_avisos', 'SG-08'),
    ('sgi_ind_salud_auditoria', 'SG-09'),
    ('sgi_ind_salud_formatos', 'SG-10'),
]


def _bind_existing(cr):
    """Liga cada xmlid sin fila a un indicador existente con su clave (activo
    o archivado). Devuelve cuántos ligó."""
    bound = []
    for name, code in HEALTH_INDICATORS:
        cr.execute("SELECT 1 FROM ir_model_data WHERE module = %s AND name = %s", (MODULE, name))
        if cr.fetchone():
            continue
        cr.execute("SELECT id FROM sgi_indicator WHERE code = %s ORDER BY id", (code,))
        ids = [row[0] for row in cr.fetchall()]
        if not ids:
            continue
        cr.execute("""
            INSERT INTO ir_model_data (module, name, model, res_id, noupdate,
                                       create_uid, write_uid, create_date, write_date)
            VALUES (%s, %s, 'sgi.indicator', %s, true, 1, 1,
                    now() at time zone 'UTC', now() at time zone 'UTC')
            ON CONFLICT DO NOTHING
        """, (MODULE, name, ids[0]))
        if cr.rowcount:
            bound.append("%s → %s" % (name, ids[0]))
        if len(ids) > 1:
            _logger.warning(
                "SGI 57.99.0: hay %d indicadores con la clave %s (ids %s); se ligó el %s a %s.%s.",
                len(ids), code, ids, ids[0], MODULE, name)
    _logger.info("SGI 57.99.0: %d indicador(es) de salud existentes ligados a su xmlid: %s",
                 len(bound), ", ".join(bound) or "ninguno")
    return len(bound)


def migrate(cr, version):
    if not version:
        return
    _bind_existing(cr)
