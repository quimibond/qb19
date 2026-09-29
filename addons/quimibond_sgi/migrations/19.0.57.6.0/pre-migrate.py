# -*- coding: utf-8 -*-
"""57.6.0 (entrega 2 de la auditoría, e2-legado; B-009 + D-011 + C-017, B-010,
decisiones D-13 y D-16). Solo SQL, sin importar código del módulo,
idempotente y sin borrar registros de negocio.

1. **Modos de cálculo retirados (B-010), defensivo.** Los modos
   ``desperdicio``, ``desperdicio_scrap``, ``disponibilidad_mantto``,
   ``preventivo_cumplido``, ``plantilla_rh``, ``inventario_ciclico``,
   ``compras_sin_devolucion``, ``margen_ventas`` y ``compras_vs_ventas`` salen
   de la selección. Cualquier indicador (activo o archivado) que aún tuviera
   uno pasa a ``manual`` y se deja en el log con su id y su modo anterior.
2. **Encuesta 151 «Checklist Auditoría Interna ISO 9001:2015» (D-16).**
   ``data/sgi_audit_data.xml`` sale del manifest. Sus 36 XML IDs (la
   encuesta, 14 preguntas o secciones y 21 respuestas) pasan a
   ``__export__`` con el prefijo ``quimibond_sgi_legado_``, igual que 56.35.0
   y 57.4.0: así Odoo no borra los registros al terminar la actualización.
   La encuesta no se borra; si estuviera activa, se archiva.

Producción el 2026-09-29 (MCP, solo lectura):
- ``sgi.indicator`` agrupado por ``calc_mode`` (activos y archivados): ninguno
  en los 9 modos retirados → el paso 1 no cambia nada.
- ``survey.survey`` 151: archivada, 0 respuestas, 7 preguntas; ``sgi.audit``
  con encuesta: 0 (no hay auditorías); ``ir.model.data`` de ``quimibond_sgi``
  sobre la encuesta 151 y sus preguntas y respuestas: 36.
- CA-02 (indicador 27) usa la encuesta 152 de satisfacción, no la 151.

Reversa del paso 2: ``UPDATE ir_model_data SET module = 'quimibond_sgi',
name = substr(name, 22) WHERE module = '__export__' AND name LIKE
'quimibond\\_sgi\\_legado\\_sgi\\_survey\\_%'`` y regresar el archivo al
manifest (``docs/historico/quimibond_sgi_data/sgi_audit_data.xml``).
"""
import logging

_logger = logging.getLogger(__name__)

PREFIX = 'quimibond_sgi_legado_'
RETIRED_MODES = (
    'desperdicio', 'desperdicio_scrap', 'disponibilidad_mantto', 'preventivo_cumplido',
    'plantilla_rh', 'inventario_ciclico', 'compras_sin_devolucion', 'margen_ventas',
    'compras_vs_ventas',
)
_SECTIONS = range(4, 11)
SURVEY_XMLIDS = (
    ('sgi_survey_audit_9001',)
    + tuple('sgi_survey_page_%d' % n for n in _SECTIONS)
    + tuple('sgi_survey_q_%d' % n for n in _SECTIONS)
    + tuple('sgi_survey_a_%d_%s' % (n, s) for n in _SECTIONS for s in ('c', 'nc', 'o'))
)
SURVEY_MODELS = ('survey.survey', 'survey.question', 'survey.question.answer')


def _retire_modes(cr):
    cr.execute("SELECT id, code, calc_mode FROM sgi_indicator WHERE calc_mode IN %s",
               (RETIRED_MODES,))
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI 57.6.0: 0 indicadores en modos retirados (B-010).")
        return
    _logger.warning("SGI 57.6.0: %d indicador(es) en modos retirados pasan a «manual» "
                    "(id, clave, modo anterior): %s", len(rows), rows)
    cr.execute("UPDATE sgi_indicator SET calc_mode = 'manual' WHERE calc_mode IN %s",
               (RETIRED_MODES,))


def _archive_survey(cr):
    cr.execute("""
        UPDATE survey_survey s SET active = false
          FROM ir_model_data d
         WHERE d.module = 'quimibond_sgi' AND d.name = 'sgi_survey_audit_9001'
           AND d.model = 'survey.survey' AND d.res_id = s.id AND s.active""")
    if cr.rowcount:
        _logger.warning("SGI 57.6.0: la encuesta de auditoría legado estaba activa; se archiva.")


def _move_xmlids(cr):
    cr.execute("""
        UPDATE ir_model_data d
           SET module = '__export__', name = %s || d.name, noupdate = true
         WHERE d.module = 'quimibond_sgi' AND d.model IN %s AND d.name IN %s
           AND NOT EXISTS (SELECT 1 FROM ir_model_data x
                            WHERE x.module = '__export__' AND x.name = %s || d.name)""",
               (PREFIX, SURVEY_MODELS, SURVEY_XMLIDS, PREFIX))
    _logger.info("SGI 57.6.0: %d XML ID(s) de la encuesta de auditoría legado pasan a "
                 "__export__ (esperado en producción: 36; los registros se quedan).",
                 cr.rowcount)


def migrate(cr, version):
    if not version:
        return
    _retire_modes(cr)
    _archive_survey(cr)
    _move_xmlids(cr)
