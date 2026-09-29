# -*- coding: utf-8 -*-
"""57.4.0 (entrega 2 de la auditoría, e2-procesos-viejos; A-002 + B-021,
A-003 + B-018, anexo A de 01-arquitectura). Solo SQL, sin importar código del
módulo, idempotente y sin borrar nada.

``data/sgi_process_data.xml`` y ``data/sgi_process_flows_extra.xml`` salen del
manifest (el SGI se instala sin procesos, decisión 6 de Jose). Sus 58 XML IDs
pasan a ``__export__`` con el prefijo ``quimibond_sgi_legado_``, igual que los
términos de fórmula en 56.35.0. Los registros se quedan como están: los
procesos archivados no se borran nunca (decisión 2), y con ellos sus etapas,
actividades y los adjuntos de respaldo de 56.24.0.

Producción el 2026-09-29 (MCP, solo lectura, empresa 1):
- ``ir.model.data`` de ``quimibond_sgi``: 21 sobre ``sgi.process`` (res_id
  1-21) y 37 sobre ``sgi.process.flow`` (res_id 1-37), todos con noupdate.
- Esos 21 procesos y 37 flujos: todos archivados (0 activos).
- ``sgi.process``: 14 activos y 25 archivados; ``sgi.process.flow``: 50
  activos y 37 archivados.
- ``__export__`` con prefijo ``quimibond_sgi_legado_``: 10 (los términos de
  56.35.0). Después de esta migración deben ser 68 (58 de procesos y flujos).

Pasos:
1. Aviso si alguno de los registros con XML ID viejo está ACTIVO (hoy: 0).
2. Si lo hay, se archiva (nunca se borra), flujos antes que procesos.
3. Los XML IDs pasan a ``__export__`` con el prefijo; ``NOT EXISTS`` evita
   choques y hace idempotente la segunda corrida.

Reversa: ``UPDATE ir_model_data SET module = 'quimibond_sgi', name =
substr(name, 22) WHERE module = '__export__' AND name LIKE
'quimibond\\_sgi\\_legado\\_%' AND model IN ('sgi.process', 'sgi.process.flow')``
y regresar los dos archivos al manifest.
"""
import logging

_logger = logging.getLogger(__name__)

PREFIX = 'quimibond_sgi_legado_'
LEGACY_MODELS = ('sgi.process', 'sgi.process.flow')


def _report_active(cr):
    cr.execute("""
        SELECT d.model, d.name
          FROM ir_model_data d
          LEFT JOIN sgi_process p ON d.model = 'sgi.process' AND p.id = d.res_id
          LEFT JOIN sgi_process_flow f ON d.model = 'sgi.process.flow' AND f.id = d.res_id
         WHERE d.module = 'quimibond_sgi' AND d.model IN %s
           AND COALESCE(p.active, f.active) IS TRUE""", (LEGACY_MODELS,))
    active = cr.fetchall()
    if active:
        _logger.warning("SGI 57.4.0: %d registro(s) del mapa viejo estaban ACTIVOS y se "
                        "archivan (no se borran): %s", len(active), active)
    else:
        _logger.info("SGI 57.4.0: 0 registro(s) del mapa viejo activos.")
    return bool(active)


def _archive_active(cr):
    cr.execute("""
        UPDATE sgi_process_flow f SET active = false
          FROM ir_model_data d
         WHERE d.module = 'quimibond_sgi' AND d.model = 'sgi.process.flow'
           AND d.res_id = f.id AND f.active""")
    cr.execute("""
        UPDATE sgi_process p SET active = false
          FROM ir_model_data d
         WHERE d.module = 'quimibond_sgi' AND d.model = 'sgi.process'
           AND d.res_id = p.id AND p.active""")


def _move_xmlids(cr):
    cr.execute("""
        UPDATE ir_model_data d
           SET module = '__export__', name = %s || d.name, noupdate = true
         WHERE d.module = 'quimibond_sgi' AND d.model IN %s
           AND NOT EXISTS (SELECT 1 FROM ir_model_data x
                            WHERE x.module = '__export__' AND x.name = %s || d.name)""",
               (PREFIX, LEGACY_MODELS, PREFIX))
    _logger.info("SGI 57.4.0: %d XML ID(s) de procesos y flujos viejos pasan a __export__ "
                 "(los registros se quedan).", cr.rowcount)
    cr.execute("""SELECT count(*) FROM ir_model_data
                   WHERE module = 'quimibond_sgi' AND model IN %s""", (LEGACY_MODELS,))
    left = cr.fetchone()[0]
    if left:
        _logger.warning("SGI 57.4.0: %d XML ID(s) de procesos/flujos siguen en quimibond_sgi "
                        "(ya había uno igual en __export__); revisar a mano.", left)


def migrate(cr, version):
    if _report_active(cr):
        _archive_active(cr)
    _move_xmlids(cr)
