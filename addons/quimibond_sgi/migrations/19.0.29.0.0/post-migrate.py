# -*- coding: utf-8 -*-
"""Fase 1 del catálogo — después de cargar modelos y datos.

- company_id = 1 (Quimibond) en todo lo del catálogo que aún no la tenga.
- Un rol «ejecuta» por cada puesto que estaba en «Puestos responsables».
  Las actividades heredadas con 0 o con varios ejecutores se reportan en el
  log: la regla «exactamente un ejecutor» se les aplica cuando se editen sus
  roles (o las cubre la carga por API de los 14 procesos nuevos).
- Tipo de documento como registro (sgi_doc_type_id) desde el código viejo.
- Aprobador del procedimiento = usuario del dueño, donde estaba vacío.
- Método de medición «Registro en Odoo» en las actividades que ya tenían
  modelo; las demás quedan «sin medir» (se ven en el tablero por método).
- quimibond_sgi.generic_user_ids = cuentas compartidas de producción
  (Supervisor 92, Auditor de Calidad 184, manufactura@ 80), si no existe.

- La carga por API (sgi.process.load_payload) queda disponible en el conector
  MCP de Odoo: sgi.process habilitado con llamadas a métodos. El método mismo
  exige el grupo Administrador SGI.

Nada se borra. Los puestos duplicados y los modelos de Studio vacíos se
limpian a mano (hr.job.sgi_merge_duplicate_jobs y
sgi.config.sgi_drop_empty_studio_models, ambos con dry_run): tocan datos
de RH y el registro de modelos, y conviene ver el reporte antes.
"""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("SELECT id FROM res_company WHERE id = 1")
    if cr.fetchone():
        company_id = 1
    else:
        cr.execute("SELECT min(id) FROM res_company")
        company_id = cr.fetchone()[0]

    cr.execute("UPDATE sgi_process SET company_id = %s WHERE company_id IS NULL",
               (company_id,))
    # Campos relacionados almacenados: se alinean con su proceso/actividad.
    cr.execute("""
        UPDATE sgi_process_activity a SET company_id = p.company_id
        FROM sgi_process p
        WHERE a.process_id = p.id AND a.company_id IS DISTINCT FROM p.company_id
    """)
    cr.execute("""
        UPDATE sgi_process_responsibility r SET company_id = p.company_id
        FROM sgi_process p
        WHERE r.process_id = p.id AND r.company_id IS DISTINCT FROM p.company_id
    """)
    cr.execute("""
        UPDATE sgi_process_flow f SET company_id = p.company_id
        FROM sgi_process p
        WHERE f.from_process_id = p.id AND f.company_id IS DISTINCT FROM p.company_id
    """)
    cr.execute("""
        UPDATE sgi_activity_link l SET company_id = a.company_id
        FROM sgi_process_activity a
        WHERE l.from_activity_id = a.id AND l.company_id IS DISTINCT FROM a.company_id
    """)
    cr.execute("UPDATE sgi_process_activity SET active = TRUE WHERE active IS NULL")
    cr.execute("""
        UPDATE sgi_process_activity SET automation_level_current = 'manual'
        WHERE automation_level_current IS NULL
    """)

    # Roles «ejecuta» desde la relación de antes (sgi_activity_job_rel, y su
    # respaldo del pre-migrate por si el update la hubiera vaciado).
    cr.execute("SELECT to_regclass('sgi_activity_job_rel_pre29')")
    backup = bool(cr.fetchone()[0])
    source = """
        SELECT activity_id, job_id FROM sgi_activity_job_rel
        %s
    """ % ("UNION SELECT activity_id, job_id FROM sgi_activity_job_rel_pre29"
           if backup else "")
    cr.execute("""
        INSERT INTO sgi_activity_role
            (activity_id, role, target_type, job_id, sequence, process_id,
             company_id, create_uid, write_uid, create_date, write_date)
        SELECT r.activity_id, 'ejecuta', 'job', r.job_id,
               10 * row_number() OVER (PARTITION BY r.activity_id ORDER BY r.job_id),
               a.process_id, a.company_id, 1, 1,
               now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
        FROM (%s) r
        JOIN sgi_process_activity a ON a.id = r.activity_id
        JOIN hr_job j ON j.id = r.job_id
        ON CONFLICT DO NOTHING
    """ % source)
    _logger.info("SGI fase 1: %d rol(es) «ejecuta» creados desde los puestos "
                 "responsables.", cr.rowcount)
    # La relación calculada queda igual a los roles «ejecuta».
    cr.execute("""
        INSERT INTO sgi_activity_job_rel (activity_id, job_id)
        SELECT activity_id, job_id FROM sgi_activity_role WHERE role = 'ejecuta'
        ON CONFLICT DO NOTHING
    """)
    if backup:
        cr.execute("DROP TABLE sgi_activity_job_rel_pre29")
    cr.execute("""
        SELECT p.code, a.number, count(r.id)
        FROM sgi_process_activity a
        JOIN sgi_process p ON p.id = a.process_id
        LEFT JOIN sgi_activity_role r ON r.activity_id = a.id AND r.role = 'ejecuta'
        WHERE a.active AND a.automation_level_current <> 'automatico'
        GROUP BY p.code, a.number
        HAVING count(r.id) <> 1
        ORDER BY p.code, a.number
    """)
    off_rule = cr.fetchall()
    if off_rule:
        _logger.warning(
            "SGI fase 1: %d actividad(es) heredadas sin exactamente un "
            "ejecutor (proceso, numeral, ejecutores): %s",
            len(off_rule), off_rule)

    # Tipo de documento como registro.
    cr.execute("""
        UPDATE documents_document d SET sgi_doc_type_id = t.id
        FROM sgi_document_type t
        WHERE d.sgi_doc_type_id IS NULL AND d.sgi_doc_type IS NOT NULL
          AND t.code = d.sgi_doc_type AND t.company_id IS NULL
    """)
    _logger.info("SGI fase 1: %d documento(s) con tipo de documento ligado.",
                 cr.rowcount)

    cr.execute("""
        UPDATE sgi_process_activity SET measure_method = 'odoo'
        WHERE measure_method IS NULL AND measure_model_id IS NOT NULL
    """)
    cr.execute("""
        INSERT INTO ir_config_parameter (key, value, create_uid, write_uid, create_date, write_date)
        SELECT 'quimibond_sgi.generic_user_ids', '92,184,80', 1, 1,
               now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
        WHERE NOT EXISTS (SELECT 1 FROM ir_config_parameter
                          WHERE key = 'quimibond_sgi.generic_user_ids')
    """)

    # Aprobador por omisión = usuario del dueño del proceso.
    cr.execute("""
        UPDATE sgi_process p SET doc_approver_id = e.user_id
        FROM hr_employee e
        WHERE p.owner_id = e.id AND p.doc_approver_id IS NULL
          AND e.user_id IS NOT NULL
    """)

    # Opcional: un tropiezo aquí no debe tumbar el update.
    try:
        with cr.savepoint():
            _enable_mcp_load(cr)
    except Exception:  # noqa: BLE001
        _logger.exception("SGI fase 1: no se pudo habilitar sgi.process en MCP; "
                          "hazlo a mano en Ajustes → MCP.")


def _enable_mcp_load(cr):
    env = api.Environment(cr, SUPERUSER_ID, {})
    if 'mcp.enabled.model' not in env:
        return
    Enabled = env['mcp.enabled.model'].with_context(active_test=False)
    model = env['ir.model']._get('sgi.process')
    record = Enabled.search([('model_id', '=', model.id)], limit=1)
    if record:
        record.write({'active': True, 'allow_read': True, 'allow_method_calls': True})
    else:
        Enabled.create({
            'model_id': model.id,
            'allow_read': True,
            'allow_method_calls': True,
            'notes': "SGI: carga del catálogo por API (load_payload). El "
                     "método exige el grupo Administrador SGI.",
        })
    _logger.info("SGI fase 1: sgi.process habilitado en MCP para load_payload.")
