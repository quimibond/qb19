<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.cron`

**Tareas programadas SGI** (AbstractModel).

Tareas programadas del SGI. Cada método ``cron_*`` es una acción planificada (ver ``docs/sgi/tecnica/crons.md``); agendan actividades con ``_sgi_schedule`` (idempotente por clave) y cada paso corre en su savepoint.

Archivos: `addons/quimibond_sgi/models/sgi_cron.py`, `addons/quimibond_sgi/models/sgi_customer_reply.py`, `addons/quimibond_sgi/models/sgi_deploy_change.py`, `addons/quimibond_sgi/models/sgi_external_doc.py`, `addons/quimibond_sgi/models/sgi_floor_kiosk.py`, `addons/quimibond_sgi/models/sgi_hse_records.py`, `addons/quimibond_sgi/models/sgi_indicator_health.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_miid.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_weekly_overdue.py`.

## Métodos públicos (25)

| Método | Qué hace (docstring) |
|---|---|
| `cron_audit_program` | Cron diario: 15 días antes del mes planeado de cada renglón del programa aprobado, agenda al auditor líder (o a MAST) preparar la auditoría. |
| `cron_calibrations` | 56.38.0 (G-006): solo avisa, no bloquea, hasta que se carguen las fechas reales (decisión 2 de la tanda 2). El bloqueo «No usar» del equipo vencido queda detrás del parámetro ``quimibond_sgi.calibrat… |
| `cron_competences` | Cron diario: competencias con vigencia (certificaciones y, desde 57.100.0, cualquier competencia con «válida hasta», N-13/P9) por vencer (30 días) o vencidas, al empleado y a RH; los satélites y exte… |
| `cron_context_review` | Cron semanal: partes interesadas (4.1/4.2) con revisión vencida. Idempotente por resumen. |
| `cron_dnc` | Cron trimestral: cierra el ciclo de la DNC (P-A01). Cuenta las brechas de competencia abiertas y agenda al coordinador de RH la distribución de la encuesta DNC (F-P-A01-17) y el plan de capacitación.… |
| `cron_documents` | Cron diario de documentos: avisos de revisión bienal, pilotos por vencer y acuses pendientes (57.95.0: un aviso por persona o por jefe, no por acuse). |
| `cron_emergency_drills` | Cron diario: vigila los simulacros de los planes de emergencia vigentes (14001/45001 8.2). Idempotente por resumen. |
| `cron_health_weekly_mail` | 57.99.0, cada lunes: mide (si falta) la semana pasada de los indicadores de salud y manda a Dirección el correo con su valor, meta, semáforo, la semana anterior y la tabla por dueño de proceso (halla… |
| `cron_hr_employee_gaps` | 57.94.0 (U-08), cada lunes: un aviso por departamento con empleados sin puesto o sin correo, a RH. Sale en Mis pendientes como «Aviso». Si la persona lo marcó «Hecho» y siguen faltando datos, el lune… |
| `cron_indicators` | Diario desde I-6: escala los planes vencidos todos los días y mide solo el tercer día hábil (o cuando el mes anterior siga sin medir). Sin ``scheduled`` (a mano) mide siempre, como antes. |
| `cron_indicators_weekly` | Cron semanal: mide los indicadores de frecuencia semanal de la semana previa. |
| `cron_legal_requirements` | Cron diario: evaluaciones de cumplimiento vencidas y permisos por vencer (≤60 días) o vencidos. Idempotente por resumen. |
| `cron_my_procedure_stale` | Semanal: avisa al Jefe MAST qué puestos con personas tienen «Mi procedimiento» sin publicar o desactualizado. Una sola actividad, sobre la revisión vigente del primer puesto desactualizado (documents… |
| `cron_news` | Cron mensual: si el mes anterior hubo cambios documentales aplicados, agenda al Jefe MAST el boletín NEWS. |
| `cron_nightly_backup` | Cron diario (02:15 de México): recalcula las cuatro listas guardadas de Mi procedimiento y anota en el log cuántas personas cambiaron (si no es 0, falta un disparo), arma el registro de cumplimiento … |
| `cron_nonconformities` | Cron diario de NC: cierra actividades ya resueltas, recalcula acciones vencidas, avisa y escala los plazos por etapa, escala NC sin acción y pide la verificación de eficacia. |
| `cron_operational_signals` | Cron diario. (a) Falla repetitiva: ≥3 correctivas del mismo equipo en 90 días → actividad al Jefe MAST sugiriendo levantar NC y revisar el plan de mantenimiento. (b) Reclamación abierta con SLA venci… |
| `cron_overdue_actions` | Escalamiento en 3 niveles de acciones vencidas: - nivel 1 (responsable): ya lo recuerda la actividad espejo (Ola 0); - > N días: además su jefe directo (employee_id.parent_id.user_id, fallback Jefe M… |
| `cron_risk_review` | Cron diario: riesgos con revisión vencida al dueño del proceso (o a MAST) y riesgos altos sin acción. |
| `cron_satisfaction_survey` | Cron trimestral: recuerda al Admin de Ventas distribuir la Encuesta de Satisfacción del Cliente (9001 9.1.2). Las respuestas alimentan el KPI CA-02 automáticamente. No envía correos a clientes por sí… |
| `cron_sign_elearning_sync` | Cron diario: sella acuses cuya firma electrónica ya se completó y otorga competencias de cursos eLearning terminados. |
| `cron_supplier_eval` | Cron trimestral: evalúa a los proveedores críticos con las recepciones del trimestre anterior (entrega a tiempo y NC) y avisa a Compras los condicionados y de baja. |
| `cron_weekly_overdue_mail` | Cron semanal (D-14): a cada persona con algo atrasado en Mis pendientes, un correo con esa lista. Cada envío va en su savepoint: un correo que falla no detiene a los demás. Devuelve la lista de usuar… |
| `cron_work_permits` | Cron cada hora: marca vencidos los permisos de trabajo autorizados que pasaron su hora de fin y avisa sobre el permiso al jefe del área (o a quien lo solicitó) y al Jefe MAST. Los avisos se cierran s… |
| `cron_worker_participation` | Cron semestral: recuerda distribuir la encuesta de consulta y participación de los trabajadores (45001 §5.4). Las respuestas y las quejas del canal interno alimentan la entrada 12 de la RxD. Idempote… |
