<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.cron`

**Tareas programadas SGI** (AbstractModel).

Archivos: `addons/quimibond_sgi/models/sgi_cron.py`, `addons/quimibond_sgi/models/sgi_customer_reply.py`, `addons/quimibond_sgi/models/sgi_external_doc.py`, `addons/quimibond_sgi/models/sgi_hse_records.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_weekly_overdue.py`.

## Métodos públicos (21)

| Método | Qué hace (docstring) |
|---|---|
| `cron_audit_program` | — |
| `cron_calibrations` | 56.38.0 (G-006): solo avisa, no bloquea, hasta que se carguen las fechas reales (decisión 2 de la tanda 2). El bloqueo «No usar» del equipo vencido queda detrás del parámetro ``quimibond_sgi.calibrat… |
| `cron_competences` | — |
| `cron_context_review` | Cron semanal: partes interesadas (4.1/4.2) con revisión vencida. Idempotente por resumen. |
| `cron_dnc` | Cron trimestral: cierra el ciclo de la DNC (P-A01). Cuenta las brechas de competencia abiertas y agenda al coordinador de RH la distribución de la encuesta DNC (F-P-A01-17) y el plan de capacitación.… |
| `cron_documents` | — |
| `cron_emergency_drills` | Cron diario: vigila los simulacros de los planes de emergencia vigentes (14001/45001 8.2). Idempotente por resumen. |
| `cron_indicators` | Diario desde I-6: escala los planes vencidos todos los días y mide solo el tercer día hábil (o cuando el mes anterior siga sin medir). Sin ``scheduled`` (a mano) mide siempre, como antes. |
| `cron_indicators_weekly` | Cron semanal: mide los indicadores de frecuencia semanal de la semana previa. |
| `cron_legal_requirements` | Cron diario: evaluaciones de cumplimiento vencidas y permisos por vencer (≤60 días) o vencidos. Idempotente por resumen. |
| `cron_my_procedure_stale` | Semanal: avisa al Jefe MAST qué puestos con personas tienen «Mi procedimiento» sin publicar o desactualizado. Una sola actividad, sobre la revisión vigente del primer puesto desactualizado (documents… |
| `cron_news` | — |
| `cron_nonconformities` | — |
| `cron_operational_signals` | Cron diario. (a) Falla repetitiva: ≥3 correctivas del mismo equipo en 90 días → actividad al Jefe MAST sugiriendo levantar NC y revisar el plan de mantenimiento. (b) Reclamación abierta con SLA venci… |
| `cron_overdue_actions` | Escalamiento en 3 niveles de acciones vencidas: - nivel 1 (responsable): ya lo recuerda la actividad espejo (Ola 0); - > N días: además su jefe directo (employee_id.parent_id.user_id, fallback Jefe M… |
| `cron_risk_review` | — |
| `cron_satisfaction_survey` | Cron trimestral: recuerda al Admin de Ventas distribuir la Encuesta de Satisfacción del Cliente (9001 9.1.2). Las respuestas alimentan el KPI CA-02 automáticamente. No envía correos a clientes por sí… |
| `cron_sign_elearning_sync` | Cron diario: sella acuses cuya firma electrónica ya se completó y otorga competencias de cursos eLearning terminados. |
| `cron_supplier_eval` | — |
| `cron_weekly_overdue_mail` | Cron semanal (D-14): a cada persona con algo atrasado en Mis pendientes, un correo con esa lista. Cada envío va en su savepoint: un correo que falla no detiene a los demás. Devuelve la lista de usuar… |
| `cron_worker_participation` | Cron semestral: recuerda distribuir la encuesta de consulta y participación de los trabajadores (45001 §5.4). Las respuestas y las quejas del canal interno alimentan la entrada 12 de la RxD. Idempote… |
