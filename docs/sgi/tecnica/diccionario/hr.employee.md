<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.employee`

Modelo de otra app que el SGI extiende.

56.7.0 (1.1 / 1.8): el procedimiento del empleado vive en campos GUARDADOS: se buscan, se agrupan y se leen por API (read/search_read) sin abrir la pantalla, y Mi equipo, «Ver como» y la ficha leen lo mismo. Se recalculan cuando cambia el …

Archivos: `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_competence.py`, `addons/quimibond_sgi/models/sgi_epp.py`, `addons/quimibond_sgi/models/sgi_hse_records.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_hr.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_document_ack_ids` | One2many | Acuses de lectura |  |  | `sgi.document.ack` |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:740` |
| `sgi_epp_delivery_count` | Integer |  |  |  |  | compute `_compute_sgi_epp_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:116` |
| `sgi_epp_delivery_ids` | One2many | Responsivas de EPP |  |  | `sgi.epp.delivery` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:115` |
| `sgi_epp_pending_count` | Integer | Responsivas sin firmar |  |  |  | compute `_compute_sgi_epp_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:117` |
| `sgi_epp_required` | Text | EPP requerido por el puesto |  |  |  | related `job_id.sgi_epp_required`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:114` |
| `sgi_health_record_ids` | One2many | Estudios y exámenes |  |  | `sgi.health.record` |  | quimibond_sgi.group_sgi_health,quimibond_sgi.group_sgi_manager | `addons/quimibond_sgi/models/sgi_hse_records.py:107` |
| `sgi_mp_job_id` | Many2one | Puesto (Mi procedimiento) |  |  | `hr.job` | related `current_version_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:222` |
| `sgi_mp_process_ids` | Many2many | Procesos donde participa |  |  | `sgi.process` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:237` |
| `sgi_mp_received_role_ids` | Many2many | Escalamientos que recibe |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:231` |
| `sgi_mp_role_ids` | Many2many | Mis actividades |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:228` |
| `sgi_mp_short_role_ids` | Many2many | Participa o se entera |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:234` |
| `sgi_my_procedure_ack_state` | Selection | Mi procedimiento |  |  |  | compute `_compute_sgi_my_procedure_ack`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:742` |
| `sgi_pending_ack_count` | Integer | # Acuses pendientes |  |  |  | compute `_compute_sgi_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:245` |
| `sgi_procedure_count` | Integer | # Mis procedimientos |  |  |  | compute `_compute_sgi_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:244` |
| `sgi_skill_gap_count` | Integer | Brechas de competencia |  |  |  | compute `_compute_sgi_skill_gap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_competence.py:8` |
| `sgi_team_ids` | Many2many | Equipos de venta | Equipos de venta de los que su usuario es miembro o líder (se capturan en Ventas). Filtran su «Mi procedimiento». |  | `crm.team` | compute `_compute_sgi_team_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:190` |
| `sgi_trial_date_end` | Date | Fin del periodo de prueba | Del contrato vigente; para medir las evaluaciones del periodo de prueba (S4-02). |  |  | related `version_id.trial_date_end`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_hr.py:56` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_deliver_epp` | «Entregar EPP»: responsiva nueva con el EPP del puesto propuesto. |
| `action_sgi_epp_deliveries` | — |
| `action_sgi_my_procedures` | — |
| `action_sgi_open_my_procedure` | — |
| `action_sgi_pending_acks` | — |
| `action_sgi_print_my_procedure` | — |
| `action_view_skill_gaps` | — |
