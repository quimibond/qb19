<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.employee`

Modelo de otra app que el SGI extiende.

57.95.0 (K-08): resumen de Mis pendientes guardado por persona. Lo leen los filtros de Mi equipo («Con pendientes atrasados», «por vencer», «Al día»), que antes armaban Mis pendientes de toda la empresa en cada búsqueda. Lo refrescan el re…

Archivos: `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_competence.py`, `addons/quimibond_sgi/models/sgi_epp.py`, `addons/quimibond_sgi/models/sgi_floor_kiosk.py`, `addons/quimibond_sgi/models/sgi_hse_records.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_hr.py`, `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (21)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_document_ack_ids` | One2many | Acuses de lectura |  |  | `sgi.document.ack` |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:744` |
| `sgi_epp_delivery_count` | Integer |  |  |  |  | compute `_compute_sgi_epp_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:144` |
| `sgi_epp_delivery_ids` | One2many | Responsivas de EPP |  |  | `sgi.epp.delivery` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:143` |
| `sgi_epp_pending_count` | Integer | Responsivas sin firmar |  |  |  | compute `_compute_sgi_epp_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:145` |
| `sgi_epp_required` | Text | EPP requerido por el puesto |  |  |  | related `job_id.sgi_epp_required`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:142` |
| `sgi_health_record_ids` | One2many | Estudios y exámenes |  |  | `sgi.health.record` |  | quimibond_sgi.group_sgi_health,quimibond_sgi.group_sgi_manager | `addons/quimibond_sgi/models/sgi_hse_records.py:119` |
| `sgi_missing_data` | Char | Le falta | Puesto (sin él no hay Mi procedimiento) o correo de trabajo (sin él no recibe firmas de Firma electrónica). |  |  | compute `_compute_sgi_missing_data`, sin guardar | hr.group_hr_user | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:424` |
| `sgi_mp_job_id` | Many2one | Puesto (Mi procedimiento) |  |  | `hr.job` | related `current_version_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:231` |
| `sgi_mp_process_ids` | Many2many | Procesos donde participa | Procesos en los que participa la persona por su puesto o su familia. Se calcula solo. |  | `sgi.process` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:249` |
| `sgi_mp_received_role_ids` | Many2many | Escalamientos que recibe | Actividades cuyo atraso le escala a esta persona. Se calcula solo. |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:241` |
| `sgi_mp_role_ids` | Many2many | Mis actividades | Actividades que la persona ejecuta o aprueba por su puesto o su familia. Se calcula solo. |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:237` |
| `sgi_mp_short_role_ids` | Many2many | Participa o se entera | Actividades en las que la persona participa o solo se entera. Se calcula solo. |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles_stored`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:245` |
| `sgi_my_procedure_ack_state` | Selection | Mi procedimiento | Si la persona ya firmó de leído su Mi procedimiento vigente. Se calcula solo. |  |  | compute `_compute_sgi_my_procedure_ack`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:746` |
| `sgi_pending_ack_count` | Integer | # Acuses pendientes |  |  |  | compute `_compute_sgi_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:263` |
| `sgi_pending_saved_late` | Integer | Pendientes atrasados (resumen guardado) |  |  |  |  | base.group_system | `addons/quimibond_sgi/models/sgi_my_pending.py:926` |
| `sgi_pending_saved_state` | Selection | Semáforo (resumen guardado) |  |  |  |  | base.group_system | `addons/quimibond_sgi/models/sgi_my_pending.py:929` |
| `sgi_pending_saved_total` | Integer | Pendientes (resumen guardado) | Total de Mis pendientes de la persona la última vez que se calculó. |  |  |  | base.group_system | `addons/quimibond_sgi/models/sgi_my_pending.py:922` |
| `sgi_procedure_count` | Integer | # Mis procedimientos |  |  |  | compute `_compute_sgi_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:262` |
| `sgi_skill_gap_count` | Integer | Brechas de competencia |  |  |  | compute `_compute_sgi_skill_gap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_competence.py:8` |
| `sgi_team_ids` | Many2many | Equipos de venta | Equipos de venta de los que su usuario es miembro o líder (se capturan en Ventas). Filtran su «Mi procedimiento». |  | `crm.team` | compute `_compute_sgi_team_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:192` |
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
