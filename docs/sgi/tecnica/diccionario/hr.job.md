<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.job`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_archived_filters.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_catalog.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (22)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_all_role_ids` | Many2many | Actividades SGI (propias y de su familia) |  |  | `sgi.activity.role` | compute `_compute_sgi_all_role_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:395` |
| `sgi_approve_count` | Integer | Aprueba |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:387` |
| `sgi_authorized_headcount` | Integer | Plantilla autorizada | Personas que Dirección autoriza para este puesto. RH-01 (cobertura de plantilla) compara contra ella a los empleados que lo ocupan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_ind2.py:737` |
| `sgi_document_ids` | Many2many | Documentos aplicables |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:192` |
| `sgi_epp_required` | Text | EPP requerido (S03-01) | Equipo de protección personal que exige este puesto. Sustituye el formato F-P-S03-01; fuente única para RH y SST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_integration.py:197` |
| `sgi_execute_count` | Integer | Ejecuta |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:385` |
| `sgi_family_id` | Many2one | Familia SGI | Familia de puestos del SGI (se define en la familia). El puesto hereda las actividades de su familia. |  | `sgi.job.family` | compute `_compute_sgi_family_id`, guardado |  | `addons/quimibond_sgi/models/sgi_catalog.py:330` |
| `sgi_family_ids` | Many2many | Familias SGI |  |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:327` |
| `sgi_inform_count` | Integer | Se entera |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:391` |
| `sgi_mp_hash_current` | Char | Huella actual de Mi procedimiento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:121` |
| `sgi_mp_job_late` | Integer | Actividades atrasadas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:123` |
| `sgi_mp_job_ok` | Integer | Actividades al día |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:124` |
| `sgi_mp_job_total` | Integer | Actividades del puesto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:127` |
| `sgi_mp_job_unmeasured` | Integer | Actividades sin medición automática |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:125` |
| `sgi_mp_stats_at` | Datetime | Cifras de Mi procedimiento al | Vacío: el puesto cambió y sus cifras se recalculan en la siguiente corrida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:128` |
| `sgi_my_procedure_doc_id` | Many2one | Mi procedimiento (vigente) |  |  | `documents.document` | compute `_compute_sgi_my_procedure_doc`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:106` |
| `sgi_my_procedure_stale` | Boolean | Mi procedimiento desactualizado | Las actividades del puesto cambiaron desde la última revisión publicada de «Mi procedimiento». |  |  | compute `_compute_sgi_my_procedure_doc`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:109` |
| `sgi_participate_count` | Integer | Participa |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:389` |
| `sgi_role_count` | Integer | Actividades SGI |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:393` |
| `sgi_team_ids` | Many2many | Equipos de venta de sus personas | Equipos de venta de los que son miembros o líderes las personas del puesto. Si todas están en alguno, «Mi procedimiento» del puesto muestra solo lo general y lo de esos equipos. |  | `crm.team` | compute `_compute_sgi_team_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:145` |
| `sgi_vacancy_approved` | Boolean | Vacante aprobada (SGI) | El puesto no tiene personas porque se va a contratar. Mientras esté vigente, el SGI lo acepta como responsable con advertencia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:337` |
| `sgi_vacancy_until` | Date | Vacante vigente hasta | Vacío = sin fecha límite. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:341` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open_my_procedure` | — |
| `action_sgi_open_my_procedure_doc` | — |
| `action_sgi_print_my_procedure` | — |
| `action_sgi_publish_all_my_procedures` | Publica (o deja igual) «Mi procedimiento» de todos los puestos con roles y personas. Una revisión nueva solo donde cambió el contenido. |
| `action_sgi_publish_my_procedure` | Archiva el PDF como documento controlado del puesto (revisión nueva solo si cambió el contenido) y deja pendiente el acuse de todos los empleados del puesto. |
| `action_sgi_view_roles` | — |
| `sgi_merge_duplicate_jobs` | Fusiona puestos duplicados (mismo nombre normalizado y empresa) y limpia los saltos de línea de los nombres. |
