<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.job`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_archived_filters.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_catalog.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (22)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_all_role_ids` | Many2many | Actividades SGI (propias y de su familia) | Actividades del SGI del puesto: las propias y las de su familia de puestos. |  | `sgi.activity.role` | compute `_compute_sgi_all_role_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:406` |
| `sgi_approve_count` | Integer | Aprueba |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:398` |
| `sgi_authorized_headcount` | Integer | Plantilla autorizada | Personas que Dirección autoriza para este puesto. RH-01 (cobertura de plantilla) compara contra ella a los empleados que lo ocupan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_ind2.py:754` |
| `sgi_document_ids` | Many2many | Documentos aplicables | Documentos controlados que aplican a este puesto. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:209` |
| `sgi_epp_required` | Text | EPP requerido (S03-01) | Equipo de protección personal que exige este puesto. Sustituye el formato F-P-S03-01; fuente única para RH y SST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_integration.py:215` |
| `sgi_execute_count` | Integer | Ejecuta |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:396` |
| `sgi_family_id` | Many2one | Familia SGI | Familia de puestos del SGI (se define en la familia). El puesto hereda las actividades de su familia. |  | `sgi.job.family` | compute `_compute_sgi_family_id`, guardado |  | `addons/quimibond_sgi/models/sgi_catalog.py:341` |
| `sgi_family_ids` | Many2many | Familias SGI |  |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:338` |
| `sgi_inform_count` | Integer | Se entera |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:402` |
| `sgi_mp_hash_current` | Char | Huella actual de Mi procedimiento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:125` |
| `sgi_mp_job_late` | Integer | Actividades atrasadas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:127` |
| `sgi_mp_job_ok` | Integer | Actividades al día |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:128` |
| `sgi_mp_job_total` | Integer | Actividades del puesto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:131` |
| `sgi_mp_job_unmeasured` | Integer | Actividades sin medición automática |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:129` |
| `sgi_mp_stats_at` | Datetime | Cifras de Mi procedimiento al | Vacío: el puesto cambió y sus cifras se recalculan en la siguiente corrida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:132` |
| `sgi_my_procedure_doc_id` | Many2one | Mi procedimiento (vigente) | Revisión vigente del Mi procedimiento del puesto, publicada por el Jefe MAST y SGI. |  | `documents.document` | compute `_compute_sgi_my_procedure_doc`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:109` |
| `sgi_my_procedure_stale` | Boolean | Mi procedimiento desactualizado | Las actividades del puesto cambiaron desde la última revisión publicada de «Mi procedimiento». |  |  | compute `_compute_sgi_my_procedure_doc`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:113` |
| `sgi_participate_count` | Integer | Participa |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:400` |
| `sgi_role_count` | Integer | Actividades SGI |  |  |  | compute `_compute_sgi_role_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:404` |
| `sgi_team_ids` | Many2many | Equipos de venta de sus personas | Equipos de venta de los que son miembros o líderes las personas del puesto. Si todas están en alguno, «Mi procedimiento» del puesto muestra solo lo general y lo de esos equipos. |  | `crm.team` | compute `_compute_sgi_team_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:147` |
| `sgi_vacancy_approved` | Boolean | Vacante aprobada (SGI) | El puesto no tiene personas porque se va a contratar. Mientras esté vigente, el SGI lo acepta como responsable con advertencia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:348` |
| `sgi_vacancy_until` | Date | Vacante vigente hasta | Vacío = sin fecha límite. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:352` |

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
