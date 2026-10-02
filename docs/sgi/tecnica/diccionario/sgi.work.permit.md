<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.work.permit`

**Permiso de trabajo de alto riesgo** (Model). Hereda de: `hr.mixin`, `sgi.base.mixin`, `sgi.format.mixin`.

Orden: `date_start desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_work_permit.py`.

## Campos (27)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `area_approved_by_id` | Many2one | Autorizó el área |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:125` |
| `area_approved_date` | Datetime | Fecha de autorización del área |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:127` |
| `area_manager_id` | Many2one | Jefe del área que autoriza | Autoriza por el área. Seguridad (Jefe MAST) autoriza aparte. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:122` |
| `check_ids` | One2many | Verificaciones y EPP |  |  | `sgi.work.permit.check` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:104` |
| `close_note` | Text | Condiciones al cierre | Cómo quedó el área: limpia, sin fuentes de ignición, energías restablecidas, guardas colocadas… |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:135` |
| `closed_by_id` | Many2one | Cerró |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:138` |
| `closed_date` | Datetime | Fecha de cierre |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:139` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:83` |
| `contractor_id` | Many2one | Contratista | Si el trabajo lo hace un contratista. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:98` |
| `date_end` | Datetime | Vigente hasta |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:107` |
| `date_start` | Datetime | Vigente desde |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:105` |
| `epp_notes` | Text | EPP adicional |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:103` |
| `equipment_id` | Many2one | Equipo |  |  | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:93` |
| `executor_ids` | Many2many | Personal que ejecuta |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:95` |
| `expired` | Boolean | Vencido | Autorizado y pasada su hora de fin. Se revisa cada hora. |  |  | compute `_compute_expired`, guardado |  | `addons/quimibond_sgi/models/sgi_work_permit.py:111` |
| `hazards` | Text | Peligros identificados | Qué puede salir mal: caída, atmósfera peligrosa, incendio, choque eléctrico… |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:100` |
| `location` | Char | Lugar exacto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:92` |
| `loto_ids` | One2many | Bloqueos (LOTO) | Bloqueos de energía ligados al permiso. El permiso no se cierra mientras uno siga aplicado. |  | `sgi.loto` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:113` |
| `maintenance_request_id` | Many2one | Orden de mantenimiento |  |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:94` |
| `name` | Char | Trabajo a realizar |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:86` |
| `requester_id` | Many2one | Solicitante |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:89` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:91` |
| `sgi_contractor_eval_ok` | Boolean | Contratista con evaluación SST vigente | La evaluación SST del contratista cubre hasta el fin del permiso. |  |  | compute `_compute_sgi_contractor_eval_ok`, sin guardar |  | `addons/quimibond_sgi/models/sgi_work_permit.py:117` |
| `sst_approved_by_id` | Many2one | Autorizó Seguridad |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:129` |
| `sst_approved_date` | Datetime | Fecha de autorización de Seguridad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:131` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:140` |
| `work_type` | Selection | Tipo de trabajo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:87` |

## Métodos públicos (9)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve_area` | — |
| `action_approve_sst` | — |
| `action_cancel` | — |
| `action_close` | — |
| `action_load_checks` | — |
| `action_reset` | Regresa a borrador y borra las autorizaciones (hay que volver a pedirlas). Desde cerrado o cancelado pasa por el candado de evidencia: solo el Jefe MAST. |
| `action_submit` | — |
| `create` | — |
| `write` | — |
