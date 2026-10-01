<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.role`

**Rol de un puesto en una actividad SGI** (Model).

Papel de un puesto, una familia o un rol relativo en una actividad (ejecuta, aprueba, participa, informa, escala). Es la fuente de Mi procedimiento, de Mis pendientes y de los aprobadores nativos.

Orden: `activity_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_catalog.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_approval_native.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`, `addons/quimibond_sgi/models/sgi_relative_roles.py`, `addons/quimibond_sgi_studio/models/sgi_approval_studio.py`.

## Campos (47)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_active` | Boolean | Actividad activa |  |  |  | related `activity_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:83` |
| `activity_id` | Many2one | Actividad | Actividad a la que se asigna el rol. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:137` |
| `activity_menu_id` | Many2one | Menú de Odoo | Menú de Odoo donde se hace la actividad. |  |  | related `activity_id.odoo_menu_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:85` |
| `activity_when` | Char | Cuándo |  |  |  | compute `_compute_activity_when`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:88` |
| `after_days` | Integer | Escala a los (días hábiles) | Solo para «Escala»: días hábiles después del plazo de la actividad en que se avisa a este destino. Puede haber varios niveles. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:165` |
| `approval_category_id` | Many2one | Categoría de Aprobaciones | Vacía: «Sincronizar» crea una categoría propia con las personas del puesto. |  | `approval.category` |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:170` |
| `approval_conflict_rule_ids` | Many2many | Otras reglas en el botón | Reglas de aprobación activas en el mismo botón que no mantiene este rol: el documento pediría dos aprobaciones. Adóptala o quítala antes de sincronizar. |  | `studio.approval.rule` | compute `_compute_approval_conflicts`, sin guardar |  | `addons/quimibond_sgi_studio/models/sgi_approval_studio.py:77` |
| `approval_domain` | Char | Condición en Odoo |  |  |  | compute `_compute_approval_domain`, guardado |  | `addons/quimibond_sgi/models/sgi_approval_native.py:194` |
| `approval_entry_count` | Integer | Aprobaciones dadas |  |  |  | compute `_compute_approval_entries`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_native.py:202` |
| `approval_kind` | Selection | Cómo se aprueba | Botón de Odoo: la regla nativa bloquea el botón del documento. Solicitud: una categoría de Aprobaciones para decisiones sin documento. Firma: una plantilla de Sign. |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:166` |
| `approval_last_date` | Datetime | Última aprobación | Fecha de la última aprobación dada con esta regla. |  |  | compute `_compute_approval_entries`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_native.py:203` |
| `approval_method` | Char | Botón que se aprueba | Método del botón (button_confirm, action_post, button_validate…). |  |  | compute `_compute_approval_method`, guardado |  | `addons/quimibond_sgi/models/sgi_approval_native.py:182` |
| `approval_model_id` | Many2one | Documento que se aprueba | Modelo de Odoo cuyo botón queda bloqueado hasta que el puesto aprueba. Se sugiere el modelo que materializa la actividad. |  | `ir.model` | compute `_compute_approval_model_id`, guardado |  | `addons/quimibond_sgi/models/sgi_approval_native.py:177` |
| `approval_rule_id` | Many2one | Regla nativa | Regla de aprobación de Odoo que el SGI creó para este rol. |  | `studio.approval.rule` |  |  | `addons/quimibond_sgi_studio/models/sgi_approval_studio.py:81` |
| `approval_sign_template_id` | Many2one | Plantilla de Sign | Plantilla de Sign que se firma para aprobar, cuando la aprobación es por firma. |  | `sign.template` |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:173` |
| `approval_state` | Selection | Aprobación en Odoo | Si la aprobación ya funciona en Odoo o qué le falta (configurarla, personas en el puesto, otra regla en el mismo botón). |  |  | compute `_compute_approval_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_native.py:198` |
| `approval_user_ids` | Many2many | Personas que aprueban | Personas que hoy aprueban: las del puesto o la familia. |  | `res.users` | compute `_compute_approval_users`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_native.py:195` |
| `cadence` | Selection | Cadencia | Cada cuánto se espera la actividad. |  |  | related `activity_id.measure_cadence`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:80` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_catalog.py:179` |
| `condition` | Char | Condición | Solo para quien aprueba o se entera: cuándo aplica, ej. «arriba del monto que se fije». Vacío = siempre. Si según el caso la ejecuta otro puesto, son dos actividades. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:169` |
| `condition_field_id` | Many2one | Campo de la condición | Solo cuando la aprobación aplica bajo una condición (ej. total de la orden). |  | `ir.model.fields` |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:185` |
| `condition_operator` | Selection | Operador | Cómo se compara el campo con el valor para que la aprobación aplique. |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:190` |
| `condition_value` | Char | Valor | Número, texto o True/False. |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:193` |
| `family_id` | Many2one | Familia de puestos | Familia de puestos a la que se asigna el rol (cuando «Asignado a» es familia). |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:153` |
| `job_id` | Many2one | Puesto | Puesto al que se asigna el rol (cuando «Asignado a» es puesto). |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:150` |
| `mp_can_go` | Boolean | Se puede ir a hacer | Indica si la actividad tiene un menú de Odoo al que se puede ir con «Ir a hacerlo». |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:328` |
| `mp_check_against` | Char | Contra qué se revisa |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:316` |
| `mp_done` | Text | Terminada cuando |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:317` |
| `mp_escalates` | Char | Si se atora, escala a |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:322` |
| `mp_external` | Char | Se hace en |  |  |  | related `activity_id.external_system`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:324` |
| `mp_how` | Text | Cómo |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:314` |
| `mp_inputs` | Char | Recibe |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:319` |
| `mp_instruction_id` | Many2one | Instructivo | Instructivo de la actividad. |  |  | related `activity_id.instruction_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:325` |
| `mp_name` | Char | Nombre de la actividad |  |  |  | related `activity_id.name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:313` |
| `mp_norms` | Char | Cumple con |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:323` |
| `mp_number` | Char | Numeral |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:312` |
| `mp_on_fail` | Text | Si no se puede |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:318` |
| `mp_outputs` | Char | Entrega |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:320` |
| `mp_related` | Char | Conforme a |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:321` |
| `mp_status` | Selection | Estado | Al día, atrasada o sin medición automática. Se calcula al mostrarlo. |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:309` |
| `mp_status_detail` | Char | Detalle del estado |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:311` |
| `mp_where` | Char | Dónde |  |  |  | compute `_compute_mp_pieces`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:315` |
| `process_id` | Many2one | Proceso | Proceso de la actividad. |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_catalog.py:175` |
| `relative_role` | Selection | Rol relativo | Rol que no es de un puesto fijo. «Dueño del proceso»: el dueño del proceso del registro (o, si no tiene, el de la actividad). Solicitante, jefe del que pide, quien detecta y área responsable se resuelven con cada registro (quien lo pide o crea, su jefe, el responsable del departamento). Quien aprue… |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:157` |
| `role` | Selection | Rol | Ejecuta (la hace), aprueba, participa, se entera o escala (recibe el atraso). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:141` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:174` |
| `target_type` | Selection | Asignado a | A quién toca: un puesto, una familia de puestos o un rol relativo (el solicitante, quien detecta, el dueño del proceso…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:146` |

## Métodos públicos (12)

| Método | Qué hace (docstring) |
|---|---|
| `action_mp_go` | «Ir a hacerlo»: el menú real de Odoo donde se ejecuta el paso. |
| `action_mp_instruction` | «Ver instructivo»: el archivo del IT de la actividad. |
| `action_mp_propose_change` | — |
| `action_open_activity` | — |
| `action_sgi_adopt_rule` | Adopta la regla manual que ya existe en el botón: la liga al rol y la deja con las personas del puesto y la condición del rol. |
| `action_sgi_open_approval_entries` | — |
| `action_sgi_sync_approval` | — |
| `create` | — |
| `cron_sgi_sync_approvals` | Cada noche: aprobadores al día (cambian las personas de los puestos) y reglas archivadas si la actividad o el rol ya no existen. |
| `sgi_relative_roles_report` | Por cada rol relativo de una actividad activa: a quién se resuelve sin registro (catálogo, escalamientos, aprobaciones nativas) y el aviso de la regla aprobador ≠ ejecutor. Solo lectura. |
| `unlink` | — |
| `write` | — |
