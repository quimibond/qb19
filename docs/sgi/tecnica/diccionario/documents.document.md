<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `documents.document`

Modelo de otra app que el SGI extiende.

L-017: las rutinas y sus conteos en el procedimiento anterior.

Archivos: `addons/quimibond_sgi/models/sgi_current_documents.py`, `addons/quimibond_sgi/models/sgi_document.py`, `addons/quimibond_sgi/models/sgi_document_owner.py`, `addons/quimibond_sgi/models/sgi_external_doc.py`, `addons/quimibond_sgi/models/sgi_formatos_bloque3.py`, `addons/quimibond_sgi/models/sgi_legacy_routine.py`, `addons/quimibond_sgi/models/sgi_my_procedure.py`, `addons/quimibond_sgi/models/sgi_my_procedure_sign.py`, `addons/quimibond_sgi/models/sgi_sign_elearning.py`, `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py`.

## Campos (61)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_ack_count` | Integer | # Acuses |  |  |  | compute `_compute_sgi_ack_stats`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:467` |
| `sgi_ack_ids` | One2many | Acuses de lectura |  |  | `sgi.document.ack` |  |  | `addons/quimibond_sgi/models/sgi_document.py:465` |
| `sgi_ack_read_pct` | Float | % Difusión | Porcentaje de acuses de lectura ya firmados sobre los pedidos. Se calcula solo. |  |  | compute `_compute_sgi_ack_stats`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:468` |
| `sgi_area_id` | Many2one | Área SGI | Área del SGI a la que pertenece el documento. |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_document.py:125` |
| `sgi_article_id` | Many2one | Artículo de Knowledge | Artículo del que se congeló esta revisión (DOC-5). |  | `knowledge.article` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:55` |
| `sgi_can_edit_transition` | Boolean |  |  |  |  | compute `_compute_sgi_can_edit_transition`, sin guardar |  | `addons/quimibond_sgi/models/sgi_document.py:263` |
| `sgi_child_document_ids` | One2many | Documentos hijos |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_document.py:480` |
| `sgi_code` | Char | Clave SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:69` |
| `sgi_content_hash` | Char | Huella del contenido |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:72` |
| `sgi_destination_label` | Char | Dónde vive en Odoo | Menú de Odoo ligado; si no hay, el worksheet de Calidad; si no, el texto «Destino en Odoo». Vacío = todavía sin destino. |  |  | compute `_compute_sgi_destination_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_document.py:249` |
| `sgi_disposition` | Selection | Disposición final | Qué se hace con el registro al cumplirse la retención. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:191` |
| `sgi_doc_change_id` | Many2one | Último cambio documental | Solicitud de cambio documental aprobada que produjo esta versión (la de alta, o la última modificación o baja aplicada). |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_document.py:132` |
| `sgi_doc_type` | Selection |  | Tipo de documento en forma de código (se calcula del tipo de documento). |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure.py:66` |
| `sgi_doc_type_id` | Many2one | Tipo de documento | Tipo de documento (procedimiento, instructivo, formato…). Define el patrón de la clave y si exige validación. |  | `sgi.document.type` |  |  | `addons/quimibond_sgi/models/sgi_document.py:92` |
| `sgi_ext_deadline` | Date | Implantar a más tardar | Recepción + 10 días hábiles (parámetro quimibond_sgi.external_doc_days). |  |  | compute `_compute_sgi_ext_deadline`, guardado |  | `addons/quimibond_sgi/models/sgi_external_doc.py:27` |
| `sgi_ext_implemented_date` | Date | Implantado el | Fecha en que el proceso dueño ya lo aplica (y difundió lo que cambia). |  |  |  |  | `addons/quimibond_sgi/models/sgi_external_doc.py:30` |
| `sgi_ext_issuer` | Char | Emisor | Quién emite el documento (cliente, norma, autoridad, proveedor). |  |  |  |  | `addons/quimibond_sgi/models/sgi_external_doc.py:21` |
| `sgi_ext_issuer_revision` | Char | Revisión del emisor | Revisión o edición como la trae el emisor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_external_doc.py:23` |
| `sgi_ext_received_date` | Date | Fecha de recepción | Fecha en que se recibió el documento externo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_external_doc.py:25` |
| `sgi_ext_state` | Selection | Implantación | Por implantar, vencido (pasó el plazo) o implantado. Se calcula solo. |  |  | compute `_compute_sgi_ext_state`, guardado |  | `addons/quimibond_sgi/models/sgi_external_doc.py:33` |
| `sgi_family_document_ids` | Many2many | Documentos de la familia | Hermanos (hijos del mismo padre) más los hijos propios. |  | `documents.document` | compute `_compute_sgi_family`, sin guardar |  | `addons/quimibond_sgi/models/sgi_document.py:482` |
| `sgi_is_controlled` | Boolean | Documento controlado SGI | Marque si es un documento controlado del SGI: lleva clave, revisión, estado y acuses. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:66` |
| `sgi_issue_date` | Date | Fecha de emisión | Fecha de emisión de esta revisión. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:144` |
| `sgi_job_ids` | Many2many | Puestos a los que aplica | Puestos que deben conocer el documento. Al publicarlo, a sus personas les llega el acuse de lectura. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_document.py:156` |
| `sgi_legacy_family` | Char | Familia del Dropbox | Clave del procedimiento del Dropbox al que pertenece el documento, sacada de su clave anterior (o de su clave si todavía no la tiene). |  |  | compute `_compute_sgi_legacy_family`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:256` |
| `sgi_legacy_routine_ids` | One2many | Rutinas del procedimiento anterior |  |  | `sgi.legacy.routine` |  | LEGACY_ROUTINE_GROUPS_ATTR | `addons/quimibond_sgi/models/sgi_legacy_routine.py:712` |
| `sgi_migration_class` | Selection | Clase de migración | A: el registro de Odoo sustituye al formato. B: se configura como punto de control con hoja de trabajo. C: Odoo lo genera como reporte. D: permanece como documento controlado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:217` |
| `sgi_migration_point_id` | Many2one | Worksheet destino | Punto de calidad (worksheet) que sustituye a este formato. El botón «Abrir worksheet» salta directo a él. |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_document.py:241` |
| `sgi_migration_state` | Selection | Estado de migración | Avance del paso de este documento del Dropbox a Odoo: pendiente, en curso, migrado, baja tramitada o se queda. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:227` |
| `sgi_migration_target` | Char | Destino en Odoo | Objeto/menú de Odoo que sustituye a este formato (p.ej. 'SGI > No Conformidades'). |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:236` |
| `sgi_my_ack_state` | Selection | Mi acuse | Su acuse de lectura de este documento: «Por leer» si le toca firmarlo, «Leído» si ya lo firmó y «Sin acuse» si no aplica a su puesto. |  |  | compute `_compute_sgi_my_ack_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_current_documents.py:25` |
| `sgi_next_review_date` | Date | Próxima revisión | Fecha de la próxima revisión del documento. Antes de esa fecha llegan dos avisos al responsable. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:160` |
| `sgi_obsolete_date` | Date | Obsoleto desde | Fecha en que el documento dejó de estar vigente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:164` |
| `sgi_obsolete_reason` | Char | Motivo de obsolescencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:166` |
| `sgi_odoo_menu_id` | Many2one | Menú de Odoo | Menú donde vive el formulario que sustituye a este documento. El botón «Abrir en Odoo» salta directo a él. |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_document.py:121` |
| `sgi_owner_id` | Many2one | Responsable SGI | Persona responsable del documento: recibe los avisos de revisión y de acuses pendientes. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_document.py:153` |
| `sgi_parent_document_id` | Many2one | Procedimiento padre | Procedimiento del que depende este documento (familia documental). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_document.py:476` |
| `sgi_pilot_end_date` | Date | Fin de prueba piloto | Fecha en que termina la prueba piloto del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:179` |
| `sgi_previous_code` | Char | Clave anterior | Clave con la que se conocía el documento antes de la clave nueva (la del Dropbox). Se busca siempre y no se sobrescribe. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:75` |
| `sgi_previous_code_date` | Date | Cambio de clave | Cuándo se cambió la clave en Odoo. Vacío = clave anterior del Dropbox, copiada por la migración. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:79` |
| `sgi_procedure_dirty` | Boolean | Procedimiento vivo pendiente de revisión | Las actividades/responsabilidades del procedimiento vivo cambiaron después de esta revisión vigente. Genere una nueva revisión controlada o confirme que el cambio no la amerita. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:204` |
| `sgi_procedure_dirty_by` | Many2one | Divergencia registrada por | Quién cambió las actividades del procedimiento después de la revisión vigente. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_document.py:212` |
| `sgi_procedure_dirty_since` | Datetime | Divergencia desde | Desde cuándo las actividades del procedimiento no coinciden con la revisión vigente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:209` |
| `sgi_process_id` | Many2one | Proceso SGI | Proceso del SGI al que pertenece el documento. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_document.py:127` |
| `sgi_publish_sign_request_id` | Many2one | Firma para entrar en vigor | Solicitud de Sign de la que depende que esta revisión entre en vigor. |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_my_procedure_sign.py:24` |
| `sgi_reference_ids` | Many2many | Referencias cruzadas | Documentos de OTRAS familias que este documento menciona (ej. P-A28 referencia P-A22, P-C01, P-D01). Lo captura MAST. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_document.py:486` |
| `sgi_replaced_by_process_id` | Many2one | Lo sustituye el proceso | Proceso de Odoo que sustituye a este procedimiento del Dropbox. El procedimiento sigue vigente mientras el proceso esté en borrador o piloto; cuando el proceso entra en vigor pasa a obsoleto y a «Baja tramitada». Lo captura el Jefe MAST. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_document.py:171` |
| `sgi_retention_years` | Integer | Retención (años) | Años que el registro/documento se conserva tras quedar obsoleto o cerrado. 0 = sin definir. Clientes automotrices suelen exigir vida del programa + años: captúrelo por documento o familia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:186` |
| `sgi_revision` | Integer | Revisión | Número de revisión del documento (00, 01…). Cada revisión aprobada lo sube en uno. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:139` |
| `sgi_revision_label` | Char | Rev. |  |  |  | compute `_compute_sgi_revision_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_document.py:142` |
| `sgi_routine_count` | Integer | Rutinas |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:715` |
| `sgi_routine_covered_count` | Integer | Cubiertas |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:717` |
| `sgi_routine_eliminated_count` | Integer | Eliminadas |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:721` |
| `sgi_routine_pending_count` | Integer | Pendientes |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:723` |
| `sgi_routine_replaced_count` | Integer | La hace Odoo |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:719` |
| `sgi_routine_resolved_pct` | Float | % resuelto |  |  |  | compute `_compute_sgi_routine_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:725` |
| `sgi_sign_template_auto` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:31` |
| `sgi_sign_template_id` | Many2one | Plantilla de firma (Sign) | Vacío: el SGI la arma sola (el PDF del documento más una hoja «Leí y entendí» con la firma colocada). Solo si se quiere otra, se elige aquí una plantilla hecha a mano en la app Firma. |  | `sign.template` |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:24` |
| `sgi_sign_template_rev` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:32` |
| `sgi_state` | Selection | Estado SGI | Solo los documentos controlados del SGI llevan estado; los demás archivos de Documentos quedan sin él (2026-09-25). |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:145` |
| `sgi_title` | Char | Título | Nombre del documento sin la clave ni la extensión del archivo. El archivo conserva su nombre original. |  |  | compute `_compute_sgi_title`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:85` |

## Métodos públicos (15)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_acks` | Crea acuses pendientes para los empleados de los puestos aplicables (idempotente). |
| `action_open_acks` | — |
| `action_sgi_assign_new_code` | Acción «Asignar clave nueva» (lista de documentos, Jefe MAST): a cada documento seleccionado le arma la siguiente clave del patrón de su tipo (D-02) con todas sus revisiones. Los que no aplican se re… |
| `action_sgi_mark_my_ack_read` | Firma MI acuse pendiente de este documento (Documentos vigentes). |
| `action_sgi_open_in_documents` | Abre el documento en la app nativa de Documentos (visor completo con carpetas), para quien quiera el explorador en vez de la ficha SGI. |
| `action_sgi_open_migration_point` | Del formato a su worksheet en un clic (y desde ahí, a sus checks). |
| `action_sgi_open_odoo_form` | «Abrir en Odoo» (57.0.0: lo usa todo Usuario SGI desde «Formatos y documentos anteriores»). Abre lo que sustituye al documento: el menú ligado (su acción; si no es una ventana, el menú mismo), si no … |
| `action_sgi_open_routines` | — |
| `action_sgi_resolve_odoo_menu` | Resuelve el «Menú de Odoo» desde el texto de «Destino en Odoo»: convierte «SGI > No Conformidades» en el menú real, igual que las actividades del procedimiento (los ir.* no se exponen por MCP, así qu… |
| `action_sgi_send_sign_requests` | Crea una solicitud de firma por cada acuse pendiente sin solicitud viva. Idempotente: re-ejecutar solo cubre a los que faltan. La creación corre con sudo (el candado real es el grupo del botón). |
| `action_sgi_view_file` | Abre el archivo del documento para previsualizarlo en el navegador. |
| `create` | — |
| `init` | Un solo VIGENTE por clave, garantizado en BD (la validación Python sola permite condición de carrera). |
| `unlink` | — |
| `write` | — |
