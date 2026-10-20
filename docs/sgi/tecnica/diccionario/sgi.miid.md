<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid`

**Manual del SGI (MIID)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Manual del SGI (MIID) de una empresa: la vista del sistema, su comparación con la revisión vigente y el historial de revisiones. Uno por empresa; lo crea la primera apertura o el paso diario.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (50)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `blocker_html` | Html | Lo que impide aprobar una revisión |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:782` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:762` |
| `diff_html` | Html | Diferencias con la revisión vigente |  |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:780` |
| `doc_type_ids` | Many2many | Tipos de documento |  |  | `sgi.document.type` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:810` |
| `document_id` | Many2one | Revisión vigente |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:765` |
| `history_html` | Html | Historial de revisiones |  |  |  | compute `_compute_history_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:792` |
| `issue_date` | Date | Emisión de la revisión vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:768` |
| `kind` | Selection | Documento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:763` |
| `last_check` | Datetime | Última comparación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:787` |
| `live_html` | Html | Vista del sistema |  |  |  | compute `_compute_live_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:788` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:761` |
| `norm_ids` | Many2many | Normas |  |  | `sgi.norm` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:797` |
| `objective_ids` | Many2many | Objetivos integrales |  |  | `sgi.objective` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:807` |
| `outdated_since` | Datetime | Desactualizado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:786` |
| `pending_request_id` | Many2one | Solicitud de cambio en curso |  |  | `approval.request` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:772` |
| `pg01_doc_type_ids` | Many2many | Tipos de documento del P-G01 |  |  | `sgi.document.type` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:813` |
| `pg01_norm_ids` | Many2many | Normas del P-G01 |  |  | `sgi.norm` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:799` |
| `policy_id` | Many2one | Política vigente |  |  | `sgi.policy` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:804` |
| `policy_text` | Html | Texto de la política |  |  |  | related `policy_id.policy_text`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:806` |
| `process_ids` | Many2many | Procesos |  |  | `sgi.process` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:802` |
| `revision_ids` | Many2many | Revisiones |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:770` |
| `revision_label` | Char | Rev. vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:767` |
| `routine_ids` | Many2many | Rutinas |  |  | `sgi.process.activity` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:818` |
| `rule_ids` | Many2many | Reglas del sistema |  |  | `sgi.system.rule` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:820` |
| `scr_cambios_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:836` |
| `scr_desarrollo_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:831` |
| `scr_desarrollo_tipos_documento_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:843` |
| `scr_documentos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:832` |
| `scr_documentos_tipos_documento_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:842` |
| `scr_donde_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:828` |
| `scr_general_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:823` |
| `scr_marco_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:834` |
| `scr_marco_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:845` |
| `scr_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:827` |
| `scr_normas_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:838` |
| `scr_objeto_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:824` |
| `scr_otros_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:837` |
| `scr_proposito_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:825` |
| `scr_requisitos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:830` |
| `scr_requisitos_reglas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:822` |
| `scr_responsabilidades_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:826` |
| `scr_rutinas_actividades_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:844` |
| `scr_rutinas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:833` |
| `scr_sistema_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:829` |
| `scr_sistema_objetivos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:841` |
| `scr_sistema_politica_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:840` |
| `scr_sistema_procesos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:839` |
| `scr_transicion_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:835` |
| `state` | Selection | Situación | Al día: la revisión vigente coincide con los datos del sistema. Sin línea base: la revisión vigente no se generó desde Odoo y no hay contra qué comparar. |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:774` |
| `to_confirm_count` | Integer | Secciones por confirmar |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:784` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | La pantalla del documento generado; ``sgi_miid_kind`` en el contexto elige cuál (por omisión el MIID). |
| `action_open_request` | — |
| `action_open_revisions` | 57.161.0: las revisiones de este documento (vigente y obsoletas). |
| `action_open_sections` | — |
| `action_open_vigente_pdf` | — |
| `action_print_live` | — |
| `action_sgi_miid_request_change` | Arma (o abre) la solicitud de cambio documental del MIID con el PDF generado, la huella y las diferencias. Nunca la envía ni la aprueba. |
