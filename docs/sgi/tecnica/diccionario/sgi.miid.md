<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid`

**Manual del SGI (MIID)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Manual del SGI (MIID) de una empresa: la vista del sistema, su comparación con la revisión vigente y el historial de revisiones. Uno por empresa; lo crea la primera apertura o el paso diario.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (48)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_ids` | Many2many | Rutinas |  |  | `sgi.process.activity` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:694` |
| `blocker_html` | Html | Lo que impide aprobar una revisión |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:660` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:640` |
| `diff_html` | Html | Diferencias con la revisión vigente |  |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:658` |
| `doc_type_ids` | Many2many | Tipos de documento |  |  | `sgi.document.type` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:688` |
| `document_id` | Many2one | Revisión vigente |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:643` |
| `history_html` | Html | Historial de revisiones |  |  |  | compute `_compute_history_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:670` |
| `issue_date` | Date | Emisión de la revisión vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:646` |
| `kind` | Selection | Documento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:641` |
| `last_check` | Datetime | Última comparación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:665` |
| `live_html` | Html | Vista del sistema |  |  |  | compute `_compute_live_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:666` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:639` |
| `norm_ids` | Many2many | Normas |  |  | `sgi.norm` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:675` |
| `objective_ids` | Many2many | Objetivos integrales |  |  | `sgi.objective` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:685` |
| `outdated_since` | Datetime | Desactualizado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:664` |
| `pending_request_id` | Many2one | Solicitud de cambio en curso |  |  | `approval.request` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:650` |
| `pg01_doc_type_ids` | Many2many | Tipos de documento del P-G01 |  |  | `sgi.document.type` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:691` |
| `pg01_norm_ids` | Many2many | Normas del P-G01 |  |  | `sgi.norm` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:677` |
| `policy_id` | Many2one | Política vigente |  |  | `sgi.policy` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:682` |
| `policy_text` | Html | Texto de la política |  |  |  | related `policy_id.policy_text`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:684` |
| `process_ids` | Many2many | Procesos |  |  | `sgi.process` | compute `_compute_screen_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:680` |
| `revision_ids` | Many2many | Revisiones |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:648` |
| `revision_label` | Char | Rev. vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:645` |
| `scr_cambios_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:709` |
| `scr_desarrollo_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:704` |
| `scr_desarrollo_tipos_documento_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:716` |
| `scr_documentos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:705` |
| `scr_documentos_tipos_documento_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:715` |
| `scr_donde_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:701` |
| `scr_general_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:696` |
| `scr_marco_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:707` |
| `scr_marco_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:718` |
| `scr_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:700` |
| `scr_normas_normas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:711` |
| `scr_objeto_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:697` |
| `scr_otros_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:710` |
| `scr_proposito_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:698` |
| `scr_requisitos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:703` |
| `scr_responsabilidades_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:699` |
| `scr_rutinas_actividades_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:717` |
| `scr_rutinas_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:706` |
| `scr_sistema_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:702` |
| `scr_sistema_objetivos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:714` |
| `scr_sistema_politica_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:713` |
| `scr_sistema_procesos_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:712` |
| `scr_transicion_html` | Html |  |  |  |  | compute `_compute_screen`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:708` |
| `state` | Selection | Situación | Al día: la revisión vigente coincide con los datos del sistema. Sin línea base: la revisión vigente no se generó desde Odoo y no hay contra qué comparar. |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:652` |
| `to_confirm_count` | Integer | Secciones por confirmar |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:662` |

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
