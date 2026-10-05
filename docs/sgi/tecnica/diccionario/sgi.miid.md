<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid`

**Manual del SGI (MIID)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Manual del SGI (MIID) de una empresa: la vista del sistema, su comparación con la revisión vigente y el historial de revisiones. Uno por empresa; lo crea la primera apertura o el paso diario.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `blocker_html` | Html | Lo que impide aprobar una revisión |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:233` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:215` |
| `diff_html` | Html | Diferencias con la revisión vigente |  |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:231` |
| `document_id` | Many2one | Revisión vigente del MIID |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:216` |
| `history_html` | Html | Historial de revisiones |  |  |  | compute `_compute_history_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:243` |
| `issue_date` | Date | Emisión de la revisión vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:219` |
| `last_check` | Datetime | Última comparación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:238` |
| `live_html` | Html | Vista del sistema |  |  |  | compute `_compute_live_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:239` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:214` |
| `outdated_since` | Datetime | Desactualizado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:237` |
| `pending_request_id` | Many2one | Solicitud de cambio en curso |  |  | `approval.request` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:223` |
| `revision_ids` | Many2many | Revisiones del MIID |  |  | `documents.document` | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:221` |
| `revision_label` | Char | Rev. vigente |  |  |  | compute `_compute_document`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:218` |
| `state` | Selection | Situación del MIID | Al día: la revisión vigente coincide con los datos del sistema. Sin línea base: la revisión vigente no se generó desde Odoo y no hay contra qué comparar. |  |  | compute `_compute_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:225` |
| `to_confirm_count` | Integer | Secciones por confirmar |  |  |  | compute `_compute_blockers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:235` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | — |
| `action_open_request` | — |
| `action_open_sections` | — |
| `action_open_vigente_pdf` | — |
| `action_print_live` | — |
| `action_sgi_miid_request_change` | Arma (o abre) la solicitud de cambio documental del MIID con el PDF generado, la huella y las diferencias. Nunca la envía ni la aprueba. |
