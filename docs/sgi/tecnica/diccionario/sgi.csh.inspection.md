<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.csh.inspection`

**Recorrido de la Comisión de Seguridad e Higiene** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Recorrido de la Comisión de Seguridad e Higiene: fecha, áreas, participantes y hallazgos. Se cierra y se puede reabrir.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_hse_records.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `area` | Char | Áreas recorridas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:139` |
| `attachment_ids` | Many2many | Acta firmada (PDF) y fotos |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:143` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:154` |
| `date` | Date | Fecha del recorrido |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:137` |
| `department_ids` | Many2many | Departamentos |  |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:140` |
| `finding_count` | Integer | Número de hallazgos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hse_records.py:148` |
| `finding_ids` | One2many | Hallazgos |  |  | `sgi.csh.finding` |  | _CSH_FINDING_GROUPS | `addons/quimibond_sgi/models/sgi_hse_records.py:146` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:136` |
| `nc_count` | Integer | NC |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hse_records.py:149` |
| `notes` | Text | Acta / observaciones generales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:142` |
| `participant_ids` | Many2many | Integrantes de la Comisión |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:141` |
| `state` | Selection | Estado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:150` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_reopen` | — |
| `action_view_ncs` | — |
| `create` | — |
