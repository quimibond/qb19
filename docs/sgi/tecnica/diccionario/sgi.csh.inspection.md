<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.csh.inspection`

**Recorrido de la Comisión de Seguridad e Higiene** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Recorrido de la Comisión de Seguridad e Higiene: fecha, áreas, participantes y hallazgos. Se cierra y se puede reabrir.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_hse_records.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `area` | Char | Áreas recorridas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:152` |
| `attachment_ids` | Many2many | Acta firmada (PDF) y fotos | Adjunte el acta firmada en PDF y las fotos del recorrido. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:159` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:172` |
| `date` | Date | Fecha del recorrido | Fecha en que se hizo el recorrido. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:149` |
| `department_ids` | Many2many | Departamentos | Departamentos que se recorrieron. |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:153` |
| `finding_count` | Integer | Número de hallazgos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hse_records.py:165` |
| `finding_ids` | One2many | Hallazgos |  |  | `sgi.csh.finding` |  | _CSH_FINDING_GROUPS | `addons/quimibond_sgi/models/sgi_hse_records.py:163` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:148` |
| `nc_count` | Integer | NC |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hse_records.py:166` |
| `notes` | Text | Acta / observaciones generales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:158` |
| `participant_ids` | Many2many | Integrantes de la Comisión | Integrantes de la Comisión de Seguridad e Higiene que participaron. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:155` |
| `state` | Selection | Estado | En captura mientras se registran los hallazgos; cerrado al terminar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:167` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_reopen` | — |
| `action_view_ncs` | — |
| `create` | — |
