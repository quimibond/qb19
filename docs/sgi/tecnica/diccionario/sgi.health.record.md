<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.health.record`

**Estudio de higiene o examen médico por trabajador** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `next_date, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_hse_records.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | Resultados (PDF) |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:53` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:59` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:46` |
| `department_id` | Many2one | Departamento |  |  |  | related `employee_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:41` |
| `employee_id` | Many2one | Trabajador |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:40` |
| `job_id` | Many2one | Puesto |  |  |  | related `employee_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:42` |
| `kind` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:43` |
| `name` | Char | Estudio o examen | Audiometría, espirometría, examen de ingreso, ruido (NOM-011), iluminación (NOM-025)… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:44` |
| `next_date` | Date | Vence | Fecha + vigencia; se puede corregir a mano. |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:48` |
| `notes` | Text | Observaciones y restricciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:52` |
| `provider_id` | Many2one | Laboratorio / médico |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:51` |
| `result` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:50` |
| `state` | Selection | Vigencia |  |  |  | compute `_compute_state`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:54` |
| `validity_months` | Integer | Vigencia (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:47` |

