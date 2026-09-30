<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.health.record`

**Estudio de higiene o examen médico por trabajador** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Estudio de higiene o examen médico de un trabajador, con resultado y vigencia. Dato sensible: solo Salud ocupacional y Jefe MAST.

Orden: `next_date, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_hse_records.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | Resultados (PDF) | Adjunte los resultados en PDF. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:63` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:71` |
| `date` | Date | Fecha | Fecha del examen o del estudio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:52` |
| `department_id` | Many2one | Departamento | Departamento del trabajador. |  |  | related `employee_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:44` |
| `employee_id` | Many2one | Trabajador | Trabajador al que corresponde el registro. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:42` |
| `job_id` | Many2one | Puesto | Puesto del trabajador. |  |  | related `employee_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:46` |
| `kind` | Selection | Tipo | Examen médico o estudio de higiene. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:48` |
| `name` | Char | Estudio o examen | Audiometría, espirometría, examen de ingreso, ruido (NOM-011), iluminación (NOM-025)… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:50` |
| `next_date` | Date | Vence | Fecha + vigencia; se puede corregir a mano. |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:56` |
| `notes` | Text | Observaciones y restricciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:62` |
| `provider_id` | Many2one | Laboratorio / médico | Laboratorio o médico que lo hizo. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:60` |
| `result` | Selection | Resultado | Resultado del examen o del estudio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:58` |
| `state` | Selection | Vigencia | Vigente, por vencer (30 días) o vencido según la fecha y la vigencia. Se calcula solo. |  |  | compute `_compute_state`, guardado |  | `addons/quimibond_sgi/models/sgi_hse_records.py:65` |
| `validity_months` | Integer | Vigencia (meses) | Meses que dura vigente el resultado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:54` |

