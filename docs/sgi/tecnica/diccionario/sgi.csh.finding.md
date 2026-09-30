<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.csh.finding`

**Hallazgo del recorrido de la Comisión de Seguridad e Higiene** (Model).

Hallazgo de un recorrido de la Comisión de Seguridad e Higiene, con severidad y responsable; puede generar NC.

Orden: `inspection_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_hse_records.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No conformidad |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:227` |
| `description` | Text | Condición o acto inseguro |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:215` |
| `disposition` | Selection | Qué se hizo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:221` |
| `inspection_id` | Many2one |  |  | sí | `sgi.csh.inspection` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:213` |
| `justification` | Char | Motivo (sin acción) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:226` |
| `location` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:216` |
| `responsible_id` | Many2one | Responsable |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:220` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:214` |
| `severity` | Selection | Riesgo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_hse_records.py:217` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_nc` | El hallazgo nace como NC con origen «Recorrido de la Comisión de Seguridad e Higiene». |
