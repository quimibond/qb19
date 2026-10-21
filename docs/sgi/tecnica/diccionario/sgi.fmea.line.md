<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.fmea.line`

**Línea de AMEF** (Model).

Modo de falla de un AMEF: severidad, ocurrencia y detección antes y después de las acciones (NPR).

Orden: `fmea_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_fmea.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:181` |
| `cause` | Char | Causa |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:173` |
| `current_controls` | Char | Controles actuales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:175` |
| `detection` | Selection | Detección (D) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:176` |
| `detection_post` | Selection | Detección post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:186` |
| `effect` | Char | Efecto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:171` |
| `failure_mode` | Char | Modo de falla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:170` |
| `fmea_id` | Many2one | AMEF |  | sí | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:167` |
| `npr` | Integer | NPR | Severidad × ocurrencia × detección. Con 100 o más la línea pide acción terminada y re-evaluación antes de marcar el AMEF vigente. |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:177` |
| `npr_post` | Integer | NPR post |  |  |  | compute `_compute_npr_post`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:187` |
| `occurrence` | Selection | Ocurrencia (O) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:174` |
| `occurrence_post` | Selection | Ocurrencia post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:185` |
| `post_note` | Char | Justificación NPR post | Obligatoria para marcar el AMEF vigente cuando el NPR post de un modo de falla con NPR alto no baja respecto al inicial (p. ej. la severidad no puede reducirse por diseño). |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:188` |
| `requires_action` | Boolean | Requiere acción |  |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:180` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:168` |
| `severity` | Selection | Severidad (S) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:172` |
| `severity_post` | Selection | Severidad post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:184` |
| `step` | Char | Paso / Función |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:169` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `unlink` | — |
