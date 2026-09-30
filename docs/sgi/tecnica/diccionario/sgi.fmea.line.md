<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.fmea.line`

**Línea de AMEF** (Model).

Modo de falla de un AMEF: severidad, ocurrencia y detección antes y después de las acciones (NPR).

Orden: `fmea_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_fmea.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:176` |
| `cause` | Char | Causa |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:170` |
| `current_controls` | Char | Controles actuales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:172` |
| `detection` | Selection | Detección (D) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:173` |
| `detection_post` | Selection | Detección post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:181` |
| `effect` | Char | Efecto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:168` |
| `failure_mode` | Char | Modo de falla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:167` |
| `fmea_id` | Many2one | AMEF |  | sí | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:164` |
| `npr` | Integer | NPR |  |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:174` |
| `npr_post` | Integer | NPR post |  |  |  | compute `_compute_npr_post`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:182` |
| `occurrence` | Selection | Ocurrencia (O) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:171` |
| `occurrence_post` | Selection | Ocurrencia post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:180` |
| `post_note` | Char | Justificación NPR post | Obligatoria para marcar el AMEF vigente cuando el NPR post de un modo de falla con NPR alto no baja respecto al inicial (p. ej. la severidad no puede reducirse por diseño). |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:183` |
| `requires_action` | Boolean | Requiere acción |  |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:175` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:165` |
| `severity` | Selection | Severidad (S) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:169` |
| `severity_post` | Selection | Severidad post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:179` |
| `step` | Char | Paso / Función |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:166` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `unlink` | — |
