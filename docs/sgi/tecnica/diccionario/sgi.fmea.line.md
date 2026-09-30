<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.fmea.line`

**Línea de AMEF** (Model).

Modo de falla de un AMEF: severidad, ocurrencia y detección antes y después de las acciones (NPR).

Orden: `fmea_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_fmea.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:167` |
| `cause` | Char | Causa |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:161` |
| `current_controls` | Char | Controles actuales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:163` |
| `detection` | Selection | Detección (D) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:164` |
| `detection_post` | Selection | Detección post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:172` |
| `effect` | Char | Efecto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:159` |
| `failure_mode` | Char | Modo de falla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:158` |
| `fmea_id` | Many2one | AMEF |  | sí | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:155` |
| `npr` | Integer | NPR |  |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:165` |
| `npr_post` | Integer | NPR post |  |  |  | compute `_compute_npr_post`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:173` |
| `occurrence` | Selection | Ocurrencia (O) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:162` |
| `occurrence_post` | Selection | Ocurrencia post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:171` |
| `post_note` | Char | Justificación NPR post | Obligatoria para marcar el AMEF vigente cuando el NPR post de un modo de falla con NPR alto no baja respecto al inicial (p. ej. la severidad no puede reducirse por diseño). |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:174` |
| `requires_action` | Boolean | Requiere acción |  |  |  | compute `_compute_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:166` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:156` |
| `severity` | Selection | Severidad (S) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:160` |
| `severity_post` | Selection | Severidad post |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:170` |
| `step` | Char | Paso / Función |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:157` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `unlink` | — |
