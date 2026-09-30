<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.ppap.element`

**Elemento de un PPAP** (Model).

Elemento de un PPAP con su documento, AMEF o plan de control y su estado.

Orden: `ppap_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_ppap.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `control_plan_id` | Many2one | Plan de control |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:270` |
| `document_id` | Many2one | Documento |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:271` |
| `fmea_id` | Many2one | AMEF |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:269` |
| `name` | Char | Elemento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:255` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:272` |
| `ppap_id` | Many2one | PPAP |  | sí | `sgi.ppap` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:252` |
| `sequence` | Integer | N° |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:254` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:256` |
| `submission` | Selection | S/R | Según el nivel AIAG del expediente: S se presenta al cliente y bloquea el envío si está pendiente; R se retiene en planta a disposición. Editable por elemento (p. ej. nivel 4). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:262` |
| `template_id` | Many2one | Elemento (catálogo) |  |  | `sgi.ppap.element.template` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:253` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `unlink` | — |
