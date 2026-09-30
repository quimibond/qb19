<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.ppap.element`

**Elemento de un PPAP** (Model).

Orden: `ppap_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_ppap.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `control_plan_id` | Many2one | Plan de control |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:258` |
| `document_id` | Many2one | Documento |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:259` |
| `fmea_id` | Many2one | AMEF |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:257` |
| `name` | Char | Elemento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:243` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:260` |
| `ppap_id` | Many2one | PPAP |  | sí | `sgi.ppap` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:240` |
| `sequence` | Integer | N° |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:242` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:244` |
| `submission` | Selection | S/R | Según el nivel AIAG del expediente: S se presenta al cliente y bloquea el envío si está pendiente; R se retiene en planta a disposición. Editable por elemento (p. ej. nivel 4). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:250` |
| `template_id` | Many2one | Elemento (catálogo) |  |  | `sgi.ppap.element.template` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:241` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `unlink` | — |
