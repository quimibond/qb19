<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.template.item`

**Punto a revisar de una plantilla de checklist** (Model).

Punto a revisar de una plantilla de checklist.

Orden: `template_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `hint` | Char | Cómo / criterio | Ej. «Presión entre 6 y 8 bar», «Llantas sin cortes». |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:166` |
| `name` | Char | Qué se revisa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:165` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:164` |
| `template_id` | Many2one |  |  | sí | `sgi.checklist.template` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:163` |

