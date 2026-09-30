<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.line`

**Punto revisado en una hoja de mantenimiento** (Model).

Punto revisado en una hoja de checklist de mantenimiento (``maintenance.request``), con su respuesta y, si falla, la solicitud correctiva.

Orden: `request_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `answer` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:186` |
| `corrective_request_id` | Many2one | Correctivo |  |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:188` |
| `hint` | Char | Criterio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:185` |
| `name` | Char | Qué se revisa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:184` |
| `note` | Char | Observación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:187` |
| `request_id` | Many2one |  |  | sí | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:182` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:183` |

