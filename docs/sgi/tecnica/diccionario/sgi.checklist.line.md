<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.line`

**Punto revisado en una hoja de mantenimiento** (Model).

Punto revisado en una hoja de checklist de mantenimiento (``maintenance.request``), con su respuesta y, si falla, la solicitud correctiva.

Orden: `request_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `answer` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:195` |
| `corrective_request_id` | Many2one | Correctivo |  |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:197` |
| `hint` | Char | Criterio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:194` |
| `name` | Char | Qué se revisa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:193` |
| `note` | Char | Observación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:196` |
| `request_id` | Many2one |  |  | sí | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:191` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:192` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_answer` | I-03: un toque por punto desde la tarjeta (Bien, Falla o No aplica). La respuesta viene en el contexto del botón y se valida aquí. |
| `create` | — |
| `write` | — |
