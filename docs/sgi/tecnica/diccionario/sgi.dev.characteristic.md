<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic`

**Característica pedida en la solicitud de desarrollo** (Model).

Característica pedida en una solicitud de desarrollo de producto (valor, tolerancia, método).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_request.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `direction` | Selection | Dirección |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:153` |
| `method` | Char | Método / norma |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:157` |
| `name` | Char | Característica |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:152` |
| `note` | Char | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:158` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:150` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:151` |
| `tolerance` | Char | Tolerancia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:156` |
| `unit` | Char | Unidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:154` |
| `value` | Char | Valor pedido |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:155` |

