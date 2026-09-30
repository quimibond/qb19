<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.catalog.load.wizard.line`

**Resultado de la carga del catálogo SGI** (TransientModel).

Orden: `id`.

Archivos: `addons/quimibond_sgi/models/sgi_load_wizard.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:111` |
| `key` | Char | Clave |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:110` |
| `kind` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:109` |
| `message` | Char | Detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:112` |
| `wizard_id` | Many2one |  |  | sí | `sgi.catalog.load.wizard` |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:108` |

