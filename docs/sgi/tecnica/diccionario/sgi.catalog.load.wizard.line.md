<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.catalog.load.wizard.line`

**Resultado de la carga del catálogo SGI** (TransientModel).

Renglón del resultado de la carga del catálogo (qué se crea, cambia, archiva o falla).

Orden: `id`.

Archivos: `addons/quimibond_sgi/models/sgi_load_wizard.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:114` |
| `key` | Char | Clave |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:113` |
| `kind` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:112` |
| `message` | Char | Detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:115` |
| `wizard_id` | Many2one |  |  | sí | `sgi.catalog.load.wizard` |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:111` |

