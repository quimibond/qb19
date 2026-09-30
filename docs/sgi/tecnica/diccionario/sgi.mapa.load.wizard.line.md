<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.mapa.load.wizard.line`

**Resultado de la carga del mapa SGI** (TransientModel).

Renglón del resultado de la carga del mapa.

Orden: `id`.

Archivos: `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:189` |
| `key` | Char | Clave |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:188` |
| `kind` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:187` |
| `message` | Char | Detalle |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:190` |
| `wizard_id` | Many2one |  |  | sí | `sgi.mapa.load.wizard` |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:186` |

