<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.source`

**Fuente de datos de indicadores** (Model).

Fuente de datos con nombre de negocio para las fórmulas de indicadores.

Orden: `sequence, name`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator_wizard.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:59` |
| `amount_field` | Char | Monto o cantidad por omisión | Campo que se suma en «Cuánto suman». |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:65` |
| `available` | Boolean | Instalada |  |  |  | compute `_compute_available`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:67` |
| `date_field` | Char | Fecha que cuenta |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:64` |
| `description` | Char | Qué cuenta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:69` |
| `domain` | Char | Filtro base | Qué registros son de esta fuente. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:62` |
| `model_name` | Char | Modelo técnico | account.move, stock.picking… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:60` |
| `name` | Char | Fuente | Nombre de negocio: «Facturas de cliente publicadas». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:56` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:58` |

