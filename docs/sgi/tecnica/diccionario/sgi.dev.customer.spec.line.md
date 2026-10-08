<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.customer.spec.line`

**Característica de las especificaciones del producto** (Model). Hereda de: `ficha.tecnica.caracteristica.mixin`.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `name_en` | Char | Characteristic |  |  |  | related `caracteristica_id.name_en`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:308` |
| `spec_id` | Many2one | Especificaciones |  | sí | `sgi.dev.customer.spec` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:306` |

