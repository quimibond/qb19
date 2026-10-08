<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.tech.sheet.line`

**Característica de la ficha técnica interna** (Model). Hereda de: `ficha.tecnica.caracteristica.mixin`.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `name_en` | Char | Characteristic |  |  |  | related `caracteristica_id.name_en`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:241` |
| `sheet_id` | Many2one | Ficha |  | sí | `sgi.dev.tech.sheet` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:240` |

