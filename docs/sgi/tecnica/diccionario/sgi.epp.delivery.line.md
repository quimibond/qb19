<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.epp.delivery.line`

**Renglón de la responsiva de EPP** (Model).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_epp_sign.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `delivery_id` | Many2one |  |  | sí | `sgi.epp.delivery` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:25` |
| `name` | Char | Artículo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:29` |
| `note` | Char | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:31` |
| `quantity` | Float | Cantidad |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:27` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:26` |
| `size` | Char | Talla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:30` |
| `uom` | Selection | Unidad |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:28` |

