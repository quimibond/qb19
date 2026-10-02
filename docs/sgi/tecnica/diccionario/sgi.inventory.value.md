<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.inventory.value`

**Valor del inventario al cierre de mes (cálculo de AL-01)** (Model).

Foto del valor del inventario al cierre de cada mes, base del indicador AL-01.

Orden: `date desc, company_id`.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one | Compañía |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:137` |
| `currency_id` | Many2one |  |  |  |  | related `company_id.currency_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:141` |
| `date` | Date | Cierre | Último día del mes al que corresponde la foto. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:138` |
| `quant_count` | Integer | # Existencias |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:142` |
| `taken_at` | Datetime | Tomada el | Fecha y hora en que se tomó la foto del inventario. |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:143` |
| `value` | Monetary | Valor del inventario |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:140` |

