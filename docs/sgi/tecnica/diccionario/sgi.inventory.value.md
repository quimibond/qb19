<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.inventory.value`

**Valor del inventario al cierre de mes (cálculo de AL-01)** (Model).

Orden: `date desc, company_id`.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one | Compañía |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:130` |
| `currency_id` | Many2one |  |  |  |  | related `company_id.currency_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:134` |
| `date` | Date | Cierre | Último día del mes al que corresponde la foto. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:131` |
| `quant_count` | Integer | # Existencias |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:135` |
| `taken_at` | Datetime | Tomada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:136` |
| `value` | Monetary | Valor del inventario |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:133` |

