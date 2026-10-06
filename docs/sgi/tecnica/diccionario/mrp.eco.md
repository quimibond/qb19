<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mrp.eco`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi_plm/models/mrp_eco.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_control_plan_ids` | Many2many | Planes de control impactados |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:37` |
| `sgi_customer_id` | Many2one | Cliente del PPAP |  |  | `res.partner` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:33` |
| `sgi_customer_notice` | Boolean | Requiere aviso al cliente | El cambio afecta a partes ya aprobadas: notificar al cliente antes de aplicar. |  |  |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:30` |
| `sgi_fmea_ids` | Many2many | AMEF impactados |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:36` |
| `sgi_ppap_auto` | Boolean | Marcado por el SGI | «Requiere PPAP» lo marcó el SGI por los clientes del producto. |  |  |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:44` |
| `sgi_ppap_count` | Integer | PPAP |  |  |  | compute `_compute_sgi_ppap_count`, sin guardar |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:51` |
| `sgi_ppap_customer_ids` | Many2many | Clientes que exigen PPAP | Clientes marcados «Exige PPAP ante cambios» que compraron el producto en los últimos meses o ya tienen un PPAP de él. |  | `res.partner` | compute `_compute_sgi_ppap_customer_ids`, sin guardar |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:39` |
| `sgi_ppap_id` | Many2one | PPAP generado |  |  | `sgi.ppap` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:35` |
| `sgi_ppap_ids` | Many2many | PPAP generados | Un PPAP por cliente, generados al aplicar el cambio. |  | `sgi.ppap` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:47` |
| `sgi_requires_ppap` | Boolean | Requiere PPAP | Si está marcado, al aplicar el cambio se generará un expediente PPAP. El SGI lo marca solo cuando el producto se vende a clientes que exigen PPAP ante cambios. |  |  |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:26` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | — |
| `action_view_sgi_ppap` | — |
| `create` | — |
| `write` | — |
