<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mrp.eco`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi_plm/models/mrp_eco.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_control_plan_ids` | Many2many | Planes de control impactados |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:18` |
| `sgi_customer_id` | Many2one | Cliente del PPAP |  |  | `res.partner` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:14` |
| `sgi_customer_notice` | Boolean | Requiere aviso al cliente | El cambio afecta a partes ya aprobadas: notificar al cliente antes de aplicar. |  |  |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:11` |
| `sgi_fmea_ids` | Many2many | AMEF impactados |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:17` |
| `sgi_ppap_id` | Many2one | PPAP generado |  |  | `sgi.ppap` |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:16` |
| `sgi_requires_ppap` | Boolean | Requiere PPAP | Si está marcado, al aplicar el cambio se generará un expediente PPAP. |  |  |  |  | `addons/quimibond_sgi_plm/models/mrp_eco.py:8` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | — |
| `action_view_sgi_ppap` | — |
