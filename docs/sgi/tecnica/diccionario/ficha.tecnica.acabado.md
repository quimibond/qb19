<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `ficha.tecnica.acabado`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_has_tintoreria` | Boolean | Lleva tintorería | La lista de materiales del acabado tiene operación de tintorería o fórmula de teñido (pestaña Tintorería). |  |  | compute `_compute_sgi_has_tintoreria`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:246` |
| `sgi_reading_ids` | One2many | Valores de la corrida |  |  | `sgi.dev.process.reading` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:243` |
| `sgi_tint_validated_by_id` | Many2one | Validó la tintorería (Supervisor Tintorería) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:249` |
| `sgi_tint_validated_date` | Datetime | Tintorería validada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:251` |

