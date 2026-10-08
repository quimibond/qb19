<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.pilot.reading`

**Lectura de laboratorio en un lote de pilotaje** (Model).

Orden: `pilot_id, lot_id, characteristic_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_pilot.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic_id` | Many2one | Característica crítica |  | sí | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:294` |
| `lot_id` | Many2one | Lote |  | sí | `sgi.dev.pilot.lot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:292` |
| `pilot_id` | Many2one | Pilotaje |  | sí | `sgi.dev.pilot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:291` |
| `sequence` | Integer | Lectura |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:299` |
| `spec_label` | Char | Especificación del cliente |  |  |  | related `characteristic_id.spec_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:298` |
| `unit` | Char |  |  |  |  | related `characteristic_id.unit`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:297` |
| `value` | Float | Valor |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:300` |
| `within_spec` | Boolean | Dentro de la especificación |  |  |  | compute `_compute_within`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:301` |

