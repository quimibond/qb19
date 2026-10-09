<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.pilot.lot`

**Lote del pilotaje** (Model).

Orden: `pilot_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_pilot.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date` | Datetime | Terminado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:254` |
| `lot_id` | Many2one | Lote |  |  | `stock.lot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:253` |
| `pilot_id` | Many2one | Pilotaje |  | sí | `sgi.dev.pilot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:250` |
| `production_id` | Many2one | Orden de producción |  |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:252` |
| `qty_produced` | Float | Cantidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:255` |
| `reading_count` | Integer |  |  |  |  | compute `_compute_reading_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:259` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:251` |
| `verdict` | Selection | Dictamen | Conforme: todas las características críticas con lecturas dentro de la especificación del cliente. No conforme: no cuenta para el pilotaje. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:256` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_evaluate` | Dictamen del lote con sus lecturas: la media de cada característica crítica dentro de la especificación del cliente. Sin lecturas sigue pendiente. |
