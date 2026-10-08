<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.dye.step`

**Tramo de la gráfica de tintorería** (Model).

Tramo de la gráfica de proceso de tintorería: de una temperatura a otra con un gradiente y un sostenimiento.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `gradient` | Float | Gradiente (°C/min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:393` |
| `hold_min` | Float | Sostener (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:394` |
| `minutes` | Float | Minutos del tramo |  |  |  | compute `_compute_minutes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:395` |
| `name` | Char | Tramo | Por ejemplo: calentamiento, agotamiento, enfriamiento. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:390` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:389` |
| `sheet_id` | Many2one |  |  | sí | `sgi.dev.process.sheet` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:388` |
| `temp_end` | Float | Temperatura final (°C) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:392` |
| `temp_start` | Float | Temperatura inicial (°C) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:391` |

