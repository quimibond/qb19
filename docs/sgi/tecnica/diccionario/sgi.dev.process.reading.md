<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.reading`

**Valor real de un parámetro de la ficha de proceso** (Model).

Valor real de un parámetro de la ficha en la corrida del desarrollo, con su número de ajuste y motivo. Nominal, tolerancia y unidad se leen de la ficha.

Orden: `section, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acabado_id` | Many2one | Ficha de acabado |  |  | `ficha.tecnica.acabado` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:216` |
| `adjustment` | Integer | Número de ajuste | Cuántas veces se ajustó el parámetro en la corrida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:228` |
| `captured` | Boolean | Capturado | Se capturó el valor real (un 0 capturado cuenta). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:226` |
| `name` | Char | Parámetro |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:220` |
| `nominal` | Char | Nominal |  |  |  | compute `_compute_spec`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:221` |
| `param_key` | Char | Campo de la ficha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:219` |
| `real` | Float | Real (corrida) | Lo que se corrió de verdad; lo valida el Jefe de Manufactura. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:224` |
| `reason_id` | Many2one | Motivo del ajuste |  |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:229` |
| `result` | Selection | Resultado |  |  |  | compute `_compute_spec`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:230` |
| `section` | Selection | Sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:218` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:217` |
| `tejido_id` | Many2one | Ficha de tejido |  |  | `ficha.tecnica.tejido` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:215` |
| `tolerance` | Char | Tolerancia |  |  |  | compute `_compute_spec`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:222` |
| `unit` | Char | Unidad |  |  |  | compute `_compute_spec`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:223` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
