<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.pilot.study`

**Estudio de habilidad por característica** (Model).

Orden: `pilot_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_pilot.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic_id` | Many2one | Característica |  | sí | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:317` |
| `cp` | Float | Cp |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:324` |
| `cp_defined` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:325` |
| `cpk` | Float | Cpk |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:326` |
| `cpk_defined` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:327` |
| `lots` | Integer | Lotes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:320` |
| `mean` | Float | Media |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:322` |
| `n` | Integer | Lecturas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:321` |
| `pilot_id` | Many2one | Pilotaje |  | sí | `sgi.dev.pilot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:316` |
| `sigma` | Float | Sigma (n−1) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:323` |
| `spec_label` | Char | Especificación del cliente |  |  |  | related `characteristic_id.spec_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:319` |
| `unit` | Char |  |  |  |  | related `characteristic_id.unit`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:318` |
| `verdict` | Selection | Dictamen | Vacío si el Cpk mínimo no está definido o no se pudo calcular. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:328` |

