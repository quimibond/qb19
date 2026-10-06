<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic`

**Característica del desarrollo de producto** (Model). Hereda de: `ficha.tecnica.caracteristica.mixin`.

Renglón de la tabla de características de un proyecto de desarrollo: lo que pide el cliente, lo medido en su muestra, el control interno, lo obtenido en la corrida y lo que aprobó.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `customer_approved` | Boolean | Aprobado por el cliente | El cliente aceptó este valor en la aprobación final. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:128` |
| `lab_requested` | Boolean | Medir en la muestra | Diseño de Producto pide al laboratorio medir este renglón en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:100` |
| `project_id` | Many2one |  | Proyecto de desarrollo al que pertenece el renglón. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:97` |
| `run_1` | Float | Lectura 1 | Primera lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:111` |
| `run_2` | Float | Lectura 2 | Segunda lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:112` |
| `run_3` | Float | Lectura 3 | Tercera lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:113` |
| `run_avg` | Float | Promedio | Promedio de las lecturas capturadas (calculado). |  |  | compute `_compute_run_avg`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:114` |
| `run_count` | Integer | Lecturas | Cuántas lecturas tiene la corrida. |  |  | compute `_compute_run_avg`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:116` |
| `run_result` | Selection | Resultado de la corrida | Cumple: dentro del control interno. Fuera del control interno: se puede embarcar con aviso a Calidad y a Diseño de Procesos. No conforme: fuera de lo que pide el cliente. |  |  | compute `_compute_results`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:120` |
| `run_text` | Char | Corrida (texto) | Lo observado en la corrida cuando la característica es cualitativa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:118` |
| `sample_result` | Selection | Muestra vs. especificación | Si lo medido en la muestra del cliente cae dentro de lo que él mismo pide (calculado). |  |  | compute `_compute_results`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:107` |
| `sample_text` | Char | Muestra (texto) | Lo observado en la muestra del cliente cuando la característica es cualitativa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:105` |
| `sample_value` | Float | Medido en la muestra | Valor que midió el laboratorio en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:103` |
| `verdict` | Selection | Dictamen | Lo decide Diseño de Producto, no el laboratorio: cumple, cumple con desviación o no cumple. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:124` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
