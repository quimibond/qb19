<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic`

**Característica del desarrollo de producto** (Model).

Renglón de la tabla de características de un proyecto de desarrollo: lo que pide el cliente, lo medido en su muestra, el control interno, lo obtenido en la corrida y lo que aprobó.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`.

## Campos (42)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ctrl_defined` | Boolean | Con control interno | El renglón tiene un margen interno propio. |  |  | compute `_compute_limits`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:265` |
| `ctrl_label` | Char | Control interno | El margen de control interno en una sola expresión. Nunca va al cliente. |  |  | compute `_compute_labels`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:267` |
| `ctrl_max` | Float | Máx. interno | Límite superior del control interno (calculado). |  |  | compute `_compute_limits`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:263` |
| `ctrl_min` | Float | Mín. interno | Límite inferior del control interno (calculado). |  |  | compute `_compute_limits`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:261` |
| `ctrl_tol_minus` | Float | Control − | Margen interno por debajo del nominal, más cerrado que el del cliente. Vacío: se usa el del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:256` |
| `ctrl_tol_plus` | Float | Control + | Margen interno por encima del nominal, más cerrado que el del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:259` |
| `customer_approved` | Boolean | Aprobado por el cliente | El cliente aceptó este valor en la aprobación final. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:298` |
| `direction` | Selection | Dirección | Largo o ancho cuando se mide en las dos direcciones. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:227` |
| `in_coa` | Boolean | En certificado | El renglón aparece en el certificado de calidad del lote (siempre contra la especificación del cliente, nunca el control interno). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:303` |
| `in_customer_spec` | Boolean | En especificación del cliente | El renglón aparece en las Especificaciones del producto que se entregan al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:300` |
| `kind` | Selection | Tipo de dato | Numérica (valor y tolerancia), cualitativa (texto) o sí / no. | sí |  | compute `_compute_from_type`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:220` |
| `lab_requested` | Boolean | Medir en la muestra | Diseño de Producto pide al laboratorio medir este renglón en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:270` |
| `method` | Char | Método / norma | Método de prueba o norma con la que se mide. |  |  | compute `_compute_from_type`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:225` |
| `name` | Char | Característica | Nombre de la característica (sale del catálogo; se puede ajustar). | sí |  | compute `_compute_from_type`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:217` |
| `note` | Char | Observaciones | Texto libre; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:306` |
| `position` | Selection | Posición | Izquierda, centro o derecha cuando se mide en tres puntos. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:229` |
| `project_id` | Many2one |  | Proyecto de desarrollo al que pertenece el renglón. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:210` |
| `run_1` | Float | Lectura 1 | Primera lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:281` |
| `run_2` | Float | Lectura 2 | Segunda lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:282` |
| `run_3` | Float | Lectura 3 | Tercera lectura de la corrida de muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:283` |
| `run_avg` | Float | Promedio | Promedio de las lecturas capturadas (calculado). |  |  | compute `_compute_run_avg`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:284` |
| `run_count` | Integer | Lecturas | Cuántas lecturas tiene la corrida. |  |  | compute `_compute_run_avg`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:286` |
| `run_result` | Selection | Resultado de la corrida | Cumple: dentro del control interno. Fuera del control interno: se puede embarcar con aviso a Calidad y a Diseño de Procesos. No conforme: fuera de lo que pide el cliente. |  |  | compute `_compute_results`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:290` |
| `run_text` | Char | Corrida (texto) | Lo observado en la corrida cuando la característica es cualitativa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:288` |
| `sample_result` | Selection | Muestra vs. especificación | Si lo medido en la muestra del cliente cae dentro de lo que él mismo pide (calculado). |  |  | compute `_compute_results`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:277` |
| `sample_text` | Char | Muestra (texto) | Lo observado en la muestra del cliente cuando la característica es cualitativa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:275` |
| `sample_value` | Float | Medido en la muestra | Valor que midió el laboratorio en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:273` |
| `sequence` | Integer |  | Orden del renglón en la tabla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:212` |
| `spec_bool` | Boolean | Sí / no pedido | Solo para características de sí / no (engomado de orillas, corte de orillas). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:251` |
| `spec_label` | Char | Especificación | La especificación del cliente en una sola expresión, para pantalla y PDF. |  |  | compute `_compute_labels`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:253` |
| `spec_limit` | Selection | Límite | Cómo se lee la especificación: nominal con tolerancia, un máximo que no se rebasa o un mínimo que se alcanza. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:234` |
| `spec_max` | Float | Máx. cliente | Límite superior que acepta el cliente (calculado). |  |  | compute `_compute_limits`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:246` |
| `spec_min` | Float | Mín. cliente | Límite inferior que acepta el cliente (calculado). |  |  | compute `_compute_limits`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:244` |
| `spec_nominal` | Float | Nominal | Valor que pide el cliente. Con límite «Máximo» o «Mínimo» es el tope. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:232` |
| `spec_text` | Char | Especificación (texto) | Solo para características cualitativas: lo que pide el cliente en palabras (tacto suave, color natural). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:248` |
| `spec_tol_minus` | Float | Tol. − | Cuánto puede quedar por debajo del nominal (en la unidad o en %, según la casilla). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:237` |
| `spec_tol_pct` | Boolean | Tolerancia en % | Las tolerancias son porcentaje del nominal, no unidades. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:242` |
| `spec_tol_plus` | Float | Tol. + | Cuánto puede quedar por encima del nominal (en la unidad o en %). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:240` |
| `type_code` | Char | Clave |  |  |  | related `type_id.code`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:216` |
| `type_id` | Many2one | Catálogo | Característica del catálogo. Al elegirla se llenan nombre, unidad, método y tipo de dato; un renglón sin catálogo se escribe libre. |  | `sgi.dev.characteristic.type` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:213` |
| `unit` | Char | Unidad | Unidad de medida del renglón. |  |  | compute `_compute_from_type`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:223` |
| `verdict` | Selection | Dictamen | Lo decide Diseño de Producto, no el laboratorio: cumple, cumple con desviación o no cumple. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:294` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
