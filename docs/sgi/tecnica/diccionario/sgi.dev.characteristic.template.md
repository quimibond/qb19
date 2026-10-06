<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic.template`

**Característica por tipo de desarrollo** (Model).

Renglón que carga un tipo de desarrollo en la tabla de características del proyecto.

Orden: `dev_type, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Los renglones archivados no se cargan en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:124` |
| `dev_type` | Selection | Tipo de desarrollo | Tipo de desarrollo cuya tabla incluye este renglón. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:103` |
| `direction` | Selection | Dirección | Largo o ancho cuando la característica se mide en las dos direcciones. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:111` |
| `in_coa` | Boolean | Va al certificado | El renglón aparece por omisión en el certificado de calidad del lote. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:122` |
| `in_customer_spec` | Boolean | Va a la especificación del cliente | El renglón aparece por omisión en las Especificaciones del producto que se entregan al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:119` |
| `kind` | Selection | Tipo de dato |  |  |  | related `type_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:109` |
| `lab_default` | Boolean | Se mide en la muestra del cliente | Al cargar el renglón queda marcado para que el laboratorio lo mida en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:116` |
| `position` | Selection | Posición | Izquierda, centro o derecha cuando se mide en tres puntos (solidez al frote, masa por orillas). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:113` |
| `sequence` | Integer |  | Orden del renglón en la tabla del proyecto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:105` |
| `type_id` | Many2one | Característica | Característica del catálogo que se carga. | sí | `sgi.dev.characteristic.type` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:106` |
| `unit` | Char | Unidad |  |  |  | related `type_id.unit`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:110` |

