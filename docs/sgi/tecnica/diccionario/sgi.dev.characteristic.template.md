<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic.template`

**Característica por tipo de desarrollo** (Model).

Renglón que carga un tipo de desarrollo en la tabla de características del proyecto.

Orden: `dev_type, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`, `addons/quimibond_sgi/models/sgi_dev_pilot.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Los renglones archivados no se cargan en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:69` |
| `caracteristica_id` | Many2one | Característica | Característica del catálogo (Fichas Técnicas de Tela → Configuración) que se carga. | sí | `ficha.tecnica.caracteristica` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:50` |
| `critical` | Boolean | Crítica (estudio de habilidad) | Entra al estudio de habilidad del pilotaje. El brief lo deja por definir (Ingeniería de Calidad): nace apagada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:76` |
| `dev_type` | Selection | Tipo de desarrollo | Tipo de desarrollo cuya tabla incluye este renglón. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:47` |
| `direction` | Selection | Dirección | Largo o ancho cuando la característica se mide en las dos direcciones. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:56` |
| `in_coa` | Boolean | Va al certificado | El renglón aparece por omisión en el certificado de calidad del lote. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:67` |
| `in_customer_spec` | Boolean | Va a la especificación del cliente | El renglón aparece por omisión en las Especificaciones del producto que se entregan al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:64` |
| `kind` | Selection | Tipo de dato |  |  |  | related `caracteristica_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:54` |
| `lab_default` | Boolean | Se mide en la muestra del cliente | Al cargar el renglón queda marcado para que el laboratorio lo mida en la muestra del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:61` |
| `position` | Selection | Posición | Izquierda, centro o derecha cuando se mide en tres puntos (solidez al frote, masa por orillas). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:58` |
| `sequence` | Integer |  | Orden del renglón en la tabla del proyecto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:49` |
| `unit` | Char | Unidad |  |  |  | related `caracteristica_id.unit`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:55` |

