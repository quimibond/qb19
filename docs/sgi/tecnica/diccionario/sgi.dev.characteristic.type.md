<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.characteristic.type`

**Característica de desarrollo (catálogo)** (Model).

Característica del catálogo de desarrollo de producto (unidad, método y tipo de dato).

Orden: `sequence, name, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Las características archivadas no se proponen en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:84` |
| `code` | Char | Clave | Clave corta y estable de la característica (p. ej. «masa», «ancho»). Con ella se reconocen los renglones al pasar del proyecto a la ficha del artículo y al calcular el rendimiento. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:66` |
| `formula` | Selection | Cálculo | Si la característica se calcula a partir de otras del mismo proyecto. El rendimiento (m/kg) sale de la masa (g/m²) y el ancho (m). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:80` |
| `kind` | Selection | Tipo de dato | Numérica: se captura valor y tolerancia y se compara. Cualitativa: solo texto (tacto, color). Sí / no: casilla. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:74` |
| `method` | Char | Método / norma | Método de prueba o norma con la que se mide (MT001 · NMX-A-3801-INNTEX-2012…). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:78` |
| `name` | Char | Característica | Nombre con el que se ve en la tabla del proyecto y en los PDF. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:70` |
| `name_en` | Char | Nombre en inglés | Nombre para los documentos bilingües al cliente (Especificaciones del producto). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:72` |
| `sequence` | Integer |  | Orden en el que aparece en los catálogos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:65` |
| `template_ids` | One2many | En tipos de desarrollo |  |  | `sgi.dev.characteristic.template` |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:85` |
| `unit` | Char | Unidad | Unidad de medida (g/m², m, %, N/5 cm…). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:77` |

