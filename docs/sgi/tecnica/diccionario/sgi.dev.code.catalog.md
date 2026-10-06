<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.code.catalog`

**Clave de codificación de artículos** (Model).

Clave de la regla de codificación de artículos de tejido y acabado (DAT P-D02-01).

Orden: `kind, sequence, code, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Las claves archivadas no se proponen al generar códigos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:164` |
| `code` | Char | Clave | Letra o letras que van en el código del artículo (W, J, Q, NT, AF…). Para la galga, las dos cifras más bajas de su rango (01, 11, 21…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:153` |
| `gauge` | Integer | Galga | Solo para la galga: el número de galga (14, 16, 18…) que se codifica con el rango. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:158` |
| `kind` | Selection | Parte del código | Posición del código de 16 caracteres que ocupa la clave. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:150` |
| `name` | Char | Descripción | Qué significa la clave (poliéster 100 %, jersey, hilo natural, natural, afelpado…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:156` |
| `range_from` | Integer | Rango desde | Solo para la galga: primer número del rango de dos cifras (01 para galga 14). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:160` |
| `range_to` | Integer | Rango hasta | Solo para la galga: último número del rango (10 para galga 14). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:162` |
| `sequence` | Integer |  | Orden dentro de su parte del código. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_characteristic.py:152` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `gauge_code` | '21' para la galga 18: las dos cifras más bajas del rango de esa galga (posiciones 7 y 8). |
| `gauge_from_code` | La galga (18) de las posiciones 7 y 8 del código ('21'…'30'). |
