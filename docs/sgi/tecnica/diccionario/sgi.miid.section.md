<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid.section`

**Sección del MIID** (Model). Hereda de: `mail.thread`.

Sección de texto fijo del MIID (una por título y subtítulo). La edita el Jefe MAST; los datos del sistema salen del bloque que declara. «Por confirmar» impide enviar o aprobar una revisión del MIID.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:138` |
| `body` | Html | Texto de la sección | Texto fijo que imprime el MIID. Escriba [[datos]] en un párrafo propio para decidir dónde van los datos del sistema; si no, van al final de la sección. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:119` |
| `body_fallback` | Boolean | Texto solo si no hay datos | Marque si el texto es el respaldo del bloque: solo se imprime cuando el sistema no trae datos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:126` |
| `clause` | Char | Numeral | Numeral del manual (p. ej. 4.4). Vacío en la identificación del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:113` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:109` |
| `heading_level` | Selection | Nivel del título | Capítulo («4 Contexto de la organización») o apartado («4.4 …»). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:116` |
| `live_block` | Selection | Datos del sistema que lleva | Datos vivos de Odoo que imprime esta sección. Cada bloque va en una sola sección; si ninguna lo lleva, sale al final del manual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:122` |
| `name` | Char | Título de la sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:115` |
| `row_note_ids` | One2many | Notas por renglón | Columna fija de un bloque de datos (12.1: la situación de cada anexo). |  | `sgi.miid.row.note` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:136` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:112` |
| `to_confirm` | Boolean | Por confirmar | Mientras alguna sección esté por confirmar, el MIID no se puede enviar ni aprobar como revisión vigente. Quítelo cuando el texto esté confirmado (Jefe MAST, Dirección o Administrador SGI). |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:129` |
| `to_confirm_note` | Text | Qué falta confirmar | Se ve en la pantalla y en el PDF de borrador. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:134` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
