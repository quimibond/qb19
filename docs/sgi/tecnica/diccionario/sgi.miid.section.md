<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid.section`

**Sección del MIID** (Model). Hereda de: `mail.thread`.

Sección de texto fijo del MIID (una por título y subtítulo). La edita el Jefe MAST; los datos del sistema salen del bloque que declara. «Por confirmar» impide enviar o aprobar una revisión del MIID.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:218` |
| `body` | Html | Texto de la sección | Texto fijo que imprime el MIID. Escriba [[datos]] en un párrafo propio para decidir dónde van los datos del sistema; si no, van al final de la sección. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:199` |
| `body_fallback` | Boolean | Texto solo si no hay datos | Marque si el texto es el respaldo del bloque: solo se imprime cuando el sistema no trae datos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:206` |
| `clause` | Char | Numeral | Numeral del manual (p. ej. 4.4). Vacío en la identificación del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:193` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:184` |
| `heading_level` | Selection | Nivel del título | Capítulo («4 Contexto de la organización») o apartado («4.4 …»). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:196` |
| `kind` | Selection | Documento | Documento generado al que pertenece la sección: el Manual del SGI (MIID) o el procedimiento de control de información documentada (P-G01). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:187` |
| `live_block` | Selection | Datos del sistema que lleva | Datos vivos de Odoo que imprime esta sección. Cada bloque va en una sola sección; si ninguna lo lleva, sale al final del manual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:202` |
| `name` | Char | Título de la sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:195` |
| `row_note_ids` | One2many | Notas por renglón | Columna fija de un bloque de datos (12.1: la situación de cada anexo). |  | `sgi.miid.row.note` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:216` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:192` |
| `to_confirm` | Boolean | Por confirmar | Mientras alguna sección esté por confirmar, el MIID no se puede enviar ni aprobar como revisión vigente. Quítelo cuando el texto esté confirmado (Jefe MAST, Dirección o Administrador SGI). |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:209` |
| `to_confirm_note` | Text | Qué falta confirmar | Se ve en la pantalla y en el PDF de borrador. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:214` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
