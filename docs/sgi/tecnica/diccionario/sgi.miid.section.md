<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid.section`

**Sección del MIID** (Model). Hereda de: `mail.thread`.

Sección de texto fijo del MIID (una por título y subtítulo). La edita el Jefe MAST; los datos del sistema salen del bloque que declara. «Por confirmar» impide enviar o aprobar una revisión del MIID.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (23)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:274` |
| `body` | Html | Texto de la sección | Texto fijo que imprime el MIID. Escriba [[datos]] en un párrafo propio para decidir dónde van los datos del sistema; si no, van al final de la sección. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:255` |
| `body_fallback` | Boolean | Texto solo si no hay datos | Marque si el texto es el respaldo del bloque: solo se imprime cuando el sistema no trae datos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:262` |
| `clause` | Char | Numeral | Numeral del manual (p. ej. 4.4). Vacío en la identificación del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:249` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:240` |
| `heading_level` | Selection | Nivel del título | Capítulo («4 Contexto de la organización») o apartado («4.4 …»). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:252` |
| `item_clause_title` | Char | Columna del numeral | Vacío: la tabla no muestra el numeral. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:293` |
| `item_count` | Integer | # Renglones |  |  |  | compute `_compute_item_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:287` |
| `item_ids` | One2many | Renglones | Lista o tabla de la sección: un renglón por concepto. Lo que antes eran viñetas o tablas escritas en el texto; cada renglón puede abrir su pantalla en Odoo. |  | `sgi.miid.item` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:283` |
| `item_label_title` | Char | Columna del concepto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:292` |
| `item_style` | Selection | Los renglones van en | Tabla: una columna por cada título que tenga nombre. Lista: el concepto en negritas y su descripción. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:288` |
| `item_text2_title` | Char | Segunda columna de texto | Vacío: la tabla no la muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:296` |
| `item_text3_title` | Char | Tercera columna de texto | Vacío: la tabla no la muestra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:298` |
| `item_text_title` | Char | Columna de la descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:295` |
| `kind` | Selection | Documento | Documento generado al que pertenece la sección: el Manual del SGI (MIID) o el procedimiento de control de información documentada (P-G01). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:243` |
| `live_block` | Selection | Datos del sistema que lleva | Datos vivos de Odoo que imprime esta sección. Cada bloque va en una sola sección; si ninguna lo lleva, sale al final del manual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:258` |
| `menu_id` | Many2one | Pantalla en Odoo | Menú donde se consulta en Odoo lo que dice la sección. La pantalla lo abre con un botón; el PDF imprime su ruta. |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:280` |
| `name` | Char | Título de la sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:251` |
| `page` | Selection | Pestaña | Pestaña de la pantalla donde se ve la sección; sale del numeral (el capítulo). |  |  | compute `_compute_page`, guardado |  | `addons/quimibond_sgi/models/sgi_miid.py:277` |
| `row_note_ids` | One2many | Notas por renglón | Columna fija de un bloque de datos (12.1: la situación de cada anexo). |  | `sgi.miid.row.note` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:272` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:248` |
| `to_confirm` | Boolean | Por confirmar | Mientras alguna sección esté por confirmar, el MIID no se puede enviar ni aprobar como revisión vigente. Quítelo cuando el texto esté confirmado (Jefe MAST, Dirección o Administrador SGI). |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:265` |
| `to_confirm_note` | Text | Qué falta confirmar | Se ve en la pantalla y en el PDF de borrador. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:270` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_form` | — |
| `action_open_menu` | — |
| `action_sgi_confirm` | 57.164.0: quita «Por confirmar» desde la pantalla del manual (Jefe MAST, Dirección o Administrador SGI; queda en el historial). |
| `write` | — |
