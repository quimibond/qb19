<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid.item`

**Renglón de una sección del MIID** (Model).

Renglón de una sección del MIID o del P-G01 (57.161.0): un concepto con su descripción y, si aplica, su numeral, columnas extra y el menú donde se ve en Odoo. Sustituye las listas y tablas que se escribían en el texto. Lo edita el Jefe MAS…

Orden: `section_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `clause` | Char | Numeral | Cláusulas de la norma que cubre (p. ej. 6.1, 7.5). |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:719` |
| `company_id` | Many2one |  |  |  |  | related `section_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_miid.py:713` |
| `kind` | Selection |  |  |  |  | related `section_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_miid.py:714` |
| `label` | Char | Concepto | Lo que se define o se regula (p. ej. «Riesgos y oportunidades», «Jefe de MAST y SGI»). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:716` |
| `menu_id` | Many2one | Pantalla en Odoo | Menú donde se hace o se consulta. La pantalla lo abre; el PDF imprime su ruta. |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:723` |
| `section_id` | Many2one | Sección |  | sí | `sgi.miid.section` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:711` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:715` |
| `text` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:720` |
| `text_2` | Text | Texto 2 | Segunda columna de la tabla, si la sección le pone título. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:721` |
| `text_3` | Text | Texto 3 | Tercera columna de la tabla, si la sección le pone título. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:722` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_menu` | — |
| `create` | — |
| `unlink` | — |
| `write` | — |
