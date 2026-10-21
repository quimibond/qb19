<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.document.type`

**Tipo de documento SGI** (Model).

Tipo de documento controlado. Los prefijos de clave viven aquí como datos: agregar un tipo o cambiar su nomenclatura no requiere programar.

Orden: `sequence, code`.

Archivos: `addons/quimibond_sgi/models/sgi_catalog.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:681` |
| `approver_text` | Char | Lo aprueba | Quién firma «Aprobó» (P-G01, 4.3). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:708` |
| `author_text` | Char | Lo elabora | Quién elabora un documento de este tipo (P-G01, 4.2). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:704` |
| `code` | Char | Código | Identificador estable (procedimiento, instructivo, formato…). El campo heredado «Tipo de documento» se calcula desde él. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:675` |
| `code_required` | Boolean | Exige clave | Apáguelo para tipos sin clave propia (documentos externos, formularios de Odoo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:691` |
| `company_id` | Many2one | Empresa | Vacío = tipo compartido por todas las empresas. |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:699` |
| `control_text` | Char | Cómo queda controlado | Cómo se publica y se controla (P-G01, 4.2). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:706` |
| `legacy_code_regex` | Char | Claves heredadas aceptadas (regex) | Expresión regular de la nomenclatura anterior que se sigue aceptando mientras se migra (ej. ^P-[AGCDEIMPSV]\d{2}$). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:695` |
| `location_text` | Char | Dónde se elabora | Dónde se escribe o se arma (P-G01, 4.2). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:705` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:679` |
| `odoo_menu_id` | Many2one | Pantalla en Odoo | Dónde se elabora o se consulta en Odoo. |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:709` |
| `prefix_pattern` | Char | Patrón de clave | Cómo se arma la clave. Marcadores: {process} = clave del proceso (C6, S2…), {seq} o {seq:02d} = consecutivo. Ej. PR-{process}, IT-{process}-{seq:02d}, CO-{seq:02d}. Vacío = clave libre. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:682` |
| `requires_process` | Boolean | Exige proceso | Un documento controlado de este tipo debe estar ligado a un proceso (salvo que conserve una clave heredada). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:687` |
| `reviewer_text` | Char | Lo revisa | Quién firma «Revisó» (P-G01, 4.3). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:707` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:680` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `sgi_next_code` | Siguiente clave libre del tipo para el proceso: arma el patrón con el consecutivo más alto usado + 1. |
