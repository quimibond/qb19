<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.document.type`

**Tipo de documento SGI** (Model).

Tipo de documento controlado. Los prefijos de clave viven aquí como datos: agregar un tipo o cambiar su nomenclatura no requiere programar.

Orden: `sequence, code`.

Archivos: `addons/quimibond_sgi/models/sgi_catalog.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:638` |
| `code` | Char | Código | Identificador estable (procedimiento, instructivo, formato…). El campo heredado «Tipo de documento» se calcula desde él. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:632` |
| `code_required` | Boolean | Exige clave | Apáguelo para tipos sin clave propia (documentos externos, formularios de Odoo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:648` |
| `company_id` | Many2one | Empresa | Vacío = tipo compartido por todas las empresas. |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:656` |
| `legacy_code_regex` | Char | Claves heredadas aceptadas (regex) | Expresión regular de la nomenclatura anterior que se sigue aceptando mientras se migra (ej. ^P-[AGCDEIMPSV]\d{2}$). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:652` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:636` |
| `prefix_pattern` | Char | Patrón de clave | Cómo se arma la clave. Marcadores: {process} = clave del proceso (C6, S2…), {seq} o {seq:02d} = consecutivo. Ej. PR-{process}, IT-{process}-{seq:02d}, CO-{seq:02d}. Vacío = clave libre. |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:639` |
| `requires_process` | Boolean | Exige proceso | Un documento controlado de este tipo debe estar ligado a un proceso (salvo que conserve una clave heredada). |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:644` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:637` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `sgi_next_code` | Siguiente clave libre del tipo para el proceso: arma el patrón con el consecutivo más alto usado + 1. |
