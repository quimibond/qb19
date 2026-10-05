<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.company.fix`

**Empresa del SGI en documentos controlados** (TransientModel).

Empresa del SGI en los documentos controlados que no tienen empresa (D-06 de datos).

Archivos: `addons/quimibond_sgi/models/sgi_company_fix.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one | Empresa del SGI |  |  | `res.company` | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:28` |
| `doc_count` | Integer | Documentos controlados sin empresa |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:30` |
| `done_count` | Integer | Documentos corregidos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_company_fix.py:36` |
| `failed_count` | Integer | Documentos que no se pudieron corregir | Se quedaron sin empresa; el motivo de cada uno está en el log del servidor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_company_fix.py:37` |
| `obsoleto_count` | Integer | Obsoletos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:32` |
| `other_count` | Integer | En borrador o piloto |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:33` |
| `routine_count` | Integer | Rutinas del procedimiento anterior que toman la empresa |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:34` |
| `vigente_count` | Integer | Vigentes |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_company_fix.py:31` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | — |
| `action_view_docs` | — |
| `create` | — |
