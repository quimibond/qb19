<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.area`

**Área documental SGI** (Model).

Orden: `code`.

Archivos: `addons/quimibond_sgi/models/sgi_area.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_area.py:13` |
| `code` | Char | Clave |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_area.py:10` |
| `department_id` | Many2one | Departamento |  |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_area.py:12` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_area.py:11` |

