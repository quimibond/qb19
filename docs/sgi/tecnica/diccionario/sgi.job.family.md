<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.job.family`

**Familia de puestos SGI** (Model).

Familia de puestos: el mismo rol repartido en puestos que solo cambian por nivel o letra (Operador de tejido circular A…J). El nivel se queda en hr.job; el SGI asigna actividades a la familia.

Orden: `code`.

Archivos: `addons/quimibond_sgi/models/sgi_catalog.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:82` |
| `code` | Char | Código |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:74` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:79` |
| `employee_count` | Integer | Empleados activos |  |  |  | compute `_compute_employee_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_catalog.py:83` |
| `job_ids` | Many2many | Puestos |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:76` |
| `name` | Char | Familia |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_catalog.py:75` |

