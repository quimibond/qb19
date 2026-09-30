<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process.responsibility`

**Responsabilidad de área en el procedimiento** (Model).

Responsabilidad de un rol/puesto dentro del procedimiento (sección 3).

Orden: `process_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_process_procedure.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one | Empresa |  |  |  | related `process_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:421` |
| `job_id` | Many2one | Puesto | Puesto de hr.job al que corresponde el rol. | sí | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:424` |
| `name` | Char | Rol en el procedimiento | Nombre del rol tal como aparece en el procedimiento (no siempre mapea 1:1 al puesto de hr.job). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:427` |
| `process_id` | Many2one | Proceso |  | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:417` |
| `responsibilities` | Text | Responsabilidades |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:431` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:420` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
