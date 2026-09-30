<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.loto.lock`

**Candado y tarjeta de un trabajador** (Model).

Orden: `loto_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_loto.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_placed` | Datetime | Puesto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:195` |
| `date_removed` | Datetime | Retirado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:196` |
| `employee_id` | Many2one | Trabajador |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:192` |
| `lock_number` | Char | Candado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:193` |
| `loto_id` | Many2one | Bloqueo |  | sí | `sgi.loto` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:190` |
| `removed_by_id` | Many2one | Retiró |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:197` |
| `state` | Selection |  |  |  |  | related `loto_id.state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_loto.py:198` |
| `tag_number` | Char | Tarjeta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:194` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_remove_lock` | Cada trabajador retira su propio candado (P-A20). El Jefe MAST puede retirarlo por él, y queda anotado quién lo hizo. |
| `create` | — |
| `unlink` | — |
| `write` | — |
