<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.loto`

**Bloqueo y etiquetado de energías (LOTO)** (Model). Hereda de: `sgi.base.mixin`, `sgi.format.mixin`.

Orden: `date_applied desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_loto.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active_locks` | Integer | Candados puestos |  |  |  | compute `_compute_active_locks`, sin guardar |  | `addons/quimibond_sgi/models/sgi_loto.py:69` |
| `affected_notified` | Boolean | Se avisó a los afectados y a su jefe | Operadores de la máquina y su jefe saben que se va a bloquear. |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:52` |
| `area_notified_end` | Boolean | Se avisó al responsable del área al terminar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:64` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:40` |
| `date_applied` | Datetime | Bloqueado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:62` |
| `date_removed` | Datetime | Retirado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:63` |
| `energy_ids` | One2many | Fuentes de energía |  |  | `sgi.loto.energy` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:55` |
| `equipment_id` | Many2one | Equipo |  | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:43` |
| `lock_ids` | One2many | Candados y tarjetas |  |  | `sgi.loto.lock` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:56` |
| `maintenance_request_id` | Many2one | Orden de mantenimiento |  |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:45` |
| `name` | Char | Trabajo a realizar | Reparar, limpiar, desatascar, cambiar herramental… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:48` |
| `removal_note` | Text | Condiciones al retirar | Guardas colocadas, herramienta recogida, personal fuera de la zona. |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:66` |
| `responsible_id` | Many2one | Responsable del bloqueo |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:50` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:70` |
| `verified_by_id` | Many2one | Comprobó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:61` |
| `work_permit_id` | Many2one | Permiso de trabajo | Si el trabajo además requiere permiso de alto riesgo. |  | `sgi.work.permit` |  |  | `addons/quimibond_sgi/models/sgi_loto.py:46` |
| `zero_energy_method` | Text | Cómo se comprobó | Intento de arranque, medición con probador, manómetro en cero, purga… |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:58` |
| `zero_energy_verified` | Boolean | Energía cero comprobada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_loto.py:57` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | Bloqueo puesto: fuentes con su punto de bloqueo, un candado y tarjeta por trabajador, aviso a los afectados y energía cero. |
| `action_cancel` | — |
| `action_remove` | Retiro: ya no queda ningún candado y se avisó al área. |
| `action_reset` | Reabrir (solo desde retirado o cancelado, candado de evidencia: Jefe MAST). |
