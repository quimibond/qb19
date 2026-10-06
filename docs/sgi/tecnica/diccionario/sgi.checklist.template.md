<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.template`

**Plantilla de checklist de mantenimiento (planta o unidades)** (Model). Hereda de: `hr.mixin`, `mail.activity.mixin`, `mail.thread`.

Plantilla de checklist de planta o de unidades: puntos, equipos, frecuencia y quién la llena. El cron diario genera las hojas del día.

Orden: `code, name`.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:74` |
| `code` | Char | Clave | Actividad o formato que sustituye: S5.15, S5.16, S5.20… |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:56` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:77` |
| `employee_ids` | Many2many | Quién lo llena | Electromecánicos o choferes que pueden firmar la hoja. Vacío: cualquiera. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:70` |
| `equipment_ids` | Many2many | Equipos o unidades | Equipos o unidades que se revisan con esta plantilla. Cada uno tiene su hoja. | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:62` |
| `frequency` | Selection | Frecuencia | Diaria (de lunes a viernes) o semanal (los lunes). El cron genera las hojas a esa frecuencia. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:57` |
| `item_ids` | One2many | Puntos a revisar |  |  | `sgi.checklist.template.item` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:73` |
| `last_run` | Date | Última generación | Último día en que se generaron hojas de esta plantilla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:75` |
| `maintenance_team_id` | Many2one | Equipo de mantenimiento | Equipo de mantenimiento al que llegan las hojas y las correctivas. |  | `maintenance.team` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:65` |
| `name` | Char | Checklist |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:55` |
| `user_id` | Many2one | Responsable de llenarlo | Usuario que ve las hojas: el de la tableta compartida o el jefe del área. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:68` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_today` | — |
| `cron_generate` | Cron diario: las plantillas que tocan hoy. |
