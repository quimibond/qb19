<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.template`

**Plantilla de checklist de mantenimiento (planta o unidades)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `code, name`.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:59` |
| `code` | Char | Clave | Actividad o formato que sustituye: S5.15, S5.16, S5.20… |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:46` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:61` |
| `employee_ids` | Many2many | Quién lo llena | Electromecánicos o choferes que pueden firmar la hoja. Vacío: cualquiera. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:55` |
| `equipment_ids` | Many2many | Equipos o unidades |  | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:51` |
| `frequency` | Selection | Frecuencia |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:47` |
| `item_ids` | One2many | Puntos a revisar |  |  | `sgi.checklist.template.item` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:58` |
| `last_run` | Date | Última generación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:60` |
| `maintenance_team_id` | Many2one | Equipo de mantenimiento |  |  | `maintenance.team` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:52` |
| `name` | Char | Checklist |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:45` |
| `user_id` | Many2one | Responsable de llenarlo | Usuario que ve las hojas: el de la tableta compartida o el jefe del área. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:53` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_today` | — |
| `cron_generate` | Cron diario: las plantillas que tocan hoy. |
