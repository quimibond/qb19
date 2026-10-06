<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.leave`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_incident_leave.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_incident_id` | Many2one | Incidente SST | Incidente del SGI que se abrió por esta incapacidad por riesgo de trabajo. |  | `sgi.incident` |  | hr_holidays.group_hr_holidays_user,quimibond_sgi.group_sgi_manager,quimibond_sgi.group_sgi_health | `addons/quimibond_sgi/models/sgi_incident_leave.py:33` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
