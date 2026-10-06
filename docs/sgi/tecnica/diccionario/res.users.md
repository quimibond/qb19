<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `res.users`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_hr.py`, `addons/quimibond_sgi/models/sgi_weekly_overdue.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_deactivated_date` | Date | Desactivado el | Fecha en que se desactivó el usuario (S6-02, contra la baja del empleado). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_hr.py:65` |
| `sgi_weekly_overdue_mail` | Boolean | Correo semanal de mis pendientes atrasados (SGI) | Cada semana, un correo con lo atrasado de «Mis pendientes» del SGI. Solo llega si hay algo atrasado. Apáguelo si no lo quiere. |  |  |  |  | `addons/quimibond_sgi/models/sgi_weekly_overdue.py:29` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `SELF_READABLE_FIELDS` | — |
| `SELF_WRITEABLE_FIELDS` | — |
| `write` | — |
