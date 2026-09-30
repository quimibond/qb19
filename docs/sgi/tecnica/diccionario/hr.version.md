<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.version`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_hr.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_departure_reason_id` | Many2one | Motivo de baja (SGI) | El motivo de la versión o, si no lo trae, el del empleado (S4-01). |  | `hr.departure.reason` | compute `_compute_sgi_departure`, guardado | hr.group_hr_user | `addons/quimibond_sgi/models/sgi_kpi_hr.py:33` |
| `sgi_departure_registered_at` | Datetime | Baja registrada el | Cuándo se capturó la fecha de baja (S4-01). |  |  |  | hr.group_hr_user | `addons/quimibond_sgi/models/sgi_kpi_hr.py:37` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
