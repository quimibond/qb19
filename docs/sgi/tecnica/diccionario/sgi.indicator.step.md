<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.step`

**Escalón trimestral de la meta de un indicador SGI** (Model).

Escalón trimestral de la meta de un indicador con trayectoria (meta y aceptable desde una fecha).

Orden: `indicator_id, date_from`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator_trajectory.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acceptable` | Float | Aceptable |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:42` |
| `date_from` | Date | Trimestre desde |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:39` |
| `indicator_id` | Many2one |  |  | sí | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:38` |
| `manual` | Boolean | Corregido a mano |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:43` |
| `name` | Char | Trimestre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:40` |
| `objective` | Float | Objetivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:41` |
| `reason` | Text | Motivo | Obligatorio al corregir un escalón a mano. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:44` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | Corregir objetivo o aceptable exige motivo; queda en el chatter. |
