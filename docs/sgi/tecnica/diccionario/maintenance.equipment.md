<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `maintenance.equipment`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_calibration.py`, `addons/quimibond_sgi/models/sgi_msa.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_calibration_count` | Integer | N° de calibraciones |  |  |  | compute `_compute_calibration_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_calibration.py:75` |
| `sgi_calibration_ids` | One2many | Calibraciones |  |  | `sgi.calibration` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:73` |
| `sgi_calibration_interval_months` | Integer | Intervalo de calibración (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:58` |
| `sgi_calibration_state` | Selection | Estado de calibración |  |  |  | compute `_compute_calibration_state`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:66` |
| `sgi_do_not_use` | Boolean | No usar | Equipo bloqueado (fuera de tolerancia o calibración vencida). |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:71` |
| `sgi_is_measuring` | Boolean | Equipo de medición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:54` |
| `sgi_is_ppe` | Boolean | Equipo de protección personal (EPP) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:79` |
| `sgi_last_calibration_date` | Date | Última calibración |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:60` |
| `sgi_magnitude` | Char | Magnitud |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:55` |
| `sgi_msa_count` | Integer | # MSA |  |  |  | compute `_compute_sgi_msa_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_msa.py:17` |
| `sgi_next_calibration_date` | Date | Próxima calibración | Se calcula como última + intervalo, pero el laboratorio puede fijar otra fecha (prevalece). |  |  | compute `_compute_next_calibration_date`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:61` |
| `sgi_ppe_expiry_date` | Date | Vencimiento del EPP |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:80` |
| `sgi_range` | Char | Rango |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:56` |
| `sgi_resolution` | Char | Resolución |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:57` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_calibrations` | — |
| `action_view_sgi_msa` | — |
