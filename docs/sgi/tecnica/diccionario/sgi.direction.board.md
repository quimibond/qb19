<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.direction.board`

**Tablero de dirección (I-9)** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_direction_board.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date` | Date | Fecha |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_direction_board.py:91` |
| `delayed_process_ids` | Many2many | Procesos con más atrasos |  |  | `sgi.process` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:98` |
| `indicator_count` | Integer |  |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:100` |
| `indicator_ids` | Many2many | Indicadores de dirección |  |  | `sgi.indicator` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:93` |
| `indicator_note` | Char | Nota de indicadores |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:101` |
| `objective_ids` | Many2many | Objetivos integrales |  |  | `sgi.objective` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:92` |
| `overdue_agreement_ids` | Many2many | Acuerdos de la RxD vencidos |  |  | `sgi.action.line` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:96` |
| `red_count` | Integer |  |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:102` |
| `red_no_plan_measure_ids` | Many2many | Rojos sin causa ni plan |  |  | `sgi.indicator.measure` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:94` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | Menú Dirección → Tablero (el título es el nombre del menú, D-005). |
| `action_open_spreadsheet` | — |
