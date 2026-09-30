<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.direction.board`

**Tablero de dirección (I-9)** (TransientModel).

Tablero de Dirección (I-9): indicadores en rojo, rojos sin plan, procesos atrasados y acuerdos vencidos. Pantalla que se calcula al abrirla.

Archivos: `addons/quimibond_sgi/models/sgi_direction_board.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date` | Date | Fecha | Fecha del tablero. |  |  |  |  | `addons/quimibond_sgi/models/sgi_direction_board.py:93` |
| `delayed_process_ids` | Many2many | Procesos con más atrasos | Procesos con más actividades atrasadas. |  | `sgi.process` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:105` |
| `indicator_count` | Integer |  |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:108` |
| `indicator_ids` | Many2many | Indicadores de dirección | Indicadores que sigue la Dirección. |  | `sgi.indicator` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:97` |
| `indicator_note` | Char | Nota de indicadores |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:109` |
| `objective_ids` | Many2many | Objetivos integrales | Objetivos integrales del año. |  | `sgi.objective` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:95` |
| `overdue_agreement_ids` | Many2many | Acuerdos de la RxD vencidos | Acuerdos de la revisión por la dirección que ya vencieron. |  | `sgi.action.line` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:102` |
| `red_count` | Integer |  |  |  |  | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:110` |
| `red_no_plan_measure_ids` | Many2many | Rojos sin causa ni plan | Mediciones en rojo que todavía no tienen causa ni plan de acción. |  | `sgi.indicator.measure` | compute `_compute_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:99` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | Menú Dirección → Tablero (el título es el nombre del menú, D-005). |
| `action_open_spreadsheet` | — |
