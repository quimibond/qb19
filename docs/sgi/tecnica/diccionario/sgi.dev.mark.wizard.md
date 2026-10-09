<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.mark.wizard`

**Marcar proyectos existentes como desarrollo de producto** (TransientModel).

Marca como desarrollo de producto varios proyectos existentes de una vez. La lista se llena con los candidatos (nombre de código de artículo o columna «Por revisar»); quite los que no sean desarrollos antes de marcar. No mueve de etapa: es…

Archivos: `addons/quimibond_sgi/models/sgi_dev_board.py`.

## Campos (3)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `candidate_count` | Integer | Candidatos |  |  |  | compute `_compute_candidate_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_board.py:129` |
| `load_lines` | Boolean | Proponer las características del tipo general | Al marcar, llena la tabla de características con la plantilla del tipo general. Si no, la tabla se llena al elegir el tipo de desarrollo en cada proyecto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_board.py:125` |
| `project_ids` | Many2many | Proyectos a marcar | Se proponen los proyectos con nombre de código de artículo y los de la columna «Por revisar». Quite los que no sean desarrollos y agregue los que falten. |  | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_board.py:120` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark` | — |
| `default_get` | — |
