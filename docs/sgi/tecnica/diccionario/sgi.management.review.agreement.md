<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.management.review.agreement`

**Acuerdo de Revisión por la Dirección** (Model).

Acuerdo de una revisión por la dirección con responsable y fecha; se sigue como acción o tarea.

Orden: `deadline, id`.

Archivos: `addons/quimibond_sgi/models/sgi_management_review.py`, `addons/quimibond_sgi/models/sgi_kpi_review.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_id` | Many2one | Acción |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:413` |
| `action_state` | Selection | Estado de la acción |  |  |  | related `action_line_id.state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_management_review.py:414` |
| `deadline` | Date | Fecha límite |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:411` |
| `done_date` | Date | Cumplido el | Fecha de cumplimiento del acuerdo: la de su acción al terminarse, o capturada a mano si el acuerdo no tiene acción (E1-02: cerrado antes de su límite). |  |  | compute `_compute_done_date`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_review.py:22` |
| `is_done` | Boolean | Cumplido |  |  |  | compute `_compute_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_management_review.py:415` |
| `name` | Char | Acuerdo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:409` |
| `responsible_id` | Many2one | Responsable |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:410` |
| `review_id` | Many2one | Revisión |  | sí | `sgi.management.review` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:407` |
| `status_label` | Char | Situación |  |  |  | compute `_compute_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_management_review.py:416` |
| `task_id` | Many2one | Tarea (anterior a 52.0.0) |  |  | `project.task` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:412` |

