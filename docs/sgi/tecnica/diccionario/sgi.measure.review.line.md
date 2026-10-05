<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.measure.review.line`

**Muestra de la revisión de medición** (Model).

Un registro de la evidencia tomado al azar para la revisión.

Orden: `review_id, record_date desc, id`.

Archivos: `addons/quimibond_sgi/models/sgi_measure_review.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `name` | Char | Registro de evidencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:225` |
| `record_date` | Date | Fecha que cuenta la medición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:226` |
| `record_user_id` | Many2one | A quién se le atribuye |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:227` |
| `res_id` | Integer | Registro |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:224` |
| `res_model` | Char | Modelo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:223` |
| `review_id` | Many2one | Revisión |  | sí | `sgi.measure.review` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:221` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_record` | Abre el registro con los permisos de quien revisa. |
