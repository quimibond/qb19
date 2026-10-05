<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.measure.review.reject`

**Revisión de medición: no corresponde** (TransientModel).

Asistente de «No corresponde» (la nota es obligatoria).

Archivos: `addons/quimibond_sgi/models/sgi_measure_review.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `note` | Text | Qué no corresponde | El filtro, el modelo, la fecha o quién aparece. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:247` |
| `review_id` | Many2one |  |  | sí | `sgi.measure.review` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:246` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
