<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.term`

**Término de la fórmula de un indicador SGI** (Model).

Orden: `indicator_id, role`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator_formula.py`, `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `aggregation` | Selection | Agregación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:94` |
| `date_field` | Char | Campo de fecha | Campo de fecha o fecha-hora del modelo sobre el que se recorta la ventana. Vacío solo con ventana «Acumulado al cierre»: entonces cuenta todo lo que hay hoy (p. ej. las existencias). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:89` |
| `delta_op` | Selection | Condición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:100` |
| `delta_unit` | Selection | Unidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:99` |
| `delta_value` | Float | N | Días, horas o el día del mes siguiente, según la unidad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:101` |
| `domain` | Text | Filtro | Dominio de Odoo, p. ej. [('state', '=', 'done')]. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:87` |
| `factor` | Float | Factor | Multiplica el resultado: −1 invierte el signo, 0.001 pasa kg a toneladas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:103` |
| `field_name` | Char | Campo a sumar / fecha A | Campo numérico a sumar; en las agregaciones de fechas, la fecha A (inicio). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:95` |
| `field_name_2` | Char | Fecha B (fin) | Segunda fecha del registro para las agregaciones «B − A». |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:97` |
| `indicator_id` | Many2one |  |  | sí | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:82` |
| `model_id` | Many2one | Modelo |  | sí | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:85` |
| `model_name` | Char | Modelo técnico |  |  |  | related `model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:86` |
| `role` | Selection | Parte de la fórmula |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:83` |
| `window` | Selection | Ventana |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:106` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
