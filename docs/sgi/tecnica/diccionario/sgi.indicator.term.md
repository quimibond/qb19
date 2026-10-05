<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.term`

**Término de la fórmula de un indicador SGI** (Model).

Término de la fórmula configurable de un indicador: modelo, dominio, campo, agregación y papel (numerador o denominador).

Orden: `indicator_id, role`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator_formula.py`, `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `aggregation` | Selection | Agregación | Cómo se agrega: contar registros, sumar un campo, o contar y promediar la diferencia entre dos fechas. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:105` |
| `date_field` | Char | Campo de fecha | Campo de fecha o fecha-hora del modelo sobre el que se recorta la ventana. Vacío solo con ventana «Acumulado al cierre»: entonces cuenta todo lo que hay hoy (p. ej. las existencias). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:100` |
| `delta_op` | Selection | Condición | Condición que debe cumplir la diferencia entre las dos fechas para contar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:114` |
| `delta_unit` | Selection | Unidad | Unidad de la diferencia entre fechas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:112` |
| `delta_value` | Float | N | Días, horas o el día del mes siguiente, según la unidad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:117` |
| `domain` | Text | Filtro | Dominio de Odoo, p. ej. [('state', '=', 'done')]. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:98` |
| `factor` | Float | Factor | Multiplica el resultado: −1 invierte el signo, 0.001 pasa kg a toneladas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:119` |
| `field_name` | Char | Campo a sumar / fecha A | Campo numérico a sumar; en las agregaciones de fechas, la fecha A (inicio). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:108` |
| `field_name_2` | Char | Fecha B (fin) | Segunda fecha del registro para las agregaciones «B − A». |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:110` |
| `indicator_id` | Many2one |  |  | sí | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:91` |
| `model_id` | Many2one | Modelo | Modelo de Odoo del que se leen los registros. | sí | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:95` |
| `model_name` | Char | Modelo técnico |  |  |  | related `model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:97` |
| `role` | Selection | Parte de la fórmula | Si el término va en el numerador o en el denominador. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:92` |
| `window` | Selection | Ventana | Qué periodo se lee: el del indicador, 3 o 12 meses móviles, o acumulado al cierre. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:122` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
