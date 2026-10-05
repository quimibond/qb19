<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.wizard`

**Nuevo indicador por fórmula** (TransientModel).

Nuevo indicador por fórmula, con preguntas y vista previa.

Archivos: `addons/quimibond_sgi/models/sgi_indicator_wizard.py`.

## Campos (16)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `amount_field_id` | Many2one | ¿Qué se suma? |  |  | `ir.model.fields` |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:101` |
| `code` | Char | Clave | Clave corta, p. ej. C2-09. Se propone con la clave del proceso. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:88` |
| `direction` | Selection | ¿Qué es mejor? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:108` |
| `filter_domain` | Char | ¿Cuáles cuentan? | En «Qué porcentaje cumple»: los que cumplen (el numerador). En los otros: solo si quiere contar o sumar algunos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:97` |
| `formula_sentence` | Char | Fórmula en palabras |  |  |  | compute `_compute_preview`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:114` |
| `frequency` | Selection | ¿Cada cuánto se mide? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:106` |
| `kind` | Selection | ¿Qué quiere saber? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:93` |
| `name` | Char | ¿Cómo se llama? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:87` |
| `preview_html` | Html | Así habría salido |  |  |  | compute `_compute_preview`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:113` |
| `process_id` | Many2one | ¿De qué proceso? |  | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:90` |
| `responsible_id` | Many2one | ¿Quién es el dueño? |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:91` |
| `source_id` | Many2one | ¿De qué registros? |  | sí | `sgi.indicator.source` |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:94` |
| `source_model` | Char |  |  |  |  | related `source_id.model_name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:96` |
| `target_acceptable` | Float | Aceptable |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:112` |
| `target_objective` | Float | Meta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:111` |
| `window` | Selection | ¿Qué periodo mira? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:105` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_create` | Crea el indicador en «prueba» con su fórmula y lo abre. |
| `action_view_records` | Los registros que cuenta el último periodo cerrado. |
