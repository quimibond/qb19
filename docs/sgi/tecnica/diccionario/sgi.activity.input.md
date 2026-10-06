<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.input`

**Entregable que recibe una actividad SGI** (Model).

Un «recibe» de la actividad: qué entregable y en cuántos días hábiles debe llegar a ella. El plazo es de quien recibe (la misma salida puede urgirle a uno y no a otro) y de él sale el eslabón atorado.

Orden: `activity_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_deliverable.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:345` |
| `applies_domain` | Char | Aplica cuando | Dominio sobre el registro del entregable de entrada. Si no se cumple, la actividad no aplica a ese registro: no vence ni cuenta como atrasada. Ej. [('partner_id.country_id.code', '!=', 'MX')]. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:736` |
| `applies_note` | Char | Aplica cuando (en palabras) | «Solo embarques de exportación». |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:741` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_deliverable.py:356` |
| `deliverable_id` | Many2one | Entregable |  | sí | `sgi.deliverable` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:347` |
| `due_field` | Char | Vence según el campo | Campo de fecha del registro de esta entrada contra el que vence la actividad (ej. «scheduled_date» de la entrega). Con esto el plazo no son días desde que llegó sino esa fecha más el margen. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:750` |
| `match_path` | Char | Se liga con la salida por | Campo de la salida que apunta al registro de esta entrada, cuando son modelos distintos (ej. «sale_id» si la salida es un stock.picking y la entrada un sale.order). Mismo modelo: no hace falta. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:743` |
| `max_days` | Integer | Plazo (días hábiles) | Días hábiles que puede pasar entre que se entrega y que esta actividad lo toma. Pasado el plazo, el eslabón está atorado. 0 = sin plazo (solo se revisa que ambas tengan evidencia). |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:350` |
| `offset_days` | Integer | Margen (días hábiles) | Días hábiles que se suman a «Vence según el campo». Negativo = antes: -2 vence dos días hábiles antes de la fecha programada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:755` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_deliverable.py:355` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:349` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
