<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.sample.wizard`

**Pedir la corrida de muestra del desarrollo** (TransientModel).

57.154.0: la corrida de muestra es la cadena del desarrollo (crudo → teñido → acabado), una orden por artículo. Los renglones se arman al abrir y se pueden ajustar antes de confirmar.

Archivos: `addons/quimibond_sgi/models/sgi_dev_sample.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_planned` | Datetime | Fecha deseada de máquina |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:214` |
| `line_ids` | One2many | Órdenes de la corrida |  |  | `sgi.dev.sample.wizard.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:211` |
| `machine_warning` | Text | Revise la máquina | Avisos de la ficha de la máquina sobre la ruta de cada lista: circular con otra galga que la del artículo o jet en el que la cantidad no cabe en una carga. No impiden pedir la corrida. |  |  | compute `_compute_machine_warning`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:217` |
| `note` | Text | Nota para Planeación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:216` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:210` |
| `suggested_qty` | Float | Cantidad sugerida del último artículo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:212` |
| `suggestion_note` | Text | Por qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:213` |
| `warning` | Text | Antes de confirmar | Lo que impide crear las órdenes: lista de materiales que no es del artículo, sin lista propia, sin ruta, sin cantidad o sin tipo de operación. |  |  | compute `_compute_warning`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:221` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | Crea una orden por renglón, en el orden de la cadena. Si algún renglón tiene un problema no se crea ninguna. |
| `default_get` | — |
