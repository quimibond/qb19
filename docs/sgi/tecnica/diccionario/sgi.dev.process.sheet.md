<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.sheet`

**Ficha de proceso de tintorería o acabado** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Ficha de proceso de tintorería o acabado de un artículo (y de la corrida de muestra que la originó).

Orden: `product_id, area, revision desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`.

## Campos (33)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:106` |
| `area` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:94` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:107` |
| `date` | Date |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:103` |
| `dye_brand` | Char | Teñido: marca |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:127` |
| `dye_chart_svg` | Html | Gráfica de proceso |  |  |  | compute `_compute_dye_chart`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:123` |
| `dye_machine` | Char | Teñido: máquina |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:126` |
| `dye_model` | Char | Teñido: modelo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:128` |
| `dye_step_ids` | One2many | Tramos de la gráfica de proceso |  |  | `sgi.dev.dye.step` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:121` |
| `dye_total_min` | Float | Tiempo total de la gráfica (min) |  |  |  | compute `_compute_dye_chart`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:122` |
| `formula_code` | Char | Número de fórmula | Los químicos no se capturan aquí: la fórmula en g/L vive en el modelo de fórmulas (Jose Sacramento). Mientras llega, el número de fórmula. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:118` |
| `lab_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:116` |
| `lab_user_id` | Many2one | Midió (Laboratorio) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:115` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:93` |
| `note` | Text | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:109` |
| `param_line_ids` | One2many | Parámetros |  |  | `sgi.dev.process.param` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:108` |
| `pass_count` | Integer | Pases de rama |  |  |  | compute `_compute_pass_count`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:129` |
| `pass_result_1` | Selection | Resultado pase 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:130` |
| `pass_result_2` | Selection | Resultado pase 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:131` |
| `pass_result_3` | Selection | Resultado pase 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:132` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:95` |
| `production_id` | Many2one | Orden de la corrida | Orden de muestra cuyos parámetros registra esta ficha. |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:99` |
| `project_id` | Many2one | Proyecto de desarrollo |  |  | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:97` |
| `proposed_by_id` | Many2one | Propuso (Diseño y Desarrollo de Procesos) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:111` |
| `proposed_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:112` |
| `revision` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:104` |
| `route_line_ids` | One2many | Ruta de proceso (hasta 10 pasos) |  |  | `sgi.dev.finish.route` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:125` |
| `state` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:105` |
| `third_pass_alerted` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:133` |
| `validated_by_id` | Many2one | Validó (supervisor del área) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:113` |
| `validated_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:114` |
| `workcenter_id` | Many2one | Máquina / centro de trabajo |  | sí | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:96` |
| `workorder_id` | Many2one | Orden de trabajo |  |  | `mrp.workorder` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:101` |

## Métodos públicos (12)

| Método | Qué hace (docstring) |
|---|---|
| `action_add_pass` | — |
| `action_back_to_draft` | — |
| `action_new_revision` | — |
| `action_print` | — |
| `action_propose` | — |
| `action_set_current` | La ficha validada de la corrida aprobada pasa a ser la ficha vigente del artículo. |
| `action_set_obsolete` | — |
| `action_validate` | — |
| `copy_data` | — |
| `create` | — |
| `sgi_format_info` | — |
| `write` | — |
