<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.machine.sheet`

**Ficha técnica de proceso por máquina (F-IT-P-P01-08-05)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Ficha técnica de proceso por máquina: parámetros, hilos y poleas por producto y centro de trabajo, con revisión.

Orden: `product_id, workcenter_id, revision desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_machine_sheet.py`, `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`.

## Campos (27)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:49` |
| `approved_by_id` | Many2one | Jefe técnico de tejido y acabado | Jefe técnico de tejido y acabado que aprueba la ficha. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:63` |
| `area` | Selection | Área | Si la ficha es de producción o de desarrollos. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:41` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:67` |
| `date` | Date | Fecha | Fecha de la ficha. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:39` |
| `engineering_by_id` | Many2one | Jefe de ingeniería de procesos | Jefe de ingeniería de procesos que revisa la ficha. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:65` |
| `fabric_note` | Text | Observaciones de tela acondicionada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:58` |
| `lab_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:109` |
| `lab_user_id` | Many2one | Laboratorista de tejido | Laboratorista de tejido que participa en la ficha. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:59` |
| `machine_note` | Text | Observaciones de máquina |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:57` |
| `mechanic_user_id` | Many2one | Mecánico de tejido | Mecánico de tejido que participa en la ficha. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:61` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:34` |
| `param_line_ids` | One2many | Parámetros |  |  | `sgi.machine.sheet.param` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:56` |
| `product_id` | Many2one | Artículo | Artículo que se fabrica con esta ficha. | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:35` |
| `production_id` | Many2one | Orden de la corrida |  |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:106` |
| `project_id` | Many2one | Proyecto de desarrollo |  |  | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:104` |
| `proposed_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:107` |
| `pulley_1` | Char | Polea 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:51` |
| `pulley_2` | Char | Polea 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:52` |
| `pulley_3` | Char | Polea 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:53` |
| `pulley_4` | Char | Polea 4 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:54` |
| `revision` | Integer | Revisión | Número de revisión de la ficha. «Nueva revisión» lo sube. |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:43` |
| `state` | Selection | Estado | Borrador, vigente u obsoleta. Solo una ficha vigente por artículo y máquina. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:45` |
| `validated_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:108` |
| `workcenter_id` | Many2one | Máquina / centro de trabajo | Máquina o centro de trabajo de la ficha. | sí | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:37` |
| `yarn_line_ids` | One2many | Materia prima (hilos) |  |  | `sgi.machine.sheet.yarn` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:50` |
| `yarn_note` | Text | Observaciones de materia prima |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:55` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_new_revision` | — |
| `action_propose` | — |
| `action_set_current` | — |
| `action_set_obsolete` | — |
| `action_validate` | — |
| `copy_data` | — |
| `create` | — |
| `sgi_format_info` | — |
