<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.machine.sheet`

**Ficha técnica de proceso por máquina (F-IT-P-P01-08-05)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Ficha técnica de proceso por máquina: parámetros, hilos y poleas por producto y centro de trabajo, con revisión.

Orden: `product_id, workcenter_id, revision desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_machine_sheet.py`.

## Campos (22)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:42` |
| `approved_by_id` | Many2one | Jefe técnico de tejido y acabado |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:54` |
| `area` | Selection | Área |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:38` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:56` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:37` |
| `engineering_by_id` | Many2one | Jefe de ingeniería de procesos |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:55` |
| `fabric_note` | Text | Observaciones de tela acondicionada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:51` |
| `lab_user_id` | Many2one | Laboratorista de tejido |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:52` |
| `machine_note` | Text | Observaciones de máquina |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:50` |
| `mechanic_user_id` | Many2one | Mecánico de tejido |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:53` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:34` |
| `param_line_ids` | One2many | Parámetros |  |  | `sgi.machine.sheet.param` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:49` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:35` |
| `pulley_1` | Char | Polea 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:44` |
| `pulley_2` | Char | Polea 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:45` |
| `pulley_3` | Char | Polea 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:46` |
| `pulley_4` | Char | Polea 4 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:47` |
| `revision` | Integer | Revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:39` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:40` |
| `workcenter_id` | Many2one | Máquina / centro de trabajo |  | sí | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:36` |
| `yarn_line_ids` | One2many | Materia prima (hilos) |  |  | `sgi.machine.sheet.yarn` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:43` |
| `yarn_note` | Text | Observaciones de materia prima |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:48` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_new_revision` | — |
| `action_set_current` | — |
| `action_set_obsolete` | — |
| `copy_data` | — |
| `create` | — |
| `sgi_format_info` | — |
