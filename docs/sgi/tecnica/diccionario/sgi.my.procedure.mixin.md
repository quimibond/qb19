<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.procedure.mixin`

**Mi procedimiento en la ficha** (AbstractModel).

«Mi procedimiento» dentro de la ficha (empleado, empleado público y puesto): las mismas listas que la pantalla de Inicio, como campos calculados, para que el procedimiento se vea donde vive la persona (app Empleados) sin pantalla aparte. C…

Archivos: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_mp_ack_ids` | Many2many | Acuses de lectura (Mi procedimiento) |  |  | `sgi.document.ack` | compute `_compute_sgi_mp_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:137` |
| `sgi_mp_can_sign` | Boolean | Puede firmar |  |  |  | compute `_compute_sgi_mp_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:144` |
| `sgi_mp_document_ids` | Many2many | Documentos que aplican al puesto |  |  | `documents.document` | compute `_compute_sgi_mp_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:139` |
| `sgi_mp_epp_ids` | Many2many | Responsivas de EPP (Mi procedimiento) |  |  | `sgi.epp.delivery` | compute `_compute_sgi_mp_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:141` |
| `sgi_mp_epp_text` | Text | EPP requerido (Mi procedimiento) |  |  |  | compute `_compute_sgi_mp_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:143` |
| `sgi_mp_process_ids` | Many2many | Procesos donde participa |  |  | `sgi.process` | compute `_compute_sgi_mp_roles`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:145` |
| `sgi_mp_received_role_ids` | Many2many | Escalamientos que recibe |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:133` |
| `sgi_mp_role_ids` | Many2many | Mis actividades |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:131` |
| `sgi_mp_short_role_ids` | Many2many | Participa o se entera |  |  | `sgi.activity.role` | compute `_compute_sgi_mp_roles`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:135` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_mp_sign` | Firmar «leído y entendido» desde la ficha: mismo candado que la pantalla (solo el propio empleado, contra la revisión vigente). |
