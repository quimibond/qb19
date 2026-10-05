<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.approval.wizard`

**Configurar una aprobación del procedimiento** (TransientModel).

Configurar la aprobación de un rol «Aprueba» con tres preguntas.

Archivos: `addons/quimibond_sgi/models/sgi_approval_wizard.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  |  |  | related `role_id.activity_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:182` |
| `approver_user_ids` | Many2many | Quién aprueba |  |  |  | related `role_id.approval_user_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:183` |
| `button_line_id` | Many2one | ¿Al pulsar qué? |  |  | `sgi.approval.wizard.button` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:191` |
| `button_line_ids` | One2many |  |  |  | `sgi.approval.wizard.button` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:193` |
| `button_supported` | Boolean |  |  |  |  | compute `_compute_button_supported`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:207` |
| `buttons_model` | Char |  | Modelo del que se cargaron las acciones. |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:194` |
| `category_name` | Char | ¿Cómo se llama la solicitud? | El nombre con el que se pide en Aprobaciones (p. ej. «Alta de usuario»). |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:204` |
| `condition_field_id` | Many2one | Cuando el campo |  |  | `ir.model.fields` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:198` |
| `condition_on` | Boolean | Solo a veces | Márquelo si solo se aprueba bajo una condición (p. ej. cuando el total pasa de cierta cantidad). |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:195` |
| `condition_operator` | Selection | sea |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:202` |
| `condition_value` | Char | el valor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:203` |
| `model_id` | Many2one | ¿Qué documento? | El documento de Odoo que se aprueba, por su nombre (Orden de compra, Factura, Entrega…). |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:187` |
| `preview_html` | Html | Así funcionará |  |  |  | compute `_compute_preview_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:209` |
| `q_what` | Selection | ¿Qué se aprueba? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:186` |
| `role_id` | Many2one | Aprobación |  | sí | `sgi.activity.role` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:180` |
| `sign_template_id` | Many2one | ¿Qué documento se firma? |  |  | `sign.template` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:206` |
| `step` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:184` |
| `suggestion` | Char | Sugerencia |  |  |  | related `role_id.approval_suggestion`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:208` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_activate` | «Activar»: escribe la aprobación en el rol y la sincroniza. |
| `action_back` | — |
| `action_next` | — |
| `create` | — |
