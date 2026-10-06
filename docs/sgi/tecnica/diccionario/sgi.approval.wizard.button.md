<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.approval.wizard.button`

**Acción que se aprueba** (TransientModel).

Una acción del formulario del documento (para elegirla por su etiqueta).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_approval_wizard.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `method` | Char |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:406` |
| `name` | Char | Acción |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:407` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:405` |
| `wizard_id` | Many2one |  |  | sí | `sgi.approval.wizard` |  |  | `addons/quimibond_sgi/models/sgi_approval_wizard.py:404` |

