<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.sign.request.wizard`

**Crear solicitud de firma ligada a un registro** (TransientModel).

Asistente para crear una solicitud de firma ligada a un registro del SGI.

Archivos: `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `partner_id` | Many2one | Firmante |  | sí | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:130` |
| `res_id` | Integer |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:128` |
| `res_model` | Char |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:127` |
| `subject` | Char | Asunto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:131` |
| `template_id` | Many2one | Plantilla de firma |  | sí | `sign.template` |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:129` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | F-003 (auditoría 2026-09): sin sudo(). Antes cualquier usuario interno podía mandar por RPC CUALQUIER plantilla de Sign a cualquier contacto, porque plantilla y solicitud se leían y creaban como supe… |
