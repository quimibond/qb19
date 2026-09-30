<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.coa.inbox`

**COA recibido por correo** (Model). Hereda de: `mail.thread`.

Correos al buzón «COA»: cada PDF se liga a su salida por el nombre del archivo. Lo que no se liga queda aquí para que Calidad lo asigne.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:335` |
| `email_from` | Char | Remitente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:322` |
| `name` | Char | Asunto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:321` |
| `picking_id` | Many2one | Asignar a la salida | Para los que no se ligaron solos: la salida a la que pertenecen. |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:330` |
| `picking_ids` | Many2many | Salidas ligadas |  |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:327` |
| `result` | Text | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:334` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:323` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_link_manual` | Calidad asigna la salida: todos los PDF del correo van a ella. |
| `message_new` | — |
