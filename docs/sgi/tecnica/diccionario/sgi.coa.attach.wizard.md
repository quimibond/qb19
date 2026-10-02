<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.coa.attach.wizard`

**Adjuntar CoA a la entrega** (TransientModel).

Asistente para adjuntar el certificado de análisis (COA) a una entrega y, si se pide, mandarlo al cliente.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | CoA (PDF) | Uno por producto. Se acepta el nombre que ya usa el laboratorio. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:254` |
| `picking_id` | Many2one | Entrega | Entrega a la que se adjunta el CoA. | sí | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:252` |
| `recipient_ids` | Many2many | Destinatarios | Contactos del cliente a los que se envía el CoA. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:257` |
| `send` | Boolean | Enviar al cliente | Marque para enviar el CoA al cliente por correo al adjuntarlo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:261` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `default_get` | — |
