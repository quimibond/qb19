<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.coa.attach.wizard`

**Adjuntar COA a la entrega** (TransientModel).

Asistente para adjuntar el certificado de análisis (COA) a una entrega y, si se pide, mandarlo al cliente.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | COA (PDF) | Uno por producto. Se acepta el nombre que ya usa el laboratorio. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:249` |
| `picking_id` | Many2one | Entrega |  | sí | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:248` |
| `recipient_ids` | Many2many | Destinatarios |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:252` |
| `send` | Boolean | Enviar al cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:255` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `default_get` | — |
