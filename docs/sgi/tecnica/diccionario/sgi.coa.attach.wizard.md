<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.coa.attach.wizard`

**Adjuntar COA a la entrega** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | COA (PDF) | Uno por producto. Se acepta el nombre que ya usa el laboratorio. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:247` |
| `picking_id` | Many2one | Entrega |  | sí | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:246` |
| `recipient_ids` | Many2many | Destinatarios |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:250` |
| `send` | Boolean | Enviar al cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:253` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `default_get` | — |
