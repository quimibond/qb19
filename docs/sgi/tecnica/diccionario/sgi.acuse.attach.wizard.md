<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.acuse.attach.wizard`

**Adjuntar acuse firmado a la entrega** (TransientModel).

Asistente para adjuntar a una entrega (``stock.picking``) el acuse firmado por el cliente.

Archivos: `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `file` | Binary | Acuse firmado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_links.py:333` |
| `file_name` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_links.py:334` |
| `note` | Char | Nota |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_links.py:335` |
| `picking_id` | Many2one |  | Entrega a la que se adjunta el acuse firmado. | sí | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_links.py:331` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_attach` | — |
