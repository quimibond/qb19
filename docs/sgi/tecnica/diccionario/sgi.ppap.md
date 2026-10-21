<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.ppap`

**PPAP - Proceso de Aprobación de Partes de Producción (P-C15)** (Model). Hereda de: `sgi.base.mixin`.

Expediente PPAP (P-C15) de un producto para un cliente, con sus elementos y la decisión del cliente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_decision` | Date | Fecha de decisión | Fecha en que el cliente aprobó, rechazó o dio aprobación interina. |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:83` |
| `date_submitted` | Date | Fecha de envío | Fecha en que se envió el PPAP al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:81` |
| `element_ids` | One2many | Elementos | La columna S/R se ajusta por elemento (p. ej. nivel 4, donde el cliente define qué pide). |  | `sgi.ppap.element` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:85` |
| `level` | Selection | Nivel | Nivel de envío que pide el cliente (1 a 5); define qué elementos se entregan. Presentar (S) va al cliente y Retener (R) se queda en planta; solo los elementos S pendientes bloquean «Marcar enviado». Cambiarlo en preparación re-aplica la tabla. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:55` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:87` |
| `partner_id` | Many2one | Cliente | Cliente que aprueba el PPAP. | sí | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:49` |
| `product_tmpl_id` | Many2one | Producto | Producto que se somete a aprobación. | sí | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:52` |
| `reason` | Selection | Motivo | Por qué se hace el PPAP. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:65` |
| `state` | Selection | Estado | Preparación, enviado, aprobado, interino o rechazado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:73` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | — |
| `action_mark_enviado` | — |
| `action_reject` | — |
| `action_reset` | — |
| `action_set_interino` | — |
| `create` | — |
| `write` | — |
