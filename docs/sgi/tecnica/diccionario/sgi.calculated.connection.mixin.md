<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.calculated.connection.mixin`

**Conexión calculada de un entregable** (AbstractModel).

Lo común a ligas y flujos calculados: no se editan a mano; si uno no aplica se desactiva con su motivo. Los capturados a mano (sin entregable) son de la versión anterior: se archivan cuando un entregable cubre las mismas actividades o proc…

Archivos: `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:454` |
| `deliverable_id` | Many2one | Entregable (catálogo) | Si viene de un entregable, se calcula sola de quién lo entrega y quién lo recibe; no se edita a mano. |  | `sgi.deliverable` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:450` |
| `inactive_reason` | Char | Motivo de desactivación | Por qué esta conexión calculada no aplica (obligatorio al desactivarla). |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:455` |
| `origin` | Selection | Origen |  |  |  | compute `_compute_origin`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:458` |

