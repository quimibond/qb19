<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.nc.cancel`

**Cancelación de no conformidad** (TransientModel).

NC-4: cancelar solo con motivo y aprobación del Jefe MAST. Cualquier usuario del SGI pide la cancelación con su motivo (queda en el chatter y agenda la aprobación a MAST); el Jefe MAST la aprueba con el mismo asistente. Nunca se llega a Ca…

Archivos: `addons/quimibond_sgi/models/sgi_nonconformity.py`.

## Campos (3)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No conformidad | No conformidad que se cancela. | sí | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1104` |
| `is_manager` | Boolean |  | Indica si usted es Jefe MAST y SGI. |  |  | compute `_compute_is_manager`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1107` |
| `reason` | Text | Motivo de la cancelación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1106` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
