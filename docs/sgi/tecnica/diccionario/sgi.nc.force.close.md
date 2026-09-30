<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.nc.force.close`

**Cierre forzado de No Conformidad** (TransientModel).

Asistente de cierre forzado de una NC con motivo; solo Jefe MAST.

Archivos: `addons/quimibond_sgi/models/sgi_nonconformity.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No Conformidad | No conformidad que se va a cerrar sin cumplir los candados. | sí | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1046` |
| `reason` | Text | Motivo del cierre forzado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1048` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
