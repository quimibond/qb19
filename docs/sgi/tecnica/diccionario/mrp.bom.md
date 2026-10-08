<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mrp.bom`

Modelo de otra app que el SGI extiende.

57.127.0 (Jose, 1a): la ruta se atribuye a quien la asigna, no al último que editó la lista.

Archivos: `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_route_assigned_by_id` | Many2one | Ruta asignada por | Quien capturó o cambió por última vez las operaciones (C1.04b). |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:53` |
| `sgi_route_assigned_date` | Datetime | Ruta asignada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:55` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
