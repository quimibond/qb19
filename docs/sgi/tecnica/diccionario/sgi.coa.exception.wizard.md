<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.coa.exception.wizard`

**Validar salida sin COA (Jefe de Calidad)** (TransientModel).

Asistente para validar una salida sin COA con motivo; solo lo usa el puesto de excepción (Jefe de Calidad).

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `picking_ids` | Many2many | Salidas |  |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:297` |
| `reason` | Text | Motivo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:298` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
