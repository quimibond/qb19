<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `maintenance.request`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_alert_id` | Many2one | NC generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:166` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_raise_nc` | Levanta una NC (equipo NC Internas) desde una solicitud correctiva, pre-llenada con el equipo/máquina y la descripción de la falla. |
