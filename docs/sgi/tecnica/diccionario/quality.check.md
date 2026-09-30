<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `quality.check`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_calibration.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_equipment_id` | Many2one | Equipo de medición | Instrumento con el que se toma la medición. Un equipo con calibración vencida o fuera de tolerancia no puede usarse para dictaminar la inspección (IATF 7.1.5.2.1). |  | `maintenance.equipment` | compute `_compute_sgi_equipment_id`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:11` |

