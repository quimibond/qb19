<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.msa.study`

**Estudio MSA (IATF 7.1.5.1.1)** (Model). Hereda de: `sgi.base.mixin`.

Estudio de sistema de medición (MSA) de un equipo: GR&R, ndc y veredicto.

Orden: `date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_msa.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic` | Char | Característica medida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:60` |
| `date` | Date | Fecha | Fecha del estudio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:57` |
| `equipment_id` | Many2one | Equipo de medición | Equipo de medición estudiado. | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_msa.py:46` |
| `grr_pct` | Float | % GRR | Porcentaje de variación del sistema de medición (solo Gage R&R de variables). |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:63` |
| `ndc` | Integer | ndc (categorías distintas) | Número de categorías distintas; AIAG pide ≥ 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:66` |
| `notes` | Text | Notas / referencia del reporte |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:76` |
| `point_id` | Many2one | Punto de control | Punto de control de calidad en el que se usa el equipo. |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_msa.py:61` |
| `study_type` | Selection | Tipo de estudio | Tipo de estudio del sistema de medición. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:50` |
| `verdict` | Selection | Veredicto | Para Gage R&R se calcula de los umbrales AIAG (%GRR <10 / 10-30 / >30); para los demás tipos se captura a mano. |  |  | compute `_compute_verdict`, guardado |  | `addons/quimibond_sgi/models/sgi_msa.py:68` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
