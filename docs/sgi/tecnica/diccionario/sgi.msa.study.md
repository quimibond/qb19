<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.msa.study`

**Estudio MSA (IATF 7.1.5.1.1)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_msa.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic` | Char | Característica medida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:56` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:54` |
| `equipment_id` | Many2one | Equipo de medición |  | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_msa.py:45` |
| `grr_pct` | Float | % GRR | Porcentaje de variación del sistema de medición (solo Gage R&R de variables). |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:58` |
| `ndc` | Integer | ndc (categorías distintas) | Número de categorías distintas; AIAG pide ≥ 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:61` |
| `notes` | Text | Notas / referencia del reporte |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:71` |
| `point_id` | Many2one | Punto de control |  |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_msa.py:57` |
| `study_type` | Selection | Tipo de estudio |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_msa.py:48` |
| `verdict` | Selection | Veredicto | Para Gage R&R se calcula de los umbrales AIAG (%GRR <10 / 10-30 / >30); para los demás tipos se captura a mano. |  |  | compute `_compute_verdict`, guardado |  | `addons/quimibond_sgi/models/sgi_msa.py:63` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
