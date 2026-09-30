<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.policy`

**Política integral del SGI** (Model). Hereda de: `sgi.base.mixin`.

Política integral del SGI con sus objetivos. Solo una vigente; «Generar acuses» la difunde con firma a los puestos del documento controlado donde se publica.

Orden: `issue_date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_policy.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `document_id` | Many2one | Documento publicado (MIID) | Documento controlado donde se publica la política (p. ej. el MIID). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_policy.py:38` |
| `health` | Selection | Salud agregada | Peor color entre los objetivos de la política (cascada abajo→arriba). |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_policy.py:46` |
| `issue_date` | Date | Fecha de emisión | Fecha de emisión de la política. |  |  |  |  | `addons/quimibond_sgi/models/sgi_policy.py:29` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_policy.py:27` |
| `objective_count` | Integer | # Objetivos |  |  |  | compute `_compute_objective_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_policy.py:44` |
| `objective_ids` | One2many | Objetivos integrales |  |  | `sgi.objective` |  |  | `addons/quimibond_sgi/models/sgi_policy.py:42` |
| `policy_text` | Html | Texto de la política |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_policy.py:28` |
| `state` | Selection | Estado | Borrador, vigente u obsoleta. Solo una política vigente. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_policy.py:32` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_acks` | Difusión con firma de la política (deuda D.28): delega en el documento controlado ligado. Antes se podía publicar la política sin difusión — dependía de ir al documento y acordarse de generar acuses. |
| `action_open_objectives` | — |
| `action_set_borrador` | — |
| `action_set_obsoleta` | — |
| `action_set_vigente` | — |
| `init` | A lo sumo UNA política vigente, garantizado en BD (la validación Python sola permite condición de carrera). Con datos legados (2+ vigentes) se loggea y se omite el índice en vez de abortar el update … |
