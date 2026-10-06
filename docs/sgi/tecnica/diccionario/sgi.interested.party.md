<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.interested.party`

**Parte interesada (ISO 4.2)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Parte interesada (4.2) con sus necesidades, requisitos legales y riesgos ligados, y revisión periódica.

Orden: `party_type, category, name`.

Archivos: `addons/quimibond_sgi/models/sgi_context.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:70` |
| `becomes_requirement` | Boolean | Se adopta como requisito del SGI | La organización DECIDE cuáles necesidades se vuelven requisito (4.2): márquelo y documente cómo se atiende. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:45` |
| `category` | Selection | Categoría | Grupo al que pertenece la parte interesada. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:31` |
| `last_review_date` | Date | Última revisión | Fecha de la última revisión de sus necesidades. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:64` |
| `legal_ids` | Many2many | Requisitos legales ligados | Requisitos legales que nacen de esta parte interesada. |  | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_context.py:58` |
| `name` | Char | Parte interesada |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:25` |
| `needs` | Text | Necesidades y expectativas | Qué espera esta parte del SGI (calidad, cumplimiento legal, condiciones seguras, continuidad de suministro…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:41` |
| `next_review_date` | Date | Próxima revisión | Fecha de la próxima revisión, según la frecuencia. Se puede cambiar. |  |  | compute `_compute_next_review_date`, guardado |  | `addons/quimibond_sgi/models/sgi_context.py:66` |
| `party_type` | Selection | Tipo | Interna o externa a la empresa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:26` |
| `process_ids` | Many2many | Procesos relacionados | Procesos que atienden sus necesidades. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_context.py:53` |
| `requirement_note` | Text | Cómo se atiende | Con qué proceso, documento, requisito legal o control se responde a la expectativa adoptada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:49` |
| `review_frequency_months` | Integer | Frecuencia de revisión (meses) | Cada cuántos meses se revisan sus necesidades. El cron avisa cuando vence. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:61` |
| `risk_ids` | Many2many | Riesgos/oportunidades ligados | Riesgos u oportunidades (incluido FODA) que nacen de esta parte. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_context.py:55` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_reviewed` | Sella la revisión periódica del contexto (4.1/4.2). |
