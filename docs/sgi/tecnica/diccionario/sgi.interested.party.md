<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.interested.party`

**Parte interesada (ISO 4.2)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `party_type, category, name`.

Archivos: `addons/quimibond_sgi/models/sgi_context.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:61` |
| `becomes_requirement` | Boolean | Se adopta como requisito del SGI | La organización DECIDE cuáles necesidades se vuelven requisito (4.2): márcalo y documenta cómo se atiende. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:41` |
| `category` | Selection | Categoría |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:28` |
| `last_review_date` | Date | Última revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:57` |
| `legal_ids` | Many2many | Requisitos legales ligados |  |  | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_context.py:53` |
| `name` | Char | Parte interesada |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:23` |
| `needs` | Text | Necesidades y expectativas | Qué espera esta parte del SGI (calidad, cumplimiento legal, condiciones seguras, continuidad de suministro…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:37` |
| `next_review_date` | Date | Próxima revisión |  |  |  | compute `_compute_next_review_date`, guardado |  | `addons/quimibond_sgi/models/sgi_context.py:58` |
| `party_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:24` |
| `process_ids` | Many2many | Procesos relacionados |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_context.py:49` |
| `requirement_note` | Text | Cómo se atiende | Con qué proceso, documento, requisito legal o control se responde a la expectativa adoptada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:45` |
| `review_frequency_months` | Integer | Frecuencia de revisión (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_context.py:55` |
| `risk_ids` | Many2many | Riesgos/oportunidades ligados | Riesgos u oportunidades (incluido FODA) que nacen de esta parte. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_context.py:50` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_reviewed` | Sella la revisión periódica del contexto (4.1/4.2). |
