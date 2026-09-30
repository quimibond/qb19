<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.instruction.publish`

**Publicar artículo de Knowledge como instructivo (IT)** (TransientModel).

Archivos: `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one |  |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:63` |
| `article_id` | Many2one |  |  |  |  | related `activity_id.instruction_article_id`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:64` |
| `code` | Char | Clave IT | Ej. IT-P-C11-05. | sí |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:65` |
| `job_ids` | Many2many | Puestos que aplican | Por omisión, los que ejecutan la actividad. |  | `hr.job` | compute `_compute_job_ids`, guardado |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:66` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_publish` | — |
