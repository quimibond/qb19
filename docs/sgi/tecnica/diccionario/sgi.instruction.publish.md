<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.instruction.publish`

**Publicar artículo de Knowledge como documento controlado** (TransientModel).

Asistente que publica un artículo de Conocimiento como revisión nueva de un instructivo, control operacional, protocolo o reglamento, con acuses para los puestos.

Archivos: `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one |  | Actividad cuyo instructivo se publica (opcional). |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:114` |
| `article_id` | Many2one | Artículo | Artículo de Conocimiento que se publica como revisión del documento. |  | `knowledge.article` | compute `_compute_article_id`, guardado |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:116` |
| `code` | Char | Clave | Clave del documento, por ejemplo IT-C4-02 o CO-E2-01. |  |  | compute `_compute_code`, guardado |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:122` |
| `current_id` | Many2one | Revisión vigente | La revisión vigente con esta clave, que quedará obsoleta. |  | `documents.document` | compute `_compute_current_id`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:124` |
| `doc_type` | Selection | Tipo de documento | Tipo del documento controlado que se publica. |  |  | compute `_compute_doc_type`, guardado |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:119` |
| `job_ids` | Many2many | Puestos que aplican | Por omisión, los que ejecutan las actividades que usan el documento; si no hay, los del documento vigente; y en controles operacionales, protocolos y reglamentos, los de las actividades del proceso. |  | `hr.job` | compute `_compute_job_ids`, guardado |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:127` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_publish` | — |
