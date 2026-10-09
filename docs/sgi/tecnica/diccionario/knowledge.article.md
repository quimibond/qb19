<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `knowledge.article`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_activity_label` | Char | Actividades | Actividades del SGI que usan este artículo como instructivo. |  |  | compute `_compute_sgi_activity_label`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:128` |
| `sgi_doc_type` | Selection | Tipo de documento | Tipo del documento controlado que explica este artículo. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:112` |
| `sgi_document_code` | Char | Clave del documento | Clave SGI del documento que explica este artículo (IT-C4-02). Sobrevive a las revisiones. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:106` |
| `sgi_document_id` | Many2one | Documento vigente | La revisión vigente del documento controlado con esta clave. |  | `documents.document` | compute `_compute_sgi_document_id`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:109` |
| `sgi_import_note` | Text | Nota de importación | Qué pasó al importar el PDF: texto extraído, páginas, actividades ligadas o sugeridas. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:125` |
| `sgi_kind` | Selection | Tipo en el SGI | Qué es este artículo para el SGI. Vacío: artículo ajeno al SGI. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:88` |
| `sgi_process_id` | Many2one | Proceso SGI | Proceso al que pertenece el artículo. |  | `sgi.process` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:103` |
| `sgi_publish_state` | Selection | Estado de publicación | Borrador: nunca publicado. Borrador del PDF vigente: la revisión vigente es el PDF y este artículo es su borrador importado. Publicado: la revisión vigente se congeló de este artículo. Cambió desde la publicación: el artículo cambió después de publicarse. |  |  | compute `_compute_sgi_publish_state`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:115` |
| `sgi_seed_hash` | Char | Huella de la siembra | Huella del texto que dejó la última siembra. Si el texto actual ya no coincide, alguien lo editó en Conocimiento y la siembra no lo vuelve a escribir. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:99` |
| `sgi_seed_key` | Char | Clave de siembra | Clave con que el sistema siembra y refresca este artículo (raiz, manual:mast, proceso:C4…). |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_article.py:96` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open` | — |
| `action_sgi_publish` | «Publicar» (Jefe MAST): abre el asistente de DOC-5 generalizado. |
| `action_sgi_request_publish` | «Pedir publicación» (dueño del proceso o quien puede escribir el borrador): una actividad para el Jefe MAST, sin duplicar. |
| `create` | — |
| `write` | — |
