<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.knowledge.import`

**Importar documentos del SGI a Conocimiento** (TransientModel).

Importa a Conocimiento los instructivos, controles operacionales, protocolos y reglamentos vigentes, como borradores para que el dueño del proceso los corrija y el Jefe MAST los publique.

Archivos: `addons/quimibond_sgi_knowledge/models/sgi_knowledge_import.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `batch_size` | Integer | Documentos por lote | Cuántos documentos nuevos se importan en cada corrida. Vuelva a pulsar «Importar» para el siguiente lote. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_import.py:70` |
| `document_ids` | Many2many | Documentos | Documentos controlados vigentes que se importan cuando elige «Los que elija». |  | `documents.document` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_import.py:65` |
| `preset` | Selection | Qué importar | Por dónde empezar: los cinco controles operacionales y los instructivos de C4, todos, o la selección de abajo. | sí |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_import.py:58` |
| `summary` | Text | Resultado | Qué pasó con cada documento en la última corrida. |  |  |  |  | `addons/quimibond_sgi_knowledge/models/sgi_knowledge_import.py:74` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_import` | — |
| `default_get` | — |
