<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.doc.review`

**Revisar un documento controlado** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_pending_actions.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `decision` | Selection | Resultado de la revisión |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:122` |
| `document_id` | Many2one | Documento |  | sí | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:121` |
| `next_review_date` | Date | Próxima revisión |  |  |  | compute `_compute_next_review_date`, sin guardar |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:129` |
| `note` | Text | Qué se revisó | Con quién y contra qué se revisó (práctica actual, norma, requisito). Queda en el historial del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:126` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
