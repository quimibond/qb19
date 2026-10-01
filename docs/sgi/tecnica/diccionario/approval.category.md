<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `approval.category`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_approval_native.py`, `addons/quimibond_sgi/models/sgi_doc_change.py`, `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_is_doc_change` | Boolean | Cambio documental SGI | Marque si esta categoría de aprobación es la de cambios a documentos del SGI. Al aprobarse, la solicitud versiona el documento y genera acuses. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:13` |
| `sgi_is_moc` | Boolean | Gestión del cambio SGI (MOC) | Cambios de proceso, infraestructura o plantilla (9001 §6.3, 45001 §8.1.3): la solicitud exige motivo, procesos afectados y evaluación de riesgos ANTES de poder aprobarse. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:17` |
| `sgi_is_mp_change` | Boolean | Cambio a «Mi procedimiento» (SGI) | Categoría que usa el botón «Proponer cambio» de Mi procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:51` |
| `sgi_role_id` | Many2one | Rol SGI que aprueba | Categoría creada para un renglón «Aprueba»: sus aprobadores siguen a las personas del puesto. |  | `sgi.activity.role` |  |  | `addons/quimibond_sgi/models/sgi_approval_native.py:157` |
| `sgi_sign_required` | Boolean | Se aprueba firmando en Sign | Al enviar la solicitud se crea la firma en Sign (elaboró → revisó → aprobó) y cada firma aprueba su renglón. El botón Aprobar de Aprobaciones queda bloqueado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:44` |

