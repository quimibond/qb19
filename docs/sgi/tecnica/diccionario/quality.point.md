<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `quality.point`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_control_plan.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_characteristic` | Char | Característica | Característica de la Master Spec del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:35` |
| `sgi_control_plan_id` | Many2one | Plan de control |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:11` |
| `sgi_cpk_target` | Float | Cpk objetivo | Cpk objetivo sugerido: 1.33 para F, 1.67 para R/S. |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:46` |
| `sgi_criticality` | Selection | Criticidad (F/R/S) | Esquema de criticidad tipo Continental: F=Función, R=Regulación, S=Seguridad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:37` |
| `sgi_equipment_id` | Many2one | Equipo de medición | Instrumento con el que se mide esta característica. Su calibración se verifica al registrar la inspección (IATF 7.1.5.2.1). |  | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:49` |
| `sgi_in_coa` | Boolean | Aparece en el Certificado de Calidad | Si está marcado, esta característica se imprime en el Certificado de Conformidad (CoA) del lote. |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:43` |
| `sgi_reaction_plan` | Text | Plan de reacción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:48` |
| `sgi_replaced_document_count` | Integer | # Formatos sustituidos |  |  |  | compute `_compute_sgi_replaced_document_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_control_plan.py:18` |
| `sgi_replaced_document_ids` | One2many | Formatos que sustituye |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:15` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open_replaced_documents` | — |
