<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.management.review`

**Revisión por la Dirección (IT-P-A10-01)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_management_review.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`.

## Campos (22)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `agreement_ids` | One2many | Acuerdos |  |  | `sgi.management.review.agreement` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:75` |
| `attendee_ids` | Many2many | Asistentes |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:28` |
| `audit_ids` | Many2many | Auditorías del periodo |  |  | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:44` |
| `audit_summary` | Text | 4. Auditorías |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:40` |
| `complaints_summary` | Text | 3. Reclamaciones de clientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:39` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:25` |
| `doc_changes_summary` | Text | 10. Cambios documentales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:56` |
| `env_summary` | Text | 8. Desempeño ambiental (scrap) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:54` |
| `kpi_red_measure_ids` | Many2many | 5. Indicadores en rojo |  |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:47` |
| `legal_summary` | Text | 11. Cumplimiento legal | 14001/45001 9.3: estado de la evaluación del cumplimiento de requisitos legales y permisos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:57` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_management_review.py:24` |
| `nc_summary` | Text | 2. No conformidades |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:38` |
| `objectives_summary` | Text | 13. Objetivos e indicadores | Objetivos integrales con sus indicadores oficiales: último valor, semáforo y rojos sin plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:66` |
| `participation_summary` | Text | 12. Consulta y participación | 45001 5.4/9.3: respuestas de la encuesta de consulta y participación y quejas del canal interno en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:61` |
| `period_from` | Date | Periodo desde |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:26` |
| `period_to` | Date | Periodo hasta |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:27` |
| `prev_agreements_summary` | Text | 1. Acuerdos previos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:37` |
| `resources_note` | Text | 9. Recursos (calibraciones/capacitación) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:55` |
| `risk_high_ids` | Many2many | 7. Riesgos de atención inmediata/alta |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:51` |
| `satisfaction_summary` | Text | 14. Satisfacción del cliente | Indicador CA-02 y reclamaciones del periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:70` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:30` |
| `supplier_summary` | Text | 6. Proveedores |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:50` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_draft` | — |
| `action_load_inputs` | — |
| `action_mark_done` | — |
| `action_validate_measures` | P-40: valida las mediciones capturadas del periodo y abre los rojos que aún no tienen causa ni acción. |
