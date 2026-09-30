<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.management.review`

**Revisión por la Dirección (IT-P-A10-01)** (Model). Hereda de: `sgi.base.mixin`.

Revisión por la dirección: carga de entradas (auditorías, NC, indicadores, quejas, riesgos…), acuerdos y cierre.

Orden: `date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_management_review.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`.

## Campos (23)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `agreement_ids` | One2many | Acuerdos |  |  | `sgi.management.review.agreement` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:86` |
| `attendee_ids` | Many2many | Asistentes | Personas que asistieron a la revisión. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:31` |
| `audit_ids` | Many2many | Auditorías del periodo | Auditorías que cubre la revisión. «Cargar entradas» las toma del periodo; se pueden ajustar. |  | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:50` |
| `audit_summary` | Text | 4. Auditorías |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:46` |
| `complaints_summary` | Text | 3. Reclamaciones de clientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:45` |
| `date` | Date | Fecha | Fecha de la reunión. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:27` |
| `doc_changes_summary` | Text | 10. Cambios documentales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:67` |
| `env_summary` | Text | 8. Desempeño ambiental (scrap) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:65` |
| `kpi_red_measure_ids` | Many2many | 5. Indicadores en rojo | Mediciones en rojo del periodo. Se llenan con «Cargar entradas». |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:54` |
| `legal_summary` | Text | 11. Cumplimiento legal | 14001/45001 9.3: estado de la evaluación del cumplimiento de requisitos legales y permisos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:68` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_management_review.py:26` |
| `nc_summary` | Text | 2. No conformidades |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:44` |
| `objectives_summary` | Text | 13. Objetivos e indicadores | Objetivos integrales con sus indicadores oficiales: último valor, semáforo y rojos sin plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:77` |
| `participation_summary` | Text | 12. Consulta y participación | 45001 5.4/9.3: respuestas de la encuesta de consulta y participación y quejas del canal interno en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:72` |
| `period_from` | Date | Periodo desde | Inicio del periodo que se revisa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:29` |
| `period_to` | Date | Periodo hasta | Fin del periodo que se revisa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:30` |
| `prev_agreements_summary` | Text | 1. Acuerdos previos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:43` |
| `resources_note` | Text | 9. Recursos (calibraciones/capacitación) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:66` |
| `risk_high_ids` | Many2many | 7. Riesgos de atención inmediata/alta | Riesgos de atención inmediata o alta. Se llenan con «Cargar entradas». |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:60` |
| `satisfaction_summary` | Text | 14. Satisfacción del cliente | Indicador CA-02 y reclamaciones del periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:81` |
| `sgi_heading` | Char | Título |  |  |  | compute `_compute_sgi_heading`, sin guardar |  | `addons/quimibond_sgi/models/sgi_management_review.py:91` |
| `state` | Selection | Estado | Borrador mientras se prepara; realizada al marcarla hecha (sus acuerdos pasan a acciones); cerrada por el Jefe MAST y SGI. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:34` |
| `supplier_summary` | Text | 6. Proveedores |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:59` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_draft` | — |
| `action_load_inputs` | — |
| `action_mark_done` | — |
| `action_validate_measures` | P-40: valida las mediciones capturadas del periodo y abre los rojos que aún no tienen causa ni acción. |
