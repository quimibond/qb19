<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.management.review`

**Revisión por la Dirección (IT-P-A10-01)** (Model). Hereda de: `hr.mixin`, `sgi.base.mixin`.

Revisión por la dirección: carga de entradas (auditorías, NC, indicadores, quejas, riesgos…), acuerdos y cierre.

Orden: `date desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_management_review.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`.

## Campos (32)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `agreement_ids` | One2many | Acuerdos |  |  | `sgi.management.review.agreement` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:112` |
| `attendee_ids` | Many2many | Asistentes | Personas que asistieron a la revisión. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:40` |
| `audit_ids` | Many2many | Auditorías del periodo | Auditorías que cubre la revisión. «Cargar entradas» las toma del periodo; se pueden ajustar. |  | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:59` |
| `audit_summary` | Text | 4. Auditorías |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:55` |
| `carried_agreement_ids` | Many2many | Acuerdos abiertos de revisiones anteriores | Acuerdos de revisiones ya realizadas o cerradas que siguen sin cumplirse. Se cargan con «Cargar entradas»; cada uno sigue siendo de su revisión. |  | `sgi.management.review.agreement` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:116` |
| `complaints_summary` | Text | 3. Reclamaciones de clientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:54` |
| `conclusion_adequacy` | Text | Adecuación | ¿El SGI cubre lo que la empresa necesita (procesos, requisitos, partes interesadas)? Obligatoria para marcar la revisión como Realizada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:127` |
| `conclusion_effectiveness` | Text | Eficacia | ¿El SGI logra los resultados previstos (objetivos, indicadores, NC, incidentes)? Obligatoria para marcar la revisión como Realizada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:131` |
| `conclusion_suitability` | Text | Conveniencia | ¿El SGI sigue siendo conveniente para la empresa y su contexto? Obligatoria para marcar la revisión como Realizada; si no hay cambios, escríbalo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:123` |
| `context_summary` | Text | 16. Cambios en el contexto y las partes interesadas | 9.3.2 b: partes interesadas nuevas y revisadas, revisiones vencidas y cuestiones FODA nuevas o evaluadas en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:99` |
| `date` | Date | Fecha | Fecha de la reunión. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:36` |
| `doc_changes_summary` | Text | 10. Cambios documentales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:76` |
| `env_aspects_summary` | Text | 17. Aspectos ambientales significativos | 14001 9.3: aspectos significativos de la matriz, sin control operacional o en evaluación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:103` |
| `env_summary` | Text | 8. Desempeño ambiental (scrap) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:74` |
| `improvement_summary` | Text | 18. Oportunidades de mejora | 9.3.2 f: propuestas de Mejora Continua, oportunidades de la matriz de riesgos y de las auditorías del periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:106` |
| `incidents_summary` | Text | 15. Incidentes y desempeño de SST | 45001 9.3: incidentes del periodo por tipo y severidad, días perdidos, abiertos, IPER de riesgo alto sin acción y permisos de trabajo vencidos. Solo conteos, sin nombres. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:95` |
| `kpi_red_measure_ids` | Many2many | 5. Indicadores en rojo | Mediciones en rojo del periodo. Se llenan con «Cargar entradas». |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:63` |
| `legal_summary` | Text | 11. Cumplimiento legal | 14001/45001 9.3: estado de la evaluación del cumplimiento de requisitos legales y permisos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:77` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_management_review.py:35` |
| `nc_summary` | Text | 2. No conformidades |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:53` |
| `objectives_summary` | Text | 13. Objetivos e indicadores | Objetivos integrales con sus indicadores oficiales: último valor, semáforo y rojos sin plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:86` |
| `output_needs` | Text | Mejora, cambios y recursos | Decisiones sobre oportunidades de mejora, cambios al SGI y recursos que se necesitan (9.3.3 a, b y c). Obligatoria para marcar la revisión como Realizada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:135` |
| `participation_summary` | Text | 12. Consulta y participación | 45001 5.4/9.3: respuestas de la encuesta de consulta y participación y quejas del canal interno en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:81` |
| `period_from` | Date | Periodo desde | Inicio del periodo que se revisa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:38` |
| `period_to` | Date | Periodo hasta | Fin del periodo que se revisa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:39` |
| `prev_agreements_summary` | Text | 1. Acuerdos previos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:52` |
| `resources_note` | Text | 9. Recursos (calibraciones/capacitación) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:75` |
| `risk_high_ids` | Many2many | 7. Riesgos de atención inmediata/alta | Riesgos de atención inmediata o alta. Se llenan con «Cargar entradas». |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:69` |
| `satisfaction_summary` | Text | 14. Satisfacción del cliente | Indicador CA-02 y reclamaciones del periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:90` |
| `sgi_heading` | Char | Título |  |  |  | compute `_compute_sgi_heading`, sin guardar |  | `addons/quimibond_sgi/models/sgi_management_review.py:142` |
| `state` | Selection | Estado | Borrador mientras se prepara; realizada al marcarla hecha (sus acuerdos pasan a acciones); cerrada por el Jefe MAST y SGI. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:43` |
| `supplier_summary` | Text | 6. Proveedores |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:68` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_draft` | — |
| `action_load_inputs` | — |
| `action_mark_done` | — |
| `action_validate_measures` | P-40: valida las mediciones capturadas del periodo y abre los rojos que aún no tienen causa ni acción. |
| `sgi_minutes_filename` | 57.101.0 (B3): nombre de la copia guardada del acta cerrada (la expresión ``attachment`` del reporte la llama). Es el mismo nombre que el de la descarga (``print_report_name``) más «.pdf». |
