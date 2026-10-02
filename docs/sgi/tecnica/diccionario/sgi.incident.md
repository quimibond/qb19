<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.incident`

**Incidente / Accidente SST (P-S02, SCAT)** (Model). Hereda de: `hr.mixin`, `sgi.base.mixin`, `sgi.pin.signature.mixin`.

Incidente o accidente de SST (P-S02) con análisis SCAT (causas inmediatas, básicas y falta de control). Todos reportan; SST, MAST y Salud ocupacional investigan y cierran.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_incident.py`.

## Campos (29)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:79` |
| `basic_causes` | Text | Causas básicas (factores personales/de trabajo) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:73` |
| `date` | Datetime | Fecha y hora | Fecha y hora en que ocurrió. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:35` |
| `days_lost` | Integer | Días perdidos | Días de trabajo perdidos por el evento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:69` |
| `description` | Text | Descripción del evento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:68` |
| `employee_ids` | Many2many | Personas afectadas | Personas afectadas. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:53` |
| `immediate_causes` | Text | Causas inmediatas (actos/condiciones) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:72` |
| `incident_type` | Selection | Tipo | Lesión, casi accidente, daño a la propiedad, incidente ambiental o enfermedad laboral. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:38` |
| `investigation_team_ids` | Many2many | Equipo de investigación | Quiénes investigaron el evento. Para cerrar: al menos un trabajador sin personal a su cargo o un integrante de la Comisión de Seguridad e Higiene (ISO 45001 5.4). |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:84` |
| `lack_of_control` | Text | Falta de control (sistema de gestión) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:74` |
| `location` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:63` |
| `name` | Char | Título |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:34` |
| `process_id` | Many2one | Proceso | Proceso donde ocurrió. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:64` |
| `reporter_employee_id` | Many2one | Reportado por (empleado) | Empleado que reporta. Desde la tableta de planta queda el de quien tecleó su PIN. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:59` |
| `reporter_id` | Many2one | Reportado por | Persona que reporta. Puede consultar cómo se cerró. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:54` |
| `risk_id` | Many2one | Riesgo / IPER relacionado | Riesgo de la matriz IPER relacionado con el evento. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:76` |
| `severity` | Selection | Severidad | Leve, moderado, grave o fatal. Los graves y fatales avisan de inmediato. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:46` |
| `sgi_alert_id` | Many2one | No conformidad generada | No conformidad generada desde el incidente. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:80` |
| `sgi_area_id` | Many2one | Área SGI | Área del SGI donde ocurrió. |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:66` |
| `sgi_effective` | Selection | Resultado de la eficacia | ¿Las acciones evitaron que se repita? El incidente solo cierra con «Eficaz». «No eficaz» lo regresa a Acciones y pide una acción nueva. |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:89` |
| `sgi_effectiveness_by` | Many2one | Verificó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:97` |
| `sgi_effectiveness_date` | Date | Fecha de la verificación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:95` |
| `sgi_effectiveness_note` | Text | Qué se verificó |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:96` |
| `sgi_from_leave` | Boolean | Desde una incapacidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:102` |
| `sgi_ineffective_count` | Integer | Verificaciones no eficaces | Veces que la verificación salió «No eficaz». |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:98` |
| `sgi_leave_count` | Integer | Incapacidades | Incapacidades por riesgo de trabajo aprobadas ligadas. |  |  | compute `_compute_sgi_leave`, sin guardar |  | `addons/quimibond_sgi/models/sgi_incident.py:103` |
| `sgi_leave_days` | Float | Días de incapacidad |  |  |  | compute `_compute_sgi_leave`, sin guardar |  | `addons/quimibond_sgi/models/sgi_incident.py:105` |
| `sgi_user_can_investigate` | Boolean |  | Usted es Jefe MAST o de Salud ocupacional: puede cambiar quién reportó. |  |  | compute `_compute_sgi_user_can_investigate`, sin guardar |  | `addons/quimibond_sgi/models/sgi_incident.py:143` |
| `state` | Selection | Estado | Reportado, en investigación, acciones o cerrado. No se cierra sin el análisis SCAT ni con acciones abiertas. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:107` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_acciones` | — |
| `action_set_cerrado` | — |
| `action_set_investigacion` | — |
| `action_set_reportado` | — |
| `create` | — |
| `write` | Reclasificar la severidad a grave/fatal FUERZA la NC y el aviso, igual que en el alta: un incidente que entró leve/moderado y la investigación eleva a grave/fatal no puede quedarse sin su NC. Se apoy… |
