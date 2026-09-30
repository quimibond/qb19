<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.incident`

**Incidente / Accidente SST (P-S02, SCAT)** (Model). Hereda de: `hr.mixin`, `sgi.base.mixin`.

Incidente o accidente de SST (P-S02) con análisis SCAT (causas inmediatas, básicas y falta de control). Todos reportan; SST, MAST y Salud ocupacional investigan y cierran.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_incident.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:70` |
| `basic_causes` | Text | Causas básicas (factores personales/de trabajo) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:64` |
| `date` | Datetime | Fecha y hora | Fecha y hora en que ocurrió. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:32` |
| `days_lost` | Integer | Días perdidos | Días de trabajo perdidos por el evento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:60` |
| `description` | Text | Descripción del evento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:59` |
| `employee_ids` | Many2many | Personas afectadas | Personas afectadas. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:50` |
| `immediate_causes` | Text | Causas inmediatas (actos/condiciones) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:63` |
| `incident_type` | Selection | Tipo | Lesión, casi accidente, daño a la propiedad, incidente ambiental o enfermedad laboral. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:35` |
| `lack_of_control` | Text | Falta de control (sistema de gestión) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:65` |
| `location` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:54` |
| `name` | Char | Título |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:31` |
| `process_id` | Many2one | Proceso | Proceso donde ocurrió. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:55` |
| `reporter_id` | Many2one | Reportado por | Persona que reporta. Puede consultar cómo se cerró. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:51` |
| `risk_id` | Many2one | Riesgo / IPER relacionado | Riesgo de la matriz IPER relacionado con el evento. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:67` |
| `severity` | Selection | Severidad | Leve, moderado, grave o fatal. Los graves y fatales avisan de inmediato. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:43` |
| `sgi_alert_id` | Many2one | No Conformidad generada | No conformidad generada desde el incidente. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:71` |
| `sgi_area_id` | Many2one | Área SGI | Área del SGI donde ocurrió. |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:57` |
| `state` | Selection | Estado | Reportado, en investigación, acciones o cerrado. No se cierra sin el análisis SCAT ni con acciones abiertas. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:75` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_acciones` | — |
| `action_set_cerrado` | — |
| `action_set_investigacion` | — |
| `action_set_reportado` | — |
| `create` | — |
| `write` | Reclasificar la severidad a grave/fatal FUERZA la NC y el aviso, igual que en el alta: un incidente que entró leve/moderado y la investigación eleva a grave/fatal no puede quedarse sin su NC. Se apoy… |
