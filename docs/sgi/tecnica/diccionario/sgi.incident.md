<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.incident`

**Incidente / Accidente SST (P-S02, SCAT)** (Model). Hereda de: `sgi.base.mixin`.

Incidente o accidente de SST (P-S02) con análisis SCAT (causas inmediatas, básicas y falta de control). Todos reportan; SST, MAST y Salud ocupacional investigan y cierran.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_incident.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:60` |
| `basic_causes` | Text | Causas básicas (factores personales/de trabajo) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:55` |
| `date` | Datetime | Fecha y hora |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:29` |
| `days_lost` | Integer | Días perdidos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:51` |
| `description` | Text | Descripción del evento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:50` |
| `employee_ids` | Many2many | Personas afectadas |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:44` |
| `immediate_causes` | Text | Causas inmediatas (actos/condiciones) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:54` |
| `incident_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:31` |
| `lack_of_control` | Text | Falta de control (sistema de gestión) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:56` |
| `location` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:47` |
| `name` | Char | Título |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:28` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:48` |
| `reporter_id` | Many2one | Reportado por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:45` |
| `risk_id` | Many2one | Riesgo / IPER relacionado |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:58` |
| `severity` | Selection | Severidad |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:38` |
| `sgi_alert_id` | Many2one | No Conformidad generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:61` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:49` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_incident.py:64` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_acciones` | — |
| `action_set_cerrado` | — |
| `action_set_investigacion` | — |
| `action_set_reportado` | — |
| `create` | — |
| `write` | Reclasificar la severidad a grave/fatal FUERZA la NC y el aviso, igual que en el alta: un incidente que entró leve/moderado y la investigación eleva a grave/fatal no puede quedarse sin su NC. Se apoy… |
