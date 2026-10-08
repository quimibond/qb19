<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.escalation`

**Aviso por tiempo de un desarrollo** (Model).

Orden: `sent_at desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_escalation.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `close_reason` | Char | Por qué se cerró |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:86` |
| `closed_at` | Datetime | Cerrado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:85` |
| `company_id` | Many2one |  |  |  | `res.company` | related `project_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:87` |
| `hours` | Float | Horas de desarrollo al avisar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:81` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:80` |
| `level` | Selection | Nivel |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:78` |
| `mail_activity_id` | Many2one | Actividad |  |  | `mail.activity` |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:83` |
| `project_id` | Many2one | Desarrollo |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:74` |
| `sent_at` | Datetime | Avisado el |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:82` |
| `since` | Datetime | Pendiente desde |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:77` |
| `state` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:84` |
| `step_code` | Char | Paso |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:75` |
| `step_name` | Char | Qué estaba pendiente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:76` |
| `user_id` | Many2one | Avisado a |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_escalation.py:79` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `cron_sgi_dev_escalation` | Cada hora: por desarrollo abierto, el paso pendiente, sus horas de desarrollo y los avisos que ya tocan. Devuelve los avisos creados. |
