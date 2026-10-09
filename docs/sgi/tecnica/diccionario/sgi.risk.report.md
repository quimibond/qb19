<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.risk.report`

**Reportar un riesgo u oportunidad** (TransientModel).

Reportar un riesgo u oportunidad con cinco preguntas.

Archivos: `addons/quimibond_sgi/models/sgi_risk_report.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `consequence` | Text | ¿Qué pasaría si pasa? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:49` |
| `existing_controls` | Text | ¿Qué se hace hoy? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:55` |
| `how_bad` | Selection | ¿Qué tan grave sería? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:53` |
| `how_good` | Selection | ¿Qué tanto ganaríamos? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:54` |
| `how_often` | Selection | ¿Qué tan seguido? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:52` |
| `kind` | Selection | ¿Qué quiere reportar? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:44` |
| `name` | Char | ¿Qué puede pasar? |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:48` |
| `preview_html` | Html | Cómo queda |  |  |  | compute `_compute_preview_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk_report.py:56` |
| `process_id` | Many2one | ¿En qué proceso? |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_risk_report.py:50` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_report` | — |
