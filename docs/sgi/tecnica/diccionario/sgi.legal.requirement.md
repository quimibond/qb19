<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.legal.requirement`

**Requisito legal / otro requisito (14001·45001 6.1.3)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Requisito legal u otro requisito (14001/45001 6.1.3): autoridad, evidencia, evaluaciones periódicas y vencimiento de permisos.

Orden: `compliance_state desc, next_eval_date, id`.

Archivos: `addons/quimibond_sgi/models/sgi_legal.py`.

## Campos (21)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:112` |
| `alert_id` | Many2one | NC de incumplimiento | NC levantada por incumplimiento del requisito. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:109` |
| `authority` | Char | Autoridad / origen | Quién lo exige: SEMARNAT, STPS, municipio, cliente, corporativo… |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:53` |
| `compliance_state` | Selection | Cumplimiento | Resultado de la última evaluación de cumplimiento. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:97` |
| `description` | Text | Obligación | Qué obliga a hacer exactamente (la parte aplicable a la planta). |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:56` |
| `document_ids` | Many2many | Documentos de evidencia | Documentos controlados que prueban el cumplimiento. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:69` |
| `eval_frequency_months` | Integer | Frecuencia de evaluación (meses) | Cada cuántos meses se evalúa el cumplimiento. El cron avisa cuando vence. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:87` |
| `eval_note` | Text | Notas de la última evaluación | Qué se revisó y qué se encontró (queda también en el chatter por el tracking del estado). |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:105` |
| `evaluation_count` | Integer |  |  |  |  | compute `_compute_evaluation_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_legal.py:80` |
| `evaluation_ids` | One2many | Evaluaciones |  |  | `sgi.legal.evaluation` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:78` |
| `evidence` | Text | Cómo se cumple / evidencia | Con qué se demuestra el cumplimiento: registro, bitácora, dictamen, constancia, documento controlado… |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:59` |
| `expiry_date` | Date | Vencimiento del permiso | Solo permisos/licencias con vigencia: fecha en que caduca el instrumento (independiente de la evaluación de cumplimiento). |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:81` |
| `kind` | Selection | Tipo | Ley o reglamento, norma oficial mexicana, permiso o licencia, requisito de cliente u otro. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:34` |
| `last_eval_date` | Date | Última evaluación | Fecha de la última evaluación de cumplimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:90` |
| `name` | Char | Requisito | La obligación concreta, en lenguaje llano (ej. «Registro como generador de residuos peligrosos», «Programa interno de protección civil vigente»). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:29` |
| `next_eval_date` | Date | Próxima evaluación | Última + frecuencia; puede fijarse a mano (prevalece, patrón de calibraciones). |  |  | compute `_compute_next_eval_date`, guardado |  | `addons/quimibond_sgi/models/sgi_legal.py:92` |
| `process_ids` | Many2many | Procesos donde aplica | Procesos a los que aplica el requisito. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:63` |
| `reference` | Char | Referencia | Instrumento y artículo/numeral (ej. NOM-052-SEMARNAT-2005, art. 42; NOM-002-STPS-2010). |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:49` |
| `responsible_id` | Many2one | Responsable de la evaluación | Persona que evalúa el cumplimiento y recibe los avisos. | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:74` |
| `risk_ids` | Many2many | Riesgos ligados | Riesgos (IPER/ambiental) cuyo control responde a este requisito. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:66` |
| `system` | Selection | Sistema | Norma a la que corresponde: ambiental, seguridad y salud, calidad o transversal. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:42` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_evaluate` | DIR-1: asistente con resultado, evidencia y próxima fecha. |
| `action_mark_cumple` | — |
| `action_mark_no_aplica` | — |
| `action_mark_no_cumple` | — |
| `action_mark_parcial` | — |
| `action_view_alert` | — |
| `action_view_evaluations` | — |
