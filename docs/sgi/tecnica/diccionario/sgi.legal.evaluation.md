<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.legal.evaluation`

**Evaluación del cumplimiento de un requisito legal** (Model).

DIR-1 (51.0.0): cada evaluación del cumplimiento es un registro con resultado, evidencia y fecha de la siguiente (9.1.2: conservar evidencia).

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_legal.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | NC |  |  |  | related `requirement_id.alert_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_legal.py:251` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:241` |
| `evidence` | Text | Evidencia revisada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:248` |
| `next_date` | Date | Próxima evaluación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:249` |
| `requirement_id` | Many2one | Requisito |  | sí | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:239` |
| `result` | Selection | Resultado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legal.py:242` |
| `user_id` | Many2one | Evaluó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_legal.py:250` |

