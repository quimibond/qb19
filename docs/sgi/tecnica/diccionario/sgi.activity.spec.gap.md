<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.spec.gap`

**Faltante de especificación de una actividad SGI** (Model).

Faltante de especificación de una actividad (sin ejecutor, sin entregable, verbo vago…). Se recalcula; alimenta Diagnóstico → Faltantes de especificación.

Orden: `severity, code, activity_id`.

Archivos: `addons/quimibond_sgi/models/sgi_activity_spec.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad | Actividad a la que le falta especificación. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:150` |
| `code` | Selection | Faltante | Qué le falta a la actividad. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:157` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:165` |
| `message` | Char | Detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:164` |
| `process_id` | Many2one | Proceso | Proceso de la actividad. |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:154` |
| `severity` | Selection | Severidad | Un error impide publicar el procedimiento; una advertencia no. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:159` |

