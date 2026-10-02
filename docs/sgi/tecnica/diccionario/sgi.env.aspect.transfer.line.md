<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.env.aspect.transfer.line`

**Riesgo ambiental por traspasar a la matriz** (TransientModel).

Un riesgo ambiental por traspasar y lo que el Jefe MAST decide del aspecto.

Archivos: `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity` | Char | Actividad u operación | Qué se hace. Propuesta: el nombre del riesgo; corríjala. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:163` |
| `aspect_type` | Selection | Tipo de aspecto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:165` |
| `condition` | Selection | Condición |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:166` |
| `life_cycle_stage` | Selection | Etapa del ciclo de vida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:167` |
| `process_id` | Many2one | Proceso |  |  |  | related `risk_id.process_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:161` |
| `risk_id` | Many2one | Riesgo |  | sí | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:159` |
| `risk_kind` | Selection | Riesgo u oportunidad |  |  |  | related `risk_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:160` |
| `score` | Integer | Puntaje del riesgo |  |  |  | related `risk_id.score`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:162` |
| `wizard_id` | Many2one |  |  | sí | `sgi.env.aspect.transfer` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:158` |

