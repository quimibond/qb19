<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.env.aspect.transfer.line`

**Riesgo ambiental por traspasar a la matriz** (TransientModel).

Un riesgo ambiental por traspasar y lo que el Jefe MAST decide del aspecto.

Archivos: `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity` | Char | Actividad u operación | Qué se hace. Propuesta: el nombre del riesgo; corríjala. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:167` |
| `aspect_type` | Selection | Tipo de aspecto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:169` |
| `condition` | Selection | Condición |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:170` |
| `error` | Char | Por qué no se traspasó | Lo llena «Traspasar a la matriz» si este renglón falló. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:172` |
| `life_cycle_stage` | Selection | Etapa del ciclo de vida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:171` |
| `process_id` | Many2one | Proceso |  |  |  | related `risk_id.process_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:165` |
| `risk_id` | Many2one | Riesgo |  | sí | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:163` |
| `risk_kind` | Selection | Riesgo u oportunidad |  |  |  | related `risk_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:164` |
| `score` | Integer | Puntaje del riesgo |  |  |  | related `risk_id.score`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:166` |
| `wizard_id` | Many2one |  |  | sí | `sgi.env.aspect.transfer` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:162` |

