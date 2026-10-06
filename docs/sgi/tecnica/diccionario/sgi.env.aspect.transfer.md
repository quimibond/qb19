<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.env.aspect.transfer`

**Traspaso de riesgos ambientales a la matriz de aspectos** (TransientModel).

Traspaso de riesgos ambientales a la matriz de aspectos (N-07).

Archivos: `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `archive_risks` | Boolean | Archivar los riesgos originales | Apagado: cada riesgo se conserva, ligado al aspecto como su tratamiento. Encendido: además se archiva (no se borra). Úselo solo si Dirección lo pidió. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:33` |
| `done_count` | Integer | Aspectos creados |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:37` |
| `failed_count` | Integer | Riesgos que no se pudieron traspasar | El motivo de cada uno está en el log del servidor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:38` |
| `line_ids` | One2many | Riesgos ambientales sin aspecto |  |  | `sgi.env.aspect.transfer.line` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:30` |
| `risk_count` | Integer | Riesgos por traspasar |  |  |  | compute `_compute_risk_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py:32` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | — |
| `action_view_aspects` | — |
| `create` | — |
| `default_get` | — |
