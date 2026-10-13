<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.required.doc`

**Información requerida al cliente en el desarrollo** (Model).

57.152.0: información que el cliente pide o debe entregar (formato anterior F-P-D01-09 / 19, bloque «Información requerida»): un renglón por documento del catálogo con su momento.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_start_approval.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `moment` | Selection | Cuándo | Si el cliente lo pide para el diseño y desarrollo, si se entrega cuando el proyecto sea factible o si no aplica. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:164` |
| `note` | Char | Observación | Texto libre; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:167` |
| `option_id` | Many2one | Documento | Documento del catálogo «Información requerida al cliente». | sí | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:161` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:159` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:160` |

