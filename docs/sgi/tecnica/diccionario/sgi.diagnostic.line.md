<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.diagnostic.line`

**Hallazgo del diagnóstico del SGI** (TransientModel).

Hallazgo de una corrida del diagnóstico, con cómo corregirlo.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_diagnostic.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `diagnostic_id` | Many2one | Diagnóstico | Corrida del diagnóstico a la que pertenece el hallazgo. | sí | `sgi.diagnostic` |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:31` |
| `fix` | Char | Dónde se arregla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:38` |
| `level` | Selection | Nivel | Qué tan grave es el hallazgo. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:36` |
| `section` | Char | Sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:35` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:34` |
| `text` | Text | Hallazgo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:37` |

