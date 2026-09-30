<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.diagnostic.line`

**Hallazgo del diagnóstico del SGI** (TransientModel).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_diagnostic.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `diagnostic_id` | Many2one | Diagnóstico |  | sí | `sgi.diagnostic` |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:30` |
| `fix` | Char | Dónde se arregla |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:36` |
| `level` | Selection | Nivel |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:34` |
| `section` | Char | Sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:33` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:32` |
| `text` | Text | Hallazgo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:35` |

