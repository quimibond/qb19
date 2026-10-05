<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.miid.row.note`

**Nota de renglón del MIID** (Model).

Nota fija de un renglón de un bloque vivo del MIID (la «Situación» de cada anexo en 12.1). La edita el Jefe MAST.

Orden: `section_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_miid.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `company_id` | Many2one |  |  |  |  | related `section_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_miid.py:265` |
| `key` | Char | Renglón | Clave del renglón al que se pega la nota (p. ej. ANEXO 9). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:267` |
| `section_id` | Many2one | Sección del MIID |  | sí | `sgi.miid.section` |  |  | `addons/quimibond_sgi/models/sgi_miid.py:263` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:266` |
| `text` | Char | Nota |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:269` |

