<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.work.permit.check`

**Verificación o EPP del permiso de trabajo** (Model).

Orden: `permit_id, category desc, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_work_permit.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `answer` | Selection | Respuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:294` |
| `category` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:289` |
| `name` | Char | Punto a verificar |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:293` |
| `note` | Char | Observación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:299` |
| `permit_id` | Many2one | Permiso |  | sí | `sgi.work.permit` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:286` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:288` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
