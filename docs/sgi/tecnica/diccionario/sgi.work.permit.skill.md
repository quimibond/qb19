<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.work.permit.skill`

**Competencia requerida por tipo de permiso de trabajo** (Model).

57.96.0 (N-06, 45001 7.2): competencia exigida por tipo de permiso.

Orden: `work_type, id`.

Archivos: `addons/quimibond_sgi/models/sgi_work_permit.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:413` |
| `note` | Char | Por qué se exige | NOM o procedimiento: NOM-009-STPS, DC-3… |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:412` |
| `skill_id` | Many2one | Competencia | Competencia que cada persona que ejecuta debe tener vigente hasta el fin del permiso. | sí | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:408` |
| `skill_type_id` | Many2one | Tipo de competencia |  |  |  | related `skill_id.skill_type_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_work_permit.py:411` |
| `work_type` | Selection | Tipo de trabajo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:407` |

