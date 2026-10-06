<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.base.mixin`

**Cimiento de registros del SGI** (AbstractModel). Hereda de: `mail.activity.mixin`, `mail.thread`.

Cimiento de los registros del SGI: chatter y actividades, folio con secuencia propia (``_sgi_sequence_code``) y agenda de actividades con ``_sgi_schedule_activity``.

Archivos: `addons/quimibond_sgi/models/sgi_base.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `folio` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_base.py:49` |
| `sgi_is_locked` | Boolean | Cerrado (solo lectura) | El registro está cerrado y es evidencia: solo el Jefe MAST lo edita. |  |  | compute `_compute_sgi_is_locked`, sin guardar |  | `addons/quimibond_sgi/models/sgi_base.py:53` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | Cinturón del candado: un registro cerrado tampoco se borra. Hoy los ACL ya niegan unlink a los usuarios en todos los modelos con estados bloqueados, pero la regla no debe depender de que ningún ACL f… |
| `write` | — |
