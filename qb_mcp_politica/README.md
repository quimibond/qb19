# qb_mcp_politica — Política del MCP

Candado en código sobre el conector MCP de Odoo (`mcp_server`, módulo de un
tercero que no se modifica). Auditoría del SGI, hallazgo **F-001**
(`docs/audit/06-seguridad.md` §5, opción B; decisión de Jose en
`docs/audit/decisiones.md`).

## Qué hace

`mcp_server` decide por modelo qué puede hacer el MCP con las casillas de
Ajustes → Técnico → MCP → *MCP Available Models*. Este módulo pone una regla
encima, para **todos los usuarios del MCP** (Jose, Jacobo, Sistemas):

- **Solo lectura** en los modelos técnicos: se pueden leer, pero no crear,
  modificar, borrar ni llamarles métodos (`call_model_method`), aunque la
  casilla diga lo contrario.
- **Desactivados**: el MCP no los ve ni para leer (tampoco sus campos).
- En pantalla, no deja marcar escritura en un modelo protegido ni activar uno
  desactivado (mensaje en español).

Todo lo demás queda como lo diga la casilla: `sgi.*` y los modelos de negocio
de la carga (`documents.*`, `approval.*`, `fleet.*`, `maintenance.*`, `hr.*`,
`quality.*`, `mail.activity`, `mail.message`…) conservan su escritura.

- **Sin borrado en el SGI** (decisión D-22, 19.0.1.1.0): `delete_record` (y
  cualquier `unlink` por XML-RPC o `call_model_method`) sobre un modelo `sgi.*`
  se rechaza aunque la casilla «Allow Delete» esté marcada. La herramienta
  contesta «archívalo (active=False)»; se archiva con `update_record`.

Las listas viven en un solo lugar: `models/politica.py`.

### Solo lectura

- Todo `ir.*` (`ir.cron`, `ir.ui.menu`, `ir.ui.view`, `ir.actions.*`,
  `ir.model*`, `ir.module.module`, `ir.filters`, `ir.default`, `ir.rule`,
  `ir.model.access`, `ir.model.data`…), **salvo `ir.attachment`**: los
  adjuntos son datos de negocio (documentos y mensajes del SGI), no
  configuración (`MODELOS_EXCEPTUADOS` en `models/politica.py`).
- `res.groups`, `res.groups.*` (`res.groups.privilege`), `res.users`,
  `res.users.*` (`res.users.settings`, `.settings.embedded.action`,
  `.settings.volumes`…), `res.company`, `mail.template`.
- `studio.*`, `spreadsheet.dashboard`, `spreadsheet.dashboard.*`,
  `spreadsheet.template`, `spreadsheet.mixin`, `web_tour.*`, `iap.*`,
  `auth.*` / `auth_*`.

### Desactivados (ni leer)

`ir.config_parameter`, `res.users.apikeys`, `res.users.apikeys.show`,
`auth_totp.device`, `auth.oauth.provider`, `fetchmail.server`, `iap.account`,
`res.users.deletion`, `res.users.log`.

## Registros que ya existían

El módulo **no cambia datos al instalarse**. Las filas de *MCP Available
Models* que ya tienen casillas de escritura marcadas en modelos protegidos (o
que están activas en un modelo desactivado) se quedan como están, pero son
**inertes**: la política decide en cada petición. Limpiarlas es la opción A
de §5 (Jose, a mano y con respaldo en CSV). Al editarlas: desmarcar todas las
casillas de escritura a la vez, o archivar la fila; archivar siempre se
permite.

Limitación: `list_models` y el recurso de modelos de `mcp_server` leen las
casillas directamente de la tabla, así que una fila vieja puede **anunciar**
operaciones que la política rechaza al ejecutarlas. Se corrige al limpiar las
filas (opción A).

Las herramientas propias (`mcp.custom.tool`, acciones de servidor) no pasan
por `mcp.enabled.model` y quedan fuera de esta política.

## Caché

Las puertas de `mcp_server` usan `@ormcache`. La sobrescritura llama a
`super()` (que sigue sirviendo de caché lo que dice la tabla) y aplica encima
una función pura del nombre del modelo; no hace falta invalidar nada extra.
Cambiar las listas requiere desplegar y reiniciar Odoo.

## Cómo se revierte

Desinstalar el módulo (Apps → *Quimibond — Política del MCP* → Desinstalar).
Vuelve a mandar solo lo que digan las casillas de Ajustes.

## Tests

`tests/test_politica.py` (`post_install`): puertas para `ir.cron`,
`res.partner` e `ir.config_parameter` con todas las casillas marcadas y las
reglas de pantalla.
