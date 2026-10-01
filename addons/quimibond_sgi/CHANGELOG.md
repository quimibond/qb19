# Changelog — quimibond_sgi

Una sección por versión del manifest, la más nueva arriba. Reconstruido el
2026-09-29 desde el historial completo de git (`git fetch --unshallow`,
auditoría K-018): la fecha y el resumen salen del commit que subió la versión;
las líneas **Migración** resumen el docstring del script de
`migrations/<versión>/`. Las versiones de las entregas que entraron a `main`
en un squash (lotes 2 a 4, #466 y #469) citan el commit de su rama.

**Regla:** el PR que sube `version` en `__manifest__.py` agrega aquí su
entrada, con el mismo número. `tools/check_addons.py --base-ref` lo exige.

Secciones posibles dentro de una entrada: Agregado, Cambiado, Corregido,
Retirado, Seguridad, Migración, Datos de producción.

## 19.0.57.90.0 — 2026-10-01

**Corregido: los indicadores de ventas contaban la venta de activo fijo.**
La medición por MCP de enero a septiembre de 2026 dio VE-01 +45 % en marzo y
a Leasing Lepezo como cliente principal: la venta de la rama ICOMATEX
($11.3 M en mar-2026, cuenta 704.23.0003, y otra de $2.0 M en junio) entraba
como venta porque los modos de código sumaban la factura completa.

- **Cambiado:** `_sgi_net_invoiced` y los modos `crecimiento_ventas`,
  `clientes_nuevos`, `concentracion_top3`, `facturacion_usd`,
  `ventas_fuera_top10`, `notas_credito`, `clientes_reactivados`,
  `retencion_clientes`, `concentracion_productos`, `presupuesto_ventas`
  y `dso_cartera` miden **líneas de producto en cuentas de ventas** (401
  ventas, 402 devoluciones y descuentos), sin 704 (activo fijo) ni 206
  (anticipos). Parámetro `quimibond_sgi.sales_account_prefixes` (default
  `401,402`). La evidencia por factura solo lista facturas con alguna línea
  de venta. Los configurables VE-02, EX-17 y EX-18 ya se habían corregido por
  dato (`('account_id.code', '=like', '40%')`).

**Agregado: indicadores «de foto».** Campo `snapshot` («Indicador de foto»)
en el indicador. Un indicador de foto mide el estado al calcular (saldo
pendiente, existencias, vigencias), así que solo se mide el último periodo
cerrado. Si se pide uno anterior, el cron, «Recalcular ahora» o
`sgi_recalculate` devuelven «Sin dato: indicador de foto, no reconstruible
para un periodo pasado». Antes devolvían el estado de hoy con la etiqueta de
otro mes. Vienen marcados los modos `cartera_vencida`, `cartera_vencida_60`,
`inventario_diferencia` y `capacitacion`. En una fórmula configurable, lo
marca quien la arma.

**Agregado:** `sgi_recalculate(..., with_details=False)` omite `detail_ids`
de la respuesta. Con 72 indicadores la respuesta pasaba de 60 mil caracteres.
Lo que sí devuelve es `detail_count`, y la medición guardada conserva los ids.

**Migración (`migrations/19.0.57.90.0/post-migrate.py`):**

1. Marca como foto los configurables S2-03, S3-03, S6-03, E2-01 y E2-03.
2. Pasa a «sin dato» las mediciones de los indicadores de foto que cumplen
   las tres condiciones siguientes. El valor anterior queda en la nota.
   - el periodo es anterior al último cerrado;
   - se recalcularon desde el 2026-10-01;
   - no están validadas.

   En producción se esperan las de enero a agosto de EX-08, EX-09, AL-01,
   S2-03, S3-03 y S6-03. E2-01 y E2-03 ya estaban casi todas en «sin dato».
3. Recalcula con el filtro nuevo las mediciones 2026 no validadas de los
   modos de ventas, y deja en el log el antes → después.

## 19.0.57.89.0 — 2026-10-01

**Corregido: fichas que no abrían por una regla de aprobación con un campo
que el documento no tiene** (producción, desde el 2026-09-30: `ValueError:
Invalid field sgi.audit.program.company_id in condition ('company_id', '=', 1)`
en `studio.approval.rule._get_approval_spec`).

- **Causa:** el 2026-09-29 se escribió a mano `('company_id', '=', 1)` (D-03,
  «solo la empresa del SGI») en `approval_domain` de seis roles «Aprueba» sin
  revisar el documento. Ningún código del SGI arma esa hoja: `approval_domain`
  es un cálculo guardado (`_sgi_condition_domain`, solo con el campo de la
  condición), pero el ORM deja escribirlo. Tres documentos no tienen
  `company_id` (`sgi.audit.program`, `sgi.ppap`, `sgi.control.plan`); la
  sincronización de `quimibond_sgi_studio` copió la condición a sus reglas
  (65, 66, 67) y Studio la evalúa con `filtered_domain` al abrir cada ficha.
- **Cambiado:** `sgi_sanitize_domain(env, modelo, dominio)` (en
  `models/sgi_approval_native.py`) quita las hojas cuyo campo no existe en el
  modelo, rutas con punto incluidas, sin tocar las demás ni los operadores
  (un `&`/`|` que pierde una rama queda en la otra; un `!` que la pierde
  desaparece; un dominio no literal, con `uid`, no se toca). El rol la aplica
  al crear y al escribir `approval_domain`, el documento o el campo de la
  condición; `_sgi_clean_approval_domain()` es lo que el satélite de Studio
  manda a la regla.
- **Migración** (`migrations/19.0.57.89.0/post-migrate.py`): limpia
  `approval_domain` de todos los roles y registra antes → después.
  Producción: 1225, 1230 y 1653 pasan de `[('company_id', '=', 1)]` a vacío;
  1604 (`budget.analytic`), 1150 y 690 (`account.move`) no cambian. Las
  reglas las limpia `quimibond_sgi_studio` 19.0.1.0.3.
- **Pruebas:** `test_approval_native.test_07`.

## 19.0.57.88.0 — 2026-10-01

**Corregido: «Mi procedimiento» guardado no se vaciaba** (`TestRoleAudit.test_07`,
«La lista del puesto ya no trae el rol, pero lo guardado no se recalculó»,
fallaba desde 57.13.0).

- **Causa** (diagnóstico de la 57.87.0 en staging): al archivar la actividad,
  el único cálculo de `hr.employee.sgi_mp_role_ids` corre dentro del write
  (`flush_all` de un `unlink` en `_sgi_refresh_spec_gaps`), con la caché ya
  archivada (`act.active` y `role.activity_active` en `False`); la búsqueda
  baja `activity_active` a la base antes de consultar y el filtro en Python
  descarta la actividad, así que la lista calculada sí sale vacía. Pero
  `_compute_sgi_mp_roles_stored` la asignaba como `.ids`, y en Odoo 19 una
  lista vacía asignada a un many2many es una **lista de comandos vacía**
  (`Many2many.write_batch`): no cambia nada. Lo guardado seguía en
  `[3565]` antes y después del cálculo, y nada lo volvía a calcular. No era la
  base atrasada ni el orden del flush: cualquier lista que quedara vacía
  (última actividad o proceso archivado, último escalamiento o «participa»,
  cambio a un puesto sin roles) conservaba lo de antes.
- **Arreglo**: el cálculo guardado (y el del puesto y el empleado público)
  asigna recordsets, que siempre son `Command.set(...)`, también vacío.
  `hr.job._sgi_mp_role_lists` filtra además en Python el proceso archivado
  (ya filtraba la actividad), para no depender de lo que la base tenga a
  medio write.
- **Pruebas**: `test_07` archiva con `write` como la interfaz (pasa por
  `_sgi_refresh_spec_gaps`) y revisa también `sgi_mp_process_ids`; se quita
  el diagnóstico de la 57.87.0. Nueva `test_07b`: escalamientos y
  «participa» que se quedan sin nada, y proceso archivado, vacían sus listas.

**Migración** (`migrations/19.0.57.88.0/post-migrate.py`): recalcula las
cuatro listas guardadas de todos los empleados (activos y archivados) y
registra, lista por lista, cuántos cambiaron y su conteo antes→después.
Idempotente.

**Datos de producción** (lectura por MCP, 2026-10-01): 0 empleados con un rol
de actividad archivada o de proceso archivado (de los 25 archivados) en lo
guardado; los 129 empleados con roles tienen puestos (83) con lista no vacía,
así que «Mis actividades» no tenía roles viejos. Los cambios posibles se
limitan a escalamientos, «participa» o procesos que se quedaron sin
reemplazo; el log de la migración da el número.

## 19.0.57.87.0 — 2026-10-01

**Diagnóstico de `TestRoleAudit.test_07`** (`test_role_audit`, «La lista del
puesto ya no trae el rol, pero lo guardado no se recalculó»; falla desde
57.13.0 en base nueva y en staging y la lectura del código no encontró la
causa). Solo cambia la prueba: ningún código de producción, y la aserción es
la misma (el texto original sigue al principio del mensaje). Al archivar la
actividad y hacer el flush, la prueba envuelve con `unittest.mock.patch.object`,
sin cambiar lo que hacen, `hr.employee._sgi_mp_touch_jobs`,
`hr.employee._compute_sgi_mp_roles_stored` y `sgi.activity.role._sgi_mp_jobs`.
Si la aserción falla, el mensaje del FAIL trae, después de
`--- diagnóstico test_07 (57.87.0)`:

- **Fotos** `[antes de archivar]`, `[archivada, antes del flush]`,
  `[después del flush]`, `[tras invalidate_recordset]`: `caché emp.roles`
  (lo que el ORM tiene en memoria para el empleado; `sin caché` si no hay) y
  `base emp.roles` (SQL directo a `hr_employee_sgi_mp_role_rel`, sin flush);
  `caché act.active` y `caché role.activity_active`; `base (act.active,
  role.activity_active)` (SQL); `pendiente`: si el empleado sigue marcado
  para recalcular `sgi_mp_role_ids` y el rol para `activity_active`
  (`env.transaction.tocompute`), y qué campos de `hr.employee` y de
  `sgi.activity.role` quedan por calcular.
- **`touch`**: cada llamada a `_sgi_mp_touch_jobs` con los puestos que
  recibió, si el empleado quedó marcado para recalcular antes y después, y
  desde dónde se llamó (archivo, línea y función).
- **`_sgi_mp_jobs`**: roles y puestos que devolvió.
- **`compute`**: cuántas veces corrió el cálculo guardado y cuántas con el
  empleado; en cada una, los ids, `su`, el contexto, la pila (doce cuadros:
  si vino del flush o de una lectura) y, cuando incluye al empleado, la foto
  de caché y base **antes** y **después** del cálculo.
- Al final: el valor del ORM tras `invalidate_recordset` y tras
  `invalidate_all`, la base, la lista del puesto en ese momento y el puesto
  guardado del empleado contra el del rol.

Cómo leerlo: si `compute` no corrió con el empleado, el recálculo nunca se
disparó o se descartó (ver `pendiente` y `touch`); si corrió y en `después`
la caché ya no trae el rol pero la base sí, la escritura del Many2many se
comparó contra una caché que no coincidía con la base (Odoo solo borra las
filas que ve en caché); si la caché y la base traen el rol después del
cálculo, el cálculo vio la actividad todavía activa (ver `caché act.active` y
`caché role.activity_active` en `antes`).

## 19.0.57.86.0 — 2026-10-01

Pruebas que fallaron en el build de staging (base = copia de producción con
57.82.0–57.85.0 ya migradas, D-02 incluida): 4 fallos y 4 errores de 1064.
`TestRoleAudit.test_07` queda pendiente aparte. Solo cambian pruebas; ningún
código de producción.

**Corregido (pruebas, error de la prueba):** `test_formatos_bloque3`,
`TestBloque3ClaveD02` no arrancaba (`KeyError: 'protocolo'` en `setUpClass`):
el helper `_doc` tomaba el tipo de un diccionario fijo de siete códigos y D-02
pide además «protocolo» y «reglamento». Fallaba en cualquier base, no solo en
staging. Ahora `_type(code)` resuelve el tipo como la migración (código,
compañía o global, activo o archivado) con el xmlid de fábrica
(`sgi_doc_type_<código>`) de respaldo; en producción «Protocolo» es el id 10
(`quimibond_sgi.sgi_doc_type_protocolo`, global, activo).

**Corregido (pruebas, choque con datos de producción):** `test_reclamaciones`
caso 4 creaba el señuelo «Reclamación Industrial» y Helpdesk le derivaba el
alias `reclamacion-industrial`, que en la copia de producción ya usa el
equipo real (UserError al crear). Los equipos de la prueba llevan
`alias_name` propio (`sgi-prueba-…`); el nombre en inglés y en español sigue
igual, que es lo que la prueba mide.

**Corregido (pruebas, efecto de D-02):** `test_excel_migration` caso 1
esperaba la clave literal F-P-D01-26 en la solicitud de desarrollo; tras D-02
el documento ligado al mapeo se llama F-C1-15 con clave anterior F-P-D01-26 y
la clave viva es la nueva, que es el comportamiento correcto
(`sgi_dev_format_code` sale de `sgi_live_parts()` del mapeo). La prueba compara
contra la clave viva del mapeo `format_ref_dev_carda` y, si no es F-P-D01-26,
exige que sea la del documento cuya clave anterior es F-P-D01-26. En una base
nueva sigue siendo F-P-D01-26.

**Corregido (pruebas, efecto de D-02):** `test_sgi_format_map` caso 4 (la
remisión lleva banner y la recepción no) leía el mapeo real de
`stock.picking`, que D-02 renombra (F-P-A16-01 → clave nueva) y
`sgi_hide_real_documents` desliga. Como ya hacía `test_format_map_varios`, la
prueba archiva los mapeos reales de remisiones y siembra el suyo con
F-P-A16-01. Revisadas las demás pruebas con claves literales F-P-/F-IT-:
todas corrieron y pasaron en el mismo build de staging (siembran sus
documentos o fijan sus mapeos); las de `TestBloque3ClaveD02` no habían
corrido y usan solo documentos propios.

**En `quimibond_ventas_presupuesto` 19.0.1.3.1** (detalle en su README):
`test_ajustes_130` (choque de presupuestos de la misma prueba) y
`test_sales_budget` caso 5.5-v (semana del pronóstico en mayo).

## 19.0.57.85.0 — 2026-10-01

**Corregido (reclamaciones, D-006 / decisión 9):** la 57.19.0 debía marcar
como reclamación los equipos 2 («Reclamaciones entretelas») y 14 («ATENCION A
CLIENTES») y en producción solo marcó el 30 (el del XML ID); Jose marcó 2 y 14
a mano el 2026-10-01. **Causa:** `helpdesk.team.name` es traducible y la
migración corre sin `lang`, así que `search([('name', 'in', …)])` comparó solo
la llave `en_US` del JSONB; los equipos 2 y 14 (creados en 2022 y 2023)
tienen ese nombre solo en `es_MX` y en `en_US` guardan otro texto. **Arreglo:**
`helpdesk.team._sgi_complaint_teams_by_name` compara el nombre en cada idioma
instalado, sin distinguir mayúsculas ni espacios de los extremos;
`_sgi_mark_complaint_teams` lo usa y sigue solo AGREGANDO la marca.

**Migración (post):** vuelve a correr la marca con la búsqueda corregida.
Solo agrega donde falta: nunca desmarca ni mueve tickets. En producción no
cambia nada (30, 2 y 14 ya están marcados); sirve para bases copiadas de
producción antes del arreglo a mano. «Reclamación Industrial» (11) sigue sin
marcar.

**Pruebas:** `test_reclamaciones` caso 4 (equipo con nombre en español solo en
`es_MX`: la búsqueda vieja no lo ve, la nueva sí; mayúsculas distintas; un
señuelo con otro nombre no se marca; una segunda corrida no escribe nada).

**En `quimibond_ventas_presupuesto` 19.0.1.3.0** (mismo PR; ese módulo no
tiene CHANGELOG, el detalle va en su README):
- «Precio mínimo plausible propio» en el producto y en la categoría
  (subcategorías heredan) para que las tiras perforadas a menos de $5/m
  (AP4032BL10.0/2 I a $1.28; KF4032T11BL1.2/0 y KF4032T11GO1.2/0 TE a $0.55)
  cuenten como precio real sin bajar el umbral general.
- «Actualizar real» recalcula el precio de lista también en «Revisado», sin
  regresarlo a borrador, con constancia en el chatter si algo cambia.

**Pendiente de decisión (sin código):**
- Equipo de ventas por cliente en el contacto (`res.partner`): lo decide Jose
  con Ventas.
- P-C13 #3, aviso por antigüedad en cuarentena: propuesta para Areli.

## 19.0.57.84.0 — 2026-10-01

**Cambiado (bloque 3 de formularios 3/3, D-02; decisiones de Jose del
2026-09-30, «Clave D-02: con script, al final del bloque 3», y del
2026-10-01 sobre las tres preguntas abiertas):**
`documents.document._sgi_apply_d02` aplica la clave nueva con
`_sgi_assign_new_code` (56.32.0). Numeración determinista (proceso, prefijo,
clave del Dropbox); el consecutivo sigue al más alto existente. Todas las
revisiones de una clave van juntas y cada documento deja «Clave nueva …
(antes …)» en su chatter.

| Qué | Clave nueva |
|---|---|
| Formatos y formatos de instructivo (consecutivo compartido) | `F-{proceso}-{nn}` |
| Instructivos | `IT-{proceso}-{nn}` |
| DAT | `DA-{proceso}-{nn}` |
| Protocolos | `PROT-{proceso}-{nn}` (patrón nuevo del tipo) |
| Procedimientos del Dropbox «No aplica (se queda)» que ningún proceso sustituye | pasan a tipo **Control operacional**, `CO-{proceso}-{nn}` (patrón del tipo: antes `CO-{seq:02d}`) |

**Conservan su clave (decisiones de Jose, 2026-10-01):**

- **Procedimientos del Dropbox «En curso»:** hasta que su proceso entre en
  vigor y queden obsoletos (se dan de baja con su clave).
- **Formularios de Odoo** (L-004): no pasan a `F-{proceso}-{nn}`. Son
  pantallas; lo que imprimen ya lleva la clave del formato ligado por el
  mapeo.
- **Anexos:** siguen a su documento padre. **Reglamentos** (por ejemplo, el
  Reglamento Interior): están registrados ante la autoridad con ese nombre y
  clave.
- MIID, diagramas, obsoletos y P-I01 con su familia.

- **No renombra archivos ni toca la clave anterior** (la del Dropbox, de
  56.32.0): el buscador «Del Dropbox a Odoo», la búsqueda «Clave SGI» y
  `_sgi_find_by_code` siguen encontrando cada documento por su clave vieja,
  **sin límite de tiempo** (C-005; el inventario decía «12 meses», pero eso
  cambió en 56.32.0). También los controles operacionales y los protocolos.
- Los mapeos de formato imprimen solos la clave nueva (apuntan al documento,
  C-006); su «Clave al ligar» se actualiza cuando era la vieja. Los mapeos
  sin documento siguen encontrando el suyo por la clave anterior (el de
  bloqueo y etiquetado, «P-A20», imprime CO-E2-04). Las ligas con
  actividades no cambian.
- Los controles operacionales ya no son procedimientos: salen de
  «Procedimientos anteriores» y del conteo de procedimientos de «Avance de la
  transición», y entran en **«Formatos y documentos anteriores»** y en los
  documentos del avance (`sgi_migration_action`, `action_open_documents` y
  `sgi.dropbox.progress` incluyen el tipo «Control operacional»). Sus rutinas
  y su clave anterior no cambian.
- `data/sgi_document_types.xml` (noupdate, solo bases nuevas) trae los
  patrones `CO-{process}-{seq:02d}` y `PROT-{process}-{seq:02d}`; en
  producción los pone la migración si el tipo sigue con el de fábrica.

**Migración (post):** `migrations/19.0.57.84.0/post-migrate.py`. Idempotente
(la segunda corrida no cambia nada); en el log, cada «vieja → nueva», los
patrones de tipo con su valor anterior y cada documento sin cambio con su
motivo. Esperado en producción (MCP, 2026-10-01, después de 57.82.0):

- **5 controles operacionales**, todos de E2: P-A17 → CO-E2-01, P-A18 →
  CO-E2-02, P-A19 → CO-E2-03, P-A20 → CO-E2-04, P-S03 → CO-E2-05. Siguen
  como procedimiento los **21 «En curso»**, 5556 (borrador con clave
  inválida, C-008) y P-I01.
- **328 claves nuevas:** C1 33, C2 21, C3 3, C4 67, C5 64, C6 6, E1 1, E2 49,
  S1 11, S2 7, S3 9, S4 41, S5 16 (instructivos 42, formatos 183, F-IT 63,
  DAT 35, protocolos 5: PROT-01…05 → PROT-E2-01…05). IT-C4-01 (3644) ya la
  tenía. Conservan su clave 66 formularios de Odoo, 15 anexos y 4
  reglamentos.
- «Clave al ligar» al día en los 25 mapeos ligados a un formato o F-IT.

**Corregido (57.82.0, antes de llegar a producción):** el informe de otras
referencias al duplicado (`_sgi_b3_other_references`) cuenta cada campo
dentro de un savepoint: un error de SQL en un modelo ajeno ya no deja la
transacción de la migración abortada.

**Pruebas:** `test_formatos_bloque3` `TestBloque3ClaveD02` (numeración
determinista con F y F-IT juntos, todas las revisiones, procedimiento «En
curso» sin tocar, «No aplica (se queda)» → `CO-{proceso}-{nn}` con su tipo,
protocolo → `PROT-{proceso}-{nn}`, anexo con padre, reglamento y formulario
de Odoo sin tocar, búsqueda por clave anterior con `_sgi_find_by_code`, la
búsqueda de Documentos y el buscador `sgi.dropbox.key` también para CO y
PROT, «Formatos y documentos anteriores» con los CO, mapeos con y sin
documento, ligas de actividades, idempotencia y post-migrate).

## 19.0.57.83.0 — 2026-10-01

**Cambiado (bloque 3 de formularios 2/3, propuesta de formatos §2, decisión
del 2026-09-30 «Responsable SGI de cada formato: el dueño del proceso»):**
`documents.document._sgi_owner_from_process` pone como responsable SGI de
cada formato vigente o en piloto (formato, F-IT, DAT, anexo y formulario de
Odoo) al usuario del dueño de su proceso (`sgi_process_id.owner_id.user_id`)
si está activo y es interno. Solo toca los que hoy tiene el Jefe MAST (el
custodio de 56.28.0, `_sgi_manager_user_id`): lo ya reasignado se respeta.
Si el dueño no tiene usuario, el formato se queda con MAST y sale en el log.
P-I01 y su familia quedan fuera. MAST conserva la aprobación y la
publicación.

**Migración (post):** `migrations/19.0.57.83.0/post-migrate.py`, después de
las bajas de 57.82.0. Idempotente; en el log, por proceso, los ids y el
cambio «Blanca Ballesteros → dueño». Esperado en producción (MCP,
2026-10-01): **238** cambian (C1 32 y C2 31 → Jessica Francisco; C3 7 →
Paris Villordo; C5 60 → Oscar González; C6 10 → Cynthia Santana; E1 5 y S1
14 → Jorge Manuel Ortiz; S2 7 y S3 9 → Irma Luna; S4 45 → Miguel Medina; S5
18 → Manuel Juárez); se quedan con MAST E2 70 (la dueña es MAST) y C4 64
(Francisco González no tiene usuario: 54 en el log y 10 de P-I01).

**Para Jose:** la lista de los 64 formatos de C4 que se quedan con MAST está
en `docs/sgi/transicion/formatos-bloque-3.md` §2. Pasan a su dueño en cuanto
Francisco González tenga usuario (ver D-08, «manufactura@»).

**No se hizo:** la familia P-A13 (7 formatos en S2, que la propuesta daba
por mal asignada a S4) no se movió: sus formatos son reportes
administrativos (anticipos, compras, facturación, inventario, importaciones)
y una lista de asistencia; no es un error evidente. Quedan con Irma Luna
hasta que MAST decida su proceso.

**Pruebas:** `test_formatos_bloque3` `TestBloque3Responsable` (dueño con
usuario, sin usuario, usuario inactivo, dueña MAST, lo reasignado se
respeta, instructivo, obsoleto y P-I01 fuera, idempotencia y post-migrate).

## 19.0.57.82.0 — 2026-10-01

**Cambiado (bloque 3 de formularios 1/3, propuesta de formatos aprobada por
Jose el 2026-10-01, §3 y §1):** duplicados y datos malos de los formatos
controlados. Tablas y métodos en `models/sgi_formatos_bloque3.py`
(`documents.document._sgi_formatos_bloque3` y sus pasos
`_sgi_b3_merge_duplicates`, `_sgi_b3_link_formats`, `_sgi_b3_uncontrol`,
`_sgi_b3_recode`, `_sgi_b3_register_odoo_forms`). Cada documento se localiza
por id **y** se verifica su clave (o clave anterior) y su empresa; si no
coincide, se salta con aviso. Lo que cambia queda en el log con su valor
anterior y en el chatter del documento.

- **Dar de baja no es archivar.** En Documentos, archivar manda a la
  papelera y Odoo **borra** lo archivado a los 30 días
  (`documents.deletion_delay`). Los duplicados quedan activos, **obsoletos**,
  con «Motivo de obsolescencia» y «Baja tramitada» (como los procedimientos
  que sustituye un proceso, 45.0.0). Antes de darlos de baja, sus ligas en
  actividades pasan al formato que se conserva. Ninguno está en
  `sgi.format.map`; uno que lo estuviera se salta.
- Bajas: F-IT-P-P04-07-01 (4026, duplicado de F-IT-P-C05-07-01),
  F-P-A23-08 (3752 → F-P-A16-07), F-P-P04-02 (4023 → F-P-C05-02; C1.11 pasa
  a F-P-C05-02), F-P-A13-01 (3725 → F-P-A01-49), F-P-C17-05 (3943 → vale
  F-IT-P-A05-01-01), F-IT-P-P01-01-03 (3989 → F-P-P02-01; C4.14) y
  F-P-P01-01 (4001, sin actividad).
- Ligas nuevas: C2.43 ← F-P-A28-04 (3875), C6.12 ← F-IT-P-A05-01-01 (3698),
  E2.37 ← F-IT-P-G03-01-01 (4063, evaluación de auditores, §1 #8).
- **F-P-V01-04 (5152)** era un reporte de visita **lleno** con la misma
  revisión 0 que el obsoleto 3359: deja de ser documento controlado (no se
  borra). 3359 sigue archivado.
- **F-P-E01-01:** la «Evaluación luminaria» (4060) pasa a **F-P-S01-02**
  (estudio de higiene de SST, NOM-025; familia P-S01, la primera libre) en la
  clave y en la clave anterior, para que la búsqueda por clave anterior no la
  confunda con la matriz; F-P-E01-01 queda en el nombre del archivo, el
  chatter y el seguimiento. Se liga a E2.31 (estudios de higiene y
  evaluaciones NOM). F-P-E01-01 queda libre para la matriz de aspectos
  ambientales: el PDF de la matriz (mapeo 42) imprime «F-P-E01-01» sin
  revisión hasta que exista el documento.
- **Altas como «Formulario de Odoo»** (sin archivo; lo que se llena es la
  pantalla): **F-P-A28-13** «Pronóstico de ventas» (C2, menú Pronósticos,
  ligado a C2.40 y como documento alternativo del mapeo de
  `sgi.sales.budget`, que ya imprimía esa clave sin documento) y
  **F-P-A28-11** «Encuesta de satisfacción del cliente» (E2, menú SGI →
  Dirección → Satisfacción del cliente, ligado a E2.12).

**Migración (post):** `migrations/19.0.57.82.0/post-migrate.py`. Idempotente;
nada se borra ni se archiva. Esperado en producción (MCP, 2026-10-01): 7
bajas, 5 actividades con formatos cambiados (C1.11, C2.43, C4.14, C5.16,
C6.12) más E2.31, E2.37, C2.40 y E2.12, 1 documento deja de ser controlado,
1 clave corregida y 2 altas. Marca «cambió» el procedimiento de C1, C2, C4,
C5, C6 y E2.

**Queda para MAST** (detalle en `docs/sgi/transicion/formatos-bloque-3.md`):
restaurar de la papelera, si se conservan, los 3 controlados archivados
(3359, 5119, 4995: Odoo los borra hacia el 29-oct); subir la matriz de
aspectos ambientales **en blanco** (no hay ninguna en Documentos: 4850 es una
carpeta y 4868 es la matriz llena de MAST) y ligarla a E2.23 y al mapeo 42;
ligar F-P-A16-07 a su actividad de C2 (no hay una evidente); confirmar el
contenido de 4001; configurar la encuesta de satisfacción en Ajustes; decidir
el proceso de la familia P-A13 (no se movió a S4: son reportes
administrativos); corregir las citas de 13 de los 17 formatos citados que no
existen y dar de alta F-P-A14-03, F-P-A06-04 y, si aplica, F-P-C05-10.

**Pruebas:** `test_formatos_bloque3` (fusión con ligas, baja sin archivar,
el que imprime un mapeo se respeta, clave o empresa distinta se salta,
registro lleno, clave equivocada libre y búsqueda por clave anterior, alta de
formulario de Odoo sin archivo con su mapeo, idempotencia, tablas reales y
post-migrate).
## 19.0.57.81.0 — 2026-10-01

**Cambiado (pulido de vistas, bloque 5: textos y pulido; revisión del
2026-09-30):**
- **Migas iguales al menú (V-M12):** las acciones se llaman como su menú
  («Programa», «Hallazgos», «Ajustes»); el menú y la acción «Auditorías
  realizadas» pasan a «Auditorías» (también muestra las de borrador y las
  planificadas); incidentes dicen «Incidentes y accidentes» en lista,
  búsqueda, pivote y actividades, y la ficha «Incidente o accidente»;
  «Revisión por la dirección» con minúscula. Árbol de menús al día. «Eficiencias
  de mi área» y «Hojas mensuales» comparten acción y siguen como estaban.
- **Sin emojis (V-B02):** el aviso de formato controlado usa el ícono
  `fa-file-text-o` en lugar de 📋 (12 avisos).
- **Sin sufijo «SGI» (V-B03):** dentro de la app, «Áreas», «Documentos»,
  «Documento controlado», «Cambio documental», «Gestión del cambio (MOC)»,
  «Formato en documento de Odoo», «Datos de la NC», «Ligas», «Recalcular
  métricas». En fichas de otras apps la pestaña se llama «SGI» (contacto,
  equipo, transferencia, tarea; «EPP» en el empleado y «Documentos
  aplicables» en el puesto, que ya tienen otras pestañas del SGI). El ECO
  de PLM va en `quimibond_sgi_plm` 3.1.0.
- **Glosario (V-B04)** en el README y barrido de etiquetas, ayudas,
  marcadores y confirmaciones: «no conformidad» (NC, sin «NCs»),
  «certificado de calidad (CoA)», «indicador» (no «KPI»), «casi accidente»,
  «Jefe MAST». Los reportes impresos no cambian.
- **Etiqueta «Estado» (V-B05)** en la hoja de eficiencias de personal.
- **Ayudas (V-B07):** «Puestos y procesos» explica qué muestra y de dónde
  salen los puestos; sin claves internas («D-08») en la ayuda de Ajustes. La
  del Pareto del revisado va en `quimibond_sgi_revisado`.
- **«Descartar» (V-B08)** en el botón para salir de los asistentes (NC,
  CoA, requisitos legales, propuestas de cambio, firmas…).
- **Botones inteligentes (V-B09):** «% Difusión» del documento solo con
  `statinfo`; en el proceso, primero los de alerta (sin evidencia,
  indicadores en rojo, riesgos altos, NC abiertas, acciones vencidas,
  faltantes) y después los catálogos.
- **Fichas con título (V-B10):** área, familia de puestos, norma, punto de
  la norma, estudio o examen (trabajador y estudio) y plantilla de checklist,
  que además tiene chatter.
- **Mi equipo (V-B14):** el total de los pendientes es «Total de la página»
  (son calculados: no hay subtotales al agrupar); «pendientes atrasados» en
  masculino en todas partes.
- **Búsqueda de acciones (V-B17):** las agrupaciones van en `<group>` con
  etiqueta corta («Responsable», «Estado», «Fecha compromiso»).

`quimibond_ventas_presupuesto` (misma 19.0.1.2.0): migas «Presupuestos»,
«Pronósticos» y «Releases de clientes». `quimibond_sgi_revisado` (misma
4.3.0): menú «Pareto de defectos de revisado» (sin paréntesis).

**Pruebas:** `test_vistas_pulido_45.TestTextosYPulido`.

## 19.0.57.80.0 — 2026-10-01

**Cambiado (pulido de vistas, bloque 4: listas, análisis y vistas
faltantes; revisión del 2026-09-30):**
- **Pastillas y barras (V-M05):** el estado va como pastilla de color en las
  listas de auditorías, incidentes, riesgos, AMEF, PPAP, planes de control,
  políticas, simulacros, mediciones, acuses y desglose de mediciones (y en
  las listas de solo lectura de la pestaña SGI de órdenes y tareas). El
  avance de la acción (selección 0/50/100 %) es pastilla en la lista global
  y botones en su ficha; la calificación de la evaluación de proveedores es
  barra de avance. Las listas **editables** dentro de las fichas (acciones,
  mediciones del indicador, elementos del PPAP) se quedan como estaban: una
  pastilla no se edita.
- **Paretos ordenados (V-M13):** el Pareto de alertas de calidad sale de
  mayor a menor (gráfica `order="desc"`, pivote por conteo). El del revisado
  va en `quimibond_sgi_revisado` 4.3.0.
- **Programa y auditorías (V-M14):** la lista del programa dice auditorías
  programadas, hechas y el avance (campos calculados sin guardar
  `line_count`, `line_done_count`, `progress_pct`); la de auditorías suma
  procesos auditados, cliente o proveedor (opcional), fin real y auditor con
  avatar.
- **Historial en mediciones y evaluaciones de proveedor (V-M16):**
  `sgi.indicator.measure` y `sgi.supplier.eval` heredan `mail.thread` y
  tienen chatter. Se sigue el valor, el estado y la nota de la medición, y la
  calificación, la clasificación y las notas de la evaluación. Sin columnas
  nuevas (mail.thread no guarda nada en la tabla del modelo).
- **Mediciones de 12 en 12 (V-B11)** en la ficha del indicador, la más
  reciente arriba.
- **Vistas que faltaban (V-B12):** calendario de auditorías (fecha
  planificada), simulacros (fecha programada) y próximas calibraciones, con
  color por estado o resultado; kanban por estado de acciones e incidentes
  (sin arrastrar: el estado sale de los botones); gráfica mensual de
  incidentes por tipo; panel lateral por tipo de documento y proceso en
  Documentos.
- **`multi_edit` (V-B13)** en las listas de acciones, riesgos, documentos y
  requisitos legales.
- **Lecciones aprendidas (V-B15)** con la búsqueda de NC (folio, proceso,
  mayores, fecha). La búsqueda y el menú de hallazgos ya existían (57.53.0 y
  57.67.0).
- **Vista de actividades (V-B18)** en calibraciones, requisitos legales,
  estudios y exámenes, recorridos CSH, objetivos y fichas de máquina.

`quimibond_ventas_presupuesto` 19.0.1.2.0 (V-M10): sumas en cantidad e
importe de las líneas y encabezado con solo el flujo (detalle en su README).

**Pruebas:** `test_vistas_pulido_45.TestListasYAnalisis`;
`quimibond_ventas_presupuesto/tests/test_sales_budget_views.py`;
`quimibond_sgi_revisado/tests/test_pareto_revisado.py`.

## 19.0.57.68.0 — 2026-09-30

Tercera corrida de las pruebas del SGI en **staging** (copia de producción):
2 fallas de 1008.

**Pruebas (la prueba estaba mal):**

- `test_indicadores_2` test_06 (S4-01, bajas registradas al día hábil
  siguiente): **causa encontrada.** La prueba creaba al empleado y le
  capturaba la baja en la misma transacción. `mail.thread.create` llama
  `_track_discard` sobre el registro nuevo (el contrato `hr.version` que
  Odoo 19 crea con el empleado) y `_track_prepare` no sigue ningún cambio de
  ese registro hasta el precommit; así, la fecha y el motivo de salida no
  dejaban seguimiento en **ninguna** base (nueva o copia de producción) y las
  tres bajas salían «sin registro» o «sin motivo»: (0, 3). La prueba confirma
  el alta (`flush_all` + precommit) antes de capturar la baja, como pasa en
  la vida real (alta y baja son dos peticiones). Lo que se verifica no
  cambia. **El indicador no cambia:** en producción el alta y la baja van en
  peticiones distintas y el seguimiento sí queda en `hr.version` (lectura
  2026-09-30: 115 cambios de «Fecha de salida» y 96 de «Motivo de salida» en
  el contrato, el último del 29-sep; los de antes de Odoo 19 siguen en
  `hr.employee` y el indicador lee ambos).

**Sin causa confirmada:** `test_role_audit` test_07 (desde 57.13.0). Se
revisó el camino completo sin encontrar dónde se pierde el recálculo:
archivar la actividad pasa por `sgi.process.activity.write`, que llama
`_sgi_mp_touch_jobs` **después** del `write` con los puestos de los roles
(el rol conserva su puesto, así que la lista no sale vacía), la búsqueda de
empleados por `sgi_mp_job_id` es la misma que la prueba comprueba, y
`add_to_compute` marca los cuatro campos guardados que calcula
`_compute_sgi_mp_roles_stored`; ningún código del SGI escribe esos campos
directo ni limpia los pendientes del ORM. Se deja sin tocar.

## 19.0.57.67.0 — 2026-09-30

Segunda corrida de las pruebas del SGI en el build de **staging** de `main`
(copia de producción, 57.66.0): 7 fallas y 8 errores de 1000. Cada una se
clasificó: (a) error real o choque entre ramas, (b) la prueba dependía de la
fecha real, (c) la prueba chocaba con datos o configuración de producción.
Ninguna prueba se saltó, quitó ni debilitó.

**Corregido (código):**

- (a) **Menús de Entregables, Flujos entre procesos y Hallazgos con xmlids
  retirados.** 45.0.0 (limpieza antes de producción, decisión del CEO del
  2026-09-24) retiró `menu_sgi_deliverables`, `sgi_deliverable_action`,
  `sgi_process_flow_action` y `sgi_audit_finding_action` (menús de «Datos
  técnicos» y acciones huérfanas), y `test_cleanup_45` exige que no vuelvan.
  57.64.0 (menús SGI → Procesos → Entregables y Flujos entre procesos) y
  57.53.0 (Mejora → Auditorías → Hallazgos) los volvieron a crear con el
  mismo xmlid. Los menús se quedan (son lo que 57.53/57.64 pidieron) con
  xmlids nuevos: `menu_sgi_deliverable_list`, `sgi_deliverable_list_action`,
  `sgi_process_flow_list_action` y `sgi_audit_finding_list_action`
  (`menu_sgi_process_flows` y `menu_sgi_audit_findings` no estaban
  retirados y siguen). `test_cleanup_45` no cambia.
  (`test_cleanup_45` test_02.)
- (a) **Plan de salida «Baja de personal» (57.14.0, S6-02), otra vez:** en
  Odoo 19 cambiar el tipo de un renglón del plan también recalcula
  `responsible_type` («preguntar al lanzar») y `responsible_id` (vacío si el
  tipo no trae usuario), no solo el resumen (57.66.0). El renglón «Desactivar
  usuario de Odoo, correo y accesos» perdía a su responsable (en producción,
  Mariano Domínguez; «Recuperar EPP…», a Blanca Ballesteros) y «Recoger
  equipo de cómputo», que copia el responsable del renglón de accesos, nacía
  sin él. `_sgi_adopt_offboarding_plan` escribe tipo, resumen, responsable y
  nota juntos. Producción (57.13.0) corre la migración 57.14.0 ya corregida;
  el staging que ya pasó por 57.14.0 conserva el plan sin esos responsables
  (se recaptura a mano en esa copia). (`test_indicadores_2` test_07; la
  prueba ahora verifica también que el renglón de accesos conserva su
  responsable.)
- (a)+(c) **Candado de evidencia y decisiones: el aviso nombra los registros
  con sudo.** El nombre del bloqueo LOTO sale de su equipo, y en Odoo un
  usuario interno solo lee los equipos que sigue (regla nativa de
  Mantenimiento «Users are allowed to access equipments they follow»; los
  mecánicos reales están en los grupos de Mantenimiento que ven todo). Al
  armar el mensaje del candado, el Usuario SGI chocaba con esa regla y la
  acción se detenía con un AccessError en vez del aviso del candado. El
  candado sigue igual (mismo estado, mismo grupo); solo el texto del aviso
  se arma con sudo (`sgi.base.mixin.write`, `unlink` y
  `_sgi_check_decision`). (`test_loto` test_02.)
- (a) **Many2many a empleados sin ser de RH (Odoo 19):** escribir un
  Many2many a `hr.employee` exige leer `hr.employee`, que solo lee RH; Odoo
  trae `hr.mixin` para eso. El Usuario SGI no podía poner a los ejecutores
  del permiso de trabajo (`AccessError` al crear). Se agrega `hr.mixin` a
  `sgi.work.permit` (ejecutores), `sgi.incident` (personas afectadas),
  `sgi.management.review` (asistentes), `sgi.csh.inspection` (integrantes)
  y `sgi.checklist.template` (empleados). Sin cambios en la base.
  (`test_work_permit` test_01 a test_05.)
- (a)+(c) **Mapeo de formato con tipo de operación archivado:** los
  criterios (`picking_type_ids`, `workcenter_ids`) se leían sin los
  archivados, así que un mapeo cuyo único tipo estaba archivado se guardaba
  como **general** y chocaba con el general del modelo («Ya existe un mapeo
  general (sin criterio) para Transfer: F-P-A16-01»). En producción la
  interna del almacén Toluca (tipo 5, «Traslados internos») está archivada.
  Ambos campos leen con `active_test=False`. (`test_format_map_varios`
  test_05.)

**Migración:** `19.0.57.67.0/post-migrate.py` borra los registros que
quedaron con los cuatro xmlids retirados (solo existen en bases que pasaron
por 57.53.0/57.64.0, como staging; producción está en 57.13.0 y no los tiene)
y su xmlid; Odoo también los borraría al final del update. Recalcula
`is_general` de los mapeos de formato y deja en el log lo que cambie.
Idempotente.

**Pruebas (la prueba estaba mal o no aislaba sus datos):**

- (a) `test_format_map_operaciones` (setUpClass): creaba los formatos F-IT-…
  con tipo «Formato», cuya nomenclatura (F-P-…) los rechaza; ahora con
  «Formato de instructivo». Además la interna copiada del almacén heredaba
  el archivado de producción y la siembra (solo tipos activos) no la
  encontraba: la copia va activa.
- (c) `test_format_map_varios` test_05: usa una interna propia y activa;
  test_05b nueva: un mapeo con un tipo archivado no se vuelve general.
- (a) `test_nomenclatura_pantallas` test_02: desde 57.44.0 (V-M08) el plan
  de auditoría imprime su clave en el pie en vivo (`format_ref_audit_plan`,
  en producción «F-P-G03-03 · Rev. 00»). La prueba busca lo escrito a mano
  en el reporte **sin el pie** y comprueba que la clave sí sale en el pie.
- (a) `test_sst_links` test_01: el numeral de la actividad lleva el paso a
  dos dígitos desde 29.2.0 (`XSL.01`, como `E2.18` en la tabla real); la
  prueba usaba `XSL.1`. test_02 revisa que la tabla real tenga ese formato.
- (c) `test_vistas_pulido` TestNcFichaYLista test_03: registraba la acción
  correctiva antes de la causa raíz; la regla H8 lo impide en la NC con
  folio, y en la copia de producción la NC lo trae (NCI-2026-0730). La causa
  raíz va primero; lo que se verifica no cambia.
- `test_menus_entregables`: xmlids nuevos, ninguno retirado (test_03) y la
  migración (test_04).

**Sin causa confirmada:** `test_indicadores_2` test_06 sigue (0, 3) en
staging: «A tiempo» no cuenta, y ni el calendario de producción (id 21,
lunes a viernes, sin días inhábiles en 2045) ni el seguimiento de
producción (sí guarda fecha y motivo de salida en `hr.version`) lo explican
en el código. La falla ahora dice qué vio el indicador (registrada, día
local, límite, seguimientos encontrados). `test_role_audit` test_07 sigue
sin tocar (desde 57.13.1).

**Revisadas sin cambio:** los «duplicate key»
`sgi_staff_efficiency_line_employee_uniq` (`TestExcelMigration` test_03) y
`sgi_sales_budget_line_product_month_partner_uniq`
(`quimibond_ventas_presupuesto`, `TestSalesBudgetClient` test_04) son el
log de `odoo.sql_db` en pruebas que comprueban justo esa restricción; no
fallaron.

## 19.0.57.66.0 — 2026-09-30

Pruebas del SGI corridas en el build de **staging** de `main` (base copia de
producción, no base nueva): 14 fallas y 9 errores de 938. Cada una se
clasificó: (a) error real o choque entre ramas, (b) la prueba dependía de la
fecha real, (c) la prueba chocaba con datos de producción. Ninguna prueba se
saltó, quitó ni debilitó.

**Corregido (código):**

- (a) **Plan de salida «Baja de personal» (57.14.0, S6-02):** al cambiarle el
  tipo a un renglón, Odoo 19 recalcula `summary` desde el tipo (compute
  guardado y editable) y el renglón perdía su texto: «Desactivar usuario de
  Odoo, correo y accesos» quedaba como «Retirar accesos (usuario de Odoo,
  correo y sistemas)», y la segunda corrida ya no encontraba el renglón por
  su prefijo. `_sgi_adopt_offboarding_plan` escribe tipo y resumen juntos.
  Producción (57.13.0) aún no corre la migración 57.14.0: la corre ya
  corregida. (`test_indicadores_2` test_07, `KeyError`.)
- (a) **Fórmula configurable, «días hábiles» entre dos fechas:** una fecha
  (`Date`) se volvía medianoche UTC y `sgi_business_days` la pasaba a la
  zona del calendario; con el de producción (America/Mexico_City) caía en el
  día anterior y «del sábado 17 al martes 20» contaba 1 hábil en vez de 2.
  Ahora una fecha va tal cual y solo un datetime cambia de zona
  (`sgi.indicator.term._sgi_delta`). (`test_indicator_formula` test_09.)
- (a) **«Hoy» del SGI en los estados calculados:** el estado del documento
  externo (`sgi_ext_state`) y la vigencia de estudios y exámenes
  (`sgi.health.record.state`) usaban `context_today` (UTC para OdooBot)
  mientras sus crons de aviso usan `sgi_today` (57.15.0): después de las
  18:00 de México el estado y el aviso podían diferir un día. Ambos usan
  `sgi_today`.
- (a) **Acción «Actividades» del SGI:** hasta 45.0.0 abría agrupada por
  proceso (`search_default_group_process`); quitar el campo del XML no lo
  borra de la base y la copia de producción lo seguía teniendo. El contexto
  va explícito (`{}`). (`test_procedure` TestProcedureActivityMenu test_01.)
- (a) **Mapeos de formato por tipo de operación (57.61.0):** la migración
  corre sin idioma (en_US) y el nombre del tipo de operación es traducible;
  además contaba los archivados. En staging saltó «Carda» y «V10-» (0),
  «Cocina Entretelas» (2), «Tintorería» (3) y «Salida de consumibles» (0).
  `_sgi_seed_operation_maps` busca solo tipos **activos** y el nombre en
  **es_MX primero** (luego los demás idiomas instalados y en_US), y toma un
  tipo solo si en ese idioma hay exactamente uno
  (`_sgi_picking_type_by_name`). Prueba nueva:
  `test_format_map_operaciones` test_05 (nombre en inglés distinto del
  español y un duplicado archivado).

**Migración:** `19.0.57.66.0/post-migrate.py` vuelve a llamar
`_sgi_seed_operation_maps()` para las bases que ya pasaron por 57.61.0
(staging); respeta los mapeos que existan (mismo documento o clave, activos
o archivados). Idempotente. En producción (es_MX, compañía 1) cada nombre
existe una sola vez y activo: Carda 80, V10- 89, Cocina Entretelas 82,
Tintorería 88, Salida de consumibles 242.

**Pruebas (la prueba estaba mal o no aislaba sus datos):**

- (b) `test_external_doc` test_02 y `test_hse_records` test_01: parchaban
  `fields.Date.context_today`, pero desde 57.15.0 los crons usan
  `sgi_today` (reloj real): congelan el reloj con `freeze_time`.
- (b→a) `test_hse_records` test_03: esperaba «semanal: solo los lunes»;
  desde 57.15.0 (G-022, decisión 14) la semanal sale el primer día hábil de
  la semana en que corra el cron, una vez por semana. La prueba verifica eso
  (el martes sale si el lunes no corrió; tras generar, no; la semana
  siguiente, sí; el sábado, no) con calendario propio de lunes a viernes.
- (b→a) `test_indicator_plan` test_03: el día 10 de febrero de 2030 es
  domingo y desde 57.15.0 el plazo del plan se adelanta al viernes 8; el
  sábado 9 ya escalaba. Verifica el plazo (8) y que no escala hasta ese día.
  Calendario propio de lunes a viernes en la clase.
- (a) `test_fichas_busquedas` test_03: el seguimiento de `mail.thread` se
  escribe en el precommit del cursor; la prueba lo corre (como
  `test_indicadores_2`) y además exige el renglón de `date_done`.
- (c) `test_fichas_busquedas` test_02: los botones llevan `groups` y Odoo
  los quita de la vista a quien no está en el grupo; OdooBot no lo está en
  la copia de producción. La ficha se pide como Jefe MAST.
- (c) `test_audit_pr3` test_04: el pie del informe sale del mapeo por
  referencia `format_ref_audit_report` (noupdate, MAST lo edita) y del
  documento vigente; la prueba oculta los documentos reales y fija su mapeo
  (como `test_laboratorio` y `test_etiquetas_lote`).
- (c) `test_ola1` test_02: la Dirección que recibe la escalación es el primer
  miembro activo del grupo; en producción es el usuario 9. `sgi_set_director`
  (en `tests/common_users.py`) saca de Dirección, dentro de la prueba, a los
  demás miembros directos activos.
- (c) `test_indicadores_2` test_08 y test_09, `test_legado` test_02: renombran
  el indicador real (S2-01, S1-05, E1-02) y luego crean el suyo, pero
  `create()` no baja lo pendiente del ORM y el INSERT chocaba con el índice
  único. Se baja el cambio antes (`flush_model`); `test_legado` busca también
  archivados.
- (c) `test_indicadores_2` test_06: motivo de salida propio y «sin motivo»
  explícito (`False`), sin depender del primer motivo ni de lo que la base
  ponga por omisión. **Causa no confirmada** con el log (la falla dice solo
  «value diff»); si vuelve a fallar, el log dirá cuál de las dos cifras.
- (c) `test_indicadores_571` test_01 y test_02: lo pendiente del ORM baja
  antes de fijar etapa y fechas por SQL; si no, el flush posterior las
  reescribía y la NC «cancelada» volvía a contar (50 % en vez de 66.67 %).
- (c) `test_my_procedure` (test_04, 08, 12, 13, 15): con «Mi procedimiento
  con firma» encendido en la base, publicar arma la hoja de firmas con PyPDF2
  y en modo prueba Odoo entrega HTML («EOF marker not found»). La clase
  publica sin firma (la firma se prueba en `test_my_procedure_sign`, con PDF
  de verdad).
- `tests/common_calendar.py`: `sgi_test_calendar(env, tz=...)` para probar
  con la zona de producción (`test_indicator_formula` test_09).

**Pendiente:** `test_role_audit` test_07 (desde 57.13.1): sin causa
encontrada en el código (el recálculo de lo guardado en el empleado al
archivar la actividad se marca con `add_to_compute` y el `flush_all` lo
corre); se deja sin tocar.

**Revisadas** (patrones de staging en las pruebas nuevas de #487; sin otro
cambio que la test_05 de arriba): `test_format_map_varios`, `test_format_map_operaciones`,
`test_laboratorio`, `test_etiquetas_lote`, `test_menus_entregables` y
`test_entregables_modelo` ya aíslan sus mapeos y documentos y usan fechas y
claves propias.

## 19.0.57.65.0 — 2026-09-30

**Cambiado (bloque 2 de formularios 6/6, inventario §3.4 y §5 #12):** 20
entregables propios sin «Modelo de Odoo» lo reciben porque su nombre dice sin
duda dónde viven. Tabla `SGI_DELIVERABLE_MODELS`
(`models/sgi_deliverable_models.py`), método
`sgi.deliverable._sgi_fill_evident_models`:

| Modelo | Entregables |
|---|---|
| Transferencia (`stock.picking`) | C2-ACUSE, C2-CITA, C2-CRUCE, C2-PEDIMENTO, C2-TRANSPORTE, C6-DESCARGA, C6-QUIMICOS |
| Orden de producción (`mrp.production`) | C3-VIEJAS, C4-PARAMETROS, C4-TARJETA |
| Control de calidad (`quality.check`) | C5-LIBERADO, C5-PRUEBAS |
| No conformidad (`quality.alert`) | C5-CONCESION |
| Orden de compra (`purchase.order`) | S1-FECHA |
| Factura (`account.move`) | S2-CONTRARRECIBO, S2-PORTAL |
| Bloqueo de periodo (`sgi.lock.date.log`) | S3-BLOQUEO |
| Movimiento bancario (`account.bank.statement.line`) | S3-CONCIL-BANCO |
| Solicitud de mantenimiento (`maintenance.request`) | S5-PREV-HECHO, S5-REPARACION |

Solo el modelo: el filtro «ya está entregado», la fecha y el usuario quedan
como estaban (`[]`, `create_date`, vacío) para que MAST los afine; ninguna
actividad se mide con ellos todavía, así que ninguna medición cambia. El
modelo viaja a sus flujos entre procesos.

**Migración (post):** `migrations/19.0.57.65.0/post-migrate.py` llama al
método: entregable por código en la empresa del SGI; escribe solo si el
modelo está vacío (en el log: «modelo vacío → modelo»); se salta el que ya
mide alguna actividad o cuya fecha o usuario no existe en el modelo.
Idempotente; nada se borra. Esperado: 20.

**Queda para MAST (96 entregables propios sin modelo; filtro «Sin modelo de
Odoo» en SGI → Procesos → Entregables).** Con candidato probable, pero no
evidente por el nombre:

| Entregable | Candidato | Duda |
|---|---|---|
| C1-COTIZACION, C1-COSTO | `sale.order` / `project.task` (FT) | ¿La cotización del desarrollo sale del pedido o de la tarea de diseño? |
| C2-ASN, C2-CARGA, C2-TARIMAS, C2-EXP-DOCS, C2-EXP-CERRADO | `stock.picking` | Pasos del embarque que hoy no dejan dato propio en la entrega |
| C2-SOL-FECHA | `mail.activity` | Actividad asignada a Planeación |
| C3-FECHA, C3-PLAN, C3-REPROG, C3-PRONOSTICO | `mrp.production` / `sgi.sales.budget` | Programa semanal y S&OP no son un registro único |
| C4-EMPACADO, C4-TONO, C4-MONTAJE | `stock.lot` / `quality.check` / `mrp.workorder` | Depende de dónde se registre hoy en planta |
| C5-CONTENCION | `quality.alert` | La contención es un paso de la NC, no un registro |
| C6-NEGATIVOS, C6-SORPRESA, S3-SORPRESA | `stock.quant` | Conteos: ¿ajuste de inventario o reporte? |
| C6-HDS | `stock.picking` / `documents.document` | ¿HDS adjunta a la recepción o en Documentos? |
| C6-DESP-LISTO, C6-VENTA-DESP | `stock.picking` (VENTA DE DESPERDICIO) / `sale.order` | |
| S1-ANTICIPO, S1-DISPERSION, S1-PROPUESTA | `account.payment` / `approval.request` | |
| S1-NECESIDAD-MP, S1-IMPORT, S1-CONFORMIDAD, S1-DIFERENCIA | `purchase.order` | Pasos de la compra sin dato propio |
| S2-PROMESA, S2-RECORDATORIO, S2-ACLARACION, S2-CASTIGO | `account.move` (seguimiento) / `mail.activity` | |
| S3-CALC-IMP, S3-DECLARACION | `account.return` | Hoy las 8 declaraciones de 2026 están en «Nuevo» (propuestas, §1 #4) |
| S3-MOV-REG, S3-POLIZAS-CIERRE | `account.move` | |
| S4-CAPACITACION, S4-INDUCCION, S4-PROG-CAP | `slide.channel.partner` / `survey.user_input` | La capacitación presencial no pasa por eLearning |
| S4-INCIDENCIAS | `hr.leave` / `hr.attendance` | |
| S4-NOMINA, S4-NOMINA-REV, S4-FINIQUITO, S4-AGUINALDO, S4-PTU, S4-CFDI-CONC | `hr.payslip.run` / `hr.payslip` | Nómina en Odoo desde el 1-ene-2027 |
| S4-ALTA-IMSS, S4-MOD-SALARIO, S4-SOL-ACCESOS | `hr.employee` / `hr.version` / `helpdesk.ticket` | |
| S5-FLOTILLA | `fleet.vehicle` | ¿Está instalada Flotilla? |
| S6-ACCESO-BAJA, S6-REV-USUARIOS, S6-VOBO-COMPRA | `helpdesk.ticket` / `res.users` / `approval.request` | |

Sin modelo natural (revisiones y reportes mensuales, trámites con acuse
externo): C1-REVISION, C2-REVISION, C3-CUMPL, C4-REVISION,
C5-AUDITORIA, C5-REPORTE, C6-REPORTE, E1-EVAL-INV, E1-FLUJO13, E1-PLAN,
E1-REPORTE-CONSEJO, E1-RIESGO-FIN, E1-VS-PPTO, E2-MATRIZ-VIG, S1-CONCIL,
S1-REP, S1-REPORTE, S1-RESP-PROV, S2-REP-CARTERA, S2-REV-CRED, S3-ARQUEO,
S3-BALANZA, S3-CONCIL-INV, S3-CONT-ELEC, S3-EEFF, S3-FECHA-INV,
S3-FISCAL-VIGENTE, S3-FLUJO, S3-RESP-REQ, S4-COMISIONES, S4-CREDITOS,
S4-CUOTAS, S4-DECL-ANUAL, S4-ISN, S4-MATRIZ, S4-PLANTILLA, S4-REPORTE-RH,
S4-VALES, S5-REPORTE, S5-SERVICIOS, S6-REPORTE, S6-VIGENCIAS. Para estos
basta con escribir en la actividad «Dónde se ejecuta» el portal o la carpeta.

**Pruebas:** `test_entregables_modelo` (solo lo vacío, respeta lo capturado,
salta un campo de fecha ajeno al modelo, idempotente, tabla real y
post-migrate).

## 19.0.57.64.0 — 2026-09-30

**Agregado (bloque 2 de formularios 5/6, inventario §4.1 y §5 #11):** menús
**SGI → Procesos → Entregables** (`sgi.deliverable`, 319 en producción) y
**SGI → Procesos → Flujos entre procesos** (`sgi.process.flow`, 50), que antes
solo se veían dentro del proceso, la actividad o el diagrama. Usan su lista,
ficha y búsqueda propias (ya existían; sin herencias). Flujos abre con el
filtro «Mapa vigente». La búsqueda de entregables suma el filtro «Sin modelo
de Odoo» (propios, sin las entradas externas) y agrupa por modelo de Odoo y
por frontera del mapa. Árbol de menús al día (`tools/sgi_menu_tree.txt`).

**Pruebas:** `test_menus_entregables` (menús con su acción y búsqueda
propia, filtro sin modelo).

## 19.0.57.63.0 — 2026-09-30

**Agregado (bloque 2 de formularios 4/6, inventario §5 #8):** etiquetas de
**material liberado, rechazado y detenido** (F-P-C04-02, -03 y -04, clase C,
hoy en Excel) en el menú Imprimir del **lote** (`stock.lot`) y de la **NC**
(`quality.alert`: una etiqueta por lote de la NC; sin lote, una con el
producto y «Sin lote»). Tamaño etiqueta 100 × 76 mm (papel
`paperformat_sgi_lot_label`), el mismo de las etiquetas Dymo/Zebra del
almacén (`stock_dymo_labels`, módulo de Consolti en la raíz que no es
dependencia del SGI: se reutiliza el tamaño, no el módulo). Lleva estado en
grande con su color, producto con referencia, lote con código de barras,
cantidad del lote, folio de la NC, fecha, firma y el **pie del formato
controlado** en vivo (mapeos por referencia `format_ref_lot_released`,
`format_ref_lot_rejected`, `format_ref_lot_held`, noupdate; MAST liga el
documento y la revisión sale sola). Archivo `report/report_lot_label.xml`,
sin herencias.

**Pruebas:** `test_etiquetas_lote` (las tres desde el lote, desde la NC con y
sin lote, pie con la clave, menú Imprimir).

## 19.0.57.62.0 — 2026-09-30

**Agregado (bloque 2 de formularios 3/6, inventario §5 #7):** laboratorio de
Calidad (C5) sobre lo que ya existía, sin modelos nuevos:

- **Equipo de laboratorio:** casilla «Equipo de laboratorio»
  (`maintenance.equipment.sgi_is_lab`) en la pestaña «Metrología / EPP (SGI)»
  del equipo. Menú **Calidad → Calidad preventiva → Metrología → Equipos de
  laboratorio**, con lista y búsqueda propias (prestados, en el laboratorio,
  vencidos, NO USAR; agrupar por «Prestado a»). El equipo marcado muestra e
  imprime el formato del instrumental de laboratorio.
- **Préstamo:** «Prestado a» (empleado) y «Prestado desde» (se llena sola)
  con seguimiento: el historial del equipo es la bitácora de préstamos. La
  ficha muestra la clave vigente del formato de préstamo
  (`format_ref_lab_loan`).
- **Verificación de laboratorio:** tipo nuevo de calibración («Verificación
  de laboratorio») para la revisión periódica del equipo. No mueve las fechas
  de calibración ni desbloquea; fuera de tolerancia deja el equipo NO USAR y
  abre la NC de evaluación de impacto, como una calibración. Menú
  **Metrología → Verificaciones de laboratorio**; la búsqueda de calibraciones
  separa calibraciones y verificaciones y agrupa por tipo. La ficha de la
  calibración muestra su formato controlado.
- Mapeos nuevos (`data/sgi_format_map_data.xml`, noupdate; criterio de
  57.60.0): verificación → F-P-C05-11 «Bitácora de revisión de equipos de
  laboratorio»; equipo de laboratorio → F-IT-P-C05-06-07 «Instrumental de
  laboratorio»; préstamo por referencia → F-P-C05-07. MAST liga el documento
  en «Formatos en documentos de Odoo» y la revisión sale sola.

**Queda para MAST:** marcar qué equipos son de laboratorio (hoy 148 equipos
de medición, todos en la categoría EMIP; ninguno se marca solo).
F-IT-P-C05-07-01 «Calibración del equipo Wesco» no se mapeó: en producción no
hay ningún equipo «Wesco» dado de alta; al darlo de alta, un mapeo de
`sgi.calibration` con filtro por ese equipo lo resuelve. F-IT-P-P04-08-01
(verificación de instrumental, C4) duplica a F-P-C05-11 (bloque 3). Las 46
calibraciones de producción (todas internas y conformes) no se tocan.

**Pruebas:** `test_laboratorio` (verificación conforme y fuera de
tolerancia, formatos, préstamo, menús).

## 19.0.57.61.0 — 2026-09-30

**Agregado (bloque 2 de formularios 2/6):** las órdenes de producción, vales
y transferencias de C4 (Producción) y C6 (Almacén e inventarios) imprimen su
propia clave con los mapeos por criterio de 57.60.0. Tabla
`SGI_OPERATION_FORMAT_MAPS` (`models/sgi_format_map_seed.py`), método
`sgi.format.map._sgi_seed_operation_maps`:

| Modelo | Clave (doc) | Cuándo aplica |
|---|---|---|
| Orden de producción | F-P-P02-01 OT entretelas (3999) | Tipos Carda (80), V10- (89), V18 (90) |
| Orden de producción | F-IT-P-P01-02-02 Orden de cocina (3993) | Tipo Cocina Entretelas (82) |
| Orden de producción | F-IT-P-P01-12-01 OT teñido (5057) | Tipo Tintorería (88) |
| Transferencia | F-IT-P-A07-01-02 Reetiquetado y empaque (3701) | Tipo Reetiquetado (209) |
| Transferencia | F-IT-P-A07-01-01 Requisición de refacciones y consumibles (3700) | Tipos Salida de consumibles (242) y Salida Refacciones a Gasto (264) |
| Transferencia | F-IT-P-A05-01-06 Devolución a proveedor (3699) | Filtro: destino ubicación de proveedor (prioridad 20) |
| Transferencia | F-P-A07-04 Devoluciones de cliente (3708) | Filtro: origen ubicación de cliente (prioridad 20) |

Las demás órdenes siguen con la general F-IT-P-P01-08-01 (tarjeta viajera) y
las salidas con F-P-A16-01, sin cambio.

**Migración (post):** `migrations/19.0.57.61.0/post-migrate.py` llama al
método: formato por clave vigente, tipos de operación por nombre exacto en la
empresa del SGI (no tienen XML ID). Nunca pisa: si ya hay un mapeo del modelo
con ese documento o esa clave (activo o archivado), se respeta y queda en el
log; si falta el formato o un tipo, o un nombre es ambiguo, el renglón se
salta. Idempotente; nada se borra. Esperado: 7 mapeos.

**Queda para MAST (no se pudo determinar con certeza):**

| Qué | Por qué quedó fuera |
|---|---|
| F-IT-P-P01-01-03 «Orden de producción de carda» (3989) | La carda imprime F-P-P02-01 (propuesta del inventario: 3989 duplica a 3999); fusionar o reasignar es del bloque 3 |
| F-P-P01-01 «Orden de trabajo» (4001) | Está en la carpeta de Mantenimiento; hay que abrir el archivo para saber si es de producción o duplica a F-P-M01-01 |
| Cocina Acabado (81, 1,020 órdenes en 2026) y Cocina Tintorería (83, 2,010) | ¿Usan la orden de cocina F-IT-P-P01-02-02 (instructivo de cocina de entretelas) o la formulación F-IT-P-P01-13-03 / check list F-IT-P-P01-15-01? |
| Re-proceso Tintorería (106, 40) y Re-proceso Acabado (107, 2) | ¿Llevan la OT de teñido F-IT-P-P01-12-01? |
| Termofijado (152, 31 órdenes) | No aparece activo en los tipos de la empresa 1; si es línea de entretelas, agregarlo a F-P-P02-01 |
| Acabado (79), Acabado producto en proceso (151), Estiramiento (112), Encogimiento (263), Corte y perforado (84), Tejido tramado (87) y desarrollo (86), conversiones | Sin formato de orden propio identificado: siguen con la tarjeta viajera general (que es del tejido circular, IT-P-P01-08) |
| F-P-P01-02 Bitácora de actividades de TAC (4022) | Es una bitácora, no un documento de Odoo identificado |
| F-IT-P-A07-01-03 / -04 Lista de embarque (nacional / exportación) | La general de salidas imprime F-P-A16-01, cuyo archivo es «CITAS» (inventario §3.6): MAST decide cuál queda antes de cambiar el general |
| F-IT-P-A07-01-05 Bitácora de embarques, -06 Rollos por embarque, -07/-08 Sellos | Bitácoras y controles, no un tipo de operación |
| F-IT-P-A05-01-01 Vale de salida de laboratorio (C5, 3698) | Ningún tipo de operación de laboratorio identificado |
| Requisición MP (113), Requisición PP y PT (210), Requisición tintorería (236) | Sin formato controlado identificado para esas requisiciones |

**Pruebas:** `test_format_map_operaciones` (crea lo que falta, respeta el
mapeo existente aunque esté archivado, salta clave o tipo inexistente,
idempotente, tabla real y post-migrate).

## 19.0.57.60.0 — 2026-09-30

**Cambiado (bloque 2 de formularios 1/6, decisión «Inventario de
formularios» 2026-09-30: «un formato por modelo» pasa al bloque 2):**
`sgi.format.map` admite **varios formatos por modelo**. Cada mapeo puede
llevar un criterio: tipos de operación (`stock.picking.type`, para
transferencias, vales y órdenes de producción), centros de trabajo (órdenes
con una operación en ese centro), categorías de producto (con sus
subcategorías) y un filtro adicional sobre el registro; y una prioridad
(`sequence`). El registro imprime el primer mapeo con criterio que cumple y,
si no cumple ninguno, el **general** del modelo (el que no tiene criterio;
uno activo por modelo, restricción en Python que sustituye a
`unique(model_id)`). En «Formatos en documentos de Odoo» la ficha tiene la
sección «Cuándo aplica», la lista muestra la prioridad y el criterio, y la
búsqueda filtra generales, con criterio y por referencia. El pie «formato
controlado» se agrega al PDF nativo de la orden de producción
(`mrp.report_mrporder`). En las transferencias, el general sigue aplicando
solo a las salidas; un mapeo con criterio aplica a cualquier tipo.

**Migración (pre):** `migrations/19.0.57.60.0/pre-migrate.py` quita la
restricción SQL `unique(model_id)` con `IF EXISTS`. Los 22 mapeos de
producción quedan como generales (`is_general` se calcula verdadero en todos)
e imprimen la misma clave que antes. Nada se borra.

**Pruebas:** `test_format_map_varios` (general sin criterio, por tipo de
operación, prioridad, categoría con subcategorías, transferencia interna,
filtro adicional, un general por modelo, criterios inválidos, pie del reporte
y `_get_for_model`).

## 19.0.57.54.0 — 2026-09-30

**Cambiado (bloque 1 de formularios 5/5, decisión «Inventario de
formularios» 2026-09-30):** las actividades críticas de seguridad, salud y
ambiente del inventario (§3.3) quedan ligadas a su pantalla de Odoo («Menú de
Odoo» y «Dónde se ejecuta en Odoo») y a su formato vigente. Tabla
`SGI_SST_ACTIVITY_LINKS` (`models/sgi_sst_links.py`), método
`sgi.process.activity._sgi_link_activity_screens`:

| Actividad | Pantalla | Formatos |
|---|---|---|
| E2.18 permiso de alto riesgo | Permisos de trabajo de alto riesgo (nuevo) | (ya tenía 4) |
| E2.21 Protección Civil | Planes de emergencia | — |
| E2.23 aspectos ambientales | Aspectos ambientales (nuevo) | — (F-P-E01-01 mal asignada) |
| E2.28 estadística de incidentes | Incidentes y accidentes | F-P-S02-01, F-P-S02-02 |
| E2.30 programa anual de SST | Objetivos integrales | — |
| E2.33 control de plagas | Mantenimiento → Solicitudes | — |
| E2.34 controles operacionales | Aspectos ambientales (nuevo) | F-P-E03-01 |
| E2.35 consulta a trabajadores | Encuestas | F-P-A10-05 |
| E2.37 evaluación de auditores | Auditorías realizadas | — (F-IT-P-C06-03-01 no existe) |
| S4.34 aviso de accidente al IMSS | Incidentes y accidentes | F-P-S02-01 |
| S5.14 LOTO | Bloqueo y etiquetado (nuevo) | — |
| C5.23 MP o proveedor nuevo | Calidad → Puntos de control | F-P-C04-06 |
| C4.24 revisión del crudo | Manufactura → Órdenes de trabajo | F-IT-P-P01-08-03 |
| C2.39 certificado T-MEC | Inventario → Entregas | F-P-A16-04 |

No cambia «Dónde se hace» (`exec_channel`: hoy papel, correo o sistema
externo): pasa a Odoo cuando las pantallas tengan uso.

**Migración (post):** `migrations/19.0.57.54.0/post-migrate.py` llama al
método: actividad por numeral en la empresa del SGI, menú por XML ID y
formato por su clave vigente (los documentos del Dropbox no tienen XML ID).
Escribe cada campo **solo si está vacío**; lo que ya estaba queda en el log
con su valor, y lo escrito con «vacío → nuevo». Idempotente; nada se borra.
Esperado: 14 actividades (E2.18 conserva sus formatos). Marca «cambió» el
procedimiento de E2, S4, S5, C2, C4 y C5.

**Pruebas:** `test_sst_links` (solo lo vacío, respeta lo capturado, clave
inexistente, idempotente, menús de la tabla real y el post-migrate).

## 19.0.57.53.0 — 2026-09-30

**Agregado (bloque 1 de formularios 4/5):** ficha, búsqueda y menú propios
de los **hallazgos de auditoría** (`sgi.audit.finding`, ISO 9001 9.2: SGI →
Mejora → Auditorías → Hallazgos) y de las **evaluaciones del cumplimiento
legal** (`sgi.legal.evaluation`, ISO 14001/45001 9.1.2: SGI → Dirección →
Evaluaciones de cumplimiento legal). Antes solo tenían lista dentro de su
auditoría o requisito y Odoo armaba una ficha genérica. Hallazgos: filtros de
NC (mayores), observaciones, oportunidades, sin disposición, NC sin generar,
internas y externas; agrupar por auditoría, tipo, proceso y cláusula; botón
«Generar NC» en la ficha. Evaluaciones: filtros «No cumple o parcial»,
«Cumple», «Este año» y por fecha; agrupar por requisito, resultado y año.
Las dos acciones son de consulta (`create: False`): el hallazgo nace en su
auditoría y la evaluación en «Registrar evaluación» del requisito, que
actualiza su estado y levanta la NC. Nombres legibles
(`_compute_display_name`) en las dos. Vistas en archivo propio
(`sgi_audit_finding_legal_eval_views.xml`), sin herencias.

**Pruebas:** `test_hallazgos_evaluaciones` (ficha y búsqueda propias, menús
con su acción, nombres legibles).

## 19.0.57.52.0 — 2026-09-30

**Agregado (bloque 1 de formularios 3/5):** bloqueo y etiquetado de
energías, LOTO (`sgi.loto`, NOM-004 e ISO 45001 8.1, S5.14; P-A20) en SGI →
Seguridad y ambiente → Bloqueo y etiquetado. Equipo
(`maintenance.equipment`), orden de mantenimiento y permiso de trabajo
opcionales, **fuentes de energía** (eléctrica, neumática, hidráulica,
mecánica, térmica, química, gravitacional) con su punto de bloqueo, **un
candado y una tarjeta por trabajador** (uno por persona), aviso a los
afectados y **energía cero comprobada** (cómo y quién). Flujo borrador →
bloqueado → retirado (o cancelado): sin todo lo anterior no se aplica; ya
aplicado, fuentes y candados no se editan; **cada trabajador retira su propio
candado** («Retirar mi candado»; el Jefe MAST puede hacerlo por él y queda
quién); el retiro exige que no quede ningún candado, el aviso al responsable
del área y las condiciones. Un bloqueo aplicado no se cancela. Retirado es
evidencia (solo MAST reabre). Ficha, lista (abre en «Equipos bloqueados»),
búsqueda por equipo o trabajador, chatter, folio `LOTO-AAAA-`, ACL, regla por
empresa y reporte con pie de formato (`format_map_loto`: P-A20 no tiene
formato propio; el pie imprime el procedimiento hasta que MAST dé de alta el
formato).

**Pruebas:** `test_loto` (requisitos para aplicar, candados bloqueados al
aplicar, cada quien retira el suyo, retiro, candado de evidencia, un candado
por trabajador y reporte).

## 19.0.57.51.0 — 2026-09-30

**Agregado (bloque 1 de formularios 2/5):** permiso de trabajo de alto riesgo
(`sgi.work.permit`, ISO 45001 8.1, E2.18; sustituye al permiso único
F-P-A14-03 que citan P-A19 y P-A24) en SGI → Seguridad y ambiente →
Permisos de trabajo de alto riesgo. Solicitante, área, lugar, equipo y orden
de mantenimiento, tipo (alturas, espacio confinado, en caliente, eléctrico,
otro), personal o contratista, peligros, EPP y **verificaciones** (se
siembran por tipo según NOM-009, 033, 027 y 029; se pueden quitar o
agregar). Flujo borrador → solicitado → autorizado → cerrado (o cancelado):
se solicita con peligros, quién ejecuta, jefe del área y todas las
verificaciones contestadas sin ningún «No»; ya solicitado, las verificaciones
no se tocan. **Dos autorizaciones selladas** (usuario y hora) de **personas
distintas**: el jefe del área indicado (o el Jefe MAST) y Seguridad (Jefe
MAST; no hay grupo de coordinador de seguridad). Vigencia con aviso de
«vencido»; el cierre pide las condiciones del área. Cerrado o cancelado es
evidencia (solo MAST lo reabre, y reabrir borra las autorizaciones). Ficha,
lista, búsqueda (por autorizar, me toca autorizar, vencidos sin cerrar),
chatter, folio `PTAR-AAAA-`, ACL, regla por empresa y reporte con pie de
formato en vivo (`format_map_work_permit`, clave F-P-A14-03, sin documento
en Documentos: imprime la clave sin revisión hasta que MAST lo dé de alta).

**Pruebas:** `test_work_permit` (verificaciones por tipo, fechas, flujo
completo con permisos, dos personas distintas, vencido y reporte).

## 19.0.57.50.0 — 2026-09-30

**Agregado (bloque 1 de formularios, seguridad, salud y ambiente, 1/5):**
matriz de aspectos e impactos ambientales (`sgi.env.aspect`, ISO 14001
6.1.2, E2.23) en SGI → Seguridad y ambiente → Aspectos ambientales. Un
renglón por aspecto de una actividad: proceso, área, actividad, tipo de
aspecto, impacto y condición (normal, anormal, emergencia). Severidad ×
frecuencia (1 a 5) da el nivel bajo, moderado (desde 5), severo (desde 10) o
crítico (desde 16), umbrales en `quimibond_sgi.aspect_moderado/severo/critico`;
es **significativo** con nivel moderado o mayor o con requisito legal
aplicable. Un aspecto significativo no queda «Evaluado» sin su control
operacional (texto o documento, E2.34). «Registrar evaluación» sella la
revisión y programa la siguiente (12 meses por default); «Tratar como riesgo»
crea el riesgo del instrumento «Aspecto ambiental» (`sgi.risk`) donde viven
las acciones, sin duplicar el modelo de riesgos. Ficha, lista, búsqueda
(significativos, sin control, revisión vencida, archivados), chatter, folio
`AA-`, ACL (Usuario escribe, Auditor lee, MAST todo), regla por empresa y
reporte «Matriz de aspectos ambientales» con el pie de formato en vivo
(`format_map_env_aspect`, clave F-P-E01-01, la que cita P-E01; hoy esa clave
está en «Evaluación luminaria» y se corrige en el bloque 3).

**Pruebas:** `test_env_aspect` (significancia, umbral por parámetro, control
exigido, riesgo de tratamiento, archivo y reporte).

## 19.0.57.44.0 — 2026-09-30

**Cambiado (pulido de vistas, bloque 3: documentos e impresos):**
- **Alta de documento (V-A08):** lo primero y en grande es el título
  (`name`, con la guía «Título del documento, sin clave»); la clave va
  después. El título limpio calculado (sin clave ni extensión) se muestra en
  gris solo si difiere de lo escrito. Sale el placeholder «Nombre del
  archivo». Acuses del documento de 40 en 40 (V-B11).
- **Pie de formato controlado (V-M08)** en los reportes que lo perdieron en
  57.20.0: plan e informe de auditoría, investigación de incidente, AMEF,
  NEWS, retención, matriz de riesgos, matriz de competencias, matriz de
  cumplimiento, lista maestra (general y por proceso) y matriz legal. Usa el
  pie en vivo de `sgi.format.map`: nuevo `sgi_footer_label(registro, ref)`
  que sirve también para modelos sin el mixin de formato; sin mapeo no pinta
  nada. **Datos (noupdate, solo altas):** mapeos por referencia
  `format_ref_audit_plan` (F-P-G03-03), `format_ref_audit_report`
  (F-P-G03-07), `format_ref_master_list` (F-P-G01-03) y `format_ref_news`
  (F-P-G01-16), claves tomadas del propio SGI; MAST liga su documento en
  «Formatos en documentos de Odoo». Los demás imprimen el pie en cuanto MAST
  mapee su modelo. Quedan sin pie el 8D (su modelo es la NC y mostraría la
  clave del reporte de NC) y las hojas de firmas (van dentro de otro PDF).
- **Reporte de NC (V-M09):** en una alerta sin folio dice que no es una NC
  del SGI (y cómo escalarla) en lugar de salir vacío; en la NC, «Sin
  desviación registrada», «Sin análisis de 5 porqués registrado», «Sin
  acciones registradas» y «Eficacia aún sin verificar» en lugar de huecos.
- **Impresos sin «False» (V-B01):** `t-esc` → `t-out` en 7 reportes; la
  retención usa `t-field` para tipo y disposición; los nombres de archivo de
  los PDF y la cantidad de EPP no imprimen «False» cuando falta el dato.

**Pruebas:** `test_vistas_pulido.TestDocumentosEImpresos`.

## 19.0.57.43.0 — 2026-09-30

**Cambiado (pulido de vistas, bloque 2: piso y vencimientos):**
- **Checklists de hoy (V-A04):** la tableta abre una **ficha propia** de la
  hoja (`sgi_checklist_request_view_form`, primaria, prioridad 90; no es
  herencia): equipo como título, plantilla y día, la lista de puntos arriba
  con la respuesta como botones grandes (Bien / Falla / No aplica) y
  «Terminar checklist» en el encabezado. «Checklists de hoy» abre primero un
  kanban con una tarjeta grande por equipo o unidad; «Hojas de checklist» usa
  la misma ficha. La ficha estándar de Mantenimiento no cambia.
- **Medición (V-A06):** un solo botón principal por estado: «Marcar
  capturado» en Pendiente y «Validar» solo en Capturado y solo para quien
  puede validar (campo `sgi_can_validate`: responsable del indicador o Jefe
  MAST). **Seguridad:** el servidor revisa lo mismo también en una escritura
  directa del estado (`write`), no solo en el botón, y responde con
  `AccessError`.
- **Mis indicadores (V-A07):** lista propia (`sgi_indicator_view_list_mine`):
  clave, nombre, unidad, último valor, semáforo, próxima captura con días
  restantes y los botones «Capturar» (abre en ficha la medición pendiente más
  antigua), «Mediciones» y «Tendencia».
- **Equipos de medición (V-A09):** lista y búsqueda propias: última y próxima
  calibración (días restantes), semáforo y «NO USAR» visibles, renglón rojo
  si no se debe usar; filtros «Vencido o por vencer» (por defecto),
  «Vencido», «Por vencer», «No usar» y fecha de próxima calibración.
- **Vencimientos (V-M01):** `remaining_days` en la próxima calibración, la
  próxima revisión de riesgos (salvo cerrados) y de documentos (salvo
  obsoletos), la próxima evaluación legal, el compromiso de las acciones
  (salvo terminadas) y la próxima fecha de estudios y exámenes.
- **Filtros por defecto (V-M02):** incidentes abiertos, auditorías abiertas,
  EPP sin firmar, simulacros programados, recorridos CSH en borrador,
  mediciones pendientes y mías, acuses pendientes, requisitos que no cumplen o
  sin evaluar, calibraciones de equipos por vencer (filtro nuevo).
- **Filtros de fecha nativos y «Míos» (V-M03):** incidentes, auditorías,
  calibraciones, simulacros, EPP, mediciones, requisitos legales, acciones
  (compromiso) y cambios documentales; «Míos» en incidentes (reporté o tengo
  una acción), auditorías (líder, auditor o auditado), requisitos legales,
  cambios documentales, y «De mis procesos» en riesgos y AMEF. En la NC,
  «Este año» (dominio fijo) pasa a filtro de fecha sobre la fecha de la NC.
- **Responsiva de EPP (V-M11):** el texto libre solo aparece si ya trae algo
  y no hay renglones; lo nuevo se captura en los renglones.

**Pruebas:** `test_vistas_pulido.TestPisoYVencimientos` (validar por botón y
por escritura, «Capturar», vistas del piso, cada `search_default_*` existe).

## 19.0.57.42.0 — 2026-09-30

**Cambiado (pulido de vistas, bloque 1: V-M04, V-M06, V-M07, V-M15):**
- **La misma lista de acciones en las 7 fichas** (NC, incidente, riesgo,
  AMEF, simulacro, objetivo y medición): tipo, qué, responsable con avatar,
  compromiso con días restantes (se oculta al terminar), terminada, avance,
  estado como pastilla; renglón rojo si vencida y gris si terminada. En
  objetivo y medición el tipo sigue oculto.
- **Botones con el mismo nombre:** «Regresar a borrador» (antes también «A
  borrador» y «Volver a borrador»), «Marcar obsoleta» en la política (antes
  «Obsoletar»), «Terminar» en la ficha de acción (antes «Marcar terminada»).
  En cada estado el siguiente paso es el único primario y va primero:
  «Planificar» y «Elaborar informe» en la auditoría, «Cerrar» en la revisión
  por la dirección, el programa de auditorías y el riesgo controlado;
  «Registrar evaluación» del riesgo pasa después de los pasos del flujo.
- **Nombre como título** en las 5 fichas que solo mostraban el folio: PPAP
  (producto), estudio MSA (equipo), simulacro (plan de emergencia),
  auditoría («Auditoría interna · procesos o cliente · fecha») y revisión por
  la dirección («Revisión por la dirección · periodo»); el folio va debajo.
  Campos calculados sin guardar `sgi_heading` en auditoría y revisión. El
  programa de auditorías dice «Programa de auditorías 2026»; acción y
  actividad llevan su título en `<h1>`.
- **Actividad:** el cumplimiento de la medición deja de ser un `statusbar` en
  el encabezado y va como pastilla junto al título, igual que la salud del
  proceso.

**Pruebas:** `test_vistas_pulido.TestFichasCoherentes`.

## 19.0.57.41.0 — 2026-09-30

**Cambiado (pulido de vistas, bloque 1: V-A03):** los seis registros que se
cierran como evidencia (incidente cerrado, riesgo cerrado, auditoría y
revisión por la dirección cerradas, PPAP aprobado, simulacro realizado)
muestran sus campos **en solo lectura** y una cinta («Cerrado», «Aprobado»,
«Realizado»), en lugar de dejar editar y rechazar al guardar. Campo calculado
`sgi_is_locked` en `sgi.base.mixin`, con la misma regla que el candado de
`write()`: el Jefe MAST sigue editando. En el incidente, además, fuera de
Jefe MAST y Salud ocupacional solo lo edita quien lo reportó y mientras siga
«Reportado» (igual que la regla de registro).

**Seguridad (V-A05, D-009, decisión de Jose):** aprobar, dar interino o
rechazar un PPAP (y regresarlo a preparación desde esa decisión), cerrar y
reabrir un riesgo, y marcar obsoleto (o sacar de obsoleto) un AMEF, un plan
de control o un plan de emergencia lo hacen **solo el Jefe MAST y el dueño
del proceso**. En la vista, los botones llevan `groups` (Jefe MAST y Dueño de
proceso); en el servidor, `write()` revisa que el usuario sea Jefe MAST o
dueño del proceso **del registro** y si no, `AccessError` con el motivo. El
proceso sale de `process_id` (riesgo, AMEF), de sus AMEF (plan de control) o
de los AMEF y planes de control de sus elementos (PPAP); el plan de emergencia
no tiene proceso, así que lo decide solo el Jefe MAST. El dueño puede
reabrir su riesgo cerrado (solo el estado; el resto sigue cerrado). Un AMEF,
plan de control o plan de emergencia obsoleto ya no pasa directo a
«Vigente»: primero «Regresar a borrador» (reservado), después «Marcar
vigente». Mensajes del candado en «usted».

**Pruebas:** `test_vistas_pulido.TestCerradoYDecisiones` (solo lectura por
usuario y estado, incidente del reportante, riesgo, PPAP y obsoletos con
usuario raso, dueño y Jefe MAST).

## 19.0.57.40.0 — 2026-09-30

**Cambiado (pulido de vistas, bloque 1: V-A01 y V-A02):** la ficha de NC
tiene **un solo juego de pestañas**: «Desviación y análisis», «Correcciones y
acciones», «Cliente», «Ligas SGI» y «Verificación y cierre» van dentro del
notebook estándar de Calidad, antes de «Descripción»; «Proveedor» al final.
En una NC con folio se ocultan las páginas de texto libre «Acciones
correctivas» y «Acciones preventivas» de Calidad (salvo que ya traigan
texto): las acciones son las del SGI. Los metros reclamados pasan a la
pestaña «Cliente» (antes una pestaña «Reclamación (SGI)» en toda alerta,
también en las del piso). Un solo aviso arriba: NC del SGI y formato
controlado juntos. Datos SGI, responsabilidades y plazos van antes de las
pestañas. Salen las herencias `sgi_quality_alert_view_form_kpi` y
`sgi_format_banner_quality_alert` (de 3 herencias sobre la ficha de Calidad a
1); Odoo las borra al terminar la actualización.

**Agregado (V-A02):** «No conformidades» abre su **lista y kanban propios**
(`sgi_nc_view_list`, `sgi_nc_view_kanban`, prioridad 90 para no ganarle a las
de Calidad en su app): folio, título, proceso, clasificación, responsables,
los tres plazos (contención, causa raíz, plan) como semáforo, etapa y fecha;
renglón en rojo si algún plazo venció. Filtro «Plazo vencido» con el campo
calculado buscable `sgi_deadline_overdue`.

**Pruebas:** `test_vistas_pulido` (ficha con un notebook, lista y kanban de la
acción, filtro «Plazo vencido»).

## 19.0.57.37.0 — 2026-09-30

**Corregido (entrega 10, `e10-manuales`):** el docstring de `sgi.policy`
(57.32.0) decía que los acuses se generan al hacerla vigente; se generan con
«Generar acuses» sobre el documento controlado ligado. Solo texto.

## 19.0.57.36.0 — 2026-09-30

**Cambiado (entrega 10, `e10-help`: D-29, K-019):** el texto de las ayudas
de pantalla vacía pasa a «usted» (57.21.0 solo había cambiado los títulos):
16 acciones que aún decían «elige», «pulsa», «tu acuse», «cuando tengas
uno», «si cambias…», etc. (desglose de mediciones, quién ejecuta, COA
recibidos, partes interesadas, planes de control, documentos vigentes,
formatos anteriores, checklists de hoy, indicadores, mis indicadores,
cumplimiento de procedimientos, mapa de procesos, evaluación de
proveedores, acciones correctivas). Fuera el emoji de «Sin brechas de
competencia» y el ID de plan «(D-17)» de la ayuda de acciones. Solo texto.

## 19.0.57.35.0 — 2026-09-30

**Cambiado (entrega 10, `e10-help` 3/3: C-018, K-019, D-29):** `help` en
los 307 campos de prioridad alta restantes: **Dirección e indicadores**
(indicador, medición, desglose, términos de fórmula, objetivos, política,
revisión por la dirección, tablero), riesgos, requisitos legales y partes
interesadas, seguridad y ambiente (incidentes, emergencias, simulacros,
estudios y exámenes, EPP, checklists), metrología, AMEF, PPAP, planes de
control, proveedores, COA, eficiencias de personal y Ajustes del SGI. En
los campos que otra clase redefine (`calc_mode`, `direction`, `state` de la
medición) el `help` va en la definición base. Con esto quedan los 589
campos de prioridad alta que seguían sin ayuda (los otros 19 de la lista de
608 ya no existen o ya la tenían). También se acorta la ayuda de «Fin de
piloto» en el cambio documental. Solo texto de ayuda: ningún cambio de
comportamiento.

## 19.0.57.34.0 — 2026-09-30

**Cambiado (entrega 10, `e10-help` 2/3: C-018, K-019, D-29):** `help` en
los 175 campos de prioridad alta de **Procesos y Mi procedimiento** que no
lo tenían: proceso, etapa, actividad, rol, liga, entregable, estadísticas
de ejecución, faltantes de especificación, propuestas de cambio, Mis
pendientes, Mi procedimiento (pantalla, chequeo y campos del empleado y del
puesto), documentos controlados, acuses y cambios documentales
(`approval.request`). En «usted», para quien llena el campo. Solo texto de
ayuda: ningún cambio de comportamiento.

## 19.0.57.33.0 — 2026-09-30

**Cambiado (entrega 10, `e10-help` 1/3: C-018, K-019, D-29):** `help` en
los 102 campos de prioridad alta de **Mejora** que no lo tenían: NC
(`quality.alert`, plazos por etapa, respuesta al cliente y al proveedor),
acciones, auditorías, programa y hallazgos, reclamaciones
(`helpdesk.ticket`), mejora continua (`project.task`), solicitud de
desarrollo (`project.project`), recorridos de la CSH y cierre forzado.
Redactados para quien llena el campo, en «usted» (el tratamiento del
español de Odoo), sin claves del Dropbox; los calculados dicen de dónde
salen. Solo texto de ayuda: ningún cambio de comportamiento.

## 19.0.57.32.0 — 2026-09-30

**Cambiado (entrega 10, `e10-docs-generadas`: K-017):** docstring de 1 a 3
líneas en los 83 modelos propios que no tenían (qué representa, quién lo
crea, qué lo cierra), incluidos los de `quimibond_sgi_knowledge` y
`quimibond_sgi_mapa`, y en los 8 métodos `cron_*` base sin docstring (NC,
documentos, NEWS, programa de auditorías, competencias, proveedores, riesgos
y medición de actividades). De ahí salen las columnas «Qué es» y «Qué hace»
de `docs/sgi/tecnica/` (`tools/sgi_docs.py`, regenerado). Solo texto: ningún
cambio de comportamiento.

## 19.0.57.31.0 — 2026-09-30

**Cambiado (entrega 10, `e10-readme-y-reglas`: K-003, K-004, K-013…K-016,
A-021, A-024, A-028…A-030, C-020, C-023, H-017, F-018, F-019, F-021):**
`README.md` nuevo de menos de 150 líneas (qué es, normas y su estado según
D-28, instalación vacía, menú, grupos, satélites y lo que el núcleo les
garantiza, instalar y probar con `--test-tags`, reglas para programar y
dónde está la documentación). El README anterior (2,784 líneas) pasa entero
a `docs/historico/sgi/README_quimibond_sgi_hasta_57.30.0.md`. `description`
del manifest reescrita y licencia **OPL-1** (D-31); lo mismo en los
satélites, con su propio bump: `quimibond_sgi_knowledge` 1.0.1,
`_mapa` 1.1.1, `_pesaje` 5.2.1 (la tolerancia se describe como parámetro,
ya no «±3 kg» fijo), `_plm` 3.0.1, `_revisado` 4.2.2 y `_studio` 1.0.2.
README corto en `_mapa`, `_pesaje`, `_plm` y `_revisado`. Párrafo del SGI
en `CLAUDE.md` y sección SGI en `docs/RUNBOOK_DESPLIEGUE.md` (se quita la
deuda falsa de «6 claves de config con doble declaración»). Sin cambios de
código ni de datos.

## 19.0.57.30.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 9/9: A-008):** la pestaña
«SGI» del puesto (roles, familia, vacante y los botones de «Mi
procedimiento») y el botón «Actividades SGI» van en la herencia del puesto
(`sgi_integration_views.xml`); el botón «Ver su procedimiento» en la del
empleado; el filtro «Mis actividades» con todos los roles del puesto y su
familia en la búsqueda de actividades; «Ir a hacerlo», «Ver registros
recientes», «Ver instructivo» y «Registrar hallazgo» en el encabezado de la
ficha de actividad. Sale `views/sgi_structure_views.xml`. **Con esto el SGI
ya no hereda ninguna vista propia** (de 32 herencias propias a 0; sobre
vistas del SGI quedan solo las de los satélites). Mismo resultado en
pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_hr_job_view_form_roles` (15007),
`sgi_hr_job_view_form_my_procedure` (15039),
`sgi_hr_employee_view_form_my_procedure` (15040),
`sgi_process_activity_view_search_my_procedure` (15041),
`sgi_process_activity_view_form_structure` (15044). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.29.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 8/9: A-008):** la solicitud de
mantenimiento de origen en la NC, los acuses y la orden de producción en el
albarán y el plan de control en el producto van en su vista base.
`sgi_links_views.xml` ya no hereda vistas propias. Mismo resultado en
pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_quality_alert_view_form_links` (15099),
`sgi_stock_picking_view_form_links` (15102),
`sgi_product_template_view_form_links` (15097). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.28.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 7/9: A-008):** la NC a
proveedor (botón «Enviar al proveedor» y pestaña «Proveedor») va en la ficha
de NC; cliente o proveedor auditado en la auditoría; «Programa sugerido» y
la columna de proveedor en el programa de auditorías; los botones de firma
en albarán y producto; «Exige firmado» y encuesta en el entregable. Todo en
su vista base; `sgi_supplier_audit_sign_views.xml` queda con el asistente de
firma y las herencias sobre vistas de otros módulos. Mismo resultado en
pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_quality_alert_view_form_supplier` (15070),
`sgi_audit_view_form_pr6` (15071), `sgi_audit_program_view_form_pr6`
(15072), `sgi_stock_picking_view_form_sign` (15077),
`sgi_product_template_view_form_sign` (15079),
`sgi_deliverable_view_form_pr6` (15080). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.27.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 6/9: A-008):** la tarea del
desarrollo en AMEF, plan de control y PPAP, «Proveedor crítico» en la
pestaña SGI del proveedor y «Categorías de proveedores críticos» en Ajustes
van en su vista base. `sgi_links_views.xml` queda con sus herencias sobre
vistas de OTROS módulos (y las tres sobre NC, albarán y producto que salen
en 8/9). Mismo resultado en pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_fmea_view_form_links` (15092),
`sgi_control_plan_view_form_links` (15093), `sgi_ppap_view_form_links`
(15094), `sgi_res_partner_view_form_links` (15104),
`sgi_settings_view_form_links` (15105). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.26.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 5/9: A-008):** el nivel del
indicador (tablero de Dirección) va en la ficha del indicador y el banner
«formato controlado» en la ficha de la revisión por la dirección, cada uno
en su vista base. Mismo resultado en pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_indicator_view_form_level` (15065),
`sgi_format_banner_mgmt_review` (14740). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.25.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 4/9: A-008):** el plan de
acción de la medición roja y la ventana de la medición (ficha y lista de
mediciones, ficha del indicador) van en `sgi_indicator_views.xml`, y
«Validar mediciones del periodo» en la ficha de la revisión por la
dirección. Sale `views/sgi_indicator_plan_views.xml`. Mismo resultado en
pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_measure_view_form_plan` (15030),
`sgi_measure_view_list_plan` (15031), `sgi_indicator_view_form_window`
(15032), `sgi_management_review_view_form_validate` (15033). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.24.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 3/9: A-008):** la trayectoria
del indicador (pestaña «Trayectoria», meta final, rango y fecha de arranque)
y el rango de la medición van en su vista base (`sgi_indicator_views.xml`).
Sale `views/sgi_indicator_trajectory_views.xml`. Mismo resultado en
pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_indicator_view_form_trajectory` (15034),
`sgi_measure_view_form_trajectory` (15035). Idempotente; solo `ir_ui_view` + `ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.23.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 2/9: A-008):** la pestaña
«Fórmula» de la ficha del indicador y el bloque «Fórmula en paralelo» de la
medición van en su vista base (`sgi_indicator_views.xml`);
`sgi_indicator_formula_views.xml` queda solo con la lista de términos. Mismo
resultado en pantalla.

**Migración (pre):** borra con `migrations/herencias_propias.py` las
herencias integradas: `sgi_indicator_view_form_formula` (15028),
`sgi_measure_view_form_formula` (15029). Idempotente; solo `ir_ui_view` +
`ir_model_data`.

**Pruebas:** `test_herencias_propias` (las integradas ya no existen).

## 19.0.57.22.0 — 2026-09-30

**Cambiado (entrega 5, `e5-herencias-propias` 1/9: A-008):** el pie
«formato controlado» de los reportes de No Conformidad, Certificado de
calidad y Acta de revisión por la dirección va dentro de cada plantilla (se
imprime igual). `report/sgi_format_footer.xml` queda solo con el pie y sus
herencias sobre reportes de OTROS módulos (venta, compra, albarán).

**Migración (pre):** `migrations/herencias_propias.py` (nuevo, lo cargan por
ruta los pre-migrate de 57.22.0 en adelante) borra de la base las herencias
propias integradas, antes de cargar los XML: solo `ir_ui_view` +
`ir_model_data` de vistas `quimibond_sgi.*` con `inherit_id`; una vista de
otro módulo que heredara una de ellas se re-apunta al padre y se avisa en el
log. Idempotente. Aquí: `report_nc_document_sgi` (14745),
`report_coa_document_sgi` (14746) y `report_mgmt_review_document_sgi`
(14747).

**Pruebas:** `test_herencias_propias` (las integradas ya no existen; el
borrado es idempotente y re-apunta una hija ajena; el pie sigue en el
reporte de NC).

## 19.0.57.21.0 — 2026-09-30

**Cambiado (entrega 5, `e5-fichas-y-busquedas`: D-007, D-008, D-013,
E-016, D-014, D-015, E-015, D-016, D-021; D-17 y D-29):**

- **Confirmación en 29 botones** que cierran, reabren, obsoletan, rechazan o
  regresan a borrador (D-008): proceso («sus actividades salen de Mi
  procedimiento»), programa y auditoría, revisión por la dirección,
  recorrido CSH, eficiencias, plan de control, plan de emergencia y
  simulacro, AMEF, política, incidente, riesgo, ficha de máquina, PPAP e
  indicador («Regresar a prueba»). Mismo texto base: «¿Desea continuar?».
- **Búsquedas** (D-014): 14 modelos con búsqueda propia (programa de
  auditorías, recorridos CSH y revisión por la dirección con estado y año o
  fecha; política, objetivos, normas, áreas, tipos de documento, plantillas
  de checklist, categorías de riesgo, formatos impresos, familias de puesto,
  elementos PPAP con «Archivados»; fuentes de NC con encendidas/apagadas) y
  «Archivados» en partes interesadas, requisitos legales y fichas de
  máquina.
- **Filtros por defecto muertos** (D-013, E-016): fuera
  `search_default_recent`, `search_default_filter_recent` y
  `search_default_group_category`; la evidencia por cliente agrupa con
  `group_by` explícito.
- **Ayudas** (D-015, E-015): «Hojas de checklist» y «Análisis por empleado»
  tienen ayuda; la del Pareto ya no habla de «TEJIDO-*». Los títulos de las
  ayudas de pantalla vacía pasan a «usted» (D-29: el tratamiento del español
  de Odoo): «Cree el primer AMEF», «Registre…», «Dé de alta…».
- **Campos técnicos solo para MAST** (D-016): dominios, rutas y campos de
  fecha de medición de la actividad y del entregable, la columna del XML ID
  y los bloques «Texto de la versión anterior» de proceso y actividad.
- **NC** (D-021): «Desviación y análisis», «Correcciones y acciones» y
  «Verificación y cierre» se ocultan en alertas de calidad sin folio del SGI,
  como ya hacía «Cliente».
- **Acción correctiva con historial** (D-007): `sgi.action.line` hereda
  `mail.thread` (responsable, compromiso y fecha de terminada con
  seguimiento) y su ficha tiene chatter. D-17 ya estaba: «Acciones
  correctivas» muestra todas con filtros «De …» por origen.

**Pendiente de D-007 (no va aquí):** evidencia obligatoria al terminar una
acción (depende de las reglas de cierre del agente G: hoy terminar la
actividad espejo cierra la acción sin adjunto), título propio de la
auditoría (hoy solo folio) y la misma secuencia de estados en las seis
fichas.

**Migración:** ninguna; el update agrega las columnas de `mail.thread` a
`sgi_action_line`.

**Pruebas:** `test_fichas_busquedas` (búsquedas con filtros y «Archivados»
en los 17 modelos, `confirm` en los botones de estado de 7 fichas,
historial de la acción al terminarla).

## 19.0.57.20.0 — 2026-09-30

**Cambiado (entrega 5, `e5-nomenclatura-en-pantallas`: D-002, D-003, D-004,
D-020, E-006; decisión 1 y D-02):** la clave del Dropbox deja de estar
escrita a mano en pantallas y reportes. El `<h1>` del documento con el
título limpio ya venía de 56.32.0.

- **11 nombres de reporte** (menú Imprimir) sin clave: «Reporte de no
  conformidad», «Plan de auditoría», «Acta de revisión por la dirección»,
  «Cambio documental», «AMEF», «Investigación de incidente», «Boletín
  NEWS», «Certificado de calidad / Certificate of Analysis», «Imprimir
  procedimiento», «Lista maestra de documentos», «Matriz de competencias».
  El PDF del cambio documental se descarga como «Cambio documental - …».
- **Encabezados impresos** (NC, plan e informe de auditoría con «Reunión de
  apertura/cierre», acta de revisión, AMEF, incidente, NEWS, CoA, Mi
  procedimiento) sin clave. La identificación del formato sale solo del pie
  en vivo (`sgi_format_footer`, C-006); la lista maestra completa pierde su
  pie fijo «F-P-G01-03», que no tiene mapeo.
- **Textos de 12 vistas:** ayudas de auditoría, cambio documental, ficha de
  máquina, eficiencias, EPP del puesto, categorías de riesgo (ya no promete
  categorías sembradas), placeholder del entregable, ayuda de «Documentos»
  («El nombre es el título, sin clave»), «Encuesta DNC», grupo de
  referencias; claves de desarrollo fuera (NC-6, C4.19, «Master Spec»,
  «(SGI)» en «EPP requerido»); los indicadores de Ajustes se nombran
  («Indicador Requisiciones (CO-02)»).
- **Documento:** la pestaña «Migración a Odoo» pasa al final y solo la ve el
  Jefe MAST. En las listas de documentos del puesto (Mi procedimiento) va
  primero el título y después la clave.

**Migración:** ninguna (los reportes no son `noupdate`; el update renombra).

**Pruebas:** `test_nomenclatura_pantallas` (ningún reporte del SGI con clave
del Dropbox en el nombre ni en el archivo; render de NC, incidente, acta,
plan e informe de auditoría sin la clave escrita a mano).

## 19.0.57.19.0 — 2026-09-30

**Cambiado (entrega 5, `e5-reclamaciones`: D-006, D-010; decisión 9 de la
tanda 2):** qué equipo es de reclamaciones lo decide la marca nueva
`helpdesk.team.sgi_is_complaint` («Equipo de reclamaciones (SGI)», en la
ficha del equipo → Posventa, solo Jefe MAST), no el XML ID
`sgi_helpdesk_team_complaints`. La leen:

- el menú «Reclamaciones de clientes» (todos los equipos marcados; sin
  ninguno, lista vacía como antes);
- el indicador de reclamaciones (`reclamos_cliente`, CA-01) y su vista de
  evidencia: **empieza a contar los tickets reales** (15 en producción)
  donde antes contaba los 0 del equipo del SGI;
- el aviso de SLA vencido, la revisión por la dirección y el diagnóstico
  (que ahora avisa si hay tickets en equipos con «reclama» en el nombre sin
  marcar).

En el ticket, el bloque «Datos de la reclamación» y el botón «Generar NC»
solo salen en equipos marcados (antes salían en los 228 tickets de Sistemas);
«Generar NC» además pide Usuario SGI y el método se niega fuera de un equipo
marcado. Ayudas de «Reclamaciones de clientes» y «Mejora continua»
corregidas (hablaban de pestañas que no existen); sin «(SGI)» en los grupos.

**Migración (post):** marca el equipo del SGI y, en la empresa del SGI, los
equipos llamados exactamente «Reclamaciones entretelas» y «ATENCION A
CLIENTES» (esperado: 30, 2 y 14). Idempotente; ningún ticket cambia de
equipo. «Reclamación Industrial» (11, 0 tickets) no se marca.

**Pruebas:** `test_reclamaciones` (3 casos: marca idempotente por nombre,
«Generar NC» solo en equipos marcados, menú e indicador por la marca);
`test_audit_hardening` A1 crea su ticket en el equipo del SGI.

## 19.0.57.18.0 — 2026-09-30

**Agregado (entrega 8, `e8-checklist-pin`: I-005, D-08):** parámetro
`quimibond_sgi.checklist_pin_required` («PIN obligatorio para firmar
checklists» en Ajustes → SGI). **Apagado por default**: se firma como hasta
hoy y, si el empleado no tiene PIN, la hoja dice «(sin PIN registrado)».
Encendido: un empleado sin PIN en su ficha no puede firmar la hoja
(«Terminar checklist» lo detiene con el aviso para RH); con PIN, el PIN
tiene que coincidir, como siempre.

**Cómo encenderlo (cuando RH haya capturado los PIN):** Ajustes → SGI →
«PIN obligatorio para firmar checklists», o el parámetro
`quimibond_sgi.checklist_pin_required` = `True` en Ajustes → Técnico →
Parámetros del sistema. Antes, llenar «Quién lo llena» en cada plantilla. El
README (puesta en marcha, paso 8) lo documenta.

**Pendiente de D-08 (no es código):** la cuenta de tableta por área la
decide Jose y la crea Sistemas; el programador no crea usuarios.

**Datos de producción (auditoría I-005):** 163 de 165 empleados sin PIN; por
eso va apagado. **Migración:** ninguna.

**Pruebas:** `test_checklist_pin` (3 casos, datos propios: apagado firma sin
PIN, encendido no firma sin PIN y sí con el PIN correcto, ajuste en
pantalla).

## 19.0.57.17.0 — 2026-09-30

**Cambiado (entrega 8, `e8-rendimiento`: G-015, G-016, G-023):**

- **Mi equipo y la ficha del puesto leen cifras guardadas** (G-015). Campos
  nuevos en `hr.job`: `sgi_mp_hash_current` (huella de «Mi procedimiento»),
  `sgi_mp_job_late`, `sgi_mp_job_ok`, `sgi_mp_job_unmeasured`,
  `sgi_mp_job_total` y `sgi_mp_stats_at`. Los recalcula el cron de medición
  después de medir (03:00, cuando cambia el semáforo), el aviso semanal de
  «Mi procedimiento» (solo los puestos marcados) y la migración. Un cambio
  de roles, del texto de una actividad o una publicación marca el puesto «por
  recalcular» (`sgi_mp_stats_at` vacío, `_sgi_mp_touch_jobs`) y, mientras
  tanto, ese puesto se calcula al vuelo **sin escribir** (se puede leer desde
  una consulta de solo lectura). «Desactualizado» en la ficha compara contra
  la huella guardada. Límite conocido: un cambio de documento que no toca
  roles ni actividades (p. ej. la clave del procedimiento relacionado) se
  refleja en la siguiente corrida nocturna.
- **Fechas hábiles por corrida** (G-016): `sgi_calendar.SgiWorkdays` pide al
  calendario los días hábiles de toda la corrida del cumplimiento semanal
  UNA vez y resuelve `_sgi_due` en memoria (mismo resultado que
  `sgi_add_business_days`; fuera del rango usa la función normal). «Tiene
  salida» se pregunta en una consulta por lote en vez de una por registro.
- **Fusión de puestos** (G-023): `flush_all()` antes del SQL; después, los
  campos guardados calculados o relacionados que apuntan a `hr.job` (p. ej.
  `hr.employee.sgi_mp_job_id`, que sale de la versión) se recalculan y el
  «Mi procedimiento» guardado de los empleados del puesto que se queda se
  marca para recalcular (`_sgi_merge_recompute`).

**Rendimiento (razonado, sin medir; medir en staging con
`--log-level=debug_sql`):** un filtro de Mi equipo pasaba por
`_sgi_my_procedure_data()` de cada puesto (auditoría: 30-80 consultas por
puesto, más de 100 puestos → 3,000-8,000 consultas por clic); ahora lee
`hr.job` guardado (una lectura por lote) más los puestos por recalcular. El
cumplimiento semanal llamaba al calendario (`_work_intervals_batch`) por
cada registro de entrada de 90 días y por cada una de las 4 semanas, más un
`search_count` por candidato vencido; ahora una llamada al calendario por
corrida y una búsqueda por entrada y semana. El costo del cálculo completo
pasa a una vez por noche (03:00).

**Migración (`migrations/19.0.57.17.0/post-migrate.py`):** llena las cifras
guardadas de los puestos con roles y personas. Solo campos nuevos.

**Pruebas:** `test_rendimiento` (3 casos, datos propios: días hábiles en
memoria iguales al calendario con festivos, cifras guardadas y «por
recalcular», recálculo tras mover el puesto por SQL).

## 19.0.57.16.0 — 2026-09-30

**Corregido (entrega 8, `e8-medicion-robusta`: G-002, H-005, G-003, H-006,
G-019, H-016, G-026; J-009):**

- **Savepoint por medición** (G-002, H-005): `_sgi_measure_odoo` mide cada
  actividad en su savepoint y **también escribe** el resultado en uno propio
  (antes el `write` quedaba fuera y un error cortaba el paso completo). Lo
  que truena queda en «Avisos de medición» (`measure_warning`: «No se pudo
  medir: …») y en el log; antes `except Exception: pass`. Los pasos del cron
  de medición y la medición semanal de cumplimiento van por
  `sgi.cron._sgi_step`, y `sgi.activity.week.stat._sgi_compute` usa un
  savepoint por actividad. Las solicitudes de firma de acuses
  (`sgi_sign_elearning`) también.
- **Filtro de evidencia inválido → aviso** (G-003): un `measure_domain` que no
  se puede leer o que nombra un campo inexistente deja la actividad **sin
  semáforo** con «Filtro de evidencia inválido: <error>». Antes se tomaba
  como `[]`: contaba todo el modelo y salía verde.
- **Solo la empresa del SGI** (H-006, G-019, D-03): la medición de
  actividades, la evidencia que abre «Ver registros» y el cumplimiento
  semanal cuentan solo registros de `sgi.config._sgi_company()` (o sin
  empresa) cuando el modelo tiene `company_id` guardado. Los crons de
  evaluación de proveedores (recepciones), EPP y competencias
  (certificaciones y formación) filtran igual; calibración ya lo hacía.
- **Semanas en hora local** (G-007 c, completa 57.15.0): el cumplimiento
  semanal corta la semana a medianoche de México y compara «a tiempo» con la
  fecha local de la salida.
- **Procedimiento de otro proceso** (H-016): al entrar en vigor un proceso,
  si una actividad activa de OTRO proceso cita como «Procedimiento
  relacionado» un documento que se acaba de obsoletar, se avisa en el
  chatter de los dos procesos y con una actividad al dueño del otro proceso
  (o al Jefe MAST), clave `procedimiento_obsoleto_citado:<proceso>`. No se
  cambia la liga (decisión 3).
- **Reincidencia** (G-026): cancelar una NC o cambiarla de proceso recalcula
  la reincidencia de las NC posteriores de los procesos afectados.
- **Aprobador ≠ solicitante** (H-018, J-010): ya lo resuelve 57.13.0 (roles
  relativos); no se duplica. La carga por API sigue avisando sin rechazar
  hasta que Jose cargue el aprobador de E2.01, S4.03 y S6.07.

**Migración:** ninguna. La próxima corrida del cron de medición deja el aviso
en las actividades con filtro inválido (H-005: las 8 con registros y sin
semáforo deberían ganar semáforo o su motivo).

**Pruebas:** `test_medicion_robusta` (6 casos, datos propios: filtro roto,
error en una actividad sin tumbar a la siguiente, segunda empresa no cuenta
(J-009), cron completo con una actividad rota, aviso de procedimiento de
otro proceso y reincidencia recalculada).

## 19.0.57.15.0 — 2026-09-30

**Cambiado (entrega 8, `e8-zona-horaria-y-dias-habiles`: G-007, G-008,
G-009, G-020, G-022; decisión 4 de la tanda 2):**

- **Hoy en hora de México** (`sgi_calendar.sgi_today`): los crons corren como
  OdooBot, sin zona, y `context_today` les daba la fecha UTC (desde las 18:00
  de México ya era «mañana»). Ahora el «hoy» de los crons del SGI, de la
  medición de actividades, del cumplimiento semanal, de los checklists y de
  los crons mensual y semanal de indicadores sale de la zona del calendario
  de días hábiles (`quimibond_sgi.business_calendar_id`; si no tiene,
  America/Mexico_City). **OdooBot no se toca.** Las semanas de
  `sgi.activity.week.stat` y `exec.stat` se cortan en hora local.
- Un datetime se pasa a **fecha local** antes de contar días hábiles
  (`sgi_business_days`, `sgi_add_business_days`): una entrada de las 19:00 ya
  no cuenta como del día siguiente.
- **Escalamientos en días hábiles** (los parámetros siguen siendo números y
  en Ajustes dicen «días hábiles»): NC sin acción (5 / 3), acción vencida al
  jefe y a Dirección (7 / 15), plazo de NC vencido a MAST (3), acuse
  pendiente (7), captura de la medición mensual (4 hábiles después del día 1)
  y semanal (2 hábiles después del lunes). Los avisos de revisión bienal y de
  piloto siguen en días naturales (son anticipación, no plazo de trabajo).
- **Vencimiento en día inhábil se adelanta** al hábil anterior: el semanal
  por día de la semana y el de mes y día (sin salirse del periodo: un lunes
  festivo pasa al martes) y el «día 10» del plan de una medición roja.
- **Cadencia trimestral, semestral o anual sin mes y día** marca «Sin plazo»
  (`no_timing`, error) aunque una entrada tenga plazo (G-008 a): su «cuándo»
  salía vacío en Mi procedimiento.
- **Corrida mensual una sola vez** (G-020): `quimibond_sgi.monthly_run_done`
  = AAAA-MM; ya no repite foto, trayectorias y cierre de presupuestos cada
  día mientras el mes anterior siga sin mediciones.
- **Checklists** (G-022, decisión 14): `schedule_date` a las 08:00 hora local
  (antes 08:00 UTC = 02:00 en México); los festivos no generan hoja; la
  semanal sale el primer día hábil de la semana en que corra el cron (una
  vez por semana, se recupera si el lunes falló); una plantilla sin equipos
  avisa al Jefe MAST (`checklist_sin_equipos`, un aviso por plantilla que se
  cierra solo al cargar equipos). `sgi.checklist.template` lleva chatter y
  actividades (`mail.thread`, `mail.activity.mixin`).

**Migración (`migrations/19.0.57.15.0/post-migrate.py`):**

1. `sgi.config._sgi_load_holidays([2026, 2027, 2028])`: los festivos de la
   LFT, art. 74 (1-ene, primer lunes de febrero, tercer lunes de marzo,
   1-may, 16-sep, tercer lunes de noviembre, 25-dic y el 1-oct sexenal), como
   ausencias globales del calendario del parámetro (21). Idempotente, nada se
   borra, no se cae al calendario de la compañía. **Los del contrato
   colectivo no están documentados en el repositorio: no se cargan**; RH los
   confirma y se capturan a mano en el calendario 21.
2. `sgi.config._sgi_move_cron_hours()` (los crons son `noupdate`, A-007):
   checklists 05:30 y medición de actividades 03:00 hora de México; legal,
   contexto, participación, Sign/eLearning y Mi procedimiento de las
   20:xx-22:xx de México a las 06:xx del mismo día UTC. Idempotente.
3. Las actividades de cadencia larga sin mes ni día recalculan faltantes.

**Datos de producción (MCP, solo lectura, 2026-09-30):** calendario 21
(America/Mexico_City) sin ausencias globales; ninguna ausencia global desde
2025 en ningún calendario; ningún empleado (calendarios 9, 13, 32) ni centro
de trabajo (9, 25, 31, 33) usa el 21. Crons 215 (checklists, 22:44 UTC), 196
(medición, 22:27), 197 y 199 (02:39), 198 (02:39, semestral), 200 (04:20) y
213 (04:40). Esperado: 21 festivos, 7 crons movidos, 15 actividades con el
faltante nuevo (C6.21, E1.01-E1.03, E1.10, E2.05, E2.12, E2.21, S3.21,
S4.22-S4.24, S4.26, S4.30, S6.02). Los crons 184 y 185 son de
`quimibond_ventas_presupuesto` y no se mueven aquí.

**Pruebas:** `test_zona_horaria` (10 casos, datos propios: calendario de
México con los festivos de 2027; vencimiento en sábado y en festivo, J-011).
`test_ola1` y `test_ola2` cuentan la antigüedad en días hábiles.

## 19.0.57.14.0 — 2026-09-29

**Agregado (indicadores 2, aprobado por Jose 2026-09-29, con sus decisiones
del mismo día):** siete modos de cálculo con detalle (numerador, denominador
y registros en la medición) en `models/sgi_indicator_ind2.py`. Definiciones
tomadas de la ficha de cada indicador en producción:

- **S2-01** `complementos_pago`: pagos de clientes del periodo (en proceso o
  pagados) aplicados a facturas PPD cuyo complemento
  (`l10n_mx_edi.document` en `payment_sent` del asiento del pago) se timbró a
  más tardar el **día 5 inclusive** del mes siguiente al pago, hora de
  México ÷ esos pagos. Sin complemento timbrado = fuera de plazo (se cuenta
  en la nota). Sin `l10n_mx_edi`, sin dato.
- **S1-05** `desviacion_precio_compra`: Σ **|precio pagado − precio de la
  OC|** × cantidad ÷ importe de esas líneas, en líneas de factura de
  proveedor publicadas (`in_invoice`, `display_type product`) con
  `purchase_line_id`; precios netos de descuento; la OC se convierte a la
  unidad (`uom._compute_price`) y moneda (a la fecha de la factura) de la
  factura, y la desviación a moneda de la compañía con el tipo de cambio de
  la línea. Valor absoluto: pagar de menos no compensa lo pagado de más. La
  nota separa pagado de más, pagado de menos y la cobertura (líneas e
  importe con OC contra el total del periodo).
- **C4-01** `ordenes_vencidas_48h` (definición nueva de Jose): foto al
  cierre de la semana. Órdenes de fabricación de la compañía abiertas
  (`state` en `confirmed`, `progress` o `to_close`; los borradores no
  cuentan) cuya fecha de fin programada venció hace más de 48 h ÷ órdenes
  abiertas. En Odoo 19
  `mrp.production.date_finished` es la fecha esperada mientras la orden no
  está hecha (`_compute_date_finished`) y pasa a la real al cerrarla, así
  que el pasado no se reconstruye: solo se mide en los 7 días siguientes al
  cierre de la semana (lo abierto al medir = lo abierto al cierre; las
  órdenes creadas después del cierre no cuentan); un periodo más viejo sale
  sin dato. No depende de las operaciones. Meta 10 % (aceptable 20 %), más
  bajo es mejor, con escalones trimestrales (trayectoria del SGI,
  `sgi.indicator.step`): 40 / 50 en T4 2026, 25 / 35 en T1 2027 y 10 / 20 en
  T2 2027; la medición se compara contra la meta de su trimestre.
- **C1-04** `desarrollos_vendidos`: artículos (`product.template`, archivados
  incluidos) de las categorías de `quimibond_sgi.finished_product_categ_ids`
  (319 «Producto Terminado» y sus hijas) dados de alta en el mismo periodo de
  hace 6 meses, con al menos un pedido de venta confirmado cuyo `date_order`
  cae en los 6 meses siguientes a su alta ÷ artículos de esa cohorte.
- **RH-01** `cobertura_plantilla`: por puesto con «Plantilla autorizada»,
  empleados que lo ocupan al cierre del periodo, hasta la plantilla ÷ suma de
  plantillas. Campo nuevo `hr.job.sgi_authorized_headcount` (seguimiento),
  en la ficha y la lista nativas del puesto
  (`views/sgi_hr_job_headcount_views.xml`).
- **S4-01** `bajas_registradas`: bajas del periodo (`departure_date`) con
  motivo cuya fecha de salida (y motivo, si tiene seguimiento) quedó
  registrada a más tardar el día hábil siguiente ÷ bajas. El registro sale
  del seguimiento **nativo** de Odoo 19 (`departure_date` y
  `departure_reason_id` ya tienen `tracking=True` en `hr.version` y en el
  empleado; no hizo falta heredarlos).
- **S6-02** `bajas_accesos_equipo`: bajas del periodo con «Retirar accesos»
  y «Recoger equipo» (de cómputo, como pide la ficha) hechas a más tardar el
  día hábil siguiente ÷ bajas. Fecha de hecho: el mensaje que publica
  `mail.activity._action_done` (`mail_activity_type_id`, subtipo
  Actividades); de respaldo, `mail.activity.date_done` de la actividad
  archivada. **No hay plan nuevo:** se usa el plan existente «Baja de
  personal» (id 5). `data/sgi_offboarding_plan_data.xml` (`noupdate`) solo
  trae tres tipos de actividad propios de empleado: «Retirar accesos»,
  «Recoger equipo» y «Recuperar EPP» (EPP no es equipo de cómputo y S6-02 no
  lo cuenta).

S2-01, S4-01 y S6-02 no se miden antes de que venza su plazo, ni C4-01 antes
de cerrar la semana: la medición queda «pendiente» con nota y el cron diario
la re-mide. El detalle de un modo puede traer `note` y se agrega a la nota de
la medición (`_sgi_measure_vals`). «Ver evidencia» de estos modos abre los
registros guardados. Parámetro nuevo sembrado: `finished_product_categ_ids`
= 319.

**Migración** (`post-migrate`), idempotente, sin borrar nada y con el valor
anterior en el log (y en el chatter del indicador):

1. `_sgi_update_ind2_fichas()`: S1-05, en `source`, «lista de precios del
   proveedor» → «precio de la orden de compra» (solo esa frase) y fórmula
   «|pagado − acordado| × cantidad ÷ compras del mes × 100». C4-01:
   nombre «Órdenes vencidas más de 48 horas», fórmula y fuente nuevas; si
   sigue en «más alto es mejor», pasa a «más bajo es mejor» con metas espejo
   10 / 20 (antes 90 / 80 a tiempo).
   `_sgi_c4_01_trajectory()` le carga los tres escalones trimestrales
   (marcados «corregido a mano» con su motivo, para que «Generar
   trayectoria» no los recalcule); un trimestre que ya tenga escalón no se
   toca y se avisa en el log.
2. `_sgi_adopt_offboarding_plan()`: en el plan «Baja de personal…» de
   empleados, «Desactivar usuario de Odoo, correo y accesos» toma el tipo
   «Retirar accesos» y «Recuperar EPP…» el tipo «Recuperar EPP»; se agrega
   «Recoger equipo de cómputo» (tipo «Recoger equipo», mismo responsable,
   secuencia y plazo que el de accesos). Sin plan o sin un renglón: aviso en
   el log y sigue.
3. `_sgi_activate_ind2()`: S2-01, S1-05, C4-01, C1-04, RH-01, S4-01 y S6-02
   pasan a su modo si siguen activos, en «manual» y sin términos de fórmula
   (criterio de E1-02 en 57.6.0); S6-02, sin «Medir desde», se mide desde el
   mes siguiente.

**Datos de producción (2026-09-29, lectura):** S2-01 agosto 76 pagos a PPD,
76 complementos a más tardar el 4-sep (100 %). S1-05 agosto 360 de 1,472
líneas con OC (24 %; 31 % del importe). C4-01 hoy: 394 órdenes confirmadas, en
proceso o por cerrar, 199 con fin programado vencido antes del 27-sep
(≈ 50 %); fuera, 39 borradores.
C1-04 cohorte de marzo 13 artículos. RH-01: ningún puesto con plantilla.
S4-01: 19 bajas desde julio, 13 sin motivo y la mayoría registrada semanas
después (carga del 7-sep). S6-02: cero actividades en empleados; plan 5 con
los renglones 18 (EPP) y 19 (accesos, Mariano Dominguez), ambos «Por hacer».

**Pruebas:** `test_indicadores_2` (9 casos, datos propios; `test_09` cubre
las fichas y los escalones de C4-01).

## 19.0.57.13.1 — 2026-09-29

**Corregido (las 7 fallas que quedaban de la primera corrida real, build de
desarrollo 38916808 sobre base nueva con demo):**

- **Pendiente, `test_role_audit.test_07`:** al archivar una actividad, el
  «Mi procedimiento» guardado del empleado sigue trayendo su rol en la prueba
  (builds 38918236 y 38978978), aunque `sgi.process.activity.write` ya marca
  el recálculo desde 56.7.0 y la lista del puesto ya lo excluye. Causa sin
  confirmar. Producción no tiene roles de actividades archivadas (0), así
  que hoy no afecta.
- **Pruebas:** `test_hierarchy.test_04` esperaba la navegación sin el mapa
  de procesos, que va primero desde 54.0.0.

- **Cambio documental firmado en Sign (56.17.0):** al enviar, el renglón del
  revisor (dueño del proceso) se agregaba al final de la caché de
  aprobadores, después del Jefe MAST que trae la categoría. Con aprobadores
  en orden, Aprobaciones dejó «pendiente» al Jefe MAST y «en espera» al
  revisor; al firmar el revisor, su aprobación fallaba (se registra y la
  firma no se revierte), el cron la reintentaba sin éxito y la solicitud
  quedaba atorada. Ahora se relee la lista en el orden de la secuencia antes
  de enviar, y la sincronización con Sign pasa a «pendiente» al aprobador que
  ya firmó si seguía en espera (Sign ya impuso el orden). **Producción
  (lectura, 2026-09-29):** la categoría 12 «Modificación de documento SGI»
  tiene firma y orden, pero no hay ninguna solicitud enviada a firma: nadie
  se atoró todavía.
- **Imprimir «Mi procedimiento» desde el perfil público:** la acción del
  reporte se armaba con sudo; Odoo trata a sudo como administrador y, en una
  compañía sin diseño de documento, devolvía el asistente «Configurar el
  diseño» a cualquier usuario. El puesto se sigue leyendo con sudo; la
  acción, con el usuario.

**Pruebas:**

- (B) `test_hierarchy` test_04: las actividades del PR-XH1 vigente con método
  de medición (`_sgi_check_procedure_measures`).
- (A) `test_mp_change` test_02: el numeral se calcula desde 29.2.0 (clave del
  proceso + paso): «4.1» queda como numeral anterior y la referencia es
  «ZMPC / ZMPC.01 …».
- (B) `test_my_procedure_ui` test_02: revisa la vista que abre la acción de
  Mi equipo (`sgi_my_team_view_hierarchy`, 54.4.0), no el organigrama por
  omisión de `hr.employee.public` (en Odoo 19 el nativo de RH). La acción
  siempre usó la suya: en producción el semáforo sí se ve. También llama a
  `action_open_my_team` en `hr.employee.public` (el modelo donde vive).
- (B) `test_sign_elearning`: el documento de la prueba nace vigente (lo lee
  todo usuario interno, 56.7.0); en Odoo 19 el Jefe MAST no leía el borrador
  ajeno sin carpeta.
- `test_role_audit` test_07: sin causa confirmada (el recálculo de lo
  guardado al archivar la actividad); se agregan aserciones intermedias para
  que la próxima corrida diga si falla la lista del puesto, la búsqueda del
  empleado o el recálculo. Ninguna aserción se quitó.

## 19.0.57.13.0 — 2026-09-29

**Corregido (entrega 8, primer bloque; H-018, J-010; decisión de Jose
2026-09-29 «roles relativos»):** los roles relativos de `sgi.activity.role`
se resuelven a personas (`models/sgi_relative_roles.py`):

- **Dueño del proceso**: el del proceso del REGISTRO que se pide o aprueba
  (`sgi_process_id`/`process_id`; si no, cualquier many2one o many2many
  guardado a `sgi.process`, p. ej. `sgi_affected_process_ids`; si no, un salto
  por el documento o la actividad ligados). Sin proceso en el registro, el de
  la actividad (antes siempre el de la actividad).
- **Solicitante**: `request_owner_id`, `sgi_requester_id`, `requester_id`,
  `requested_by` o `request_user_id`; si no, `create_uid` (salvo sistema).
  **Jefe del área que pide**: su `hr.employee.parent_id`. **Quien lo
  detecta**: `sgi_detected_by_id`, `detected_by_id`, `reporter_id` o
  `sgi_requester_id`; si no, `create_uid`. **Área responsable**: el
  `manager_id` del departamento del registro (`sgi_area_id.department_id`,
  `department_id` u otro many2one a `hr.department`). Sin registro solo se
  resuelve «Dueño del proceso»; lo que no se resuelve regresa vacío con su
  motivo (log y reporte).
- **Aprobador ≠ quien ejecuta o pide**: en «Aprueba» y «Escala» se quita a
  quien también ejecuta la actividad o pide el registro; si no queda nadie,
  sube a su jefe directo (`parent_id`, saltando a quien también ejecute o
  pida); sin jefe queda vacío con aviso. El caso S6.07 (Mariano aprobaba lo
  que él ejecuta) sube a su jefe.
- **Escalamientos a «Dueño del proceso» en Mis pendientes**: los 41 de
  producción no le llegaban a nadie (la lista guardada del empleado solo ve
  puestos y familias). Ahora le llegan al dueño del proceso de la actividad,
  o a su jefe si el dueño también la ejecuta (`_sgi_relative_escalations`).
- **Ejecutor relativo** (C2.35, S1.01, S1.18): en la adherencia cuenta como
  correcto cualquier empleado de la empresa (lo hace quien pide o detecta);
  genéricos, sin empleado y sistema siguen fuera.
- **Aprobaciones nativas**: «Jefe del área que pide» con «Solicitud en
  Aprobaciones» crea la categoría con `manager_approval = required` (nativo:
  el jefe de quien hace la solicitud). En botón o firma, un relativo que
  depende del registro queda en el estado nuevo «Depende de cada registro».
- La carga por API avisa (`kind='role'`) cuando un «Aprueba» es
  «Solicitante»; no lo rechaza (`quimibond_sgi_mapa` todavía lo trae en
  E2.01, S4.03 y S6.07).
- `sgi.activity.role.sgi_relative_roles_report()`: a quién resuelve hoy cada
  rol relativo (solo lectura, se puede llamar por MCP).

**Pruebas:** `test_relative_roles` (11 casos, datos propios).

**Corregido (primera corrida real de las pruebas, build de desarrollo
38913511 sobre base nueva con demo: 76 fallas de 1,199):**

- **Diagramas:** `sgi.diagram.data` tomaba el método antes de poner los
  parámetros en el contexto, así que todo diagrama ignoraba lo elegido
  (instrumento de riesgos, vista de cumplimiento legal, carriles por etapa,
  equipo de ventas, mercado). Y cada caja de actividad unía
  `sale_team_ids | fiscal_position_ids` (modelos distintos: `TypeError`),
  así que el flujo del proceso y los demás diagramas con actividades
  tronaban desde 56.16.0.
- **Quien no es de RH y lee empleados (Odoo 19 solo le da el perfil
  público):** `hr.employee.sgi_mp_job_id` y `sgi_my_procedure_ack_state` sin
  prefetch (no están en `hr.employee.public`; al leer cualquier campo de un
  empleado, el prefetch los arrastraba y Odoo rechazaba la lectura); el
  responsable del departamento en el PDF de «Mi procedimiento» se lee con
  sudo, igual que los empleados; el número de empleado del PDF de
  eficiencias (`registration_number`, grupo de RH) también. Publicar e
  imprimir «Mi procedimiento» y el PDF de eficiencias tronaban para el Jefe
  MAST y los jefes de área sin RH.
- **Una sola empresa (D-03) en «Mi procedimiento»:** la lista de personas y
  puestos de MAST / Dirección / administrador, «Mi equipo», los puestos a
  publicar y la revisión previa se limitan a la empresa del SGI (antes todas:
  un empleado de otra empresa no permitida al usuario tronaba la pantalla).
- **Restricción perdida:** `sgi_approval_native._check_condition` tenía el
  mismo nombre que la de `sgi_catalog` y la reemplazaba: desde 56.5.0 no se
  revisaba que la condición solo vaya en «aprueba», «informa» o «escala».
  Se renombra a `_check_approval_condition`.
- **Acciones (`sgi.action.line`):** la actividad espejo se agenda, actualiza
  y cierra con sudo; el responsable de una acción sobre un registro que no
  puede editar (un riesgo de otro proceso) no podía terminarla ni reabrirla.
- **Familias de puestos:** la regla «un puesto, una familia» comparaba
  también contra familias archivadas cuando el contexto traía
  `active_test=False` (la carga por API): no se podía crear la familia nueva
  de un puesto cuya familia vieja estaba archivada.
- **Documentos controlados:** el responsable SGI por defecto también cuando
  «controlado» llega por el contexto (`default_sgi_is_controlled`: alta
  documental, lista maestra, externos); antes el alta por código tronaba.
- **Pie de formato (`sgi.format.map`):** la regla «documento o clave» se
  revisa también al crear (un `@api.constrains` no corre si sus campos no
  vienen en el alta).
- **Checklists de mantenimiento:** sin equipo de mantenimiento en la
  plantilla ni en el equipo se mandaba `maintenance_team_id = False` y la
  solicitud no se creaba (NOT NULL en Odoo 19); ahora toma el default de Odoo.
- **Responsiva de EPP:** creada con renglones y sin `items`, la lista de EPP
  quedaba vacía (el alta ponía `items = False` y bloqueaba el cálculo).
- **Plan de control del artículo:** el compute guardado no tenía de qué
  depender y nunca proponía el plan; ahora lo propone el plan al incluir el
  artículo (a los que aún no tienen).
- **Aprobación nativa:** `approval_state` con dependencias (seguía «Por
  sincronizar» en la misma transacción después de sincronizar).
- **Migración 57.1.0:** `_sgi_cierre_nc_formula` baja lo escrito antes de su
  SQL (una segunda llamada en la misma transacción repetía los migrados).
- **Proponer cambio:** la propuesta se crea con sudo (nace como copia de la
  actividad, con formatos que quien propone puede no poder leer); el autor
  sigue siendo quien propone (F-005).
- **Satélites:** `quimibond_sgi_revisado` 19.0.4.2.1 deja «Calidad PQ»
  después de «Reproceso» (con solo el anterior en `selection_add` quedaba al
  final de la lista); `quimibond_sgi_studio` 19.0.1.0.1 archiva las
  actividades de una regla archivada escribiendo la nota, sin «Marcar como
  hecho» (Studio lo intercepta con su lógica de aprobación y tronaba con
  AccessError).

**Datos de producción (2026-09-29, lectura):** 165 empleados y 129 puestos,
todos de la empresa 1; Areli (128) sin grupo de RH; 0 roles con condición
fuera de aprueba/informa (5 y 6); 3 plantillas de checklist sin equipos; 10
planes de control sin artículos; 4 solicitudes de aprobación de Studio. Nada
de esto pide migración.

**Pruebas (corrida real, base nueva):** se ajustan las que quedaron viejas
frente a un cambio intencional (A) y las que no preparaban sus datos para
una base nueva de Odoo 19 (B):

- (A) `test_audit_hardening` a6 (cerrar un incidente es de MAST y Salud,
  56.34.0), `test_hierarchy` test_03 (el mapa es el diagrama `process_map`;
  `sgi_map_data` se retiró en 57.8.0), `test_indicator_trajectory` (los
  escalones se generan solos desde 55.0.0), `test_ola_b` (objetivo sin
  indicadores = «sin dato», 53.5.0), `test_procedure` (sin agrupación por
  proceso desde 45.0.0: panel lateral), `test_format_map_documento` test_04
  (el borrado del documento ligado queda en `set null`, decisión de Jose del
  lote 2).
- (B) claves con la nomenclatura del tipo (`PR-{proceso}`,
  `IT-{proceso}-{nn}`, `ANEXO nn`) en `test_hierarchy`, `test_doc_change_sign`,
  `test_excel_migration`, `test_external_doc`, `test_links`,
  `test_sign_builder`; ejecutor obligatorio (`test_due_long`,
  `test_cleanup_b10`); método de medición en procedimientos vigentes
  (`test_dropbox_key`, `test_legacy_routine`); Jefe MAST y Dirección activos
  (OdooBot está archivado) con `common_users.sgi_set_mast/sgi_set_director`
  en `test_fase7`, `test_fase8`, `test_ola1`, `test_pegamento`,
  `test_pr6_external`; `assertRaises` sin tuplas (`test_integridad`,
  `test_entrega4`, `test_role_audit`); ubicaciones, cuentas y diario de la
  compañía de la prueba (`test_indicator_i3`, `test_indicator_p21`,
  `test_expansion_kpis`); etapa «Abierta» explícita (`test_nc_deadlines`);
  Odoo 19: `res.partner` sin `date` (`test_bandeja`), destinatarios del
  correo en `recipient_ids` (`test_weekly_overdue`), `date_order` = momento de
  confirmar (`test_kpi_fields`), organigrama sin `parent_field`
  (`test_my_procedure_ui`), campos de `get_views` para todos los grupos
  (`test_entrega1c`), asistente de diseño de documento en base nueva
  (`test_my_procedure`), ficha `hr.employee` solo para RH
  (`test_my_procedure` test_15), ACL de Documentos por registro
  (`test_perm_auditor`), menús de satélites instalados (`test_menu_tree`),
  seguimiento confirmado antes de migrar (`test_calibracion_avisos`),
  lectura tras `flush` de lo guardado (`test_role_audit` test_07) y el envío a
  firma como Jefe MAST (`test_sign_elearning`).

## 19.0.57.12.0 — 2026-09-29

**Cambiado (D-11 ampliada, Jose 2026-09-29):**
`sgi.config.sgi_drop_empty_studio_models` borra también los acompañantes de
Studio de los cuatro modelos (`<modelo>_*`) junto con su padre: las 3 tablas
`_stage` (3 filas cada una: «Nuevo», «En progreso», «Listo»),
`x_no_conformidades_tag` y `x_no_conformidades_line_0ff2d` (vacías), con sus
acciones, vistas y menús (el 1643 «Calendario de obligaciones Stages», bajo
Contabilidad → Configuración personalizada). Lista cerrada en
`_SGI_STUDIO_COMPANIONS` con las filas esperadas: un acompañante con más
filas, que no esté en la lista, que no sea de Studio o al que apunte un campo
de fuera de la familia detiene todo y se reporta en `companion_problems`. Un
acompañante con filas se respalda justo antes de borrarlo en un CSV (todas
sus columnas, por SQL) adjunto a la empresa del SGI, que sobrevive al
borrado. Recuento justo antes de cada borrado. Sigue manual, en el shell y
con `dry_run=True` por default; no corre en el update.

**Datos de producción (2026-09-29, lectura):** los cuatro padres de Studio
sin registros visibles; `x_actividades_obligato_stage`,
`x_calendario_de_obliga_stage` y `x_no_conformidades_stage` con 3 filas cada
una (creadas en 2024), `x_no_conformidades_tag` y
`x_no_conformidades_line_0ff2d` con 0; los únicos campos que apuntan a los
acompañantes son de la propia familia (`x_studio_stage_id`,
`x_studio_tag_ids`, `x_no_conformidades_id`); el menú 1643 abre la acción
2506 de `x_calendario_de_obliga_stage`.

**Pruebas:** `test_studio_cleanup` test_06–test_10 (borrado con respaldo,
más filas, campo de fuera, acompañante no esperado y la lista de
producción).

**Nota:** `e2-sale-automotriz` (57.12.0 en el plan) no entró en este lote;
esta versión la toma la ampliación de D-11.

## 19.0.57.11.0 — 2026-09-29

**Cambiado (A-016, A-013, E-014):** el presupuesto y el pronóstico de ventas
salen al módulo nuevo **`quimibond_ventas_presupuesto`** (19.0.1.0.0, depende
del SGI, de `sale_stock`, `web_grid` y `account_budget`; `auto_install`):
modelos `sgi.sales.budget`, `.line` e `.import` (conservan su nombre técnico y
sus tablas), `budget.analytic.sgi_sales_budget_id`, vistas, reporte,
análisis, 8 menús de Ventas → Presupuesto y pronóstico, reglas, accesos,
secuencia, pie de formato, los crons de cobertura del pronóstico y
revaluación del S2, el cierre de mes, los 9 parámetros (mismas claves) y sus
ajustes. El núcleo ya no depende de `web_grid` ni de `account_budget`.

**Cambiado:** el núcleo deja los ganchos `sgi.cron._sgi_monthly_close_steps`
(cierre de mes) y `sgi.diagnostic._sgi_key_settings_checks` (Ajustes clave).
El KPI VE-02 (`presupuesto_ventas`), su evidencia y el aviso del Diagnóstico
leen el presupuesto solo si el módulo está.

**Migración (pre, `migrations/19.0.57.11.0/pre-migrate.py`):** cambia
`ir_model_data.module` de modelos, campos, valores de selección,
restricciones, herencias (`ir.model.inherit`), vistas, acciones, reporte,
accesos, reglas, menús, crons, secuencia y pie de formato, y la columna
`module` de `ir_model_constraint` e `ir_model_relation`; nada se borra ni se
recrea (E-002: los menús 2434/2435 conservan su id). Marca el módulo nuevo
para instalar en el mismo update y detiene el update si hay presupuestos y
no se puede instalar. `migrations/mudanza.py` suma `ir.model.inherit` y
`instalar(obligatorio=...)`; el pre-migrate de 57.9.0 lo usa con Studio (11
roles con regla en producción).

**Datos de producción (2026-09-29, lectura):** `sgi.sales.budget` = 4 (ids
4, 5, 7, 137; borrador, 2026) con 1,291 líneas; menús 2434 y 2435 (acciones
3886 y 3887) bajo 2438, más 2449 y 2444–2447; crons 184 y 185 activos con los
valores del XML; `sgi.format.map` 9 igual al XML (con `document_id` 3882, que
el XML no toca); folios PPV-2026-004…137; 50 XML IDs del núcleo sobre estos
registros, todos declarados por el módulo nuevo.

**Pruebas (J-018):** `test_sales_budget` (117 pruebas) pasa al módulo nuevo;
`test_multicompany` test_03 y los dos modelos de F-014, la aserción de
presupuesto de `test_entrega4` test_02 y `cron_forecast_coverage` de
`test_avisos_crons` pasan a `quimibond_ventas_presupuesto/tests/test_sales_budget_sgi.py`.

## 19.0.57.10.0 — 2026-09-29

**Cambiado (A-019):** MA-03 «Calidad PQ» sale del núcleo a
**`quimibond_sgi_revisado`** 19.0.4.2.0: el modo `calidad_pq` se registra allá
con `selection_add` (mismo lugar en la lista, después de «reproceso»;
`ondelete='set default'`), junto con `_calc_calidad_pq`, `_detail_calidad_pq`,
su fuente, su evidencia y los dos avisos del Diagnóstico. El cálculo es el
mismo, línea por línea. El núcleo deja el gancho
`sgi.diagnostic._sgi_floor_quality_lines` y ya no lee `mrp.revision.log`.
La siembra de MA-03 en una base nueva queda en `manual`; el satélite la pasa a
`calidad_pq` al instalarse solo si sigue en manual y sin mediciones (B-001).

**Cambiado (A-020):** la tolerancia de peso de rollo
(`quimibond_sgi.pesaje_tolerance_kg`, misma clave) la siembra y la muestra en
Ajustes → SGI → Piso **`quimibond_sgi_pesaje`** 19.0.5.2.0
(`post_init_hook` idempotente y su propia herencia de la vista de ajustes). El
núcleo ya no la siembra ni declara el campo.

**Migración (pre, `migrations/19.0.57.10.0/pre-migrate.py`):** mueve a los
satélites el XML ID del valor de selección `calidad_pq` y el del campo de
ajustes (`ir_model_data.module`, sin borrar). Si `mrp_revisado_telas` no
estuviera, los indicadores en `calidad_pq` pasan a `manual` con aviso.

**Datos de producción (2026-09-29, lectura):** MA-03 (id 16) activo en
`calidad_pq`, última medición 08/2026 = 70.88; `quimibond_sgi_revisado`,
`quimibond_sgi_pesaje`, `mrp_revisado_telas` y `pesaje_rollos_tejido`
instalados.

**Pruebas (J-018):** `test_kpi_fase4` pasa a
`quimibond_sgi_revisado/tests/test_calidad_pq.py` sin cambios (test_03) y
suma el modo registrado desde el satélite (test_04) y la siembra que no pisa
a MAST (test_05); `quimibond_sgi_pesaje` suma test_05 (ajuste y tolerancia).

## 19.0.57.9.0 — 2026-09-29

**Cambiado (A-015):** el manifest lista solo las dependencias directas, cada
una con lo que la usa; las que ya traen otras (`base`, `mail`, `hr`, `stock`,
`purchase`, `approvals`, `quality_control`) no se repiten.

**Retirado (A-011, A-012):** las dependencias `sale_management` (sin ningún
uso) y `hr_timesheet` (solo escribía `allow_timesheets=False` en el proyecto
de Diseño y Desarrollo; el archivo es `noupdate` y en producción no cambia
nada). En producción no se desinstala nada.

**Cambiado (A-010, D-10):** la regla de aprobación de Studio del rol
«Aprueba» (tipo «Botón de Odoo») sale al satélite nuevo
**`quimibond_sgi_studio`** (`auto_install` con `web_studio`), con el cierre de
avisos al archivar una regla (antes `sgi_approval_rule_archive.py`). El núcleo
ya no depende de `web_studio`: se quedan el tipo, el documento, el botón, la
condición, las solicitudes de Aprobaciones, las firmas de Sign, el cron, el
menú y las aprobaciones de Studio en Mis pendientes (solo si Studio está). El
núcleo deja ganchos `_sgi_button_*`; sin el satélite, «Sincronizar» un rol de
botón avisa que falta el módulo.

**Cambiado (A-014):** DOC-5, el instructivo en Conocimiento, sale al satélite
nuevo **`quimibond_sgi_knowledge`** (`auto_install` con `knowledge`). El
núcleo ya no depende de `knowledge`.

**Migración (pre, `migrations/19.0.57.9.0/pre-migrate.py`, con el
procedimiento común `migrations/mudanza.py`):** los XML IDs de lo que sale
cambian de `module` en `ir_model_data` (campos, modelo transitorio, vistas,
reporte, acceso); nada se borra ni se recrea. Marca los dos satélites para
instalar en el mismo update.

**Datos de producción (2026-09-29, lectura):** 11 roles con
`approval_rule_id` y 11 reglas `studio.approval.rule` activas con
`sgi_role_id` (creadas ese día); 1,072 roles `approval_kind = 'boton'`; 0
actividades con `instruction_article_id`, 0 documentos con `sgi_article_id` y
ningún otro campo de `sgi.*`/`documents.document`/`hr.*` apunta a
`knowledge.article`; `web_studio`, `knowledge`, `sale_management` y
`hr_timesheet` instalados.

**Pruebas (J-018):** `test_approval_native` test_02–04 y
`test_approval_rule_archive` pasan a `quimibond_sgi_studio`, con
`test_bandeja` test_07 (aprobación de Studio en Mis pendientes, ya sin
`skipTest`) y la aserción «una solicitud no bloquea ningún botón»;
`test_pr6_external` test_05 pasa a `quimibond_sgi_knowledge` (sin
`skipTest`). Nueva en el núcleo: `test_approval_native` test_03 (puesto sin
personas).

## 19.0.57.8.0 — 2026-09-29

**Retirado (B-011):** 27 métodos `action_*` sin botón, menú ni llamador,
verificados con grep en todo el repo y contra las vistas de producción (0 de
Studio): en `sgi.my.procedure` `action_show_late/ok/unmeasured/short/
documents/nc/measures/legal/doc_reviews/epp`, `action_focus_pending` y
`action_precheck`; en `sgi.process` `action_sgi_view_process_map/sipoc/
doc_tree`, `action_sgi_view_flows`, `action_print_risk_matrix`,
`action_print_master_list`, `action_view_inbound_references`,
`action_view_no_method_activities` y `action_print_procedure`; además
`sgi.activity.role.action_sgi_open_approval_rule`,
`approval.request.action_sgi_open_sign_request`,
`sgi.indicator.measure.action_open_plan`,
`sgi.process.activity.action_sgi_mp_propose_change`,
`hr.employee.action_sgi_my_procedure_view` (con
`hr.job._sgi_my_procedure_view_action`, que solo usaba él) y
`sgi.sales.budget.action_open_lines`. **Se queda** `action_show_received`: sí
es botón de la pantalla (el informe lo daba por muerto). Los reportes siguen
en el menú Imprimir.

**Retirado (I-022, G-013):** las listas de Mi procedimiento que ninguna vista
mostraba (`pending_action_ids`, `pending_nc_ids`, `pending_measure_ids`,
`pending_legal_ids`, `pending_doc_review_ids`) y sus conteos; se calculaban en
cada apertura. Los pendientes viven en «Mis pendientes».

**Retirado (B-012):** `_sgi_done_activities`, `sgi_business_day_of`,
`_sgi_file_size`, `_sgi_mp_role_label`, `sgi_map_data`, `_sgi_sentence` y
`_sgi_aggregate`. Se queda `sgi_next_code`.

**Retirado (B-013):** 10 campos no guardados sin uso:
`sgi.activity.role.approval_model_name`, `sgi.audit.checklist.line.audit_state`,
`hr.job.sgi_role_ids`, `sgi.deliverable.link_ids`,
`approval.request.sgi_purchase_order_ids`,
`sgi.management.review.agreement_action_ids`,
`sgi.process.activity.sgi_mp_change_ids`, `maintenance.equipment.sgi_msa_ids`,
`documents.document.sgi_publish_sign_state` y
`sgi.process.activity.exec_stat_ids`. Se queda `linked_document_ids`.

**Retirado (B-014, D-012):** la acción `sgi_process_hierarchy_action` (sin
menú) y las vistas `sgi_nc_view_pivot`/`sgi_nc_view_graph` (ninguna acción
las usaba). **Corregido:** «Pareto de alertas de calidad» usa sus vistas
(equipo × etiqueta) por `view_ids`; antes ganaban las estándar.

**Cambiado (A-023, B-019):** archivos de datos y vistas renombrados por tema,
sin cambiar XML IDs ni el orden del manifest: `sgi_sequences_fase2/3/6` →
`sgi_sequences_audit_risk`, `_quality_sst`, `_policy_budget`;
`sgi_cron_fase2/3` → `sgi_cron_indicators_audit`, `sgi_cron_calibration_budget`;
`sgi_control_plans_fase4` → `sgi_control_plans`; `sgi_fase7_data` →
`sgi_emergency_satisfaction_data`; `sgi_fase8_data` →
`sgi_operational_signals_data`; `sgi_pr6_data` → `sgi_supplier_nc_data`;
`views/sgi_pr6_views` → `views/sgi_supplier_audit_sign_views`;
`demo/sgi_demo_fase3` → `demo/sgi_demo_quality`.

**Migración (post, `migrations/19.0.57.8.0/post-migrate.py`, B-006):** borra
las 15 filas de `ir_model_data` de otros módulos que apuntaban a
`sgi.employer.obligation` (solo esas filas; ningún dato).

**Datos de producción (2026-09-29, lectura):** 15 filas `ir.model.data` con
`sgi_employer_obligation`; 0 vistas de producción que citen los métodos o
campos retirados fuera del módulo; la acción 4053 sin menú.

**Pruebas:** ajustadas `test_my_procedure_ui`, `test_my_procedure`,
`test_hierarchy`, `test_structure_sgi`, `test_structure`, `test_spec`,
`test_fase10`, `test_indicator_detail`, `test_pr4_docs_legal`,
`test_pr6_external` y `test_cleanup_b10` (los pendientes se prueban en
`sgi.my.pending`).

## 19.0.57.7.0 — 2026-09-29

**Cambiado (D-11):** `sgi.config.sgi_drop_empty_studio_models` cuenta las
filas por SQL (archivados incluidos, sin depender de permisos: `x_emp_activity`
no tiene ninguno) y **no borra ninguno si uno de los cuatro tiene registros**.
Justo antes de cada borrado vuelve a contar; si ya no es 0, `UserError` y se
revierte todo. Quita primero los one2many de Studio del propio modelo, reporta
los modelos acompañantes (`_stage`, `_tag`, `_line…`, que no se borran) y los
campos que Odoo quitará, y deja cada borrado en `ir.logging`. Se corre a mano
en el shell (`dry_run=True` por default). No corre en el update.

**Agregado (D-14):** correo semanal por persona con lo atrasado de su «Mis
pendientes» (`sgi.cron.cron_weekly_overdue_mail`, plantilla
`mail_template_sgi_weekly_overdue`). Solo a quien tiene algo atrasado; cada
quien lo apaga en Preferencias (`res.users.sgi_weekly_overdue_mail`, encendido
por default). Reutiliza el cron `sgi_cron_weekly_digest`, que sale
**apagado**.

**Retirado:** `sgi.cron.cron_weekly_digest`, el resumen semanal viejo a MAST y
Dirección (B-017).

**Cambiado (D-15):** «Solicitud de compra SGI» y «Cambio de proceso /
infraestructura (MOC SGI)» se instalan archivadas; en producción se archivan
con la familia OP-PTAR (`sgi.config._sgi_archive_unused_catalogs`, con CSV de
respaldo adjunto; no archiva nada que tenga solicitudes o roles).

**Migración (post, `migrations/19.0.57.7.0/post-migrate.py`):** reescribe el
cron 201 al correo por persona y lo deja apagado; archiva 13, 14 y OP-PTAR si
siguen sin uso.

**Datos de producción (2026-09-29, lectura):** `x_calendario_de_obliga`,
`x_no_conformidades` y `x_actividades_obligato` con 0 registros (también
archivados); `x_emp_activity` sin permiso de lectura (se cuenta por SQL al
correr). Acompañantes: `_stage` con 3 registros cada uno, `x_no_conformidades_tag`
y `x_no_conformidades_line_0ff2d` con 0. `approval.request` en 13 y 14: 0.
OP-PTAR (57): 2 puestos, 3 empleados, 0 roles.

**Pruebas:** `test_studio_cleanup` y `test_weekly_overdue` (nuevas);
`test_sign_elearning` sin el digest; `test_ola_certificable` reactiva la
categoría MOC para probar su candado.

## 19.0.57.6.0 — 2026-09-29

**Cambiado:** E1-02 se mide con `acuerdos_rxd` (D-13). El modo ahora cuenta
los acuerdos de la Revisión por la Dirección realizada o cerrada con fecha
límite en el periodo, cumplidos a tiempo (`done_date` ≤ `deadline`; también
los cumplidos a mano sin acción), con nota «sin acuerdos», fuente del dato y
evidencia (antes «Este modo aún no tiene vista de evidencia»).

**Retirado:** la encuesta de auditoría legado (B-009, D-011, D-016): campos
`sgi.audit.survey_id` y `survey_input_ids`, `sgi.audit.finding.survey_line_id`,
la pestaña «Encuesta (legado)», los botones «Contestar checklist (encuesta)» y
«Hallazgos de la encuesta» con sus métodos, y el checklist por encuesta del
plan de auditoría impreso. `data/sgi_audit_data.xml` sale del manifest a
`docs/historico/quimibond_sgi_data/`. Se queda «Evaluar al auditor».
También `documents.document.sgi_revision_legacy` (C-017; 0 con dato).

**Retirado:** nueve modos de cálculo sin uso en producción (B-010):
`desperdicio`, `desperdicio_scrap`, `disponibilidad_mantto`,
`preventivo_cumplido`, `plantilla_rh`, `inventario_ciclico`,
`compras_sin_devolucion`, `margen_ventas` y `compras_vs_ventas`, con sus
cálculos, evidencia, fuente del dato, `_detail_preventivo_cumplido`,
`_sgi_waste_category_ids` y el ajuste «Categoría del byproduct de
desperdicio». Las 6 siembras que los usaban toman el modo de producción.

**Migración (pre, `migrations/19.0.57.6.0/pre-migrate.py`):** pasa a
`manual` cualquier indicador en un modo retirado (hoy 0); archiva la encuesta
151 si estuviera activa; sus 36 XML IDs pasan a `__export__` con prefijo
`quimibond_sgi_legado_` (la encuesta no se borra).

**Migración (post, `migrations/19.0.57.6.0/post-migrate.py`):** E1-02 pasa a
`acuerdos_rxd` solo si está activo, en «manual» y sin términos de fórmula.

**Datos de producción (2026-09-29, lectura):** `sgi.indicator` por
`calc_mode` (activos y archivados): 0 en los 9 modos retirados y 0 en
`acuerdos_rxd`. E1-02 = id 171, manual, sin términos, 0 mediciones. Encuesta
151 archivada, 0 respuestas; 0 `sgi.audit`, 0 hallazgos; 36 XML IDs de la
encuesta. `sgi_revision_legacy` con dato: 0. Acuerdos de RxD: 0.

**Pruebas:** `test_legado` (nueva); se retiran `TestAuditChecklist`
(`test_fase8`) y las pruebas de los modos retirados (`test_kpi_fase4`,
`test_kpi20`, `test_expansion_kpis`); `test_ola_certificable` usa fórmula sin
términos en vez de `plantilla_rh`.

## 19.0.57.5.0 — 2026-09-29

**Cambiado:** la actualización del módulo solo corre `seed_parameters`
(A-006). `recompute_pending_measures` sale de `data/sgi_parameters.xml` y
corre todos los días en el cron de indicadores (D-12), cada medición en su
savepoint (un indicador con error ya no detiene a los demás) y con conteo en
el log. `migrate_document_families` también sale del update; el método queda
para llamarse a mano (B-020).

**Agregado:** botón «Recalcular mediciones pendientes» en la lista de
indicadores, solo para el Administrador SGI (grupo en la vista y revisión en
el servidor; D-12). Con selección recalcula esos indicadores; sin selección,
todos.

**Retirado:** `activate_auto_indicators`, `_SGI_AUTO_INDICATORS`,
`fix_kpi_seeds` y `harden_noupdate` (B-001): ya no se llamaban desde ningún
lado salvo pruebas. La siembra del modo automático vive en el XML
`noupdate`.

**Corregido:** «Calidad preventiva» y «Paretos de calidad» con padre fijo
`quality_control.menu_quality_root` y secuencias 23 y 24, las de producción
(E-008). Se retiran `ir.ui.menu._sgi_attach_quality_menus`, su `<function>` y
`SGI_MENU_QUALITY_ENTRIES`. El cron `sgi_cron_my_procedure_stale` queda en
`<data noupdate="1">`, como ya estaba en la base (A-007).

**Pruebas:** `test_siembras_y_funciones` (nueva); ajustadas `test_fase8`,
`test_kpi20` y `test_expansion_kpis`.

## 19.0.57.4.0 — 2026-09-29

**Retirado:** el mapa viejo de procesos sale del módulo (A-002 + B-021,
decisión 6: el SGI se instala sin procesos). `data/sgi_process_data.xml`
(21 procesos MP-*/P-* y 19 flujos) y `data/sgi_process_flows_extra.xml`
(18 flujos) dejan el manifest y pasan a `docs/historico/quimibond_sgi_data/`.

**Retirado:** la siembra del piloto P-VEN (`seed_procedure_ventas`, sus
constantes `_SGI_VENTAS_*` y `_sgi_vigente_docs_by_codes`) y
`seed_process_purposes` con `_SGI_PROCESS_PURPOSES` (A-003, B-002).
`harden_noupdate` ya no lista `sgi.process` ni `sgi.process.flow`. Los scripts
de un solo uso `carga_documental.py`, `post_carga_documental.py` y
`reporte_telas_rollout.py` salen de `tools/` del módulo a
`docs/historico/quimibond_sgi_tools/` (B-018).

**Cambiado:** las pruebas crean sus propios procesos (`XPM-A`, `XPM-B`) en
lugar de usar `proc_*`/`flow_*` o «el primer proceso» de la base
(`test_process_map`, `test_process_map_46`, `test_flows_48`, `test_ola1`,
`test_ola2`, `test_format_map_documento`; J-019). Se retira `TestProcedureVentasSeed` con su siembra, y
`test_flows_48.test_01` (verificaba los flujos del XML). Prueba nueva
`test_procesos_viejos`.

**Migración (pre, `migrations/19.0.57.4.0/pre-migrate.py`):** los 58 XML IDs
(21 `sgi.process`, 37 `sgi.process.flow`) pasan a `__export__` con prefijo
`quimibond_sgi_legado_`; si alguno estuviera activo se archiva. No borra nada.

**Migración (post, `migrations/19.0.57.4.0/post-migrate.py`):** respaldo CSV
(adjunto en el proceso) de las 7 filas de `sgi.process.responsibility` del
proceso archivado P-VEN. Las filas se quedan: el modelo no tiene `active`.

**Datos de producción (2026-09-29, lectura):** 58 XML IDs `quimibond_sgi`
sobre procesos (21) y flujos (37), todos archivados; `sgi.process` 14 activos
y 25 archivados; `sgi.process.flow` 50 activos y 37 archivados.

## 19.0.57.3.0 — 2026-09-29

**Retirado:** las 39 carpetas de migración de 19.0.13.7.0 a 19.0.56.24.0
(`19.0.13.7.0` … `19.0.56.24.0`; A-026, B-003, A-027). Ninguna vuelve a correr:
producción está en 19.0.57.0.0 (o 19.0.56.38.2) y todas las bases son copia
de producción. Su resumen queda abajo, en las líneas **Migración** de cada
versión, y el código sigue en git (etiqueta `sgi-antes-de-limpieza-56.x` sobre
`main`, que se pone antes de integrar esta versión).
Con ellas se van las dos convenciones de nombre (`post-migration.py` contra
`post-migrate.py`, A-027). Quedan las migraciones ≥ 19.0.56.25.0.

**Retirado:** `models/sgi_groups_cleanup.py` (limpieza de grupos de 56.8.0, ya
aplicada; solo la llamaba la migración 56.8.0 y llevaba un login fijo) y su
prueba `tests/test_perm_groups.py` (B-005).

## 19.0.57.2.0 — 2026-09-29

**Agregado:** este CHANGELOG, reconstruido desde git para las 243 versiones
anteriores (K-018, D-30), y el chequeo en `tools/check_addons.py`: si un PR
cambia la versión del manifest de `quimibond_sgi`, también tiene que cambiar
este archivo y traer la sección de la versión nueva.

## 19.0.57.1.0 — 2026-09-29

**Cambiado:** TR-01 «Cierre de NC» pasa a fórmula (cerradas en el periodo ÷
abiertas en el periodo, sin canceladas) y se retira el modo `cierre_nc`
(`_calc_cierre_nc`, `_detail_cierre_nc`). MA-02 solo cuenta órdenes en kg.

**Corregido:** `calc_status` y `calc_checked` se llenan desde la última
medición en el cron diario de indicadores (`_sgi_calc_status_backfill`).

**Migración:** `post-migrate` pasa a fórmula los indicadores que seguían en
`cierre_nc` (respeta términos puestos por MAST; deja el modo anterior en el
chatter). Idempotente.

## 19.0.57.0.1 — 2026-09-29

**Corregido:** el Buscador de «Del Dropbox a Odoo» (`sgi.dropbox.key`) fallaba
en producción: `documents.document.name` es jsonb y se mezclaba con
`sgi_title` en un `coalesce`. Ahora toma el texto es_MX / en_US.

## 19.0.57.0.0 — 2026-09-29

Del Dropbox a Odoo, rutina por rutina, buscador y avance.

**Migración (post, `migrations/19.0.57.0.0/post-migrate.py`):** primera carga del grupo «Dueño de proceso (SGI)» (``quimibond_sgi.group_sgi_process_owner``), que ve «Rutina por rutina», «Procedimientos anteriores» y «Avance de la transición».

Commit `09a18eb` en la rama de la entrega; entró a `main` en el squash `d5e0b61` (PR #469).

## 19.0.56.39.0 — 2026-09-29

Del Dropbox a Odoo, formatos y documentos anteriores.

**Migración (post, `migrations/19.0.56.39.0/post-migrate.py`):** los instructivos, DAT, anexos, protocolos y reglamentos controlados y activos que NO tienen clase de migración pasan a clase D («Sigue como documento») y «No aplica (se queda)».

Commit `55f5d66` en la rama de la entrega; entró a `main` en el squash `d5e0b61` (PR #469).

## 19.0.56.38.2 — 2026-09-29

La clave heredada del Dropbox vale en sgi.format.map.

Commit `e0e2f2e`.

## 19.0.56.38.1 — 2026-09-29

Decisiones de Jose sobre la entrega 8a.

**Migración (post, `migrations/19.0.56.38.1/post-migrate.py`):** se liberan TODOS los equipos de medición que están en «No usar». 1. Respaldo en la tabla ``sgi_equipment_do_not_use_bak_563801`` (id, name, sgi_do_not_use, sgi_calibration_state, sgi_next_calibration_date y la hora del respaldo) de cada equipo que se va a…

Commit `1078998` en la rama de la entrega; entró a `main` en el squash `e7bed84` (PR #466).

## 19.0.56.38.0 — 2026-09-29

Calibración avisa sin bloquear, con resumen diario.

**Migración (post, `migrations/19.0.56.38.0/post-migrate.py`):** la calibración solo avisa, no bloquea, y los avisos van al Coordinador de Laboratorio y al Jefe de Calidad en un resumen diario.

Commit `56a97ef` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.37.0 — 2026-09-29

Avisos de los crons con clave y cierre por episodio.

**Migración (post, `migrations/19.0.56.37.0/post-migrate.py`):** los avisos de MAST que quedaron en otras bandejas. 1. ``quimibond_sgi.mast_user_id``: si no está puesto (o vale 0), se fija al usuario activo con login ```` (en producción, el 128, Blanca Areli Ballesteros).

Commit `a6fdc78` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.36.0 — 2026-09-29

Mis pendientes con todo adentro (entrega 8a, bandeja).

Commit `7f9f4bf` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.35.0 — 2026-09-29

export_payload, dominios portables, categoría «Proponer cambio» al núcleo y frontera del mapa.

**Migración (pre, `migrations/19.0.56.35.0/pre-migrate.py`):** Solo SQL, sin importar código del módulo, idempotente y sin borrar nada. 1. A-004 / D-01: los 10 términos de fórmula de ``sgi_indicator_formula_data.xml`` (ubicaciones, categorías, tipo de operación y proveedor con IDs de producción) salen del núcleo: el…

Commit `10a4b9f` en la rama de la entrega; entró a `main` en el squash `65a5284` (PR #464).

## 19.0.56.34.0 — 2026-09-29

El SGI es de una sola empresa (entrega 3, e3-multiempresa).

Commit `3742218` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.33.0 — 2026-09-29

Integridad del modelo (entrega 3, e3-integridad).

Commit `09e7786` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.32.0 — 2026-09-29

Clave del Dropbox como clave anterior y clave nueva D-02 (C-004, C-005, C-007, C-009).

**Migración (post, `migrations/19.0.56.32.0/post-migrate.py`):** Corre después de 56.30.0 (C-006: los pies de formato ya están ligados al documento, decisión 4 de Jose).

Commit `d6f2a4f` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.31.0 — 2026-09-29

El documento manda en procedimiento ↔ proceso (C-001, L-003, L-005).

**Migración (post, `migrations/19.0.56.31.0/post-migrate.py`):** estado de migración de los procedimientos según la decisión de Jose (decisiones.md, «Estado de migración de los procedimientos (C-003)»): - los 23 sustituidos (con ``sgi_replaced_by_process_id``) → «En curso» (pasan a «Baja tramitada» cuando su proceso…

**Migración (pre, `migrations/19.0.56.31.0/pre-migrate.py`):** ``sgi.process.replaced_document_ids`` deja de ser un Many2many (tabla ``sgi_process_replaced_doc_rel``) y pasa a ser el One2many inverso de ``documents.document.sgi_replaced_by_process_id``.

Commit `bfc5181` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.30.0 — 2026-09-29

El pie de formato controlado sale del documento (C-006).

**Migración (post, `migrations/19.0.56.30.0/post-migrate.py`):** liga cada ``sgi.format.map`` a su documento controlado por la clave con la que se sembró, ANTES de que la clave del Dropbox pase a clave anterior (decisión 4 de Jose: primero C-006, después la migración de clave).

Commit `e40db91` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.29.0 — 2026-09-29

Árbol de menús de la entrega 4 en un solo archivo.

**Migración (post, `migrations/19.0.56.29.0/post-migrate.py`):** A. Salarios de eficiencias: los importes pasan del grupo de Nómina (hr_payroll.group_hr_payroll_user, que en producción incluye a usuarios que no deben verlos) al grupo propio «Salarios de eficiencias (SGI)».

Commit `9a67d48` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.28.0 — 2026-09-29

Entrega 1c de la auditoría (seguridad).

**Migración (post, `migrations/19.0.56.28.0/post-migrate.py`):** Areli (Jefe MAST y SGI) queda como responsable SGI de los documentos controlados que no tienen (489 en producción el 2026-09-29: 487 vigentes y 2 obsoletos), antes de que la regla «controlado ⇒ con responsable» empiece a exigirlo.

Commit `c40791e`.

## 19.0.56.27.0 — 2026-09-29

Archivar una regla de aprobación cierra sus avisos.

Commit `fa8f78c`.

## 19.0.56.26.0 — 2026-09-29

Entrega 1b, Mis pendientes solo de la empresa del SGI.

Commit `9aa8c83`.

## 19.0.56.25.0 — 2026-09-29

Entrega 1 de la auditoría (instalación limpia, siembras, Sign y salud).

Commit `7ec8736`.

## 19.0.56.24.1 — 2026-09-29

Sgi.my.pending se extiende como TransientModel.

Commit `4415976`.

## 19.0.56.24.0 — 2026-09-29

Menú SGI reordenado con los nombres de producción y pendientes sin archivados.

**Migración (post, `migrations/19.0.56.24.0/post-migrate.py`):** borra, con respaldo, los roles de actividades archivadas (223 en producción, todos «ejecuta», de los 15 procesos viejos P-xxx).

Commit `d386922`.

## 19.0.56.23.0 — 2026-09-28

Actividades ligadas a los requisitos de la norma.

Commit `4a3027f`.

## 19.0.56.22.0 — 2026-09-28

Eficiencias por área, checklists firmados con PIN y respuesta al cliente en la NC.

**Migración (post, `migrations/19.0.56.22.0/post-migrate.py`):** - Eficiencias de personal: los jefes de departamento (usuario del jefe en hr.department) entran al grupo «Captura de eficiencias».

Commit `0d39511`.

## 19.0.56.21.0 — 2026-09-28

Documento externo, estudios/exámenes, recorrido CSH y checklists.

Commit `275a5f1`.

## 19.0.56.20.0 — 2026-09-28

Menús con claves, vencimientos largos, orígenes de NC y acta.

**Migración (post, `migrations/19.0.56.20.0/post-migrate.py`):** las NC que nacieron de un incidente SST quedan con origen «Incidente/accidente SST» (antes se guardaban como «Proceso»).

Commit `d8d292b`.

## 19.0.56.19.0 — 2026-09-28

«Mi procedimiento» firmado en Sign antes de entrar en vigor.

**Migración (post, `migrations/19.0.56.19.0/post-migrate.py`):** «Mi procedimiento» se firma en Sign antes de entrar en vigor (decisión del CEO: el SGI usa Sign).

Commit `0f54d72`.

## 19.0.56.18.0 — 2026-09-28

Acuses y responsivas de EPP firmados sin plantilla manual.

Commit `8626a9b`.

## 19.0.56.17.0 — 2026-09-28

Cambio documental firmado en Sign.

**Migración (post, `migrations/19.0.56.17.0/post-migrate.py`):** cambio documental firmado en Sign y categorías de Aprobaciones que sí se pueden aprobar. - «Modificación de documento SGI»: se aprueba firmando en Sign (elaboró → revisó → aprobó).

Commit `249c64e`.

## 19.0.56.16.1 — 2026-09-28

Etiqueta única para sgi_mp_ack_ids.

Commit `617ba61`.

## 19.0.56.16.0 — 2026-09-28

Líneas de negocio («Aplica a»).

**Migración (post, `migrations/19.0.56.16.0/post-migrate.py`):** líneas de negocio con lo que Odoo ya tiene. 1. «C2 Pedido a entrega» mezcla pedido industrial, de confección y exportación.

Commit `20c2631`.

## 19.0.56.15.0 — 2026-09-28

Bloque 10: retiro de sgi.employer.obligation y roles de actividades archivadas fuera de los cálculos.

**Migración (pre, `migrations/19.0.56.15.0/pre-migrate.py`):** se retira ``sgi.employer.obligation`` (S4-04), sin registros en producción; las obligaciones viven en ``qb_obligation``.

Commit `30d6ca3`.

## 19.0.56.14.0 — 2026-09-28

Bloques 8 y 9: tablero de dirección sin oficiales y proveedores sin datos.

**Migración (post, `migrations/19.0.56.14.0/post-migrate.py`):** «Sin datos» no es «Baja». Antes, un proveedor sin recepciones con fecha compromiso tenía OTD 0 y calificación 30 → «Baja» (44 de 87 evaluaciones con OTD 0; 84 de 87 proveedores en «Baja»).

Commit `d7789ab`.

## 19.0.56.13.0 — 2026-09-28

Bloque 7: clasificación de formatos en lote (7.4).

Commit `49c2465`.

## 19.0.56.12.0 — 2026-09-28

Bloque 6: motivo cuando un indicador no calcula, cierre de avisos y crons escalonados.

**Migración (post, `migrations/19.0.56.12.0/post-migrate.py`):** horarios escalonados de los crons del SGI. Antes, 10 crons diarios arrancaban a las 20:44 UTC y «Mediciones semanales» a la misma hora que «Mediciones de indicadores».

Commit `a5078a6`.

## 19.0.56.11.0 — 2026-09-28

Bloque 5: obsolescencia con fecha, motivo y proceso que sustituye (DOC-2).

**Migración (post, `migrations/19.0.56.11.0/post-migrate.py`):** campos nuevos «Obsoleto desde», «Motivo de obsolescencia» y «Lo sustituye el proceso». Los documentos que ya estaban obsoletos toman como fecha su última modificación y como motivo «Obsoleto antes de 56.11.0» (no se sabe el motivo real).

Commit `2147d32`.

## 19.0.56.10.0 — 2026-09-28

Bloque 4: programa de auditorías sin auditor no se aprueba; flujo completo con proceso real.

Commit `e27adb9`.

## 19.0.56.9.0 — 2026-09-28

Bloque 3: acción terminada exige 100 % y fecha no futura (3.6).

**Migración (post, `migrations/19.0.56.9.0/post-migrate.py`):** una acción terminada tiene 100 % de avance y fecha de término de hoy o antes. Datos existentes (no se borra nada): - «Terminada» con fecha FUTURA (en producción, la acción 63 «Abrir mercado mexicano», 0 % y fecha 2026-11-30): se reabre (sin fecha de…

Commit `e4ece13`.

## 19.0.56.8.0 — 2026-09-28

Bloque 2: Jefe MAST solo con Areli y retiro de grupos heredados.

**Migración (post, `migrations/19.0.56.8.0/post-migrate.py`):** - 2.2 PERM-2: Jefe MAST y SGI con un solo miembro directo (Areli); los demás miembros directos pasan a Usuario SGI.

Commit `86d6b9e`.

## 19.0.56.7.0 — 2026-09-28

«Mi procedimiento» como ficha con pestañas y correcciones de la auditoría.

**Migración (post, `migrations/19.0.56.7.0/post-migrate.py`):** los documentos controlados en piloto o vigente quedan de solo lectura para los usuarios internos (access_internal = 'view').

Commit `056c0ab`.

## 19.0.56.6.2 — 2026-09-28

«Nuevo» de la lista de Mis actividades abre la propuesta de actividad nueva.

Commit `d068042`.

## 19.0.56.6.1 — 2026-09-28

Botón nativo «Nuevo» en Mis actividades abre la propuesta de actividad nueva.

Commit `1b6ff0a`.

## 19.0.56.6.0 — 2026-09-28

Cómo se aprueba (botón, solicitud o firma) y reglas duplicadas en el botón.

Commit `94d6aa6`.

## 19.0.56.5.0 — 2026-09-28

El rol «Aprueba» ligado a la aprobación nativa de Odoo.

Commit `f54afee`.

## 19.0.56.4.0 — 2026-09-28

Proponer cambio edita la actividad de verdad y se aplica al aprobarse.

Commit `84178b8`.

## 19.0.56.3.2 — 2026-09-28

Proponer cambio abre sin error (propuesta y motivo obligatorios en la vista, no en el modelo).

Commit `ebcfce9`.

## 19.0.56.3.1 — 2026-09-28

Etiqueta propia para pending_late (chocaba con late_count en el build).

Commit `204f388`.

## 19.0.56.3.0 — 2026-09-28

Mis pendientes en una sola lista con semáforo (pantalla y Mi equipo).

Commit `9ecd728`.

## 19.0.56.2.0 — 2026-09-28

Documentos del puesto desde sus actividades, proponer cambios a Mi procedimiento y aviso sin empleado.

**Migración (post, `migrations/19.0.56.2.0/post-migrate.py`):** la categoría de Aprobaciones «Proponer cambio a mi procedimiento (SGI)» queda marcada para el botón «Proponer cambio».

Commit `dbc1148`.

## 19.0.56.1.1 — 2026-09-28

Mi procedimiento abierto desde Mi equipo muestra el puesto y las actividades del empleado.

**Migración (post, `migrations/19.0.56.1.1/post-migrate.py`):** pantallas «Mi procedimiento» guardadas con empleado y sin puesto (abiertas desde Mi equipo) reciben el puesto del empleado.

Commit `c6dae03`.

## 19.0.56.1.0 — 2026-09-25

Build sin amarillo (etiquetas duplicadas, compute mixto, botón kanban, acceso al wizard de acuses, firmas en la variante) y README de 56.0.0.

Commit `e7dfb55`.

## 19.0.56.0.0 — 2026-09-25

Migración de formatos Excel, bloque del programador.

Commit `236ab1a`.

## 19.0.55.0.0 — 2026-09-25

Fórmulas para 28 indicadores más (fechas relativas, comparar fechas, solo conteo) y campos nuevos.

**Migración (pre, `migrations/19.0.55.0.0/pre-migrate.py`):** varios términos con el mismo papel se suman; la restricción SQL «un numerador y un denominador por indicador» ya no existe en el código y Odoo no siempre la retira al actualizar.

Commit `7a94d4d`.

## 19.0.54.4.0 — 2026-09-25

Mi equipo abre en organigrama con el estado SGI de cada persona.

Commit `966e992`.

## 19.0.54.3.0 — 2026-09-25

Cada diagrama con el trazado que le corresponde.

Commit `445ca21`.

## 19.0.54.2.0 — 2026-09-25

«Mi procedimiento» sin pestañas, cada lista en su botón.

**Migración (pre, `migrations/19.0.54.2.0/pre-migrate.py`):** «Mi procedimiento» sin pestañas; la herencia propia sgi_my_procedure_view_form_pr6 (lista de documentos por revisar) guardada en la base apuntaba a un campo que ya no está en la vista.

Commit `74a7ebd`.

## 19.0.54.1.0 — 2026-09-25

Pantalla «Mi procedimiento» como ficha de persona con botones y tarjetas compactas.

**Migración (pre, `migrations/19.0.54.1.0/pre-migrate.py`):** la ficha del proceso vuelve a cambiar de forma (sin pestaña Conexiones) y las herencias del propio módulo guardadas en la base (procedimiento, estructura) se revalidan contra el padre nuevo antes de recargarse: el build reventó igual que en 54.0.0.

Commit `066d963`.

## 19.0.54.0.0 — 2026-09-25

El diagrama como vista de cada acción y ficha del proceso con botones.

**Migración (pre, `migrations/19.0.54.0.0/pre-migrate.py`):** la ficha del proceso cambió de forma (pestañas → botones). Odoo revalida las vistas heredadas que YA están en la base en cuanto carga la vista padre nueva, antes de llegar al archivo que las corrige; la de estructura tenía xpaths sobre…

Commit `a5ec01b`.

## 19.0.53.6.0 — 2026-09-25

Catálogo ISO de diagramas, impresión aparte y ajuste de carriles.

Commit `14be8f3`.

## 19.0.53.5.0 — 2026-09-25

Campos de liga entrada ↔ salida y cuatro reglas de datos.

**Migración (post, `migrations/19.0.53.5.0/post-migrate.py`):** - Los documentos de Odoo que no son del SGI quedan sin «Estado SGI» (había 2,524 marcados como borrador solo por el default del campo).

Commit `636fb4d`.

## 19.0.53.4.0 — 2026-09-25

Motor de diagramas en HTML y seis diagramas.

Commit `992b91a`.

## 19.0.53.3.1 — 2026-09-25

Mapa de procesos con acabado moderno.

Commit `027c2e4`.

## 19.0.53.3.0 — 2026-09-25

Mapa de procesos con conexiones (componente sgi_process_map).

Commit `719cbbf`.

## 19.0.53.2.1 — 2026-09-25

Tarjetas del diagrama solo con numeral, etapa y título.

Commit `0c6d540`.

## 19.0.53.2.0 — 2026-09-25

Diagramas con la vista nativa hierarchy (mapa de procesos y flujo de actividades).

Commit `5d03a31`.

## 19.0.53.1.5 — 2026-09-25

Tarjetas de Mi procedimiento sin el «cuándo» apilado.

Commit `562e1dd`.

## 19.0.53.1.4 — 2026-09-25

Orden de carga de vistas heredadas (el build de producción reventó).

Commit `e15b818`.

## 19.0.53.1.3 — 2026-09-25

OrderedSet en los métodos de búsqueda y acuses al publicar instructivo.

Commit `9981cc1`.

## 19.0.53.1.2 — 2026-09-25

Correcciones de la primera corrida real de las pruebas de los PR 1 a 7.

Commit `c4d131f`.

## 19.0.53.1.1 — 2026-09-25

Mi procedimiento con usuarios sin RH, y registrar 8 archivos de pruebas.

Commit `72cbafc`.

## 19.0.53.1.0 — 2026-09-25

PR 7 del plan, sustituir un proceso sin dejar nada colgado (PR-1).

Commit `01f1fe3`.

## 19.0.53.0.0 — 2026-09-25

PR 6 del plan, proveedores, clientes y firmas (NC-6, AU-4, AU-5, DOC-4, DOC-5, REG-1, REG-2).

Commit `49ac1ac`.

## 19.0.52.0.0 — 2026-09-25

PR 5 del plan, revisión por la dirección de diciembre (DIR-2, DIR-3, DIR-4, PER-3).

**Migración (post, `migrations/19.0.52.0.0/post-migrate.py`):** - DIR-2: los riesgos ya evaluados (probabilidad e impacto capturados) reciben `last_eval_date` = su última modificación y, si no tenían próxima revisión, el siguiente enero o julio; así el cron los pone en el ciclo semestral.

Commit `96b956e`.

## 19.0.51.0.1 — 2026-09-25

Import local de sgi_safe_domain en sgi_audit (el import a nivel de módulo cargaba sgi_activity_spec antes que sgi_catalog y rompía el registro en el build de main).

Commit `e35e665`.

## 19.0.51.0.0 — 2026-09-25

PR 4 del plan, documentos y matriz legal (DOC-1, DOC-3, DIR-1).

**Migración (post, `migrations/19.0.51.0.0/post-migrate.py`):** DIR-1 hace obligatorio el responsable de cada requisito legal: los que estaban sin responsable reciben al Jefe MAST (primer usuario del grupo) y quedan en el log para reasignarlos.

Commit `0cc69b1`.

## 19.0.50.0.0 — 2026-09-25

PR 3 del plan, auditoría lista para octubre (AU-1 a AU-3).

Commit `e7b0a10`.

## 19.0.49.0.1 — 2026-09-25

<group> sin atributos en las vistas de búsqueda (el RNG de Odoo 19 tampoco acepta string=; rompía el build de main).

Commit `68be390`.

## 19.0.49.0.0 — 2026-09-25

PR 2 del plan, no conformidades que sí se cierran (NC-1 a NC-5).

**Migración (post, `migrations/19.0.49.0.0/post-migrate.py`):** - NC-5: las NC del SGI (con folio) que estén en una etapa ajena a los equipos del SGI (Nuevo / Confirmado / Acción propuesta / Resuelto, las nativas de Calidad) pasan a su equivalente (Abierta / Abierta / Seguimiento / Cerrada) y se quita a los equipos del…

Commit `9aeac24`.

## 19.0.48.1.1 — 2026-09-25

Quitar expand= de los <group> de búsqueda (RNG de Odoo 19 lo rechaza; rompía el build de main).

Commit `e1b8db6`.

## 19.0.48.1.0 — 2026-09-25

«Mi procedimiento» como pestaña nativa en la ficha del empleado y del puesto.

Commit `da47ecc`.

## 19.0.48.0.0 — 2026-09-25

PR 1 del plan de auditoría (auditor de solo lectura y EPP con responsiva).

Commit `ed7b0c3`.

## 19.0.47.1.0 — 2026-09-25

Diagnóstico del SGI como lista nativa de hallazgos.

**Migración (post, `migrations/19.0.47.1.0/post-migrate.py`):** el Diagnóstico del SGI ya no es un wizard HTML. Se borran la acción de ventana y el formulario viejos (Odoo no los quita solo al desaparecer del XML) y se vacía la acción del menú si quedó apuntando a algo borrado.

Commit `34c6f2f`.

## 19.0.47.0.0 — 2026-09-25

Mi procedimiento y Revisión previa solo con vistas nativas.

Commit `6cedb34`.

## 19.0.46.1.0 — 2026-09-25

Mi equipo es una lista nativa y «Ver su procedimiento» desde empleado, puesto y equipo.

Commit `83509fd`.

## 19.0.46.0.1 — 2026-09-25

Inicio no apunta a la acción borrada del Panel de procesos.

**Migración (post, `migrations/19.0.46.0.1/post-migrate.py`):** menús del SGI con acción borrada. Odoo no vacía `action` cuando el menuitem deja de traerla; Inicio quedó apuntando al Panel de procesos retirado en 45.0.0 y abría con «El registro no existe».

Commit `bc41ac5`.

## 19.0.46.0.0 — 2026-09-24

El PDF de «Mi procedimiento» trae lo mismo que la pantalla.

Commit `0de7c80`.

## 19.0.45.0.5 — 2026-09-24

Cuatro pruebas dejan de depender de datos de la copia de producción.

Commit `8f92c32`.

## 19.0.45.0.4 — 2026-09-24

La prueba del religado crea la revisión vieja antes que la vigente.

Commit `72ad0f7`.

## 19.0.45.0.3 — 2026-09-24

Las pruebas de limpieza usan claves con nomenclatura heredada válida.

Commit `9d8b415`.

## 19.0.45.0.2 — 2026-09-24

El religado respeta la familia de clave y no tira el build por datos viejos.

**Migración (post, `migrations/19.0.45.0.2/post-migrate.py`):** Limpieza antes de producción (CEO, 2026-09-24). Deja la base igual que `main`: lo que ya no está en el código se borra aquí de forma explícita (menús, acciones y vistas por xmlid, y los dos menús de Studio con su acción), lo que tiene datos se religa…

Commit `550b250`.

## 19.0.45.0.1 — 2026-09-24

La migración de limpieza leía el xmlid después de borrar el registro.

Commit `f0f1380`.

## 19.0.45.0.0 — 2026-09-24

Limpieza antes de producción (menús, acciones, Studio, religado y retiro por ola).

Commit `9c3c5bd`.

## 19.0.44.2.3 — 2026-09-24

Pruebas de la ficha de proceso respetan el candado de medición y usan un IT binario.

Commit `508c9b2`.

## 19.0.44.2.2 — 2026-09-24

La ficha de proceso hereda sin selectores @string (el build de main fallaba).

Commit `83f72e4`.

## 19.0.44.2.1 — 2026-09-24

El aviso semanal de Mi procedimiento cuelga del puesto desactualizado.

Commit `6996fac`.

## 19.0.44.2.0 — 2026-09-24

Paso 6 de la estructura — «Registrar hallazgo» para el auditor en proceso y actividad.

Commit `7169a34`.

## 19.0.44.1.0 — 2026-09-24

Estructura del SGI pasos 3 y 4 — ficha de proceso con pestañas y ficha de actividad con «Ir a hacerlo».

Commit `fff122b`.

## 19.0.44.0.0 — 2026-09-24

Menú del SGI en 5 entradas por perfil (paso 2 de la estructura).

Commit `1c140ca`.

## 19.0.43.3.0 — 2026-09-24

Mi procedimiento en el orden de la estructura del SGI y pantalla «Mi equipo».

Commit `7c510f4`.

## 19.0.43.2.0 — 2026-09-24

Mi procedimiento — correcciones del CEO, documentos del puesto y Mis pendientes.

Commit `a1b1c63`.

## 19.0.43.1.1 — 2026-09-24

La pantalla Mi procedimiento usa hr.employee.public y sudo (usuarios sin lectura de hr.employee).

Commit `1d56893`.

## 19.0.43.1.0 — 2026-09-24

Pantalla «Mi procedimiento» en Inicio (tarjetas por cadencia, estado, firma, equipo).

Commit `809b343`.

## 19.0.43.0.0 — 2026-09-24

«Mi procedimiento» por puesto (PDF archivado, acuses, vista en Inicio).

Commit `414e976`.

## 19.0.42.1.1 — 2026-09-24

Merge main en la rama del COA (versión 19.0.42.1.1).

Commit `4c09108`.

## 19.0.42.1.0 — 2026-09-24

Etiquetas de la frase del procedimiento en negritas en el PDF.

Commit `423a9ff`.

## 19.0.42.0.6 — 2026-09-24

El campo COA ya no revisa todos los adjuntos al abrir un traslado.

Commit `28e16a4`.

## 19.0.42.0.5 — 2026-09-24

Las pólizas de cierre anual no entran a EX-01, EX-02 ni a las fórmulas.

Commit `e32917b`.

## 19.0.42.0.4 — 2026-09-24

Corrige 5 pruebas de I-7, fórmula y P-40 en el build de main.

Commit `0f8a8e1`.

## 19.0.42.0.3 — 2026-09-24

La extensión de sgi.cron (I-6) es AbstractModel.

Commit `a611492`.

## 19.0.42.0.2 — 2026-09-24

Etiqueta única para los términos de la fórmula.

Commit `b6f4055`.

## 19.0.42.0.1 — 2026-09-24

Herencias del formulario de medición sin selector string.

Commit `dc84fa0`.

## 19.0.42.0.0 — 2026-09-24

Meta con trayectoria y sentido «dentro de un rango» (I-7).

Commit `633813d`.

## 19.0.41.0.0 — 2026-09-24

Plan de acción en rojo (I-4), calendario de cálculo (I-6), ventana visible (I-8) y validación masiva (P-40).

**Migración (post, `migrations/19.0.41.0.0/post-migrate.py`):** I-6: los crons de medición pasan a diarios; el método decide si hoy toca medir (tercer día hábil del mes / lunes).

Commit `04b6f3c`.

## 19.0.40.0.1 — 2026-09-24

AL-01 sin stock.valuation.layer (no existe en Odoo 19).

Commit `41f932b`.

## 19.0.40.0.0 — 2026-09-24

Modo «fórmula configurable» con corrida en paralelo.

Commit `3af6092`.

## 19.0.39.0.1 — 2026-09-24

desperdicio_kg sin «not child_of» y dos pruebas ajustadas a la copia de producción.

Commit `ac61a37`.

## 19.0.39.0.0 — 2026-09-24

Reproceso, diferencia de inventario y energía por tonelada (P-21).

Commit `d029849`.

## 19.0.38.0.0 — 2026-09-24

Fórmulas corregidas de MA-05, EX-01 y EX-02 (I-3).

**Migración (post, `migrations/19.0.38.0.0/post-migrate.py`):** I-3: MA-05, EX-01 y EX-02 pasan a las fórmulas aprobadas, solo si siguen en el modo viejo (una decisión de MAST posterior se respeta).

Commit `7b3549e`.

## 19.0.37.0.0 — 2026-09-24

NC por persistencia (I-5).

Commit `3f5ae92`.

## 19.0.36.0.0 — 2026-09-24

Detalle por medición (I-1) e indicadores confiables (I-2).

Commit `a60c0bf`.

## 19.0.35.0.0 — 2026-09-24

No surtir lotes sin liberar (P-7) y entrega del traslado Embarcar.

**Migración (post, `migrations/19.0.35.0.0/post-migrate.py`):** Liga los traslados internos de 2026 con la entrega de su pedido (sgi_delivery_picking_id), para que C2.21 y C2.26 tengan historia.

Commit `82b68fd`.

## 19.0.34.0.0 — 2026-09-24

Relaciones para ligar entradas (P-3) y vencimiento por campo (P-4).

**Migración (post, `migrations/19.0.34.0.0/post-migrate.py`):** P-3: llena las ligas nuevas con lo que ya existe. - ``documents.document.sgi_doc_change_id``: la última solicitud de cambio documental aprobada que apunta al documento.

Commit `040418d`.

## 19.0.33.0.0 — 2026-09-24

Modos genéricos del indicador (P-1).

Commit `7cdff0d`.

## 19.0.32.0.2 — 2026-09-23

El estado del COA se recalcula al marcar al cliente.

**Migración (post, `migrations/19.0.32.0.2/post-migrate.py`):** Recalcula el estado del COA de las salidas abiertas que ya tienen «Requiere COA»: en 19.0.32.0.1 se quedaron en «no aplica» porque solo se recalculó el requisito y no el estado que depende de él.

Commit `d9b3665`.

## 19.0.32.0.1 — 2026-09-23

La vista del COA en el cliente no lleva group_ids.

Commit `e1d329d`.

## 19.0.32.0.0 — 2026-09-23

COA ligado a la entrega y al pedido (fase 1).

**Migración (post, `migrations/19.0.32.0.0/post-migrate.py`):** COA fase 1 (19.0.32.0.0). - Marca «Requiere COA en cada embarque» en los 12 clientes que lo piden (compañía principal).

Commit `c394075`.

## 19.0.31.0.0 — 2026-09-23

El SGI muestra solo la estructura vigente.

Commit `030deca`.

## 19.0.30.1.2 — 2026-09-23

El reporte del procedimiento no rendía.

Commit `b56d4cd`.

## 19.0.30.1.1 — 2026-09-23

Pruebas aisladas de los datos reales (secciones C y D).

Commit `2eec9bc`.

## 19.0.30.1.0 — 2026-09-23

Calendario hábil, semana ISO, candados probados con usuario real, NC sin etapa.

**Migración (post, `migrations/19.0.30.1.0/post-migrate.py`):** Días hábiles del SGI (19.0.30.1.0): el calendario de la compañía en producción (id 9) es de lunes a jueves y sábado.

Commit `83dcc83`.

## 19.0.30.0.0 — 2026-09-23

Actividades específicas (dónde, cómo, terminado, plazo, si falla, escalamiento).

**Migración (post, `migrations/19.0.30.0.0/post-migrate.py`):** Actividades específicas (19.0.30.0.0): calcula los faltantes de especificación de las actividades que ya existían.

Commit `f72d17d`.

## 19.0.29.5.0 — 2026-09-22

Replaces archiva también las actividades del proceso sustituido.

Commit `ed99744` (PR #333).

## 19.0.29.4.0 — 2026-09-22

Llaves desconocidas son error, replaces, fórmula del indicador, fecha compromiso registrada.

Commit `385f47c` (PR #332).

## 19.0.29.3.0 — 2026-09-22

Condición solo en aprueba/informa; inicio y fin calculados.

Commit `d10f86e` (PR #331).

## 19.0.29.2.0 — 2026-09-22

Estructura en vez de texto — numeral, etapas, entregables que miden y conectan.

**Migración (post, `migrations/19.0.29.2.0/post-migrate.py`):** Estructura en vez de texto (19.0.29.2.0). - Paso entero por actividad, en el orden en que ya estaban (secuencia, id); el numeral se recalcula como clave del proceso + paso.

**Migración (pre, `migrations/19.0.29.2.0/pre-migrate.py`):** Numeral y sección dejan de ser texto: se guardan antes del update. El numeral pasa a calcularse (clave del proceso + paso).

Commit `797494d` (PR #330).

## 19.0.29.1.0 — 2026-09-22

Menú de seis entradas y fichas de proceso y actividad simples.

Commit `3f4fc59`.

## 19.0.29.0.0 — 2026-09-22

Catálogo único, fase 1 (roles, tipos de documento, revisión entera, carga por API).

**Migración (post, `migrations/19.0.29.0.0/post-migrate.py`):** Fase 1 del catálogo — después de cargar modelos y datos. - company_id = 1 (Quimibond) en todo lo del catálogo que aún no la tenga.

**Migración (pre, `migrations/19.0.29.0.0/pre-migrate.py`):** Fase 1 del catálogo — antes de cargar los modelos nuevos. 1. La revisión del documento pasa de texto a entero.

Commit `eeb0257`.

## 19.0.28.1.0 — 2026-09-21

quimibond_sgi: declarar hr_timesheet en depends.

Commit `dab9ef2` (PR #324).

## 19.0.28.0.0 — 2026-08-29

quimibond_sgi: fix multi-compañía del motor de indicadores y saneo de mediciones.

Commit `0218746` (PR #199).

## 19.0.27.0.0 — 2026-08-29

quimibond_sgi: KPIs automáticos del plan de expansión comercial EX-01..EX-16.

Commit `5f85c6c` (PR #194).

## 19.0.26.0.0 — 2026-08-24

Integraciones Sign/eLearning y digest semanal.

Commit `26f13c9`.

## 19.0.25.0.1 — 2026-08-24

**Corregido:** Quitar expand del group en search views (hotfix del build).

Commit `4c9eef8`.

## 19.0.25.0.0 — 2026-08-24

Bump a 19.0.25.0.0 + README de la ola certificable.

Commit `ac63ec1`.

## 19.0.24.0.0 — 2026-08-24

Bump a 19.0.24.0.0 (correcciones de la auditoría 2026-08).

Commit `b75c5c4`.

## 19.0.23.2.0 — 2026-08-24

**Corregido:** Migración pre-update — la vista heredada vieja bloqueaba el rename.

**Migración (pre, `migrations/19.0.23.2.0/pre-migrate.py`):** borra la vista heredada vieja de `activity_ids` antes del update (el rename chocaba con ella).

Commit `9d58eae`.

## 19.0.23.1.0 — 2026-08-24

**Corregido:** activity_ids del proceso chocaba con mail.activity.mixin — el build no cargaba.

Commit `7f7646d`.

## 19.0.23.0.0 — 2026-08-24

**Corregido:** El aviso de eslabón atorado tumbaba el cron — sgi.process sin activity_schedule.

Commit `f1813ac`.

## 19.0.22.2.0 — 2026-08-24

**Corregido:** La resolución de menús tronaba — complete_name no es almacenado.

Commit `5a3d3e1`.

## 19.0.22.1.0 — 2026-08-23

Resolver el menú del formulario de Odoo desde su destino de migración.

Commit `5497628`.

## 19.0.22.0.0 — 2026-08-23

Formularios consistentes y documento tipo «Formulario de Odoo».

Commit `352c9c8`.

## 19.0.21.1.0 — 2026-08-23

Ficha de proceso más clara y con el procedimiento al frente.

Commit `9a824a8`.

## 19.0.21.0.0 — 2026-08-23

Flujo de la cadena, NC automatica y tablero de cumplimiento (fase 11).

Commit `409ba34`.

## 19.0.20.5.0 — 2026-08-23

Referencias entrantes entre procedimientos (fase 10.5).

Commit `ef4a11b`.

## 19.0.20.4.0 — 2026-08-23

La cadena visible en todo el sistema (fase 10.4).

Commit `484d924`.

## 19.0.20.3.0 — 2026-08-23

Encadenamiento entre actividades del procedimiento (fase 10.3).

Commit `3b4b5bb`.

## 19.0.20.2.0 — 2026-08-23

El paso del procedimiento siempre navegable (fase 10.2).

Commit `0bf8c4e`.

## 19.0.20.1.0 — 2026-08-23

Mapeo de medicion por nombre tecnico y cron blindado (fase 10.1).

Commit `e403001`.

## 19.0.20.0.0 — 2026-08-23

Actividades de procedimiento medibles con acciones reales de Odoo (fase 10).

Commit `72cfe15`.

## 19.0.19.6.0 — 2026-08-23

El menu Procesos abre primero el mapa de procesos.

Commit `d1ce7f5`.

## 19.0.19.5.0 — 2026-08-23

Un solo juego de etapas NC compartido por ambos equipos.

Commit `7e1bd68`.

## 19.0.19.4.0 — 2026-08-23

Los menus de Calidad preventiva/Tableros se acomodan antes de Configuracion.

Commit `a127299`.

## 19.0.19.3.0 — 2026-08-23

Del formato a su worksheet en un clic (liga real de migracion).

Commit `93a4445`.

## 19.0.19.2.0 — 2026-08-23

Las encuestas de fase 9 viven en produccion (creadas por MCP), no como semilla.

Commit `a0d8a54`.

## 19.0.19.1.0 — 2026-08-23

Especificaciones del producto (C04-06/C14-02) y EPP por puesto (S03-01).

Commit `1f9b2b8`.

## 19.0.19.0.0 — 2026-08-23

Formularios que sustituyen formatos — ronda sin piso (fase 9).

Commit `72bf990`.

## 19.0.18.4.0 — 2026-08-23

La OC estampa su clave real (F-P-A02-03, no la requisicion).

Commit `d6b45ca`.

## 19.0.18.3.0 — 2026-08-23

El menu raiz de Calidad se DESCUBRE en runtime (sin depender de xmlid).

Commit `317325e`.

## 19.0.18.2.0 — 2026-08-23

Build — el menu de Calidad se recuelga en runtime, sin ref dura.

Commit `535fedb`.

## 19.0.18.1.0 — 2026-08-23

Los Tableros de calidad (Paretos) se mudan del Panel a la app Calidad.

Commit `27b9a40`.

## 19.0.18.0.0 — 2026-08-23

La operacion vive en su app — el SGI solo gestiona.

Commit `6865386`.

## 19.0.17.0.0 — 2026-08-23

Candados AIAG en AMEF (NPR post a la baja) y PPAP por nivel (S/R).

Commit `542c807`.

## 19.0.16.8.0 — 2026-08-21

UX de 'Panel' y 'Revisión por la Dirección' — cierre del tour de menús.

Commit `8bd244f`.

## 19.0.16.7.0 — 2026-08-21

UX de 'Calidad preventiva' — equipos de medición, AMEF, planes y calibraciones.

Commit `afcac97`.

## 19.0.16.6.0 — 2026-08-21

UX de 'Riesgos y auditorías' — el auditor no sale de la auditoría.

Commit `ed16f64`.

## 19.0.16.5.0 — 2026-08-21

UX de 'Medición' — el KPI enseña su configuración y su tendencia.

Commit `9e4bf8e`.

## 19.0.16.4.0 — 2026-08-21

UX de 'Documental' — registrar, difundir y cambiar sin fricción.

Commit `ef875cc`.

## 19.0.16.3.0 — 2026-08-21

UX de 'Mejora continua' — el tablero de NC ya no pierde registros.

Commit `f1939e0`.

## 19.0.16.2.0 — 2026-08-21

UX de 'Mi trabajo' — el empleado puede ver, entender y TERMINAR lo suyo.

Commit `8851442`.

## 19.0.16.1.0 — 2026-08-21

Diagnóstico conectado al piso + ligado rápido de puntos de control.

Commit `370b660`.

## 19.0.16.0.0 — 2026-08-21

Del OTD de proveedores + Diagnóstico del SGI.

Commit `533a896`.

## 19.0.15.2.0 — 2026-08-21

UX de procesos — el sistema se explica solo.

Commit `7d5add7`.

## 19.0.15.1.0 — 2026-08-21

Fase 8 — las señales que Odoo ya registra alimentan la mejora continua.

Commit `2834a82`.

## 19.0.15.0.0 — 2026-08-21

Fase 7 — cierre de bucles ISO (pasos 3-8 de la evaluación).

Commit `88bfbfb`.

## 19.0.14.1.0 — 2026-08-21

Auditoría de flujos — lote 1 de mejoras (quick wins).

**Migración (pre, `migrations/19.0.14.1.0/pre-migration.py`):** Los 6 ir.config_parameter que se declaraban como `<record>` en data (además de sembrarse en sgi.config.seed_parameters) dejan de estar en los XML.

Commit `82f6d8e`.

## 19.0.14.0.0 — 2026-08-17

Registro de fuentes de NC automaticas — interruptor por motivo.

Commit `88c68b1`.

## 19.0.13.12.0 — 2026-07-24

**Cambiado:** Simplificar el modelo del presupuesto sin cambiar comportamiento.

Commit `6cb068b`.

## 19.0.13.11.0 — 2026-07-24

**Corregido:** Multicompañía en el presupuesto — filtrar por compañía y blindar el prefetch.

Commit `38feafb`.

## 19.0.13.10.0 — 2026-07-24

Ajustes post-revisión — precios honestos, gobernanza del revisado y conciliación.

Commit `844d50a`.

## 19.0.13.9.0 — 2026-07-22

Cobertura del pronóstico — pedidos capturados vs pronosticados.

Commit `a85ff37`.

## 19.0.13.8.0 — 2026-07-22

**Corregido:** Import de presupuesto — resolución de cliente exacta antes que parcial.

Commit `36aba44`.

## 19.0.13.7.0 — 2026-07-21

Presupuesto y pronóstico alineados al P-A28 Rev.15 — ciclos, roles e interconexión.

**Migración (post, `migrations/19.0.13.7.0/post-migration.py`):** Mini-fase 5.5 (P-A28 Rev.15): el pronóstico es un documento vivo y ya no se aprueba, su estado terminal es 'revisado'.

Commit `f6df476`.

## 19.0.13.6.0 — 2026-07-21

Control de precios — lista vs facturado y cobertura de listas.

Commit `1d3fe29`.

## 19.0.13.5.0 — 2026-07-21

Análisis del presupuesto — submenú con 4 vistas preconfiguradas.

Commit `24fb45f`.

## 19.0.13.4.0 — 2026-07-21

**Corregido:** Borrar un presupuesto ya no truena (flush sobre líneas eliminadas).

Commit `bc44030`.

## 19.0.13.3.0 — 2026-07-21

**Corregido:** Plantilla de presupuesto — filas por cliente × producto del historial.

Commit `2648ae6`.

## 19.0.13.2.0 — 2026-07-21

Presupuesto — paso 3, panel comercial, curva y cierre accionable.

Commit `bba9d83`.

## 19.0.13.1.0 — 2026-07-21

Presupuesto — paso 2, menú padre, plantilla descargable, ficha guiada.

Commit `40dca6c`.

## 19.0.13.0.0 — 2026-07-21

Presupuesto "solo cantidades" — paso 1, manda la lista, doble moneda.

Commit `afd60a6`.

## 19.0.12.2.0 — 2026-07-21

Pronóstico — consumo (demanda neta) y envío al MPS.

Commit `6f2334d`.

## 19.0.12.1.0 — 2026-07-21

Pronóstico semanal — paso 2, importación, impresión y menús.

Commit `e6ad047`.

## 19.0.12.0.0 — 2026-07-21

Pronóstico semanal por cliente — paso 1, el modelo aprende semanas.

Commit `1b56cfa`.

## 19.0.11.6.0 — 2026-07-21

**Corregido:** Presupuesto de ventas — el pivot puede agregar facturado/pedido.

Commit `aacbecf`.

## 19.0.11.5.0 — 2026-07-21

Presupuesto de ventas — importación desde el Excel real (F-P-A28-18).

Commit `780b740`.

## 19.0.11.4.0 — 2026-07-21

Presupuesto de ventas — precio sugerido desde la lista del cliente.

Commit `b8aecfd`.

## 19.0.11.3.0 — 2026-07-21

Presupuesto de ventas — dimensión cliente opcional (sin doble conteo).

Commit `26c3009`.

## 19.0.11.2.0 — 2026-07-21

Presupuesto de ventas — paso 3, conexión al SGI (VE-02 + cierre de mes).

Commit `cf2d3b5`.

## 19.0.11.1.0 — 2026-07-21

Presupuesto de ventas — paso 2, matriz, menús, import y reporte.

Commit `a1d825d`.

## 19.0.11.0.0 — 2026-07-21

Presupuesto maestro de ventas — paso 1, modelo + real automático.

Commit `707e0c8`.

## 19.0.10.1.0 — 2026-07-21

KPIs automáticos 2.0 — paso 2, parámetros + proxy + RH-02.

Commit `1d23c04`.

## 19.0.10.0.0 — 2026-07-21

KPIs automáticos 2.0 — paso 1, los 4 directos.

Commit `8007db2`.

## 19.0.9.2.0 — 2026-07-21

Abrir el archivo desde las listas de documentos.

Commit `7ff62e5`.

## 19.0.9.1.0 — 2026-07-20

**Corregido:** Cierre olas A+B — semilla no dispara G14; escalar incidente fuerza NC.

Commit `7e5032e`.

## 19.0.9.0.0 — 2026-07-20

**Cambiado:** Paquete OLA A + OLA B.

Commit `ec36895`.

## 19.0.8.1.0 — 2026-07-20

Actividad del procedimiento ligada a un menú real de Odoo.

Commit `be3225a`.

## 19.0.8.0.0 — 2026-07-20

Procedimiento vivo paso 1 — secciones y actividades como datos.

Commit `a6ae15c`.

## 19.0.7.0.0 — 2026-07-20

OLA2 paso 1 — política integral, cabeza de la cascada (H13).

Commit `1b23137`.

## 19.0.6.0.0 — 2026-07-20

OLA1 paso 1 — causa raíz antes que acción (H8) + cierre real de NC mayor.

Commit `8cdc607`.

## 19.0.5.0.0 — 2026-07-20

**Cambiado:** OLA0 paso 1 — sgi.base.mixin, mata duplicacion de folio.

Commit `56b2a4e`.

## 19.0.4.4.17 — 2026-07-20

**Corregido:** Candados criticos de la auditoria ISO (H1, H5, H6).

Commit `99692c7`.

## 19.0.4.4.16 — 2026-07-20

**Corregido:** Quitar target=inline de la accion de Ajustes (Odoo 19 lo elimino).

Commit `2421d59`.

## 19.0.4.4.15 — 2026-07-20

Clic en el valor -> Ver evidencia; el KPI explica su comportamiento.

Commit `56636eb`.

## 19.0.4.4.14 — 2026-07-20

Cada indicador declara su fuente — automatico vs manual.

Commit `17012a0`.

## 19.0.4.4.13 — 2026-07-20

Panel de Ajustes amigable (adios a Parametros del sistema).

Commit `b3c6ab3`.

## 19.0.4.4.12 — 2026-07-20

**Cambiado:** Revision vista por vista de listas y formularios.

Commit `c3ad5d1`.

## 19.0.4.4.11 — 2026-07-20

Siembra de objetivos de proceso desde los SIPOC reales.

Commit `89a2f15`.

## 19.0.4.4.10 — 2026-07-20

Navegacion reagrupada — de 21 entradas de primer nivel a 9.

Commit `1bc2f97`.

## 19.0.4.4.9 — 2026-07-20

**Cambiado:** Limpieza de auditoria integral.

Commit `fe18f1b`.

## 19.0.4.4.8 — 2026-07-20

**Corregido:** Quitar expand del grupo de busqueda (rompia el build de main).

Commit `6813a22`.

## 19.0.4.4.7 — 2026-07-20

**Cambiado:** Mejores practicas de UI/UX en todas las vistas.

Commit `407ed89`.

## 19.0.4.4.6 — 2026-07-20

Menu Mis acciones — la lista personal de pendientes del SGI.

Commit `b8010d4`.

## 19.0.4.4.5 — 2026-07-20

Familia documental por nomenclatura + referencias cruzadas.

Commit `9bdbde7`.

## 19.0.4.4.4 — 2026-07-20

Ficha completa de proceso (caracterizacion navegable).

Commit `5382ca7`.

## 19.0.4.4.3 — 2026-07-20

**Corregido:** Claves reales de venta segun P-A28 Rev.15.

Commit `a483288`.

## 19.0.4.4.2 — 2026-07-20

**Corregido:** Endurecer noupdate en crons y mapeo de claves ya existentes.

Commit `567b233`.

## 19.0.4.4.1 — 2026-07-20

**Corregido:** Siembra de parametros idempotente (el build fallaba por clave duplicada).

Commit `54eae11`.

## 19.0.4.4.0 — 2026-07-20

Mapa completo de entradas/salidas entre procesos (mini-fase 4.8).

Commit `eafc6b8`.

## 19.0.4.3.1 — 2026-07-20

**Corregido:** Pie de clave SGI dentro del area imprimible (div.page).

Commit `6ac758f`.

## 19.0.4.3.0 — 2026-07-20

Claves de formato SGI en pantalla y PDF (mini-fase 4.7).

Commit `6cd925d`.

## 19.0.4.2.1 — 2026-07-20

**Corregido:** action_view_sgi_ppap también en product.product.

Commit `69e45e4`.

## 19.0.4.2.0 — 2026-07-20

**Corregido:** Diferencia Panel (tablero de salud) de Procesos (administración).

Commit `170211a`.

## 19.0.4.1.0 — 2026-07-17

Menú de seguimiento de migración de formatos a Odoo.

Commit `105d089`.

## 19.0.4.0.0 — 2026-07-17

Liga la calidad operativa del piso al SGI (Fase 4.1).

Commit `57aec54`.

## 19.0.3.0.0 — 2026-07-17

Fase 3 — herramientas automotrices (core tools).

Commit `ab08126`.

## 19.0.2.0.0 — 2026-07-17

Objetivos, indicadores y medición F-P-A10-03 (Fase 2).

Commit `35844c7`.

## 19.0.1.0.0 — 2026-07-17

Estructura, seguridad, catálogos y mapa de procesos.

Commit `f992ba6`.
