# 12 — Transición «Del Dropbox a Odoo» (agente L)

**Fecha:** 2026-09-29 · **Módulo:** `quimibond_sgi` 19.0.56.24.1 · **Modo:** solo lectura (código + producción por MCP con `search_records`/`aggregate_records`/`get_record`/`get_fields`; ningún write).
**Fuente del análisis rutina por rutina:** `Del_Dropbox_a_Odoo_rutina_por_rutina` (export de la Google Sheet de Jose, en el scratchpad de la sesión como `rutinas_A.xlsx`). **No se copió al repo** (decisión 4: datos de producción fuera del módulo); aquí solo queda el formato, el validador y sus resultados.
**Apoyo en `docs/audit/12-transicion/`:**

| Archivo | Qué es |
|---|---|
| `faltantes_transicion.csv` | 491 renglones (52 procedimientos, 375 documentos del Dropbox y 64 «Formulario de Odoo» que también vienen del Dropbox): id, clave, tipo, estados y **qué le falta** según la regla de completitud de §3.8 |
| `procedimientos_2026-09-29.csv` | foto de producción de los 52 procedimientos (id, clave, estado, estado de migración, proceso que lo sustituye, proceso, PDF) |
| `documentos_dropbox_2026-09-29.csv` | foto de los 439 documentos no procedimiento (clave, tipo, clase, estado, si tiene destino, liga real y padre) |
| `actividades_activas_2026-09-29.csv` | las 310 actividades activas (id, numeral) |
| `validar_rutinas.py` | validador en Python puro (openpyxl solo si la entrada es .xlsx) |
| `resultado_validacion_2026-09-29.txt` | salida del validador contra el libro de Jose: **OK, 0 errores, 1 aviso** |
| `plantilla_rutina_por_rutina.csv` | encabezado del formato de importación (sin datos) |

## 1. Resumen

- Hoy la transición vive repartida en 14 piezas (B-020) y el análisis rutina por rutina **no tiene dónde guardarse en Odoo**. Propongo un solo modelo nuevo, `sgi.legacy.routine`, con el procedimiento como encabezado (el documento, que ya es la fuente de verdad de la sustitución), más dos vistas SQL de solo lectura: el buscador por clave anterior y el avance por proceso (§3).
- El libro de Jose **pasa el validador sin errores**: 815 rutinas de 49 procedimientos (709 cubiertas, 68 reemplazadas, 38 pendientes), los 263 numerales citados existen entre las 310 actividades activas, sin duplicados `(clave, n)`, sin P-I01, y el «Proceso nuevo» de cada procedimiento coincide con Odoo. Ningún procedimiento sustituido tiene rutinas pendientes. El único aviso: las 38 pendientes no tienen decisión ni responsable.
- **Completitud medida en producción:** 0 de 427 documentos del Dropbox (0 de 52 procedimientos) tienen completos sus datos de transición, porque la clave anterior está vacía en todos. Sin contar la clave, que se llena con una migración, quedan completos 174 de 427 (120 formatos y 54 formatos de instructivo) y **ningún procedimiento** (les faltan el estado decidido y las rutinas). La lista exacta está en `faltantes_transicion.csv`.
- **Lo más grave:** P-I01, el procedimiento con credenciales, **lo puede abrir cualquier usuario interno** (`access_internal = view`, L-001). Además, la migración que propone C-001 rellena la sustitución desde el lado del proceso, **contra la decisión 3**, y marcaría como sustituidos a 13 procedimientos, entre ellos P-I01 (L-003). También, la migración de la clave anterior (C-004) deja fuera 64 formatos del Dropbox (L-004).
- 21 hallazgos: 1 crítico, 7 altos, 9 medios y 4 bajos. Esfuerzo de construcción: unas 57 h, sin contar la captura de datos que le toca a MAST.

## 2. Hallazgos

| ID | Elemento | Hallazgo | Evidencia | Severidad | Acción | Propuesta concreta | Esfuerzo (h) | Depende de |
|---|---|---|---|---|---|---|---|---|
| L-001 | `documents.document` 3560 (P-I01), carpeta de C4; `sgi.process` 102 (C4) `replaced_document_ids` | El procedimiento que, según Jose, **contiene credenciales** está en `access_internal = view`: cualquier usuario interno lo abre desde Documentos. Además está en la M2M de sustitución de C4 y en «migrado». Su familia (6 IT-P-I01, 7 DAT P-I01 y 3 F-P-I01) también está abierta a lectura, y **no se verificó si contiene credenciales** (no se abrió ningún contenido) | `search_records documents.document [sgi_code='P-I01']` → 3560, `access_internal: view`, `sgi_migration_state: migrado`; `aggregate [controlado, tipo≠MP] por access_internal` → 50 de 52 procedimientos en `view`; `search_records sgi.process` → C4 `replaced_document_ids [3574, 3573, 3560]` | Crítica | Corregir | **Hoy, a mano (Jose o Sistemas):** poner 3560 en `access_internal = none`, dejar como miembros solo a Jose y al Jefe MAST, sacarlo de la carpeta pública y **rotar las credenciales** que contiene. Después, revisar a mano (Jose) si su familia I01 contiene credenciales. En la sección: P-I01 queda excluido del importador y del buscador (parámetro `quimibond_sgi.dropbox_excluded_codes = P-I01`), y cuando se archive deja de aparecer. Ningún informe ni CSV de esta auditoría cita su contenido | 1 (+ rotación, Sistemas) | — |
| L-002 | Rutinas de los procedimientos anteriores (no hay modelo) | Las 815 rutinas y su destino (actividad, «lo hace Odoo», pendiente) viven solo en una hoja de cálculo. Por eso no hay «rutina por rutina» (decisión 7), ni avance por proceso, ni evidencia de control de cambios dentro del SGI (ISO 9001 6.3) | 00-inventario («no hay modelo»); `grep -rn legacy_routine addons/` → 0 | Alta | Agregar | Modelo `sgi.legacy.routine` (§3.2): procedimiento anterior (documento), n, rutina, frecuencia, responsable anterior, estado (cubierta / reemplazada / eliminada / pendiente), actividades nuevas M2M, motivo, decisión y responsable de las pendientes, revisión, resolución (fecha y quién). Con `mail.thread`, llave única `(procedimiento, n)` y sin borrado. Contadores guardados en el documento procedimiento | 10 | — |
| L-003 | Migración de C-001 (`03-modelo.md` §2.1, paso 2: `UPDATE documents_document … FROM sgi_process_replaced_doc_rel`) | La migración propuesta **copia la M2M del proceso a la M2O del documento** donde la M2O está vacía. La decisión 3 dice lo contrario: «No se rellena nada desde el lado del proceso; los 23 que hoy tiene el documento son la decisión». Si corre, 13 procedimientos «con pendientes», **entre ellos P-I01**, quedan como sustituidos, y la clasificación 23 / 21 / 5 se rompe. Además, al entrar C4 en vigor, P-I01 se volvería obsoleto de forma automática | `search_records sgi.process` (M2M): 3538, 3573, 3560, 3558, 3557, 3555, 3548, 3546, 3545, 3532, 3505, 3536, 3499 solo del lado del proceso; decisiones.md §3 | Alta | Corregir | En `19.0.56.25.0/pre-migrate.py` se queda **solo el respaldo** (paso 0) y el registro de conflictos (paso 1). El paso 2 se quita. Así, al pasar `replaced_document_ids` a One2many inversa, el lado del proceso sale de los 23 del documento. Prueba: después de migrar, `process.replaced_document_ids` = documentos con `sgi_replaced_by_process_id = process` (23 en 11 procesos) y 3560 fuera de C4 | 0.5 | C-001 |
| L-004 | Migración de C-004 (`03-modelo.md` §2.2: `sgi_doc_type_id NOT IN (… 'formulario_odoo' …)`) | Excluye los **64 «Formulario de Odoo (vista)»**, que son formatos del Dropbox ya migrados (claves F-…; E-004 lo confirma). Sin clave anterior, el buscador no encuentra, por ejemplo, F-P-G05-01 (NC, 4086), F-P-A28-12 (pedido, 3878) ni F-P-M01-01 (orden de mantenimiento, 3658), justo los que más se van a buscar | `documentos_dropbox_2026-09-29.csv`: 64 filas `formulario_odoo`, todas con clave F-… o F-IT-… | Alta | Corregir | Quitar `'formulario_odoo'` de la exclusión. Resultado esperado: **490 filas** (426 + 64) la primera vez y 0 la segunda. 5119 (P-A28 obsoleto) sí entra, porque es la misma clave vieja | 0.5 | C-004, C-006 |
| L-005 | `documents.document.sgi_migration_state` de los procedimientos; `sgi_cleanup.py:155-171` | Los 49 procedimientos más P-I01 están en «migrado» (C-003). Hay que aplicar la decisión: 23 sustituidos y 21 con pendientes a «En curso», 5 de control operacional a «No aplica». Además, nada pasa a «Baja tramitada» cuando el proceso entra en vigor: `_sgi_obsolete_replaced_documents` escribe `sgi_state`, el motivo y la M2O, pero **no** `sgi_migration_state` | `procedimientos_2026-09-29.csv` (50 «migrado», 1 «baja» = 5119, 1 «pendiente» = 5556); `sgi_cleanup.py:164` | Alta | Corregir | Migración SQL idempotente de §3.10 (44 filas a `en_curso` y 5 a `na`; la segunda corrida da 0). En `sgi_cleanup.py:164` se agrega `'sgi_migration_state': 'baja'`. Restricción nueva (solo para escrituras nuevas) en procedimientos: nunca «migrado»; «baja» solo si el proceso que lo sustituye está vigente; con proceso que lo sustituye, 0 rutinas pendientes; «No aplica» sin proceso que lo sustituya | 2 | C-003, L-003 |
| L-006 | Buscador por clave anterior (menú `menu_sgi_dropbox_search`, E-001) | No existe. El filtro de Documentos solo busca la clave anterior durante 12 meses (C-005) y no ve rutinas ni numerales de las copias archivadas (144 `legacy_number`, todas en actividades archivadas) | `sgi_document_views.xml:258`; `search_records sgi.process.activity [active=False, legacy_number≠False]` → 144 | Alta | Agregar | Vista SQL `sgi.dropbox.key` (§3.4) que une (1) documentos con clave anterior, (2) rutinas («P-A01 · 12») y (3) numerales archivados («P-DYD 4.1.2»). Botón **«Abrir en Odoo»**: menú → acción del menú, worksheet → punto de calidad, rutina → sus actividades, procedimiento → proceso que lo sustituye. Una regla de registro hace que solo se vean los documentos que el usuario puede leer | 8 | L-002, L-004, C-005 |
| L-007 | Menús y acciones de la sección (E-001: 4 de 5 por crear) | Hay que concretar lo que E dejó reservado: acción de rutinas, de procedimientos anteriores y de avance, con vistas propias de solo lectura | `05-menus/arbol_final.md` §«Del Dropbox a Odoo» | Alta | Agregar | Vistas, acciones y filtros por defecto de §3.5 y §3.6, con el mismo árbol y los mismos xml_id que `arbol_final.md` (10 buscador, 20 procedimientos, 30 formatos = menú 2419, 40 rutinas, 50 avance). El importador va como botón en la lista de rutinas, no como sexto menú | 8 | E-001, E-004, L-002, L-006 |
| L-008 | Importación del análisis | No hay forma de cargar las 815 rutinas sin capturarlas a mano | — | Alta | Agregar | `sgi.legacy.routine.load_routines(payload, dry_run)` (API, igual que `load_payload`) y el asistente «Importar rutinas» (XLSX o CSV), con llave `(clave, n)`, modo de prueba, idempotencia y reporte por fila (§3.9). Solo Jefe MAST | 12 | L-002, L-004, L-005 |
| L-009 | Tablero de avance (menú `menu_sgi_dropbox_progress`) | E-001 propone por ahora un pivote sobre documentos. Con el modelo de rutinas ya se puede tener el tablero completo por proceso (pregunta 6 de 05-menus) | — | Media | Agregar | Vista SQL `sgi.dropbox.progress`, un renglón por proceso vigente (§3.6): procedimientos sustituidos, con pendientes y de control operacional; rutinas por estado y % resuelto; documentos migrados, en papel y pendientes, con ligas a cada lista | 6 | L-002 |
| L-010 | Campos de transición en `documents.document` (`sgi_previous_code`, `sgi_previous_code_date`, `sgi_migration_*`, `sgi_odoo_menu_id`, `sgi_replaced_by_process_id`) | E-005 resuelve la vista. Pero cualquier usuario con escritura en Documentos puede cambiar estos campos por la ficha de Documentos, por importación o por RPC. La decisión 7 dice que solo edita Jefe MAST | `grupos.csv` (316 implica `documents.group_documents_user`); `sgi_document.py:620` (`write` sin guarda) | Media | Corregir | Guarda en `documents.document.write()` y `create()`: si `vals` trae alguno de esos campos, el usuario no está en `group_sgi_manager` y no es `su`, lanzar `AccessError`. Excepciones con contexto `sgi_transition_write=True` para el cron que resuelve menús y para la aplicación de cambios documentales (`sgi_doc_change.py:93`, que copia `sgi_odoo_menu_id`). Prueba con un Usuario SGI | 2 | E-005 |
| L-011 | Destinos de formatos «migrados» (`sgi_migration_target` sin `sgi_odoo_menu_id` ni worksheet) | **50 formatos** (48 F y 2 F-IT) de clase A o B en «migrado» y 2 «en curso» solo tienen el destino en texto: el botón «Abrir en Odoo» no funciona. El cron solo resuelve el menú de los «Formulario de Odoo» (`sgi_process_procedure.py:1275`) | `faltantes_transicion.csv` (regla «migrado sin liga real…»); `search_records [menú=False, worksheet=False, estado in (migrado, en_curso)]` → 89, de ellos 52 de clase A o B | Media | Corregir | Ampliar el dominio del cron a `sgi_migration_class in ('a','b')` de cualquier tipo del Dropbox. Lo que no se resuelva queda en el filtro «Sin liga real» de «Formatos y documentos anteriores» para que MAST lo ligue a mano | 1 | — |
| L-012 | Clase de migración en tipos que no son formato | **118** documentos del Dropbox sin clase: 48 IT, 42 DAT, 15 anexos, 5 protocolos, 5 reglamentos, el MIID, el diagrama y 1 formulario con «x». De ellos, 25 están «migrado» sin ningún destino (ya en C-003). La selección A/B/C/D se pensó para formatos; nadie decidió qué es un IT «migrado» | `aggregate documents.document por sgi_doc_type, sgi_migration_state`; `faltantes_transicion.csv` | Media | Decidir | Pregunta 3. Mientras tanto, el filtro «Sin clase» por defecto en «Formatos y documentos anteriores» y una restricción (solo para escrituras nuevas): «migrado» exige clase A, B o C con destino, y la clase D exige «No aplica» | 1 (+ captura MAST) | Pregunta 3 |
| L-013 | Clase contra estado de migración | Contradicciones: 6 formatos «No aplica» con clase A (3723, 3807, 3850, 3879, 3880, 3884); 5 de clase D en «pendiente» (3644, 3845, 3859, 4764, 4765); 2 de clase D en «migrado» (4066, 4068); 3359 obsoleto en «pendiente» (ya en C-008) | `documentos_dropbox_2026-09-29.csv` | Media | Corregir | La restricción de L-012 y la lista para MAST en «Formatos y documentos anteriores» (filtro «Clase y estado no cuadran»). No se corrigen por migración: son decisiones de MAST | 0.5 | L-012 |
| L-014 | Familias sin procedimiento | **34 documentos** llevan la clave de un procedimiento que **no está entre los 52**: P-A23 (9 F y 1 R), P-P07 (5 F-IT y 4 IT), P-A13 (8 F), P-A30 (3 F), P-C10 (2 DAT) y P-A05 (2 F-IT). No se les puede poner procedimiento padre, así que no aparecen en ningún «procedimiento anterior» | `search_records [sgi_parent_document_id=False]` → 107 (52 procedimientos + 55); cruce de claves en `faltantes_transicion.csv` (regla «familia … no está entre los 52») | Media | Decidir | Pregunta 2. Recomendado: agrupar por la familia de la clave (`sgi_legacy_family`, calculado desde la clave anterior) y no inventar procedimientos. En el buscador, «familia sin procedimiento cargado» | 1 | C-004 |
| L-015 | Hoja «Pendientes» (38) | Ninguna de las 38 rutinas pendientes tiene decisión (crear actividad, regla o eliminar) ni responsable. Son justo las que impiden que 21 procedimientos pasen a sustituidos | Validador: aviso `pendientes` (38 sin decisión) | Media | Agregar | Campos `decision`, `decision_owner_id` y `decision_deadline` en la rutina, con el filtro por defecto «Pendientes sin decisión» y una columna en el tablero. **No** se genera actividad de Odoo automática por cada pendiente: se decide en revisión con Jose | 0.5 (en L-002) | L-002 |
| L-016 | Llave del procedimiento anterior | Hay dos claves con dos documentos: P-A28 (3538 vigente; 5119 obsoleto, sin archivo, `access_internal = none`) y P-A14 (3518; 5556 borrador, C-008). Si la importación busca solo por clave, es ambigua | `procedimientos_2026-09-29.csv` | Media | Corregir | El importador resuelve la clave al único procedimiento **vigente o en piloto** y activo, y si hay 0 o más de 1 es error. 5119: pregunta 7 | 0.2 | C-008 |
| L-017 | Contadores por procedimiento | «Procedimientos anteriores» y el tablero necesitan agrupar y ordenar por rutinas cubiertas y pendientes | — | Media | Agregar | En `documents.document`, `sgi_legacy_routine_ids` (One2many) y los campos guardados `sgi_routine_count`, `_covered_count`, `_replaced_count`, `_eliminated_count`, `_pending_count` y `sgi_routine_resolved_pct` (compute con `store=True`, depende de `sgi_legacy_routine_ids.state/active`). Solo cambian en procedimientos | 1 | L-002 |
| L-018 | `sgi.process.activity.legacy_number` (C-016) | Con el modelo de rutinas, la liga «de qué procedimiento viene esta actividad» es la M2M de la rutina. No hace falta el `legacy_document_id` que C-016 dejó abierto. `legacy_number` se queda **de solo lectura** para las 144 actividades archivadas (copias P-*/MP-*) y el buscador lo lee | C-016; 144 con dato, 0 en activas | Baja | Documentar | Inversa `sgi_legacy_routine_ids` en la actividad (M2M, misma tabla) y botón inteligente «Viene de» visible solo para 317, 318 y 319 (decisión 11 de la tanda 2: la clave vieja no se muestra al personal). `legacy_number` sin cambios | 0.5 | L-002 |
| L-019 | 47 actividades activas sin rutina de antes | Ninguna rutina cita a 47 de las 310 actividades: S6 completo (11), S2 10, S3 6, S1 5, C2 4, C3 3, C6 3, S5 2, C4 1, C5 1 y S4 1. Es lo esperado (trabajo nuevo, sobre todo TI y facturación), pero en el tablero parecería un hueco | Validador: 263 numerales citados contra 310 (`actividades_activas_2026-09-29.csv`) | Baja | Documentar | Columna «Actividades nuevas (sin antecedente)» en el avance por proceso, con liga a la lista. No es pendiente | 0.3 | L-009 |
| L-020 | Libro de Jose, hoja «Instrucciones» | Dice «47 procedimientos» (son 49) y define un estado «eliminada» que no usa ninguna fila. También trae las columnas «Revisión» y «Comentario» (vacías) para la revisión de Areli | Libro: hoja «Instrucciones», hoja «Rutina por rutina» (815 filas, 0 «eliminada», 0 con revisión) | Baja | Documentar | El importador acepta los cuatro estados y guarda «Revisión» y «Comentario» en la rutina (`review_state`, `review_note`). Corregir el texto del libro (Jose) | 0.1 | — |
| L-021 | Pruebas y versión | Modelos nuevos sin cambio de versión son **error** del CI (`tools/check_addons.py`), y los tests nuevos tienen que ir en `tests/__init__.py` | CLAUDE.md del repo | Baja | Documentar | Versión propia para la sección (p. ej. `19.0.57.0.0`) después de `19.0.56.25.0` (C-001, C-004, C-005, L-003, L-004, L-005). Pruebas de §3.11 registradas | 0 | — |

## 3. Diseño de la sección «Del Dropbox a Odoo»

### 3.1 Lo que existe hoy (y se reubica, B-020)

| Pieza | Dónde | Estado en producción | Uso en la sección |
|---|---|---|---|
| `documents.document.sgi_previous_code` / `_date` | `sgi_document.py:36-41` | 0 de 491 con dato | Llave del buscador (tras C-004/C-005 y L-004) |
| `sgi_migration_class` / `_state` / `_target` / `_point_id`, `sgi_odoo_menu_id` | `sgi_document.py:72, 148-175` | clase: 373 con dato, 118 sin clase; estado en §3.8 | Destino de formatos y documentos (submenú 30) |
| `sgi_replaced_by_process_id` | `sgi_document.py:111` | 23 (decisión 3) | Encabezado de «Procedimientos anteriores» |
| `sgi.process.replaced_document_ids` | `sgi_process.py:52` | 26 (M2M, diverge: C-001) | Pasa a inversa (C-001 sin paso 2, L-003) |
| `sgi_parent_document_id` | `sgi_document.py:242` | falta en 55 no procedimientos (34 son familias sin procedimiento, L-014) | Familia del procedimiento anterior |
| `sgi.process.activity.legacy_number` | `sgi_process_procedure.py:507` | 144, todas archivadas | Buscador (numerales de las copias archivadas) |
| `sgi.process.activity.format_document_ids` / `related_procedure_id` | `:564-577` | 111 actividades activas citan formatos | Buscador: «qué actividad lo usa» |
| `sgi.format.map` (9) | `sgi_format_map.py`, menú 2420 | 9 activos (pedido, OC, remisión, NC…) | Se queda en Configuración (C-006); el buscador lo alcanza vía el documento |
| `sgi.document.type.legacy_code_regex`, `_sgi_legacy_match` | `sgi_catalog.py:593-677` | — | Valida la clave anterior al importar |
| Acción 3870 `sgi_migration_action` + menú 2419 | `sgi_document_views.xml:392-492`, `sgi_menus.xml:127` | solo formatos y sin obsoletos | Submenú 30 (E-004) |
| `sgi_document_resolve_menu_action`, `action_sgi_resolve_odoo_menu`, cron | `sgi_document.py:192`, `sgi_process_procedure.py:1271-1277` | solo `formulario_odoo` | Botón de Jefe MAST (F-016) y cron ampliado (L-011) |
| `sgi.config.migrate_document_families` | `sgi_format_map.py:284` | — | Botón manual de la sección (B-001) |
| Procesos archivados P-*/MP-* (25) y `SGI_RELINK_MAP` | `sgi_cleanup.py:87-104` | 25 archivados | Solo por liga desde el buscador (decisión 3: nada archivado en menús) |

### 3.2 Modelo `sgi.legacy.routine` («Rutina del procedimiento anterior»)

**Por qué un modelo nuevo y ningún encabezado propio.** El encabezado natural (el procedimiento anterior) **ya existe**: es el `documents.document` de tipo procedimiento, que la decisión 3 hizo fuente de verdad de la sustitución (`sgi_replaced_by_process_id`), con estado de migración, proceso, PDF, familia y acuses. Un `sgi.legacy.procedure` duplicaría proceso, estado y sustitución, que es justo lo que C-001 corrige. Los conteos de la hoja «Resumen por procedimiento» salen calculados (L-017).

`_name = 'sgi.legacy.routine'`, `_description = "Rutina del procedimiento anterior"`, `_inherit = ['mail.thread']`, `_order = 'procedure_code, n'`, `_rec_name` calculado `display_name` = «P-A02 · 11 — Resguardar los registros…».

| Campo | Tipo | string | help | Notas |
|---|---|---|---|---|
| `procedure_id` | Many2one `documents.document` | Procedimiento anterior | «Procedimiento del Dropbox del que sale esta rutina (su PDF queda como histórico)». | `required`, `index`, `ondelete='restrict'`, `domain=[('sgi_is_controlled','=',True),('sgi_doc_type','=','procedimiento')]`, `tracking` |
| `procedure_code` | Char | Clave anterior | «Clave del procedimiento en el Dropbox (P-A02). Solo aquí y en el buscador». | `related='procedure_id.sgi_previous_code'`, `store=True`, `index=True` |
| `n` | Integer | N.º | «Número de la rutina dentro del procedimiento anterior, tal como viene en el análisis». | `required`; `CHECK (n > 0)` |
| `name` | Char | Rutina | «Qué se hacía en el sistema anterior, en una línea». | `required`, `tracking` |
| `frequency` | Char | Frecuencia anterior | «Cada cuándo se hacía (texto del procedimiento; no es el vencimiento de Odoo)». | texto libre: hay más de 100 frecuencias distintas |
| `previous_owner` | Char | Responsable anterior | «Puesto que la hacía según el procedimiento del Dropbox». | texto: los puestos viejos no son `hr.job` |
| `state` | Selection | Estado | «Cubierta: una actividad de Odoo la hace. Reemplazada: Odoo la hace solo o era redundante. Eliminada: se dejó a propósito. Pendiente: nadie la cubre todavía». | `[('cubierta',"Cubierta por una actividad"),('reemplazada',"La hace Odoo"),('eliminada',"Eliminada a propósito"),('pendiente',"Pendiente")]`, `required`, `default='pendiente'`, `index`, `tracking` |
| `activity_ids` | Many2many `sgi.process.activity` | Actividades que la cubren | «Actividades del proceso nuevo que hacen esta rutina». | tabla `sgi_legacy_routine_activity_rel (routine_id, activity_id)`, `domain=[('active','=',True)]`, `tracking` |
| `activity_numbers` | Char | Numerales | «Numerales de las actividades, para buscar y exportar». | compute `store=True` (`'; '.join(sorted(number))`) |
| `reason` | Text | Motivo | «Cómo la cubre la actividad, por qué la hace Odoo solo, por qué se eliminó o por qué está pendiente». | `tracking` |
| `process_id` | Many2one `sgi.process` | Proceso nuevo | «Proceso que sustituye al procedimiento, o su proceso actual si todavía no lo sustituye». | compute `store=True`: `procedure_id.sgi_replaced_by_process_id or procedure_id.sgi_process_id`; `index` |
| `procedure_migration_state` | Selection | Estado del procedimiento | — | `related='procedure_id.sgi_migration_state'` (no guardado) |
| `decision` | Selection | Decisión | «Qué se hará con la pendiente». | `[('actividad',"Crear actividad"),('regla',"Volverla regla o automatización"),('eliminar',"Eliminar con motivo")]`, solo visible si `state == 'pendiente'` |
| `decision_owner_id` | Many2one `res.users` | Responsable de decidir | «Quién cierra la pendiente (dueño del proceso o MAST)». | `ondelete='set null'` |
| `decision_deadline` | Date | Fecha compromiso | — | — |
| `review_state` | Selection | Revisión | «Revisión del dueño del proceso sobre el análisis». | `[('ok',"OK"),('corregir',"Corregir")]` (columnas amarillas del libro) |
| `review_note` | Text | Comentario de revisión | — | — |
| `resolved_date` | Date | Resuelta el | «Cuándo dejó de estar pendiente». | `readonly`; lo pone `write()` al salir de «pendiente» |
| `resolved_uid` | Many2one `res.users` | Resuelta por | — | `readonly` |
| `company_id` | Many2one `res.company` | Empresa | — | `related='procedure_id.company_id'`, `store=True` (módulo de una sola empresa, C-012) |
| `active` | Boolean | Activa | — | `default=True`: se archiva, nunca se borra |

**Restricciones:**
- SQL: `unique(procedure_id, n)` («Ya existe la rutina n de ese procedimiento») y `CHECK (n > 0)`.
- `@api.constrains('state','activity_ids','reason')`: «cubierta» exige al menos una actividad; «reemplazada», «eliminada» y «pendiente» exigen motivo; «pendiente» no lleva actividades.
- `@api.constrains('procedure_id')`: procedimiento controlado, activo y no excluido (`quimibond_sgi.dropbox_excluded_codes`).
- En `documents.document` (L-005), `@api.constrains('sgi_replaced_by_process_id','sgi_migration_state')` y el de rutinas (`state`): un procedimiento con proceso que lo sustituye no puede tener rutinas pendientes activas, y al revés.
- Actividad archivada después: la rutina **no** se toca (es historia). El tablero la cuenta como «cubierta por actividad archivada» (filtro), para que MAST la reasigne.

**Índices:** `procedure_id`, `procedure_code`, `state`, `process_id` y la tabla M2M (índice de Odoo por las dos columnas).

**En la actividad (L-018):** `sgi_legacy_routine_ids = fields.Many2many('sgi.legacy.routine', 'sgi_legacy_routine_activity_rel', 'activity_id', 'routine_id', string="Viene de (rutinas anteriores)")`, `groups='quimibond_sgi.group_sgi_auditor,quimibond_sgi.group_sgi_manager,quimibond_sgi.group_sgi_director'`.

**En el documento (L-017):** `sgi_legacy_routine_ids` (One2many) y los seis contadores guardados. Además, `sgi_legacy_family` (Char, compute `store=True`, `index`) = la clave de procedimiento dentro de la clave anterior (`F-P-A23-04` → `P-A23`; `DAT P-C10-01` → `P-C10`), para agrupar familias sin procedimiento (L-014).

### 3.3 Relación con los estados decididos

| Grupo (decisiones.md, C-003) | Cómo se reconoce | `sgi_migration_state` | Rutinas | Qué dispara el cambio |
|---|---|---|---|---|
| 23 sustituidos | `sgi_replaced_by_process_id` con valor | `en_curso` → `baja` | 0 pendientes (validado: se cumple en los 23) | El proceso pasa a `vigente` → `_sgi_obsolete_replaced_documents` obsoleta y pone `baja` (L-005) |
| 21 con pendientes | sin proceso que lo sustituya, no control operacional | `en_curso` | ≥ 1 pendiente (36 de las 38) | Jose liga el proceso **en el documento** cuando las pendientes llegan a 0 (la restricción lo impide antes) |
| 5 de control operacional (P-A17, P-A18, P-A19, P-A20, P-S03) | `na` | `na` | cubiertas + 2 pendientes (P-A18 y P-S03, 1 cada uno) | Nunca se sustituyen: siguen vigentes |
| P-I01 | excluido por parámetro | se decide aparte (pregunta 1) | no se importan | Retiro por separado (L-001) |

### 3.4 Buscador por clave anterior (`sgi.dropbox.key`)

Modelo de solo lectura (`_auto = False`, `_order = 'key'`) sobre una vista SQL. Cada renglón es una clave vieja y dice **qué es hoy y dónde vive**.

| Campo | Tipo | Qué trae |
|---|---|---|
| `key` | Char | `P-A14`, `F-P-G05-01`, `IT-P-P01-08`, `DAT P-C06-01`, `ANEXO 6`, `P-A02 · 11` (rutina) o `P-DYD 4.1.2` (numeral de una copia archivada) |
| `kind` | Selection | procedimiento, formato, formato de instructivo, formulario de Odoo, instructivo, DAT, anexo, protocolo, reglamento, manual, diagrama, rutina, numeral archivado |
| `title` | Char | nombre del documento sin clave (`sgi_title`, C-009) o texto de la rutina |
| `document_id` | M2O `documents.document` | el documento; en rutinas, su procedimiento |
| `routine_id` | M2O `sgi.legacy.routine` | solo rutinas |
| `activity_id` | M2O `sgi.process.activity` | solo numerales archivados |
| `process_id` | M2O `sgi.process` | proceso que lo sustituye o proceso actual |
| `migration_state` / `routine_state` | Selection | estado de migración o de la rutina |
| `odoo_menu_id` / `point_id` | M2O | liga real |
| `destination` | Char (compute, no guardado) | menú > worksheet > texto (`sgi_destination_label`, C-011) o numerales de la rutina |

```sql
CREATE OR REPLACE VIEW sgi_dropbox_key AS
SELECT d.id * 10 + 1 AS id, d.sgi_previous_code AS key, t.code AS kind,
       d.id AS document_id, NULL::int AS routine_id, NULL::int AS activity_id,
       COALESCE(d.sgi_replaced_by_process_id, d.sgi_process_id) AS process_id,
       d.sgi_migration_state AS migration_state, NULL AS routine_state,
       d.sgi_odoo_menu_id AS odoo_menu_id, d.sgi_migration_point_id AS point_id
  FROM documents_document d JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
 WHERE d.sgi_is_controlled AND d.active AND d.sgi_previous_code IS NOT NULL
   AND t.code <> 'mi_procedimiento'
UNION ALL
SELECT r.id * 10 + 2, r.procedure_code || ' · ' || r.n, 'rutina',
       r.procedure_id, r.id, NULL, r.process_id, NULL, r.state, NULL, NULL
  FROM sgi_legacy_routine r WHERE r.active
UNION ALL
SELECT a.id * 10 + 3, p.code || ' ' || a.legacy_number, 'numeral_archivado',
       a.related_procedure_id, NULL, a.id, a.process_id, NULL, NULL, a.odoo_menu_id, NULL
  FROM sgi_process_activity a JOIN sgi_process p ON p.id = a.process_id
 WHERE a.active IS NOT TRUE AND a.legacy_number IS NOT NULL;
```
(Nombres de columna por confirmar contra el esquema en el build. `title` y `destination` se calculan en Python para no leer `jsonb` de traducciones en SQL.)

- **Excluidos:** el parámetro `quimibond_sgi.dropbox_excluded_codes` se aplica en `_where` (P-I01). Un documento archivado no sale (5556, P-I01 cuando se archive).
- **Seguridad de lectura:** `ir.rule` global en `sgi.dropbox.key`: `['|', ('document_id', '=', False), ('document_id', 'any', [])]`. El `any` hace que Odoo aplique a `documents.document` las reglas del usuario, así que nadie ve en el buscador un documento de una carpeta que no puede abrir (carpeta Dirección, decisión 10). **Pendiente de verificación en el build** con una prueba (P-L6).
- **Botones en la lista y la ficha:**
  - **«Abrir en Odoo»** (`action_open_target`). Si hay menú, abre la acción del menú (igual que `action_sgi_open_odoo_form`). Si hay worksheet, el punto de calidad. Si es rutina con una actividad, la actividad; con varias, la lista de actividades. Si es un procedimiento sustituido, la ficha del proceso. Si no hay destino, avisa «Todavía no tiene destino en Odoo».
  - **«Ver el anterior»** abre la ficha de solo lectura del documento con su PDF.
- **Búsqueda:** `<field name="key" filter_domain="['|', ('key','ilike',self), ('title','ilike',self)]"/>` (sin ventana de 12 meses). Filtros: Procedimientos · Formatos · Instructivos y DAT · Rutinas · Sin destino · Pendientes, más «Numerales archivados» (apagado por defecto). Agrupar por proceso y por tipo. El `<group>` del `<search>` va **sin atributos** (regla del RNG, `tools/check_odoo_views.py`).
- `title` necesita ser buscable: se guarda en la vista (columna `title` = `d.name` / `r.name`); el título limpio de C-009 se usa solo al mostrar.

### 3.5 Vistas, acciones y filtros (cuadra con `05-menus/arbol_final.md`)

| Sec. | Menú (xml_id) | Acción | Modelo y dominio | Vistas | Filtro o agrupación por defecto | Edición |
|---:|---|---|---|---|---|---|
| — | **Del Dropbox a Odoo** (`menu_sgi_dropbox`, Procesos, sec. 60) | — | carpeta; hereda 316 y 317 | — | — | — |
| 10 | Buscador por clave anterior (`menu_sgi_dropbox_search`) | `sgi_dropbox_key_action` | `sgi.dropbox.key`, sin dominio | lista (clave, tipo, título, proceso, estado, destino, «Abrir en Odoo»), ficha, búsqueda | ninguno: el cursor queda en «Clave» | solo lectura (modelo SQL) |
| 20 | Procedimientos anteriores (`menu_sgi_dropbox_procedures`) | `sgi_dropbox_procedure_action` | `documents.document`, `[('sgi_is_controlled','=',True),('sgi_doc_type','=','procedimiento')]`, contexto `{'active_test': True}` | lista `sgi_dropbox_procedure_view_list` (clave anterior, título, proceso actual, **lo sustituye**, estado de migración, rutinas, cubiertas, reemplazadas, pendientes, % resuelto); ficha `sgi_dropbox_procedure_view_form` (encabezado de solo lectura, **PDF histórico** con `widget="pdf_viewer"` `readonly="1"`, pestaña «Rutinas» con la One2many editable solo para 318, pestaña «Documentos de la familia»); búsqueda | agrupar por «Estado de migración»; filtros Sustituidos, Con pendientes, Control operacional, **Sin rutinas** | `sgi_replaced_by_process_id` y `sgi_migration_state` editables solo para 318 (`readonly="not user_has_groups(...)"` en la vista y guarda de L-010) |
| 30 | Formatos y documentos anteriores (`menu_sgi_migration`, 2419, se mueve) | `sgi_migration_action` (3870) **[cambia, E-004]** | el dominio de E-004 **más `formulario_odoo`** y sin excluir obsoletos | kanban por estado, lista con `destination`, «Abrir en Odoo» y «Ver archivo» | «No obsoletos» (se puede quitar); filtros nuevos: Sin clase (L-012), Sin liga real (L-011), Clase y estado no cuadran (L-013), Familia sin procedimiento (L-014) | `multi_edit` solo para 318 (vista con `edit="0"` para los demás) |
| 40 | Rutina por rutina (`menu_sgi_dropbox_routines`) | `sgi_dropbox_routine_action` | `sgi.legacy.routine` | lista (clave · n, rutina, frecuencia, responsable anterior, estado con insignia, numerales, motivo, decisión); ficha con chatter; pivote proceso × estado; búsqueda | agrupar por procedimiento; filtros Pendientes, **Pendientes sin decisión**, Reemplazadas, Cubiertas por actividad archivada, Marcadas «Corregir» | crear y editar solo 318; botón **«Importar rutinas»** en el encabezado de la lista (`groups="quimibond_sgi.group_sgi_manager"`) |
| 50 | Avance de la transición (`menu_sgi_dropbox_progress`) | `sgi_dropbox_progress_action` | `sgi.dropbox.progress` | lista con barras (`widget="progressbar"`), gráfica de barras apiladas y pivote | ninguno (14 renglones, uno por proceso) | solo lectura |

Todas las acciones llevan `help` de dos líneas (D-015). Ningún nombre de acción ni de vista lleva claves del Dropbox (decisión 1).

### 3.6 Tablero de avance (`sgi.dropbox.progress`)

Vista SQL, un renglón por proceso **activo**. Columnas (todas enteras, salvo los %):
- **Procedimientos:** `procedures_replaced` (documentos con `sgi_replaced_by_process_id = proceso`), `procedures_pending` (en `en_curso` sin sustitución, con `sgi_process_id = proceso`), `procedures_control` (`na`).
- **Rutinas** (por `sgi.legacy.routine.process_id`): `routines_total`, `_covered`, `_replaced`, `_eliminated`, `_pending`, `routines_resolved_pct = (total − pendientes) / total`.
- **Documentos** del Dropbox no procedimiento, por `sgi_process_id`: `docs_total`, `docs_migrated` (migrado o baja, con liga real), `docs_paper` (clase D o «No aplica»: **lo que sigue en papel**), `docs_pending` (pendiente o en curso), `docs_incomplete` (sin clase, sin liga o contradicción, igual que §3.8).
- `activities_new`: actividades activas del proceso que no cita ninguna rutina (L-019).

Cada número es un botón (`type="object"`) que abre la lista filtrada del submenú 20, 30 o 40. Con los datos de hoy más el libro importado, el tablero diría, por ejemplo, **C5: 5 procedimientos sustituidos, 9 con pendientes y 16 rutinas pendientes**. «% migrado» del proceso = (rutinas resueltas + documentos migrados o que siguen en papel) / (rutinas + documentos). Todo con datos, nada escrito a mano.

### 3.7 Permisos (decisión 7: todos consultan, solo edita Jefe MAST y SGI)

`security/ir.model.access.csv`:

| id | modelo | grupo | r | w | c | u |
|---|---|---|---|---|---|---|
| `access_sgi_legacy_routine_user` | `model_sgi_legacy_routine` | `group_sgi_user` | 1 | 0 | 0 | 0 |
| `access_sgi_legacy_routine_auditor` | `model_sgi_legacy_routine` | `group_sgi_auditor` | 1 | 0 | 0 | 0 |
| `access_sgi_legacy_routine_manager` | `model_sgi_legacy_routine` | `group_sgi_manager` | 1 | 1 | 1 | 0 |
| `access_sgi_dropbox_key_user` / `_auditor` | `model_sgi_dropbox_key` | 316 / 317 | 1 | 0 | 0 | 0 |
| `access_sgi_dropbox_progress_user` / `_auditor` | `model_sgi_dropbox_progress` | 316 / 317 | 1 | 0 | 0 | 0 |
| `access_sgi_legacy_routine_import_manager` (+ `_line`) | asistente | `group_sgi_manager` | 1 | 1 | 1 | 1 |

- **Nadie borra rutinas** (`unlink` = 0; se archivan). Dirección (319) lee por implicar a Usuario SGI; con F-013 no hereda la escritura de 318. Administrador SGI (325) implica a 318.
- **Reglas:** `sgi.dropbox.key` con la regla `any` de §3.4. `sgi.legacy.routine` no necesita regla por empresa (C-012: una sola empresa); si algún día hay multiempresa, `company_id` ya está guardado.
- **Documentos:** guarda en el servidor de L-010, más E-005 en las vistas.
- **Menús:** sin `groups` propios (heredan 316 y 317 de la raíz), como en `arbol_final.md`.

### 3.8 Regla de completitud y medición en producción (2026-09-29)

Un documento del Dropbox tiene **completos sus datos de transición** si cumple todo lo que aplica a su tipo:

| Regla | Aplica a | Qué exige |
|---|---|---|
| R1 | todos | `sgi_previous_code` con dato |
| R2 | procedimiento | estado decidido: sustituido → `en_curso`/`baja`; con pendientes → `en_curso`; control operacional → `na`; nunca `migrado` |
| R3 | procedimiento | rutinas cargadas (≥ 1) |
| R4 | procedimiento | PDF histórico (`datas`) |
| R5 | formato, F-IT, formulario de Odoo, IT, DAT, anexo, PROT, R, MIID, DF | clase A, B, C o D (no vacía ni «x») |
| R6 | ídem | `migrado`/`en_curso` de clase A o B → menú o worksheet (liga real); de clase C → nombre del reporte; sin clase → algún destino |
| R7 | ídem | clase y estado cuadran: D ↔ `na`; A, B o C nunca en `na` |
| R8 | formulario de Odoo | `migrado` con menú |
| R9 | lo que lleva clave de procedimiento (`F-P-…`, `IT-P-…`, `DAT P-…`, `R-P-…`) | procedimiento padre, o familia declarada sin procedimiento (L-014) |

**Resultado** (detalle en `faltantes_transicion.csv`, una fila por documento con todo lo que le falta):

| Tipo | Total | Completos | Completos sin contar R1 | Qué falta, sin contar R1 |
|---|---:|---:|---:|---|
| Procedimiento | 52 | 0 | 0 | 49 en alcance: 44 estado → «En curso», 5 → «No aplica», 49 sin rutinas. Aparte: 5556 (clave inválida, archivar), 5119 (P-A28 obsoleto sin archivo), 3560 (P-I01) |
| Formato (F) | 193 | 0 | 120 | 48 «migrado» y 2 «en curso» sin liga real; 6 «No aplica» con clase A; 3 clase D en «pendiente»; 2 clase D en «migrado»; 20 de familias sin procedimiento; 3359 obsoleto en «pendiente» |
| Formato de instructivo (F-IT) | 65 | 0 | 54 | 2 sin liga real; 2 clase D en «pendiente»; 7 de familias sin procedimiento |
| Instructivo (IT) | 48 | 0 | 0 | 48 sin clase; 4 de la familia P-P07 |
| DAT | 42 | 0 | 0 | 42 sin clase; 1 «migrado» sin destino; 2 de la familia P-C10 |
| Anexo | 15 | 0 | 0 | sin clase y «migrado» sin destino |
| Protocolo / Reglamento | 5 / 5 | 0 | 0 | sin clase; 5 + 4 «migrado» sin destino; R-P-A23-02 de familia sin procedimiento |
| Manual / Diagrama | 1 / 1 | 0 | 0 | sin clase; el diagrama es la plantilla DF-X-XXX-XX (C-008) |
| **Documentos del Dropbox (427)** | **427** | **0** | **174** | — |
| Formulario de Odoo (también del Dropbox, fuera de los 427) | 64 | 0 | 61 | 3745 con clase «x»; 2 sin menú |

Consultas: `search_records documents.document [sgi_is_controlled, sgi_doc_type_id not in (2,16)]` (439, paginado por id), la misma para procedimientos (52), `aggregate … groupby sgi_doc_type` con `sgi_odoo_menu_id:count`, `sgi_migration_point_id:count`, `sgi_parent_document_id:count` y `sgi_previous_code:count` (0 en todos), `[sgi_migration_target = False]` (118), `[menú = False, worksheet = False, estado in (migrado, en_curso)]` (89) y `[sgi_parent_document_id = False]` (107). El script que arma el CSV desde esas lecturas **no está en el repo**: vive en el scratchpad de la sesión porque trae las listas de ids transcritas. Se reproduce con las consultas citadas.

### 3.9 Formato de importación

**Entrada.** El libro de Jose tal cual (XLSX) o su hoja «Rutina por rutina» en CSV UTF-8. Encabezado en `plantilla_rutina_por_rutina.csv`:

| Columna | Obligatoria | Regla |
|---|---|---|
| `clave` | sí | `P-Xnn`; se resuelve al único procedimiento controlado, **activo y vigente o en piloto** con `sgi_previous_code = clave` (antes de C-004, por `sgi_code`). 0 o más de 1 = error. Excluidos por parámetro = error, sin repetir el contenido de la fila |
| `procedimiento` | no | solo se compara con el título del documento (aviso si no se parece) |
| `n` | sí | entero > 0 (acepta `1.0`) |
| `rutina` | sí | texto |
| `frecuencia`, `responsable_anterior` | no | texto |
| `estado` | sí | `cubierta`, `reemplazada`, `eliminada`, `pendiente` (sin importar mayúsculas ni acentos; alias `odoo` → reemplazada) |
| `actividades_odoo` | según estado | numerales `C2.06` separados por `;` o `,`, cada uno activo |
| `motivo` | según estado | obligatorio si no es «cubierta» |
| `Revisión`, `Comentario` | no | `OK` / `Corregir` y texto → `review_state`, `review_note` |

Hojas opcionales del mismo libro:
- **«Pendientes»**: por `(clave, rutina)`, llena `decision` y `decision_owner_id` (usuario por nombre o login; si no existe, aviso).
- **«Documentos por migrar»**: **solo se compara** contra Odoo y reporta diferencias; no escribe. Odoo manda sobre documentos, y la hoja es un corte del 28-sep. Hoy: 158 filas, 0 diferencias.
- **«Resumen por procedimiento»**: se compara con lo cargado (conteos y proceso nuevo).

**API** (igual que `load_payload`, para MCP y para el asistente):
```python
env['sgi.legacy.routine'].load_routines({
    "dry_run": True,                 # por defecto True
    "archive_missing": False,        # True: archiva las rutinas del procedimiento que ya no vienen
    "routines": [{"clave": "P-A02", "n": 11, "rutina": "…", "frecuencia": "Continuo",
                  "responsable_anterior": "Compras", "estado": "reemplazada",
                  "actividades": ["S1.05", "S1.09"], "motivo": "…",
                  "revision": "", "comentario": ""}],
    "pending": [{"clave": "P-A01", "rutina": "…", "decision": "actividad", "responsable": "login"}],
})
# → {"ok": bool, "dry_run": bool,
#    "summary": {"created": n, "updated": n, "archived": n, "unchanged": n},
#    "changes": [{"clave", "n", "action", "fields"}],
#    "errors": [{"fila", "clave", "n", "regla", "message"}], "warnings": [...]}
```
- **Idempotente:** llave `(procedure_id, n)`; solo escribe lo que cambia (el mismo `_diff` de `sgi_load.py`). La segunda corrida da `created = updated = 0`.
- **Transacción por procedimiento:** si una fila de P-C05 falla, no se carga nada de P-C05 y el resto sí (así los conteos siempre cuadran con el Resumen).
- **Modo de prueba:** savepoint que se deshace, con `tracking_disable` (el mismo patrón de `load_payload`, `sgi_load.py:215-259`).
- **Permiso:** solo `group_sgi_manager` (o `su`).
- **No escribe** en documentos, procesos ni actividades. Solo rutinas.
- **Asistente «Importar rutinas»** (`sgi.legacy.routine.import`, copia del patrón de `sgi.catalog.load.wizard`): sube el archivo (openpyxl ya se usa en `sgi_sales_budget_import.py:72`), **«Probar»** muestra renglones con acción y error por fila, **«Cargar»** solo se habilita si la prueba dio 0 errores. El archivo no se guarda después de cargar (`attachment=False` y vacío del transitorio).
- **Antes de importar**, correr `validar_rutinas.py` (mismas reglas, fuera de Odoo). Contra el libro de hoy: **OK**.

**Aceptación de la carga:** 815 rutinas activas, 709 / 68 / 38; los contadores de los 49 procedimientos iguales a la hoja «Resumen»; 263 actividades con al menos una rutina; la segunda corrida con 0 cambios; P-I01 con 0 rutinas.

### 3.10 Migraciones previas (orden)

1. **C-006**: ligar `sgi.format.map` y las 8 claves fijas en código al documento (decisiones §4: va primero).
2. **`19.0.56.25.0/pre-migrate.py`** (C-001 corregido por L-003): respaldo de `sgi_process_replaced_doc_rel` y registro de conflictos. **Sin** copiar de la M2M a la M2O.
3. **`19.0.56.25.0/post-migrate.py`**:
   - (a) C-004 con L-004, clave anterior en 490 documentos:
     ```sql
     UPDATE documents_document d SET sgi_previous_code = d.sgi_code
       FROM sgi_document_type t
      WHERE t.id = d.sgi_doc_type_id AND t.code NOT IN ('mi_procedimiento', 'externo')
        AND d.sgi_is_controlled AND d.sgi_previous_code IS NULL AND d.sgi_code IS NOT NULL
        AND d.id <> 5556;
     ```
   - (b) C-003 con L-005, estados de procedimientos, solo si hoy dicen «migrado» (no pisa lo que MAST cambie a mano):
     ```sql
     WITH p AS (SELECT d.id, d.sgi_previous_code AS k FROM documents_document d
                  JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
                 WHERE t.code = 'procedimiento' AND d.sgi_is_controlled AND d.active
                   AND d.sgi_state IN ('vigente', 'piloto') AND d.sgi_migration_state = 'migrado')
     UPDATE documents_document d
        SET sgi_migration_state = CASE WHEN p.k IN ('P-A17','P-A18','P-A19','P-A20','P-S03')
                                       THEN 'na' ELSE 'en_curso' END
       FROM p WHERE d.id = p.id AND p.k <> 'P-I01';
     ```
     Esperado: 49 filas (44 `en_curso` y 5 `na`) y 0 la segunda vez. P-I01 según la pregunta 1. Las claves van en la migración porque son **decisión fechada** de Jose (decisiones.md), no estructura del módulo.
4. **`19.0.57.0.0`** (modelos nuevos, con cambio de versión obligatorio por el CI): `sgi.legacy.routine`, las vistas SQL, el asistente, las ACL, la regla, las vistas, los menús y la guarda de L-010. Sin datos con XML ID (decisión 4).
5. **Carga en producción** (Jose o MAST): validador → «Probar» → «Cargar» → aceptación de §3.9.

### 3.11 Pruebas (`tests/test_legacy_routine.py` y `tests/test_dropbox_key.py`, registradas en `tests/__init__.py`)

- **P-L1:** las restricciones: cubierta sin actividad, reemplazada sin motivo, pendiente con actividad y `(procedimiento, n)` duplicado son `ValidationError`/`IntegrityError`.
- **P-L2:** con un procedimiento sustituido no se puede crear una rutina pendiente, y al revés.
- **P-L3:** `load_routines` en modo de prueba no escribe. La carga real crea; la segunda corrida da 0 cambios; `archive_missing` archiva y no borra.
- **P-L4:** la transacción es por procedimiento: una fila mala deja sin cargar solo su procedimiento.
- **P-L5:** una clave excluida es error y el reporte no contiene el texto de la fila.
- **P-L6:** el buscador no muestra un documento de una carpeta sin acceso (regla `any`). Clave vieja, rutina y numeral archivado se encuentran, y «Abrir en Odoo» devuelve la acción del menú.
- **P-L7:** un Usuario SGI no puede escribir `sgi_migration_state` ni `sgi_replaced_by_process_id` (`AccessError`), y el cron con contexto sí.
- **P-L8:** proceso en vigor → documento obsoleto y `baja` (amplía `test_cleanup_45.py`).
- **P-L9:** la migración de §3.10 (b) sobre una base de prueba deja «En curso» y «No aplica» y es idempotente.

## 4. Preguntas que requieren decisión de negocio (Jose)

| # | Pregunta | Recomendación |
|---|---|---|
| 1 | **P-I01:** ¿se cierra hoy su acceso y se rotan las credenciales? ¿Qué estado de migración lleva mientras se retira? ¿Su familia (IT-P-I01, DAT P-I01, F-P-I01) también tiene credenciales? | Sí: `access_internal = none` hoy, rotar, y revisar tú la familia. Estado: se deja fuera de la migración de §3.10 y se archiva cuando esté su sustituto. Archivado, no aparece en la sección |
| 2 | **34 documentos de familias sin procedimiento** (P-A05, P-A13, P-A23, P-A30, P-C10, P-P07): ¿esos procedimientos existían en el Dropbox y no se cargaron, o las claves son de familias que ya no existen? | Agruparlos por la familia de su clave (`sgi_legacy_family`) sin inventar procedimientos. Si existe el PDF, cargarlo como procedimiento **obsoleto** para que tengan padre, sin rutinas (no cambia los 49) |
| 3 | **Clase de migración para IT, DAT, anexos, protocolos y reglamentos** (118 sin clase): ¿qué significa que un instructivo esté «migrado»? | Instructivo o DAT que se publica en Knowledge o sigue como PDF → clase D «Sigue como documento» y «No aplica». Solo A, B o C si Odoo lo sustituye. Los 25 «migrado» sin destino vuelven a «pendiente» hasta que MAST los clasifique |
| 4 | **Las 38 pendientes:** ¿quién decide cada una y para cuándo? | El dueño del proceso nuevo, con fecha compromiso a 30 días, revisado contigo. La decisión se registra en la rutina (L-015) y el tablero muestra «pendientes sin decisión» |
| 5 | ¿Se mantiene el estado **«eliminada»** aunque hoy no lo use ninguna rutina? | Sí: es la salida que la hoja «Pendientes» ya ofrece («eliminar con motivo») |
| 6 | ¿La revisión de Areli y los dueños («Revisión» y «Comentario») se captura en Odoo o sigue en la hoja? | En Odoo (`review_state` y `review_note`): así queda con la rutina como evidencia de 6.3 y la hoja deja de ser fuente |
| 7 | **5119** (P-A28 obsoleto, «Ventas», sin archivo, sin acceso para internos): ¿es historia de una revisión anterior o un registro vacío? | Si no tiene archivo en ninguna versión, archivarlo (nunca borrarlo), igual que 5556. La llave de P-A28 queda en 3538 |
| 8 | **Contradicciones de clase contra estado** (6 «No aplica» con clase A, 5 clase D en «pendiente», 2 clase D en «migrado»): ¿las corrige MAST antes de la carga? | Sí, antes de encender la restricción de L-012. Son 13 documentos y la lista está en `faltantes_transicion.csv` |

## 5. Cobertura

| Qué | Revisados | Total | Cómo |
|---|---:|---:|---|
| Piezas de transición de B-020 | 14 | 14 | código (`sgi_document.py`, `sgi_process.py`, `sgi_process_procedure.py`, `sgi_catalog.py`, `sgi_format_map.py`, `sgi_cleanup.py`, `sgi_load.py`, `sgi_load_wizard.py`) y vistas (`sgi_document_views.xml:42-145, 385-492`, `sgi_menus.xml:127`) |
| Procedimientos en producción | 52 | 52 | `search_records`, campo por campo (estado, migración, sustitución, proceso, PDF, acceso) |
| Documentos del Dropbox no procedimiento (+ formularios de Odoo) | 439 | 439 | `search_records` paginado por id + 6 consultas de conjunto (destino, liga, padre, clave anterior) |
| Actividades activas | 310 | 310 | numerales completos (`actividades_activas_2026-09-29.csv`); 111 con formatos citados; 144 archivadas con `legacy_number` (conteo y muestra) |
| Procesos activos (M2M de sustitución) | 14 | 14 | `search_records sgi.process` |
| `sgi.format.map` | 9 | 9 | `search_records` |
| Libro de Jose | 5 hojas, 815 + 50 + 38 + 158 filas | todas | `validar_rutinas.py` (reglas del encargo + Resumen, Pendientes y Documentos), y la prueba del validador con 5 errores sembrados (P-I01, duplicado, numeral inexistente, cubierta sin actividad, texto con forma de credencial): los detectó todos y no imprimió el texto de la fila excluida |
| Menú 2419 / acción 3870 | 1 / 1 | 1 / 1 | código; la acción no se puede leer por MCP (E) |

**Límites:** no hay staging. Las vistas SQL, la regla `any` y las migraciones quedan **pendientes de verificación en el build**. El contenido de P-I01 y de su familia **no se abrió** (a propósito). La guarda de escritura de L-010 hay que probarla contra los flujos de Documentos que escriben esos campos: aplicar cambios documentales y la resolución de menú. Los hallazgos de C, D, E y H sobre lo mismo se citan y no se repiten: C-001, C-003, C-004, C-005, C-006, C-008, C-009, C-011, C-012, C-016, D-002, D-015, D-017, E-001, E-004, E-005, F-013, F-016 y B-020.
