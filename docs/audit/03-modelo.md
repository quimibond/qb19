# 03 — Modelo de datos (agente C)

**Fecha:** 2026-09-29 · **Módulo:** `quimibond_sgi` 19.0.56.24.1 · **Modo:** solo lectura (código + producción por MCP con `search_records`/`aggregate_records`; ningún write).
**Apoyo:** `docs/audit/03-modelo/campos_sin_help.csv` (1,284 campos con prioridad) y `docs/audit/03-modelo/campos_sin_help_por_modelo.csv`.

## 1. Resumen

- **Procedimiento ↔ proceso (prioridad 3):** hoy hay dos verdades y ya divergen *en datos*: la M2M del proceso liga 26 documentos, la M2O del documento 23; solo 13 coinciden, 10 están solo en la M2O y 13 solo en la M2M. Los 36 siguen **vigentes**. La M2O se llenó el 2026-09-29 a las 01:11 UTC (Jose, uid 7), después del inventario, que decía «0». Propuesta: la **M2O del documento manda**; la del proceso pasa a One2many inversa. Hay migración idempotente y pruebas.
- **Clave anterior (prioridad 4):** `sgi_previous_code` está vacío en los 427 documentos. La clave del Dropbox vive en `sgi_code`: 426 de 427 cumplen `SGI_CODE_REGEX` y 414 nombres empiezan con ella. Propuesta: copiar `sgi_code`→`sgi_previous_code` sin fecha (marca «Dropbox», búsqueda permanente), no volver a sobrescribirla y generar la clave nueva con `sgi.document.type.sgi_next_code(proceso)`, documento por documento. Los pies de formato y 8 claves fijas en código buscan por texto de clave; hay que ligarlos al documento antes de renombrar.
- La nomenclatura está definida dos veces (`SGI_CODE_REGEX` en Python y `sgi.document.type` en datos), y la configuración de producción ya no es la del XML (`noupdate`). Por eso entró una clave inválida (doc 5556).
- `sgi_migration_state` contradice el estado real: 50 procedimientos «migrado» siguen vigentes y sin destino, y 75 documentos «migrado» no tienen ningún destino.
- Multiempresa: 50 de 74 modelos propios no tienen `company_id`, hay 0 `check_company` y varias claves son únicas globales. En los hechos es un módulo de una sola empresa, sin declararlo.
- 205 Many2one almacenados sin `ondelete` explícito. Lo que importa: las 13 referencias a `sgi.process` y las 5 a `sgi.area` hoy quedan en *set null*; deben ser *restrict*.
- 1,284 campos sin `help`: 977 aparecen en vistas de su modelo y 608 son de prioridad alta (selección, relación, número o fecha visibles).
- Computes sin `store` (303 contando related): no se confirmó ninguno usado en dominio, `group_by` u `_order` sin `search=`. Los `related` que se usan en dominios son buscables.

## 2. Hallazgos

| ID | Elemento | Hallazgo | Evidencia | Severidad | Acción | Propuesta concreta | Esfuerzo (h) | Depende de |
|---|---|---|---|---|---|---|---|---|
| C-001 | `documents.document.sgi_replaced_by_process_id` (`models/sgi_document.py:111`) ↔ `sgi.process.replaced_document_ids` (`models/sgi_process.py:52`) | Dos representaciones de «qué procedimiento sustituye cada proceso», sin sincronía. Divergen hoy: M2M 26 documentos en 12 procesos (S2 y S6 sin nada); M2O 23 en 11 procesos; 13 en ambas, 10 solo en la M2O (3581, 3579, 3578, 3577, 3576, 3572, 3556, 3554, 3553, 3542), 13 solo en la M2M (3538, 3573, 3560, 3558, 3557, 3555, 3548, 3546, 3545, 3532, 3505, 3536, 3499). No hay conflictos: donde están las dos, apuntan al mismo proceso. La M2O se llenó por fuera del flujo (`readonly=True` solo en la vista), el 2026-09-29 01:11 UTC, uid 7. El inventario cuenta 28 en la M2M; por MCP se leen 26 (la lectura de una M2M omite archivados: pendiente confirmar si hay 2 documentos archivados en la relación) | `search_records sgi.process [company_id=1]` campo `replaced_document_ids`; `search_records documents.document [sgi_replaced_by_process_id != False]` (23, todos vigentes, `write_date` 2026-09-29 01:11) | Alta | Corregir | Fuente única = M2O del documento. `replaced_document_ids` pasa a `One2many('documents.document','sgi_replaced_by_process_id')`. Detalle en §2.1 | 10 | Decisión Jose P-1 |
| C-002 | `sgi_cleanup.py:153-171` (`_sgi_obsolete_replaced_documents`) | El obsoletado al poner el proceso en vigor lee la M2M y escribe la M2O: la M2O solo quedaba llena si el documento ya era obsoleto. Ahora Jose la usa para decir «lo sustituirá» con el documento todavía vigente. Son dos significados para un mismo campo | código; los 23 con M2O están `vigente` y `sgi_obsolete_reason` vacío | Media | Corregir | Un solo significado: «proceso que lo sustituye». El documento sigue **vigente** mientras el proceso esté en borrador o piloto (no hay dos versiones en vigor). Al pasar el proceso a `vigente` se obsoletan los documentos con la M2O igual al proceso y queda `sgi_obsolete_date`/`reason`. En la ficha, un aviso «Lo sustituirá C2 (piloto)» | 3 | C-001 |
| C-003 | `documents.document.sgi_migration_state` en procedimientos | Estado incoherente: 50 procedimientos en «migrado», sin destino y vigentes; C2 es el único proceso en piloto y ninguno está vigente | `aggregate documents.document` [controlado, migrado, sin target/menú/punto] por tipo: Procedimiento 50, Anexo 15, Protocolo 5, Reglamento 4, DAT 1 = 75 | Alta | Corregir | En tipo procedimiento, `sgi_migration_state` sale de la sustitución: sin M2O → `pendiente`; M2O con proceso en borrador o piloto → `en_curso`; proceso vigente y documento obsoleto → `migrado`. Para los demás tipos, restricción Python: «migrado exige destino (menú, worksheet o texto) o clase d», **solo para escrituras nuevas**. Los 25 no procedimiento se corrigen como dato en «Del Dropbox a Odoo» | 4 | C-001 |
| C-004 | `documents.document.sgi_previous_code` (`sgi_document.py:36`) | Vacío en los 427 documentos del Dropbox; la clave vieja vive en `sgi_code`. Va contra la decisión 1 (la clave del Dropbox vive solo en `sgi_previous_code`) | `search_records` ×5 (427 filas, `sgi_previous_code`=False en todas las leídas); inventario 00 | Alta | Corregir | Migración y clave nueva: §2.2 | 6 | P-2, C-006 |
| C-005 | `sgi_document.py:649-659` (`write`), `:512-528` (`_sgi_find_by_code`), `views/sgi_document_views.xml:258` | (a) Cada cambio de `sgi_code` **sobrescribe** `sgi_previous_code`: un segundo renombre borra la clave del Dropbox. (b) La clave anterior solo se encuentra 12 meses después de `sgi_previous_code_date`, pero la decisión 7 pide un buscador permanente por clave vieja | código | Alta | Corregir | (a) Si `sgi_previous_code` ya existe y `sgi_previous_code_date` es nula (marca Dropbox), `write` no la toca; los renombres posteriores quedan en el tracking de `sgi_code`. (b) `_sgi_find_by_code` y el `filter_domain` aceptan `sgi_previous_code_date = False OR >= hoy-12m`. El buscador de «Del Dropbox a Odoo» busca `sgi_previous_code` sin ventana. Pruebas: `test_catalog_fase1.py:279` más una nueva de doble renombre | 3 | C-004 |
| C-006 | `sgi.format.map.sgi_code/sgi_code_alt` (`sgi_format_map.py:28-33`, 9 registros en `data/sgi_format_map_data.xml`); claves fijas en `sgi_staff_efficiency.py:168`, `sgi_sales_budget.py:843`, `sgi_epp_sign.py:125`, `sgi_machine_sheet.py:106`, `sgi_dev_request.py:19-20`, `report/report_procedure.xml:75,79`, `report/report_doc_change.xml:34`, `report/report_calibration_label.xml:57,73` | El pie «Formato controlado» de pedidos, OC, remisiones, NC y demás, y varios reportes, buscan el documento **por el texto de la clave del Dropbox** (`_revision_of(code)`, `_sgi_document_by_code`) y la imprimen. Si se renombra `sgi_code`, el pie pierde la revisión; si no, sigue imprimiendo la clave vieja (decisión 1) | `grep` de claves `F-P-…` en `models/`, `report/`, `data/` | Alta | Corregir | Agregar `document_id` (M2O a `documents.document`, `ondelete='restrict'`) en `sgi.format.map` y dejar `sgi_code` como `related='document_id.sgi_code'`. Migración que liga los 9 mapas por clave. Las claves fijas en código pasan a xml_id o parámetro que apunta al documento (o a `_sgi_find_by_code`, que ya cubre la clave anterior). Hacerlo **antes** de renombrar claves | 8 | C-004 |
| C-007 | `SGI_CODE_REGEX` (`sgi_document.py:14-25`) contra `sgi.document.type.prefix_pattern/legacy_code_regex` (`data/sgi_document_types.xml`, `noupdate`) | La nomenclatura está definida dos veces. `SGI_CODE_REGEX` solo la usan `tools/carga_documental.py:84` y `sgi_format_map.py:47`. Además, producción ya no es el XML: procedimiento con patrón `P-{process}` (el XML dice `PR-{process}`), `code_required=False` en procedimiento y formato, `requires_process=False` en ambos. Con eso `_check_sgi_code` no revisa esos tipos, y así entró el doc 5556 con clave «P-A14 MEDIO AMBIENTE Y SEGURIDAD EN EL TRABAJO» | `search_records sgi.document.type` (16 tipos); doc 5556 | Media | Decidir | Una sola fuente: los tipos. `SGI_CODE_REGEX` se sustituye por `sgi.document.type._sgi_any_match()` en format map y queda documentado como regex solo de la herramienta de carga. Pedir a Jose el patrón definitivo (P-2) y volver a encender `code_required`/`requires_process` en procedimiento y formato: los 427 tienen proceso | 3 | P-2 |
| C-008 | Datos de clave en `documents.document` | Anomalías: 5556 (borrador, clave inválida, duplica a 3518 P-A14); 3359 (obsoleto) y 5152 (vigente) con la misma clave F-P-V01-04 y revisión 0, ambos activos, contra `_check_sgi_revision_unique`; 4022 clave `F-P-P01-00` truncada (archivo «F-P-P01-002»); 4995 `DF-X-XXX-XX` (plantilla) vigente; 3760 `R-P-A23-02` «Reporte de consumo de Gas LP» tipificado como reglamento; 3644 «F-P-P01-10 INSTRUCTIVO RAMA NUEVO» como formato; 3930 con dos claves en el nombre (F-P-C06-08 y -09); DAT con separador mixto (37 «DAT P-…», 5 «DAT-P-…»); 11 nombres «F-P-A-16-nn» con clave «F-P-A16-nn» | `search_records` de los 427 y `get` de los ids citados | Media | Corregir | Lista de limpieza de datos para Jose, en «Del Dropbox a Odoo» y **antes** de la migración de C-004: clave, tipo o baja. No se borra nada; lo que sobre se archiva | 2 | — |
| C-009 | `documents.document.name` | Títulos mezclados: 414 de 427 nombres empiezan con la clave del Dropbox («P-D01 DISEÑO Y DESARROLLO.pdf»); los 13 que no: 11 «F-P-A-16», 5119 «Ventas», 4751 «15. DAT-P-G01-07…». Pantallas y reportes muestran `sgi_code — name` (`report_sign_sheet.xml:42,93`, `report_doc_change.xml:56`) y repiten la clave dos veces | `search_records` | Media | Corregir | No renombrar archivos en la migración: el nombre es el archivo, que es evidencia. Agregar `sgi_title` (compute store, buscable): el nombre sin extensión y sin la clave inicial (`sgi_code` o `sgi_previous_code`, tolerante a «F-P-A-16»). Vistas y reportes del SGI usan `sgi_title` | 4 | C-004 |
| C-010 | `documents.document.sgi_doc_type` (Selection compute/inverse, `sgi_document.py:47-67`) contra `sgi_doc_type_id` (`:44`) | El mismo dato dos veces. La selección tiene 38 usos en Python y 21 en XML; la M2O, 15 y 6. Un tipo nuevo creado en Configuración deja la selección vacía y los dominios `('sgi_doc_type','=',…)` no lo ven | `grep -w` | Media | Corregir | Migrar los 59 usos a `sgi_doc_type_id.code` (o `_sgi_type_is('procedimiento')`). Después, la selección queda como `related` de solo lectura por una versión y se retira con respaldo | 8 | — |
| C-011 | `sgi_migration_target` (Char), `sgi_odoo_menu_id`, `sgi_migration_point_id` (`sgi_document.py:69-74,160-172`) | El destino de migración se guarda de tres formas (texto, menú, worksheet), sin prioridad clara. Los 258 formatos tienen alguno. Según la decisión 7 no es obsoleto: se reubica | `aggregate` [target \| menú \| punto] = F 193 + F-IT 65 | Baja | Documentar | Compute `sgi_destination_label` (menú > worksheet > texto) para «Del Dropbox a Odoo». El texto queda para destinos sin menú. `action_sgi_resolve_odoo_menu` sigue convirtiendo texto en menú | 2 | — |
| C-012 | Multiempresa (74 `Model` propios) | Solo 24 tienen `company_id` (17 por `related` del padre). Sin él: `sgi.indicator`, `sgi.risk`, `sgi.audit`, `sgi.audit.program` (`unique(year)`), `sgi.action.line`, `sgi.legal.requirement`, `sgi.objective`, `sgi.policy`, `sgi.management.review`, `sgi.norm` (`unique(code)`), `sgi.area` (`unique(code)`), `sgi.format.map` y 38 más. `check_company`/`_check_company_auto`: 0 usos. `sgi.indicator` es `unique(code)` global. El usuario tiene 8 empresas permitidas | `modelos.csv`/`campos.csv`; `grep check_company` = 0 | Media | Decidir | Recomendado: declararlo **de una sola empresa** (README + restricción que impide crear procesos fuera de la empresa 1) y no agregar `company_id` a 50 modelos. Si algún día hay otra empresa: `company_id` en los 12 modelos raíz, `related` en los hijos, `_check_company_auto` y claves únicas `(code, company_id)` | 2 (declarar) / 24 (multiempresa) | P-3 |
| C-013 | Many2one sin `ondelete` (205 almacenados, no related) | El default de Odoo es *set null* (*restrict* si es requerido). Borrar un proceso deja sin proceso, sin aviso, a documentos (`sgi_document.py:77`), indicadores (`sgi_indicator.py:71`), riesgos (`sgi_risk.py:66`), incidentes, AMEF, hallazgos, líneas de programa, cambios de actividad y pendientes (13 campos a `sgi.process`), y lo mismo con `sgi.area` (5). También las cadenas de evidencia política→objetivo (`sgi_objective.py:27`) y objetivo→indicador (`sgi_indicator.py:80`) | script sobre `campos.csv` (lista completa en la sección 5) | Media | Corregir | `ondelete='restrict'` en las M2O a `sgi.process`, `sgi.area`, `sgi.policy`, `sgi.objective`, `sgi.norm.clause` y en las de evidencia a `documents.document` (`sgi.audit.report_document_id`, `sgi.policy.document_id`, `sgi.emergency.plan.document_id`, `sgi.control.plan.document_id`). Los procesos se archivan, no se borran. Las referencias a `res.users`, `quality.alert` y `hr.*` se quedan en *set null*. Las 23 líneas hijas ya están en *cascade* (verificado vía One2many inversos) | 3 | — |
| C-014 | `sgi_replaced_by_process_id` (propuesta de C-001) | Si la M2O quedara en `ondelete='cascade'`, el `Command.set` del cargador (`sgi_load.py:458,469`) sobre la nueva One2many **borraría** los documentos que salen de la lista: en una One2many, las filas que se quitan se borran si el inverso es *cascade* | semántica de One2many en Odoo | Alta (preventivo) | Documentar | La M2O va con `ondelete='restrict'` y nunca *cascade*, con una prueba que lo fija (P-T3 en §2.1) | 0.5 | C-001 |
| C-015 | `sgi_document.py:111` | `sgi_replaced_by_process_id` sin restricción: acepta cualquier documento (formato, anexo), procesos archivados y documentos no controlados | código | Media | Agregar | `@api.constrains`: solo documentos controlados; tipo `procedimiento` (o `instructivo`, si Jose lo pide); proceso activo de la misma empresa. `index=True` ya está | 1 | C-001 |
| C-016 | `sgi.process.activity.legacy_number` (`sgi_process_procedure.py:507`) | Sin datos (0/310) y con `readonly=True`: el cargador solo lo llena cuando el numeral no se puede traducir a paso (`:809-810`). Para «rutina por rutina» (decisión 7) hace falta capturarlo y que diga de qué procedimiento viene | inventario 00; código | Baja | Documentar | Se queda (decisión 7). Documentar el formato esperado («P-A28 5.3») y hacerlo editable para `group_sgi_manager`. Si hace falta ligar la rutina a su procedimiento anterior, que sea una M2O `legacy_document_id` en lugar de texto (a decidir en fase 2) | 1 | — |
| C-017 | `documents.document.sgi_revision_legacy` (`sgi_document.py:88`) | 0 documentos con dato en producción; 1 uso en Python y 1 en XML | `aggregate [sgi_revision_legacy != False]` = 0 | Baja | Decidir | Candidato a archivar (lo evalúa el agente B). No se toca en esta fase | 0.5 | B |
| C-018 | Campos sin `help` (1,284; 977 visibles en vistas de su modelo) | 608 de prioridad alta: selección, M2O, M2M, booleanos, números o fechas visibles. Modelos con más de alta: `quality.alert` 43, `sgi.risk` 23, `sgi.indicator` 21, `sgi.indicator.measure` 21, `sgi.process.activity` 20, `approval.request` 19, `documents.document` 19, `sgi.activity.role` 17, `sgi.my.procedure` 17, `res.config.settings` 16, `sgi.activity.change` 16, `sgi.audit` 15 | `03-modelo/campos_sin_help.csv` | Media | Agregar | Escribir `help` a los 608 de prioridad alta en 3 tandas (Mejora; Procesos y Mi procedimiento; Dirección e indicadores). Lo técnico (`*_count`, `company_id`, `active`) no lo necesita | 20 | — |
| C-019 | Campos sin `string` explícito (78, casi todos técnicos) | Visibles para el usuario sin etiqueta propia: `sgi.indicator.measure.state`, `sgi.machine.sheet.state`, `sgi.indicator.direction`, `sgi.indicator.calc_mode`, `sgi.catalog.load.wizard.state`, `sgi.management.review.agreement.is_done/status_label` (Odoo muestra «State», «Direction»…) | script sobre `campos.csv` | Baja | Agregar | `string` en español en esos 7; al resto le basta la etiqueta derivada | 0.5 | — |
| C-020 | Nombres inconsistentes | Mismo concepto con nombres distintos: NC ligada = `alert_id` (8 modelos), `sgi_alert_id` (4, incluidos `sgi.calibration` y `sgi.incident`, que son propios), `nc_alert_id` (1). Área = `sgi_area_id` M2O (5 modelos) contra `area` Char en `sgi.csh.inspection` y `area` Selection en `sgi.machine.sheet`. Responsable = `responsible_id` / `owner_id` / `user_id`. Revisión = Char en `sgi.fmea`/`sgi.control.plan` e Integer en `sgi.machine.sheet`/`sgi.sales.budget`/documentos. 31 campos de modelos propios llevan prefijo `sgi_` innecesario | script sobre `campos.csv` | Baja | Documentar | No renombrar ahora (costo de migración, datos vivos). Regla en README para lo nuevo: `alert_id`, `area_id`→`sgi.area`, `responsible_id`, revisión `Integer`. `area` de `csh.inspection` y `machine.sheet` → M2O `sgi.area` cuando se toquen (fase 2) | 1 | — |
| C-021 | Índices | Las tablas del SGI son chicas (ack 149, mediciones 120, exec.stat 604, documentos 4,532, de ellos 588 controlados). No falta ningún índice que se note. Los campos más filtrados sin índice son `documents.document.sgi_state` (29 dominios en Python) y `sgi_is_controlled` (22) | `aggregate` de conteos; script de frecuencia de dominios | Baja | Agregar | `index=True` en `sgi_state` y `sgi_is_controlled` (tabla compartida con toda la app Documentos). Nada más por ahora | 0.5 | — |
| C-022 | `required` | Todos los indicadores y riesgos tienen proceso (0 sin proceso), pero `process_id` no es requerido en `sgi.indicator` ni en `sgi.risk`. `company_id` requerido en `sgi.health.record`, `sgi.csh.inspection`, `sgi.checklist.template`, `sgi.sales.budget` y `sgi.staff.efficiency` sin regla multiempresa (lo ve F) | `search_records sgi.indicator [process_id=False]` = 0; `aggregate sgi.risk [process_id=False]` = 0 | Baja | Corregir | `required=True` + `ondelete='restrict'` en `process_id` de indicador y riesgo | 1 | C-013 |
| C-023 | Computes sin `store` (274 según inventario; 303 contando related, 11 con `search=`) | El escaneo (dominios XML y Python, `group_by`, vistas de búsqueda, `_order`) no confirmó ningún compute sin `store` ni `search` usado donde Odoo lo exige. Los 15 cruces por nombre fueron homónimos de otro modelo o `related` (buscables). `_order`: todos los campos existen y están almacenados (`folio` viene de `sgi.base.mixin`). Los `related` no almacenados en dominios de pendientes (`quality.alert.sgi_stage_is_closing/is_cancel`, 4 archivos) cuestan una subconsulta | script (sección 5) | Baja | Documentar | Sin cambio obligatorio. Si «Mis pendientes» se vuelve lento: `store=True` en `sgi_stage_is_closing/is_cancel`. Limitación: el cruce es por nombre de campo, no por modelo del dominio | 0 | — |

### 2.1 Propuesta prioridad 3 — una sola fuente para «procedimiento sustituido por proceso»

**Qué manda:** `documents.document.sgi_replaced_by_process_id` (Many2one).
Por qué:
- una clave vive en un solo proceso (`_check_sgi_code_family`) y el religado ya asigna «una clave, un destino» (`sgi_cleanup.py:219-232` y siguientes);
- es la que Jose ya está capturando;
- se filtra, se indexa y se agrupa en «Del Dropbox a Odoo»;
- ningún dato actual pide que un procedimiento lo sustituyan dos procesos (0 conflictos).

**Cambios de código (fase 3):**
1. `sgi_process.py:52`: `replaced_document_ids = fields.One2many('documents.document', 'sgi_replaced_by_process_id', string="Procedimientos que sustituye")`. En la vista del proceso (`views/sgi_process_views.xml:333`) va una lista editable en lugar de `many2many_tags`, con `domain` a controlados de tipo procedimiento.
2. `sgi_document.py:111`: se quita `readonly=True` y se agregan `ondelete='restrict'` (C-014), `tracking=True` y `groups`/`readonly` por vista para que solo `group_sgi_manager` lo edite. Se agrega la restricción de C-015.
3. `sgi_cleanup.py:153-171`: sin cambio de lógica, porque `process.replaced_document_ids` sigue existiendo. El `write` a la M2O dentro del bucle sobra, pero no estorba.
4. `sgi_load.py:449-469`: en el alta y en la actualización, `vals['replaced_document_ids'] = [Command.set(ids)]` sigue sirviendo con la One2many. Antes de escribir, reportar como error del lote cualquier documento que ya tenga **otro** proceso en la M2O (no se reasigna en silencio). Al quitarlo de la lista, la M2O queda vacía; no se borra nada gracias a *restrict* (C-014).
5. Estado `vigente` (C-002): el procedimiento sigue vigente hasta que su proceso entre en vigor. Aviso en la ficha y columna «Lo sustituirá» en la lista maestra. `sgi_migration_state` de los procedimientos sale de ahí (C-003).

**Migración (`migrations/19.0.56.25.0/pre-migrate.py`, idempotente, sin tocar datos de negocio):**
```sql
-- 0. respaldo de la relación vieja (la tabla puede desaparecer al cambiar el tipo del campo)
CREATE TABLE IF NOT EXISTS sgi_process_replaced_doc_rel_bak_562500 AS
  SELECT * FROM sgi_process_replaced_doc_rel;
-- 1. conflictos: documento en 2+ procesos, o M2O distinta de la M2M → solo se registran en el log
SELECT r.document_id, array_agg(r.process_id), d.sgi_replaced_by_process_id
  FROM sgi_process_replaced_doc_rel r JOIN documents_document d ON d.id = r.document_id
 GROUP BY r.document_id, d.sgi_replaced_by_process_id
HAVING count(*) > 1 OR (d.sgi_replaced_by_process_id IS NOT NULL
                        AND d.sgi_replaced_by_process_id <> ALL(array_agg(r.process_id)));
-- 2. copia solo donde la M2O está vacía y hay un único proceso
UPDATE documents_document d SET sgi_replaced_by_process_id = r.process_id
  FROM (SELECT document_id, min(process_id) AS process_id
          FROM sgi_process_replaced_doc_rel GROUP BY document_id HAVING count(*) = 1) r
 WHERE d.id = r.document_id AND d.sgi_replaced_by_process_id IS NULL;
```
No cambia `sgi_state`, fechas ni motivos. Con los datos de hoy: los 13 de solo M2M se copian, los 10 de solo M2O se quedan y resultan **36 documentos ligados en 12 procesos**, con 0 conflictos. Correr antes con un `SELECT` de prueba (pendiente de verificación en el build).

**Pruebas (`tests/test_replaced_by_process.py`, registrarla en `tests/__init__.py`):**
- P-T1: escribir la M2O del documento hace que aparezca en `process.replaced_document_ids`, y al revés con `Command.link`.
- P-T2: proceso en piloto: el documento sigue `vigente`. Proceso → `vigente`: el documento pasa a `obsoleto` con `sgi_obsolete_reason` y `sgi_obsolete_date`. Correr dos veces no hace nada.
- P-T3: el cargador con `replaced_documents` sin uno de antes deja la M2O vacía y el documento **existe** (no se borra).
- P-T4: el cargador rechaza un documento que ya sustituye otro proceso.
- P-T5: la restricción rechaza formatos, no controlados y procesos archivados.
- P-T6: la migración con una relación duplicada no sobrescribe y lo registra.
- P-T7: `test_cleanup_45.py:82-125` sigue verde. Ajustar sus `(6,0,ids)`, que valen igual para la One2many.

La carga de los 52 procedimientos la hace Jose cuando Dirección confirme cuáles siguen vigentes. No es tarea de esta auditoría.

### 2.2 Propuesta prioridad 4 — clave nueva y clave anterior

**Medición en producción (2026-09-29):**

| Qué | Resultado |
|---|---|
| Documentos del Dropbox (controlados, sin MP ni formularios de Odoo) | 427 |
| `sgi_code` por tipo | MIID 1 · P 52 · IT 48 · F 193 · F-IT 65 · DAT 42 (37 «DAT P-», 5 «DAT-P-») · PROT 5 · DF 1 · R 5 · Anexo 15 |
| `sgi_code` que cumple `SGI_CODE_REGEX` | 426 de 427 (falla 5556) |
| Nombres que empiezan con la clave | 414 de 427 (13 excepciones en C-009) |
| `sgi_previous_code` con dato | 0 de 427 |
| Documentos con proceso | 427 de 427 (C4 91, E2 89, C5 79, S4 43, C1 35, C2 27, S5 17, S1 12, S3 11, S2 8, C6 7, C3 5, E1 3) |
| Configuración de tipos en producción | procedimiento `P-{process}`, IT `IT-{process}-{seq:02d}`, F `F-{process}-{seq:02d}`; sin patrón nuevo: F-IT, DAT, PROT, DF, R, Anexo, MIID |

**Qué campo es qué:**
- `sgi_code` es la **clave vigente**, la nueva, la única que se muestra en títulos, menús y pies de formato.
- `sgi_previous_code` es la **clave del Dropbox**, solo como dato: se busca en la ficha, en «Del Dropbox a Odoo» y en la lista maestra (`report_master_list_all.xml:13` ya tiene la columna).
- `sgi_previous_code_date` vacío significa «clave del Dropbox, sin caducidad». Con fecha significa un renombre hecho en Odoo, con la ventana de 12 meses (C-005).

**Migración (`migrations/19.0.56.25.0/post-migrate.py`, idempotente):**
```sql
UPDATE documents_document d
   SET sgi_previous_code = d.sgi_code          -- sin fecha = clave del Dropbox
 WHERE d.sgi_is_controlled IS TRUE
   AND d.sgi_previous_code IS NULL
   AND d.sgi_code IS NOT NULL
   AND d.sgi_doc_type_id NOT IN (SELECT id FROM sgi_document_type
                                  WHERE code IN ('mi_procedimiento','formulario_odoo','externo'))
   AND d.id <> 5556;                           -- clave inválida: se corrige antes (C-008)
```
- **No** toca `sgi_code` ni `name`. Por SQL no pasa por `write()`, así que no hay tracking, no se dispara `_obsolete_code` y no hay chatter masivo. El índice único de vigentes no se afecta.
- Resultado esperado: 426 filas la primera vez y 0 la segunda.

**Cómo se genera la clave nueva** (fase 3; acción de servidor o asistente «Asignar clave nueva», solo para `group_sgi_manager`, documento por documento o por lote de un proceso):
- `sgi_code = sgi_doc_type_id.sgi_next_code(sgi_process_id)`. Ya existe (`sgi_catalog.py:686-713`), calcula el consecutivo mayor + 1 por proceso y ya ve el archivado.
- Solo se asigna a documentos que **siguen** en el sistema nuevo: IT, DAT, anexos, formatos clase c/d, y los clase a/b todavía no migrados.
- Los sustituidos (procedimientos con `sgi_replaced_by_process_id`, formatos migrados a Odoo) se obsoletan con su clave vieja y no reciben clave nueva. Como obsoletos no salen en menús ni en títulos.
- Antes hay que definir el patrón de F-IT, DAT, PROT, DF, R, Anexo y MIID (P-2). El de procedimiento (`P-{process}` contra `PR-{process}`) choca visualmente con la clave del Dropbox: «P-C2» contra «P-C02».
- El `write()` de hoy guardaría la clave anterior con fecha. Con C-005 conserva la del Dropbox (fecha vacía) y no la sobrescribe.
- El `name` no se toca: el título limpio sale de `sgi_title` (C-009).

**Impacto en reportes y búsquedas:**
- Pies y reportes que buscan por texto de clave (C-006): ligarlos al documento **antes** de la primera clave nueva.
- `_onchange_sgi_code_parent` (`sgi_document.py:270-284`) deduce el padre con el regex del Dropbox. Con claves nuevas deja de sugerir (inocuo, porque la familia ya es una M2O).
- `_sgi_reparent_family` y `_obsolete_code` trabajan por `sgi_code` exacto. Un renombre cambia la clave de una sola revisión: renombrar **todas las revisiones de la clave juntas** (el asistente lo hace por clave, no por registro), igual que la regla «una clave, un destino» de `sgi_cleanup.py`.
- `tools/carga_documental.py:84` y `tools/post_carga_documental.py` (11 usos): quedan como herramientas históricas de la carga del Dropbox. Documentarlo en su cabecera.
- Búsqueda «Clave SGI» (`sgi_document_views.xml:257-258`) y `_sgi_find_by_code`: con C-005 encuentran la clave del Dropbox siempre.

**Pruebas:** la migración deja 426 y es idempotente; el doble renombre conserva la clave del Dropbox; `_sgi_find_by_code('P-A28')` encuentra el documento después del renombre y 13 meses después; el pie de `sale.order` imprime la clave nueva y la revisión viva después del renombre (C-006); el asistente renombra todas las revisiones de una clave o ninguna.

## 3. Preguntas que requieren decisión de negocio (Jose)

| # | Pregunta | Recomendación |
|---|---|---|
| P-1 | ¿Algún procedimiento del Dropbox lo sustituyen **dos** procesos (p. ej. P-C01 Reclamaciones entre C5 y C2)? | No. Un procedimiento → un proceso, que es lo que ya impone «una clave, un destino». Si parte del contenido va a otro proceso, se anota en la actividad (`legacy_number`) y no en la sustitución. Así la M2O basta (C-001) |
| P-2 | ¿Cuál es la nomenclatura nueva definitiva por tipo? Hoy procedimiento es `P-{process}` en producción y `PR-{process}` en el XML; F-IT, DAT, PROT, DF, R, Anexo y MIID no tienen patrón nuevo. ¿Se vuelven a encender `code_required` y `requires_process` en procedimiento y formato? | `PR-{proceso}` para no confundir con P-A28. IT, F y MA como ya están. F-IT → `F-{proceso}-{nn}` (formato de su proceso). DAT → `DA-{proceso}-{nn}`. Anexo, PROT y R según decida MAST. Encender los dos candados: los 427 tienen proceso |
| P-3 | ¿El SGI es de una sola empresa (PNTQ, id 1) para siempre? | Sí, declararlo así (C-012) y no agregar `company_id` a 50 modelos |
| P-4 | ¿Los formatos ya migrados a Odoo (clase a/b «migrado», 162) necesitan clave nueva o se obsoletan con su clave del Dropbox? | Se obsoletan como están, con su clave vieja en `sgi_previous_code` (y en `sgi_code` como historia). Solo reciben clave nueva los documentos que siguen como documento |
| P-5 | ¿Se renombran los archivos (`name`) para quitar la clave, o basta con el título limpio calculado? | Basta con el título calculado (`sgi_title`); el archivo se queda como vino (evidencia) |
| P-6 | Documentos con datos malos (C-008): ¿5556 se archiva (duplica a P-A14)? ¿Cuál de 3359/5152 queda? ¿4995 (plantilla DF-X-XXX-XX) sale de vigente? | Archivar 5556 y 3359; 4995 a borrador o archivo; corregir la clave de 4022 y el tipo de 3760 y 3644 |

## 4. Cobertura

| Tipo | Revisados | De | Cómo |
|---|---:|---:|---|
| Campos (`campos.csv`) | 1,661 | 1,661 | Todos por script: `help`, `string`, `ondelete`, `compute`/`store`/`search`, `company_id`, `required`, `index`, nombres y homónimos. Revisión semántica manual completa en `documents.document` (≈50 campos `sgi_*`), `sgi.process`, `sgi.process.activity` (numeral), `sgi.document.type`, `sgi.format.map` y `sgi.document.ack`. En los demás modelos los duplicados semánticos se buscaron por nombre y tipo, no leyendo cada modelo |
| Modelos propios | 102 | 102 | `company_id`, `_order`, restricciones SQL (36) y Python (54 `@api.constrains`) |
| Modelos heredados | 47 | 47 | Campos agregados sin prefijo `sgi_` (3: dos `selection_add` de vistas y `sale.order.commitment_date`, que solo agrega `tracking`; correcto) |
| Satélites (6 campos `quimibond_sgi_plm`) | 0 | 6 | Fuera del alcance de esta tanda |

**Límites dichos de frente:**
- El cruce «compute usado en dominio» es por nombre de campo, no por el modelo del dominio (C-023).
- La M2M `replaced_document_ids` se leyó con la regla de activos por defecto: 26 contra los 28 del inventario, pendiente de confirmar.
- Los datos de C-001 cambiaron durante la auditoría (escritura de Jose a las 01:11 UTC).
- Todo lo de la migración y la vista queda **pendiente de verificación en el build** (no hay staging).

## 5. Consultas de evidencia (reproducibles)

```text
MCP search_records sgi.process [["company_id","=",1]] fields code,state,replaced_document_ids
MCP search_records documents.document [["sgi_replaced_by_process_id","!=",false]] fields sgi_code,sgi_state,write_date,write_uid
MCP aggregate_records documents.document groupby [sgi_replaced_by_process_id, sgi_state] domain [sgi_is_controlled=true]
MCP search_records documents.document [["sgi_is_controlled","=",true],["sgi_doc_type_id","not in",[15,16]]] fields name,sgi_code  (offset 0..400, order id) → 427
MCP search_records sgi.document.type fields code,prefix_pattern,legacy_code_regex,requires_process,code_required
MCP aggregate_records documents.document groupby [sgi_migration_state, sgi_migration_class] (427)
MCP aggregate_records documents.document groupby [sgi_doc_type_id] domain [migrado, sin target/menú/punto] → 75
MCP aggregate_records documents.document groupby [sgi_process_id] (427, todos con proceso)
python3 sobre docs/audit/inventario/campos.csv y modelos.csv (M2O sin ondelete, computes, company_id, help → 03-modelo/*.csv)
```
