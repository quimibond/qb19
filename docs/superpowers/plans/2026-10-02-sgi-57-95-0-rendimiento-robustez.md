# Entrega 57.95.0 — Rendimiento y robustez (plan de implementación)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Renumeración:** es la ficha **«57.99.0 — Rendimiento y robustez»** del plan general (`docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`, K-08, K-05, D-06). Sale **ahora** como `19.0.57.95.0` porque no tiene puerta de decisión y va antes de las fichas que esperan a Dirección (SST y ambiente, Cláusulas, Interfaz, Salud del SGI). Esas fichas toman el siguiente número libre (57.96.0 en adelante) cuando se inicien; la Task 5.9 lo anota en el mapa del plan general.

**Goal:** que el SGI aguante el volumen que viene sin inundar a nadie ni ponerse lento: (1) **un aviso por persona o por jefe**, no uno por acuse pendiente (hoy 147 acuses; 116 avisos le llegarían al Jefe MAST a partir del 5 y 7 de octubre); (2) **los filtros de Mi equipo leen un resumen guardado** por empleado en lugar de calcular los pendientes de toda la empresa en cada búsqueda; (3) **clase del aviso indexada** (`mail.activity.sgi_cron_kind`) para el barrido de episodios; (4) **respaldo nocturno** que recalcula las cuatro listas guardadas de Mi procedimiento y anota en el log cuántas cambiaron (≠ 0 = falta un disparo); (5) **K-05**: las entregas facturadas y la fecha de pago siguen a sus datos, y lo ajustado a mano ya no se pisa; (6) **D-06**: los controlados nuevos nacen con la empresa del SGI, regla de empresa en `sgi.legacy.routine` y un asistente **manual** del Jefe MAST (cuenta primero, aplica después, con nota en cada documento) para los 492 documentos controlados sin empresa. **Ningún dato de negocio cambia en el despliegue.**

**Architecture:** una versión de `quimibond_sgi` (19.0.57.95.0) sobre 57.94.1. Python en modelos existentes (`sgi_cron.py`, `sgi_my_pending.py`, `sgi_my_procedure_screen.py`, `sgi_links.py`, `sgi_kpi_account.py`, `sgi_document.py`), un modelo transitorio nuevo (`sgi.company.fix`, el asistente de D-06) con su ACL, vista, acción y menú, una acción planificada nueva (`sgi_cron_nightly_backup`, XML `noupdate` nuevo), una regla de registro nueva y **una migración técnica** (`pre-migrate` que crea y llena la columna `mail_activity.sgi_cron_kind` con SQL; no toca datos de negocio). Campos nuevos: `mail.activity.sgi_cron_kind`; `hr.employee.sgi_pending_saved_total/_late/_state`; `account.move.sgi_picking_manual` y `sgi_picking_proposed_ids` (sin guardar). Pruebas nuevas en un archivo (`test_rendimiento_robustez.py`, 17 casos) y una prueba existente ajustada (`test_my_pending.test_02`).

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh), Python 3, XML de vistas, `odoo.tests.TransactionCase`. Checadores del repo: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`, `flake8`.

**Base de código verificada:** rama `claude/confident-mendel-8yg7xu` recién igualada a `origin/main`, HEAD `c791802c` (merge del PR #513, hotfix 19.0.57.94.1 sobre el PR #508; manifest `19.0.57.94.1`). Las líneas de la ficha del plan general (`sgi_cron.py:679-687`, `sgi_my_pending.py:710-735`, `sgi_cron.py:198-207`, `sgi_my_procedure_screen.py:251, 276-289`, `sgi_links.py:172-186`, `sgi_kpi_account.py:63-75`) son viejas; abajo van las de este HEAD. Si cambian, buscar por el nombre del método. **Producción está en 19.0.57.90.1** (consulta del 2026-10-02): 57.91.0 a 57.94.1 todavía no se despliegan; esta entrega se despliega con ellas o después. Lo que no se pudo comprobar sin Odoo va marcado «VERIFICAR:» (sección 1.8).

---

## 0. Reglas (resumen de la sección 0 del plan general)

Leer antes de empezar: `CLAUDE.md` (raíz), `addons/quimibond_sgi/README.md` (glosario y «usted»), las entradas 57.92.0, 57.94.0 y 57.94.1 de `addons/quimibond_sgi/CHANGELOG.md`.

1. **Versión y CHANGELOG:** `__manifest__.py` a `'19.0.57.95.0'` y `## 19.0.57.95.0 — AAAA-MM-DD` arriba de la 57.94.1. Esta entrega **agrega un modelo** (`sgi.company.fix`): sin bump, `tools/check_addons.py` da **error**.
2. **Pruebas registradas** en `tests/__init__.py` (el checker lo exige), clases con `@tagged('post_install', '-at_install')`.
3. **Las pruebas solo corren en Odoo.sh**, build de desarrollo de la rama, con `--test-tags /quimibond_sgi` (no el suite completo: las pruebas de nómina lo detienen). El CI no instala el SGI. Ciclo: prueba → push → ver el fallo en el build → código → checadores → push → leer el build.
4. **Checadores locales** (0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   flake8 addons/quimibond_sgi
   ```
5. **Sin herencias propias** de vistas del SGI. La única herencia que se toca es de otro módulo y se edita en su `<record>`: `sgi_account_move_view_form_links` (hereda `account.view_move_form`). La vista del asistente es un registro nuevo (primaria).
5b. **`_name` en extensiones con mixin** (57.94.1): una clase que hereda un modelo **más** un mixin (`_inherit = [...]` en lista) declara `_name`; `tools/check_odoo_views.py` marca una lista `_inherit` sin `_name`. En esta entrega ninguna clase nueva lo hace: `HrEmployeePendingSaved` (`_inherit = 'hr.employee'`), `MailActivitySgiCron` y `AccountMoveLink` heredan un solo modelo por cadena, y `sgi.company.fix` es modelo nuevo con `_name`. Si al implementar se agrega un mixin a alguna, poner `_name` igual al modelo heredado.
6. **Menús** solo en `views/sgi_menus.xml` y su árbol en `addons/quimibond_sgi/tools/sgi_menu_tree.txt` (`tests/test_menu_tree.py` lo compara). Esta entrega agrega **un** menú (Task 5.7).
7. **«Usted» y glosario:** `tests/test_usted.py` revisa toda cadena nueva (nada de «tú», «tu», «puedes»; «Jefe MAST», «no conformidad», «las NC»).
8. **Despliegue:** PR rama → `main`, revisar el build; PR `main` → `quimibond`; `odoo-update quimibond_sgi` según `docs/RUNBOOK_DESPLIEGUE.md`.
9. **Producción es de solo lectura.** El despliegue no cambia datos de negocio: la migración solo crea y llena una columna técnica de `mail.activity` (sección «Migración»). La corrección de D-06 (empresa en 492 documentos y, por arrastre, en 815 rutinas) **no** es migración: es un asistente que el Jefe MAST corre a mano **después** del visto bueno escrito de Jose en el PR (pregunta Q1). Nada se borra.

---

## 1. Decisiones de diseño (con evidencia del código)

### 1.1 Lo que se midió en producción (2026-10-02, MCP, solo conteos, compañía 1)

| Qué | Cifra |
|---|---|
| Versión instalada de `quimibond_sgi` | 19.0.57.90.1 |
| Acuses pendientes (`sgi.document.ack`, `state = pendiente`) | 147: 7 del 24-sep y 140 del 28-sep (publicación de Mi procedimiento) |
| … de personas sin usuario / con usuario | 116 / 31 (30 usuarios distintos) |
| Avisos de acuse que ya existen (`sgi_cron_key =like 'acuse_pendiente:%'` o resumen «Acuse pendiente: …») | 0: ningún acuse ha pasado los 7 días hábiles (`quimibond_sgi.doc_ack_pending_days`). Los 7 del 24-sep los cruzan el **lunes 5-oct** y los 140 del 28-sep el **miércoles 7-oct** (sin festivos) |
| Empleados sin usuario (compañía 1) | 132, con 16 jefes distintos y 1 sin jefe; 3 de esos jefes no tienen usuario (51 personas a su cargo) |
| Empleados en el alcance de Mi equipo (con usuario o puesto) | 164 de 165 |
| `mail.activity` (todas, con archivadas) | 7,260 (6,648 archivadas); 80 con `sgi_cron_key` (9 archivadas) |
| Documentos controlados | 589: **492 sin empresa** (482 vigentes, 9 obsoletos, 1 borrador; todos activos, binarios, ninguno en carpeta con empresa, ninguno con acuses) y 97 con la compañía 1 (los Mi procedimiento) |
| `sgi.legacy.routine` | 815, todas sin empresa (`company_id` es `related` guardado de `procedure_id.company_id`) |
| Usuarios internos activos sin la compañía 1 entre sus empresas | 0 |
| Facturas (`account.move`) con `sgi_picking_ids` | 20,964 (15,944 de cliente, 4,771 de proveedor, 249 notas) |
| Facturas sin entregas guardadas cuyas entregas ya están hechas (el caso K-05) | 1 (de proveedor) |
| Facturas pagadas o en pago sin `sgi_payment_date` | 0 |

Con esto: **hoy el aviso por acuse mandaría ~147 avisos (116 al Jefe MAST) la semana del 5-oct**. Con 57.95.0: a lo más ~30 «Documentos por leer y firmar» (uno por usuario) y ~16 «Acuses pendientes de su gente» (uno por jefe); al Jefe MAST le llegan 4 (los equipos de los 3 jefes sin usuario y la persona sin jefe). Si 57.95.0 no llega a producción antes del 5-oct, ver Q8.

### 1.2 Un aviso por persona o por jefe (K-08, «aviso por acuse»)

Hoy (`models/sgi_cron.py`, `cron_documents`, l.617-700): por cada acuse pendiente de más de `doc_ack_pending_days` días hábiles (l.677-683), `_ack_notice` (l.685-692) agenda sobre el **documento** «Acuse pendiente: <empleado>» con clave `acuse_pendiente:<id del acuse>` al usuario del empleado o, sin usuario, al Jefe MAST (`manager_id`, l.683). Barrido al final (l.695-697). En Mis pendientes, 57.92.0 ya oculta el aviso de quien tiene su propio renglón «acuse» (`sgi_my_pending.py`, `_sgi_notice_covered`, l.~285-305, rama `acuse_pendiente:`).

Decisión:
- **Persona con usuario activo** → un aviso **«Documentos por leer y firmar: N»** sobre **su ficha de empleado** (`hr.employee`), clave `acuses_propios`, a su usuario. Como ya tiene sus renglones «acuse», Mis pendientes no lo muestra como «Aviso» (mismo criterio de 57.92.0); sigue sirviendo de campanita y correo.
- **Persona sin usuario** → un aviso por **jefe**: sobre la ficha del jefe (`parent_id`), clave `acuses_equipo`, **al usuario del jefe** si lo tiene y está activo; si el jefe no tiene usuario, **al Jefe MAST** (sobre la misma ficha del jefe, para que se sepa de qué equipo es). Sin jefe, sobre la ficha de la propia persona y al Jefe MAST. Resumen: «Acuses pendientes de su gente: N (P personas)», «Acuses pendientes de la gente de <jefe> (sin usuario): N (P personas)» o «Acuses pendientes de <persona> (sin jefe): N». La nota lista hasta 20 renglones «persona — documento (desde el dd/mm/aaaa)» y «y N más».
- **Por qué jefe y no documento** (la ficha decía «por documento o por jefe»): los 140 acuses del 28-sep son de 91 documentos de Mi procedimiento distintos (uno por puesto, de 1 a 7 acuses cada uno); por documento serían ~91 avisos, por jefe ~16. Y el jefe es quien puede sentar a su gente a leer. Pregunta Q3.
- **Clave sin usuario** (regla de 56.37.0): la clave es la clase (`acuses_propios` / `acuses_equipo`) y el ancla es la ficha; si el jefe recibe usuario mañana, el mismo aviso se reasigna (`_sgi_refresh_activity`) en vez de duplicarse. Precedente de avisos sobre `hr.employee`: `cron_competences` (`sgi_cron.py:1383-1405`).
- **Vencimiento** del aviso: el día en que el acuse más viejo del grupo cruzó el umbral (creación + `doc_ack_pending_days` días hábiles). Fijo mientras el grupo no cambie: el aviso no se reescribe cada día (antes era `today`).
- **Avisos viejos** (`acuse_pendiente:<id>`, de antes de 57.95.0; 0 en producción, pueden existir en staging): un barrido aparte los cierra en la primera corrida con «se reemplazó por un aviso por persona o por jefe».
- **Mis pendientes:** `hr.employee` entra a `NOTICE_MODELS` (los avisos con clave sobre fichas de empleado salen como «Aviso»; hoy también los de certificaciones y formación de `cron_competences`, que no tienen otro renglón). `_sgi_notice_covered` oculta el `acuses_propios` del mismo usuario. **«Ir»** en un `acuses_equipo` abre la lista de **Acuses de lectura** pendientes de esa gente (si quien abre es Usuario SGI; si no, la ficha, como antes).

### 1.3 Mi equipo: los filtros leen un resumen guardado (K-08)

Hoy (`models/sgi_my_pending.py`, `HrEmployeePublicPending`, l.~868-918): `sgi_mp_pending_total/_late/_state` son calculados sin guardar; sus `_search_*` llaman `_sgi_pending_ids_where`, que arma Mis pendientes de **todos** los empleados de la empresa (164) en cada búsqueda («Con pendientes atrasados», «Con pendientes por vencer», «Al día» de `views/sgi_my_procedure_views.xml:566-568`).

Decisión:
- Tres campos guardados en `hr.employee` (privado; nadie los ve en pantalla): `sgi_pending_saved_total`, `sgi_pending_saved_late`, `sgi_pending_saved_state`. Se escriben **solo si cambian** (`_sgi_save_pending_summary`), con `tracking_disable`.
- Se refrescan (a) cada noche para todo el alcance (Task 5.5) y (b) cada vez que alguien abre una lista de pendientes (`sgi.my.pending._sgi_build`: Mis pendientes, «Ver pendientes», «Pendientes del equipo»), con lo que ya se calculó para abrirla.
- Los `_search_*` de Mi equipo buscan en esos campos (`hr.employee` en sudo). **Las columnas siguen en vivo** (solo la página que se ve). Consecuencia: durante el día el filtro puede ir atrás de la columna hasta que la persona o su jefe abran la lista o pase la noche. Los cambios por fecha (vencer) ocurren a medianoche y el respaldo corre a las 02:15. Pregunta Q4.
- `_sgi_pending_ids_where` se quita (sin otros usos).

### 1.4 Clase del aviso indexada (K-08, barrido)

Hoy `_sgi_sweep` (`sgi_cron.py:191-212`) arma `('sgi_cron_key', '=', kind) | ('sgi_cron_key', '=like', kind + ':%')` por cada clase: el `=like` no usa el índice B-tree de `sgi_cron_key` (`varchar_pattern_ops` no existe en ese índice) y recorre `mail_activity` completa con sus archivadas, que nunca se podan.

Decisión: `mail.activity.sgi_cron_kind` = parte de `sgi_cron_key` antes del primer «:» (Char calculado y guardado, `@api.depends('sgi_cron_key')`, `index='btree_not_null'`: solo indexa las ~80 filas con clave). El barrido busca `('sgi_cron_kind', 'in', kinds)`: misma semántica exacta (clave = clase o empieza con «clase:») y usa el índice. Las archivadas no se tocan (nada se borra). La columna se crea y se llena en `pre-migrate` con SQL (sección «Migración»), para que el ORM no recalcule las 7,260 actividades en Python ni les cambie `write_date`.

### 1.5 Respaldo nocturno de Mi procedimiento (K-08)

Hoy las cuatro listas guardadas del empleado (`sgi_mp_role_ids`, `sgi_mp_received_role_ids`, `sgi_mp_short_role_ids`, `sgi_mp_process_ids`; `models/sgi_my_procedure_screen.py:228-275`) dependen de `sgi_mp_job_id`, la familia y el usuario; todo lo demás (roles, actividades, publicación, líneas de negocio) las marca a mano con `_sgi_mp_touch_jobs` (l.~276-289; 13 llamadas en 7 archivos). Un disparo olvidado dejó listas viejas de 57.13 a 57.88.

Decisión: `hr.employee._sgi_mp_nightly_recompute()` toma la foto de las cuatro listas de todos los empleados activos, las marca para recalcular (`env.add_to_compute`), baja todo (`flush_all`) y compara. Devuelve los empleados que cambiaron; si son 0: `INFO` «0 cambios en N personas»; si no: `WARNING` con cuántas y sus ids («falta un disparo de recálculo») y marca sucias las cifras de sus puestos (`_sgi_mp_mark_dirty`). Lo corre un cron nuevo diario, **«SGI: Respaldo nocturno (Mi procedimiento y Mi equipo)»** (02:15 de México, 08:15 UTC), que después refresca el resumen de 1.3. Cada paso en su savepoint (`_sgi_step`) y solo lo corre el sistema (`sgi_require_system`).

### 1.6 K-05: entregas facturadas y fecha de pago

Hoy (`models/sgi_links.py:172-192`): `account.move.sgi_picking_ids` (Many2many guardado, `readonly=False`) depende de `invoice_line_ids.sale_line_ids.move_ids.picking_id` y `…purchase_line_id.move_ids.picking_id`, pero filtra `p.state == 'done'` **sin depender de `state`**: una factura hecha antes de validar la entrega se queda vacía (en producción: 1). Y en cuanto hay entregas hechas, **pisa** lo que se ajustó a mano (`if pickings or not move.sgi_picking_ids`). `models/sgi_kpi_account.py:63-75`: `sgi_payment_date` usa `invoice_date` como respaldo y lee `account_id.account_type` sin depender de ellos.

Decisión («separar propuesto de ajustado»):
- `sgi_picking_proposed_ids` (calculado **sin guardar**): las entregas hechas de las líneas. Es lo que el sistema propone.
- `sgi_picking_manual` (Boolean, `readonly`): se enciende cuando alguien escribe `sgi_picking_ids` a mano (override de `write`; el recálculo no pasa por `write`).
- `sgi_picking_ids` (el guardado, el que leen las actividades S2.08 por `match_path`): igual a lo propuesto, salvo que esté ajustado a mano (no se toca) o que lo propuesto venga vacío y ya haya algo guardado (se conserva, como hoy: protege los ajustes de antes de 57.95.0, que no traen la marca). Dependencias completas: `move_type`, `sgi_picking_manual` y `….picking_id.state` de ambos caminos.
- Botón **«Volver a las entregas propuestas»** en la pestaña SGI de la factura (con ajuste a mano o cuando lo guardado difiere de lo propuesto, `sgi_picking_outdated` sin guardar): apaga la marca y pone lo propuesto.
- La marca se enciende solo si lo escrito **difiere** de lo propuesto (el formulario reenvía `sgi_picking_ids` cuando un onchange lo recalcula).
- `sgi_payment_date`: agrega `invoice_date` y `line_ids.account_id` a las dependencias.
- **Costo:** la dependencia de `picking_id.state` dispara el recálculo de las facturas ligadas a una entrega cuando cambia su estado (una o dos por entrega), no de toda la tabla. La factura de producción que quedó vacía no se recalcula sola (pregunta Q7).

### 1.7 D-06: empresa en documentos controlados y en las rutinas

Aviso de nombres: el «D-06» de la auditoría 2026-10 es un **hallazgo de datos** («Datos sin compañía»), no la decisión D-06 de `docs/audit/decisiones.md` (incidentes). Aquí se escribe «D-06 (datos)».

Hoy: 492 controlados sin empresa (1.1); `sgi.legacy.routine.company_id` es `related='procedure_id.company_id', store=True` (`models/sgi_legacy_routine.py:~170`) y es el único modelo con `company_id` sin regla de empresa (las demás en `security/sgi_security.xml:99-210`). El 57.89.0 recuerda lo que pasa al escribir reglas de empresa a mano sobre datos sin revisar.

Decisión, en tres partes:
1. **Que no vuelva a pasar (código):** un documento que nace controlado sin empresa, o que se vuelve controlado sin empresa, toma la empresa del SGI (`sgi.config._sgi_company()`) en `create`/`write`, **salvo** que su carpeta tenga empresa (la pone Documents) o que su familia (misma `sgi_code`) ya exista: entonces toma la de la familia, aunque sea ninguna (`_sgi_family_company`). Motivo: `_sgi_same_code_docs` compara por empresa; una revisión nueva con PNTQ dejaría de ver a sus anteriores sin empresa (revisión que sube, clave+revisión única) hasta que corra el asistente. Solo registros nuevos o que cambian; no toca los 492.
2. **Regla** `rule_sgi_legacy_routine_company` (`['|', ('company_id', '=', False), ('company_id', 'in', company_ids)]`, la de todos los modelos del SGI). Con las 815 sin empresa no cambia lo que nadie ve.
3. **Los 492 (datos de negocio):** asistente **manual** `sgi.company.fix` («Empresa en documentos controlados», Administración SGI → Configuración, solo Jefe MAST). Al abrirlo **cuenta** (por estado y las rutinas que arrastra) sin escribir nada; «Ver los documentos» los lista; «Asignar la empresa del SGI» escribe en lotes de 100 (cada lote en su savepoint), deja en cada documento una nota «Empresa del SGI asignada: <empresa> (antes sin empresa). D-06, 57.95.0; la aplicó <usuario>» y escribe en el log antes (ids) y después (cuántos). Las rutinas toman la empresa solas (`related` guardado). Idempotente: vuelve a abrirlo y cuenta 0. **No hay post-migrate de datos.** Se corre después del OK escrito de Jose (Q1).
- Efecto que hay que saber antes de aplicarlo: la regla de Documents oculta un documento con empresa a quien trabaja con **otras** empresas seleccionadas en el selector. En producción todos los usuarios internos activos tienen la compañía 1 entre sus permitidas (0 sin ella), pero quien tenga seleccionada solo otra razón social dejará de ver esos documentos hasta seleccionar PNTQ, igual que hoy con los 97 Mi procedimiento.

### 1.8 Lo que no se pudo verificar fuera de Odoo.sh

No hay fuentes de Odoo 19 en este entorno. Confirmar en el shell de Odoo.sh antes de empujar el código (Task 5.1, paso 4):
- VERIFICAR: que una columna creada antes por `pre-migrate` hace que el ORM **no** recalcule el campo guardado nuevo: `grep -n "fields_to_compute\|def update_db_column" /home/odoo/src/odoo/odoo/orm/models.py /home/odoo/src/odoo/odoo/orm/fields.py` (en 17/18 `update_db` devuelve `True` solo si creó la columna). Si sí recalcula, no pasa nada grave (7,260 filas), pero se mueve `write_date`: anotarlo en el CHANGELOG.
- VERIFICAR: `index='btree_not_null'` en un `Char` calculado guardado crea `mail_activity__sgi_cron_kind_index` parcial (`WHERE sgi_cron_kind IS NOT NULL`); punto (10) pendiente de 57.94.0, mismo patrón.
- VERIFICAR: que un cálculo guardado que **no asigna** un registro (`continue`) conserva el valor de la base en Odoo 19 (`sgi_picking_ids` ajustado a mano). Precedente en el mismo archivo: `StockPickingLink._compute_sgi_production_id` (`sgi_links.py:~230`). La prueba `test_13` lo confirma; si falla, asignar `move.sgi_picking_ids = move._origin.sgi_picking_ids` leyendo antes de marcar (ver nota en la Task 5.6).
- VERIFICAR: `env.add_to_compute(field, records)` + `env.flush_all()` recalcula y guarda en Odoo 19: `grep -n "def add_to_compute\|def flush_all\|_recompute_all" /home/odoo/src/odoo/odoo/orm/environments.py`.
- VERIFICAR: `account.move.line.sale_line_ids` se puede dar al crear la línea y `stock.move.sale_line_id` llena `sale.order.line.move_ids` (`grep -n "sale_line_ids\|move_ids" /home/odoo/src/odoo/addons/sale/models/account_move_line.py /home/odoo/src/odoo/addons/sale_stock/models/sale_order_line.py | head`).
- VERIFICAR: `hr.employee.write` con campos propios del SGI no pasa por la versión (`hr.version`) ni por la sincronización con `res.users` (`grep -n "def write" -A40 /home/odoo/src/odoo/addons/hr/models/hr_employee.py | grep -n "version\|_sync"`).
- VERIFICAR (revisión del plan): un aviso **sobre `hr.employee`** asignado a un usuario **sin** «Empleados / Encargado» (el operador con usuario, el jefe). `cron_competences` es el único precedente y en producción hay **0** actividades con clave sobre `hr.employee` (MCP, 2026-10-02), así que nunca se ha probado con datos reales. Revisar en el shell: `grep -n "_check_access_assignation\|def create" -A30 /home/odoo/src/odoo/addons/mail/models/mail_activity.py | grep -n "check_access\|UserError\|message_subscribe"` y el ACL de `hr.employee` para `base.group_user` (`grep -n "model_hr_employee," /home/odoo/src/odoo/addons/hr/security/ir.model.access.csv`). Si Odoo 19 exige que quien recibe pueda leer el registro, el aviso de un no-RH revienta en `_sgi_for_each` (cuenta como falla y **apaga el barrido** de toda la corrida): cambiar el ancla a `res.users` del que recibe (con `acuses_propios`) o al documento del acuse más viejo. Además, crear la actividad **suscribe** a quien la recibe como seguidor de la ficha del empleado (`message_subscribe` en `mail.activity.create`): el Jefe MAST y los jefes quedarían siguiendo fichas de RH. Si es así, desuscribirlos tras agendar o anotarlo en el CHANGELOG. `test_01`, `test_03` y `test_05` (usuarios sin grupo de RH) lo prueban.
- VERIFICAR: Documents no tiene `_check_company` que impida poner empresa a un documento en una carpeta sin empresa (`grep -n "_check_company\|check_company" /home/odoo/src/enterprise/documents/models/documents_document.py | head`).
- Verificado en el repo: `sgi.document.ack.write` deja pasar al sistema (`action_mark_read` como superusuario, `sgi_document.py:1414-1423`); `sgi_require_system` lanza `AccessError` (`models/sgi_guard.py`); los avisos sobre `hr.employee` ya existen (`cron_competences`).

---

## 2. Archivos

- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (import `Markup`; `MailActivitySgiCron.sgi_cron_kind`; `_sgi_sweep`; bloque de acuses de `cron_documents`; métodos nuevos `_sgi_ack_route`, `_sgi_ack_summary`, `_sgi_ack_note`, `_sgi_ack_deadline`, `_sgi_ack_team_acks`, `cron_nightly_backup`)
- Modify: `addons/quimibond_sgi/models/sgi_my_pending.py` (`NOTICE_MODELS`; `_sgi_notice_covered`; `_sgi_build`; `action_open`; clase nueva `HrEmployeePendingSaved`; `_search_sgi_mp_pending_*`; se quita `_sgi_pending_ids_where`)
- Modify: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` (logger; `HrEmployeeMyProcedureTab._sgi_mp_nightly_recompute`)
- Modify: `addons/quimibond_sgi/models/sgi_links.py` (`AccountMoveLink`)
- Modify: `addons/quimibond_sgi/models/sgi_kpi_account.py` (`_compute_sgi_payment_date` depends)
- Modify: `addons/quimibond_sgi/models/sgi_document.py` (`DocumentsDocument.create` y `write`: empresa del SGI)
- Create: `addons/quimibond_sgi/models/sgi_company_fix.py`; Modify: `addons/quimibond_sgi/models/__init__.py`
- Create: `addons/quimibond_sgi/data/sgi_nightly_cron.xml`; Create: `addons/quimibond_sgi/views/sgi_company_fix_views.xml`
- Modify: `addons/quimibond_sgi/views/sgi_links_views.xml` (record `sgi_account_move_view_form_links`), `views/sgi_menus.xml`, `addons/quimibond_sgi/tools/sgi_menu_tree.txt`
- Modify: `addons/quimibond_sgi/security/sgi_security.xml`, `security/ir.model.access.csv`
- Create: `addons/quimibond_sgi/migrations/19.0.57.95.0/pre-migrate.py`
- Create: `addons/quimibond_sgi/tests/test_rendimiento_robustez.py`; Modify: `tests/__init__.py`, `tests/test_my_pending.py`, `tests/test_bandeja.py`
- Modify: `docs/sgi/usuarios/jefe-de-area.md`, `docs/sgi/usuarios/mast.md`, `docs/sgi/administracion/manual-jefe-mast.md`
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), `docs/audit/decisiones.md` (cuando Jose conteste Q1-Q3)

---

## Task 5.0: Rama

- [x] **Step 1:** seguir en `claude/confident-mendel-8yg7xu` (ya igualada a `origin/main`). Si se trabaja fuera de esa sesión: `git fetch origin main && git checkout -B claude/sgi-57-95-rendimiento origin/main`. Confirmar `grep -n "'version'" addons/quimibond_sgi/__manifest__.py` → `19.0.57.94.1`.

## Task 5.1: Pruebas nuevas (fallan antes del código)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_rendimiento_robustez.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py` (al final)
- Modify: `addons/quimibond_sgi/tests/test_my_pending.py` (`test_02`), `addons/quimibond_sgi/tests/test_bandeja.py` (`test_13`)

- [x] **Step 1: Escribir `tests/test_rendimiento_robustez.py`**

```python
# -*- coding: utf-8 -*-
"""57.95.0 (auditoría 2026-10: K-08, K-05, D-06 de datos): rendimiento y
robustez.

- Acuses pendientes: un aviso por persona con usuario y uno por jefe para la
  gente sin usuario (antes, uno por acuse); el Jefe MAST solo recibe los de
  jefes sin usuario o personas sin jefe.
- Clase del aviso indexada (``sgi_cron_kind``) para el barrido de episodios.
- Los filtros de Mi equipo leen un resumen guardado por empleado: no
  recalculan Mis pendientes de toda la empresa en cada búsqueda.
- Respaldo nocturno: recalcula las cuatro listas guardadas de Mi
  procedimiento y dice cuántas personas cambiaron (0 en régimen).
- K-05: las entregas de la factura siguen al estado de la entrega; lo
  ajustado a mano no se pisa; la fecha de pago depende de la fecha de factura.
- D-06: los controlados nuevos nacen con la empresa del SGI; el asistente del
  Jefe MAST cuenta y asigna la empresa a los que no tienen; regla de empresa
  en las rutinas del procedimiento anterior."""
from datetime import timedelta
from unittest.mock import patch

from odoo import fields
from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast

ACK_KINDS = ('acuse_pendiente', 'acuses_propios', 'acuses_equipo')


class _RendimientoCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        # La base del build es copia de producción (147 acuses pendientes el
        # 2026-10-02): que ninguno real cruce el umbral en estas pruebas. Si
        # el aviso de un acuse real fallara, ``failures`` > 0 y el barrido no
        # correría (test_04). Se deshace al final de la prueba.
        cls.env.flush_all()
        cls.env.cr.execute("UPDATE sgi_document_ack SET create_date = now() "
                           "WHERE state = 'pendiente'")
        cls.env.invalidate_all()
        cls.today = fields.Date.context_today(cls.env.user)
        cls.mast = sgi_set_mast(cls.env, login='rr_mast')
        cls.company = cls.env['sgi.config']._sgi_company()
        cls.Activity = cls.env['mail.activity'].with_context(active_test=False)
        cls.Pending = cls.env['sgi.my.pending']
        cls.Cron = cls.env['sgi.cron']
        Employee = cls.env['hr.employee']
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.boss_user = new_test_user(cls.env, login='rr_jefe', groups=groups)
        cls.own_user = new_test_user(cls.env, login='rr_propio', groups=groups)
        cls.boss = Employee.create({'name': 'Jefe RR', 'user_id': cls.boss_user.id})
        cls.boss_nouser = Employee.create({'name': 'Jefe sin usuario RR'})
        cls.op1 = Employee.create({'name': 'Operador uno RR', 'parent_id': cls.boss.id})
        cls.op2 = Employee.create({'name': 'Operador dos RR', 'parent_id': cls.boss.id})
        cls.op3 = Employee.create({'name': 'Operador tres RR', 'parent_id': cls.boss_nouser.id})
        cls.orphan = Employee.create({'name': 'Operador sin jefe RR'})
        cls.own = Employee.create({'name': 'Con usuario RR', 'user_id': cls.own_user.id,
                                   'parent_id': cls.boss.id})
        cls.process = cls.env['sgi.process'].create({'code': 'Z95', 'name': 'Proceso 57.95'})
        Doc = cls.env['documents.document']
        cls.doc_a = Doc.create({
            'name': 'Instructivo A RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-01', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})
        cls.doc_b = Doc.create({
            'name': 'Instructivo B RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-02', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})

    def _ack(self, employee, doc, days_ago=30):
        """Acuse pendiente que ya pasó el umbral de días hábiles del cron."""
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': employee.id})
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_document_ack SET create_date = %s WHERE id = %s",
                            (fields.Datetime.now() - timedelta(days=days_ago), ack.id))
        ack.invalidate_recordset(['create_date'])
        return ack

    def _notices(self, anchor, kind):
        return self.Activity.search([('res_model', '=', anchor._name), ('res_id', '=', anchor.id),
                                     ('sgi_cron_kind', '=', kind)])


@tagged('post_install', '-at_install')
class TestAvisosDeAcuse(_RendimientoCase):

    def test_01_sin_usuario_un_aviso_por_jefe(self):
        acks = self._ack(self.op1, self.doc_a) | self._ack(self.op1, self.doc_b) \
            | self._ack(self.op2, self.doc_a)
        own_ack = self._ack(self.own, self.doc_a)
        self.Cron.cron_documents()
        notice = self._notices(self.boss, 'acuses_equipo').filtered('active')
        self.assertEqual(len(notice), 1, "Un aviso por jefe, no uno por acuse.")
        self.assertEqual(notice.user_id, self.boss_user, "Va al jefe, no al Jefe MAST.")
        self.assertIn("3", notice.summary)
        self.assertIn("Operador uno RR", notice.note)
        self.assertIn("Operador dos RR", notice.note)
        self.assertNotIn("Con usuario RR", notice.note, "Quien tiene usuario recibe el suyo.")
        self.assertEqual(notice.sgi_cron_key, 'acuses_equipo')
        self.assertFalse(self.Activity.search([
            ('res_model', '=', 'documents.document'), ('res_id', 'in', (self.doc_a | self.doc_b).ids),
            ('sgi_cron_kind', 'in', ACK_KINDS)]), "Ya no hay avisos de acuse sobre el documento.")
        self.assertTrue(acks and own_ack)

    def test_02_jefe_sin_usuario_o_sin_jefe_al_jefe_mast(self):
        self._ack(self.op3, self.doc_a)
        self._ack(self.orphan, self.doc_b)
        self.Cron.cron_documents()
        team = self._notices(self.boss_nouser, 'acuses_equipo').filtered('active')
        self.assertEqual(team.user_id, self.mast, "Jefe sin usuario: al Jefe MAST, sobre la ficha del jefe.")
        self.assertIn("Jefe sin usuario RR", team.summary)
        alone = self._notices(self.orphan, 'acuses_equipo').filtered('active')
        self.assertEqual(alone.user_id, self.mast, "Sin jefe: al Jefe MAST, sobre la ficha de la persona.")

    def test_03_con_usuario_un_aviso_propio_que_mis_pendientes_no_repite(self):
        self._ack(self.own, self.doc_a)
        self._ack(self.own, self.doc_b)
        self.Cron.cron_documents()
        notice = self._notices(self.own, 'acuses_propios').filtered('active')
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, self.own_user)
        self.assertIn("2", notice.summary)
        rows = self.Pending._sgi_pending_values(self.own_user)[self.own_user.id]
        self.assertEqual(len([r for r in rows if r['kind'] == 'acuse'
                              and r['res_model'] == 'sgi.document.ack']) >= 2, True)
        self.assertFalse([r for r in rows if r['kind'] == 'aviso' and r['res_id'] == notice.id],
                         "Ya tiene sus renglones «acuse»: el aviso no se repite.")

    def test_04_idempotente_cierra_al_firmar_y_cierra_los_viejos(self):
        ack = self._ack(self.op1, self.doc_a)
        legacy = self.doc_b.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=self.today,
            summary='Acuse pendiente: Operador uno RR', user_id=self.mast.id)
        legacy.sudo().sgi_cron_key = 'acuse_pendiente:%d' % ack.id
        self.Cron.cron_documents()
        self.Cron.cron_documents()
        self.assertEqual(len(self._notices(self.boss, 'acuses_equipo')), 1, "Dos corridas, un aviso.")
        self.assertFalse(legacy.active, "El aviso de uno por acuse se cierra: lo reemplaza el agrupado.")
        self.assertTrue(legacy.sgi_episode_closed)
        ack.action_mark_read()
        self.Cron.cron_documents()
        notice = self._notices(self.boss, 'acuses_equipo')
        self.assertFalse(notice.active, "Sin acuses pendientes el aviso se cierra solo.")
        self.assertTrue(notice.sgi_episode_closed)

    def test_05_el_jefe_lo_ve_en_mis_pendientes_e_ir_abre_los_acuses(self):
        acks = self._ack(self.op1, self.doc_a) | self._ack(self.op2, self.doc_b)
        self.Cron.cron_documents()
        notice = self._notices(self.boss, 'acuses_equipo').filtered('active')
        rows = self.Pending.with_user(self.boss_user)._sgi_build(self.boss)
        line = rows.filtered(lambda r: r.kind == 'aviso' and r.res_id == notice.id)
        self.assertEqual(len(line), 1, "La ficha del empleado entra a los avisos (NOTICE_MODELS).")
        action = line.with_user(self.boss_user).action_open()
        self.assertEqual(action['res_model'], 'sgi.document.ack')
        shown = self.env['sgi.document.ack'].with_user(self.boss_user).search(action['domain'])
        self.assertEqual(shown & acks, acks)

    def test_06_clase_del_aviso_indexada_y_barrido(self):
        field = self.env['mail.activity']._fields['sgi_cron_kind']
        self.assertTrue(field.store and field.index)
        make = self.doc_a.activity_schedule
        ep = make('mail.mail_activity_data_todo', summary='Ep RR', user_id=self.mast.id)
        other = make('mail.mail_activity_data_todo', summary='Otro RR', user_id=self.mast.id)
        plain = make('mail.mail_activity_data_todo', summary='Solo RR', user_id=self.mast.id)
        manual = make('mail.mail_activity_data_todo', summary='Manual RR', user_id=self.mast.id)
        ep.sudo().sgi_cron_key = 'episodio_rr:5'
        other.sudo().sgi_cron_key = 'episodio_rr_otro:5'
        plain.sudo().sgi_cron_key = 'episodio_rr'
        self.assertEqual(ep.sgi_cron_kind, 'episodio_rr')
        self.assertEqual(other.sgi_cron_kind, 'episodio_rr_otro')
        self.assertEqual(plain.sgi_cron_kind, 'episodio_rr')
        self.assertFalse(manual.sgi_cron_kind)
        self.Cron._sgi_new_run()._sgi_sweep(['episodio_rr'], "ya no aplica")
        self.assertFalse(ep.active)
        self.assertFalse(plain.active)
        self.assertTrue(other.active, "Otra clase con el mismo prefijo no se barre.")
        self.assertTrue(manual.active)


@tagged('post_install', '-at_install')
class TestMiEquipoGuardado(_RendimientoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        objective = cls.env['sgi.objective'].create({'name': 'Objetivo 57.95'})
        cls.late = cls.env['sgi.action.line'].create({
            'name': 'Acción atrasada RR', 'responsible_id': cls.own_user.id,
            'date_commit': cls.today - timedelta(days=3), 'objective_id': objective.id})

    def test_07_el_filtro_no_recorre_la_empresa(self):
        Public = self.env['hr.employee.public'].with_user(self.boss_user)
        boom = AssertionError("El filtro recalculó Mis pendientes de la empresa.")
        with patch.object(type(self.Pending), '_sgi_summary_employees', side_effect=boom):
            before = Public.search([('sgi_mp_pending_late', '>', 0)])
        self.assertNotIn(self.own.id, before.ids, "Sin resumen guardado todavía.")
        self.env['hr.employee']._sgi_refresh_pending_summary(self.own)
        with patch.object(type(self.Pending), '_sgi_summary_employees', side_effect=boom):
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_late', '>', 0)]).ids)
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_state', '=', 'atrasada')]).ids)
            self.assertIn(self.own.id, Public.search([('sgi_mp_pending_total', '>=', 1)]).ids)
            self.assertNotIn(self.own.id, Public.search([('sgi_mp_pending_state', '=', 'al_dia')]).ids)

    def test_08_el_resumen_solo_escribe_lo_que_cambia(self):
        Employee = self.env['hr.employee']
        changed = Employee._sgi_refresh_pending_summary(self.own)
        self.assertIn(self.own, changed)
        self.assertGreaterEqual(self.own.sgi_pending_saved_late, 1)
        self.assertEqual(self.own.sgi_pending_saved_state, 'atrasada')
        self.assertFalse(Employee._sgi_refresh_pending_summary(self.own), "Sin cambios no escribe.")

    def test_09_abrir_la_lista_refresca_el_resumen(self):
        Employee = self.env['hr.employee']
        Employee._sgi_refresh_pending_summary(self.own)
        total = self.own.sgi_pending_saved_total
        self.env['sgi.action.line'].create({
            'name': 'Otra acción RR', 'responsible_id': self.own_user.id,
            'date_commit': self.today - timedelta(days=1), 'objective_id': self.late.objective_id.id})
        self.Pending.with_user(self.own_user)._sgi_build(self.own)
        self.own.invalidate_recordset(['sgi_pending_saved_total'])
        self.assertEqual(self.own.sgi_pending_saved_total, total + 1)


@tagged('post_install', '-at_install')
class TestRespaldoNocturno(_RendimientoCase):

    def test_10_listas_de_mi_procedimiento_cero_cambios_en_regimen(self):
        job = self.env['hr.job'].create({'name': 'PUESTO RESPALDO RR'})
        emp = self.env['hr.employee'].create({'name': 'Persona respaldo RR', 'job_id': job.id})
        activity = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Actividad respaldo RR', 'number': '9.1',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        self.env.flush_all()
        self.env.invalidate_all()
        Employee = self.env['hr.employee']
        self.assertIn(activity.role_ids, emp.sgi_mp_role_ids)
        changed = Employee._sgi_mp_nightly_recompute()
        self.assertNotIn(emp, changed, "Con los disparos completos no cambia nada.")
        self.assertFalse(Employee._sgi_mp_nightly_recompute(), "Segunda corrida: 0 cambios.")
        # Un disparo que falta: la lista guardada se quedó vieja.
        self.env.cr.execute("DELETE FROM hr_employee_sgi_mp_role_rel WHERE employee_id = %s", (emp.id,))
        emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertFalse(emp.sgi_mp_role_ids)
        with self.assertLogs('odoo.addons.quimibond_sgi.models.sgi_my_procedure_screen',
                             level='WARNING') as logs:
            changed = Employee._sgi_mp_nightly_recompute()
        self.assertIn(emp, changed)
        self.assertIn("falta un disparo", "\n".join(logs.output))
        emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertIn(activity.role_ids, emp.sgi_mp_role_ids, "El respaldo la corrige.")

    def test_11_respaldo_nocturno_solo_el_sistema(self):
        with self.assertRaises(AccessError):
            self.Cron.with_user(self.boss_user).cron_nightly_backup()
        self.assertTrue(self.Cron.cron_nightly_backup())
        self.assertTrue(self.env.ref('quimibond_sgi.sgi_cron_nightly_backup').active)


@tagged('post_install', '-at_install')
class TestEntregasFacturadas(TransactionCase):
    """K-05."""

    def _sale_flow(self):
        customer = self.env['res.partner'].create({'name': 'Cliente K05'})
        product = self.env['product.product'].create({'name': 'Tela K05', 'type': 'consu'})
        order = self.env['sale.order'].create({
            'partner_id': customer.id,
            'order_line': [(0, 0, {'product_id': product.id, 'product_uom_qty': 1,
                                   'price_unit': 10.0})]})
        line = order.order_line
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        customers = self.env.ref('stock.stock_location_customers')
        picking = self.env['stock.picking'].create({
            'picking_type_id': wh.out_type_id.id, 'partner_id': customer.id,
            'location_id': wh.lot_stock_id.id, 'location_dest_id': customers.id,
            'move_ids': [(0, 0, {
                'product_id': product.id, 'product_uom_qty': 1.0, 'product_uom': product.uom_id.id,
                'sale_line_id': line.id, 'location_id': wh.lot_stock_id.id,
                'location_dest_id': customers.id})]})
        picking.action_confirm()
        picking.move_ids.write({'quantity': 1.0, 'picked': True})
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        invoice = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': customer.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': 1, 'price_unit': 10.0,
                'account_id': income.id, 'tax_ids': [(6, 0, [])],
                'sale_line_ids': [(6, 0, line.ids)]})]})
        return picking, invoice

    def test_12_la_factura_sigue_al_estado_de_la_entrega(self):
        picking, invoice = self._sale_flow()
        self.assertFalse(invoice.sgi_picking_ids, "La entrega aún no se valida.")
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        self.assertEqual(invoice.sgi_picking_ids, picking,
                         "Validar la entrega la propone en la factura (dependencia de state).")
        self.assertFalse(invoice.sgi_picking_manual)
        # El formulario reenvía lo que el onchange recalculó: escribir lo
        # mismo que lo propuesto no es un ajuste a mano.
        invoice.write({'sgi_picking_ids': [(6, 0, picking.ids)]})
        self.assertFalse(invoice.sgi_picking_manual)
        self.assertFalse(invoice.sgi_picking_outdated)

    def test_13_lo_ajustado_a_mano_no_se_pisa_y_se_puede_regresar(self):
        picking, invoice = self._sale_flow()
        other = picking.copy()
        invoice.write({'sgi_picking_ids': [(6, 0, other.ids)]})
        self.assertTrue(invoice.sgi_picking_manual, "Escribirlas a mano marca el ajuste.")
        picking.button_validate()
        self.assertEqual(invoice.sgi_picking_ids, other, "Lo ajustado a mano no se pisa.")
        self.assertEqual(invoice.sgi_picking_proposed_ids, picking, "Lo propuesto se ve aparte.")
        invoice.action_sgi_picking_reset()
        self.assertFalse(invoice.sgi_picking_manual)
        self.assertEqual(invoice.sgi_picking_ids, picking)

    def test_14_dependencias_completas(self):
        Move = self.env['account.move']
        picking_deps = Move._fields['sgi_picking_ids'].depends
        self.assertIn('invoice_line_ids.sale_line_ids.move_ids.picking_id.state', picking_deps)
        self.assertIn('invoice_line_ids.purchase_line_id.move_ids.picking_id.state', picking_deps)
        self.assertIn('sgi_picking_manual', picking_deps)
        self.assertIn('invoice_date', Move._fields['sgi_payment_date'].depends)


@tagged('post_install', '-at_install')
class TestEmpresaDelSgi(_RendimientoCase):
    """D-06 (datos)."""

    def _loose(self, code, doc_type='instructivo', **extra):
        doc = self.env['documents.document'].create(dict({
            'name': 'Sin empresa %s' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': 'vigente'}, **extra))
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (doc.id,))
        doc.invalidate_recordset(['company_id'])
        return doc

    def test_15_el_asistente_cuenta_y_luego_aplica(self):
        # La copia de producción trae 492 controlados sin empresa: aplicar el
        # asistente sobre ellos haría la prueba lenta y dependiente de datos
        # reales (un lote que choque con una regla deja ``again`` > 0). Se
        # les pone empresa por SQL dentro de la prueba (se deshace al final).
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = %s "
                            "WHERE sgi_is_controlled IS TRUE AND company_id IS NULL",
                            (self.company.id,))
        self.env.invalidate_all()
        procedure = self._loose('P-Z95', doc_type='procedimiento')
        loose = procedure | self._loose('IT-Z95-03', sgi_process_id=self.process.id)
        routine = self.env['sgi.legacy.routine'].create({
            'procedure_id': procedure.id, 'n': 1, 'name': 'Rutina RR', 'state': 'eliminada',
            'reason': 'Prueba 57.95'})
        self.assertFalse(routine.company_id)
        free = self.env['documents.document'].create({'name': 'No controlado RR', 'type': 'binary'})
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (free.id,))
        free.invalidate_recordset(['company_id'])
        with self.assertRaises(AccessError):
            self.env['sgi.company.fix'].with_user(self.boss_user).create({})
        wizard = self.env['sgi.company.fix'].with_user(self.mast).create({})
        self.assertEqual(wizard.doc_count, 2)
        self.assertEqual(wizard.routine_count, 1)
        self.assertFalse(loose.filtered('company_id'), "Contar no escribe nada.")
        wizard.action_apply()
        loose.invalidate_recordset(['company_id'])
        self.assertEqual(loose.company_id, self.company)
        self.assertTrue(any("Empresa del SGI asignada" in (m.body or '') for m in procedure.message_ids))
        routine.invalidate_recordset(['company_id'])
        self.assertEqual(routine.company_id, self.company, "La rutina la toma de su procedimiento.")
        free.invalidate_recordset(['company_id'])
        self.assertFalse(free.company_id, "Un documento no controlado no se toca.")
        self.assertEqual(wizard.done_count, 2)
        self.assertFalse(wizard.failed_count)
        again = self.env['sgi.company.fix'].with_user(self.mast).create({})
        self.assertEqual(again.doc_count, 0, "Idempotente.")

    def test_16_controlado_nuevo_nace_con_la_empresa_del_sgi(self):
        doc = self.env['documents.document'].create({
            'name': 'Nuevo RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-04', 'sgi_process_id': self.process.id})
        self.assertEqual(doc.company_id, self.company)
        free = self.env['documents.document'].create({'name': 'Libre RR', 'type': 'binary'})
        self.env.flush_all()
        self.env.cr.execute("UPDATE documents_document SET company_id = NULL WHERE id = %s", (free.id,))
        free.invalidate_recordset(['company_id'])
        free.write({'sgi_is_controlled': True, 'sgi_doc_type': 'instructivo',
                    'sgi_code': 'IT-Z95-05', 'sgi_process_id': self.process.id})
        self.assertEqual(free.company_id, self.company, "Al volverse controlado toma la empresa.")
        # Una revisión nueva de una familia que vive sin empresa (D-06 aún
        # sin aplicar) se queda con su familia: la revisión se sigue
        # comparando contra las anteriores.
        old = self._loose('IT-Z95-06', sgi_process_id=self.process.id)
        rev = self.env['documents.document'].create({
            'name': 'Revisión RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z95-06', 'sgi_revision': old.sgi_revision + 1,
            'sgi_process_id': self.process.id})
        self.assertFalse(rev.company_id)

    def test_17_regla_de_empresa_en_rutinas(self):
        rule = self.env.ref('quimibond_sgi.rule_sgi_legacy_routine_company')
        self.assertIn('company_ids', rule.domain_force)
        other = self.env['res.company'].create({'name': 'Otra empresa RR'})
        procedure = self.env['documents.document'].create({
            'name': 'Procedimiento otra RR', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-Z96', 'sgi_state': 'vigente',
            'company_id': other.id})
        routine = self.env['sgi.legacy.routine'].create({
            'procedure_id': procedure.id, 'n': 1, 'name': 'Rutina otra RR', 'state': 'eliminada',
            'reason': 'Prueba 57.95'})
        self.assertEqual(routine.company_id, other)
        self.assertFalse(self.env['sgi.legacy.routine'].with_user(self.mast).search(
            [('id', '=', routine.id)]), "El Jefe MAST (solo PNTQ) no la ve.")
```

- [x] **Step 2: Ajustar `tests/test_my_pending.py` `test_02`** (l.~64-71): el filtro ya no recalcula; refrescar el resumen guardado antes de buscar. Después de `row = Public.browse(self.emp.id)` y antes del primer `Public.search`:

```python
        # 57.95.0 (K-08): los filtros de Mi equipo leen el resumen guardado
        # (cron nocturno o al abrir una lista de pendientes).
        self.env['hr.employee']._sgi_refresh_pending_summary(self.emp)
```

- [x] **Step 2b: Ajustar `tests/test_bandeja.py` `test_13_mi_equipo_sin_usuario`** (l.~318-329): también filtra Mi equipo (`Public.search([('sgi_mp_pending_state', '=', 'atrasada')])`, l.326) y con el filtro guardado fallaría. Después de `row = Public.browse(self.floor.id)` y antes del `assertIn(... Public.search(...))`:

```python
        # 57.95.0 (K-08): el filtro lee el resumen guardado.
        self.env['hr.employee']._sgi_refresh_pending_summary(
            self.env['hr.employee'].browse(self.floor.id))
```

(`grep -rn "sgi_mp_pending_\(late\|state\|total\)'" addons/quimibond_sgi/tests` en c791802c: solo `test_my_pending.py:70-71` y `test_bandeja.py:326` filtran; `test_my_procedure_ui.py:82` solo revisa el arch.)

- [x] **Step 3: Registrar** al final de `tests/__init__.py`:

```python
from . import test_rendimiento_robustez
```

- [ ] **Step 4: Confirmar en el shell de Odoo.sh lo de la sección 1.8** (`fields_to_compute`, `add_to_compute`, `sale_line_ids`, `hr.employee.write`, `_check_company` de Documents). Si algo difiere, ajustar la prueba antes de empujar.

- [x] **Step 5: Push solo de las pruebas y verlas fallar**

```bash
python3 tools/check_addons.py --base-ref origin/main
flake8 addons/quimibond_sgi/tests/test_rendimiento_robustez.py addons/quimibond_sgi/tests/test_my_pending.py addons/quimibond_sgi/tests/test_bandeja.py
git add addons/quimibond_sgi/tests/test_rendimiento_robustez.py addons/quimibond_sgi/tests/test_my_pending.py addons/quimibond_sgi/tests/test_bandeja.py addons/quimibond_sgi/tests/__init__.py
git commit -m "quimibond_sgi: pruebas de rendimiento y robustez (fallan antes del código)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

`check_addons` avisará «archivos cambiados sin bump» (advertencia): la versión sube en la Task 5.9.

Esperado en el log (`--test-tags /quimibond_sgi`): errores en todo `TestAvisosDeAcuse` (`sgi_cron_kind` no existe), `TestMiEquipoGuardado` (`_sgi_refresh_pending_summary`), `TestRespaldoNocturno` (`_sgi_mp_nightly_recompute`, `cron_nightly_backup`), `TestEntregasFacturadas.test_12` (la factura no sigue a `state`), `test_13` y `test_14` (`sgi_picking_manual`), y `TestEmpresaDelSgi` (`sgi.company.fix`, regla). `test_my_pending.test_02` y `test_bandeja.test_13` **dan error** antes del código (`_sgi_refresh_pending_summary` no existe); pasan después de la Task 5.4.

## Task 5.2: Clase del aviso indexada (`sgi_cron_kind`) y su migración

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (`MailActivitySgiCron`, l.35-58; `_sgi_sweep`, l.190-212)
- Create: `addons/quimibond_sgi/migrations/19.0.57.95.0/pre-migrate.py`

- [x] **Step 1: Campo**, en `MailActivitySgiCron` después de `sgi_cron_run` (l.57-58):

```python
    # 57.95.0 (K-08): la clase del aviso (la clave antes del primer «:»),
    # guardada e indexada solo donde hay clave. El barrido de episodios
    # busca por aquí en vez de ``sgi_cron_key =like 'clase:%'``, que recorría
    # todas las actividades con sus archivadas. La columna la crea y la llena
    # migrations/19.0.57.95.0/pre-migrate.py.
    sgi_cron_kind = fields.Char(
        string="Clase del aviso (SGI)", compute='_compute_sgi_cron_kind', store=True,
        index='btree_not_null', copy=False, readonly=True)

    @api.depends('sgi_cron_key')
    def _compute_sgi_cron_kind(self):
        for activity in self:
            activity.sgi_cron_kind = (activity.sgi_cron_key or '').split(':', 1)[0] or False
```

- [x] **Step 2: `_sgi_sweep`** (l.191-212): reemplazar el armado de `key_domain` y la búsqueda por

```python
        # 57.95.0 (K-08): por la clase indexada. Misma semántica que antes
        # (clave igual a la clase o que empieza con «clase:»).
        stale = self.env['mail.activity'].sudo().with_context(active_test=False).search(
            [('sgi_cron_kind', 'in', list(kinds)), ('sgi_episode_closed', '=', False),
             '|', ('sgi_cron_run', '=', False), ('sgi_cron_run', '!=', run)])
```

- [x] **Step 3: `migrations/19.0.57.95.0/pre-migrate.py`**

```python
# -*- coding: utf-8 -*-
"""57.95.0 (K-08): columna técnica ``mail_activity.sgi_cron_kind``.

Crea la columna antes de que el ORM cargue el campo calculado guardado y la
llena con SQL: la parte de ``sgi_cron_key`` antes del primer «:» (vacía →
NULL). Así el ORM no recalcula en Python todas las actividades (7,260 en
producción el 2026-10-02, 80 con clave) ni les cambia ``write_date``. El
índice parcial lo crea el ORM después.

Solo escribe esa columna, en lotes de 5,000 por id, y solo en las filas cuya
clase no coincide (idempotente: una segunda corrida no escribe nada). No toca
datos de negocio ni borra nada. Registra cuántas filas llenó."""
import logging

_logger = logging.getLogger(__name__)
BATCH = 5000


def migrate(cr, version):
    if not version:
        return
    cr.execute("ALTER TABLE mail_activity ADD COLUMN IF NOT EXISTS sgi_cron_kind varchar")
    total = 0
    while True:
        cr.execute("""
            UPDATE mail_activity
               SET sgi_cron_kind = NULLIF(split_part(sgi_cron_key, ':', 1), '')
             WHERE id IN (
                   SELECT id FROM mail_activity
                    WHERE sgi_cron_key IS NOT NULL
                      AND sgi_cron_kind IS DISTINCT FROM NULLIF(split_part(sgi_cron_key, ':', 1), '')
                    ORDER BY id
                    LIMIT %s)
        """, (BATCH,))
        if not cr.rowcount:
            break
        total += cr.rowcount
    cr.execute("SELECT count(*) FROM mail_activity WHERE sgi_cron_kind IS NOT NULL")
    _logger.info("SGI 57.95.0: sgi_cron_kind llenado en %d actividades (%d con clase en total).",
                 total, cr.fetchone()[0])
```

- [x] **Step 4: Checadores** (sección 0, punto 4) y **commit**

```bash
git add addons/quimibond_sgi/models/sgi_cron.py addons/quimibond_sgi/migrations/19.0.57.95.0/pre-migrate.py
git commit -m "quimibond_sgi: clase del aviso indexada para el barrido de episodios (K-08)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.3: Un aviso por persona o por jefe (K-08)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (import; bloque de acuses de `cron_documents`, l.672-697; métodos nuevos junto a `_sgi_manager_user_id`, l.231)
- Modify: `addons/quimibond_sgi/models/sgi_my_pending.py` (`NOTICE_MODELS` l.~76-80; `_sgi_notice_covered` l.~262-306; `action_open` l.~640-660)

- [x] **Step 1: Import.** `from markupsafe import escape` (l.9) pasa a `from markupsafe import Markup, escape`.

- [x] **Step 2: Métodos de apoyo**, después de `_sgi_sales_admin_user_id` (l.~254-258):

```python
    # ------------------------------------------------------------------
    # 57.95.0 (K-08): avisos de acuse agrupados.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_ack_route(self, employee, manager_id):
        """(clase, ficha ancla, usuario que recibe) del aviso de un acuse
        pendiente. Con usuario activo: «acuses_propios» sobre su ficha. Sin
        usuario: «acuses_equipo» sobre la ficha de su jefe (sin jefe, la suya)
        al usuario del jefe o, si no tiene, al Jefe MAST."""
        employee = employee.sudo()
        user = employee.user_id
        if user and user.active:
            return 'acuses_propios', employee, user.id
        anchor = employee.parent_id or employee
        boss_user = anchor.user_id if anchor != employee else self.env['res.users']
        if boss_user and boss_user.active:
            return 'acuses_equipo', anchor, boss_user.id
        return 'acuses_equipo', anchor, manager_id

    @api.model
    def _sgi_ack_team_acks(self, anchor):
        """Acuses pendientes del aviso «acuses_equipo» de ``anchor``: de la
        gente sin usuario activo cuyo jefe es ``anchor`` (o ``anchor`` mismo
        si no tiene jefe). Lo usa «Ir» de Mis pendientes."""
        anchor = anchor.sudo()
        acks = self.env['sgi.document.ack'].sudo().search([
            ('state', '=', 'pendiente'), ('document_id.active', '=', True),
            '|', ('employee_id.parent_id', '=', anchor.id), ('employee_id', '=', anchor.id)])
        return acks.filtered(
            lambda a: (a.employee_id.parent_id or a.employee_id) == anchor
            and not (a.employee_id.user_id and a.employee_id.user_id.active))

    @api.model
    def _sgi_ack_summary(self, kind, anchor, acks, user_id):
        people = acks.employee_id
        if kind == 'acuses_propios':
            return "Documentos por leer y firmar: %d" % len(acks)
        if people == anchor:
            return "Acuses pendientes de %s (sin jefe): %d" % (anchor.name, len(acks))
        if anchor.user_id and anchor.user_id.id == user_id:
            return "Acuses pendientes de su gente: %d (%d personas)" % (len(acks), len(people))
        return "Acuses pendientes de la gente de %s (sin usuario): %d (%d personas)" % (
            anchor.name, len(acks), len(people))

    @api.model
    def _sgi_ack_note(self, acks, ack_days, limit=20):
        rows = []
        for ack in acks[:limit]:
            doc = ack.document_id
            title = (doc.sgi_title if 'sgi_title' in doc._fields else False) or doc.name or ''
            since = fields.Datetime.context_timestamp(self, ack.create_date).date() \
                if ack.create_date else False
            rows.append(Markup("<li>%s — %s (desde el %s)</li>") % (
                ack.employee_id.name or '', title, since.strftime('%d/%m/%Y') if since else '-'))
        more = Markup("<p>Y %d más.</p>") % (len(acks) - limit) if len(acks) > limit else Markup('')
        return Markup("<p>Llevan más de %d días hábiles sin firmar de leído y entendido:</p>"
                      "<ul>%s</ul>%s") % (ack_days, Markup('').join(rows), more)

    @api.model
    def _sgi_ack_deadline(self, acks, ack_days):
        """El día en que el acuse más viejo cruzó el umbral: fijo mientras el
        grupo no cambie (el aviso no se reescribe cada día)."""
        first = min(acks.mapped('create_date'))
        start = fields.Datetime.context_timestamp(self, first).date()
        return sgi_add_business_days(self.env, start, ack_days)
```

- [x] **Step 3: `cron_documents`**, reemplazar desde `acks = self.env['sgi.document.ack'].search([` (l.~679) hasta el final del método (l.~698) por:

```python
        # 57.95.0 (K-08): un aviso por persona con usuario («acuses_propios»,
        # sobre su ficha) y uno por jefe para la gente sin usuario
        # («acuses_equipo», sobre la ficha del jefe; sin jefe, sobre la
        # persona). Antes, uno por acuse sobre el documento: con Mi
        # procedimiento publicado eran 147 (116 al Jefe MAST, 2026-10-02).
        acks = self.env['sgi.document.ack'].search([
            ('state', '=', 'pendiente'),
            ('create_date', '<', limit_date),
            ('document_id.active', '=', True),
        ], order='create_date, id')
        manager_id = self._sgi_manager_user_id()
        Ack = self.env['sgi.document.ack']
        groups = defaultdict(lambda: Ack)
        targets = {}
        for ack in acks:
            kind, anchor, user_id = self._sgi_ack_route(ack.employee_id, manager_id)
            groups[(kind, anchor.id)] |= ack
            targets[(kind, anchor.id)] = user_id

        def _group_notice(kind):
            def _notice(anchor):
                group = groups[(kind, anchor.id)]
                user_id = targets[(kind, anchor.id)]
                self._sgi_schedule(
                    anchor, self._sgi_ack_summary(kind, anchor, group, user_id),
                    self._sgi_ack_note(group, ack_days), user_id,
                    date_deadline=self._sgi_ack_deadline(group, ack_days), key=kind)
            return _notice

        Employee = self.env['hr.employee'].sudo()
        for kind in ('acuses_propios', 'acuses_equipo'):
            anchors = Employee.browse(sorted({aid for k, aid in groups if k == kind}))
            failures += self._sgi_for_each(anchors, _group_notice(kind), "acuses pendientes")
        self._sgi_sweep(['revision_bienal', 'piloto_por_vencer', 'acuses_propios', 'acuses_equipo'],
                        "el documento ya se revisó, el piloto cerró o los acuses se dieron", failures)
        # Los de uno por acuse (antes de 57.95.0) ya no se ven en ninguna corrida.
        self._sgi_sweep(['acuse_pendiente'],
                        "se reemplazó por un aviso por persona o por jefe", failures)
        return True
```

Actualizar el docstring de `cron_documents`: «… y acuses pendientes (57.95.0: un aviso por persona o por jefe, no por acuse)». `defaultdict` ya se importa (l.5).

- [x] **Step 4: Mis pendientes** (`models/sgi_my_pending.py`).

`NOTICE_MODELS` (l.~76-80), agregar al final de la tupla:

```python
                 # 57.95.0 (K-08): avisos de acuse por persona o por jefe (y
                 # los de certificaciones y formación de cron_competences).
                 'hr.employee')
```

En `_sgi_notice_covered`, junto a `indicator_acts = notices.browse()`:

```python
        # 57.95.0: «Documentos por leer y firmar» de la persona misma.
        own_ack_acts = notices.browse()
```

en el `for act in notices:` agregar la rama, antes de `elif key.startswith('capturar_indicador:')`:

```python
            elif act.sgi_cron_kind == 'acuses_propios' and act.res_model == 'hr.employee':
                own_ack_acts |= act
```

y antes de `if indicator_acts:`:

```python
        if own_ack_acts:
            emps = env['hr.employee'].sudo().browse(set(own_ack_acts.mapped('res_id'))).exists()
            emp_user = {emp.id: emp.user_id.id for emp in emps}
            for act in own_ack_acts:
                if emp_user.get(act.res_id) == act.user_id.id:
                    covered.add(act.id)
```

(La rama vieja `acuse_pendiente:` se queda: cubre los avisos de antes hasta que el barrido los cierre; `test_bandeja.test_25` la usa.)

En `action_open`, dentro de `if self.kind == 'aviso' and self.res_model == 'mail.activity':`, después del bloque de RH (`rh_empleados_incompletos`):

```python
            # 57.95.0 (K-08): el aviso de acuses de un equipo abre los acuses
            # pendientes de esa gente (Usuario SGI lee acuses; sin el grupo,
            # la ficha, como antes).
            if act and act.sgi_cron_kind == 'acuses_equipo' and act.res_model == 'hr.employee' \
                    and self.env.user.has_group('quimibond_sgi.group_sgi_user'):
                acks = self.env['sgi.cron']._sgi_ack_team_acks(
                    self.env['hr.employee'].sudo().browse(act.res_id))
                return {
                    'type': 'ir.actions.act_window', 'name': "Acuses pendientes del equipo",
                    'res_model': 'sgi.document.ack', 'view_mode': 'list,form',
                    'views': [(self.env.ref('quimibond_sgi.sgi_document_ack_view_list').id, 'list'),
                              (self.env.ref('quimibond_sgi.sgi_document_ack_view_form').id, 'form')],
                    'domain': [('id', 'in', acks.ids)], 'target': 'current'}
```

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_cron.py addons/quimibond_sgi/models/sgi_my_pending.py
git commit -m "quimibond_sgi: un aviso de acuses por persona o por jefe, no por acuse (K-08)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.4: Resumen guardado de Mis pendientes y filtros de Mi equipo (K-08)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_my_pending.py` (clase nueva; `_sgi_build` l.~600-614; `HrEmployeePublicPending` l.~868-918)

- [x] **Step 1: Clase nueva**, antes de `class SgiMyProcedurePending` (l.~805):

```python
class HrEmployeePendingSaved(models.Model):
    """57.95.0 (K-08): resumen de Mis pendientes guardado por persona. Lo
    leen los filtros de Mi equipo («Con pendientes atrasados», «por
    vencer», «Al día»), que antes armaban Mis pendientes de toda la empresa
    en cada búsqueda. Lo refrescan el respaldo nocturno (sgi.cron,
    cron_nightly_backup) y cada lista de pendientes que se abre
    (sgi.my.pending._sgi_build). Las columnas de Mi equipo siguen en vivo."""
    _inherit = 'hr.employee'

    sgi_pending_saved_total = fields.Integer(
        string="Pendientes (resumen guardado)", readonly=True, copy=False,
        help="Total de Mis pendientes de la persona la última vez que se calculó.")
    sgi_pending_saved_late = fields.Integer(
        string="Pendientes atrasados (resumen guardado)", readonly=True, copy=False)
    sgi_pending_saved_state = fields.Selection(
        PENDING_STATES, string="Semáforo (resumen guardado)", readonly=True, copy=False)

    @api.model
    def _sgi_pending_scope(self):
        """Quienes salen en Mi equipo: la empresa del SGI, con usuario o puesto."""
        company = self.env['sgi.config']._sgi_company()
        return self.sudo().search([('company_id', 'in', (company.id, False)),
                                   '|', ('user_id', '!=', False), ('sgi_mp_job_id', '!=', False)])

    @api.model
    def _sgi_save_pending_summary(self, summary):
        """{empleado.id: (total, atrasadas, peor estado)} → escribe solo lo
        que cambió. Devuelve los empleados escritos."""
        Employee = self.sudo().with_context(tracking_disable=True, mail_notrack=True)
        changed = Employee.browse()
        for emp in Employee.browse(list(summary)).exists():
            total, late, worst = summary[emp.id]
            new = {'sgi_pending_saved_total': total, 'sgi_pending_saved_late': late,
                   'sgi_pending_saved_state': worst or ('al_dia' if total else False)}
            vals = {k: v for k, v in new.items() if emp[k] != v}
            if vals:
                emp.write(vals)
                changed |= emp
        return changed

    @api.model
    def _sgi_refresh_pending_summary(self, employees=None):
        """Recalcula y guarda el resumen. Sin empleados: todo el alcance de Mi
        equipo, y quien salió de él con un resumen viejo queda en cero."""
        scope = employees.sudo() if employees else self._sgi_pending_scope()
        summary = self.env['sgi.my.pending']._sgi_summary_employees(scope)
        if not employees:
            gone = self.sudo().search([('id', 'not in', scope.ids),
                                       ('sgi_pending_saved_total', '>', 0)])
            summary.update({emp.id: (0, 0, False) for emp in gone})
        return self._sgi_save_pending_summary(summary)
```

- [x] **Step 2: `_sgi_build`**, después de `values = self._sgi_pending_values_employees(employees)`:

```python
        # 57.95.0 (K-08): lo que se acaba de calcular refresca el resumen
        # guardado que leen los filtros de Mi equipo.
        self.env['hr.employee']._sgi_save_pending_summary(
            {emp.id: self._sgi_count(values.get(emp.id, [])) for emp in employees})
```

- [x] **Step 3: Búsquedas de Mi equipo.** En `HrEmployeePublicPending`, borrar `_sgi_pending_ids_where` y reemplazar los tres `_search_*` por:

```python
    @api.model
    def _sgi_saved_ids(self, domain):
        """57.95.0 (K-08): los filtros leen el resumen guardado en
        hr.employee (sudo); ya no recalculan Mis pendientes de la empresa."""
        return self.env['hr.employee'].sudo().search(domain).ids

    @api.model
    def _sgi_saved_numeric(self, fname, operator, value):
        from .sgi_my_procedure_screen import _sgi_as_list
        if operator not in self._NUMERIC_OPS:
            raise UserError("Filtro no soportado sobre los pendientes.")
        if operator in ('in', 'not in'):
            value = _sgi_as_list(value)
        return [('id', 'in', self._sgi_saved_ids([(fname, operator, value)]))]

    @api.model
    def _search_sgi_mp_pending_total(self, operator, value):
        return self._sgi_saved_numeric('sgi_pending_saved_total', operator, value)

    @api.model
    def _search_sgi_mp_pending_late(self, operator, value):
        return self._sgi_saved_numeric('sgi_pending_saved_late', operator, value)

    @api.model
    def _search_sgi_mp_pending_state(self, operator, value):
        # Import local: cargar el módulo de la pantalla desde aquí a nivel de
        # módulo cambiaría el orden del registro (regla en CLAUDE.md).
        from .sgi_my_procedure_screen import _sgi_as_list
        values = _sgi_as_list(value)
        positive = operator in ('=', 'in')
        return [('id', 'in', self._sgi_saved_ids(
            [('sgi_pending_saved_state', 'in' if positive else 'not in', values)]))]
```

Actualizar el `help` de los tres campos: «… En los filtros se usa el resumen guardado (de la noche o de la última vez que se abrió su lista de pendientes).» `_sgi_pending_by_employee` y `_compute_sgi_mp_pending` no cambian (columnas en vivo).

- [x] **Step 4: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_my_pending.py
git commit -m "quimibond_sgi: Mi equipo filtra con un resumen guardado por persona (K-08)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.5: Respaldo nocturno de Mi procedimiento y de Mi equipo (K-08)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py` (imports; `HrEmployeeMyProcedureTab`, l.~219-290)
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (método nuevo después de `cron_documents`)
- Create: `addons/quimibond_sgi/data/sgi_nightly_cron.xml`; Modify: `addons/quimibond_sgi/__manifest__.py`

- [x] **Step 1: Logger** en `sgi_my_procedure_screen.py`, después de `from collections.abc import Iterable`:

```python
import logging
```

y después de los imports de `odoo` y `.sgi_my_procedure`:

```python
_logger = logging.getLogger(__name__)
```

- [x] **Step 2: Recálculo de respaldo**, en `HrEmployeeMyProcedureTab` después de `_sgi_mp_touch_jobs`:

```python
    _SGI_MP_LIST_FIELDS = ('sgi_mp_role_ids', 'sgi_mp_received_role_ids',
                           'sgi_mp_short_role_ids', 'sgi_mp_process_ids')

    @api.model
    def _sgi_mp_nightly_recompute(self):
        """57.95.0 (K-08): respaldo de los disparos a mano
        (``_sgi_mp_touch_jobs``). Recalcula las cuatro listas guardadas de
        todas las personas activas y compara con lo que había. En régimen da 0;
        si da más, la lista vieja se corrige aquí y el log dice cuántas y
        quiénes: falta un disparo en algún cambio de roles, actividades o
        publicación (así nació el error de 57.13.0 a 57.88.0). Devuelve los
        empleados que cambiaron."""
        fnames = self._SGI_MP_LIST_FIELDS
        employees = self.sudo().search([])

        def snapshot(emp):
            return tuple(frozenset(emp[fname].ids) for fname in fnames)

        before = {emp.id: snapshot(emp) for emp in employees}
        for fname in fnames:
            self.env.add_to_compute(self._fields[fname], employees)
        self.env.flush_all()
        employees.invalidate_recordset(list(fnames))
        changed = employees.filtered(lambda emp: snapshot(emp) != before[emp.id])
        if changed:
            changed.sgi_mp_job_id._sgi_mp_mark_dirty()
            _logger.warning(
                "SGI: el respaldo nocturno de Mi procedimiento corrigió las listas de %d de %d "
                "persona(s) %s: falta un disparo de recálculo.",
                len(changed), len(employees), changed.ids[:50])
        else:
            _logger.info("SGI: respaldo nocturno de Mi procedimiento: 0 cambios en %d personas.",
                         len(employees))
        return changed
```

- [x] **Step 3: Cron** en `sgi_cron.py`, después de `cron_documents`:

```python
    # ------------------------------------------------------------------
    # 57.95.0 (K-08) — Respaldo nocturno
    # ------------------------------------------------------------------
    @api.model
    def cron_nightly_backup(self):
        """Cron diario (02:15 de México): recalcula las cuatro listas guardadas
        de Mi procedimiento y anota en el log cuántas personas cambiaron (si no
        es 0, falta un disparo), y refresca el resumen de Mis pendientes que
        leen los filtros de Mi equipo. Cada paso en su savepoint."""
        sgi_require_system(self.env)
        Employee = self.env['hr.employee']
        self._sgi_step("respaldo de las listas de Mi procedimiento",
                       Employee._sgi_mp_nightly_recompute)
        self._sgi_step("resumen de Mis pendientes por persona",
                       Employee._sgi_refresh_pending_summary)
        return True
```

- [x] **Step 4: `data/sgi_nightly_cron.xml`**

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo noupdate="1">
    <!-- 57.95.0 (K-08): respaldo nocturno. Las cuatro listas guardadas de
         Mi procedimiento (el log dice cuántas cambiaron; en régimen, 0) y el
         resumen de Mis pendientes que leen los filtros de Mi equipo. A las
         08:15 UTC (02:15 de México): después de medianoche, para que lo que
         venció ayer ya cuente como atrasado. -->
    <record id="sgi_cron_nightly_backup" model="ir.cron">
        <field name="name">SGI: Respaldo nocturno (Mi procedimiento y Mi equipo)</field>
        <field name="model_id" ref="model_sgi_cron"/>
        <field name="state">code</field>
        <field name="code">model.cron_nightly_backup()</field>
        <field name="interval_number">1</field>
        <field name="interval_type">days</field>
        <field name="nextcall" eval="(DateTime.today() + relativedelta(days=1, hour=8, minute=15, second=0)).strftime('%Y-%m-%d %H:%M:%S')"/>
        <field name="active" eval="True"/>
    </record>
</odoo>
```

Manifest, después de `'data/sgi_floor_cron.xml',`:

```python
        # 57.95.0 (K-08): respaldo nocturno de Mi procedimiento y Mi equipo.
        'data/sgi_nightly_cron.xml',
```

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_my_procedure_screen.py addons/quimibond_sgi/models/sgi_cron.py addons/quimibond_sgi/data/sgi_nightly_cron.xml addons/quimibond_sgi/__manifest__.py
git commit -m "quimibond_sgi: respaldo nocturno de Mi procedimiento y del resumen de Mi equipo (K-08)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.6: K-05, entregas facturadas y fecha de pago

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_links.py` (`AccountMoveLink`, l.170-192)
- Modify: `addons/quimibond_sgi/models/sgi_kpi_account.py` (l.63-64)
- Modify: `addons/quimibond_sgi/views/sgi_links_views.xml` (record `sgi_account_move_view_form_links`, l.~97-110)

- [x] **Step 1: `AccountMoveLink` completo**

```python
class AccountMoveLink(models.Model):
    _inherit = 'account.move'

    sgi_picking_ids = fields.Many2many(
        'stock.picking', 'sgi_move_picking_rel', 'move_id', 'picking_id',
        string="Entregas facturadas", compute='_compute_sgi_picking_ids', store=True,
        readonly=False, copy=False,
        help="Entregas (o recepciones) que esta factura cobra (S2.08). Se proponen "
             "desde las líneas del pedido; se pueden ajustar a mano.")
    # 57.95.0 (K-05): lo propuesto separado de lo ajustado a mano.
    sgi_picking_proposed_ids = fields.Many2many(
        'stock.picking', string="Entregas propuestas", compute='_compute_sgi_picking_proposed_ids',
        help="Entregas hechas de las líneas de la factura: lo que el sistema propone.")
    sgi_picking_manual = fields.Boolean(
        string="Entregas ajustadas a mano", readonly=True, copy=False,
        help="Alguien cambió a mano las entregas facturadas: el sistema ya no las reemplaza "
             "con las propuestas. «Volver a las entregas propuestas» lo quita.")

    _SGI_PICKING_DEPENDS = ('move_type',
                            'invoice_line_ids.sale_line_ids.move_ids.picking_id.state',
                            'invoice_line_ids.purchase_line_id.move_ids.picking_id.state')

    def _sgi_proposed_pickings(self):
        self.ensure_one()
        Picking = self.env['stock.picking']
        if not self.is_invoice(include_receipts=True):
            return Picking
        lines = self.invoice_line_ids
        pickings = lines.sale_line_ids.move_ids.picking_id | lines.purchase_line_id.move_ids.picking_id
        return pickings.filtered(lambda p: p.state == 'done')

    @api.depends(*_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_proposed_ids(self):
        for move in self:
            move.sgi_picking_proposed_ids = move._sgi_proposed_pickings()

    # 57.95.0 (K-05): antes no dependía del estado de la entrega (una factura
    # hecha antes de validarla se quedaba vacía) y pisaba lo ajustado a mano.
    @api.depends('sgi_picking_manual', *_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_ids(self):
        for move in self:
            if not move.is_invoice(include_receipts=True):
                move.sgi_picking_ids = False
                continue
            proposed = move._sgi_proposed_pickings()
            # Lo ajustado a mano no se toca. Sin propuesta y con algo
            # guardado, tampoco: protege los ajustes de antes de 57.95.0, que
            # no traen la marca.
            if move.sgi_picking_manual or (not proposed and move.sgi_picking_ids):
                continue
            move.sgi_picking_ids = proposed

    # 57.95.0 (revisión del plan): ¿lo guardado difiere de lo propuesto?
    # Muestra lo propuesto y el botón también en las facturas viejas que se
    # quedaron atrás sin marca (la de proveedor de Q7).
    sgi_picking_outdated = fields.Boolean(
        string="Entregas distintas de las propuestas", compute='_compute_sgi_picking_outdated',
        help="Las entregas guardadas no son las que el sistema propone hoy.")

    @api.depends('sgi_picking_ids', *_SGI_PICKING_DEPENDS)
    def _compute_sgi_picking_outdated(self):
        for move in self:
            move.sgi_picking_outdated = bool(move.is_invoice(include_receipts=True)) \
                and move.sgi_picking_ids != move._sgi_proposed_pickings()

    def write(self, vals):
        # La marca de ajuste a mano se decide DESPUÉS de escribir y solo si lo
        # escrito difiere de lo propuesto: el formulario de la factura manda
        # ``sgi_picking_ids`` al guardar cada vez que un onchange lo recalcula
        # (Odoo 17+ envía los campos que cambió el onchange); marcarlo siempre
        # que viniera en ``vals`` congelaría facturas que nadie ajustó.
        res = super().write(vals)
        if 'sgi_picking_ids' in vals and 'sgi_picking_manual' not in vals:
            for move in self:
                manual = move.sgi_picking_ids != move._sgi_proposed_pickings()
                if move.sgi_picking_manual != manual:
                    super(AccountMoveLink, move).write({'sgi_picking_manual': manual})
        return res

    def action_sgi_picking_reset(self):
        """«Volver a las entregas propuestas»: quita el ajuste a mano y pone
        lo propuesto, aunque lo propuesto venga vacío (explícito: no depende
        de la rama que conserva lo guardado)."""
        for move in self:
            super(AccountMoveLink, move).write({
                'sgi_picking_manual': False,
                'sgi_picking_ids': [Command.set(move._sgi_proposed_pickings().ids)]})
        return True
```

(Import: `from odoo import api, fields, models` (l.17 de `sgi_links.py`) pasa a `from odoo import Command, api, fields, models`.)

Nota (1.8, VERIFY 3): el `continue` que conserva lo guardado **no es nuevo**: el cálculo de hoy ya lo hace (la rama `if pickings or not move.sgi_picking_ids` no asigna cuando no hay entregas hechas) y `StockPickingLink._compute_sgi_production_id` igual. En Odoo, `Field.compute_value` quita el campo de «por calcular» antes de llamar al cálculo, y un campo guardado que el cálculo no asigna se lee de la base. Si `test_13` mostrara lo contrario, **no** leer la relación por SQL: usar el patrón seguro, un Many2many guardado **no calculado** `sgi_picking_manual_ids` (su propia tabla) que llena `write` cuando hay ajuste, y en el cálculo `move.sgi_picking_ids = move.sgi_picking_manual_ids if move.sgi_picking_manual else (proposed or move.sgi_picking_manual_ids)`; así el cálculo siempre asigna.

- [x] **Step 2: `sgi_payment_date`** (`sgi_kpi_account.py:63-64`):

```python
    # 57.95.0 (K-05): también la fecha de factura (respaldo) y la cuenta de
    # las líneas (filtro por tipo de cuenta).
    @api.depends('payment_state', 'invoice_date', 'line_ids.account_id',
                 'line_ids.matched_debit_ids.max_date', 'line_ids.matched_credit_ids.max_date')
```

- [x] **Step 3: Vista**, en el record `sgi_account_move_view_form_links` reemplazar el `<group>` de la página «SGI»:

```xml
                    <group>
                        <field name="sgi_picking_ids" widget="many2many_tags"/>
                        <!-- 57.95.0 (K-05): lo propuesto, cuando hay ajuste a mano. -->
                        <field name="sgi_picking_manual" invisible="not sgi_picking_manual"/>
                        <field name="sgi_picking_outdated" invisible="1"/>
                        <field name="sgi_picking_proposed_ids" widget="many2many_tags"
                               invisible="not sgi_picking_manual and not sgi_picking_outdated"/>
                    </group>
                    <button name="action_sgi_picking_reset" type="object"
                            string="Volver a las entregas propuestas" class="btn-secondary"
                            invisible="not sgi_picking_manual and not sgi_picking_outdated"/>
```

- [x] **Step 4: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_links.py addons/quimibond_sgi/models/sgi_kpi_account.py addons/quimibond_sgi/views/sgi_links_views.xml
git commit -m "quimibond_sgi: entregas facturadas siguen al estado y respetan el ajuste a mano (K-05)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.7: D-06 (datos), empresa del SGI en controlados y regla en rutinas

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_document.py` (`DocumentsDocument.create` l.~1170; `write` l.~1209-1256)
- Create: `addons/quimibond_sgi/models/sgi_company_fix.py`; Modify: `models/__init__.py` (al final)
- Create: `addons/quimibond_sgi/views/sgi_company_fix_views.xml`; Modify: `__manifest__.py`
- Modify: `addons/quimibond_sgi/security/sgi_security.xml` (después de `rule_sgi_inventory_value_company`, l.~202-206), `security/ir.model.access.csv` (al final)
- Modify: `addons/quimibond_sgi/views/sgi_menus.xml` (después de `menu_sgi_config_ppap_elements`), `addons/quimibond_sgi/tools/sgi_menu_tree.txt`

- [x] **Step 1: Controlados nuevos con la empresa del SGI.** En `create`, dentro del `for vals in vals_list:` (antes de `if vals.get('sgi_state') == 'vigente' …`):

```python
            # 57.95.0 (D-06 de datos): un controlado nace con la empresa del
            # SGI (492 de 589 no la tenían el 2026-10-02), salvo que su
            # carpeta ya dé una empresa o que su familia (misma clave) viva
            # sin empresa: ver ``_sgi_family_company``.
            if vals.get('sgi_is_controlled') and not vals.get('company_id'):
                folder = self.sudo().browse(vals['folder_id']) if vals.get('folder_id') else None
                if not (folder and folder.company_id):
                    vals['company_id'] = self._sgi_family_company(vals.get('sgi_code'))
```

Método nuevo en `DocumentsDocument` (junto a `_sgi_same_code_docs`):

```python
    @api.model
    def _sgi_family_company(self, code, exclude_ids=()):
        """57.95.0 (D-06 de datos): empresa de un controlado que no trae
        empresa. Si la clave ya existe (otra revisión, una copia), la de su
        familia, aunque sea ninguna: ``_sgi_same_code_docs`` y con él
        ``_check_sgi_revision_unique`` y ``_sgi_check_revision_increases``
        comparan solo dentro de la misma empresa, y una revisión nueva con
        PNTQ dejaría de ver a sus anteriores sin empresa mientras el Jefe MAST
        no corra el asistente. Clave nueva: la empresa del SGI."""
        if code:
            family = self.sudo().with_context(active_test=False).search(
                [('sgi_code', '=', code), ('sgi_is_controlled', '=', True),
                 ('id', 'not in', list(exclude_ids))], limit=1)
            if family:
                return family.company_id.id or False
        return self.env['sgi.config']._sgi_company().id
```

En `write`, después de `res = super().write(vals)`:

```python
        if vals.get('sgi_is_controlled') and 'company_id' not in vals:
            # 57.95.0 (D-06 de datos): al volverse controlado sin empresa
            # (misma regla que create: la familia manda; si no hay, el SGI).
            for doc in self.filtered(lambda d: not d.company_id and not d.folder_id.company_id):
                company_id = self._sgi_family_company(doc.sgi_code, exclude_ids=doc.ids)
                if company_id:
                    super(DocumentsDocument, doc).write({'company_id': company_id})
```

- [x] **Step 2: `models/sgi_company_fix.py`**

```python
# -*- coding: utf-8 -*-
"""57.95.0 (auditoría 2026-10, hallazgo de datos D-06 «Datos sin compañía»;
no es la decisión D-06 de incidentes de docs/audit/decisiones.md).

Asistente del Jefe MAST que pone la empresa del SGI en los documentos
controlados sin empresa (492 el 2026-10-02). Es un cambio de datos de
negocio: no va en migración; el Jefe MAST lo corre a mano con el visto bueno
de Jose. Al abrirlo solo cuenta; «Asignar» escribe en lotes, deja una nota en
cada documento y registra antes y después en el log. Las rutinas del
procedimiento anterior toman la empresa solas (``related`` guardado).
Idempotente; nada se borra."""
import logging

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError

_logger = logging.getLogger(__name__)
BATCH = 100


class SgiCompanyFix(models.TransientModel):
    """Empresa del SGI en los documentos controlados que no tienen empresa (D-06 de datos)."""
    _name = 'sgi.company.fix'
    _description = "Empresa del SGI en documentos controlados"

    company_id = fields.Many2one('res.company', string="Empresa del SGI",
                                 compute='_compute_counts')
    doc_count = fields.Integer(string="Documentos controlados sin empresa", compute='_compute_counts')
    vigente_count = fields.Integer(string="Vigentes", compute='_compute_counts')
    obsoleto_count = fields.Integer(string="Obsoletos", compute='_compute_counts')
    other_count = fields.Integer(string="En borrador o piloto", compute='_compute_counts')
    routine_count = fields.Integer(
        string="Rutinas del procedimiento anterior que toman la empresa", compute='_compute_counts')
    done_count = fields.Integer(string="Documentos corregidos", readonly=True)
    failed_count = fields.Integer(
        string="Documentos que no se pudieron corregir", readonly=True,
        help="Se quedaron sin empresa; el motivo de cada uno está en el log del servidor.")

    @api.model
    def _sgi_check_manager(self):
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST asigna la empresa del SGI a los documentos "
                              "controlados.")

    @api.model
    def _sgi_loose_docs(self):
        return self.env['documents.document'].sudo().with_context(active_test=False).search(
            [('sgi_is_controlled', '=', True), ('company_id', '=', False)], order='id')

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_manager()
        return super().create(vals_list)

    def _compute_counts(self):
        docs = self._sgi_loose_docs()
        routines = self.env['sgi.legacy.routine'].sudo().with_context(active_test=False).search_count(
            [('procedure_id', 'in', docs.ids)]) if docs else 0
        company = self.env['sgi.config']._sgi_company()
        vigente = len(docs.filtered(lambda d: d.sgi_state == 'vigente'))
        obsoleto = len(docs.filtered(lambda d: d.sgi_state == 'obsoleto'))
        for wiz in self:
            wiz.company_id = company
            wiz.doc_count = len(docs)
            wiz.vigente_count = vigente
            wiz.obsoleto_count = obsoleto
            wiz.other_count = len(docs) - vigente - obsoleto
            wiz.routine_count = routines

    def action_view_docs(self):
        self._sgi_check_manager()
        return {
            'type': 'ir.actions.act_window', 'name': "Documentos controlados sin empresa",
            'res_model': 'documents.document', 'view_mode': 'list,form',
            'views': [(False, 'list'), (False, 'form')],
            'domain': [('id', 'in', self._sgi_loose_docs().ids)], 'target': 'current',
        }

    def action_apply(self):
        self.ensure_one()
        self._sgi_check_manager()
        company = self.env['sgi.config']._sgi_company()
        docs = self._sgi_loose_docs()
        who = self.env.user.display_name
        _logger.info("SGI D-06: antes, %d documentos controlados sin empresa: %s", len(docs), docs.ids)
        body = Markup("Empresa del SGI asignada: <b>%s</b> (antes sin empresa). "
                      "D-06, 57.95.0; la aplicó %s.") % (company.display_name, who)

        def _fix(records):
            # Escribir company_id corre _check_sgi_revision_unique (clave +
            # revisión por empresa): un controlado sin empresa que repite la
            # clave y revisión de uno de PNTQ no puede pasar.
            records.write({'company_id': company.id})
            for doc in records:
                doc.message_post(body=body, subtype_xmlid='mail.mt_note')

        done = failed = self.env['documents.document']
        for start in range(0, len(docs), BATCH):
            batch = docs[start:start + BATCH]
            try:
                with self.env.cr.savepoint():
                    _fix(batch)
                done |= batch
                continue
            except Exception:
                _logger.warning("SGI D-06: falló el lote %s; se reintenta documento por documento.",
                                batch.ids)
            # Un documento malo no detiene a los otros 99 del lote.
            for doc in batch:
                try:
                    with self.env.cr.savepoint():
                        _fix(doc)
                    done |= doc
                except Exception:
                    failed |= doc
                    _logger.exception("SGI D-06: el documento %s (%s) se queda sin empresa.",
                                      doc.id, doc.sgi_code or doc.name)
        left = len(self._sgi_loose_docs())
        _logger.info("SGI D-06: después, %d documentos con la empresa %s, %d con error %s y %d "
                     "sin empresa.", len(done), company.display_name, len(failed), failed.ids, left)
        self.write({'done_count': len(done), 'failed_count': len(failed)})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}
```

`models/__init__.py`, al final: `from . import sgi_company_fix`.

- [x] **Step 3: `views/sgi_company_fix_views.xml`**

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo>
    <!-- 57.95.0 (D-06 de datos): el Jefe MAST pone la empresa del SGI en los
         controlados sin empresa. Al abrir solo cuenta. -->
    <record id="sgi_company_fix_view_form" model="ir.ui.view">
        <field name="name">sgi.company.fix.form</field>
        <field name="model">sgi.company.fix</field>
        <field name="arch" type="xml">
            <form string="Empresa del SGI en documentos controlados">
                <sheet>
                    <div class="alert alert-info" role="alert">
                        Revise primero cuántos documentos controlados no tienen empresa.
                        «Asignar la empresa del SGI» les pone la empresa, deja una nota en el
                        historial de cada uno y no borra nada. Úselo solo con el visto bueno de
                        Dirección.
                    </div>
                    <group>
                        <field name="company_id"/>
                        <field name="doc_count"/>
                        <field name="vigente_count"/>
                        <field name="obsoleto_count"/>
                        <field name="other_count"/>
                        <field name="routine_count"/>
                        <field name="done_count" invisible="not done_count"/>
                        <field name="failed_count" invisible="not failed_count"/>
                    </group>
                </sheet>
                <footer>
                    <button name="action_apply" type="object" string="Asignar la empresa del SGI"
                            class="btn-primary" invisible="not doc_count"
                            confirm="Se pondrá la empresa del SGI en todos los documentos contados. ¿Continuar?"/>
                    <button name="action_view_docs" type="object" string="Ver los documentos"
                            invisible="not doc_count"/>
                    <button string="Cerrar" special="cancel"/>
                </footer>
            </form>
        </field>
    </record>

    <record id="sgi_company_fix_action" model="ir.actions.act_window">
        <field name="name">Empresa en documentos controlados</field>
        <field name="res_model">sgi.company.fix</field>
        <field name="view_mode">form</field>
        <field name="target">new</field>
    </record>
</odoo>
```

Manifest, antes del comentario de menús (`# menus: TODOS en un archivo y al final`):

```python
        # 57.95.0 (D-06 de datos): empresa en documentos controlados.
        'views/sgi_company_fix_views.xml',
```

- [x] **Step 4: ACL** (al final de `security/ir.model.access.csv`):

```
access_sgi_company_fix_manager,sgi.company.fix.manager,model_sgi_company_fix,group_sgi_manager,1,1,1,0
```

- [x] **Step 5: Regla**, en `security/sgi_security.xml` después de `rule_sgi_inventory_value_company`:

```xml
    <!-- 57.95.0 (D-06 de datos): sgi.legacy.routine era el único modelo
         con company_id sin regla. Con sus 815 sin empresa nadie deja de ver
         nada; cuando el Jefe MAST asigne la empresa a sus procedimientos,
         las rutinas la toman solas. -->
    <record id="rule_sgi_legacy_routine_company" model="ir.rule">
        <field name="name">SGI: Rutinas del procedimiento anterior por empresa</field>
        <field name="model_id" ref="model_sgi_legacy_routine"/>
        <field name="domain_force">['|', ('company_id', '=', False), ('company_id', 'in', company_ids)]</field>
    </record>
```

- [x] **Step 6: Menú** (`views/sgi_menus.xml`, después de `menu_sgi_config_ppap_elements`):

```xml
    <!-- 57.95.0 (D-06 de datos): asistente manual del Jefe MAST. -->
    <menuitem id="menu_sgi_config_company_fix" name="Empresa en documentos controlados"
              parent="menu_sgi_config" action="sgi_company_fix_action" sequence="90"/>
```

y en `addons/quimibond_sgi/tools/sgi_menu_tree.txt`, después de la línea de «Elementos PPAP»:

```
SGI/Administración SGI/Configuración/Empresa en documentos controlados | menu_sgi_config_company_fix | -
```

- [x] **Step 7: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_document.py addons/quimibond_sgi/models/sgi_company_fix.py addons/quimibond_sgi/models/__init__.py addons/quimibond_sgi/views/sgi_company_fix_views.xml addons/quimibond_sgi/views/sgi_menus.xml addons/quimibond_sgi/tools/sgi_menu_tree.txt addons/quimibond_sgi/security addons/quimibond_sgi/__manifest__.py
git commit -m "quimibond_sgi: empresa del SGI en controlados nuevos, regla en rutinas y asistente manual (D-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 5.8: Manuales

**Files:**
- Modify: `docs/sgi/usuarios/jefe-de-area.md` (sección 2.1), `docs/sgi/usuarios/mast.md`, `docs/sgi/administracion/manual-jefe-mast.md` (secciones 7 y 11, y una nueva para D-06)

- [x] **Step 1: Textos** («usted»):
  - **Jefe de área, 2.1:** «Si alguien de su gente sin usuario de Odoo lleva más de 7 días hábiles sin firmar un documento, le llega **un** aviso «Acuses pendientes de su gente: N» (no uno por documento). En Mis pendientes, **Ir** abre la lista de esos acuses. Los filtros de Mi equipo («Con pendientes atrasados», «por vencer», «Al día») usan el resumen de la noche o de la última vez que se abrió la lista de la persona; las cifras de cada renglón son las de este momento.»
  - **MAST (usuario):** «Los acuses pendientes ya no le llegan uno por uno: la gente con usuario recibe su aviso y la que no tiene, el de su jefe. A usted solo le llegan los equipos de jefes sin usuario y las personas sin jefe.»
  - **Manual del Jefe MAST, 7:** el párrafo de acuses (mismo texto que arriba). **11:** «Hay 27 acciones planificadas…; desde 57.95.0 incluye «SGI: Respaldo nocturno (Mi procedimiento y Mi equipo)», diario a las 02:15. Si en el log aparece «falta un disparo de recálculo», avise a quien mantiene el módulo: alguna pantalla cambió roles sin recalcular Mi procedimiento (el respaldo ya lo corrigió).» **Nueva sección «Empresa en documentos controlados»:** dónde está (Administración SGI → Configuración), que primero cuenta, que solo se aplica con el visto bueno de Dirección, qué escribe (empresa y nota; las rutinas la toman solas), que no borra nada y que se puede volver a abrir para comprobar que quedó en 0.

- [x] **Step 2: Commit**

```bash
git add docs/sgi/usuarios docs/sgi/administracion
git commit -m "docs: avisos de acuse por jefe, respaldo nocturno y empresa en controlados (57.95.0)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Migración

- **`migrations/19.0.57.95.0/pre-migrate.py` (técnica, Task 5.2):** crea `mail_activity.sgi_cron_kind` (`ADD COLUMN IF NOT EXISTS`) y la llena con `NULLIF(split_part(sgi_cron_key, ':', 1), '')` solo donde hay clave y la clase no coincide, en lotes de 5,000 por id. En producción escribe 80 filas. Idempotente; no toca datos de negocio ni `write_date`; nada se borra. Va en `pre-` y no en `post-` (la ficha decía post-migrate) porque con la columna ya creada el ORM no recalcula el campo en Python sobre las 7,260 actividades (VERIFICAR 1.8).
- **Columnas nuevas que crea el ORM, vacías:** `hr_employee.sgi_pending_saved_total/_late/_state` (se llenan en el primer respaldo nocturno o al abrir cualquier lista de pendientes; mientras, los filtros de Mi equipo salen vacíos: ver «Datos de producción» del CHANGELOG), `account_move.sgi_picking_manual` (NULL = sin ajuste a mano). `sgi_picking_proposed_ids` no se guarda. La tabla transitoria `sgi_company_fix`.
- **Registros nuevos:** el cron `sgi_cron_nightly_backup` (XML nuevo `noupdate`, se crea en la primera carga), la regla `rule_sgi_legacy_routine_company`, la acción, la vista y el menú del asistente.
- **No hay migración de datos de negocio.** D-06 (empresa en 492 documentos y 815 rutinas) lo aplica el Jefe MAST con el asistente, después del OK escrito de Jose (Q1). La factura de proveedor que quedó sin entregas (K-05) tampoco se toca (Q7).

## Task 5.9: Versión, CHANGELOG, documentación y build

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), este plan (casillas)

- [x] **Step 1: Versión** `'19.0.57.95.0'` en `__manifest__.py` (l.21).

- [x] **Step 2: CHANGELOG**, arriba de `## 19.0.57.94.1`

```markdown
## 19.0.57.95.0 — 2026-10-XX

**Rendimiento y robustez** (auditoría 2026-10: K-08, K-05, D-06 de datos; es la
ficha «57.99.0» del plan general, renumerada porque sale antes de las fichas
con puerta de decisión).

### Cambiado

- **Un aviso de acuses por persona o por jefe (K-08):** antes, uno por acuse
  pendiente sobre el documento (147 acuses el 2026-10-02; 116 avisos le
  habrían llegado al Jefe MAST la semana del 5-oct). Ahora: quien tiene
  usuario recibe «Documentos por leer y firmar: N» sobre su ficha
  (`acuses_propios`; Mis pendientes no lo repite, ya tiene sus renglones); la
  gente sin usuario, un aviso a su jefe «Acuses pendientes de su gente: N»
  (`acuses_equipo`, sobre la ficha del jefe), y al Jefe MAST solo los equipos
  de jefes sin usuario y las personas sin jefe. Vence el día en que el acuse
  más viejo cruzó el umbral. Los avisos de uno por acuse se cierran en la
  primera corrida. «Ir» abre los acuses pendientes del equipo.
- **Mis pendientes:** los avisos con clave sobre fichas de empleado
  (`hr.employee`) salen como «Aviso» (acuses, y también certificaciones y
  formación de `cron_competences`).
- **Mi equipo (K-08):** «Con pendientes atrasados», «Con pendientes por
  vencer» y «Al día» leen un resumen guardado por persona
  (`hr.employee.sgi_pending_saved_*`), que refrescan el respaldo nocturno y
  cada lista de pendientes que se abre; ya no calculan Mis pendientes de toda
  la empresa en cada búsqueda. Las columnas siguen en vivo.
- **Barrido de avisos (K-08):** por la clase indexada
  `mail.activity.sgi_cron_kind` en lugar de `sgi_cron_key =like 'clase:%'`.
- **Entregas facturadas (K-05):** `account.move.sgi_picking_ids` depende del
  estado de la entrega (una factura hecha antes de validar la entrega ya se
  llena al validarla) y no pisa lo ajustado a mano (`sgi_picking_manual`);
  lo propuesto se ve aparte (`sgi_picking_proposed_ids`) y «Volver a las
  entregas propuestas» quita el ajuste. `sgi_payment_date` depende también de
  la fecha de factura y de la cuenta de las líneas.
- **Documentos controlados (D-06 de datos):** un documento que nace o se
  vuelve controlado sin empresa toma la empresa del SGI.

### Agregado

- **Respaldo nocturno** (cron «SGI: Respaldo nocturno (Mi procedimiento y Mi
  equipo)», 02:15 de México): recalcula las cuatro listas guardadas de Mi
  procedimiento y dice en el log cuántas personas cambiaron (en régimen, 0;
  si no, «falta un disparo de recálculo» y las corrige), y refresca el resumen
  de Mi equipo.
- **«Empresa en documentos controlados»** (Administración SGI →
  Configuración, solo Jefe MAST, `sgi.company.fix`): cuenta los controlados
  sin empresa (y las rutinas que la arrastran) y, a mano, les pone la empresa
  del SGI en lotes, con una nota en cada documento y el antes y el después en
  el log. Idempotente; nada se borra.

### Seguridad

- Regla de empresa en `sgi.legacy.routine` (era el único modelo con
  `company_id` sin regla). Con las 815 rutinas sin empresa nadie deja de ver
  nada.

### Migración

`pre-migrate` técnico: crea y llena `mail_activity.sgi_cron_kind` con SQL (80
filas en producción), en lotes y de forma idempotente, sin tocar datos de
negocio. Lo demás son columnas nuevas vacías y registros nuevos (cron `noupdate`
en un XML nuevo, regla, menú).

### Datos de producción

- **D-06 no se aplica en el despliegue.** Con el visto bueno escrito de Jose
  (en el PR), el Jefe MAST abre Administración SGI → Configuración →
  «Empresa en documentos controlados», revisa que cuente 492 (482 vigentes, 9
  obsoletos, 1 borrador) y 815 rutinas, y aplica.
- Los filtros de Mi equipo salen vacíos hasta el primer respaldo nocturno (o
  hasta que cada quien abra su lista). Para tenerlos el mismo día: correr la
  acción planificada a mano (Ajustes → Técnico → Acciones planificadas →
  «SGI: Respaldo nocturno…» → Ejecutar).

**Pruebas:** `test_rendimiento_robustez` (17 casos: avisos por jefe, al Jefe
MAST, propios sin repetir en Mis pendientes, idempotencia y cierre, «Ir»,
clase indexada y barrido, filtro de Mi equipo sin recalcular la empresa,
resumen que solo escribe cambios y que se refresca al abrir la lista,
respaldo con 0 cambios en régimen y corrección con aviso en el log, solo el
sistema, entregas que siguen al estado, ajuste a mano y regreso, dependencias,
asistente de empresa, empresa en controlados nuevos y regla de rutinas);
`test_my_pending.test_02` y `test_bandeja.test_13` refrescan el resumen antes de filtrar.

**Verificación pendiente en Odoo.sh:** (1) que el ORM no recalcule
`sgi_cron_kind` con la columna ya creada; (2) el índice parcial
`btree_not_null`; (3) que el `continue` de `_compute_sgi_picking_ids`
conserve lo ajustado a mano; (4) en el log del día siguiente al despliegue,
«respaldo nocturno de Mi procedimiento: 0 cambios» o la lista de quienes
cambiaron.
```

- [x] **Step 3: Documentación técnica generada**

```bash
python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
```

(Debe aparecer el cron nuevo en `docs/sgi/tecnica/crons.md`, el modelo `sgi.company.fix` y los campos nuevos.)

- [x] **Step 4: Mapa del plan general.** En `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`, renglón «57.99.0» del mapa y título de la ficha: «57.99.0 → entregada como **57.95.0** (2026-10-XX); las fichas con puerta toman el siguiente número libre (57.96.0) al iniciarse».

- [x] **Step 5: Checadores** (sección 0, punto 4). Esperado: 0 errores (sin la advertencia de bump).

- [x] **Step 6: Commit, push y build**

```bash
git add -A addons/quimibond_sgi docs/sgi docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md
git commit -m "quimibond_sgi 19.0.57.95.0: rendimiento y robustez (K-08, K-05, D-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

Esperado en el build de la rama (`--test-tags /quimibond_sgi`): `test_rendimiento_robustez` 17/17, `test_my_pending` y `test_menu_tree` OK, y ningún fallo nuevo respecto del build de `main`. Si falla una prueba existente que filtraba Mi equipo, agregar `_sgi_refresh_pending_summary` antes del filtro (no volver al cálculo en vivo). Si falla una que creaba controlados y esperaba empresa vacía, ajustar la prueba. En el `update.log`: «SGI 57.95.0: sgi_cron_kind llenado en N actividades» y sin errores de `mail_activity__sgi_cron_kind_index`.

- [x] **Step 7: Marcar las casillas** de este plan y commit (`docs: plan 57.95.0 al día`).

- [ ] **Step 8: Verificación en Odoo.sh y en producción** (solo lectura, por MCP)
  - `ir.module.module [('name','=','quimibond_sgi')]` → `latest_version = 19.0.57.95.0`.
  - `get_fields('mail.activity', ['sgi_cron_kind'])`; `mail.activity [('sgi_cron_key','!=',False),('sgi_cron_kind','=',False),('active','in',[True,False])]` → 0.
  - Después de la primera corrida de `cron_documents` con acuses vencidos: `aggregate_records('mail.activity', ['sgi_cron_kind','user_id'], [('sgi_cron_kind','in',['acuses_propios','acuses_equipo'])])` → a lo más ~30 propios y ~16 de equipo; al Jefe MAST (uid 128) ≤ 4 de equipo. `mail.activity [('sgi_cron_kind','=','acuse_pendiente'),('active','=',True)]` → 0.
  - Después del primer respaldo nocturno: `aggregate_records('hr.employee', ['sgi_pending_saved_state'])` con renglones; en el log de Odoo.sh, la línea «respaldo nocturno de Mi procedimiento».
  - K-05: `account.move [('company_id','=',1),('sgi_picking_ids','=',False),('move_type','in',[...]),'|',('invoice_line_ids.sale_line_ids.move_ids.picking_id.state','=','done'),('invoice_line_ids.purchase_line_id.move_ids.picking_id.state','=','done'),('create_date','>=','<fecha del deploy>')]` → 0.
  - D-06, **solo después** de que el Jefe MAST aplique el asistente con el OK de Jose: `documents.document [('sgi_is_controlled','=',True),('company_id','=',False),('active','in',[True,False])]` → 0; `sgi.legacy.routine [('company_id','=',False),('active','in',[True,False])]` → 0.

---

## Preguntas para Jose (con la opción por omisión)

- **Q1. D-06: empresa en los 492 documentos controlados sin empresa (y, por arrastre, 815 rutinas).** Es un cambio de datos de negocio. **Por omisión:** no se hace en el despliegue; con su OK escrito en el PR, el Jefe MAST corre «Empresa en documentos controlados» (cuenta primero, aplica después, nota en cada documento, nada se borra). Efecto a saber: quien trabaje con otra razón social seleccionada sin PNTQ deja de ver esos documentos hasta seleccionarla (hoy 0 usuarios internos activos no tienen PNTQ entre sus empresas). Se anota en `docs/audit/decisiones.md` como «D-06 (datos)».
- **Q2. Acuses de gente sin usuario: ¿a quién?** **Por omisión:** al jefe directo si tiene usuario activo; si no (hoy 3 jefes sin usuario con 51 personas) o no hay jefe (1 persona), al Jefe MAST. Alternativa: subir por la cadena hasta el primer jefe con usuario (hoy terminaría en Dirección de Operaciones para esas 51).
- **Q3. ¿Uno por jefe o uno por documento?** La ficha permitía ambos. **Por omisión:** por jefe (~16 avisos contra ~91 por documento con lo de hoy), y uno por persona para quien tiene usuario.
- **Q4. Filtros de Mi equipo con resumen guardado.** Durante el día el filtro puede ir atrás de la columna hasta que la persona o su jefe abran la lista o pase la noche. **Por omisión:** sí; si se prefiere, las columnas también pueden leer el resumen guardado (todo igual de viejo, pero consistente).
- **Q5. Avisos sobre fichas de empleado en Mis pendientes.** Además de los acuses, entran los avisos de certificaciones y formación que ya existen (`cron_competences`). **Por omisión:** sí (D-04, todo adentro).
- **Q6. Hora del respaldo nocturno.** **Por omisión:** 02:15 de México (08:15 UTC).
- **Q7. K-05: la factura de proveedor que quedó sin entregas** aunque ya están hechas. **Por omisión:** no se recalcula por migración; se llena sola cuando cambie su entrega o con «Volver a las entregas propuestas» desde la factura (el botón sale porque lo guardado difiere de lo propuesto, `sgi_picking_outdated`).
- **Q8. Fecha.** Producción sigue en 57.90.1 (código de un aviso por acuse). Los 7 acuses del 24-sep cruzan el umbral el lunes 5-oct y los 140 del 28-sep el miércoles 7-oct: si 57.91.0 a 57.95.0 no llegan antes, el cron agenda ~147 avisos (116 al Jefe MAST), que 57.95.0 cerraría en su primera corrida. **Por omisión:** desplegar 57.91.0–57.95.0 juntas antes del 7-oct. Si no se alcanza, ¿se sube temporalmente `quimibond_sgi.doc_ack_pending_days` (parámetro del sistema; cambio de configuración que también necesita su OK)?

---

## Revisión del plan (2026-10-02, contra c791802c)

Cambios hechos a este plan por la revisión:
1. Ruta del generador: `tools/sgi_docs.py` (raíz del repo), no `addons/quimibond_sgi/tools/sgi_docs.py`.
2. `_RendimientoCase.setUpClass`: los acuses pendientes reales de la copia de producción no cruzan el umbral (si uno fallara, `failures` > 0 apaga el barrido y `test_04` caería).
3. `test_15`: los 492 controlados reales sin empresa reciben empresa por SQL dentro de la prueba; conteos exactos; `done_count`/`failed_count`.
4. `test_bandeja.test_13` también filtra Mi equipo: se ajusta como `test_my_pending.test_02` (Step 2b); texto de «esperado antes del código» corregido (dan error, no pasan).
5. K-05: la marca de ajuste a mano se decide después de escribir y solo si difiere de lo propuesto (el formulario reenvía `sgi_picking_ids` tras un onchange); «Volver a las entregas propuestas» escribe lo propuesto explícitamente; `sgi_picking_outdated` (sin guardar) muestra botón y propuesta también sin marca (Q7); nota de VERIFY 3 con el patrón seguro (Many2many manual no calculado), sin SQL; `Command` en el import; aserción nueva en `test_12`.
6. D-06 parte 1: `_sgi_family_company` (la familia de la clave manda, aunque viva sin empresa; carpeta con empresa no se toca) para no partir una familia entre PNTQ y «sin empresa» antes de correr el asistente; aserción nueva en `test_16`.
7. Asistente D-06: si un lote falla, reintento documento por documento; `failed_count` en el asistente y en el log.
8. 1.8: VERIFICAR nuevo sobre avisos en `hr.employee` asignados a usuarios sin RH (acceso y seguidores).

## Lo que se construyó distinto del plan (2026-10-02)

La entrada `## 19.0.57.95.0` de `addons/quimibond_sgi/CHANGELOG.md` describe lo
entregado. Diferencias con las tareas de arriba:

- **Anclas de los avisos de acuse (Task 5.3):** ningún aviso va sobre
  `hr.employee` (la VERIFICACIÓN de 1.8 no se pudo hacer antes de codificar).
  El propio (`acuses_propios:<empleado>`) va sobre el **documento** de su
  acuse pendiente más viejo (`sgi.document.ack` no hereda
  `mail.activity.mixin`); el de equipo (`acuses_equipo:<jefe>`) sobre el
  **departamento** del jefe; jefe con usuario sin departamento, jefe sin
  usuario o persona sin jefe → Jefe MAST. Se revisa que quien recibe lea el
  registro, se reintenta al Jefe MAST, no se dejan seguidores nuevos y sin
  Jefe MAST se conserva el aviso. `hr.employee` no entró a `NOTICE_MODELS`
  (Q5 ya no aplica). Pruebas 05b, 05c y 05d nuevas.
- **Mi equipo (Task 5.4):** los filtros leen el resumen guardado con un
  `search_read` del alcance (lo nunca calculado cuenta como cero) en lugar de
  un dominio sobre los campos; alcance por `sgi_mp_job_id`; campos solo
  sistema.
- **K-05 (Task 5.6):** patrón seguro: `sgi_picking_manual_ids` guardado y no
  calculado; el cálculo siempre asigna (ajuste → lo ajustado; sin propuesta →
  lo guardado, `_origin`, para los ajustes de antes de 57.95.0; si no, lo
  propuesto). Propuesta y comparación en sudo y por ids
  (`compute_sudo=True`). Prueba 13b nueva (21 en total).
- **D-06 (Task 5.7), revisión:** `write` filtra en sudo; `create` toma la
  carpeta de los valores o de `default_folder_id`. El resumen de Mi equipo
  pone en cero a quien salió del alcance con `total > 0`.
- **D-06 (Task 5.7):** el asistente anota con WARNING (no ERROR) los lotes y
  documentos que fallan.
- **Pruebas 11 y 17** buscaron el cron y la regla sin `env.ref` mientras los
  xmlid no existían (el checador de referencias); al final usan `env.ref`.
