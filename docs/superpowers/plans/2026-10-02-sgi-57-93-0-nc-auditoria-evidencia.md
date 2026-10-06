# Entrega 57.93.0 — NC y auditoría con evidencia (plan de implementación)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** que una NC solo cierre con una eficacia real (resultado «Eficaz», verificada en o después de su fecha, y con acción nueva si salió «No eficaz»), que una acción correctiva no se termine sin evidencia, que lo cerrado (NC, incidente, revisión, auditoría) no se reescriba, que una NC de auditoría menor o mayor siempre tenga su NC, que el programa sugerido cubra todos los procesos y que toda devolución de cliente levante su NC. Incluye lo pendiente de 57.91.0: FUNC-C13 al crear, la actividad de eficacia a quien puede cerrar y la prueba HTTP del portal del proveedor con código de error.

**Architecture:** una versión de `quimibond_sgi` (19.0.57.93.0) sobre 57.92.0. Todo el cambio vive en Python de modelos que ya existen (`quality.alert`, `sgi.action.line`, `sgi.audit`, `sgi.audit.finding`, `sgi.audit.program`, `stock.picking`, el controlador del portal), cinco campos nuevos y tres vistas propias editadas en su `<record>` (más la herencia de la ficha de Calidad, que es de otro módulo). Sin modelos nuevos y **sin migración**: el ORM crea las columnas y no hay datos que rellenar (sección «Migración»). Pruebas nuevas en dos archivos (`TransactionCase` y `HttpCase`) y ajuste de nueve pruebas existentes que cerraban NC sin resultado de eficacia.

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh), Python 3, XML de vistas y QWeb, `odoo.tests.TransactionCase` y `odoo.tests.HttpCase`. Checadores del repo: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`, `flake8`.

**Base de código verificada:** rama `claude/confident-mendel-8yg7xu`, HEAD `bf1f9892` (manifest `19.0.57.92.0`, bandeja 57.92.0 terminada; `tests/test_usted.py` ya existe y revisa toda cadena nueva: «usted» y glosario). Lo que no se pudo comprobar sin Odoo está marcado con «VERIFICAR:» (sección 1.8 y notas de las tareas). Las líneas citadas son de ese HEAD; si cambian, buscar por el nombre del método.

---

## 0. Reglas (resumen de la sección 0 del plan general)

Leer antes de empezar: `CLAUDE.md` (raíz), `addons/quimibond_sgi/README.md` (glosario y «usted»), la entrada 57.92.0 de `addons/quimibond_sgi/CHANGELOG.md`.

1. **Versión y CHANGELOG:** `__manifest__.py` a `'19.0.57.93.0'` y `## 19.0.57.93.0 — AAAA-MM-DD` arriba de la 57.92.0. `tools/check_addons.py --base-ref origin/main` lo exige.
2. **Pruebas registradas** en `tests/__init__.py` (el checker lo exige), clases con `@tagged('post_install', '-at_install')`.
3. **Las pruebas solo corren en Odoo.sh**, build de desarrollo de la rama, con `--test-tags /quimibond_sgi` (no el suite completo). El CI no instala el SGI. Ciclo: prueba → push → ver el fallo en el build → código → checadores → push → leer el build.
4. **Checadores locales** (0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   flake8 addons/quimibond_sgi
   ```
5. **Sin herencias propias** de vistas del SGI: `sgi_action_line_view_form` y `sgi_audit_program_view_form` se editan en su `<record>`. `sgi_quality_alert_view_form` hereda de `quality_control.quality_alert_view_form` (otro módulo): se edita ahí mismo, sin crear otra herencia.
6. **Menús:** esta entrega no toca menús.
7. **«Usted» y glosario:** `tests/test_usted.py` (57.92.0) revisa todas las cadenas entre comillas de `.py` y `.xml`: nada de «tú», «tu», «puedes», ni «No Conformidad» con mayúscula ni «NCs» (se escribe «las NC», «no conformidad», «Jefe MAST»).
8. **Despliegue:** PR rama → `main`, revisar el build; PR `main` → `quimibond`; `odoo-update quimibond_sgi` según `docs/RUNBOOK_DESPLIEGUE.md`.
9. **Producción es de solo lectura.** Esta entrega no cambia datos de negocio (sin migración). Si al final se decide la pregunta Q1 (NC de devolución borradas), eso sí necesita el visto bueno escrito de Jose en el PR.

---

## 1. Decisiones de diseño (con evidencia del código)

### 1.1 Eficacia con resultado (N-02, H-B1.1 y H-B1.2)

Hoy (`models/sgi_nonconformity.py`):
- Campos de eficacia: `sgi_effectiveness_note` (l.117), `sgi_effectiveness_date` (l.118), `sgi_effectiveness_by` (l.122), `sgi_effectiveness_due` (l.196, readonly, la pone `_sgi_schedule_effectiveness`).
- `_sgi_schedule_effectiveness` (l.399-429): al terminar la **última correctiva** fija `sgi_effectiveness_due = max(date_done) + N días` (parámetro `quimibond_sgi.nc_effectiveness_days`, 90) y agenda «Verificar eficacia de la NC X (a N días)» a `sgi_effectiveness_by` o al Jefe MAST. **No hace nada** si ya hay `sgi_effectiveness_due` o `sgi_effectiveness_date` (l.413).
- `_sgi_check_can_close` (l.595-636) solo pide nota y fecha (l.605-606); no compara la fecha con nada.
- La ficha (`views/sgi_nonconformity_views.xml`, record `sgi_quality_alert_view_form`, pestaña «Verificación y cierre», l.185-193) muestra nota, fecha, «verificada por» y la lección.
- Etapas (`data/sgi_stages.xml`, noupdate): `sgi_nc_int_stage_open` (Abierta), `sgi_nc_int_stage_followup` (Seguimiento), `sgi_nc_int_stage_closed` (Cerrada, `sgi_is_closing_stage`), `sgi_nc_int_stage_cancel` (Cancelada).
- El cron (`models/sgi_cron.py:532-545`) agenda «Verificar eficacia…» cuando todas las acciones terminaron, no hay fecha de eficacia y la fecha programada ya llegó (o no hay).

Decisión:
- **Campo nuevo `sgi_effective`** (Selection `eficaz` / `no_eficaz`, «Resultado de la eficacia», tracking, sin copiar). El cierre exige `sgi_effective == 'eficaz'` además de nota y fecha. Se muestra en la pestaña «Verificación y cierre» con `widget="radio"`.
- **Fecha de eficacia**: el cierre exige (a) que no sea futura, (b) `>= sgi_effectiveness_due` si hay fecha programada, y (c) `>=` la última `date_done` de las acciones de la NC. La regla (a) es necesaria: sin ella, (b) se cumple tecleando una fecha futura. El **cierre forzado** (wizard `sgi.nc.force.close`, solo Jefe MAST, con motivo) no pasa por `_sgi_check_can_close` (`write`, l.651-657): esa es la salida cuando no se puede esperar.
- **«No eficaz»**: al escribir `sgi_effective = 'no_eficaz'` (transición) el sistema, en `_sgi_on_ineffective()`:
  1. publica en el chatter la nota y la fecha de esa verificación (con `Markup`, escapadas);
  2. cierra las actividades «Verificar eficacia…» de la NC;
  3. sube `sgi_ineffective_count` (Integer nuevo, readonly) y limpia `sgi_effectiveness_note`, `sgi_effectiveness_date` y `sgi_effectiveness_due` (para que `_sgi_schedule_effectiveness` vuelva a programar cuando termine la correctiva nueva; ver l.413). **`sgi_effective` se queda en `no_eficaz`** hasta la verificación siguiente (así la ficha lo dice);
  4. regresa la NC a **Seguimiento** (`quimibond_sgi.sgi_nc_int_stage_followup`) si no está ahí;
  5. agenda «Registrar acción correctiva nueva de la NC X (no eficaz)» a quien verifica la eficacia (1.6).
- **Acción nueva**: cada `sgi.action.line` guarda en `effectiveness_round` (Integer nuevo, readonly) el `sgi_ineffective_count` de su NC al crearse. Con `sgi_ineffective_count > 0`, la NC no cierra sin una correctiva **terminada** con `effectiveness_round >= sgi_ineffective_count` (`_sgi_needs_new_corrective()`). Se usa un contador y no una fecha porque dentro de una prueba toda la transacción comparte `create_date` (la comparación por fecha fallaba siempre). Al crear esa correctiva se cierra la actividad «Registrar acción correctiva nueva…».
- **Cron**: no pide verificar eficacia mientras falte la correctiva nueva (`_sgi_needs_new_corrective()`), y asigna con el mismo método que la NC (1.6).
- **Reincidencia**: no cambia. `_compute_sgi_recurrence` cuenta NC, no verificaciones.
- **Indicador «% NC eficaces a la primera»** (solo se anota): sale de NC cerradas con `sgi_ineffective_count == 0` entre NC cerradas. Se construye en 57.98.0 (Salud del SGI).
- **Producción**: 17 NC, 0 cerradas, 14 canceladas, 3 abiertas. Ninguna tiene que rellenar `sgi_effective`; las 3 abiertas pedirán «Eficaz» al cerrarse.

### 1.2 Evidencia en acciones correctivas (N-02, H-B1.4)

Hoy `action_mark_done` (l.856-863) solo sella fecha y 100 %. La fecha también se pone a mano en la lista editable de la NC (l.131-146 de la vista) y desde la actividad espejo del chatter (`MailActivity._action_done`, l.20-37, con `sudo()`).

Decisión (el patrón más simple que ya usa el módulo):
- Dos campos en `sgi.action.line`: `evidence_note` (Text, «Evidencia») y `evidence_attachment_ids` (Many2many `ir.attachment`, tabla `sgi_action_line_evidence_rel`, «Archivos de evidencia», `widget="many2many_binary"` en la ficha). Mismo patrón que `sgi_hse_records.py:63` y `:161` (`attachment_ids` + `many2many_binary` en `views/sgi_hse_views.xml:64,210`).
- **Cuenta como evidencia** cualquiera de: nota no vacía, archivo en `evidence_attachment_ids`, o archivo en el chatter de la acción (`message_attachment_count`, la acción ya hereda `mail.thread`, l.788).
- **Cuándo se exige**: solo `action_type == 'correctiva'` (la ficha de la entrega; la auditoría proponía también preventivas: pregunta Q2). Se revisa en la **transición** a terminada (sin `date_done` → con `date_done`) por cualquier vía: `write`, `create` con fecha y la actividad del chatter. El candado vive en `write`/`create`, no solo en el botón, porque la lista editable de la NC deja poner la fecha a mano.
- **Exentos**: el superusuario (`env.user._is_superuser()`: OdooBot, crons, migraciones y el entorno de las pruebas). Se usa `_is_superuser()` y no `env.su` porque Mis pendientes y el espejo del chatter escriben con `sudo()` conservando el usuario (`sgi_my_pending.py:756`, `sgi_nonconformity.py:30`): con `env.su` esos caminos se brincarían el candado.
- **Desde el chatter**: si la acción no tiene evidencia y la persona marca hecha la actividad **con archivos adjuntos** (`attachment_ids` de `_action_done`), esos archivos se ligan como evidencia; sin archivos, `UserError` antes de cerrar nada.
- **Exención de lo ya terminado**: no hace falta filtro por fecha de la versión. El candado solo corre al pasar a terminada, así que lo terminado antes de 57.93.0 no se toca (producción: 4 `sgi.action.line`, todas terminadas). Una acción vieja que se reabra y se vuelva a terminar sí pide evidencia, y es lo correcto.
- **Acuerdos de la revisión por la dirección**: nacen como `correctiva` (`sgi_management_review.py:397-403`), así que también pedirán evidencia (pregunta Q3).

### 1.3 Lo cerrado no se reescribe (K-03, auditoría A-3)

Hoy:
- `sgi.base.mixin.write` (`models/sgi_base.py:168-188`) ya bloquea editar incidente (`_sgi_locked_states = ('cerrado',)`, `sgi_incident.py:24`), revisión (`('cerrada',)`, `sgi_management_review.py:21`) y auditoría (`('cerrada',)`, `sgi_audit.py:197`) cerrados, salvo Jefe MAST y superusuario. `quality.alert` **no** usa el mixin: `QualityAlert.write` (l.638) solo valida el **cambio** de etapa.
- `SgiActionLine.write` (l.1029) no mira el estado del origen. La regla `rule_sgi_action_line_user_own` (`security/sgi_security.xml:276`) deja al responsable o al creador quitar `date_done` de una correctiva de una NC ya cerrada.
- `SgiAuditFinding` (l.514) solo protege `unlink` (l.563-574); su `write` no; el auditor tiene ACL 1,1,1,1 (`ir.model.access.csv:42`).

Decisión (excepción común: `sgi_bypass_allowed(env)` = superusuario o Jefe MAST, `sgi_base.py:14-20`):
- **Acciones** (`sgi.action.line`): con el origen cerrado — NC en etapa de cierre, incidente `cerrado`, revisión `cerrada` — quien no es Jefe MAST no **modifica ni borra una acción terminada**, no **borra** ninguna y no **agrega** acciones. Las acciones **pendientes** de un origen cerrado sí se pueden terminar: pasa con el cierre forzado de una NC y con los acuerdos de una revisión ya cerrada (la revisión cierra sin exigir acuerdos terminados, `sgi_management_review.py:414-416`). Campos libres (se mueven solos): `message_*`, `activity_*` (incluye `activity_id`, que `_sgi_close_activity` limpia al terminar), `website_message*`, `rating_*`. La revisión se agrega en su extensión (`SgiActionLineReview`, `sgi_management_review.py:457`) sobrescribiendo `_sgi_origin_closed()`. Riesgos, AMEF, simulacros, objetivos e indicadores quedan fuera (no son de la ficha; el riesgo ya revalida su cierre al reabrir una acción, l.1047-1050).
- **La NC misma** (la auditoría A-3 lo propone y la ficha lo deja implícito al decir «con la NC cerrada»): en etapa de cierre, quien no es Jefe MAST solo puede (a) escribir campos del chatter y actividades y (b) **reabrirla** cambiando solo `stage_id` si es el dueño del proceso (`_sgi_user_can_close`, l.468-475; D-009). Cualquier otro campo → `UserError`. Es el mismo criterio de FUNC-C13 en sentido inverso.
- **Efecto colateral que se corrige**: terminar una acción pendiente de una NC cerrada (cierre forzado) llama `_sgi_schedule_effectiveness` (l.1053-1054), que escribe en la NC. Con el candado nuevo eso tronaría para un usuario normal: se filtran las NC en etapa de cierre antes de llamarlo.
- **Hallazgos** (`sgi.audit.finding`): con la auditoría `cerrada`, quien no es Jefe MAST no crea ni modifica hallazgos (el borrado ya estaba). Cubre también `_sgi_sync_finding` desde el checklist de una auditoría cerrada.
- **Vistas**: el candado es del servidor. No se ponen campos en solo lectura en esta entrega (el patrón `sgi_is_locked` del mixin no aplica a `quality.alert`); queda para 57.97.0 (Interfaz) si se pide.

### 1.4 Auditoría: NC obligatoria, programa sugerido y cobertura (N-03, H-B2.1 y H-B2.2)

Hoy (`models/sgi_audit.py`):
- `_sgi_check_can_close` (l.454-475) solo exige NC ligada en `nc_mayor` (l.460); una `nc_menor` cierra con `sin_accion` + motivo o con `mejora`. El candado corre en `write` solo si no es superusuario (l.408-414).
- `action_suggest_lines` (l.113-139) filtra `('state', 'in', ('vigente', 'piloto'))` (l.126). Estados del proceso: `borrador`, `piloto`, `vigente` (`sgi_activity_spec.py:722-726`). En producción casi todos están en borrador.
- No hay aviso de cobertura de 3 años.

Decisión:
- `nc_menor` y `nc_mayor` **exigen `alert_id`**, sin importar la disposición. «Sin acción» y «Mejora» quedan para conformidad, observación y oportunidad de mejora.
- El programa sugerido toma **todos los subprocesos activos** (`[('parent_id', '!=', False)]`, sin filtro de estado). Docstring, mensaje y `help` del botón se actualizan.
- **Cobertura de 3 años**, lo más chico y concreto: dos campos calculados sin guardar en `sgi.audit.program`, `coverage_gap_ids` (Many2many `sgi.process`) y `coverage_gap_count`: subprocesos activos sin renglón en este programa ni en los programas de los dos años anteriores. Aviso amarillo en la ficha del programa (record `sgi_audit_program_view_form`) y, al aprobar, una nota en el chatter con los procesos que faltan (no bloquea). Las normas del alcance quedan fuera (el programa no tiene normas por año completo; se anota en el CHANGELOG).

### 1.5 Devolución de cliente (N-12, H-B12.1)

Hoy `_sgi_is_customer_return` (`models/sgi_integration.py:40-49`) exige recepción (`incoming`) cuyos movimientos devuelven un movimiento de un picking `outgoing`.

**La hipótesis de la auditoría no se sostiene** (consulta de solo lectura a producción, 2026-10-02, MCP):
- Las 26 devoluciones validadas desde junio son del tipo 91 «Toluca: Devoluciones» (`code = 'incoming'`), salen de «Socios/Clientes» y su `origin_returned_move_id` es de pickings `TL/OUT/…` tipo 2 «Órdenes de entrega» (`outgoing`). El código actual **sí** las reconoce.
- Las 21 anteriores al 2026-08-21 son de antes de la fuente `devolucion_cliente` (`sgi.alert.source` id 8, creada 2026-08-21 20:27).
- De las 7 posteriores, **las 7 levantaron NC**: el chatter de TL/IN-/00180, 00182, 00185, 00186 dice «se levantó la NC NCI-2026-0140 / 0142 / 0147 / 0148», pero esas NC **ya no existen** (borradas antes de K-02; `sgi_return_alert_id` quedó vacío). Solo sobreviven NCI-2026-0206 (TL/IN-/00189) y la de TL/IN-/00191. Esto también explica parte del hueco de folios 0147–0205 (H-B1.6). Pregunta Q1.
- Movimientos hechos desde ubicación de cliente **sin** `origin_returned_move_id`: 7 en toda la historia, el último de 2023 (6 de PdV, tipo `outgoing`, y uno de recepción); ninguno desde junio.
- De paso: el mensaje del chatter sale escapado («`&lt;b&gt;NCI-2026-0206&lt;/b&gt;`»), porque `message_post` recibe un `str` (`sgi_integration.py:77-79`).

Decisión: cambiar igual la detección a `location_id.usage == 'customer'` en los movimientos hechos de una recepción (`incoming`), como pide la ficha: cubre una devolución capturada a mano (sin «Devolver») y cualquier ruta, y no agrega ruido (las órdenes de PdV son `outgoing`). La prueba de la ruta de 3 pasos se escribe para **confirmar** que ya funciona (pasa antes del código); la de la devolución a mano es la que falla antes del código. Producto y descripción de la NC se toman de los movimientos que vienen de clientes (antes, de los que tenían `origin_returned_move_id`). Se corrigen con `Markup` los dos mensajes de `sgi_integration.py` (l.77-79 y l.174-175) y el de la eficacia programada (`sgi_nonconformity.py:427`).

### 1.6 FUNC-C13 al crear y la actividad de eficacia (pendiente de 57.91.0)

- `QualityAlert.create` (l.231-258) no pasa por `_sgi_check_stage_move`: una NC con folio se crea directo en «Cerrada» (o «Cancelada», brincándose también NC-4). Decisión: **antes** de `super().create()` (para no gastar folio: la secuencia `sgi_seq_nc_internal` es estándar y un `rollback` no regresa el número), quien no es Jefe MAST ni superusuario no crea una NC en una etapa de cierre o de cancelación, ni por `stage_id` en los valores ni por `default_stage_id` del contexto (alta rápida en la columna «Cerrada» del kanban). El Jefe MAST sí (cargas históricas). Los flags `sgi_is_closing_stage`/`sgi_is_cancel_stage` solo existen en etapas del SGI, así que no afecta las alertas de piso.
- La actividad «Verificar eficacia» va a `sgi_effectiveness_by` o al Jefe MAST (l.415-416) y el cron a `sgi_effectiveness_by`, `user_id` o MAST (`sgi_cron.py:540`). Desde 57.91.0 solo cierran el Jefe MAST y el dueño del proceso: quien verificaba podía no poder cerrar. Decisión: método `_sgi_effectiveness_user_id()` = usuario activo del dueño del proceso; si no hay, el Jefe MAST (`sgi.cron._sgi_manager_user_id`, `sgi_cron.py:231`). Lo usan `_sgi_schedule_effectiveness`, el cron y la actividad de «No eficaz». `sgi_effectiveness_by` sigue siendo un dato que se captura.

### 1.7 Portal del proveedor (K-07, pendiente de 57.91.0; auditoría A-5 y §5)

Hoy `controllers/portal_nc.py` redirige con `&error=<texto de la excepción>` (l.42-46) y la plantilla (`views/sgi_portal_templates.xml:11`) pinta `error` tal cual (escapado, pero el texto lo pone quien arma el enlace). No hay `HttpCase` del portal.

Decisión: la URL lleva un **código** (`estado`, `faltan`, `otro`); el controlador lo traduce con un diccionario y un código desconocido no pinta nada. El controlador valida antes de llamar al modelo (NC que ya no espera respuesta → `estado`; causa o acción vacías → `faltan`) y cualquier otro `UserError` → `otro`. La plantilla no cambia (sigue pintando `error`, que ahora es el texto del diccionario). Prueba `HttpCase`: token inválido, token de otra NC, segundo envío, campos vacíos y texto inyectado en `error`. El token CSRF se toma del formulario de la página (no de una API interna de pruebas que no se pudo verificar fuera de Odoo.sh).

### 1.8 Lo que no se pudo verificar fuera de Odoo.sh

No hay fuentes de Odoo 19 en este entorno. Confirmar en el shell de Odoo.sh antes de empujar el código (Task 3.1, paso 4):
- VERIFICAR: `mail.activity.action_feedback(feedback=False, attachment_ids=None)` y que llama a `_action_done(feedback=..., attachment_ids=...)`: `grep -n "def action_feedback\|def _action_done" /home/odoo/src/odoo/addons/mail/models/mail_activity.py`.
- VERIFICAR: `stock.warehouse.delivery_steps` acepta `'pick_pack_ship'` y existen `pick_type_id`, `out_type_id`, `in_type_id`, `lot_stock_id`, `default_location_src_id`: `grep -n "delivery_steps\|pick_pack_ship" /home/odoo/src/odoo/addons/stock/models/stock_warehouse.py | head`.
- VERIFICAR: `HttpCase.url_open(url, data=None, ..., allow_redirects=True)`: `grep -n "def url_open" /home/odoo/src/odoo/odoo/tests/common.py`.
- VERIFICAR (bajo riesgo): que con `pick_pack_ship` `out_type_id.default_location_src_id` sea la ubicación de Salida y no `lot_stock_id` (lo asume `test_18`): `grep -n "default_location_src_id" /home/odoo/src/odoo/addons/stock/models/stock_warehouse.py | head`.
- VERIFICAR: que un `HttpCase` anónimo conserve la cookie de sesión entre el GET que lee el `csrf_token` del formulario y el POST (el opener es una `requests.Session`); y que `Location` de la redirección termine en `/my` (puede venir absoluta). Si el CSRF falla, usar `self.authenticate(None, None)` en `setUp`.
- VERIFICAR: `widget="radio"` con `options="{'horizontal': true}"` en un `<group>` de una vista heredada de Calidad (patrón estándar de Odoo; el checker local no valida formularios: revisar el `update.log`).
- VERIFICAR: que `test_candados_evidencia._closable_nc` de 57.91.0 de verdad falle hoy por H8 (crea la correctiva antes de la causa raíz); si pasa, el orden de evaluación de la restricción es otro y la reescritura de la Task 3.2 sigue siendo correcta.
- VERIFICAR: Odoo 19, el popover «Marcar hecha» de una actividad de tipo por hacer: ¿permite subir archivos? (si no, la evidencia de una correctiva se captura en la acción antes de marcar hecha la actividad; el mensaje del candado ya lo dice).
- Verificado en el repo: `res.users._is_superuser()` (lo usa ya `models/sgi_guard.py:16`; verificado en el repo).

---

## 2. Archivos

- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (campos de `quality.alert` y `sgi.action.line`; `MailActivity._action_done`; `QualityAlert.create/write/_sgi_check_can_close/_sgi_schedule_effectiveness`; métodos nuevos `_sgi_effectiveness_user_id`, `_sgi_needs_new_corrective`, `_sgi_on_ineffective`, `_sgi_check_closed_edit`; `SgiActionLine.create/write/unlink`; métodos nuevos `_sgi_origin_closed`, `_sgi_check_closed_origin`, `_sgi_needs_evidence`, `_sgi_has_evidence`, `_sgi_check_evidence`)
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (`SgiActionLineReview._sgi_origin_closed`)
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (l.538-540)
- Modify: `addons/quimibond_sgi/models/sgi_audit.py` (import; `action_approve`; `action_suggest_lines`; campos de cobertura; `_sgi_check_can_close`; `SgiAuditFinding.create/write`)
- Modify: `addons/quimibond_sgi/models/sgi_integration.py` (import; `_sgi_is_customer_return`; `_sgi_create_return_alert`; mensaje l.174)
- Modify: `addons/quimibond_sgi/controllers/portal_nc.py`
- Modify: `addons/quimibond_sgi/views/sgi_nonconformity_views.xml`, `views/sgi_action_line_views.xml`, `views/sgi_audit_views.xml`
- Create: `addons/quimibond_sgi/tests/test_nc_auditoria_evidencia.py`, `addons/quimibond_sgi/tests/test_portal_nc_http.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py`
- Modify (pruebas existentes que cierran NC): `tests/test_nc_flow.py`, `tests/test_nc_deadlines.py`, `tests/test_candados_evidencia.py`, `tests/test_pegamento.py`, `tests/test_ola_b.py`, `tests/test_ola1.py`, `tests/test_capture_reply.py`, `tests/test_pr6_external.py`
- Modify: `docs/sgi/usuarios/mast.md`, `docs/sgi/usuarios/operador-o-supervisor.md`, `docs/sgi/usuarios/auditor.md`, `docs/sgi/administracion/manual-jefe-mast.md`
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado)

---

## Task 3.0: Rama

- [x] **Step 1:** 57.93.0 va encima de 57.92.0. Si se trabaja en la sesión `claude/confident-mendel-8yg7xu`, seguir en ella. Si no: cuando 57.92.0 ya esté en `main`, `git fetch origin main && git checkout -B claude/sgi-57-93-nc-auditoria origin/main`. Confirmar `grep -n "'version'" addons/quimibond_sgi/__manifest__.py` → `19.0.57.92.0`.

## Task 3.1: Pruebas nuevas (fallan antes del código)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_nc_auditoria_evidencia.py`
- Create: `addons/quimibond_sgi/tests/test_portal_nc_http.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py` (al final)

- [x] **Step 1: Escribir `tests/test_nc_auditoria_evidencia.py`**

```python
# -*- coding: utf-8 -*-
"""57.93.0 (auditoría 2026-10: N-02, N-03, N-12, K-03 y lo que quedó de
FUNC-C13 en 57.91.0): la NC y la auditoría cierran con evidencia.

- La eficacia tiene resultado (Eficaz / No eficaz) y se registra en o después
  de la fecha programada; «No eficaz» regresa la NC a Seguimiento y pide una
  acción correctiva nueva. La verificación le llega a quien puede cerrar.
- Una acción correctiva no se termina sin evidencia.
- Una NC no nace cerrada ni cancelada.
- Con la NC, el incidente o la revisión cerrados, sus acciones terminadas y
  la NC misma solo las modifica el Jefe MAST; igual los hallazgos de una
  auditoría cerrada.
- Una NC menor o mayor de auditoría exige su NC ligada; el programa sugerido
  incluye los procesos en borrador y avisa la cobertura de 3 años.
- Toda recepción desde la ubicación de clientes levanta la NC de devolución."""
from datetime import date, timedelta

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import assert_locked, sgi_set_mast


class _NcEvidenciaCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.mast = sgi_set_mast(env, login='n02_mast')
        cls.sgi_user = new_test_user(
            env, login='n02_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner_user = new_test_user(
            env, login='n02_owner', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = env['hr.employee'].create({'name': 'N02 Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create({
            'code': 'ZN2', 'name': 'Proceso eficacia', 'owner_id': cls.owner.id})
        cls.team = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.stage_follow = env.ref('quimibond_sgi.sgi_nc_int_stage_followup')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.stage_cancel = env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')

    def _nc(self, **vals):
        return self.env['quality.alert'].create(dict({
            'title': 'N02 NC', 'team_id': self.team.id, 'stage_id': self.stage_follow.id,
            'sgi_process_id': self.process.id, 'sgi_root_cause': 'Causa N02'}, **vals))

    def _line(self, alert, **vals):
        return self.env['sgi.action.line'].create(dict({
            'alert_id': alert.id, 'name': 'Acción N02', 'action_type': 'correctiva',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today()}, **vals))

    def _eficaz(self, alert):
        alert.write({'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Sin reincidencia',
                     'sgi_effectiveness_date': date.today()})

    def _summaries(self, alert, prefix):
        return alert.activity_ids.filtered(lambda a: (a.summary or '').startswith(prefix))


@tagged('post_install', '-at_install')
class TestNcEficacia(_NcEvidenciaCase):

    def test_01_eficacia_antes_de_la_fecha_no_cierra(self):
        nc = self._nc()
        self._line(nc).action_mark_done()   # superusuario: sin candado de evidencia
        self.assertEqual(nc.sgi_effectiveness_due, date.today() + timedelta(days=90))
        self._eficaz(nc)
        with self.assertRaisesRegex(UserError, 'se programó'):
            nc.write({'stage_id': self.stage_closed.id})
        # La salida cuando no se puede esperar: cierre forzado con motivo.
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'El cliente dejó de comprar el producto'}).action_confirm()
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_02_eficaz_cierra_cuando_llega_la_fecha(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        nc.write({'sgi_effectiveness_due': date.today()})  # llegó la fecha programada
        self._eficaz(nc)
        nc.with_user(self.owner_user).write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_03_sin_resultado_no_cierra(self):
        nc = self._nc(sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=date.today())
        self._line(nc, date_done=date.today())
        with self.assertRaisesRegex(UserError, 'Eficaz'):
            nc.write({'stage_id': self.stage_closed.id})
        nc.write({'sgi_effective': 'eficaz'})
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_04_no_eficaz_regresa_y_pide_correctiva_nueva(self):
        nc = self._nc(stage_id=self.stage_open.id)
        self._line(nc).action_mark_done()
        nc.write({'sgi_effectiveness_due': date.today()})
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Volvió a pasar en el turno 3',
                  'sgi_effectiveness_date': date.today()})
        self.assertEqual(nc.stage_id, self.stage_follow)
        self.assertEqual(nc.sgi_ineffective_count, 1)
        self.assertEqual(nc.sgi_effective, 'no_eficaz')
        self.assertFalse(nc.sgi_effectiveness_note or nc.sgi_effectiveness_date or nc.sgi_effectiveness_due)
        self.assertIn('Volvió a pasar en el turno 3', "".join(nc.message_ids.mapped('body')))
        self.assertFalse(self._summaries(nc, 'Verificar eficacia'))
        todo = self._summaries(nc, 'Registrar acción correctiva nueva')
        self.assertEqual(todo.user_id, self.owner_user)
        self._eficaz(nc)
        with self.assertRaisesRegex(UserError, 'No eficaz'):
            nc.write({'stage_id': self.stage_closed.id})
        # La correctiva nueva cierra el aviso y vuelve a programar la eficacia.
        nc.write({'sgi_effectiveness_note': False, 'sgi_effectiveness_date': False})
        second = self._line(nc, name='Correctiva nueva N02')
        self.assertEqual(second.effectiveness_round, 1)
        self.assertFalse(self._summaries(nc, 'Registrar acción correctiva nueva'))
        second.action_mark_done()
        self.assertEqual(nc.sgi_effectiveness_due, date.today() + timedelta(days=90))
        nc.write({'sgi_effectiveness_due': date.today()})
        self._eficaz(nc)
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_05_mast_reabre_una_nc_cerrada_no_eficaz(self):
        nc = self._nc(sgi_effective='eficaz', sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=date.today())
        self._line(nc, date_done=date.today())
        nc.write({'stage_id': self.stage_closed.id})
        nc.with_user(self.mast).write({'sgi_effective': 'no_eficaz',
                                       'sgi_effectiveness_note': 'Reincidió en planta'})
        self.assertEqual(nc.stage_id, self.stage_follow)
        self.assertEqual(nc.sgi_ineffective_count, 1)

    def test_06_la_verificacion_le_llega_a_quien_puede_cerrar(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        self.assertEqual(self._summaries(nc, 'Verificar eficacia').user_id, self.owner_user)
        orphan = self.env['sgi.process'].create({'code': 'ZN2B', 'name': 'Proceso sin dueño'})
        nc2 = self._nc(sgi_process_id=orphan.id)
        self._line(nc2).action_mark_done()
        self.assertEqual(self._summaries(nc2, 'Verificar eficacia').user_id, self.mast)

    def test_07_nc_no_nace_cerrada_ni_cancelada(self):
        Alert = self.env['quality.alert'].with_user(self.sgi_user)
        for stage in (self.stage_closed, self.stage_cancel):
            with self.assertRaisesRegex(UserError, 'nace abierta'):
                Alert.create({'title': 'N02 nace cerrada', 'team_id': self.team.id,
                              'stage_id': stage.id})
        with self.assertRaisesRegex(UserError, 'nace abierta'):
            Alert.with_context(default_stage_id=self.stage_closed.id).create(
                {'title': 'N02 nace cerrada', 'team_id': self.team.id})
        self.assertFalse(self.env['quality.alert'].search([('title', '=', 'N02 nace cerrada')]))
        legacy = self.env['quality.alert'].with_user(self.mast).create({
            'title': 'N02 carga histórica', 'team_id': self.team.id, 'stage_id': self.stage_closed.id})
        self.assertEqual(legacy.stage_id, self.stage_closed)


@tagged('post_install', '-at_install')
class TestAccionEvidencia(_NcEvidenciaCase):

    def test_08_correctiva_sin_evidencia_no_se_termina(self):
        line = self._line(self._nc())
        with self.assertRaisesRegex(UserError, 'evidencia'):
            line.with_user(self.sgi_user).action_mark_done()
        self.assertFalse(line.date_done)
        line.with_user(self.sgi_user).write(
            {'evidence_note': 'OT-1234 firmada por el supervisor del turno'})
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_09_correccion_no_pide_evidencia(self):
        line = self._line(self._nc(), action_type='correccion')
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_10_desde_el_chatter_con_archivo(self):
        nc = self._nc()
        line = self._line(nc)
        activity = line.activity_id
        self.assertTrue(activity)
        with self.assertRaisesRegex(UserError, 'evidencia'):
            activity.with_user(self.sgi_user).action_feedback(feedback="Hecho")
        attachment = self.env['ir.attachment'].create({
            'name': 'foto_evidencia.jpg', 'raw': b'evidencia',
            'res_model': 'quality.alert', 'res_id': nc.id})
        activity.with_user(self.sgi_user).action_feedback(
            feedback="Hecho", attachment_ids=attachment.ids)
        line.invalidate_recordset()
        self.assertTrue(line.date_done)
        self.assertIn(attachment, line.evidence_attachment_ids)


@tagged('post_install', '-at_install')
class TestCerradoEsEvidencia(_NcEvidenciaCase):

    def _closed_nc(self):
        nc = self._nc(sgi_effective='eficaz', sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=date.today())
        line = self._line(nc, date_done=date.today(), evidence_note='OT-1')
        nc.write({'stage_id': self.stage_closed.id})
        return nc, line

    def test_11_nc_cerrada_solo_la_modifica_mast(self):
        nc, _line = self._closed_nc()
        assert_locked(self, nc.with_user(self.sgi_user).write, {'sgi_root_cause': 'Otra causa'})
        assert_locked(self, nc.with_user(self.sgi_user).write, {'stage_id': self.stage_follow.id})
        nc.with_user(self.sgi_user).message_post(body="Comentario después del cierre")
        nc.with_user(self.mast).write({'sgi_followup_comments': 'Nota del Jefe MAST'})
        # D-009: el dueño del proceso sí la reabre (solo la etapa).
        nc.with_user(self.owner_user).write({'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)

    def test_12_acciones_de_una_nc_cerrada(self):
        nc, line = self._closed_nc()
        assert_locked(self, line.with_user(self.sgi_user).write, {'date_done': False})
        assert_locked(self, line.with_user(self.sgi_user).write, {'name': 'Otra cosa'})
        assert_locked(self, line.with_user(self.sgi_user).unlink)
        assert_locked(self, self.env['sgi.action.line'].with_user(self.sgi_user).create, {
            'alert_id': nc.id, 'name': 'Tarde', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today()})
        line.with_user(self.mast).write({'name': 'Redacción corregida por el Jefe MAST'})

    def test_13_accion_pendiente_de_nc_forzada_se_termina(self):
        nc = self._nc()
        line = self._line(nc, action_type='correccion')
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Proveedor dado de baja'}).action_confirm()
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_14_incidente_y_revision_cerrados(self):
        incident = self.env['sgi.incident'].create({
            'name': 'N02 incidente', 'incident_type': 'casi_accidente',
            'immediate_causes': 'a', 'basic_causes': 'b', 'lack_of_control': 'c'})
        inc_line = self.env['sgi.action.line'].create({
            'incident_id': incident.id, 'name': 'Guarda en la cortadora', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today(),
            'date_done': date.today()})
        incident.write({'state': 'cerrado'})
        assert_locked(self, inc_line.with_user(self.sgi_user).write, {'date_done': False})
        review = self.env['sgi.management.review'].create({
            'period_from': date(2049, 1, 1), 'period_to': date(2049, 6, 30), 'state': 'realizada'})
        done = self.env['sgi.action.line'].create({
            'review_id': review.id, 'name': 'Acuerdo cumplido', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today(),
            'date_done': date.today()})
        pending = self.env['sgi.action.line'].create({
            'review_id': review.id, 'name': 'Acuerdo en curso', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': date.today()})
        review.write({'state': 'cerrada'})
        assert_locked(self, done.with_user(self.sgi_user).write, {'date_done': False})
        # Los acuerdos pendientes se siguen trabajando con la revisión cerrada.
        pending.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(pending.date_done)


@tagged('post_install', '-at_install')
class TestAuditoriaCierre(_NcEvidenciaCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.auditor = new_test_user(
            cls.env, login='n03_auditor', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.audited = cls.env['sgi.process'].create({'code': 'ZN3', 'name': 'Proceso auditado N03'})

    def _audit(self):
        return self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, self.audited.ids)], 'state': 'informe'})

    def test_15_nc_menor_exige_su_nc(self):
        audit = self._audit()
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'nc_menor', 'process_id': self.audited.id,
            'description': 'Registro sin firma', 'disposition': 'sin_accion',
            'reason_no_action': 'Caso aislado'})
        assert_locked(self, audit.with_user(self.auditor).action_close)
        finding.write({'disposition': 'mejora'})
        assert_locked(self, audit.with_user(self.auditor).action_close)
        finding.action_generate_nc()   # superusuario: el auditor solo lee NC
        audit.with_user(self.auditor).action_close()
        self.assertEqual(audit.state, 'cerrada')

    def test_16_observacion_sin_accion_y_hallazgo_cerrado(self):
        audit = self._audit()
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'observacion', 'process_id': self.audited.id,
            'description': 'Etiqueta borrosa', 'disposition': 'sin_accion',
            'reason_no_action': 'Se corrigió en sitio'})
        audit.with_user(self.auditor).action_close()
        self.assertEqual(audit.state, 'cerrada')
        assert_locked(self, finding.with_user(self.auditor).write, {'description': 'Otra redacción'})
        assert_locked(self, self.env['sgi.audit.finding'].with_user(self.auditor).create, {
            'audit_id': audit.id, 'finding_type': 'observacion', 'description': 'Tarde'})
        finding.with_user(self.mast).write({'description': 'Redacción corregida por el Jefe MAST'})

    def test_17_programa_sugerido_y_cobertura_de_tres_anos(self):
        Process = self.env['sgi.process']
        macro = Process.create({'code': 'ZN3M', 'name': 'Macro N03'})
        draft = Process.create({'code': 'ZN3A', 'name': 'Sub borrador N03', 'parent_id': macro.id})
        recent = Process.create({'code': 'ZN3B', 'name': 'Sub auditado en 2097', 'parent_id': macro.id})
        old = Process.create({'code': 'ZN3C', 'name': 'Sub auditado en 2096', 'parent_id': macro.id})
        Program = self.env['sgi.audit.program']
        Line = self.env['sgi.audit.program.line']
        Line.create({'program_id': Program.create({'year': 2097}).id, 'process_id': recent.id,
                     'planned_month': '3'})
        Line.create({'program_id': Program.create({'year': 2096}).id, 'process_id': old.id,
                     'planned_month': '3'})
        program = Program.create({'year': 2099})
        self.assertIn(draft, program.coverage_gap_ids)
        self.assertIn(old, program.coverage_gap_ids, "2096 queda fuera del ciclo 2097-2099.")
        self.assertNotIn(recent, program.coverage_gap_ids)
        self.assertNotIn(macro, program.coverage_gap_ids, "Solo subprocesos.")
        program.action_suggest_lines()
        self.assertIn(draft, program.line_ids.process_id, "Los procesos en borrador también se auditan.")
        self.assertNotIn(macro, program.line_ids.process_id)
        self.assertFalse(program.coverage_gap_ids & (draft | old))


@tagged('post_install', '-at_install')
class TestDevolucionCliente(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # Almacén propio con entrega en tres pasos (Pick → Empaque → Entrega).
        cls.warehouse = env['stock.warehouse'].create({
            'name': 'Almacén N12', 'code': 'N12', 'delivery_steps': 'pick_pack_ship'})
        cls.customers = env.ref('stock.stock_location_customers')
        cls.suppliers = env.ref('stock.stock_location_suppliers')
        cls.partner = env['res.partner'].create({'name': 'Cliente N12', 'is_company': True})
        cls.product = env['product.product'].create({'name': 'Tela N12', 'is_storable': False})

    def _done(self, picking_type, src, dest, qty, origin_move=False):
        # Mismo patrón que test_fase8.TestCustomerReturnNc.
        picking = self.env['stock.picking'].create({
            'partner_id': self.partner.id, 'picking_type_id': picking_type.id,
            'location_id': src.id, 'location_dest_id': dest.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': qty,
                'location_id': src.id, 'location_dest_id': dest.id,
                'origin_returned_move_id': origin_move.id if origin_move else False,
            })],
        })
        picking.action_confirm()
        picking.move_ids.write({'quantity': qty, 'picked': True})
        picking._action_done()
        self.assertEqual(picking.state, 'done')
        return picking

    def test_18_devolucion_de_una_entrega_en_tres_pasos(self):
        """Hipótesis de la auditoría (H-B12.1). Pasa ANTES del código: en
        producción las devoluciones sí apuntan al OUT (ver el plan, 1.5)."""
        wh = self.warehouse
        out_src = wh.out_type_id.default_location_src_id
        self.assertNotEqual(out_src, wh.lot_stock_id, "Con tres pasos la entrega sale de Salida.")
        out = self._done(wh.out_type_id, out_src, self.customers, 5)
        ret = self._done(wh.in_type_id, self.customers, wh.lot_stock_id, 2,
                         origin_move=out.move_ids[:1])
        self.assertTrue(ret.sgi_return_alert_id)

    def test_19_devolucion_capturada_a_mano(self):
        ret = self._done(self.warehouse.in_type_id, self.customers, self.warehouse.lot_stock_id, 3)
        alert = ret.sgi_return_alert_id
        self.assertTrue(alert, "Sin «Devolver» también es devolución: viene de Clientes.")
        self.assertEqual(alert.product_id, self.product)
        self.assertEqual(alert.sgi_origin_type, 'reclamacion')
        self.assertIn('<b>%s</b>' % alert.sgi_folio, "".join(ret.message_ids.mapped('body')))

    def test_20_recepcion_de_proveedor_no_es_devolucion(self):
        rec = self._done(self.warehouse.in_type_id, self.suppliers, self.warehouse.lot_stock_id, 3)
        self.assertFalse(rec.sgi_return_alert_id)
```

Notas para quien ejecuta:
- VERIFICAR: el conteo «20 casos» del CHANGELOG cuando el build los liste (aquí: TestNcEficacia 7, TestAccionEvidencia 3, TestCerradoEsEvidencia 4, TestAuditoriaCierre 3, TestDevolucionCliente 3).
- `nc.write({'sgi_effectiveness_due': ...})` funciona aunque el campo sea `readonly` en la vista: el `readonly` del campo Python solo lo hace de solo lectura en la interfaz. Simula «llegó la fecha».
- `assert_locked` (`tests/common_users.py:17-23`) exige `UserError` que **no** sea `AccessError`: confirma que lo detiene el candado y no un permiso. `sgi.action.line` no da borrado a Usuario SGI (`ir.model.access.csv:11`, 1,1,1,0), por eso el candado de `unlink` va **antes** de `super().unlink()`.
- La revisión de `test_14` usa `action_type='correccion'` a propósito para no mezclar con el candado de evidencia (que se prueba en `TestAccionEvidencia`).

- [x] **Step 2: Escribir `tests/test_portal_nc_http.py`**

```python
# -*- coding: utf-8 -*-
"""57.93.0 (K-07, lo que quedó de 57.91.0): el portal del proveedor probado
por HTTP. Un token inválido o de otra NC lleva a /my sin tocar nada; un
segundo envío no reescribe la respuesta; la URL lleva un código de error y
no texto (un enlace armado no pone texto en una página de Quimibond)."""
import re
from urllib.parse import quote

from odoo.tests import HttpCase, tagged

from .common_users import sgi_set_mast

CSRF = re.compile(r'name="csrf_token" value="([^"]+)"')


@tagged('post_install', '-at_install')
class TestPortalNcHttp(HttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_set_mast(cls.env, login='k07_mast')  # a quién avisa la respuesta
        team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        supplier = cls.env['res.partner'].create({
            'name': 'Proveedor portal K07', 'is_company': True, 'supplier_rank': 1,
            'email': 'calidad@k07.test'})
        cls.nc = cls.env['quality.alert'].create({
            'title': 'K07 hilo fuera de especificación', 'team_id': team.id,
            'partner_id': supplier.id})
        cls.nc.action_sgi_send_to_supplier()
        cls.other = cls.env['quality.alert'].create({
            'title': 'K07 otra NC', 'team_id': team.id, 'partner_id': supplier.id})
        cls.other.action_sgi_send_to_supplier()

    def _get(self, alert, token, extra=''):
        return self.url_open('/my/nc/%d?access_token=%s%s' % (alert.id, token, extra),
                             allow_redirects=False)

    def _csrf(self):
        # El token CSRF sale del formulario real (misma sesión del opener).
        page = self._get(self.other, self.other.access_token)
        self.assertEqual(page.status_code, 200)
        return CSRF.search(page.text).group(1)

    def _post(self, alert, token, cause='Lote mezclado', action='Segregar y reponer'):
        return self.url_open('/my/nc/%d/answer' % alert.id, data={
            'csrf_token': self._csrf(), 'access_token': token,
            'cause': cause, 'action': action}, allow_redirects=False)

    def _location(self, response):
        self.assertIn(response.status_code, (301, 302, 303))
        return response.headers['Location']

    def test_01_token_invalido(self):
        self.assertTrue(self._location(self._get(self.nc, 'token-falso')).endswith('/my'))
        self.assertTrue(self._location(self._post(self.nc, 'token-falso')).endswith('/my'))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'enviada')

    def test_02_token_de_otra_nc(self):
        self.assertTrue(self._location(self._get(self.nc, self.other.access_token)).endswith('/my'))
        self._post(self.nc, self.other.access_token)
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'enviada')

    def test_03_respuesta_y_segundo_envio(self):
        token = self.nc.access_token
        self.assertIn('saved=1', self._location(self._post(self.nc, token)))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'contestada')
        self.assertIn('error=estado', self._location(
            self._post(self.nc, token, cause='Otra causa inventada')))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_cause, 'Lote mezclado')

    def test_04_codigo_de_error_y_no_texto(self):
        token = self.nc.access_token
        self.assertIn('error=faltan', self._location(self._post(self.nc, token, cause='   ')))
        page = self._get(self.nc, token, '&error=faltan')
        self.assertIn('La causa y la acción son obligatorias', page.text)
        page = self._get(self.nc, token, '&error=' + quote('Llame al 555-0000 para cobrar'))
        self.assertEqual(page.status_code, 200)
        self.assertNotIn('555-0000', page.text)
```

- [x] **Step 3: Registrar** al final de `tests/__init__.py`:

```python
from . import test_nc_auditoria_evidencia
from . import test_portal_nc_http
```

- [x] **Step 4: Confirmar en el shell de Odoo.sh lo de la sección 1.8** (firmas de `action_feedback`, `delivery_steps`, `url_open`). Si algo difiere, ajustar la prueba antes de empujar.

- [x] **Step 5: Push solo de las pruebas y verlas fallar**

```bash
python3 tools/check_addons.py --base-ref origin/main
flake8 addons/quimibond_sgi/tests/test_nc_auditoria_evidencia.py addons/quimibond_sgi/tests/test_portal_nc_http.py
git add addons/quimibond_sgi/tests/test_nc_auditoria_evidencia.py addons/quimibond_sgi/tests/test_portal_nc_http.py addons/quimibond_sgi/tests/__init__.py
git commit -m "quimibond_sgi: pruebas de NC y auditoría con evidencia (fallan antes del código)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

`check_addons` avisará «archivos cambiados sin bump» (advertencia, no error): la versión sube en la Task 3.10.

Esperado en el log (`--test-tags /quimibond_sgi`): errores en `TestNcEficacia` (campo `sgi_effective` no existe), `TestAccionEvidencia`, `TestCerradoEsEvidencia`, `TestAuditoriaCierre` 15 y 17, `TestDevolucionCliente.test_19` y `TestPortalNcHttp` 03 y 04. **Pasan antes del código:** `test_18` (confirma 1.5: la ruta de tres pasos no era la causa), `test_20`, `TestPortalNcHttp` 01 y 02 (el control de token ya existía), `test_16` hasta el `assert_locked` del hallazgo. Si `test_18` falla, la hipótesis de la auditoría sí aplica en Odoo 19 y el cambio de 1.5 la cubre igual: anotarlo en el CHANGELOG.

## Task 3.2: Eficacia con resultado, fecha y «No eficaz» (N-02) y actividad a quien cierra

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py`
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (l.538-540)
- Modify: `addons/quimibond_sgi/views/sgi_nonconformity_views.xml`
- Modify (pruebas existentes): `tests/test_nc_flow.py`, `tests/test_nc_deadlines.py`, `tests/test_candados_evidencia.py`, `tests/test_pegamento.py`, `tests/test_ola_b.py`, `tests/test_ola1.py`, `tests/test_capture_reply.py`

- [x] **Step 1: Campos nuevos en `QualityAlert`**, después de `sgi_effectiveness_by` (l.122-123)

```python
    # 57.93.0 (N-02): la verificación de eficacia tiene resultado. Solo
    # «Eficaz» deja cerrar; «No eficaz» regresa la NC a Seguimiento y pide
    # una acción correctiva nueva (_sgi_on_ineffective).
    sgi_effective = fields.Selection([
        ('eficaz', "Eficaz"),
        ('no_eficaz', "No eficaz"),
    ], string="Resultado de la eficacia", tracking=True, copy=False,
        help="Resultado de la verificación de eficacia. La NC solo cierra con «Eficaz». «No eficaz» la "
             "regresa a Seguimiento y pide una acción correctiva nueva.")
    sgi_ineffective_count = fields.Integer(
        string="Verificaciones no eficaces", readonly=True, copy=False,
        help="Veces que la verificación de eficacia salió «No eficaz». Cero al cerrar = eficaz a la primera.")
```

- [x] **Step 2: Métodos de apoyo**, junto a `_sgi_schedule_effectiveness` (antes de l.399)

```python
    def _sgi_effectiveness_user_id(self):
        """57.93.0 (FUNC-C13): la eficacia la verifica quien puede cerrar la
        NC: el usuario activo del dueño del proceso; sin él, el Jefe MAST."""
        self.ensure_one()
        owner_user = self.sudo().sgi_process_id.owner_id.user_id
        if owner_user and owner_user.active:
            return owner_user.id
        return self.env['sgi.cron']._sgi_manager_user_id()

    def _sgi_needs_new_corrective(self):
        """57.93.0 (N-02): tras un «No eficaz», ¿falta la acción correctiva
        nueva terminada? Nueva = registrada en la ronda actual
        (effectiveness_round >= sgi_ineffective_count)."""
        self.ensure_one()
        if not self.sgi_ineffective_count:
            return False
        return not self.sgi_action_line_ids.filtered(
            lambda l: l.action_type == 'correctiva' and l.date_done
            and l.effectiveness_round >= self.sgi_ineffective_count)

    def _sgi_on_ineffective(self):
        """57.93.0 (N-02): la verificación salió «No eficaz». Deja la
        verificación en el historial, la limpia para la siguiente, regresa la
        NC a Seguimiento y pide la acción correctiva nueva."""
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        Cron = self.env['sgi.cron']
        for alert in self:
            folio = alert.sgi_folio or alert.name
            alert.message_post(body=Markup(
                "<b>Verificación de eficacia: no eficaz</b> (%s).<br/>%s") % (
                    alert.sgi_effectiveness_date or fields.Date.context_today(alert),
                    alert.sgi_effectiveness_note or ''))
            alert.activity_ids.sudo().filtered(
                lambda a: (a.summary or '').startswith("Verificar eficacia")).action_feedback(
                feedback="No eficaz: se pidió una acción correctiva nueva.")
            vals = {
                'sgi_ineffective_count': alert.sgi_ineffective_count + 1,
                'sgi_effectiveness_note': False,
                'sgi_effectiveness_date': False,
                'sgi_effectiveness_due': False,
            }
            if followup and alert.stage_id != followup and not alert.stage_id.sgi_is_cancel_stage:
                vals['stage_id'] = followup.id
            alert.write(vals)
            Cron._sgi_schedule(
                alert, "Registrar acción correctiva nueva de la NC %s (no eficaz)" % folio,
                "La verificación de eficacia salió «No eficaz». Revise la causa raíz y registre "
                "una acción correctiva nueva; la NC no cierra sin ella.",
                alert._sgi_effectiveness_user_id())
```

- [x] **Step 3: `_sgi_schedule_effectiveness`** (l.399-429) completo

```python
    def _sgi_schedule_effectiveness(self):
        """Al terminar la última acción correctiva: fecha de verificación a N
        días (parámetro) y actividad a quien puede cerrar la NC (57.93.0:
        dueño del proceso o Jefe MAST, antes «verificada por»)."""
        Param = self.env['ir.config_parameter'].sudo()
        try:
            days = int(Param.get_param('quimibond_sgi.nc_effectiveness_days', 90) or 90)
        except (TypeError, ValueError):
            days = 90
        Cron = self.env['sgi.cron']
        for alert in self:
            corrective = alert.sgi_action_line_ids.filtered(lambda l: l.action_type == 'correctiva')
            if not corrective or any(not l.date_done for l in corrective):
                continue
            if alert.sgi_effectiveness_due or alert.sgi_effectiveness_date:
                continue
            last = max(corrective.mapped('date_done'))
            due = last + relativedelta(days=days)
            alert.sgi_effectiveness_due = due
            user_id = alert._sgi_effectiveness_user_id()
            summary = "Verificar eficacia de la NC %s (a %d días)" % (alert.sgi_folio or alert.name, days)
            if user_id and not Cron._sgi_activity_exists(alert, summary, user_id):
                alert.activity_schedule(
                    'mail.mail_activity_data_todo', summary=summary,
                    note="La última acción correctiva terminó el %s. Verifique la eficacia el %s o "
                         "después y registre el resultado («Eficaz» o «No eficaz»), la nota y la "
                         "fecha en la pestaña Verificación y cierre; la NC solo cierra con "
                         "«Eficaz»." % (last, due),
                    user_id=user_id, date_deadline=due)
            alert.message_post(body=Markup(
                "Verificación de eficacia programada para el <b>%s</b>.") % due)
```

- [x] **Step 4: Candados nuevos en `_sgi_check_can_close`**, después del bloque de nota y fecha (l.605-606)

```python
            # 57.93.0 (N-02): resultado y fecha de la eficacia.
            if alert.sgi_effective != 'eficaz':
                problems.append("• Falta el resultado de la verificación de eficacia: «Eficaz» "
                                "(pestaña Verificación y cierre).")
            eff_date = alert.sgi_effectiveness_date
            if eff_date:
                if eff_date > fields.Date.context_today(alert):
                    problems.append("• La fecha de eficacia (%s) no puede ser futura." % eff_date)
                due = alert.sgi_effectiveness_due
                if due and eff_date < due:
                    problems.append(
                        "• La eficacia se programó para el %s y se registró el %s: verifíquela en "
                        "esa fecha o después, o pida al Jefe MAST un cierre forzado con motivo."
                        % (due, eff_date))
                done_dates = [d for d in alert.sgi_action_line_ids.mapped('date_done') if d]
                if done_dates and eff_date < max(done_dates):
                    problems.append(
                        "• La eficacia (%s) se registró antes de que terminara la última acción (%s)."
                        % (eff_date, max(done_dates)))
            if alert._sgi_needs_new_corrective():
                problems.append("• La verificación anterior salió «No eficaz»: registre y termine "
                                "una ACCIÓN CORRECTIVA nueva.")
```

- [x] **Step 5: «No eficaz» en `QualityAlert.write`** (l.638). Al inicio, junto a `newly_mayor`:

```python
        newly_ineffective = self.browse()
        if vals.get('sgi_effective') == 'no_eficaz':
            newly_ineffective = self.filtered(
                lambda a: a.sgi_folio and a.sgi_effective != 'no_eficaz')
```

y justo después de `res = super().write(vals)` (l.668):

```python
        if newly_ineffective:
            newly_ineffective._sgi_on_ineffective()
```

- [x] **Step 6: Ronda de eficacia en `SgiActionLine`.** Campo, después de `activity_id` (l.837-838):

```python
    # 57.93.0 (N-02): ronda de verificación de eficacia de su NC al crearse.
    effectiveness_round = fields.Integer(
        string="Ronda de eficacia", readonly=True, copy=False, default=0,
        help="Veces que la eficacia de la NC había salido «No eficaz» cuando se registró la acción. "
             "Tras un «No eficaz» la NC pide una correctiva de la ronda nueva.")
```

`SgiActionLine.create` (l.1023-1027) queda así:

```python
    @api.model_create_multi
    def create(self, vals_list):
        Alert = self.env['quality.alert']
        prepared = []
        for vals in vals_list:
            vals = self._sgi_done_vals(vals)
            if vals.get('alert_id') and 'effectiveness_round' not in vals:
                vals = dict(vals, effectiveness_round=Alert.browse(vals['alert_id']).sudo()
                            .sgi_ineffective_count)
            prepared.append(vals)
        lines = super().create(prepared)
        lines._sgi_sync_activity()
        # 57.93.0 (N-02): la correctiva nueva atiende el aviso del «No eficaz».
        for line in lines.filtered(lambda l: l.action_type == 'correctiva' and l.alert_id
                                   and l.effectiveness_round
                                   and l.effectiveness_round >= l.alert_id.sgi_ineffective_count):
            line.alert_id.activity_ids.sudo().filtered(
                lambda a: (a.summary or '').startswith("Registrar acción correctiva nueva")
            ).action_feedback(feedback="Se registró la acción correctiva «%s»." % line.name)
        return lines
```

(Las Tasks 3.3 y 3.5 agregan a este mismo `create` el candado de evidencia y el de origen cerrado; ver el método final en la Task 3.5.)

- [x] **Step 7: El cron** (`models/sgi_cron.py:538-540`)

```python
            if all_done and not alert.sgi_effectiveness_date and (not due or due <= today) \
                    and not alert._sgi_needs_new_corrective():
                # 57.93.0 (FUNC-C13): a quien puede cerrar la NC.
                user_id = alert._sgi_effectiveness_user_id()
```

(El resto del bloque no cambia. Tras un «No eficaz» sin correctiva nueva, el aviso «Registrar acción correctiva nueva…» ya está agendado; el cron no pide verificar algo que no ha cambiado.)

- [x] **Step 8: Vista** (`views/sgi_nonconformity_views.xml`, record `sgi_quality_alert_view_form`)

Aviso de arriba (l.33-37), texto:

```xml
                        <i class="fa fa-thumb-tack" title="NC del SGI"/> <b>NC del SGI</b> — folio
                        <field name="sgi_folio" readonly="1" class="oe_inline fw-bold"/>.
                        Para cerrarla: causa raíz, acciones terminadas (las correctivas, con
                        evidencia) y verificación de eficacia «Eficaz» en la fecha programada o
                        después (las mayores además piden los 5 porqués y la lección aplicada).
                        La cierran el dueño del proceso o el Jefe MAST.
```

Grupo «Eficacia» (l.187-193):

```xml
                        <group string="Eficacia">
                            <field name="sgi_effective" widget="radio" options="{'horizontal': true}"/>
                            <field name="sgi_effectiveness_note"/>
                            <field name="sgi_effectiveness_date"/>
                            <field name="sgi_effectiveness_by"/>
                            <field name="sgi_ineffective_count" invisible="not sgi_ineffective_count"/>
                            <field name="sgi_lesson_captured"
                                   invisible="sgi_classification != 'mayor'"/>
                        </group>
```

- [x] **Step 9: Adaptar las pruebas existentes que cierran una NC** (sin resultado «Eficaz» ya no cierran; con la correctiva terminada antes de la eficacia, la fecha queda programada a 90 días)

| Archivo | Dónde | Cambio |
|---|---|---|
| `tests/test_nc_flow.py` | `test_02_close_lock`, `alert.write({...})` (l.71-75) | agregar `'sgi_effective': 'eficaz'` |
| `tests/test_pegamento.py` | `alert.write({...})` (l.31-39) | agregar `'sgi_effective': 'eficaz'` |
| `tests/test_ola_b.py` | `_ready_major_nc` (l.78-86) y `test_02_minor_does_not_need_lesson` (l.103-109) | agregar `'sgi_effective': 'eficaz'` al `create` |
| `tests/test_ola1.py` | `_mayor_ready` (l.71-74), `TestOla1Links._mayor` (l.117-124), `test_05` (l.233-234) y `test_06` (l.254-255) | agregar `sgi_effective='eficaz'` / `'sgi_effective': 'eficaz'` |
| `tests/test_capture_reply.py` | `_closable` (l.94-97) | `'sgi_effective': 'eficaz'` y `'sgi_effectiveness_date': fields.Date.context_today(self)` en lugar de `date(2046, 4, 1)` (ya no se acepta fecha futura; la acción termina hoy) |
| `tests/test_candados_evidencia.py` | `_closable_nc` (l.64-76) | reescribir (abajo) |
| `tests/test_nc_deadlines.py` | `test_04_eficacia_programada_y_cierre` (l.116-134) | reescribir (abajo) |

`test_candados_evidencia._closable_nc`: la causa raíz y la eficacia van al crear la NC. Así la correctiva no queda programada a 90 días y, además, se respeta H8 (sin causa raíz no hay correctiva, `_sgi_check_root_cause_before_capa`, l.917-934; el orden actual crea la correctiva antes de la causa raíz y debería fallar con `ValidationError`: confirmarlo en el log de 57.91.0).

```python
    def _closable_nc(self):
        # La segunda NC del mismo proceso sale reincidente y pide una acción
        # CORRECTIVA terminada (_sgi_check_can_close): se registra correctiva.
        # 57.93.0: causa raíz y eficacia «Eficaz» al crear; si la correctiva
        # terminara antes, la eficacia quedaría programada a 90 días.
        alert = self.env['quality.alert'].create({
            'title': 'K01 NC', 'team_id': self.team_int.id,
            'sgi_process_id': self.process.id, 'sgi_root_cause': 'Causa K01',
            'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Eficaz',
            'sgi_effectiveness_date': date.today()})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Corregir K01', 'responsible_id': self.env.user.id,
            'action_type': 'correctiva',
            'date_commit': date.today(), 'date_done': date.today(), 'progress': '100'})
        return alert
```

`test_nc_deadlines.test_04_eficacia_programada_y_cierre`:

```python
    def test_04_eficacia_programada_y_cierre(self):
        nc = self._nc(sgi_root_cause='causa')
        line = self.env['sgi.action.line'].create({
            'alert_id': nc.id, 'action_type': 'correctiva', 'name': 'Capacitar',
            'responsible_id': self.user.id, 'date_commit': date.today()})
        self.assertFalse(nc.sgi_effectiveness_due)
        line.action_mark_done()
        self.assertEqual(nc.sgi_effectiveness_due, date.today() + timedelta(days=90))
        # 57.93.0 (FUNC-C13): la verificación va al dueño del proceso, que puede cerrar.
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'quality.alert'), ('res_id', '=', nc.id),
            ('summary', 'ilike', 'eficacia'), ('user_id', '=', self.owner_user.id)])
        self.assertTrue(acts)
        self.assertEqual(acts[0].date_deadline, nc.sgi_effectiveness_due)
        with self.assertRaises(UserError):
            nc.write({'stage_id': self.stage_closed.id})
        nc.write({'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Sin reincidencia',
                  'sgi_effectiveness_date': date.today()})
        # 57.93.0 (N-02): antes de la fecha programada no cierra.
        with self.assertRaises(UserError):
            nc.write({'stage_id': self.stage_closed.id})
        nc.write({'sgi_effectiveness_due': date.today()})  # llegó la fecha
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)
```

Después del cambio, revisar que ninguna otra prueba cierre una NC: `grep -rn "stage_closed\|sgi_nc_int_stage_closed\|sgi_is_closing_stage', '=', True" addons/quimibond_sgi*/tests/*.py`. Las que esperan `UserError` (`test_flows_48.test_07`, `test_ola1.test_06/07`, `test_audit_hardening` A3) siguen fallando por su motivo original; `test_audit_hardening.py:171` crea una NC cerrada como superusuario (permitido, 1.6).

- [x] **Step 10: Checadores** (sección 0, punto 4). Esperado: 0 errores.

- [x] **Step 11: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/models/sgi_cron.py addons/quimibond_sgi/views/sgi_nonconformity_views.xml addons/quimibond_sgi/tests
git commit -m "quimibond_sgi: eficacia con resultado, en su fecha y «No eficaz» que pide correctiva nueva (N-02)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.3: Evidencia obligatoria en acciones correctivas (N-02, H-B1.4)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py`
- Modify: `addons/quimibond_sgi/views/sgi_action_line_views.xml`, `views/sgi_nonconformity_views.xml`

- [x] **Step 1: Campos en `SgiActionLine`**, después de `progress` (l.~824)

```python
    # 57.93.0 (N-02, H-B1.4): una correctiva se termina con evidencia.
    evidence_note = fields.Text(
        string="Evidencia",
        help="Qué demuestra que la acción se hizo: número de orden, documento, registro o foto. Una "
             "acción correctiva no se termina sin evidencia (esta nota o un archivo).")
    evidence_attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_action_line_evidence_rel', 'line_id', 'attachment_id',
        string="Archivos de evidencia",
        help="Fotos, registros o documentos que demuestran la acción.")
```

- [x] **Step 2: Métodos del candado**, junto a `_sgi_check_done` (l.892)

```python
    def _sgi_needs_evidence(self):
        """57.93.0 (N-02): las correctivas piden evidencia al terminarse."""
        self.ensure_one()
        return self.action_type == 'correctiva'

    def _sgi_has_evidence(self):
        self.ensure_one()
        return bool((self.evidence_note or '').strip() or self.evidence_attachment_ids
                    or self.sudo().message_attachment_count)

    def _sgi_check_evidence(self):
        """57.93.0 (N-02, H-B1.4): una acción correctiva no se da por
        terminada sin evidencia. Solo el sistema (superusuario) queda exento;
        un sudo() que conserva al usuario no (Mis pendientes, el chatter)."""
        if self.env.user._is_superuser():
            return
        missing = self.filtered(
            lambda l: l.date_done and l._sgi_needs_evidence() and not l._sgi_has_evidence())
        if missing:
            raise UserError(
                "Una acción correctiva no se da por terminada sin evidencia. Abra la acción y "
                "escriba en «Evidencia» qué lo demuestra (orden, documento, registro) o adjunte "
                "el archivo: %s" % ", ".join(missing.mapped('name')))
```

- [x] **Step 3: En `write`** (l.1029), al inicio, después de `vals = self._sgi_done_vals(vals)`:

```python
        finishing = self.filtered(lambda l: not l.date_done) if vals.get('date_done') \
            else self.browse()
```

y justo después de `res = super().write(vals)`:

```python
        finishing._sgi_check_evidence()
```

En `create`, después de `lines = super().create(prepared)`:

```python
        lines.filtered('date_done')._sgi_check_evidence()
```

- [x] **Step 4: Desde el chatter** (`MailActivity._action_done`, l.20-37). Entre la búsqueda de `lines` y `super()`:

```python
        # 57.93.0 (N-02): la actividad espejo de una correctiva sin evidencia
        # solo se marca hecha con archivos; esos archivos quedan como evidencia.
        if lines and not self.env.user._is_superuser():
            missing = lines.filtered(lambda l: l._sgi_needs_evidence() and not l._sgi_has_evidence())
            if missing and not attachment_ids:
                raise UserError(
                    "La acción correctiva «%s» necesita evidencia: adjunte el archivo al marcar "
                    "hecha la actividad, o capture la evidencia en la acción."
                    % ", ".join(missing.mapped('name')))
            if missing:
                missing.write({'evidence_attachment_ids': [(4, att_id) for att_id in attachment_ids]})
```

(`lines` viene con `sudo()` y conserva al usuario: el `write` posterior de `date_done` vuelve a pasar por `_sgi_check_evidence` y ya encuentra la evidencia.)

- [x] **Step 5: Vistas**

`views/sgi_action_line_views.xml`, record `sgi_action_line_view_form`, después del `<group>` de «Compromiso»/«Ejecución» (l.~124) y antes del `<group invisible="1">`:

```xml
                    <group string="Evidencia">
                        <field name="evidence_note"
                               placeholder="Qué demuestra que se hizo: orden, documento, registro…"/>
                        <field name="evidence_attachment_ids" widget="many2many_binary"/>
                    </group>
```

En el mismo archivo, el `confirm` de los dos botones «Terminar» (lista l.~33 y ficha l.~103):

```
confirm="Se sellará la fecha de hoy como terminación y el avance al 100%. Si es una acción correctiva, capture antes la evidencia. ¿La acción está realmente terminada?"
```

`views/sgi_nonconformity_views.xml`, lista de acciones de la NC (l.134-145), después de `<field name="name"/>`:

```xml
                            <field name="evidence_note" optional="show"/>
```

- [x] **Step 6: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/views/sgi_action_line_views.xml addons/quimibond_sgi/views/sgi_nonconformity_views.xml
git commit -m "quimibond_sgi: una acción correctiva no se termina sin evidencia (N-02)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.4: FUNC-C13 al crear: una NC no nace cerrada ni cancelada

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`QualityAlert.create`, l.231)

- [x] **Step 1: Candado antes de `super().create()`** (primeras líneas de `create`)

```python
    @api.model_create_multi
    def create(self, vals_list):
        # 57.93.0 (FUNC-C13 al crear): una NC nace abierta. Crearla en
        # Cerrada se brincaba los candados de cierre y quién cierra; en
        # Cancelada, el motivo aprobado (NC-4). Se revisa ANTES de crear para
        # no gastar folio (la secuencia no regresa números). El Jefe MAST y
        # el sistema sí pueden (cargas históricas).
        if not sgi_bypass_allowed(self.env):
            default_stage = self.env.context.get('default_stage_id')
            stage_ids = {vals.get('stage_id') or default_stage for vals in vals_list} - {False, None}
            bad = self.env['quality.alert.stage'].sudo().browse(stage_ids).filtered(
                lambda s: s.sgi_is_closing_stage or s.sgi_is_cancel_stage)
            if bad:
                raise UserError(
                    "Una NC nace abierta: no se crea directamente en «%s». Créela, registre sus "
                    "acciones y ciérrela (o pida su cancelación) desde la ficha."
                    % ", ".join(bad.mapped('name')))
        alerts = super().create(vals_list)
```

(El resto del método no cambia.)

- [x] **Step 2: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py
git commit -m "quimibond_sgi: una NC no se crea directamente cerrada ni cancelada (FUNC-C13)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.5: K-03, lo cerrado solo lo modifica el Jefe MAST

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py`
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (`SgiActionLineReview`, l.457)
- Modify: `addons/quimibond_sgi/models/sgi_audit.py` (`SgiAuditFinding`, l.514)

- [x] **Step 1: Constante común** en `sgi_nonconformity.py`, después de `_SGI_DEADLINE_STATES` (l.53-57)

```python
# 57.93.0 (K-03): campos que se mueven solos en un registro cerrado (chatter,
# actividades y la actividad espejo de una acción); no cuentan como edición.
_SGI_FREE_PREFIXES = ('message_', 'activity_', 'website_message', 'rating_')
```

- [x] **Step 2: La NC cerrada.** Método junto a `_sgi_user_can_close` (l.468):

```python
    def _sgi_check_closed_edit(self, vals):
        """57.93.0 (K-03): una NC cerrada es evidencia. Quien no es Jefe MAST
        solo escribe en el chatter y, si es el dueño del proceso, la reabre
        (solo la etapa, D-009)."""
        if sgi_bypass_allowed(self.env):
            return
        touched = {k for k in vals if not k.startswith(_SGI_FREE_PREFIXES)}
        if not touched:
            return
        closed = self.filtered(lambda a: a.sgi_folio and a.stage_id.sgi_is_closing_stage)
        if touched == {'stage_id'}:
            closed = closed.filtered(lambda a: not a._sgi_user_can_close())
            if closed:
                raise UserError(
                    "La NC %s está cerrada: la reabren el Jefe MAST o el dueño del proceso."
                    % ", ".join(closed.mapped('sgi_folio')))
            return
        if closed:
            raise UserError(
                "La NC %s está cerrada y es evidencia: solo el Jefe MAST la modifica. Si hay un "
                "error real, pida que la reabran." % ", ".join(closed.mapped('sgi_folio')))
```

Y como **primera línea** de `QualityAlert.write` (l.638):

```python
        self._sgi_check_closed_edit(vals)
```

- [x] **Step 3: Las acciones.** En `SgiActionLine`, junto a `_sgi_origin` (l.955):

```python
    def _sgi_origin_closed(self):
        """57.93.0 (K-03): ¿ya cerró el registro dueño de la acción? NC en
        etapa de cierre o incidente cerrado; la revisión por la dirección se
        agrega en sgi_management_review.py."""
        self.ensure_one()
        line = self.sudo()
        if line.alert_id:
            return bool(line.alert_id.stage_id.sgi_is_closing_stage)
        if line.incident_id:
            return line.incident_id.state == 'cerrado'
        return False

    def _sgi_check_closed_origin(self, vals=None, any_line=False):
        """57.93.0 (K-03): con el origen cerrado, quien no es Jefe MAST no
        cambia una acción terminada (any_line: tampoco agrega ni borra). Las
        pendientes sí se terminan (cierre forzado, acuerdos de una revisión)."""
        if sgi_bypass_allowed(self.env):
            return
        if vals is not None and not any(not k.startswith(_SGI_FREE_PREFIXES) for k in vals):
            return
        locked = self.filtered(lambda l: (any_line or l.date_done) and l._sgi_origin_closed())
        if locked:
            raise UserError(
                "La acción «%s» pertenece a un registro cerrado y es evidencia: solo el Jefe MAST "
                "la modifica. Si hay un error real, pida que reabran el registro."
                % ", ".join(locked.mapped('name')))
```

`write` (l.1029): primera línea `self._sgi_check_closed_origin(vals)`.

`unlink` (l.1059): primera línea `self._sgi_check_closed_origin(any_line=True)` (antes de `super()`: así el candado habla antes que el ACL, que no da borrado a Usuario SGI).

El `create` final (une las Tasks 3.2, 3.3 y 3.5):

```python
    @api.model_create_multi
    def create(self, vals_list):
        Alert = self.env['quality.alert']
        prepared = []
        for vals in vals_list:
            vals = self._sgi_done_vals(vals)
            if vals.get('alert_id') and 'effectiveness_round' not in vals:
                vals = dict(vals, effectiveness_round=Alert.browse(vals['alert_id']).sudo()
                            .sgi_ineffective_count)
            prepared.append(vals)
        lines = super().create(prepared)
        # 57.93.0 (K-03): a un registro cerrado no se le agregan acciones.
        lines._sgi_check_closed_origin(any_line=True)
        # 57.93.0 (N-02): evidencia de las correctivas que nacen terminadas.
        lines.filtered('date_done')._sgi_check_evidence()
        lines._sgi_sync_activity()
        # 57.93.0 (N-02): la correctiva nueva atiende el aviso del «No eficaz».
        for line in lines.filtered(lambda l: l.action_type == 'correctiva' and l.alert_id
                                   and l.effectiveness_round
                                   and l.effectiveness_round >= l.alert_id.sgi_ineffective_count):
            line.alert_id.activity_ids.sudo().filtered(
                lambda a: (a.summary or '').startswith("Registrar acción correctiva nueva")
            ).action_feedback(feedback="Se registró la acción correctiva «%s»." % line.name)
        return lines
```

- [x] **Step 4: Que terminar una acción de una NC cerrada no escriba en la NC.** En `write`, l.1053-1054:

```python
        # NC-3: terminar la última correctiva programa la eficacia a 90 días.
        # 57.93.0 (K-03): no en una NC ya cerrada (cierre forzado con acciones
        # pendientes): la NC cerrada no se modifica.
        if vals.get('date_done'):
            self.mapped('alert_id').filtered(
                lambda a: a.sgi_folio and not a.stage_id.sgi_is_closing_stage
            )._sgi_schedule_effectiveness()
```

- [x] **Step 5: La revisión por la dirección** (`sgi_management_review.py`, dentro de `SgiActionLineReview`, después de `_sgi_origin`):

```python
    def _sgi_origin_closed(self):
        """57.93.0 (K-03): un acuerdo terminado de una revisión cerrada es evidencia."""
        self.ensure_one()
        if self.review_id:
            return self.sudo().review_id.state == 'cerrada'
        return super()._sgi_origin_closed()
```

- [x] **Step 6: Los hallazgos** (`sgi_audit.py`). Import a nivel de módulo (después de l.8; `sgi_base` va antes en `models/__init__.py`, l.2, y no hereda modelos de otros archivos, así que el import es seguro):

```python
from .sgi_base import sgi_bypass_allowed
```

En `SgiAuditFinding`, antes de `unlink` (l.563):

```python
    def _sgi_check_audit_open(self):
        """57.93.0 (K-03): con la auditoría cerrada, sus hallazgos son
        evidencia: solo el Jefe MAST los crea o modifica."""
        if sgi_bypass_allowed(self.env):
            return
        locked = self.filtered(lambda f: f.audit_id.state == 'cerrada')
        if locked:
            raise UserError(
                "La auditoría %s está cerrada: sus hallazgos son evidencia y solo el Jefe MAST "
                "los modifica. Pídale reabrir la auditoría si hay un error real."
                % ", ".join(locked.mapped('audit_id.display_name')))

    @api.model_create_multi
    def create(self, vals_list):
        findings = super().create(vals_list)
        findings._sgi_check_audit_open()
        return findings

    def write(self, vals):
        self._sgi_check_audit_open()
        res = super().write(vals)
        if 'audit_id' in vals:
            self._sgi_check_audit_open()
        return res
```

- [x] **Step 7: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/models/sgi_management_review.py addons/quimibond_sgi/models/sgi_audit.py
git commit -m "quimibond_sgi: NC, acciones y hallazgos de un registro cerrado solo los modifica el Jefe MAST (K-03)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.6: N-03, auditoría que no deja NC sin tratar y programa que cubre todo

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_audit.py`
- Modify: `addons/quimibond_sgi/views/sgi_audit_views.xml` (record `sgi_audit_program_view_form`, l.4-51)
- Modify: `addons/quimibond_sgi/tests/test_pr6_external.py` (`test_03_au5_programa_sugerido`, l.106-130)

- [x] **Step 1: NC menor o mayor exige NC ligada.** En `SgiAudit._sgi_check_can_close` (l.454-475), reemplazar el bloque de `nc_mayor` (l.459-464):

```python
            # 57.93.0 (N-03): toda no conformidad de auditoría, menor o mayor,
            # se trata como NC (ISO 10.2): «sin acción» y «mejora» quedan para
            # observaciones, oportunidades y conformidades.
            if finding.finding_type in ('nc_menor', 'nc_mayor') and not finding.alert_id:
                problems.append(
                    "• El hallazgo «%s» es una no conformidad %s: debe tener su NC ligada "
                    "(use «Generar NC»)." % (
                        label, "mayor" if finding.finding_type == 'nc_mayor' else "menor"))
                continue
```

- [x] **Step 2: Programa sugerido con todos los subprocesos.** En `action_suggest_lines` (l.113-139):

```python
    def action_suggest_lines(self):
        """AU-5 (53.0.0): programa sugerido. Una línea por subproceso,
        repartidos por trimestre; los procesos con NC abiertas o indicadores
        en rojo, dos veces al año. Solo agrega los que aún no están.
        57.93.0 (N-03): también los procesos en borrador (casi todos lo están
        mientras el Dropbox siga vigente; ISO 9.2.2 pide cubrirlos)."""
```

y la búsqueda (l.125-127):

```python
            processes = self.env['sgi.process'].search(
                [('parent_id', '!=', False)], order='code, name')
```

y el mensaje (l.136-137):

```python
            program.message_post(body="Programa sugerido: %d línea(s) agregadas (todos los "
                                      "subprocesos; los que tienen NC abiertas o indicadores en "
                                      "rojo, dos veces al año)." % created)
```

- [x] **Step 3: Cobertura de 3 años.** Campos en `SgiAuditProgram`, después de `progress_pct` (l.58-59):

```python
    # 57.93.0 (N-03): ISO 9.2.2, todos los procesos dentro del ciclo de 3 años.
    coverage_gap_ids = fields.Many2many(
        'sgi.process', string="Sin auditar en 3 años", compute='_compute_coverage_gap',
        help="Subprocesos sin renglón en este programa ni en los de los dos años anteriores.")
    coverage_gap_count = fields.Integer(string="Procesos sin auditar en 3 años",
                                        compute='_compute_coverage_gap')
```

Método, después de `_compute_progress`:

```python
    @api.depends('year', 'line_ids.process_id')
    def _compute_coverage_gap(self):
        processes = self.env['sgi.process'].search([('parent_id', '!=', False)])
        for program in self:
            previous = self.search([('year', '>=', (program.year or 0) - 2),
                                    ('year', '<', program.year or 0)])
            covered = previous.line_ids.process_id | program.line_ids.process_id
            gap = processes - covered
            program.coverage_gap_ids = gap
            program.coverage_gap_count = len(gap)
```

En `action_approve`, dentro del segundo `for program in self:` (l.98-107), después de `program.state = 'aprobado'`:

```python
            if program.coverage_gap_ids:
                program.message_post(body=Markup(
                    "<b>Cobertura de 3 años:</b> se aprobó con %d subproceso(s) sin auditar en este "
                    "programa ni en los dos anteriores: %s.") % (
                        program.coverage_gap_count,
                        ", ".join(program.coverage_gap_ids.mapped('display_name'))))
```

- [x] **Step 4: Vista del programa** (record `sgi_audit_program_view_form`). `help` del botón «Programa sugerido» (l.16-19):

```xml
                            help="Una línea por subproceso (en borrador, piloto o vigente), repartidos por trimestre; los procesos con NC abiertas o indicadores en rojo, dos veces al año."/>
```

Dentro de `<sheet>`, entre el `<div class="oe_title">` y `<field name="line_ids">`:

```xml
                    <div class="alert alert-warning" role="alert" invisible="not coverage_gap_count">
                        <b>Cobertura de 3 años:</b>
                        <field name="coverage_gap_count" class="oe_inline"/> subproceso(s) sin
                        auditoría en este programa ni en los dos anteriores (ISO 9.2.2). Agréguelos
                        o deje el motivo en el historial:
                        <field name="coverage_gap_ids" widget="many2many_tags" readonly="1" nolabel="1"/>
                    </div>
```

- [x] **Step 5: Ajustar `test_pr6_external.test_03_au5_programa_sugerido`** (l.106-130). En la copia de producción hay muchos subprocesos en borrador: la prueba compara contra los suyos, no contra el total.

```python
    def test_03_au5_programa_sugerido(self):
        Process = self.env['sgi.process']
        macro = Process.create({'code': 'XP6M', 'name': 'Macro PR6'})
        p1 = Process.create({'code': 'XP6A', 'name': 'Sub A', 'parent_id': macro.id, 'state': 'vigente'})
        p2 = Process.create({'code': 'XP6B', 'name': 'Sub B', 'parent_id': macro.id, 'state': 'piloto'})
        p3 = Process.create({'code': 'XP6C', 'name': 'Sub C borrador', 'parent_id': macro.id})
        self.env['quality.alert'].create({
            'title': 'NC abierta en A', 'team_id': self.team_int.id, 'sgi_process_id': p1.id})
        program = self.env['sgi.audit.program'].create({'year': 2099})
        program.action_suggest_lines()
        lines = program.line_ids
        # 57.93.0 (N-03): también los borradores; el macroproceso no.
        self.assertTrue({p1, p2, p3} <= set(lines.mapped('process_id')))
        self.assertNotIn(macro, lines.mapped('process_id'))
        self.assertEqual(len(lines.filtered(lambda l: l.process_id == p1)), 2,
                         "Con NC abierta se audita dos veces al año.")
        self.assertEqual(len(lines.filtered(lambda l: l.process_id == p2)), 1)
        count = len(program.line_ids)
        program.action_suggest_lines()
        self.assertEqual(len(program.line_ids), count, "Idempotente.")
        # 4.4 (56.9.0): sin auditor líder no se aprueba.
        with self.assertRaises(UserError):
            program.action_approve()
        program.line_ids.write({'lead_auditor_id': self.env.user.id})
        program.action_approve()
        with self.assertRaises(UserError):
            program.action_suggest_lines()
```

- [x] **Step 6: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_audit.py addons/quimibond_sgi/views/sgi_audit_views.xml addons/quimibond_sgi/tests/test_pr6_external.py
git commit -m "quimibond_sgi: NC menor de auditoría con su NC, programa sugerido completo y cobertura de 3 años (N-03)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.7: N-12, devolución de cliente por la ubicación de origen

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_integration.py`

- [x] **Step 1: Import** (después de l.1): `from markupsafe import Markup`

- [x] **Step 2: `_sgi_is_customer_return`** (l.40-49)

```python
    def _sgi_is_customer_return(self):
        """Recepción validada que trae material DESDE la ubicación de clientes.

        57.93.0 (N-12): se decide por el origen físico del movimiento
        (``location_id.usage == 'customer'``), no por el tipo del picking
        devuelto: cubre la devolución capturada a mano (sin «Devolver») y
        cualquier ruta de entrega. Las órdenes de PdV son salientes y no
        cuentan; la devolución a proveedor tampoco (sale, no entra)."""
        self.ensure_one()
        if self.picking_type_id.code != 'incoming':
            return False
        return any(move.location_id.usage == 'customer'
                   for move in self.move_ids if move.state == 'done')
```

- [x] **Step 3: `_sgi_create_return_alert`** (l.51-79): los productos salen de los movimientos que vienen de clientes y el mensaje con `Markup`

```python
            returned = picking.move_ids.filtered(lambda m: m.location_id.usage == 'customer')
```

```python
                picking.message_post(body=Markup(
                    "Devolución de cliente: se levantó la NC <b>%s</b>.")
                    % (alert.sgi_folio or alert.title))
```

Y el de mantenimiento (l.174-175), mismo error de escape:

```python
        self.message_post(body=Markup("Se levantó la NC <b>%s</b> por esta falla.") % (
            alert.sgi_folio or alert.name))
```

- [x] **Step 4: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_integration.py
git commit -m "quimibond_sgi: toda recepción desde clientes levanta la NC de devolución (N-12)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.8: K-07, portal del proveedor con código de error

**Files:**
- Modify: `addons/quimibond_sgi/controllers/portal_nc.py`

- [x] **Step 1: Controlador completo**

```python
# -*- coding: utf-8 -*-
"""NC-6: el proveedor ve la NC y contesta causa y acción en el portal.

Acceso con el token del portal (enlace del correo) o como usuario portal
del proveedor; sin token válido, 403. Solo lectura de lo que el proveedor
necesita (folio, producto, lote, desviación, plazo) y un formulario de dos
campos que escribe por `quality.alert._sgi_supplier_answer`.

57.93.0 (K-07): la URL lleva un código de error, no el texto: un enlace
armado no pone texto en una página de Quimibond. Un código desconocido no
muestra nada.
"""
from odoo import http
from odoo.exceptions import AccessError, MissingError, UserError
from odoo.http import request

from odoo.addons.portal.controllers.portal import CustomerPortal

SGI_PORTAL_ERRORS = {
    'estado': "Esta NC ya no espera su respuesta. Si necesita corregirla, escriba a quien se la envió.",
    'faltan': "La causa y la acción son obligatorias.",
    'otro': "No se pudo registrar su respuesta. Escriba a quien le envió la NC.",
}


class SgiSupplierNcPortal(CustomerPortal):

    def _sgi_nc(self, alert_id, access_token):
        try:
            return self._document_check_access('quality.alert', alert_id, access_token=access_token)
        except (AccessError, MissingError):
            return None

    @http.route(['/my/nc/<int:alert_id>'], type='http', auth='public', website=False, sitemap=False)
    def portal_nc(self, alert_id, access_token=None, **kw):
        alert = self._sgi_nc(alert_id, access_token)
        if alert is None or not alert.sgi_folio:
            return request.redirect('/my')
        return request.render('quimibond_sgi.portal_nc_page', {
            'nc': alert.sudo(), 'access_token': access_token or '', 'saved': kw.get('saved'),
            'error': SGI_PORTAL_ERRORS.get(kw.get('error') or ''), 'page_name': 'sgi_nc',
        })

    @http.route(['/my/nc/<int:alert_id>/answer'], type='http', auth='public', methods=['POST'],
                website=False, csrf=True)
    def portal_nc_answer(self, alert_id, access_token=None, cause=None, action=None, **kw):
        alert = self._sgi_nc(alert_id, access_token)
        if alert is None or not alert.sgi_folio:
            return request.redirect('/my')
        base = '/my/nc/%d?access_token=%s' % (alert.id, access_token or '')
        if alert.sudo().sgi_supplier_state != 'enviada':
            return request.redirect(base + '&error=estado')
        if not (cause or '').strip() or not (action or '').strip():
            return request.redirect(base + '&error=faltan')
        try:
            alert.sudo()._sgi_supplier_answer(cause, action)
        except UserError:
            return request.redirect(base + '&error=otro')
        return request.redirect(base + '&saved=1')
```

La plantilla `views/sgi_portal_templates.xml` no cambia: sigue pintando `error`, que ahora es el texto del diccionario o nada.

- [x] **Step 2: Checadores y commit**

```bash
git add addons/quimibond_sgi/controllers/portal_nc.py
git commit -m "quimibond_sgi: el portal del proveedor manda códigos de error, no texto, y tiene prueba HTTP (K-07)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 3.9: Manuales de usuario

**Files:**
- Modify: `docs/sgi/usuarios/mast.md` (2.2, l.32-35), `docs/sgi/usuarios/operador-o-supervisor.md` (tabla l.127 y pregunta l.142-145), `docs/sgi/administracion/manual-jefe-mast.md` (sección 9, l.134-137), `docs/sgi/usuarios/auditor.md` (2.1 l.30-31 y 2.2 l.41-44)

- [x] **Step 1: Textos** («usted»):
  - Cierre de NC (los tres primeros): «Una NC no se cierra sin causa raíz, acciones terminadas (las correctivas, con evidencia: una nota o un archivo) y verificación de eficacia con resultado **Eficaz**, registrada en la fecha programada o después (y en una NC mayor, los 5 porqués y la lección aplicada). Si la verificación sale **No eficaz**, la NC regresa a Seguimiento y pide una acción correctiva nueva. Solo la cierran el dueño del proceso o el Jefe MAST; ya cerrada, solo el Jefe MAST la modifica.»
  - Pregunta frecuente del operador: agregar «o la acción correctiva no tiene evidencia».
  - Auditor 2.1: «**Programa sugerido** propone una línea por subproceso (también los que siguen en borrador). Si algún subproceso no se ha auditado en este programa ni en los dos anteriores, la ficha lo avisa (cobertura de 3 años).»
  - Auditor 2.2: «Una no conformidad menor o mayor siempre lleva su NC (**Generar NC**); «sin acción» y «mejora» son para observaciones y oportunidades. Con la auditoría cerrada, los hallazgos solo los corrige el Jefe MAST.»

- [x] **Step 2: Commit**

```bash
git add docs/sgi/usuarios docs/sgi/administracion
git commit -m "docs: cierre de NC con eficacia y evidencia, y hallazgos de auditoría (57.93.0)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Migración: ninguna

- Campos nuevos: `quality.alert.sgi_effective` (sin valor), `quality.alert.sgi_ineffective_count` (0), `sgi.action.line.evidence_note`, `sgi.action.line.evidence_attachment_ids` (tabla nueva vacía), `sgi.action.line.effectiveness_round` (0; un entero sin valor se lee como 0). `coverage_gap_*` no se guardan. El ORM crea las columnas al actualizar.
- No hay datos que rellenar: 0 NC cerradas (no hay que marcar «Eficaz» a nadie), 4 acciones todas terminadas (la evidencia solo se exige al terminar), 0 auditorías y 0 hallazgos.
- **No cambia datos de negocio**: no necesita el visto bueno de Jose. (La ficha mencionaba un post-migrate «que solo crea el campo»: no hace falta, el ORM lo hace; no se crea `migrations/19.0.57.93.0/`.)
- Si Jose decide la Q1 (rehacer las NC de devolución borradas), eso va en una entrega aparte o en la pista de datos, con su OK escrito.

## Task 3.10: Versión, CHANGELOG, documentación y build

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), este plan (casillas)

- [x] **Step 1: Versión** `'19.0.57.93.0'` en `__manifest__.py` (l.21).

- [x] **Step 2: CHANGELOG**, arriba de `## 19.0.57.92.0`

```markdown
## 19.0.57.93.0 — 2026-10-XX

**NC y auditoría con evidencia** (auditoría 2026-10: N-02, N-03, N-12, K-03; y lo pendiente de 57.91.0: FUNC-C13 al crear, K-07 portal).

### Agregado

- **Resultado de la eficacia (N-02):** `quality.alert.sgi_effective` (Eficaz / No
  eficaz). La NC solo cierra con «Eficaz». «No eficaz» deja la verificación en
  el historial, la limpia, regresa la NC a Seguimiento, sube
  `sgi_ineffective_count` y pide una acción correctiva nueva
  (`sgi.action.line.effectiveness_round`); sin ella terminada la NC no cierra.
  `sgi_ineffective_count` deja listo el indicador «% de NC eficaces a la
  primera» (57.98.0).
- **Evidencia en acciones correctivas (N-02, H-B1.4):** `evidence_note` y
  `evidence_attachment_ids` en `sgi.action.line`. Una correctiva no se termina
  sin nota, archivo o archivo en su chatter, por cualquier vía (botón, lista,
  chatter: ahí los archivos adjuntos al marcar hecha la actividad quedan como
  evidencia). Solo el sistema queda exento. Lo terminado antes no se toca.
- **Cobertura de 3 años (N-03):** `sgi.audit.program.coverage_gap_ids`: aviso
  en la ficha del programa y nota al aprobarlo con los subprocesos sin
  auditar en ese programa ni en los dos anteriores. Las normas del alcance
  quedan pendientes.

### Cambiado

- **Fecha de eficacia (N-02, H-B1.1):** no futura, en o después de la fecha
  programada (`sgi_effectiveness_due`) y de la última acción terminada; si no
  se puede esperar, cierre forzado del Jefe MAST con motivo.
- **Verificación de eficacia a quien cierra (FUNC-C13):** la actividad y el
  aviso del cron van al dueño del proceso (sin dueño, al Jefe MAST), no a
  «Eficacia verificada por».
- **Auditoría (N-03, H-B2.1):** un hallazgo de no conformidad menor o mayor
  exige su NC ligada para cerrar la auditoría; «sin acción» y «mejora» quedan
  para observaciones, oportunidades y conformidades.
- **Programa sugerido (N-03, H-B2.2):** todos los subprocesos, también en
  borrador.
- **Devolución de cliente (N-12):** se detecta por `location_id.usage ==
  'customer'` en una recepción. La hipótesis de la entrega en tres pasos no
  se confirmó en producción (las 26 devoluciones desde junio sí apuntan al
  OUT; las 7 posteriores al 21-ago levantaron NC y 4 de ellas,
  NCI-2026-0140/0142/0147/0148, se borraron antes de K-02).
- **Portal del proveedor (K-07):** la URL lleva un código de error (`estado`,
  `faltan`, `otro`), no el texto.

### Seguridad

- **K-03:** una NC cerrada solo la modifica el Jefe MAST (el dueño del
  proceso la reabre: solo la etapa, D-009). Con la NC, el incidente o la
  revisión por la dirección cerrados, sus acciones terminadas solo las
  modifica el Jefe MAST y nadie más agrega o borra acciones; las pendientes
  se siguen terminando. Con la auditoría cerrada, sus hallazgos solo los crea
  o modifica el Jefe MAST.
- **FUNC-C13 al crear:** una NC no se crea directamente en Cerrada ni en
  Cancelada (salvo el Jefe MAST o el sistema), sin gastar folio.

### Corregido

- Mensajes del chatter que salían con `&lt;b&gt;` (devolución de cliente,
  falla de mantenimiento, eficacia programada): `Markup`.

**Migración:** ninguna. El ORM crea las columnas; no hay datos que rellenar
(0 NC cerradas, 4 acciones terminadas, 0 auditorías).

**Pruebas:** `test_nc_auditoria_evidencia` (20 casos) y `test_portal_nc_http`
(4, `HttpCase`), nuevas. Ajustadas para cerrar con «Eficaz»: `test_nc_flow`,
`test_nc_deadlines.test_04`, `test_candados_evidencia`, `test_pegamento`,
`test_ola_b`, `test_ola1`, `test_capture_reply`; `test_pr6_external.test_03`
por el programa sugerido.
```

- [x] **Step 3: Documentación técnica generada**

```bash
python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
```

- [x] **Step 4: Checadores** (sección 0, punto 4). Esperado: 0 errores (sin la advertencia de bump).

- [x] **Step 5: Commit, push y build**

```bash
git add -A addons/quimibond_sgi docs/sgi
git commit -m "quimibond_sgi 19.0.57.93.0: NC y auditoría con evidencia" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

Esperado en el build de la rama (`--test-tags /quimibond_sgi`): `test_nc_auditoria_evidencia` 20/20 y `test_portal_nc_http` 4/4 OK, y ningún fallo nuevo respecto del build de `main`. Si una prueba existente falla por «Eficaz», por la fecha de eficacia o por evidencia, ajustar la prueba (resultado «Eficaz», eficacia antes de terminar la correctiva, o `evidence_note`), nunca quitar el candado. Si falla por K-03 una prueba que edita algo cerrado con un usuario normal, revisar que sea de verdad una edición de evidencia; si lo es, hacerla como Jefe MAST.

- [x] **Step 6: Marcar las casillas** de este plan y commit (`docs: plan 57.93.0 al día`).

- [ ] **Step 7: Verificación en Odoo.sh y en producción** (solo lectura, por MCP)
  - `ir.module.module [('name','=','quimibond_sgi')]` → `latest_version = 19.0.57.93.0`.
  - `get_fields('quality.alert', ['sgi_effective','sgi_ineffective_count'])`, `get_fields('sgi.action.line', ['evidence_note','evidence_attachment_ids','effectiveness_round'])`, `get_fields('sgi.audit.program', ['coverage_gap_ids'])`: existen.
  - `update.log` del deploy sin errores de `sgi_action_line_evidence_rel`.
  - En la base de staging (copia de producción), abrir el programa de auditorías 2027 en borrador: el aviso de cobertura lista los subprocesos.
  - Después de la siguiente devolución validada (tipo 91 «Toluca: Devoluciones»): `stock.picking [('picking_type_id','=',91),('state','=','done'),('date_done','>=','<fecha del deploy>')]` → `sgi_return_alert_id` lleno y el chatter con el folio en negritas, no `&lt;b&gt;`.

---

## Preguntas para Jose (con la opción por omisión)

- **Q1. NC de devolución borradas.** Las devoluciones TL/IN-/00180, 00182, 00185 y 00186 (25-ago a 22-sep) levantaron las NC NCI-2026-0140, 0142, 0147 y 0148, y esas NC ya no existen (se borraron antes de K-02). ¿Se vuelven a levantar? **Por omisión:** no por migración; Calidad las levanta a mano desde cada recepción si siguen vigentes, y se anota en `docs/audit/decisiones.md` junto con el hueco de folios (H-B1.6).
- **Q2. Evidencia en preventivas.** La auditoría proponía evidencia en correctivas **y** preventivas; la ficha dice correctivas. **Por omisión:** solo correctivas (una línea en `_sgi_needs_evidence` lo amplía).
- **Q3. Acuerdos de la revisión por la dirección.** Nacen como correctivas (`sgi_management_review.py:397-403`), así que desde 57.93.0 piden evidencia para terminarse. **Por omisión:** sí (el acuerdo cumplido debe demostrarse en la siguiente revisión).
- **Q4. Candado de la NC cerrada misma.** La ficha habla de acciones y hallazgos; el plan también cierra la NC (como propuso A-3 y como ya están incidente, revisión y auditoría). **Por omisión:** sí, con la reapertura por el dueño del proceso (D-009).
