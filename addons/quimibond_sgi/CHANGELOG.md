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
