# quimibond_sgi — Sistema de Gestión Integral (Fases 1, 2, 3 y 4)

Addon de Odoo 19 Enterprise que lleva el SGI documental de PNTQ (ISO 9001:2015 +
14001:2015, 45001 en preparación) a Odoo **extendiendo apps nativas** (Documentos,
Aprobaciones, Calidad, Helpdesk, Proyecto) y creando solo lo que Odoo no tiene
(mapa de procesos, acuses de lectura, catálogos SGI). Toda la lógica vive en el
módulo: **cero Studio, cero automation rules de UI**.

## Qué instala/configura el módulo (automático)

- **Grupos de seguridad** (privilegio "SGI"): Usuario SGI, Auditor SGI,
  Jefe MAST y SGI, Dirección de Operaciones (SGI). Cada uno implica los grupos
  base de las apps que toca (Calidad, Documentos, Helpdesk, Proyecto, Aprobaciones).
- **Catálogos**: 10 áreas documentales (G, A, C, D, E, I, M, P, S, V); normas
  ISO 9001/14001/45001 con cláusulas 4.1–10.3; secuencias anuales de folio de NC
  (`NCI-AAAA-####` / `NCE-AAAA-####`).
- **Mapa de procesos**: 5 macroprocesos + 16 procesos nivel 2 + la cadena de flujos
  COP (Crédito y Cobranza → Ventas → Planeación → … → Facturación → CxC).
- **No Conformidades** (sobre `quality.alert`): equipos "NC Internas" y "NC Externas"
  con etapas Abierta → Seguimiento → Cerrada + Cancelada; folio anual; 5 porqués;
  líneas de acción; **candados de cierre** (causa raíz + acciones terminadas +
  verificación de eficacia) con cierre forzado exclusivo del Jefe MAST.
- **Control documental** (sobre `documents.document`): clave validada con la
  nomenclatura real, tipo, área, revisión, estado (Borrador/Piloto/Vigente/Obsoleto),
  puestos aplicables, próxima revisión bienal, **acuses de lectura** con % de difusión.
- **Cambios documentales** (sobre `approval.request`): categoría "Modificación de
  documento SGI" (alta/modificación/baja, prueba piloto ≤90 días, procesos afectados);
  al aprobarse versiona el documento y dispara acuses.
- **Reclamaciones** (sobre `helpdesk.ticket`): equipo "Reclamaciones de clientes"
  con etapas y política SLA de primera respuesta; botón "Generar NC".
- **Mejora continua** (sobre `project.task`): proyecto "Mejora Continua SGI" con
  candado de cierre (fecha límite + evidencia adjunta).
- **Crons** (diario NC, diario documentos, mensual NEWS) y **reportes** QWeb
  F-P-G05-01 (NC) y F-P-G01-16 (NEWS).
- **Integraciones**: smart buttons de NC en picking/producción/contacto,
  "Mis procedimientos"/"Acuses pendientes" en empleado, documentos aplicables en puesto.

## Checklist de puesta en marcha (manual, en la instancia)

1. **Usuarios en los grupos SGI** (Ajustes → Usuarios):
   - Jefe de MAST y SGI → grupo **Jefe MAST y SGI**.
   - Dirección de Operaciones → grupo **Dirección de Operaciones (SGI)**.
   - Auditores → **Auditor SGI**; resto del personal operativo → **Usuario SGI**.
2. **Aprobadores de la categoría** "Modificación de documento SGI"
   (Aprobaciones → Configuración → Categorías): agregar los 2 aprobadores en
   secuencia (Jefe MAST y SGI → Dirección de Operaciones) y marcar el mínimo.
3. **Helpdesk**: activar/ajustar la **política SLA** del equipo "Reclamaciones de
   clientes" y configurar el **alias de correo** del equipo si se usará entrada por email.
4. **Documents**: crear (si no existe) el árbol de **carpetas** espejo de las
   secciones 00–23 del SGI y asignar permisos por área. Los documentos controlados
   se registran con su clave y revisión vigente.
5. **Parámetro** `quimibond_sgi.nc_escalation_days` (Ajustes → Técnico → Parámetros
   del sistema): días para escalar NC internas sin acción (por defecto 5; las
   externas/reclamación escalan a 3 por regla de negocio).
6. **Empleados** con su **puesto** (`hr.job`) correcto: los acuses y "Mis
   procedimientos" filtran por puesto.
7. En dev/staging los **crons están desactivados**: pruébalos con
   Ajustes → Técnico → Acciones planificadas → "Ejecutar manualmente".

## Fase 2 — Gestión y Medición (v19.0.2.0.0)

Extiende el mismo addon (depende ahora también de `survey`, `purchase`,
`approvals_purchase` y `maintenance`).

### Qué instala/configura (automático)

- **Objetivos e Indicadores** (`sgi.objective`, `sgi.indicator`,
  `sgi.indicator.measure`, F-P-A10-03): catálogo de los 9 Objetivos Integrales
  2026-2030 (ANEXO 6) y ~26 KPIs reales con metas Objetivo/Aceptable, semáforo
  verde/amarillo/rojo (respeta `direction`), y **motor de cálculo automático** por
  `calc_mode`. Cron mensual (día 1) que crea la medición del mes anterior
  (idempotente): los KPIs automáticos se calculan y quedan "capturados"; los
  manuales quedan "pendientes" con actividad al responsable (límite día 5).
  Una medición **roja validada** de un indicador con `nc_on_red=True` genera
  **una** NC pre-llenada (equipo NC Internas, origen "indicador") y la liga.
- **Auditorías** (`sgi.audit.program`, `sgi.audit`, `sgi.audit.finding`, P-G03):
  programa anual → auditoría con folio `AUD-AAAA-NN`, checklist en `survey`
  (plantilla ISO 9001 secciones 4-10 incluida), hallazgos con disposición y botón
  "Generar NC". **Candados**: el auditor no audita procesos de los que es dueño;
  no se cierra con hallazgos sin disposición resuelta. Cron diario de avisos 15
  días antes del mes planificado.
- **Riesgos y oportunidades** (`sgi.risk` + `sgi.risk.category`): los 5 instrumentos
  evaluables (R&O 5×5, IPER 3×3, ambiental, patrimonial 5×5, FODA sin score) con
  nivel de atención computado, mapa de calor (pivot proceso × nivel) y kanban.
  Umbrales R&O parametrizables. Acciones en `sgi.action.line` (constraint XOR
  NC/Riesgo). Cron diario de revisión de riesgos vencidos → actividad al dueño.
- **Proveedores** (`sgi.supplier.eval` + herencia `res.partner`, 8.4): evaluación
  trimestral (OTD de recepciones + NCs → calificación ponderada → clase
  acreditado/condicionado/baja), cron trimestral idempotente que actualiza el
  contacto y avisa a Compras. Requisición interna vía categoría de Aprobaciones
  "Solicitud de compra SGI" (`approval_type=purchase`, genera RFQ nativas).
- **Revisión por la Dirección** (`sgi.management.review`, IT-P-A10-01): folio
  `RD-AAAA-NN`, botón "Cargar entradas" que llena las 10 entradas 9.3.2 con el
  snapshot del periodo, y salidas (acuerdos) que al marcar "Realizada" crean tareas
  en el proyecto "Acuerdos RxD". **Candado**: no se realiza sin ≥1 acuerdo con
  responsable y fecha.
- **Voz del cliente/trabajador**: equipo Helpdesk "Quejas y Sugerencias" (alias
  `quejas-sugerencias`, etiquetas Queja/Sugerencia/SST-Condición insegura) y
  plantilla de Encuesta de Satisfacción del Cliente.
- **Reportes** QWeb: Plan de auditoría (F-P-G03-03), Informe de auditoría y Acta
  de Revisión por la Dirección (F-IT-P-A10-01-01).

### `calc_mode` — cobertura del motor de cálculo

| calc_mode | Fuente | Estado |
|---|---|---|
| `otif_ventas` | Pickings salida done vs `date_deadline`/`scheduled_date` | Implementado |
| `otd_compras` | Recepciones done vs `date_planned` de la OC | Implementado |
| `produccion_vs_programado` | `mrp.production` done: qty_produced/product_qty | Implementado |
| `desperdicio` | `stock.scrap` / producción del periodo | Implementado |
| `cierre_nc` | NCs cerradas/detectadas en el periodo | Implementado |
| `reclamos_cliente` | Tickets del equipo Reclamaciones | Implementado |
| `presupuesto_ventas` | Facturado (out_invoice posted) vs `monthly_budget` | Implementado |
| `preventivo_cumplido` | `maintenance.request` preventivas en etapa "done" | Implementado (aprox.) |
| `rotacion_rh` | Bajas (`departure_date`) / plantilla activa | Implementado (aprox.) |
| `disponibilidad_mantto` | Requiere paros de centros de trabajo | Devuelve None → captura manual |
| `plantilla_rh` | Requiere plantilla presupuestada por puesto | Devuelve None → captura manual |
| `reproceso` | Sin fuente confiable aún | Devuelve None → captura manual |
| `inventario_diferencia` | Requiere conteos físicos registrados | Devuelve None → captura manual |

Los que devuelven None caen a captura manual sin bloquear el cron.

### Configuración de instancia pendiente (Fase 2)

1. **Responsables y procesos de indicadores**: asignar `responsible_id` y
   `process_id` en cada `sgi.indicator` (se dejaron vacíos-opcionales).
2. **Presupuesto de ventas**: capturar `monthly_budget` en el indicador VE-02
   (o el parámetro `quimibond_sgi.monthly_sales_budget`).
3. **Dashboard ejecutivo**: armar con Spreadsheet/Dashboards sobre las mediciones
   (graph/pivot mes × indicador ya disponibles en el menú Medición).
4. **Formulario web del buzón QR**: apuntar el QR de planta al formulario web del
   equipo "Quejas y Sugerencias" (Helpdesk → Configuración → Sitio web/alias).
5. **Envío de encuestas**: configurar el envío automático post-entrega de la
   "Encuesta de Satisfacción del Cliente" (Encuestas → Compartir / automatización
   de instancia).
6. **Aprobadores de "Solicitud de compra SGI"**: definir aprobadores y, si aplica,
   el almacén/ubicación por defecto de las RFQ.
7. **Umbrales**: `quimibond_sgi.risk_ryo_inmediata/media/intermedia` (16/9/4),
   `quimibond_sgi.supplier_weight_otd/quality` (0.7/0.3) y
   `quimibond_sgi.supplier_nc_penalty` (10 pts por NC) son parametrizables.
8. **Indicadores semanales**: los KPIs con `frequency=weekly` (p.ej. OTIF LO-01)
   los mide el cron semanal (lunes, semana previa); los mensuales, el cron mensual.
   Ambos son idempotentes por (indicador, periodo).

### Plan de activación gradual de `nc_on_red`

`nc_on_red` arranca en **False** en todos los indicadores. Antes de activarlo en
producción **por indicador**:

1. Deja correr el cron mensual 1 mes y compara el valor calculado (KPIs
   automáticos) contra el Excel **F-P-A10-03** del mes correspondiente en staging.
2. Cuando el KPI coincida con el Excel, valida la medición ("Validar") y verifica
   que el semáforo es correcto.
3. Solo entonces activa `nc_on_red` en ese indicador. A partir de ahí, cada
   medición roja **validada** generará su NC automática. Nunca actives todos a la
   vez: valida uno, observa un ciclo, avanza al siguiente (esto elimina ~60% de las
   NCs que hoy se levantan a mano por indicador incumplido).

## Fase 3 — Herramientas automotrices (v19.0.3.0.0)

Bloques (core tools que exigen clientes como Seiren y Continental):

### Qué instala/configura (automático)

- **Plan de control (P-C11)** — modelo `sgi.control.plan` (folio `PC-AAAA-NN`,
  estados borrador/vigente/obsoleto). Extiende `quality.point` con característica,
  criticidad **F/R/S** (tipo Continental), Cpk objetivo, aparece-en-CoA y plan de
  reacción. Candado: no pasa a vigente sin ≥1 punto; al marcar obsoleto agenda
  revisión al Jefe MAST (no desactiva los puntos). Botón **CoA (P-C07)** bilingüe
  ES/EN en `stock.lot` con las inspecciones de puntos marcados para el certificado.
- **Calibración (P-C03)** — modelo `sgi.calibration` + extensión de
  `maintenance.equipment` (equipo de medición, magnitud/rango/resolución, intervalo,
  próxima calibración y estado vigente/por vencer/vencido calculados). Regla IATF
  7.1.5: resultado *fuera de tolerancia* bloquea el equipo (**No usar**), crea NC
  interna de evaluación de impacto y agenda al Jefe MAST; una calibración conforme
  libera el candado.
- **AMEF (P-C10)** — `sgi.fmea` + `sgi.fmea.line` con **NPR = S×O×D** calculado; una
  línea con NPR ≥ `quimibond_sgi.fmea_npr_action` (100) exige acción. Candado: no
  pasa a vigente si hay líneas de NPR alto sin acción. Reporte PDF.
- **PPAP (P-C15)** — `sgi.ppap` genera automáticamente los **18 elementos AIAG**
  (idempotente), enlazándolos a registros reales (AMEF, plan de control, documento).
  Candados: "enviado" sin elementos pendientes; "aprobado" requiere el elemento 18
  (PSW) en listo/aprobado. Botones inteligentes en cliente y producto.
- **Puente PLM** (módulo aparte `quimibond_sgi_plm`, `auto_install`) — al aplicar un
  ECO marcado "Requiere PPAP" crea el expediente PPAP (motivo: cambio de ingeniería)
  y, si requiere aviso al cliente, agenda actividad a Ventas.
- **Competencias (P-A01)** — análisis de brechas nativo sobre `hr_skills`: vista SQL
  `sgi.competence.gap` (pivote por departamento × tipo de competencia), botón de
  brechas en el empleado y cron de vigencias de certificaciones/formación. Plantilla
  de encuesta **DNC (F-P-A01-17)**.
- **Incidentes SST (P-S02, SCAT)** — `sgi.incident` (folio `INC-AAAA-NN`, tipos y
  severidad leve/moderado/grave/fatal). Candados: no cierra sin las 3 capas SCAT ni
  con acciones abiertas; graves/fatales avisan de inmediato al Jefe MAST y Dirección.
  Cualquier usuario SGI puede reportar. **EPP (P-S03)**: `maintenance.equipment`
  marcado como EPP con fecha de vencimiento; el cron notifica próximos vencimientos.
- **Pegamento PROT-05/D7** — al cerrar una NC **mayor** se agenda al Jefe MAST
  actualizar el AMEF y el plan de control (lección aprendida).
- **XOR de acciones extendido** — `sgi.action.line` acepta exactamente un origen:
  NC, riesgo, línea de AMEF o incidente.
- **Crons diarios** (idempotentes): calibraciones + EPP; competencias/certificaciones.

### Configuración de instancia pendiente (Fase 3)

1. **Matriz de habilidades por puesto**: cargar `hr.job.skill` (competencia + nivel
   esperado) por puesto y las `hr.employee.skill` del personal; la brecha se calcula
   sola (menú Medición → *Brechas de competencia (DNC)*).
2. **Coordinador de RH**: fijar `quimibond_sgi.rh_user_id` (destinatario de los avisos
   de certificaciones/formación por vencer).
3. **Continental Master Specs**: capturar en cada `quality.point` la característica,
   criticidad F/R/S y Cpk objetivo (1.33 F / 1.67 R·S) desde la Master Spec.
4. **Firma del CoA**: si se requiere firma electrónica, enlazar el reporte CoA con la
   app *Firma* (plantilla sobre el PDF).
5. **Onboarding / Frontdesk / ESG**: pendientes de decisión de alcance (no incluidos).
6. **Equipos de medición y EPP**: marcar los `maintenance.equipment` existentes como
   *equipo de medición* / *EPP* y capturar intervalos y vencimientos.
7. **Decisión PLM**: activar el flujo de ECO→PPAP requiere instalar *mrp_plm*; el
   puente `quimibond_sgi_plm` se instala solo cuando ambos módulos coexisten.

### Deuda técnica / mejoras futuras (Fase 4)

- **Endurecer el candado de AMEF** (`sgi.fmea.action_set_vigente`): hoy una línea con
  NPR alto se da por atendida con **una acción registrada**, aunque no esté terminada
  (así lo pedía la spec de Fase 3). Para Fase 4, considerar exigir que la acción tenga
  al menos responsable + fecha de compromiso, o incluso fecha de terminación, antes de
  permitir el paso a *vigente*.
- **Commits por bloque**: retomar un commit atómico por bloque funcional (en Fase 3
  el grueso quedó en un solo commit).

## Fase 4 — Conexión con el piso real y tableros (v19.0.4.0.0)

Cierra el círculo dentro de Odoo: el SGI se alimenta de la operación real de piso
(pesaje, revisado, subproducto de desperdicio) y los resultados llegan a dirección
(tableros). **Sin Shop Floor y sin tocar `quimibond_intelligence`/Supabase.** No se
modifica ningún módulo de piso: solo se agregan ganchos hacia el SGI.

### Qué instala/configura (automático)

- **Escalar a NC del SGI** (botón en `quality.alert`, grupo Auditor+, visible solo si
  la alerta no tiene folio): mueve la alerta a NC Internas, le asigna folio y origen
  'proceso' conservando producto/orden/picking. Las alertas rutinarias de los equipos
  de piso siguen su flujo; solo lo sistémico se escala (el concentrado F-P-G05-02 no se
  contamina).
- **Planes de control que envuelven los puntos reales** (Recepción de MP F-P-P03-01 y
  Tejido Circular — Revisado). El enlace de los `quality.point` se hace en el
  `post_init_hook` por **búsqueda segura por equipo**: si el equipo no existe en la BD
  (staging≠producción), registra en log y sigue sin romper el update.
- **Puente de pesaje** (`quimibond_sgi_pesaje`, auto_install): al confirmar un rollo
  fuera de la tolerancia ±3 kg, crea una alerta ligada a la orden/producto (una sola
  por rollo).
- **KPIs recalibrados a las fuentes reales del piso** (ver tabla abajo).
- **Vistas pivot/graph** para tableros: Pareto de alertas de calidad (`quimibond_sgi`)
  y Pareto de defectos del revisado por causa TEJIDO-* (`quimibond_sgi_revisado`,
  auto_install).
- **Mapa de procesos conectado a los objetos vivos** (4.6): cada flujo apunta a su
  modelo de Odoo (`odoo_model_id`) con botón **«Ver registros»** (invisible si es un
  entregable documental); 14 flujos operativos + 5 de soporte ligados.
- **Smart button «NC del proveedor»** en `purchase.order` (alertas del proveedor).
- **Botón «Levantar NC»** en solicitudes de mantenimiento correctivas (crea la NC en
  NC Internas pre-llenada con el equipo y la falla; idempotente).
- **CoA en el portal del cliente**: botón «Publicar CoA en portal» en el lote que
  adjunta el PDF a la(s) entrega(s) (visible en el portal). Explícito, sin automatismo.

### KPIs — fuente real y estado de validación (Fase 4.2)

| KPI (`calc_mode`) | Fuente real | Estado |
|---|---|---|
| `desperdicio` | Kilos del subproducto **SALDO TEJIDO D** (categoría `SubProducto`, param `quimibond_sgi.waste_subproduct_category`) / kilos producidos | Validar contra Excel de producción del mes de referencia |
| `desperdicio_scrap` | `stock.scrap` / kilos producidos (cálculo histórico, conservado) | OK |
| `calidad_pq` | `mrp.revision.log`: rollos sin causa (defecto = etiqueta TEJIDO-*) / total | Validar con datos reales de revisado |
| `cumplimiento_programa` | `mrp.production` producido vs planificado (inicio en el periodo) | **Aproxima el MPS** (usa MOs con inicio programado en el periodo, no el plan maestro literal). Validar contra el Excel de producción un mes **antes** de activar su `nc_on_red` |
| `inventario_ciclico` | `|ajustes de inventario|` (movimientos `is_inventory`) / existencias en ubicaciones internas | **Requiere conteos cíclicos activos**; existencias «contadas» = proxy de on-hand actual |
| (otros de Fase 2) | ventas/compras/mantto/RH nativos | Sin cambio |

Todos degradan a `None` cuando faltan datos o configuración en la instancia.

### Tableros (Fase 4.4 — configuración de instancia)

Se arman con **Hojas de cálculo / Tableros nativos** (sin JS). Fuentes de datos:

1. **Producción por circular y turno** — `mrp.workorder`/`mrp.production` (pivot nativo,
   fila = centro de trabajo, medida = cantidad/tiempo).
2. **Pareto de defectos del revisado** — menú *SGI → Tableros → Pareto de defectos
   (revisado)* (pivot de `mrp.revision.log` por causa TEJIDO-*, filtro «Con defecto»).
3. **Desperdicio (SALDO TEJIDO D) por tela y circular** — movimientos de subproducto de
   la categoría `SubProducto` (pivot de `stock.move` byproduct).
4. **Cumplimiento del programa semanal** — KPI `cumplimiento_programa` (menú Medición).
5. **Panel SGI ejecutivo** — NCs (concentrado), KPIs (menú Medición), riesgos
   (mapa de calor), calibraciones (metrología) y *SGI → Tableros → Pareto de alertas*.

### Configuración de instancia pendiente (Fase 4)

1. **Retro-vinculación**: se ejecuta sola al instalar si existen los equipos «CALIDAD
   Materia Prima» y «Revisado de Tela». Si se agregan/renombran equipos después,
   reinstalar el módulo o re-ejecutar `post_init_hook` desde el shell.
2. **Categoría de desperdicio**: confirmar que el subproducto SALDO TEJIDO D vive en la
   categoría `SubProducto` (o ajustar el parámetro `quimibond_sgi.waste_subproduct_category`).
3. **Días festivos** en el calendario 24/7 3 Turnos (festivos MX): configurar en
   *Empleados → Configuración → Tiempo libre → Festivos públicos* (config de instancia).
4. **Rollout de las 186 telas**: correr el reporte de trabajo
   `addons/quimibond_sgi/tools/reporte_telas_rollout.py`
   (`odoo-bin shell --no-http < …`), que lista las telas sin operación TEJIDO/BoM
   completa agrupadas por familia y exporta `/tmp/telas_rollout.csv`. **No** configura
   telas: el criterio (capacidades, tiempos) es de Producción.
5. **Báscula (IoT)**: verificar `iot_scale_common` + drivers del repo; el pesaje ya usa
   el widget `peso_bascula`. Documentar en sitio qué básculas responden y cuáles faltan.
6. **Usuarios por centro de trabajo**: asignar responsables/técnicos por circular
   (config de instancia).
7. **WhatsApp** (solo si contratan la integración Meta): plantillas para NC asignada,
   equipo bloqueado y KPI rojo. Si no, actividades + correo ya cubren (no bloqueante).
8. **Portal del auditor SIDE**: carpetas de Documentos en solo lectura con permiso de
   fecha de expiración para la auditoría de vigilancia (config de Documentos).
9. **Buzón QR**: publicar el formulario web del equipo «Quejas y Sugerencias» (si sigue
   pendiente de Fase 2).

### Configuración de instancia pendiente (Fase 4.6 — mapa de procesos)

Ganchos que este bloque NO configura (los captura el equipo en la instancia):

10. **Quality point «Verificación de embarque»** en salidas (checklist de empaque por
    cliente) que alimente el KPI «embarques sin error».
11. **Conteos cíclicos activados** (para que `inventario_ciclico` sea confiable).
12. **Firma de pedido en portal** para exportación (aceptación en línea del cliente).
13. **Aprobadores de la requisición de compra SGI** y niveles de seguimiento de cobranza.
14. **Propiedades custom del lote** (ancho/gramaje/tono) y **motivos de pérdida en CRM**.
15. **Resto de flujos de soporte** del mapa (además de los 5 ya cargados): ligarlos a su
    modelo desde *SGI → Flujos* o el formulario del proceso.

## Herramientas de shell (`tools/`)

Scripts de un solo uso, se corren con `odoo-bin shell --no-http < tools/<script>.py`:

- **`reporte_telas_rollout.py`** — lista de trabajo de telas sin operación TEJIDO/BoM
  completa (solo lectura, exporta `/tmp/telas_rollout.csv`).
- **`carga_documental.py`** — carga masiva del SGI documental (ZIP con las carpetas
  00-23) a la app Documentos. **`DRY_RUN=True` por defecto**: la primera corrida solo
  imprime el plan y escribe `/tmp/carga_documental_reporte.csv`; para cargar de verdad
  se pone `DRY_RUN=False` y se re-ejecuta. Crea el árbol de carpetas espejo (con
  subcarpetas de departamento en 02), y por cada archivo con nombre que cumple la
  nomenclatura de PNTQ crea el documento controlado (clave, tipo, área, revisión —
  «Rev NN» o «00»—, vigente, emisión = fecha del archivo, próxima revisión +2 años).
  Salta carpetas con «obsolet/baja/anterior»; los archivos sin clave van a **POR
  CLASIFICAR** (no controlados). Idempotente (salta lo ya cargado), reporta duplicados
  de clave dentro del ZIP, y **no** genera acuses ni asigna puestos (eso es manual).
  **Correr primero en staging, validar el CSV, y solo con visto bueno en producción.**

## Instalación / actualización (shell Odoo.sh)

```bash
odoo-update quimibond_sgi && odoosh-restart http
```

> **NO** cambiar la versión del manifest de `quimibond_intelligence` ni de otros
> módulos del repo (un bump dispara `-u` global que falla por errores preexistentes
> de Studio).

## Tests

```bash
odoo-bin --test-tags /quimibond_sgi -u quimibond_sgi --stop-after-init --no-http
```

Cubren (Fase 1): secuencias de folio, candados de cierre de NC (+ cierre forzado),
validación de clave documental, obsoletización de versión vigente previa, acuses
idempotentes, piloto >90 días, aprobación que versiona el documento, reclamación → NC
y validación del mapa de procesos.

Cubren (Fase 2): semáforo higher/lower_better con los 2 umbrales; cron de
indicadores idempotente + NC única con `nc_on_red` (y nada sin el flag); constraint
auditor≠dueño de proceso, cierre bloqueado sin disposición y "Generar NC" con origen
correcto; score/nivel por instrumento (R&O, IPER, patrimonial), FODA sin score y XOR
de acciones NC/Riesgo; clase de proveedor por umbrales; y RxD (cargar entradas,
candado sin acuerdos, acuerdos → tareas). **33 tests, 0 fallos.**

Cubren (Fase 3): calibración fuera de tolerancia que bloquea el equipo + crea NC y su
posterior liberación con calibración conforme; NPR de AMEF y candado de vigente sin
acción; PPAP con 18 elementos y candados de enviado/aprobado (PSW); incidente SST que
no cierra sin las 3 capas SCAT ni con acciones abiertas y aviso de graves; XOR de
acciones entre NC/Riesgo/AMEF/Incidente; brechas de competencia (DNC) por puesto; y NC
mayor cerrada → actividad de actualización de AMEF/plan de control. **15 tests, 0 fallos.**

Cubren (Fase 4): escalar una alerta operativa a NC (folio + equipo NC Internas + ligas
conservadas; bloqueo si ya es NC); `_calc_desperdicio` con el subproducto SALDO TEJIDO D
(y degradado a None sin categoría); `_calc_calidad_pq` con `mrp.revision.log`;
retro-vinculación segura (post_init sin equipos no truena; con equipo, liga el punto al
plan); y el puente de pesaje (rollo fuera de tolerancia → una sola alerta por rollo, en
`quimibond_sgi_pesaje`). **9 tests, 0 fallos.**

Cubren (Fase 4.6): flujo con modelo → botón abre el act_window del modelo correcto;
flujo documental → botón bloqueado; «Levantar NC» en mantenimiento correctivo crea la
NC ligada (idempotente); y CoA publicado adjunta el PDF a la entrega. **4 tests.**

Suite completa (quimibond_sgi + puentes): **64 tests, 0 fallos.**
