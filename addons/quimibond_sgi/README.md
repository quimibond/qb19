# quimibond_sgi — Sistema de Gestión Integral (Fases 1–8)

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
  Una medición **roja validada** de un indicador oficial con `nc_on_red=True`
  genera **una** NC pre-llenada (equipo NC Internas, origen "indicador") y la
  liga; desde 19.0.37.0.0 por persistencia (dos rojos seguidos, o uno si es
  crítico), ver «Lógica de indicadores».
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
| `disponibilidad_mantto` | Requiere paros de centros de trabajo | Devuelve None → captura manual (MT-01 en manual desde 2026-09-24) |
| `plantilla_rh` | Requiere plantilla presupuestada por puesto | Devuelve None → captura manual (RH-01 en manual desde 2026-09-24) |
| `reproceso` | kg de órdenes de los tipos de reproceso ÷ kg de hilo y fibra consumidos | Implementado (P-21, 19.0.39.0.0) |
| `inventario_diferencia` | \|valor de ajustes\| ÷ valor del inventario (capas de valuación) | Implementado (19.0.39.0.0) |

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

## Fases 5 y 6 — Política integral y Presupuesto/Pronóstico de ventas (v19.0.13.x)

Documentadas a detalle en `docs/SGI_DIAGRAMA_FLUJOS.html` (flujos completos con
cada botón y candado). Resumen:

- **Política Integral** (`sgi.policy`, folio `POL-`): borrador → vigente →
  obsoleta, con índice único de UNA política vigente (publicar obsoleta la
  anterior). Objetivos Integrales del ANEXO 6 ligados a indicadores; la salud
  del objetivo agrega la de los procesos de sus KPIs.
- **Presupuesto de ventas P-A28** (`sgi.sales.budget`, folio `PPV-`): un mismo
  par de modelos sirve el presupuesto mensual por mercado (F-P-A28-18, se
  aprueba y congela; revisiones con `action_revise`) y el pronóstico semanal
  por cliente (F-P-A28-13, documento vivo: solo borrador ⇄ revisado — mini-fase
  5.5, migración 19.0.13.7.0). Captura por plantilla Excel + wizard de
  importación, grid producto × mes, precarga desde pedidos/facturado; el precio
  SIEMPRE sale de la lista de precios. Aprobación exclusiva de Dirección con
  candado de cobertura de precios; envío de demanda neta al MPS; crons de
  cierre de mes, cobertura semanal y revaluación S2 (junio); reporte QWeb con
  ambas matrices. Menú espejado dentro de la app Ventas.
- **Diagrama de flujos y deuda técnica**: `docs/SGI_DIAGRAMA_FLUJOS.html` y
  `docs/SGI_DEUDA_TECNICA.md` en la raíz del repo.

## Fase 7 — Cierre de bucles ISO (v19.0.15.0.0)

Sale de la evaluación `docs/SGI_EVALUACION_ESTRUCTURA.md` (pasos 3–8):

- **Satisfacción del cliente (9001 9.1.2)**: KPI CA-02 automático desde las
  respuestas de la Encuesta de Satisfacción (promedio 1-5 → %), menú de
  respuestas bajo Medición, y cron trimestral que recuerda al Admin de Ventas
  distribuirla. El módulo NUNCA manda correos a clientes por sí solo.
- **DNC (P-A01)**: cron trimestral que cuenta las brechas abiertas y agenda a
  RH la distribución de la encuesta F-P-A01-17 (ahora con menú propio) y el
  plan de capacitación.
- **Alta documental ligada**: botón «Crear documento» en la solicitud de alta
  aprobada; el documento nace ligado a la solicitud (trazabilidad completa).
- **Emergencias (14001/45001 8.2)**: `sgi.emergency.plan` (PE-) con brigada,
  frecuencia y riesgos IPER/ambientales ligados; `sgi.emergency.drill` (SIM-)
  con candado: un simulacro con observaciones o no satisfactorio exige
  hallazgos y al menos una acción CAPA (`sgi.action.line.drill_id`, quinto
  origen del XOR). Cron diario de vencimientos. Menú: Riesgos y auditorías →
  Emergencias.
- **Alta de proveedores (8.4.1)**: estatus Nuevo/Aprobado/Bloqueado en el
  partner (pestaña SGI Proveedor, botones solo Jefe MAST). Un proveedor
  BLOQUEADO no puede recibir órdenes de compra confirmadas. Sin estatus =
  fuera del alcance SGI (no se bloquea nada — compatibilidad total).
- **MSA (IATF 7.1.5.1.1)**: `sgi.msa.study` (MSA-) por equipo de medición;
  Gage R&R con veredicto AIAG automático (<10% aceptable / 10-30% marginal /
  >30% inaceptable); inaceptable → actividad al Jefe MAST (el bloqueo del
  equipo es decisión humana). Smart button MSA en el equipo.

## Fase 8 — Señales operativas y checklist de auditoría (v19.0.15.1.0)

Aprovecha lo que Odoo ya registra, sin captura adicional:

- **Checklist → hallazgos**: el botón «Hallazgos del checklist» de la auditoría
  convierte las respuestas de la encuesta en hallazgos (No conforme → NC
  menor, Observación → observación), idempotente por respuesta. Se acabó la
  doble captura del auditor.
- **Devolución de cliente → NC automática**: validar una recepción que
  devuelve una entrega a cliente levanta una NC de origen Reclamación con
  cliente, producto y remisión ligados (fuente `devolucion_cliente`,
  apagable). Una devolución a proveedor NO dispara nada.
- **Señales operativas** (cron diario): ≥3 correctivas del mismo equipo en 90
  días → actividad a MAST sugiriendo NC y revisión del preventivo;
  reclamación abierta con SLA vencido → actividad de escalamiento.
- **Lecciones aprendidas (7.1.6)**: menú bajo Mejora continua con las NC
  cerradas con lección aplicada — causa raíz, eficacia y proceso. La memoria
  del sistema, que ya existía pero no se podía consultar.

## Ola «certificable» (v19.0.25.0.0, auditoría 2026-08)

Cierra las brechas normativas detectadas por la segunda auditoría (reporte con
adopción medida en producción):

- **Requisitos legales y evaluación del cumplimiento** (14001/45001 §6.1.3 y
  §9.1.2): `sgi.legal.requirement` con frecuencia de evaluación, botones
  Cumple/Parcial/No cumple, NC mayor automática (fuente
  `requisito_legal_incumplido`) y cron de evaluaciones vencidas y permisos por
  vencer. Menú: Riesgos y auditorías → Requisitos legales.
- **Partes interesadas** (§4.2): `sgi.interested.party` con necesidades y
  expectativas, adopción como requisito, ligas a riesgos/legales y revisión
  periódica vigilada por cron. Menú: Panel → Partes interesadas.
- **Objetivos con plan de acción** (§6.2.2): el objetivo integral gana
  chatter y líneas CAPA propias (sexta fuente del XOR); su salud ahora agrega
  también el último semáforo de sus KPIs (deuda B.18).
- **Retención de registros** (§7.5.3): años de retención + disposición final
  por documento, filtro «Sin retención definida» y reporte imprimible «Tabla
  de retención».
- **Consulta y participación** (45001 §5.4): la encuesta F-P-A10-05 queda
  cableada (cron semestral a RH) y la RxD gana las entradas 11 (cumplimiento
  legal) y 12 (participación + quejas internas).
- **Encuesta de satisfacción configurable**: CA-02 puede re-apuntarse desde
  Ajustes a la encuesta histórica (575 respuestas viven en una encuesta
  archivada distinta de la sembrada).
- **MOC** (9001 §6.3 / 45001 §8.1.3): categoría de Aprobaciones «Cambio de
  proceso / infraestructura» con gate duro (motivo + procesos afectados +
  evaluación de riesgos).
- **Diseño y desarrollo** (§8.3): proyecto semilla con etapas-gate
  8.3.2–8.3.6 y candado de evidencia en la transferencia.
- **Correo crítico**: incidente grave/fatal, NC mayor y bloqueo por
  calibración avisan por correo (plantillas editables) además de la actividad.
- **Deudas cerradas**: B.16 (recalcular medición), B.19 (línea del programa
  con cierre real), C.25 (AMEF↔plan de control), D.28 (acuses de la
  política), D.29 (constante de atención máxima), D.30 (button_box del
  partner consolidado). 8D imprimible desde la NC; certificado de calibración
  adjunto y obligatorio en externas; Diagnóstico con sección «Contexto y
  cumplimiento».

**Diferido a propósito** (requiere datos cargados, no código del addon):
SPC/Cpk desde quality.check (necesita metrología cargada). La **carga de
registros** (política, riesgos, acuses, validación de mediciones, programa de
auditorías, metrología, competencias por puesto, 8.4.1, presupuesto) es la
Ola 0 operativa: el Diagnóstico del SGI es su checklist.

## Integraciones Sign / eLearning y digest semanal (v19.0.26.0.0)

Cierra los diferidos de la ola certificable usando las apps **Firma (Sign)**
y **eLearning**, ya instaladas en la instancia (ahora dependencias del addon):

- **Acuses con firma electrónica**: en el documento controlado (pestaña
  Acuses de lectura, solo Jefe MAST) se liga una **plantilla de Sign** hecha
  con el PDF del documento y su campo de firma; el botón «Enviar acuses a
  firma» crea una solicitud por empleado pendiente (idempotente, salta a
  quien no tiene contacto/correo) y el cron diario **sella el acuse** cuando
  la solicitud queda firmada (la firma electrónica es la evidencia; pasa el
  candado A2 vía superusuario del cron). La columna «Firma electrónica» del
  acuse muestra el avance sin exigir permisos de Sign al empleado (related
  almacenado).
- **eLearning → DNC**: menú Empleados → Competencias (SGI) → «Cursos y
  competencias (eLearning)»: lista editable que mapea curso → competencia
  (`hr.skill`) y nivel otorgado. El cron diario registra o **sube** (nunca
  baja) la competencia de los asistentes con curso terminado, cerrando la
  brecha en la DNC sin captura manual.
- **Digest semanal**: correo-resumen a Jefe MAST + Dirección (mismos
  destinatarios del correo crítico) con NC abiertas, acciones vencidas, KPIs
  en rojo, evaluaciones legales vencidas/incumplidas, riesgos altos sin
  tratamiento, acuses pendientes y contexto por revisar. Cada métrica corre
  en su propio savepoint; si todo está en cero lo dice («sin pendientes»).

Configuración manual restante: crear las plantillas de Sign por documento
(una vez, colocando el campo de firma sobre el PDF) y mapear los cursos
existentes a competencias.

## Catálogo único — fase 1 del rediseño (v19.0.29.0.0, 2026-09)

El SGI pasa a ser el **catálogo único de actividades** de la empresa: cada
actividad existe una vez y de ella salen el procedimiento, la descripción de
puesto, los indicadores y la lista de automatización. Fase 1 de 5 (las
siguientes: evidencia y medición por ejecutor real, motor de reglas,
revisiones y descripción de puesto, automatización/tableros/seguridad).

- **Roles como filas** (`sgi.activity.role`): cada actividad dice qué puesto
  la **ejecuta**, **aprueba**, **participa** o **se entera**, con condición
  opcional. Exactamente un «ejecuta» (cero si `automation_level_current =
  automatico`) y a lo más un «aprueba» sin condición. «Puestos responsables»
  (`responsible_job_ids`) queda calculado desde los roles «ejecuta» sobre la
  misma tabla de antes; escribirlo crea/quita roles. El puesto (`hr.job`)
  muestra sus actividades y contadores por rol.
- **Actividad**: `instruction_id` (instructivo IT), `value_class` (VA / NVA
  necesaria / NVA), nivel de automatización actual/meta y método, `active`
  (la carga archiva, nunca borra) y numeral único por proceso.
- **Proceso**: ficha (`start_trigger`, `end_trigger`, `inputs`, `outputs`),
  `replaced_document_ids`, `company_id` y clave única **por empresa**. Dueño
  sin usuario o archivado → salud en rojo y aviso en la ficha. Aprobador por
  omisión = usuario del dueño; Vo.Bo. por omisión = parámetro
  `quimibond_sgi.vobo_user_id` (id del usuario Jefe de MAST y SGI; **hay que
  capturarlo** en Parámetros del sistema).
- **Tipos de documento como datos** (`sgi.document.type`, Configuración →
  Tipos de documento): patrón de clave (`PR-{process}`,
  `IT-{process}-{seq:02d}`, `F-{process}-{seq:02d}`, `CO-{seq:02d}`,
  `MA-{process}-{seq:02d}`, `DP-{seq:02d}`), si exige proceso y la
  nomenclatura heredada que se sigue aceptando mientras se migra (`P-A28`,
  `F-P-A28-04`…). `sgi_doc_type` se conserva calculado desde el tipo.
  `sgi.document.type.sgi_next_code(proceso)` propone la siguiente clave.
- **Revisión entera** (`sgi_revision`; `sgi_revision_label` = «05» para
  imprimir). Única por clave + revisión + empresa entre documentos activos;
  una revisión nueva va por arriba de la última de su clave y no baja (salvo
  `sgi_revision_correction` de un Jefe MAST); una clave, aun dada de baja, no
  se reutiliza en otro tipo ni en otro proceso. Lo no numérico que había queda
  en `sgi_revision_legacy`.
- **Clave anterior** (`sgi_previous_code`): al cambiar la clave se guarda sola;
  la búsqueda «Clave SGI» y `_sgi_find_by_code` la encuentran 12 meses.
- **Multiempresa**: `company_id` y regla por empresa en proceso, actividad,
  rol, liga, flujo, responsabilidad y tipo de documento.
- **Grupo Administrador SGI** (implica Jefe MAST): el único que carga por API,
  fusiona puestos y administra tipos de documento. Se asigna a mano.

### Fase 1.1 — familias, roles relativos, vacantes y medición

- **Familias de puestos** (`sgi.job.family`, Procesos → Familias de puestos):
  el mismo rol en puestos que solo cambian por nivel o letra (OP-TEJ, OP-INS,
  LAB…). Un puesto está en una sola familia por empresa; `hr.job.sgi_family_id`
  se calcula desde la familia. Un rol se asigna a un **puesto**, una **familia**
  o un **rol relativo** (`target_type`); la regla de un solo «ejecuta» cuenta
  filas, así que una familia cuenta como una. `responsible_job_ids` expande las
  familias. Una familia está vacía solo si todos sus puestos lo están.
- **Roles relativos** (`relative_role`): solicitante, jefe del solicitante,
  quien detecta, área responsable, dueño del proceso. Solo el dueño se
  resuelve a un puesto; los demás no cuentan para «puesto sin persona» ni
  para adherencia.
- **Vacante aprobada** (`hr.job.sgi_vacancy_approved` / `sgi_vacancy_until`,
  pestaña Actividades SGI del puesto): un puesto sin empleados con vacante
  vigente se carga con advertencia; sin ella, es error.
- **Método de medición obligatorio** (`measure_method`): Odoo, por
  consecuencia (`measure_proxy_activity_id`: copia el conteo y el estado de la
  actividad que la prueba; sin ciclos), correo y manual (grises «pendiente»
  hasta la fase 2), muestreo (`sample_cadence`) y no aplica (exige
  `measure_justification`). «Sin medir» solo con el procedimiento en borrador:
  un procedimiento no pasa a piloto con actividades sin método, ni a vigente con
  métodos incompletos (el error las lista). La ficha del proceso y
  Procesos → Medición por método muestran cuánto se mide de verdad
  (odoo + consecuencia entre el total).
- **Quién ejecutó** (`measure_user_field`, p. ej. `create_uid`): el cron hace un
  `read_group` por ese campo en la ventana de 30 días y clasifica cada
  ejecución en `measure_executor_json`: **correcto**, **otro_puesto**,
  **generico** (cuentas compartidas del parámetro
  `quimibond_sgi.generic_user_ids`; la migración pone 92,184,80),
  **sin_empleado** o **sistema** (OdooBot). Adherencia = correcto entre todo lo
  que no es sistema. Avisos en la ficha (`measure_warning`): adherencia < 80 %,
  ejecuciones genéricas o sin empleado, y actividad «manual» con más de la mitad
  de ejecuciones del sistema. TODO fase 2: pasa a `sgi.activity.evidence`.
- **El detalle vive en un modelo, no en JSON** (`sgi.activity.exec.stat`): una
  fila por actividad, semana (lunes), usuario y clase, con empleado, puesto y
  familia al momento de medir. Lo escribe el cron (reemplaza las 4 semanas que
  recalcula; si nada cambió no escribe); nadie lo edita. La adherencia y los
  contadores de la actividad salen de esas 4 semanas.

### Pantallas (reorganizadas el 22-sep-2026)

El menú del SGI tiene **seis entradas y máximo tres niveles** (antes eran 14,
con tres tableros y las actividades en tres lugares). Todo el árbol vive en
`views/sgi_menus.xml`; los xmlids no cambiaron.

| Menú | Qué hay |
|---|---|
| **Inicio** | Tablero (hoja de cálculo «Salud del SGI», se arma con los pivotes de Análisis), Mis actividades, Mis acciones, Mis mediciones, Mis procedimientos, Mis acuses |
| **Procesos** | Mapa de procesos (kanban por tipo con barra de medición, dueño, % medido y adherencia), Actividades, Roles por puesto, Cadena de actividades, Flujos, Riesgos y oportunidades, Requisitos legales, Partes interesadas |
| **Mejora** | No conformidades, Concentrado de NC, Acciones, Reclamaciones, Quejas y sugerencias, Incidentes SST, Mejoras, Lecciones aprendidas, Auditorías y su programa, Planes de emergencia, Simulacros |
| **Documentos** | Documentos, Cambios documentales, Migración de formatos |
| **Análisis** | Política → Objetivos → Indicadores → Mediciones, Satisfacción del cliente, Quién hace qué, Tendencia de ejecuciones, Cobertura de medición, Cumplimiento de procedimientos, Diagnóstico, Revisión por la Dirección |
| **Configuración** | Cargar catálogo (solo Administrador SGI), Familias de puesto, Tipos de documento, Ajustes, Áreas, Normas… (solo MAST) |

Calidad preventiva (AMEF, PPAP, planes de control, metrología) y el Pareto de
alertas viven en la app **Calidad**; evaluación de proveedores en Compras,
presupuesto en Ventas y competencias en Empleados.

**Ficha del proceso**: 3 botones (Actividades, En rojo, NC abiertas), botón
«Imprimir procedimiento» arriba y 4 pestañas: *Ficha* (objetivo, disparadores,
entradas y salidas, indicadores y riesgos), *Actividades* (lista con quién
ejecuta y aprueba, método, estado y adherencia; abajo el texto del
procedimiento y las firmas), *Conexiones* (flujos y ligas) y *Documentos*.

**Ficha de la actividad**: arriba el número, el nombre, el proceso y el método
(y los avisos en amarillo si los hay); 4 pestañas: *Qué y quién* (roles,
descripción, instructivo, formatos, valor y automatización), *Cómo se mide*,
*Quién la ejecutó* (últimas 4 semanas) y *Cadena*.

### Carga por API: `sgi.process.load_payload(payload, dry_run=False)`

Por JSON-RPC o por el conector MCP (`call_model_method` sobre `sgi.process`;
la migración habilita el modelo en MCP con llamadas a métodos). Alta o
actualización por llave natural (entregable `code`; proceso `code`; actividad
proceso + paso; rol actividad + rol + puesto; «recibe» actividad + entregable;
indicador `code`). Escribe solo lo que cambió: la segunda corrida del mismo
JSON reporta cero cambios. `dry_run` corre todo en un savepoint que se deshace
y reporta lo que se crearía, actualizaría o archivaría. Una transacción por
proceso: si una actividad falla, el proceso entero se deshace y se reporta.
Las actividades de un proceso cargado que no vienen en el JSON se **archivan**
(`"archive_missing": false` lo evita); su texto original se queda en ellas.

```json
{
  "dry_run": true,
  "deliverables": [
    {"code": "C2-PEDIDO", "name": "Pedido confirmado", "model": "sale.order",
     "domain": "[('state', '=', 'sale')]", "date_field": "date_order",
     "user_field": "user_id", "acceptance_criteria": "Precio y fecha confirmados"},
    {"code": "C2-OC-CLIENTE", "name": "Orden de compra del cliente", "document": "F-C2-01"}
  ],
  "processes": [{"code": "C2", "name": "Ventas", "process_type": "cadena de valor",
                 "owner_employee_id": 244,
                 "replaced_documents": ["P-A28"], "replaces": ["P-VEN"]}],
  "activities": [
    {"process": "C2", "number": "C2.03", "stage": "A. Pedido",
     "name": "Verificar número de parte y precio", "value_class": "nva_n",
     "roles": [{"role": "ejecuta", "family": "PEDIDOS"},
               {"role": "informa", "relative": "dueno_proceso"}],
     "inputs": [{"code": "C2-OC-CLIENTE", "days": 1}],
     "outputs": ["C2-PEDIDO"],
     "measure": {"method": "entregable", "deliverable": "C2-PEDIDO"},
     "cadence": "evento",
     "automation": {"current": "manual", "target": "asistido"}}
  ],
  "indicators": [{"code": "C2-01", "name": "Entregas completas y a tiempo (OTIF)",
                  "process": "C2", "frequency": "monthly",
                  "formula": "Entregas completas en la fecha compromiso ÷ entregas del mes",
                  "source": "Fecha compromiso del pedido contra fecha de la entrega",
                  "responsible_employee_id": 244}]
}
```

- **Llaves desconocidas = error**, con la ruta completa
  (`processes[0].activities[5].origin_note`) y la lista de llaves válidas de
  ese nivel. Si hay una, no se carga nada. Los indicadores van al nivel
  principal con `process`, no dentro del proceso.
- **`replaces`** (proceso): códigos de procesos que el nuevo sustituye. En la
  carga real se archivan **con sus actividades** (que conservan su texto),
  **sus ligas y sus flujos** (con el motivo «Proceso X sustituido por C2»), y
  el proceso nuevo **adopta sus indicadores y riesgos activos**. Su chatter
  dice «Sustituido por C2» y el reporte lista cada cosa archivada o movida;
  con `dry_run` solo se reporta. Un código que no existe es error. Los
  documentos del proceso viejo siguen vigentes: se vuelven obsoletos solo al
  publicar el proceso nuevo (`replaced_documents` + `publish`).

**Estructura vigente.** El SGI muestra por defecto solo lo de procesos
activos: indicadores y riesgos con el filtro «De procesos vigentes», la
cadena de actividades con «Estructura vigente» y los flujos con «Mapa
vigente» (solo los calculados de entregables entre procesos activos; los
flujos no se capturan a mano). Los indicadores y riesgos sin proceso o con su
proceso archivado siguen activos y midiéndose, y se ven con el filtro
«Pendientes de proceso nuevo». Los crons ignoran lo archivado: la cadena solo
evalúa ligas entre actividades activas, el resumen semanal solo cuenta
indicadores y riesgos de procesos vigentes, la NC automática de un indicador
de proceso archivado nace sin proceso y la revisión de un riesgo así va al
Jefe MAST.
- **Indicadores**: `formula` y `source` (texto; salen en la ficha y en el
  procedimiento impreso), `frequency` `monthly`/`weekly`, y el responsable
  como `responsible_employee_id` (su usuario) o `responsible` (id o login).
  **Modos genéricos (P-1):** `calc_mode` `actividad_a_tiempo` con
  `activity` (`"C2.17"` o `"PROC:NUM"`) toma el % a tiempo del cumplimiento
  semanal de esa actividad; `entregable_completo` con `deliverable` (código)
  toma el % de lo entregado en el periodo que cumple `complete_domain` (sin
  `deliverable`, usa el entregable con el que se mide `activity`). Sin
  entregas o sin plazo medible en el periodo la medición queda pendiente. El
  reporte avisa si falta la actividad, el entregable o su «completo».
- **`number`**: `"C2.03"` (clave del proceso + paso) o el paso como entero
  (`3`). Es un número, no texto: el numeral que se imprime se calcula y se
  renumera solo si cambia la clave del proceso. Otro formato es error.
- **`stage`**: la etapa del proceso («A. Pedido»); se crea la primera vez.
- **`deliverables`** se procesan antes que los procesos. `model` + `domain` +
  `date_field` (+ `user_field`) dicen **cuándo quedó entregado**: con ellos, la
  actividad que lo entrega se mide con `"measure": {"method": "entregable"}`
  y no lleva `evidence` (se captura una vez, en el entregable). Si la
  actividad entrega un solo entregable con modelo, `deliverable` se puede
  omitir. Sin `model`, el entregable es documental.
- **`inputs`**: lo que la actividad recibe; `"C2-PEDIDO"` o
  `{"code": "C2-PEDIDO", "days": 2}`. `days` es el **plazo en días hábiles**
  (lunes a viernes) para que llegue a esta actividad; pasado el plazo sin que
  ella ejecute, el eslabón está atorado. `0` o sin `days` = sin plazo.
  **Vencimiento por campo (P-4):** `{"code": "C2-PROGRAMA", "due_field":
  "scheduled_date", "offset_days": -2}` vence contra esa fecha del registro
  de la entrada más el margen en días hábiles (negativo = antes), en lugar
  de días desde que llegó; el campo debe existir y ser de fecha en el modelo
  del entregable (si no, error). Con `match` cuando la salida es otro modelo.
- **Relaciones para ligar entradas (P-3):** `budget.analytic` (app
  Presupuestos) tiene `sgi_sales_budget_id` → presupuesto de ventas del SGI
  (E1.02, `"match": "sgi_sales_budget_id"`); `documents.document` tiene
  `sgi_doc_change_id` → última solicitud de cambio documental aprobada que
  lo dejó así, la pone el propio cambio al aplicarse (E2.02, `"match":
  "sgi_doc_change_id"`); `sgi.management.review` tiene `audit_ids` →
  auditorías del periodo, las llena «Cargar entradas» y se ajustan a mano
  (E2.14, `"match": "audit_ids"`). Cuando la salida apunta a varias entradas
  manda la última: la salida no podía hacerse antes.
- **`outputs`**: códigos de lo que la actividad entrega.
- **Las ligas y los flujos no se cargan**: salen solos de `inputs`/`outputs`
  (una liga por quien entrega × quien recibe; un flujo si cruzan proceso) y no
  se editan a mano. Si una no aplica se desactiva con su motivo en la
  actividad (pestaña Cadena). `links_to`, `links`, `flows` e `inputs`/`outputs`
  como texto del proceso son **error**.
- **Un ejecutor por actividad.** Si según el caso la ejecuta otro puesto, son
  dos actividades. Una familia de puestos (`family`) solo cuando los puestos
  son intercambiables (PEDIDOS). `condition` solo va en `aprueba` o `informa`
  («arriba del monto que se fije»); en `ejecuta` o `participa` es error.
- **Inicio y fin del proceso** no se capturan: inicia con lo que sus
  actividades reciben y ninguna produce; termina con lo que entregan y ninguna
  recibe. `start_trigger`/`end_trigger` se ignoran con aviso.
- Puestos por id o por nombre normalizado (sin mayúsculas, espacios y saltos
  de línea colapsados). **Nunca se crean**: si no existe o es ambiguo, error.
- Dueño: `owner_employee_id`, o `owner_job` = el único empleado activo con
  usuario en ese puesto (si hay 0 o varios, aviso y queda sin dueño).
- `evidence` (método `odoo`): la primera fuente `odoo_model` va a los campos
  de medición de la actividad; las demás llegan en la fase 2.

Ejemplo por MCP: `call_model_method("sgi.process", "load_payload", [payload])`.

### Actividades específicas (19.0.30.0.0)

Cada actividad contesta siete preguntas; lo que falte queda en
**Análisis → Faltantes de especificación** (pivot proceso × faltante) y en un
aviso arriba de la ficha.

| Pregunta | Campo | Llave de la carga |
|---|---|---|
| Qué | nombre + `check_against` | `name`, `check_against` |
| Quién | rol `ejecuta` | `roles` |
| Dónde | `exec_channel`, `odoo_menu_id`, `external_system`, `location_id`, `workcenter_id`, `place_note` | `where: {channel, menu (xml_id), external_system, location (nombre completo), workcenter (código), place}` |
| Cómo | instructivo o `how_steps` | `instruction`, `how_steps` |
| Cuándo | días en la entrada, o `due_weekday` / `due_business_day` | `inputs[].days`, `due: {weekday: 0-6}` o `{business_day: 1-23}` |
| Terminado | `done_criteria` + `complete_domain` del entregable | `done_criteria`, `deliverables[].complete_domain` / `complete_criteria` |
| Si falla | `on_fail` + rol `escala` con `after_days` | `on_fail`, `roles: [{role: "escala", relative: "dueno_proceso", after_days: 3}]` |

- **Entrada condicionada:** `inputs[].applies_domain` (dominio sobre el
  registro de entrada) y `applies_note`. Si no se cumple, la actividad no
  aplica a ese registro: no vence ni cuenta como atrasada. `inputs[].match` es
  el campo de la salida que apunta a la entrada cuando son modelos distintos
  (ej. `sale_id`); con el mismo modelo no hace falta.
- **Estado del proceso:** `state` (`borrador` o `piloto`) y `publish: true`.
  Publicar (botón o carga) exige cero faltantes de tipo error en actividades
  y que cada indicador tenga meta, fórmula, fuente, responsable y frecuencia.
  En borrador y piloto todo se carga incompleto; la carga reporta los
  faltantes como avisos, agrupados por código.
- **Indicadores:** además `target` (meta), `unit`, `direction` (`up`/`down`),
  `baseline_value`, `target_date`.
- **Verbos:** `quimibond_sgi.vague_verbs` (error: «dar seguimiento»,
  «gestionar»…) y `quimibond_sgi.compare_verbs` (piden `check_against`), en
  Parámetros del sistema; cambiarlos recalcula los faltantes.
- **Medición semanal** (**Análisis → Cumplimiento semanal**): por actividad y
  semana, entradas aplicables, salidas hechas, completas, a tiempo (días
  hábiles del calendario de la compañía, con festivos) y vencidas abiertas.
  Va en su propio modelo (`sgi.activity.week.stat`), no en `exec.stat`, porque
  aquel tiene un renglón por usuario y repetir los totales los multiplicaría.
- **Por proceso:** % de actividades en Odoo, % en papel/correo/teléfono y %
  de lo que se hace en Odoo que se mide solo.
- Escalamiento automático (cron que crea actividades al vencer): fase 2. Los
  campos y el rol `escala` ya existen.



### Limpiezas a mano (con reporte previo)

- **Puestos duplicados y nombres con saltos de línea**:
  `hr.job.sgi_merge_duplicate_jobs(dry_run=True)` reporta; con `dry_run=False`
  conserva el puesto con más empleados, mueve TODAS sus referencias (empleados,
  roles, documentos, responsabilidades…) y archiva los demás. Hoy hay
  duplicados por mayúsculas (Director de Ventas, Coordinador de Ventas
  Industrial, Administrador de Ventas y Marketing…) que hacen ambigua la carga:
  correrla **antes** de cargar los 14 procesos.
- **Modelos de Studio vacíos** (`x_emp_activity`, `x_no_conformidades`,
  `x_actividades_obligato`, `x_calendario_de_obliga`):
  `sgi.config.sgi_drop_empty_studio_models(dry_run=True)`. Solo borra los que
  siguen en cero, con sus vistas, acciones y menús. Fuera de un update (borrar
  un modelo recarga el registro).

### Migración (19.0.29.0.0)

`pre-migrate`: revisión y nueva revisión de texto a entero; quita la
unicidad global de la clave de proceso. `post-migrate`: `company_id = 1`, un
rol «ejecuta» por cada puesto responsable (las actividades heredadas con 0 o
varios ejecutores se listan en el log; la regla se les aplica al editar sus
roles), tipo de documento como registro, aprobador = usuario del dueño y
`sgi.process` habilitado en MCP. Nada se borra; los 25 procesos actuales se
archivan después, desde datos, cuando sus actividades y documentos pasen a los
14 nuevos.

Verificar después de desplegar:

```sql
select count(*) from sgi_activity_role;                       -- ≈ roles migrados
select data_type from information_schema.columns
 where table_name = 'documents_document' and column_name = 'sgi_revision';  -- integer
select count(*) from documents_document where sgi_is_controlled and sgi_doc_type_id is null;
```

## Alcance multiempresa (decisión de arquitectura)

La instancia tiene varias compañías. Desde la fase 1 del catálogo
(v19.0.29.0.0) el **catálogo** (procesos, actividades, roles, ligas, flujos,
responsabilidades, tipos de documento) lleva `company_id` y regla por empresa.
El resto del SGI sigue siendo exclusivo de PRODUCTORA DE NO TEJIDOS QUIMIBOND
(PNTQ) y sus modelos NO llevan `company_id` (salvo el
presupuesto de ventas, que sí lo necesita para valuar). Es una decisión
consciente: los registros SGI son globales de la planta. Si otra compañía
adoptara el SGI, la Fase correspondiente deberá añadir `company_id` por etapas
(evidencia primero: NC ya lo hereda de quality; luego riesgos, auditorías,
documental) — no intentar hacerlo de golpe.

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

Cubren (catálogo, fase 1 — `test_catalog_fase1.py`): exactamente un ejecutor
(cero si es automática) y un aprobador sin condición; cambio de ejecutor en
una sola escritura; `responsible_job_ids` heredado crea roles; numeral único
por proceso; carga idempotente (segunda corrida y su dry-run sin cambios),
dry-run que no escribe, puesto inexistente = error sin crear nada y proceso
deshecho, archivado de actividades que ya no vienen, puesto ambiguo; patrones
de tipo de documento y siguiente clave; revisión única y creciente; clave no
reutilizable en otra familia; clave anterior encontrada; dueño inválido = rojo.
**No se han corrido todavía**: el SGI depende de Enterprise y no entra al CI;
correrlas en la base de pruebas de Odoo.sh con el comando de arriba.

## Lógica de indicadores: detalle y confiabilidad (I-1, I-2, 19.0.36.0.0)

Spec: «Lógica de indicadores del SGI» (2026-09-24). Código en
`models/sgi_indicator_detail.py`, pruebas en `tests/test_indicator_detail.py`.

- **I-1, cada medición guarda su detalle.** `numerator`, `denominator`,
  `sample_size` y los registros que la forman (`detail_model` + `detail_ids`);
  «Ver registros» abre esa lista guardada, no una consulta nueva. Un modo lo
  aporta con `_detail_<modo>(date_from, date_to)` → `{value, numerator,
  denominator, model, ids}`; primera tanda: a tiempo, completo, OTIF, OTD,
  entregas completas, embarques sin error, pedidos cancelados, cierre de NC,
  calidad PQ, preventivo, DSO y cartera vencida. Los demás modos conservan su
  `_calc_` y guardan al menos modelo, ids y casos cuando la tabla de
  evidencia conoce su universo. `sgi.indicator._sgi_aggregate(mediciones)`
  da el valor de varios periodos como suma de numeradores entre suma de
  denominadores (nunca promedio de porcentajes); el pivote trae ambos.
- **I-2, el cero no es «sin dato».** La medición automática sin registros
  queda en `sin_dato` (gris, sin semáforo, no pide captura). Con menos casos
  que `quimibond_sgi.indicator_min_sample` (5) se marca «muestra chica».
  El indicador nace **en prueba** (`status`) y el Jefe MAST lo pasa a
  **oficial** tras revisar una vez la lista de registros; solo un indicador
  oficial, con dato y con muestra suficiente abre NC (el interruptor
  `nc_on_red` sigue siendo necesario). «Medir desde» (`measure_from`) evita
  crear mediciones de periodos anteriores al dato confiable. El último
  valor y semáforo del indicador salen de la última medición **con dato**.
  Los 93 indicadores existentes quedan en prueba al actualizar: es el freno
  a las 20 NC de golpe que pedía la spec, sin tocar `nc_on_red`.
- Carga JSON: llaves `status` y `measure_from` en `indicators`.
- **I-3, fórmulas corregidas (19.0.38.0.0).** Tres modos nuevos con detalle
  (`models/sgi_indicator_i3.py`), y la migración cambia MA-05, EX-01 y EX-02
  a ellos si seguían en el modo viejo:
  `desperdicio_kg` (kg que entran a las ubicaciones de desperdicio, órdenes y
  ajustes, ÷ kg de hilo y fibra consumidos en órdenes, 3 meses móviles; solo
  líneas en kg; parámetros `waste_location_ids` 39,43 y
  `waste_input_categ_ids` 350,356), `margen_ebitda` ((ingresos − costo de
  ventas − gastos de operación) ÷ ingresos por tipo de cuenta, 12 meses
  móviles; fuera depreciación, otros ingresos y financieros 701) y
  `compras_mp_vs_ventas` (facturas de proveedor de la categoría Materia Prima
  menos notas de crédito ÷ ingresos, 3 meses móviles; parámetro
  `raw_material_categ_id` 318). Las metas y el objetivo integral las ajusta
  MAST: MA-05 arranca ≈ 16 % contra una meta de 0.8 %, EX-02 ≈ 35 % contra 78 %.
  El margen sobre pedidos (`margen_ventas`) sigue disponible para un indicador
  nuevo de C1/C2.
- **P-21 y automáticos sin dato (19.0.39.0.0).** Diagnóstico del 24-sep-2026
  de los 9 automáticos que siempre salían sin dato. Tres se miden desde Odoo
  (`models/sgi_indicator_p21.py`): `reproceso` (MA-04: kg producidos por las
  órdenes cuyo tipo de operación está en `quimibond_sgi.rework_picking_type_ids`,
  sembrado 106 Re-proceso Tintorería y 107 Re-proceso Acabado, ÷ kg de hilo y
  fibra consumidos; solo líneas en kg, así que una orden de reproceso en metros
  no entra; «Acabado producto en proceso» se agrega al parámetro cuando
  producción lo confirme), `inventario_diferencia` (AL-01: |valor de los ajustes
  de inventario del mes| ÷ valor del inventario al cierre, desde las capas de
  valuación) y `consumo_energia` (TR-03: facturado por el proveedor de energía ÷
  toneladas de hilo y fibra consumidas; pesos por tonelada, antes total en
  pesos). El denominador en kg es uno solo (`_sgi_kg_consumed`, el de MA-05):
  cada kg cuenta una vez aunque pase por tejido y tintorería. MT-01, MT-02,
  RH-01 y RH-02 pasaron a manual en producción sin retirarse (nadie captura
  paros, preventivos, plantilla autorizada ni habilidades por puesto); MA-02
  sigue sin capacidad configurada y VE-02 sin presupuesto aprobado.
- **I-5, NC por persistencia (19.0.37.0.0).** «NC automática» (`nc_on_red`)
  ya no abre una NC por un solo rojo: hacen falta **dos periodos seguidos en
  rojo** (semana o mes, según la frecuencia), o **uno** si el indicador está
  marcado como **crítico** (`critical`; llave `critical` en el JSON). Nunca
  con «sin dato» ni «muestra chica», y mientras la NC del periodo anterior
  siga abierta la medición nueva se liga a esa misma NC en vez de abrir otra.
  Sigue exigiendo indicador oficial y medición validada.

## Fórmula configurable (19.0.40.0.0)

Modo de cálculo `configurable` (`models/sgi_indicator_formula.py`): el
indicador se calcula con dos **términos** capturados en la pestaña Fórmula
(`sgi.indicator.term`), numerador y denominador. Cada término dice de qué
modelo sale, con qué filtro (dominio de Odoo), sobre qué campo de fecha se
recorta la ventana, cómo se agrega (contar, sumar un campo o sumar su valor
absoluto), por qué factor se multiplica (−1 invierte el signo, 0.001 pasa kg
a toneladas) y qué ventana usa (el periodo, 3 o 12 meses móviles, o acumulado
hasta el cierre). Si el modelo tiene `company_id`, se filtra solo a la
compañía del KPI. La medición guarda numerador, denominador y los registros
del numerador, igual que los modos con detalle; el valor va ×100 cuando la
unidad del indicador lleva `%`.

- **Validación:** el dominio pasa por `safe_eval` (nunca `eval`) y por una
  búsqueda de prueba al guardar; los campos de fecha y de suma tienen que
  existir en el modelo con el tipo correcto; el factor no puede ser cero. Un
  indicador tiene un solo numerador y un solo denominador.
- **Quién edita:** solo el grupo Administrador del SGI (`group_sgi_admin`);
  los demás la ven. La pestaña queda de solo lectura para quien no puede.
- **Trazabilidad:** todo cambio de un término (crear, modificar, quitar) queda
  en el chatter del indicador con el antes y el después, y **regresa el
  indicador a «prueba»**: hay que volver a revisar la lista de registros.
- **En paralelo:** un indicador que sigue en un modo de código pero ya tiene
  términos corre la fórmula en cada medición nueva y guarda su resultado en
  `parallel_value` / `parallel_numerator` / `parallel_denominator`
  («Fórmula en paralelo» en la medición). La regla es migrar un indicador a
  `configurable` solo después de un mes con el mismo número.
- **Sembradas** (`data/sgi_indicator_formula_data.xml`, noupdate, con ids de
  producción en los dominios): MA-05 desperdicio, MA-04 reproceso (solo
  Re-proceso Tintorería), AL-01 diferencia de inventario, TR-03 energía por
  tonelada y EX-02 compras de materia prima. Diferencias conocidas con el
  modo de código: MA-04 por fórmula no excluye subproductos de la orden de
  reproceso; EX-02 por fórmula usa la fecha contable de la línea en vez de
  la fecha de factura. EX-01 (EBITDA) no cabe: son tres términos.
- Fuera del modo: los ids de ubicaciones, categorías, tipos de operación y
  proveedor viven en el texto del dominio, no en parámetros; en una copia con
  ids distintos la fórmula apunta a otra cosa.

Pruebas: `TestIndicatorFormula` 01–07.

## No surtir lotes sin liberar (P-7, 19.0.35.0.0)

Las requisiciones de producción (Requisición MP y Requisición PP y PT) no se
validan con un lote que Calidad no ha liberado (C6.10). «Sin liberar» es un
lote que sigue en una ubicación de espera (6 Entrada MP, cuarentenas,
Inspección, Liberación) o cuyo último control de calidad falló. El bloqueo es
duro y avisa qué lotes y por qué; lo que sigue es avisar a Control de
producción y a Calidad. Parámetros: `quimibond_sgi.release_block_enabled`,
`release_block_picking_type_ids` (113,210) y `unreleased_location_ids`
(324,44,36,246,45); se siembran al actualizar y se ajustan en Parámetros del
sistema. Código: `models/sgi_release.py`, pruebas `tests/test_release.py`.

**EX-07 Días de cartera (P-11).** La medición de agosto marcaba 134 porque
se calculó antes del filtro por compañía (sumaba la cartera del grupo). Al
recalcular daba 35, también mal: la cartera se tomaba como el saldo pendiente
*hoy* de las facturas de entonces (lo cobrado en septiembre ya no contaba) y
llevaba IVA mientras las ventas no. Ahora la cartera es el saldo contable de
las cuentas de clientes al cierre del periodo y las ventas de 90 días van con
IVA: agosto 2026 ≈ 51 días. Las facturas viejas sin cobrar (2018–2020, ~3.3 M)
pesan ~6 días; sacarlas es decisión de Finanzas, no del cálculo.

**Traslado Embarcar → su entrega.** `stock.picking.sgi_delivery_picking_id`
(«Entrega que surte») liga cada traslado interno con pedido a la orden de
entrega del mismo pedido (la abierta más próxima; la última si todas están
hechas; editable). Con él C2.21 y C2.26 se miden contra la fecha programada
de la entrega (`"match": "sgi_delivery_picking_id"` + `due_field`). La
migración liga los traslados de 2026.

## COA ligado a la entrega y al pedido (fase 1, 19.0.32.0.0)

- **Cliente** (`res.partner`, pestaña Ventas y compras): «Requiere COA en cada
  embarque» (`sgi_requires_coa`, por compañía) y «Reciben el COA»
  (`sgi_coa_recipient_ids`; vacío = el contacto de la entrega). Lo editan SGI y
  Calidad. Se evalúa sobre la empresa comercial: las plantas heredan.
- **Salida** (`stock.picking`, solo salidas): `sgi_coa_status`
  `no_aplica` / `pendiente` / `adjunto` / `enviado`, los PDF
  (`sgi_coa_attachment_ids`), quién y cuándo (`sgi_coa_date`, `sgi_coa_uid`) y
  cuándo se mandó (`sgi_coa_sent_date`). Botón «Adjuntar COA»: sube los PDF
  (con el nombre que ya usa el laboratorio), precarga destinatarios y, si se
  envía, manda la plantilla «COA <cliente> – <entrega> – <pedido>».
- **Validar sin COA** solo avisa. Con `quimibond_sgi.coa_block_validation =
  True` se bloquea, y solo el puesto `quimibond_sgi.coa_exception_job_id`
  (Jefe de Calidad, 204) valida dejando el motivo en el chatter.
- **Pedido**: `sgi_coa_status` = el peor estado de sus salidas; botón «COA»
  con los archivos; filtro «COA pendiente».
- **Buzón `coa@`** (SGI > Calidad preventiva > COA recibidos): cada PDF
  `<producto> <factura>.pdf` se liga solo (factura → `invoice_origin` → salida
  hecha con ese producto, la más cercana a la factura) y queda `enviado`. Lo
  que no se liga queda en «COA sin ligar» para asignarlo a mano.
- Carga inicial (migración 19.0.32.0.0): los 12 clientes que lo requieren en la
  compañía principal; las salidas ya validadas no se marcan.
- Medición en el SGI (C2.24): entregable sobre `stock.picking` con
  `complete_domain` `[("sgi_coa_status", "=", "enviado")]` y
  `applies_domain` `[("sgi_requires_coa", "=", True)]`, al recargar el JSON de C2.
