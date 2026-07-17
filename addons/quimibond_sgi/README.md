# quimibond_sgi — Sistema de Gestión Integral (Fases 1 y 2)

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
7. **Umbrales**: `quimibond_sgi.risk_ryo_inmediata/media/intermedia` (16/9/4) y
   `quimibond_sgi.supplier_weight_otd/quality` (0.7/0.3) son parametrizables.

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
