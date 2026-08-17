# Manual Técnico — Quimibond SGI para Odoo 19

**Módulos:** `quimibond_sgi` v19.0.4.1.0 + puentes `quimibond_sgi_plm`, `quimibond_sgi_pesaje`, `quimibond_sgi_revisado`
**Plataforma:** Odoo 19 Enterprise en Odoo.sh · **Repo:** `quimibond/qb19` (carpeta `addons/`)
**Audiencia:** desarrolladores, administrador del sistema, consultor Odoo.

---

## 1. Qué es y qué resuelve

Sistema de Gestión Integral (ISO 9001:2015 + 14001:2015 + 45001:2018 en preparación) de
Productora de No Tejidos Quimibond (PNTQ), implementado DENTRO de Odoo extendiendo las
apps nativas (Documentos, Aprobaciones, Calidad, Helpdesk, Proyecto, Mantenimiento,
Encuestas, Skills) y creando solo los modelos que Odoo no tiene. Sustituye el SGI
documental que vivía en Dropbox/Excel, conservando la nomenclatura y los flujos que
auditan SIDE Certificaciones y los clientes automotrices (Seiren, Continental).

### Principios de arquitectura (obligatorios para todo cambio futuro)

1. **Cada dato tiene UN dueño.** Si Odoo tiene el objeto, se extiende (prefijo `sgi_`);
   nunca se crea un modelo espejo.
2. **Cero Studio.** Toda la lógica vive en los módulos del repo. Las automatizaciones de
   UI y Studio quedan prohibidas para lógica del SGI (histórico de builds rotos).
3. **Candados de negocio en servidor** (constrains / overrides de `write`), nunca solo
   en vistas.
4. **Strings en español**, sin i18n.
5. Modelos con flujo → `mail.thread` + `mail.activity.mixin`, estados con `tracking=True`.
6. **Puentes `auto_install`** para integraciones con módulos que pueden no estar
   (PLM, pesaje, revisado): el módulo principal nunca depende duro de ellos.

## 2. Estructura de módulos y dependencias

```
addons/
  quimibond_sgi/            # módulo principal (todo el SGI)
  quimibond_sgi_plm/        # ECO (mrp_plm) -> PPAP; auto_install con mrp_plm
  quimibond_sgi_pesaje/     # rollo fuera de tolerancia de peso -> alerta; auto_install con pesaje_rollos_tejido
  quimibond_sgi_revisado/   # vistas pareto de defectos TEJIDO-*; auto_install con mrp_revisado_telas
```

`quimibond_sgi` depende de: `base, mail, hr, hr_skills, stock, purchase, documents,
approvals, approvals_purchase, quality_control, quality_mrp, helpdesk, project,
sale_management, maintenance, survey` (todas Enterprise estándar de Odoo.sh).

Estructura interna del módulo principal:

```
models/    sgi_area, sgi_norm, sgi_process, sgi_document, sgi_doc_change,
           sgi_nonconformity, sgi_complaint, sgi_improvement, sgi_objective,
           sgi_indicator, sgi_audit, sgi_risk, sgi_supplier_eval,
           sgi_management_review, sgi_control_plan, sgi_calibration, sgi_fmea,
           sgi_ppap, sgi_incident, sgi_competence, sgi_integration, sgi_cron
data/      secuencias (x3 archivos), áreas, normas+cláusulas, procesos+flujos,
           etapas/equipos (calidad, helpdesk, proyecto, aprobaciones),
           indicadores, objetivos, elementos PPAP, encuesta DNC, helpdesk
           interno, crons (x3 archivos), planes de control F4
views/     una por modelo + sgi_menus.xml + dashboards + hooks de mapa
report/    NC (F-P-G05-01), NEWS (F-P-G01-16), CoA, AMEF, incidente, auditoría,
           acta de Revisión por la Dirección
security/  sgi_security.xml (grupos) + ir.model.access.csv
tools/     carga_documental.py, post_carga_documental.py, reporte_telas_rollout.py
tests/     ~64 tests (@tagged post_install)
demo/      sgi_demo_fase3.xml (solo bases con demo)
```

## 3. Modelo de datos

### 3.1 Modelos NUEVOS

| Modelo | Propósito | Campos/lógica clave |
|---|---|---|
| `sgi.area` | Catálogo de áreas documentales (G,A,C,D,E,I,M,P,S,V) | `code` único; liga opcional a `hr.department` |
| `sgi.norm` / `sgi.norm.clause` | ISO 9001/14001/45001 y sus cláusulas 4.1–10.3 | data precargada; usado por NC y hallazgos |
| `sgi.process` | Proceso del mapa (jerarquía macroproceso→proceso) | `parent_store`; dueño, depto, puestos, documentos; contadores de salud (NC abiertas, acciones vencidas); `action_view_records` |
| `sgi.process.flow` | **Entregable output→input entre procesos** | origen≠destino; `document_id` (formato con que se entrega); `odoo_model_id` + botón "Ver registros" (mapa navegable) |
| `sgi.document.ack` | Acuse de lectura por empleado | unique(document, employee); `action_mark_read` solo el propio empleado o manager |
| `sgi.action.line` | Acción/corrección (compartida) | **XOR**: exactamente uno de `alert_id`/`risk_id`/`fmea_line_id`/`incident_id`; estado computado abierta/vencida/terminada (recompute diario por cron) |
| `sgi.objective` | Objetivos integrales 2026-2030 (ANEXO 6) | catálogo data |
| `sgi.indicator` / `.measure` | KPIs F-P-A10-03 | 2 umbrales (Objetivo/Aceptable), sentido, `calc_mode` (14 modos automáticos + manual), `nc_on_red`; medición única por (indicador, periodo); semáforo `False` en pendientes; validación restringida a responsable/manager |
| `sgi.audit.program`/.line | Programa anual P-G03 | unique(year); línea→`action_create_audit` |
| `sgi.audit` / `.finding` | Auditoría + hallazgos | folio AUD-; constraint auditor ≠ dueño de proceso auditado; checklist vía `survey`; candado de cierre: todo hallazgo con disposición (NC creada o justificación) |
| `sgi.risk` (+category) | 5 instrumentos: RyO 5×5, IPER 3×3, ambiental, patrimonial, FODA | folio RSG-; score/nivel por instrumento (umbrales en `ir.config_parameter`); residual; constraint escala IPER (incluye residuales); FODA exige tipo F/O/D/A |
| `sgi.supplier.eval` | Evaluación 8.4 por periodo | OTD de recepciones + NC del proveedor; score ponderado (params `supplier_weight_otd/quality`, `supplier_nc_penalty`); clase Acreditado/Condicionado/Baja |
| `sgi.management.review`/.agreement | Revisión por la Dirección | folio RD-; `action_load_inputs` llena las 10 entradas 9.3.2 (snapshot); acuerdos → tareas del proyecto "Acuerdos RxD"; candado: Realizada exige acuerdo con responsable+fecha |
| `sgi.control.plan` | Plan de control P-C11 | folio PC-; o2m de `quality.point`; vigente exige ≥1 punto; obsoleto agenda revisión de puntos |
| `sgi.calibration` | Evento de calibración P-C03 | conforme/fuera_tolerancia; al crear actualiza equipo; **fuera de tolerancia → equipo `sgi_do_not_use` + NC mayor automática** (IATF 7.1.5); `next_date` del laboratorio prevalece (write conjunto) |
| `sgi.fmea` / `.line` | AMEF P-C10 | folio AMEF-; NPR=S×O×D (umbral param `fmea_npr_action`=100); candado: vigente exige acción en líneas con NPR alto; re-evaluación post |
| `sgi.ppap` / `.element` (+template) | PPAP P-C15 | folio PPAP-; 18 elementos AIAG generados al crear (idempotente); enviado exige cero pendientes; aprobado exige PSW listo; elementos REFERENCIAN registros reales (fmea/control plan/documents) |
| `sgi.incident` | Incidente SST P-S02 (SCAT) | folio INC-; candado de cierre: 3 capas de causas + acciones terminadas; grave/fatal → actividad inmediata a MAST y Dirección; liga a riesgo IPER |
| `sgi.competence.gap` | Vista SQL de brechas | `_auto=False`; usa `hr.version` (Odoo 19: puesto/depto viven ahí) + `hr_job_skill` vs `hr_employee_skill` con vigencia |
| `sgi.nc.force.close` | Wizard de cierre forzado de NC | TransientModel; solo `group_sgi_manager`; motivo obligatorio al chatter |

### 3.2 Modelos EXTENDIDOS (prefijo `sgi_` en todos los campos)

| Modelo nativo | Qué agrega el SGI |
|---|---|
| `documents.document` | Documento controlado: clave (regex de nomenclatura PNTQ), tipo, área, proceso, Rev. NN, fechas, estado (borrador/piloto/vigente/obsoleto), puestos aplicables, próxima revisión (+2 años), acuses (% difusión), **migración de formatos** (clase A/B/C/D, estado, destino). Unicidad de vigente por clave con auto-obsoletado del anterior |
| `approval.request` / `.category` | F-P-G01-06: documento afectado, alta/modificación/baja, rev. vigente→nueva, prueba piloto ≤90 días naturales (inicio ≤15 días hábiles atrás), procesos afectados; al aprobar → versiona el documento, dispara acuses (o actividad de alta) |
| `quality.alert` (+stage/team) | LA No Conformidad: folio anual por equipo (NCI-/NCE- vía `sgi_sequence_id` del team), origen, clasificación M/m/OB, requisito ISO, solicitante, multi-responsable, desviación, 5 porqués + Ishikawa, verificación de eficacia, exhorto/administrativa/N-A, verificó/aprobó, NCR externo, liga a reclamación/medición. **Candados de cierre** (etapas con `sgi_is_closing_stage`): causa raíz + acciones terminadas + eficacia; bypass solo manager vía wizard. **Trigger PROT-05/D7**: NC mayor recién cerrada → actividad "actualizar AMEF/plan de control". **`action_sgi_escalate_to_nc`**: alerta operativa → equipo NC Internas + folio + etapa Abierta |
| `helpdesk.ticket` | Reclamación P-C01: pedido/producto/lote/metros/disposición; botón "Generar NC" (liga bidireccional) |
| `project.project`/`.task`/`.task.type` | Mejora F-P-A10-02: tipo/área/proceso; candado a etapa Terminada: fecha límite + ≥1 adjunto |
| `maintenance.equipment` | Equipo de medición (magnitud/rango/resolución/intervalo, próxima calibración computada `readonly=False`, semáforo vigente/por_vencer/vencido, "No usar") + EPP (flag + vencimiento) |
| `maintenance.request` | Botón "Levantar NC" en correctivas (idempotente vía `sgi_alert_id`) |
| `quality.point` | Plan de control: característica, criticidad F/R/S, "va al CoA", Cpk objetivo, plan de reacción, liga a `sgi.control.plan` |
| `stock.lot` | Botón "Certificado de calidad" (CoA de los checks con `sgi_in_coa`; guardia si no hay); publicación del PDF al picking de entrega (portal) |
| `stock.picking` / `mrp.production` | Contadores/smart buttons de NC |
| `purchase.order` | Smart button "NC del proveedor" |
| `res.partner` | Clase de proveedor (Acreditado/Condicionado/Baja), score, evaluaciones, contadores NC/reclamaciones |
| `hr.job` / `hr.employee` | Documentos aplicables por puesto; "Mis procedimientos", acuses pendientes, brechas de competencia |
| `sale.order` (vía helpdesk) | referenciado por reclamaciones |
| `mrp.eco` (puente PLM) | Requiere PPAP / aviso a cliente / AMEF y planes impactados; al aplicar ECO → crea PPAP (motivo cambio_ingenieria) |
| `mrp.weigh.roll.wizard` (puente pesaje) | Confirmación forzada fuera de tolerancia (±kg, param `pesaje_tolerance_kg`) → alerta en "Revisado de Tela"; idempotente por rollo |

### 3.3 Secuencias (todas `use_date_range=True`, folio anual)

`NCI- / NCE-` (por equipo de calidad), `AUD-`, `RSG-`, `RD-`, `PC-`, `AMEF-`, `PPAP-`, `INC-`.

## 4. Seguridad

Grupos (privilegio "SGI", jerarquía por `implied_ids`):

| Grupo | Implica | Permisos |
|---|---|---|
| `group_sgi_user` (Usuario SGI) | base.user + user de Calidad/Documentos/Helpdesk/Proyecto/Aprobaciones | lectura general; crear NC/acuses/incidentes/mediciones; **sin** unlink de acciones |
| `group_sgi_auditor` | user | + crear auditorías/hallazgos |
| `group_sgi_manager` (Jefe MAST y SGI) | auditor + manager de las apps nativas | CRUD total SGI; cierre forzado; menú Configuración y Migración |
| `group_sgi_director` | manager | Revisión por la Dirección |

ACLs completos en `ir.model.access.csv` (fila user y manager por modelo; auditor donde aplica).

## 5. Automatización

### Crons (todos idempotentes; helper `_sgi_schedule` deduplica actividades)

| Cron | Frecuencia | Hace |
|---|---|---|
| Seguimiento de NC | diario | recompute de acciones vencidas; escalamiento por inacción (param `nc_escalation_days`=5; 3 para externas/reclamación; **solo equipos con `sgi_sequence_id`**); actividad de verificación de eficacia |
| Vencimientos documentales | diario | revisión bienal (60/30 días), pilotos por vencer, acuses >7 días |
| NEWS mensual | mensual | PDF de cambios aprobados del mes + actividad a MAST (ancla: una approval.request) |
| Indicadores mensual | mensual día 1 | crea mediciones del mes anterior (solo `frequency=monthly`); automáticas calculadas, manuales con actividad; NC automática SOLO medición roja+validada+`nc_on_red` (sin duplicar) |
| Indicadores semanal | semanal | ídem para `frequency=weekly` (OTIF) |
| Avisos de programa de auditorías | diario | actividad al líder 15 días antes del mes planificado |
| Revisión de riesgos vencidos | diario | actividad al dueño del proceso |
| Evaluación de proveedores | trimestral | genera evaluaciones + actualiza partner + actividad a Compras si Condicionado/Baja |
| Calibraciones | diario | recompute del semáforo; por vencer 30 días; **vencido → `sgi_do_not_use`** |
| Competencias/EPP | diario | certificaciones y resume lines por vencer 30 días; EPP vencido |

### Triggers en código (no cron)

- NC mayor → cierre → actividad de actualización AMEF/plan de control (write de quality.alert).
- Calibración fuera de tolerancia → bloqueo + NC (create de sgi.calibration).
- ECO aplicado con "Requiere PPAP" → PPAP (puente PLM).
- Pesaje confirmado fuera de tolerancia → alerta (puente pesaje).
- Aprobación de F-P-G01-06 → versionado de documento + acuses.

### Fuentes de NC automáticas (`sgi.alert.source`)

Toda NC que levanta el sistema pasa por un único punto de entrada. **Nunca llamar
a `quality.alert.create()` directo desde un automatismo.**

```python
alert = self.env['quality.alert'].sgi_auto_create('mi_fuente', vals)
if not alert:
    return  # fuente apagada por MAST
```

`sgi_auto_create(code, vals, count_suppression=True)`:

- consulta el registro `sgi.alert.source` por `code`;
- si está **activa** → crea la NC y la estampa con `sgi_source_id`;
- si está **apagada** y es `automatico` → devuelve recordset **vacío** y cuenta la
  omisión (`suppressed_count`, `last_suppressed_on`);
- si está **apagada** y es `manual` → lanza `UserError` (hay una persona esperando
  respuesta del botón: no fallar en silencio);
- si el `code` **no está declarado** → crea igual y avisa al log. *Fail-open*
  deliberado: perder una NC es peor que registrar una de más.

`count_suppression=False` es para llamadores re-entrantes (un cron que reevalúa el
mismo hecho cada corrida) y evita que el contador de omisiones pierda sentido. El
caso vivo es el cron de indicadores, que marca `sgi_nc_suppressed` en la medición.

**Para agregar una fuente nueva** (dos pasos, sin tocar Ajustes ni vistas):

1. Un registro en `data/…` con `noupdate="1"` — obligatorio, para que un
   `odoo-update` no vuelva a encender lo que MAST apagó a propósito:
   ```xml
   <record id="sgi_alert_source_mi_fuente" model="sgi.alert.source">
       <field name="code">mi_fuente</field>
       <field name="name">Nombre visible para MAST</field>
       <field name="trigger_type">automatico</field>   <!-- o manual -->
       <field name="origin_module">mi_modulo</field>
       <field name="trigger_note">Qué condición la dispara, en lenguaje de piso.</field>
   </record>
   ```
2. La llamada a `sgi_auto_create` en el disparador.

El interruptor aparece solo en **SGI → Configuración → Fuentes de NC automáticas**.
Apagar una fuente **no** altera el control operativo de fondo (bloqueo de equipo,
aviso al operador, semáforo del indicador): sólo deja de abrirse el expediente de
NC. El cambio queda firmado en el chatter de la fuente (`tracking=True` en
`enabled`) para sustentarlo en auditoría.

Fuentes declaradas hoy: `indicador_semaforo_rojo`, `calibracion_fuera_tolerancia`,
`incidente_sst_grave` (automáticas) · `auditoria_hallazgo`, `reclamacion_cliente`,
`mantenimiento_falla` (manuales) · `pesaje_rollo_fuera_peso` (automática, vive en
`quimibond_sgi_pesaje`).

### `calc_mode` de indicadores (fuentes reales)

`otif_ventas` (pickings salida vs fecha compromiso) · `otd_compras` (recepciones vs
date_planned de OC) · `produccion_vs_programado` · `desperdicio` (**subproducto SALDO
TEJIDO D**, categoría param `waste_subproduct_category`; NO stock.scrap) ·
`desperdicio_scrap` (histórico) · `calidad_pq` (mrp.revision.log sin causa) ·
`cumplimiento_programa` (MOs con inicio en periodo — aproxima el MPS; validar antes de
`nc_on_red`) · `cierre_nc` · `reclamos_cliente` · `preventivo_cumplido` · `rotacion_rh` ·
`presupuesto_ventas` (VE-02: facturación neta vs **presupuesto de ventas aprobado**
del periodo — `sgi.sales.budget` líneas del mes, todos los equipos; SIEMPRE sobre
importe en moneda compañía, nunca cantidades mezcladas; fallback al parámetro de
Ajustes con nota) · `inventario_ciclico` (requiere conteos) ·
stubs documentados que devuelven None → captura manual.

**Presupuesto maestro de ventas (`sgi.sales.budget`, v19.0.11):** matriz tipo MPS
del F-P-A28-18 por mercado (crm.team) y año, producto × mes en cantidad y pesos.
El real sale solo de lo facturado (`account.move.line.balance`, ya en moneda
compañía — no se reconvierte) y, complementario, de lo pedido (sale.order.line
confirmadas, importe con `currency._convert`). Unidades por línea (`uom_id`,
categoría vía `_has_common_reference`): las cantidades NUNCA se suman entre
unidades; el único total global es el de dinero. Captura en grid de Enterprise
(`web_grid`, `grid_update_cell` propio) e importación `base_import`. VE-02 lee el
presupuesto aprobado; el cierre de mes (cron mensual) avisa al responsable del
equipo por debajo de `sales_budget_alert_pct`.

**Dimensión cliente (opcional por línea).** `sgi.sales.budget.line.partner_id`
(vacío = global del producto para el mercado; con cliente = esa cuenta).
Unicidad producto+mes+cliente con `UNIQUE NULLS NOT DISTINCT` (PG15+: el cliente
nulo es un valor propio). Anti-doble-conteo: un producto no puede tener a la vez
líneas con y sin cliente en el mismo presupuesto. El real por cliente filtra por
`commercial_partner_id` del documento (los pedidos llegan a contactos/direcciones
de entrega; se usa la empresa comercial, patrón del módulo intelligence).
`amount_real_unbudgeted` en la cabecera = real del equipo en el año menos el real
capturado por las líneas (lo vendido sin presupuestar). El grid gestiona solo el
esquema por producto (fila = producto); **el presupuesto por cliente se captura
en la vista lista/ficha** (dos dimensiones de fila no se resolvieron en grid); el
reporte desglosa por cliente con subtotal de producto.

**Precio sugerido desde la lista.** `price_unit_budget` (moneda compañía,
editable); `amount_budget` = qty × precio (compute almacenado invertible: capturar
el importe despeja el precio). Un `@api.onchange('product_id','partner_id',
'uom_id')` sugiere el precio de `partner.property_product_pricelist` (o
`list_price` sin cliente) y **nunca pisa** uno ya capturado. Listas en otra moneda
se convierten a compañía con el tipo presupuestal `budget_planning_rate` (USD→MXN;
0 = tipo del día) y dejan rastro en `price_source` ("Lista 'Export USD': 2.15 USD
× 17.50 = …"). La cotización borrador NO usa este precio: al cotizar se deja que
Odoo aplique la lista vigente (la diferencia ppto vs real es información). El
reporte añade columna precio unitario presupuestado vs precio promedio real.

**Importación desde el Excel real.** Asistente `sgi.sales.budget.import`
(TransientModel, botón "Importar desde Excel" solo en borrador) que parsea el
F-P-A28-18 con openpyxl: detecta la fila de encabezados por 'PRODUCTO', pares
"<mes> m"/"<mes> $" (tolerante a mayúsculas/acentos/espacios), columnas opcionales
UNIDAD y CLIENTE. Matching de producto por default_code → nombre exacto → nombre
ilike único; los no-match se reportan (chatter + resultado) y NO abortan. Todo-o-
nada por hoja para errores ESTRUCTURALES (savepoint); los errores de datos por
línea usan savepoint anidado y se reportan. Modo de choque replace (default) /
add. Convive con base_import (tabla plana).

**Pronóstico semanal por cliente (F-P-A28-13, v19.0.12).** `sgi.sales.budget.kind`
= presupuesto (mensual por mercado, default, todo igual) / pronostico (semanal por
cliente). En pronóstico: `partner_id` en cabecera (obligatorio; unicidad
año+equipo+cliente+kind), líneas con `date` = lunes de la semana y `customer_code`
(código del cliente para el material). Real del pronóstico = COMPROMETIDO (pedidos
confirmados del cliente comercial por `commitment_date`/expected/date_order de la
semana), no facturado. Drill-down `action_view_week_orders`. Importador v2: el
mismo asistente lee el forecast.xlsx cuando el presupuesto es pronóstico (fila
`SEMANA` con números 1–52 anclados al primer lunes del año; producto col A, código
cliente col B; bloques repetidos se suman; filas PO/TOTAL/FECHA se ignoran; comas
de miles). Reporte QWeb con banner F-P-A28-13 (`sgi_code_alt`, override
`_sgi_format_code` por kind). **Captura del pronóstico = plan B (lista/ficha por
semana), NO grid**: el grid semanal (52 columnas) chocaba con el esquema por
cliente de `grid_update_cell` (fuerza partner vacío) y su escala no era verificable
sin la UI; el grid mensual por producto se mantiene para el presupuesto.

**Consumo de pronóstico y demanda al MPS (v19.0.12.2).** `qty_net_demand` (compute
almacenado junto a la foto del real, mismo refresco) = `max(qty_budget, qty_real)`:
los pedidos confirmados CONSUMEN el pronóstico de su semana (forecast consumption);
si superan lo pronosticado, manda el pedido. Se muestra en lista/pivot (tercera
medida) y en el reporte F-P-A28-13 (tercera fila). `action_preload_from_orders`
(pronóstico borrador): crea celdas para las semanas del horizonte que ya tienen
pedidos y NO tienen pronóstico, con `qty_budget` = lo comprometido (idempotente, no
pisa capturas). `action_send_to_mps` (pronóstico aprobado): vuelca la demanda NETA
por producto/semana al forecast del Programa Maestro (`mrp.production.schedule` +
`mrp.product.forecast` de `mrp_mps`), convertida a la unidad del producto; crea el
schedule si falta y el re-envío actualiza sin duplicar; registra en el chatter.
`mrp_mps` es OPCIONAL (no está en depends): el botón se oculta vía
`sgi_mps_available` si el módulo no está. PROHIBIDO crear pedidos de venta desde el
pronóstico (demanda ficticia): la precarga sólo LEE pedidos.

**Facturado/pedido almacenados (foto).** `qty_real`/`amount_real`/`qty_ordered`/
`amount_ordered` son computes ALMACENADOS (`store=True`, `aggregator='sum'`) para
poder agregarse en pivot/graph — un measure no almacenado rompe el pivot ("No
aggregate function…"). Son una foto: se recalculan al tocar la línea, con el botón
"Actualizar facturado/pedido" del presupuesto y en el cron mensual; NO se
refrescan solos al timbrar una factura nueva.

**KPIs 2.0 (v19.0.10):** `crecimiento_ventas` (VE-01, facturación neta timbrada del
periodo vs mismo periodo año anterior, variación %) · `ots_atendidas` (MT-03,
maintenance.request cerradas etapa done vs creadas) · `requisiciones` (CO-02,
approval.request de categoría de compras aprobadas vs solicitadas; categoría
autodetectada por `approval_type='purchase'` o param) · `embarques_sin_error` (AL-02,
pickings salida done sin devolución de cliente `returned_move_ids` vs total) ·
`produccion_vs_capacidad` (MA-02, producción real vs param `production_monthly_capacity`,
prorrateada por días si el periodo no es mensual) · `consumo_energia` (TR-03, facturado
del periodo por el proveedor param `energy_partner_id`; sin proveedor → 0 con nota) ·
`compras_sin_devolucion` (**PROXY** de errores en OC; a validar por MAST; NO se activa en
la siembra) · `capacitacion` (RH-02, competencias vigentes vs requeridas vía
`sgi.competence.gap` con vigencia `valid_to`; foto a hoy, sin cota de periodo).
Cada modo declara su fuente en `_SOURCE_INFO`, navega a su evidencia (dict `_EVIDENCE`
o rama propia de `action_view_evidence`), respeta `_sgi_period_bounds` (fin inclusivo) y
`_sgi_dt_bounds`. `sgi.config.activate_auto_indicators()` fija el `calc_mode` SOLO si el
indicador sigue en `manual` (no pisa decisiones de MAST); CO-03 queda fuera a propósito.
Manuales a propósito (sin fuente confiable): TR-02 (papel), TR-04 (residuos), LO-02
(documentación de exportaciones), TI-01 (uptime, lo mide Odoo.sh externo).

## 6. Reportes QWeb

F-P-G05-01 (NC individual) · F-P-G01-16 (NEWS) · CoA por lote (bilingüe, checks con
`sgi_in_coa`) · AMEF · Investigación de incidente · Plan/Informe de auditoría · Acta de
Revisión por la Dirección. Regla QWeb 19: nada de `t-field` directo en `li`/`td`
(envolver en `span`).

## 7. Herramientas de migración (`tools/`, se corren con `odoo-bin shell`)

| Script | Qué hace | Claves de uso |
|---|---|---|
| `carga_documental.py` | ZIP del SGI (secciones 00-23) → app Documentos con metadatos (clave/tipo/área/rev/vigente) | `DRY_RUN=True` por defecto; CSV en /tmp; salta carpetas obsolet/baja/anterior y (config actual) secciones de registros 07/09/13/14/17; idempotente; **commits por lote (`COMMIT_EVERY=50`)** — obligatorio: una corrida monolítica muere por OOM (`Killed`) y hace rollback; éxito = línea "Cambios CONFIRMADOS" |
| `post_carga_documental.py` | liga flujos↔formatos por clave; documentos↔procesos por familia; rescata claves fuera de norma (prefijos "01.", guiones A-16→A16); **pre-clasifica la migración de formatos por familia**; genera worklists (/tmp/worklist_worksheets.csv, /tmp/worklist_puestos.csv); aplica puestos masivos desde /tmp/puestos_documentos.csv | dry-run por defecto; idempotente; correr DOS veces la real la primera vez (los rescates habilitan ligas de flujos) |
| `reporte_telas_rollout.py` | lista de telas sin operación TEJIDO/BoM completa (rollout de las 186) | solo lectura; CSV en /tmp |

Nota `odoo-bin shell`: revierte la transacción al salir → los scripts hacen
`env.cr.commit()` explícito en modo real.

## 8. Tests

~64 tests `@tagged('post_install', '-at_install')` cubriendo: folios y secuencias,
candados de NC/mejora/AMEF/PPAP/incidente/auditoría/RxD, unicidad de vigente,
generación de acuses idempotente, cambios documentales (piloto 90 días, versionado),
semáforos de indicadores (ambos sentidos), cron de indicadores idempotente + NC única,
XOR de acciones, escalas de riesgo por instrumento, evaluación de proveedores,
calibración (bloqueo/desbloqueo/fecha del laboratorio), escalar a NC (folio+etapa),
KPIs de piso (desperdicio por subproducto, calidad PQ), pesaje→alerta, retro-vinculación
segura, gap de competencias, ECO→PPAP (puente). Los builds de ramas de desarrollo de
Odoo.sh los corren automáticamente en cada push.

## 9. Despliegue y operación (Odoo.sh)

- **Ramas:** `main` = staging; `quimibond` = producción; `SGI` = integración de
  desarrollo; el flujo es: sesión de desarrollo → push a `SGI` → revisión → merge a
  `main` (staging build) → merge a `quimibond` (producción).
- **Actualizar el módulo:** `odoo-update quimibond_sgi && odoosh-restart http`.
  Los puentes se auto-instalan. Cambios de datos XML (menús, etapas) requieren update;
  cambios puros de Python solo restart.
- **REGLA CRÍTICA:** jamás subir la versión del manifest de `quimibond_intelligence`
  ni de los módulos de piso — un bump dispara `-u` en el build y los errores
  preexistentes de Studio (campos `x_studio_*`, visibles como warnings en cada shell)
  lo marcan rojo. `quimibond_sgi` SÍ versiona con normalidad (19.0.X.Y.Z por fase).
- **Crons en staging** están desactivados: probar con "Ejecutar manualmente".
- **Límites Odoo.sh:** crons con lotes cortos; procesos de shell con commits por lote
  (lección OOM de la carga).
- **Deuda conocida:** campos Studio legados en la BD (warnings ruidosos, limpiar antes
  del upgrade a Odoo 20); candado de AMEF acepta acciones sin terminar (endurecer);
  `_sgi_records_domain` de flujos devuelve dominio vacío a propósito.

## 10. Configuración de instancia (no código) — referencia

Usuarios en grupos SGI; aprobadores de "Modificación de documento SGI" y "Solicitud de
compra"; SLA/alias de los dos equipos de Helpdesk; responsables de indicadores;
`nc_on_red` por indicador SOLO tras validar un mes contra el Excel; matrices de riesgo;
skills esperadas en los 50 puestos; certificaciones con vigencia; equipos de medición e
intervalos; Master Specs como quality points; formulario web del buzón QR; dashboards de
hojas de cálculo; portal del auditor (permiso con caducidad); Frontdesk; onboarding
plans. Detalle completo en el README del módulo.

## 11. Parámetros del sistema (`ir.config_parameter`)

> **Regla: un parámetro se declara en UN solo lugar.** El default va en
> `sgi.config._SGI_DEFAULT_PARAMS` y lo crea `seed_parameters()` — nunca además
> como `<record model="ir.config_parameter">` en un **módulo dependiente**.
> `seed_parameters()` corre al final de la carga de `quimibond_sgi`, o sea antes
> de los datos de los puentes, y crea la fila **sin xmlid**: en una instalación
> limpia el `<record>` del puente intenta insertar una clave que ya existe y
> `ir_config_parameter_key_uniq` tumba el registry entero (`Failed to load
> registry`). Le pasó a `pesaje_tolerance_kg`.
>
> Dentro de `quimibond_sgi` conviven algunos `<record>` con su entrada en el
> dict (`nc_escalation_days`, `risk_ryo_*`, `fmea_npr_action`,
> `waste_subproduct_category`): hoy no truenan sólo porque sus archivos se
> cargan antes que `data/sgi_parameters.xml`. Es frágil — al mover un archivo
> de posición en el manifest, revísalo.

| Clave | Default | Uso |
|---|---|---|
| `quimibond_sgi.nc_escalation_days` | 5 | días sin acción antes de escalar NC interna |
| `quimibond_sgi.fmea_npr_action` | 100 | umbral de NPR que exige acción |
| `quimibond_sgi.risk_ryo_inmediata/media/intermedia` | 16/9/4 | umbrales matriz RyO |
| `quimibond_sgi.supplier_weight_otd/quality` | 0.7/0.3 | ponderación del score |
| `quimibond_sgi.supplier_nc_penalty` | 10 | puntos por NC de proveedor |
| `quimibond_sgi.waste_subproduct_category` | SubProducto | categoría del desperdicio |
| `quimibond_sgi.pesaje_tolerance_kg` | 3.0 | tolerancia del puente de pesaje |
| `quimibond_sgi.monthly_sales_budget` | — | presupuesto si el indicador no lo define |
| `quimibond_sgi.rh_user_id` | — | copia de avisos de vigencias de RH |

### Parámetros añadidos en KPIs 2.0 (v19.0.10, indicadores automáticos)

| Clave | Default | Uso |
|---|---|---|
| `quimibond_sgi.purchase_approval_category_id` | 0 | KPI CO-02: categoría de aprobación que cuenta como requisición de compra. 0 = autodetectar las de `approval_type='purchase'`; fíjalo solo si hay varias |
| `quimibond_sgi.production_monthly_capacity` | 0 | KPI MA-02: capacidad instalada mensual de producción (misma unidad que la producción, p.ej. kg). Se prorratea por días en periodos no mensuales. 0 = captura manual |
| `quimibond_sgi.energy_partner_id` | 0 | KPI TR-03: proveedor de energía (res.partner) cuyas facturas del periodo suman el consumo. 0 = sin configurar → medición en 0 con nota |
| `quimibond_sgi.sales_budget_alert_pct` | 80 | Cierre de mes: umbral (%) de cumplimiento acumulado del presupuesto de ventas bajo el cual se avisa al responsable del equipo |
| `quimibond_sgi.budget_planning_rate` | 0 | Tipo de cambio presupuestal USD→MXN para sugerir precios de listas en otra moneda. 0 = tipo de cambio del día de captura |

### Parámetros añadidos en Ola 1 (Motor de Mejora, ISO 10)

| Clave | Default | Uso |
|---|---|---|
| `quimibond_sgi.nc_recurrence_months` | 12 | ventana para contar reincidencia de NC del mismo proceso (misma cláusula pesa doble) |
| `quimibond_sgi.action_escalation_manager_days` | 7 | días de acción vencida para escalar al jefe directo |
| `quimibond_sgi.action_escalation_director_days` | 15 | días de acción vencida para escalar a Dirección |

### Parámetros añadidos en Ola 2 (Línea Dorada, cascada ISO)

| Clave | Default | Uso |
|---|---|---|
| `quimibond_sgi.nc_escalation_days_external` | 3 | días para escalar NC de auditoría externa / reclamación de cliente (antes fijo en código) |
| `quimibond_sgi.doc_review_notice_days` | 60 | primer aviso de revisión bienal documental |
| `quimibond_sgi.doc_review_notice_days_final` | 30 | segundo (último) aviso de revisión bienal |
| `quimibond_sgi.doc_pilot_notice_days` | 7 | aviso de vencimiento de una prueba piloto |
| `quimibond_sgi.doc_ack_pending_days` | 7 | días para reclamar un acuse de lectura pendiente |

**Umbrales que se dejaron fijos a propósito** (no son política que Calidad afine, sino cadencia operativa o estructura de calendario):
- Deadlines de captura de indicadores (mensual +4 días, semanal +2 días): atados al ciclo de reporte.
- Aviso de preparación de auditoría (15 días antes del mes planificado): lead time estándar del programa.
- Horizontes de 30 días para calibración/EPP/certificaciones por vencer: norma de planeación (30 días) ligada a la realidad operativa; cambiarla no es decisión de Calidad.
- Fronteras trimestrales de la evaluación de proveedores: son aritmética de calendario (trimestre natural), no un umbral.

## 12. Historial de fases

| Versión | Contenido |
|---|---|
| 19.0.1.0.0 | Fase 1: mapa de procesos, documental+acuses, cambios F-P-G01-06, NC con candados, reclamaciones, mejoras |
| 19.0.2.0.0 | Fase 2: indicadores+NC automática, auditorías, riesgos (5 instrumentos), proveedores, Revisión por la Dirección, voz del cliente |
| 19.0.3.0.0 | Fase 3: plan de control+CoA, calibración IATF, AMEF, PPAP, incidentes SCAT, competencias, puente PLM |
| 19.0.4.0.0 | Fase 4: conexión al piso real (pesaje/revisado/cuarentena), KPIs recalibrados, mapa navegable, dashboards, puentes pesaje/revisado |
| 19.0.4.1.0 | Menú de migración de formatos + scripts de carga/post-carga documental |

---

## Apéndice: Configuración y operación sin código (v19.0.4.4.0)

### Regla general: qué sobrevive a un `odoo-update`

| Tipo de dato | Regla | Ejemplos |
|---|---|---|
| **Operativo** (`noupdate=1`) — lo que edita el usuario MANDA y sobrevive updates | Editar libre en la interfaz | Indicadores (metas/responsables), etapas de NC, objetivos, catálogo de riesgos, equipos Helpdesk, secuencias de folios, **crons** (pausar/cambiar frecuencia), **mapeo de claves de formato**, parámetros del sistema |
| **Estructural** (`noupdate=0`) — el código MANDA y se re-aplica en cada update | Cambios se piden por desarrollo | Mapa de procesos y flujos (nombres/estructura), áreas, normas y cláusulas, vistas, reportes, seguridad |

Nota: en procesos y flujos, los campos que se llenan en la interfaz y NO vienen
en el código (dueño del proceso, documento ligado al flujo) **sí sobreviven**;
solo nombre/estructura se re-aplican.

### Parámetros del sistema (Ajustes → Técnico → Parámetros del sistema, buscar `quimibond_sgi.`)

| Parámetro | Default | Qué controla |
|---|---|---|
| `nc_escalation_days` | 5 | Días sin acciones antes de escalar una NC (externas: 3, fijo en código) |
| `fmea_npr_action` | 100 | NPR a partir del cual el AMEF exige acción |
| `risk_ryo_inmediata` / `_media` / `_intermedia` | 16 / 9 / 4 | Cortes de la matriz RyO 5×5 |
| `supplier_weight_otd` / `_quality` | 0.7 / 0.3 | Pesos de la evaluación de proveedores |
| `supplier_nc_penalty` | 10.0 | Puntos que descuenta cada NC al proveedor |
| `pesaje_tolerance_kg` | 3.0 | Tolerancia de peso de rollo (báscula de piso) |
| `waste_subproduct_category` | SubProducto | Categoría del byproduct de desperdicio (SALDO) |
| `monthly_sales_budget` | 0 | Presupuesto mensual de ventas para el KPI (capturar) |
| `rh_user_id` | 0 | Usuario de RH que recibe actividades automáticas (capturar) |

### Dónde se administra cada cosa (menús, sin código)

| Qué | Dónde |
|---|---|
| Claves de formato en documentos de Odoo | SGI → Configuración → Formatos en documentos de Odoo |
| Migración de formatos (clase A/B/C/D, destino, estado) | SGI → Migración de formatos |
| Áreas, normas, cláusulas, categorías de riesgo, elementos PPAP | SGI → Configuración |
| Folios (NCI-, AUD-, RSG-…) | Ajustes → Técnico → Secuencias |
| Crons (frecuencia, pausar) | Ajustes → Técnico → Acciones planificadas |
| Etapas de NC, equipos de calidad | Desde el tablero (kanban) / app Calidad |
| Hojas de trabajo de calidad (clase B) | App Calidad → Puntos de control |

### Qué SÍ requiere desarrollo (pedirlo, no improvisarlo con Studio)

- Agregar un **modelo nuevo** al mapeo de claves (banner + pie de PDF)
- Nuevos **KPIs automáticos** (modos de cálculo), nuevos candados o crons
- Cambios a la **estructura** del mapa de procesos
- **Regla de la casa: cero Odoo Studio en el SGI** — todo cambio va por el módulo
