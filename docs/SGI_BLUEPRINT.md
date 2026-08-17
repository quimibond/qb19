# SGI Quimibond en Odoo 19 — Blueprint de Implementación

**Objetivo:** migrar el Sistema de Gestión Integral de PNTQ (ISO 9001:2015 + ISO 14001:2015 + ISO 45001:2018) de Dropbox/Excel a Odoo 19 Enterprise (Odoo.sh), reutilizando al máximo las apps nativas y construyendo custom solo lo que Odoo no tiene. El SGI deja de ser un archivero: se alimenta de la operación real (ventas, compras, producción, RRHH, mantenimiento) y actúa sobre ella (alertas, actividades, aprobaciones, KPIs automáticos, NCs que nacen solas).

**Base de este documento:**
- Lectura completa del SGI vigente (Drive, jul-2026): MIID Rev.02, 43 procedimientos, ~45 instructivos, ~180 formatos, DATs, 50 descripciones de puesto, ~60 KPIs (F-P-A10-03), 6 instrumentos de riesgo, concentrados de NC 2025-2026, mapeos SIPOC, protocolos, reglamentos y documentos externos de clientes (Seiren, Continental).
- Verificación contra la documentación oficial de Odoo 19 (https://www.odoo.com/documentation/19.0/) de las capacidades de todas las apps relevantes.

---

## 1. Principios de arquitectura

1. **Cada dato tiene UN dueño.** Si Odoo ya tiene el objeto (alerta de calidad, equipo, empleado, documento, ticket), el SGI le agrega campos vía módulo — nunca crea un modelo espejo. Modelo nuevo solo cuando Odoo no tiene nada (mapa de procesos, riesgos, AMEF, PPAP).
2. **Cero Studio.** Los builds de Odoo.sh ya sufren errores preexistentes de Studio; además Studio guarda las customizaciones como datos XML no versionables en Git, y las automatizaciones/reglas de aprobación de Studio pueden cambiar el plan de precios. Todo va en **un solo addon delgado**: `quimibond_sgi` (campos + pegamento + modelos genuinamente nuevos).
3. **Workflows con lo nativo:** etapas kanban, actividades y planes de actividades, app Aprobaciones, reglas de automatización.
4. **Un solo repositorio documental:** la app Documentos reemplaza Dropbox. NC, auditorías, PPAP y calibraciones referencian archivos ahí.
5. **La nomenclatura y los flujos actuales se respetan** (claves P-Xnn / IT / F / DAT / PROT, folios anuales, estados A/S/C, flujo F-P-G01-06, NEWS mensual): el personal y los auditores de SIDE encuentran lo mismo que hoy, pero vivo.
6. **Go-live por etapas con datos reales en cada etapa.** Un SGI adoptado al 60% vale más que uno completo al 0%.

## 2. Correcciones clave que arrojó la investigación (vs. suposiciones)

| Tema | Realidad verificada en Odoo 19 |
|---|---|
| Workflow actions de Documentos | **Ya no existen en 19.** Reemplazadas por "Actions on Select" (server actions por carpeta) y Automations (requieren Studio → evitamos; el pegamento va en el módulo). |
| Versionado de Documentos | Nativo pero simple (historial de subidas + lock). Sin número de revisión, estado ni aprobación → esos campos van en la extensión. |
| App Aprobaciones | Existe en Enterprise (sin página en docs): categorías custom, aprobadores en secuencia, mínimo de aprobaciones, documento requerido. Extensible con campos vía módulo. |
| quality.alert | Trae nativo: causa raíz, pestañas de acciones correctivas/preventivas, equipo, responsable, producto/lote, work center, picking, etapas kanban por equipo. |
| quality.point | 9 tipos (instrucciones, medida con norma+tolerancia, pasa/falla, worksheet con condiciones de éxito, spreadsheet, foto, registrar producción/consumo, imprimir etiqueta), frecuencias todas/aleatoria %/periódica, **failure locations** (enrutan lo rechazado a cuarentena). |
| SPC / cpk | **No existe nativo.** Se calcula fuera (Quimibond Intelligence vía Supabase) sobre las medidas capturadas por quality checks. |
| Calibración | No hay metrología nativa. Se modela: equipo de medición = `maintenance.equipment` + solicitud preventiva recurrente + worksheet de calibración + campos custom (resultado, certificado, próxima fecha, "no usar"). |
| OTD / OTIF | **No hay reporte nativo.** Se calcula de `date_done` vs `commitment_date`/`date_deadline` (el sync a Supabase ya trae odoo_deliveries). Compras sí trae dashboard con % de entregas a tiempo por proveedor. |
| Límite de crédito | Solo **advierte, no bloquea** la venta. Bloqueo duro = automatización en el módulo si se requiere. |
| Capacitación con vigencia | **Nativo en 19**: Empleados → Certificaciones con vigencia y alerta a 90 días; menú Learning (eLearning + presencial vía Eventos) alimenta el Resumé. |
| Nómina México | **Nueva localización oficial en 19** (`l10n_mx_hr_payroll*`) con timbrado CFDI de nómina, IMSS, prima de riesgo. |
| ISO 45001 (incidentes, IPER, exámenes médicos) | **No hay nada nativo.** Va al modelo custom de riesgos + incidentes (o Helpdesk interno), EPP vía Equipment de Mantenimiento asignado a empleados. |
| ISO 14001 | App **ESG** nativa (bajo Finanzas): huella de carbono Scopes 1-3, energía, iniciativas ligadas a Proyecto. Residuos: scrap con motivos + cuentas de merma. |
| Cuarentena | No es objeto propio: recepción en 3 pasos (Input→Quality Control→Stock) + failure locations de Quality + ubicaciones no reservables. |
| Requisición interna de compra | No existe nativa: categoría "Solicitud de compra" en la app Aprobaciones → genera RFQ. |
| CFDI | Nativo completo: 4.0, PACs, **complemento de comercio exterior** (`l10n_mx_edi_extended`, exportaciones a Continental/Seiren/Shawmut) y **Carta Porte** (`l10n_mx_edi_stock`). |
| WhatsApp | Integración nativa Enterprise (Meta Business API): plantillas aprobadas, envío desde registros y automatizaciones — sustituye los avisos informales de defectos por radio/WhatsApp personal. |
| Crons en Odoo.sh | Máximo cada 5 min, con time-limit forzado: lotes pequeños, commit por lote, idempotencia. |

## 3. Mapa maestro: bloque del SGI → solución en Odoo 19

| Bloque SGI (documento rector) | App nativa base | Extensión en `quimibond_sgi` | Custom nuevo |
|---|---|---|---|
| Control documental (P-G01) | **Documentos** (carpetas = secciones 00-23, permisos por área, compartir con caducidad) | `documents.document`: clave validada, tipo, área, Rev. NN, fechas, estado Piloto/Vigente/Obsoleto, puestos aplicables, próxima revisión bienal | Acuses de lectura (modelo mini) |
| Cambios documentales (F-P-G01-06) | **Aprobaciones**: categoría "Modificación de documento SGI", secuencia Jefe MAST y SGI → Dirección de Operaciones | `approval.request`: campos del formato (rev. vigente→aprueba, alta/modificación/baja, prueba piloto 90 días, procesos afectados); al aprobar → versiona el documento | — |
| Difusión NEWS (F-P-G01-16) | Reporte QWeb sobre cambios aprobados del mes + Email Marketing para el boletín | — | Solo el reporte |
| No conformidades (F-P-G05-01/02) | **Calidad → quality.alert** (etapas = Abierta/Seguimiento/Cerrada/Cancelada) | Folio anual int/ext, Mayor/Menor/Observación, requisito (norma+cláusula), origen, solicitante+cargo, multi-responsable con puesto, 5 porqués estructurado, verificación de eficacia, exhorto/administrativa/N-A, candados de cierre | Líneas de acción correctiva (one2many: responsable, compromiso, avance 0/50/100, eficacia) |
| Salidas no conformes (P-G04) + cuarentena (P-C13) | Recepción 3 pasos + failure locations + scrap con motivos | Campos del reporte F-P-G04-05 en el quality check / alerta | — |
| Reclamaciones de cliente (P-C01) | **Helpdesk**: equipo "Reclamaciones", SLA ≤3 días, portal, alias de correo; after-sales nativo (devolución, nota de crédito, reparación) | Lote, metros, disposición; botón "→ NC" | — |
| Quejas/sugerencias internas (F-P-A10-04, QR) | Formulario web → ticket Helpdesk (equipo interno); QR apunta a la URL | — | — |
| Mejora continua (F-P-A10-02) | **Proyecto**: un proyecto por área, tarea = mejora, plantillas de tarea | Tipo de mejora; candados: fechas inicio/fin y evidencia adjunta obligatorias | — |
| Mapa de procesos (ANEXO 2/3, SIPOC) | — | — | **`sgi.process` + `sgi.process.flow`** (output→input) |
| Indicadores (F-P-A10-03) | **Hojas de cálculo/Dashboards** con datos vivos para visualización | — | **`sgi.indicator` + `sgi.indicator.measure`** (meta, sentido, fuente, medición mensual) + cron NC automática |
| Auditorías internas (P-G03) | **Encuestas** como checklist puntuado en tableta | — | **`sgi.audit.program` / `sgi.audit` / hallazgos** → hallazgo crea quality.alert |
| Riesgos (6 instrumentos) | — | — | **`sgi.risk`** con tipo (RyO/IPER/ambiental/patrimonial/FODA/AMEF-liga) y matriz configurable |
| Revisión por la Dirección (IT-P-A10-01) | Acuerdos = tareas de Proyecto con responsable/fecha | — | **`sgi.management.review`** con snapshot automático de entradas 9.3.2 |
| Plan de control / specs Continental (P-C11) | **quality.point** por producto/operación + worksheets | Criticidad F-R-S, flag CoA, cpk objetivo | — |
| Calibración (P-C03) | **Mantenimiento**: equipo + preventivo recurrente + worksheet + Block Workcenter | Flags equipo de medición, resultado, certificado, vencimiento, "no usar"; fuera de tolerancia → exige alerta de calidad | — |
| AMEF (P-C10) | — | — | **`sgi.fmea`** (líneas S×O×D=NPR, semáforo, liga a acciones) |
| PPAP (P-C15) | — | — | **`sgi.ppap`** checklist 18 elementos que referencia registros reales |
| Cambios de ingeniería / BoM | **PLM (mrp.eco)**: etapas custom, aprobadores por rol (requerido/opcional/comenta), versionado de BoM, docs contenidos hasta aplicar | Flags "¿re-PPAP?" "¿avisar cliente?" | — |
| Competencias y capacitación (P-A01) | **hr_skills** (Expected Skills en el puesto) + **Learning/Certificaciones con vigencia** + eLearning + Eventos (presencial con asistencia y quiz) + Encuestas (DNC F-P-A01-17, eficacia) + Onboarding plans (inducción 3 meses) | Poco o nada | Gap analysis puesto↔empleado si el nativo no alcanza |
| Descripciones de puesto (F-P-A01-01, 50 puestos) | `hr.job` (Job Summary + Expected Skills) + vista Hierarchy (organigrama 9 niveles) | Campos de perfil que falten (experiencia en años, etc.) | — |
| Proveedores (evaluación) | Dashboard nativo de Compras (% on-time por proveedor) + conteo de alertas origen recepción | `res.partner`: clasificación Acreditado/Condicionado/Baja, score | Histórico formal de evaluación por periodo (modelo mini, opcional) |
| Incidentes/accidentes SST (P-S02, SCAT) | — | — | **`sgi.incident`** (o extensión del modelo de riesgos) con metodología SCAT |
| EPP (P-S03) | Equipment (Mantenimiento) asignado a empleado/departamento | Vigencia/talla si hace falta | — |
| Requisitos legales (19 NOMs, F-P-E02-01) | Documentos con tags + fechas | Fecha de próxima revisión + responsable | — |
| Ambiental: residuos/energía (P-E01..E06) | Scrap con motivos + cuentas de merma; app **ESG** (Scopes 1-3, iniciativas) | — | — |
| Control de visitantes/contratistas (PROT-01) | **Frontdesk**: estaciones por acceso, kiosko, QR, notificación al anfitrión | — | — |
| Lecciones aprendidas (ANEXO 14), partes interesadas, matriz comunicación, FODA, glosario | **Conocimiento** (artículos jerárquicos con historial y plantillas) | — | — |
| Firmas (COAs, liberaciones) | **Firma** (Sign; página oficial de validez para México, NOM-151 citada) | — | — |
| Bitácoras por máquina (F-IT-P-P01-xx) | **Shop Floor**: operador con PIN, registro con lote, checks integrados, paros, worksheets en tablet | — | — |
| Trazabilidad tarjeta viajera (F-IT-P-P01-08-01) | Lotes con propiedades custom (ancho/gramaje/tono), traceability report, GS1-128 por rollo con Barcode | — | — |
| Satisfacción del cliente | Encuestas + Marketing Automation (envío periódico y seguimiento a no-respondientes) | — | — |

## 4. Detalle por fase

### FASE 0 — Preparación (1 vez, antes de todo)

1. **Seguridad inmediata:** rotar la contraseña de Odoo expuesta en texto plano en P-I01; política de nunca poner credenciales en documentos.
2. **Staging primero:** crear rama de staging en Odoo.sh; toda app nueva se instala y prueba ahí (los builds de producción cargan los errores viejos de Studio; recordar la regla de no cambiar la versión del manifest de `quimibond_intelligence`).
3. **Instalar apps** (verificar en Apps cuáles ya están): Documentos, Aprobaciones, Calidad, Helpdesk, Conocimiento, Firma, Encuestas, Proyecto, Mantenimiento (ya), PLM, eLearning, Eventos, Frontdesk, Dashboards. WhatsApp e IoT se dejan para Fase 4.
4. **Grupos de seguridad SGI** (en el módulo): Usuario SGI (todos) / Auditor / Jefe MAST y SGI (admin del SGI) / Dirección.
5. **Catálogos base** (en el módulo): áreas documentales (G, A, C, D, E, I, M, P, S, V), normas ISO 9001/14001/45001 con cláusulas 4.1–10.3, secuencias con folio anual (NC int/ext, quejas, mejoras, cambios).
6. **Decidir la migración documental:** árbol de carpetas de Documentos espejo de las secciones 00-23 del Drive, permisos por área. Migrar SOLO revisiones vigentes (los obsoletos se quedan archivados en Drive/backup); cada archivo entra con su clave y Rev. actual como metadatos.

**Criterio de salida:** apps instaladas en staging, árbol documental definido, credenciales rotadas.

### FASE 1 — Núcleo documental y de mejora (primer go-live)

**1.1 Control documental**
- Extensión `documents.document`: clave (regex de su nomenclatura), tipo (MIID/P/IT/F/F-IT/DAT/PROT/DF/R/ANEXO/externo), área, proceso, Rev. NN + fecha de emisión, estado (Borrador/Prueba piloto/Vigente/Obsoleto), responsable, puestos aplicables (m2m `hr.job`), próxima revisión (auto +2 años).
- Listas maestras F-P-G01-03/-09 = vistas filtradas (interna/externa) exportables.
- **Acuses de lectura** (`sgi.document.ack`): al publicar revisión → acuse pendiente a cada empleado cuyo puesto aplica; menú "Mis procedimientos" por usuario; smart buttons en empleado.
- Cron: revisión bienal (aviso 60/30 días), pruebas piloto por vencer (90 días), acuses pendientes >7 días.

**1.2 Cambios documentales (F-P-G01-06 → app Aprobaciones)**
- Categoría "Modificación de documento SGI" con aprobadores en secuencia: Jefe MAST y SGI → Dirección de Operaciones; documento requerido.
- Campos extendidos: documento afectado, qué modifica (formato/contenido), rev. vigente → rev. que se aprueba, tipo (alta/modificación/baja), prueba piloto (inicio/fin, candado 90 días naturales y máx. 15 días hábiles previos), procesos afectados.
- Pegamento: al aprobar → crea la nueva versión en Documentos, cambia estado, dispara acuses.
- **NEWS mensual:** reporte QWeb de solicitudes aprobadas del mes (tabla ÁREA/PUESTO | CLAVE | DOCUMENTO | MODIFICACIONES, marca "P. PILOTO") como borrador para que MAST publique; envío opcional por Email Marketing.

**1.3 No conformidades (sobre `quality.alert`)**
- Equipos de calidad = series: "NC Internas" / "NC Externas" (folio anual por equipo); etapas kanban: Abierta → Seguimiento → Cerrada + Cancelada (mapa exacto de su A/S/C).
- Campos F-P-G05-01: origen (proceso/auditoría interna/auditoría externa/reclamación/indicador incumplido), solicitante+cargo, auditor líder, clasificación (Mayor/Menor/Observación), requisito (norma+cláusula), proceso detectado (m2o al mapa), responsables m2m con puesto, desviación, correcciones (líneas), tiempo de implantación, verificación de eficacia (texto+adjuntos+fecha+quién), comentarios de seguimiento, ¿requirió AC?, acción a seguir (Exhorto/A. Administrativa/N-A), verificó/aprobó.
- **5 Porqués estructurado** (problema→porqué 1-5→causa raíz) + Ishikawa 5-6M opcional (PROT-05).
- **Candados (el 45% de NCs abiertas es el enemigo):** no pasar a Cerrada sin causa raíz + acciones terminadas + verificación de eficacia. Automatización: actividad al responsable al crear; escalamiento al Jefe MAST y SGI a los N días (externas: 3 días, acuerdo de Dirección).
- Concentrado F-P-G05-02 = vista lista (FECHA|FOLIO|HALLAZGO|M/m/OB|RESPONSABLE|ESTATUS|CIERRE) por año e int/ext, exportable.
- Reporte QWeb con el layout del formato oficial para auditorías.
- Botones nativos de Quality ya permiten levantar alerta desde picking/orden de fabricación/Shop Floor.

**1.4 Reclamaciones de cliente (Helpdesk)**
- Equipo "Reclamaciones de cliente": alias de correo, portal, SLA ≤3 días de primera respuesta, etapas (Nueva→Contención→Análisis→Respuesta→Cerrada).
- Campos: pedido/factura, producto, lote, metros, disposición. Botón "Generar NC" (origen=reclamación). After-sales nativo para devolución/nota de crédito/reparación.

**1.5 Mejora continua (Proyecto)**
- Proyecto "Mejora Continua" con etiqueta por área (o un proyecto por área), tarea = mejora, subtareas = tareas del plan.
- Extensión: tipo de mejora (ambiental/proceso/recurso/otros); candados: fechas inicio/fin obligatorias y evidencia adjunta para cerrar (sus dos hallazgos recurrentes de auditoría).

**1.6 Mapa de procesos (custom, columna vertebral)**
- `sgi.process`: código, nombre, tipo (COP/estratégico/soporte), macroproceso padre, dueño, departamento, puestos (m2m), documentos aplicables, indicadores, contadores de salud (NCs abiertas, acciones vencidas).
- `sgi.process.flow`: entregable, proceso origen → proceso destino, formato de entrega (m2o documento), criterio de aceptación, modelo Odoo que lo materializa. Validación: sin salidas sin receptor.
- **Datos precargados de su SGI real:** 5 macroprocesos (ANEXO 2), cadena COP (ANEXO 3: CxC→Ventas→Planeación→Compras→Producción→Inspección→Almacén→Facturación→Cobro→Atención al Cliente + soportes) y flujos SIPOC leídos (Ventas F-P-A28-13/14 → Planeación; Planeación F-P-A12-01/02/03 → Producción/Almacenes/Ventas; Compras; Auditoría de Calidad TAC).

**Criterios de aceptación Fase 1:** ciclo NC completo con candados funcionando; cambio documental con 2 aprobaciones y NEWS generado; acuses activos; mapa navegable con flujos reales; reclamación → NC trazada de punta a punta; el concentrado en pantalla reemplaza al Excel.

### FASE 2 — Gestión y medición

**2.1 Indicadores (F-P-A10-03 → `sgi.indicator`)**
- Modelo: proceso, objetivo integral al que aporta (ANEXO 6), fórmula/fuente (selection: manual / OTD ventas / entregas compras / NCs por proceso / cierre de acciones / reclamaciones / disponibilidad mantenimiento / scrap / rotación RH / etc.), meta, sentido, frecuencia, responsable. Mediciones mensuales (`sgi.indicator.measure`) con semáforo.
- Los ~60 KPIs actuales se cargan con sus metas reales; los calculables de Odoo se llenan por cron; los demás, captura mensual con actividad recordatoria.
- **NC automática:** cron de cierre de mes → medición fuera de meta genera `quality.alert` origen "indicador incumplido" al responsable (automatiza ~60% de sus NCs actuales).
- Visualización: dashboards de Hojas de cálculo con datos vivos + vistas pivot/gráfica.

**2.2 Auditorías internas (P-G03)**
- `sgi.audit.program` (anual, F-P-G03-01) + `sgi.audit` (procesos, auditor líder/equipo, validación auditor ≠ dueño del proceso, fechas plan/real) + hallazgos (conformidad/observación/NC menor/NC mayor/oportunidad).
- Checklist en Encuestas (tipo matriz, puntaje) ejecutable en tableta — mismo hábito actual del Auditor de Calidad.
- Hallazgo → botón crea quality.alert origen "auditoría interna". Cierre de auditoría exige disposición de todos los hallazgos. Las NCR de SIDE (F0902E) entran como NC externas con nº de proyecto.

**2.3 Riesgos y oportunidades (`sgi.risk`)**
- Un modelo, 5 tipos con campos por tipo: RyO (F-P-C09-02: 18 campos, impacto×probabilidad → 4 niveles de atención), IPER (F-P-S01-01: P×C 1-3, condición R/NOR/E), aspectos ambientales (F-P-E01-01 + controles operacionales), patrimonial (F-P-A14-02: P×I 1-5), FODA por puesto (F-P-C09-01).
- Acciones de mitigación = mismas líneas de acción de las NC; riesgo residual re-evaluado; revisión anual programada; riesgos altos aparecen solos en la Revisión por la Dirección.

**2.4 Proveedores**
- Dashboard nativo de Compras (% on-time por proveedor) + conteo de alertas origen recepción/proveedor.
- `res.partner`: clasificación **Acreditado/Condicionado/Baja** (sus términos) + score; smart buttons NCs/evaluaciones. Proveedor "Baja/Condicionado" → actividad a Compras. Modelo mini de evaluación por periodo solo si quieren histórico formal.
- Requisición interna: categoría "Solicitud de compra" en Aprobaciones → RFQ.

**2.5 Revisión por la Dirección (IT-P-A10-01)**
- `sgi.management.review`: fecha, asistentes, y **snapshot automático de entradas 9.3.2**: % cumplimiento de acuerdos previos, NCs int/ext por estado, reclamaciones y SLA, resultados de auditorías, KPIs vs meta, proveedores, riesgos altos, ambiental (scrap/ESG), capacitaciones vencidas.
- Acuerdos = tareas de Proyecto con responsable/fecha → el % de cumplimiento de la siguiente revisión se calcula solo. 4-5 revisiones/año como hoy.

**2.6 Voz del cliente y del trabajador**
- Encuesta de satisfacción (Encuestas + Marketing Automation para envío periódico y seguimiento a no-respondientes).
- Buzón QR (F-P-A10-04): formulario web → ticket Helpdesk equipo interno; sirve también para reportes de condiciones inseguras (participación 45001).

**Criterios de aceptación Fase 2:** cierre de mes genera mediciones y NCs automáticas correctas (validar 1 mes contra la realidad antes de confiar); primera auditoría interna ejecutada en tableta; acta de revisión con inputs auto-llenados.

### FASE 3 — Core tools, recursos y tri-norma completa

**3.1 Plan de control (P-C11) sobre quality.point**
- Las Master Specs de Continental se cargan como puntos de control por producto/operación: característica, nominal/min/max, método, frecuencia (todas/aleatoria %/periódica), worksheet con condiciones de éxito. Extensión: criticidad F-R-S, flag "va al CoA", cpk objetivo.
- Failure location → cuarentena automática del rollo rechazado. El CoA/reporte de conformidad bilingüe (F-P-C07-xx) sale de las medidas capturadas (reporte QWeb) y se firma con Sign (mejora que Laboratorio ya pidió).

**3.2 Calibración (P-C03) sobre Mantenimiento**
- Cada instrumento (metros MLT WESCO, balanzas, dinamómetro, equipo de laboratorio) = `maintenance.equipment` categoría "Equipo de medición" con preventivo recurrente "Calibración" + worksheet (criterio de aceptación).
- Extensión: resultado (conforme/fuera de tolerancia), certificado adjunto, próxima fecha, estado "no usar". Fuera de tolerancia → obliga alerta de calidad para evaluar producto medido desde la última calibración (IATF 7.1.5).

**3.3 AMEF (P-C10, `sgi.fmea`)**
- Por producto/proceso; líneas: paso, modo de falla, efecto, S, causa, O, controles, D, NPR calculado con semáforo; NPR alto exige acción. Sus AMEF (2017-2019) se migran y actualizan.
- Pegamento PROT-05/D7: al cerrar una NC mayor, pregunta "¿actualiza AMEF / plan de control / lecciones aprendidas (Conocimiento)?" y crea la actividad.

**3.4 PPAP (P-C15, `sgi.ppap`)**
- Expediente por cliente+producto+nivel (1-5): 18 elementos como checklist con estado (N/A, pendiente, listo, aprobado), cada uno **referenciando** el registro real (AMEF, quality.points, diagrama de flujo en Documentos, estudios). Estados: preparación → enviado → aprobado/interino/rechazado. Cumple los manuales de Seiren (8D con contención 24h → plantilla de actividades sobre la NC).

**3.5 Cambios de ingeniería (PLM)**
- ECO types por dominio (producto/BoM/proceso) con etapas custom y aprobadores por rol (3 niveles nativos: requerido/opcional/solo comenta). Versionado de BoM con historial. Extensión: flags "¿requiere re-PPAP?" "¿avisar al cliente?". El F-P-G01-06 queda solo para documentos del SGI; el cambio técnico vive en PLM.

**3.6 Competencias y capacitación (P-A01)**
- Catálogo de skills + **Expected Skills por puesto** (matriz de competencias de los 50 puestos, desde sus F-P-A01-01).
- DNC anual = Encuesta (F-P-A01-17); programa anual = cursos eLearning + Eventos (presencial: asistencia con check-in, gafetes, quiz de eficacia); **Certificaciones con vigencia** (alerta nativa a 90 días).
- Inducción ≤3 meses = Onboarding plan con actividades encadenadas. Rotación ≤5% = Retention report nativo. Gap analysis puesto↔empleado: reporte custom solo si el nativo no alcanza.

**3.7 SST y ambiental (45001/14001)**
- Incidentes/accidentes (P-S02): `sgi.incident` con SCAT (o segundo equipo Helpdesk interno si se prefiere sin código), liga a IPER y acciones.
- EPP (P-S03): Equipment asignado a empleado con vigencia. Montacargas: Fleet (servicios, odómetro, accidentes).
- PROT-01: **Frontdesk** con estación por acceso (caseta/recepción), QR de auto-check-in, campos obligatorios (empresa para contratistas), notificación al anfitrión; reglamento de contratistas firmado vía Sign. Citas de visitas/auditorías: Appointments.
- Ambiental: scrap con motivos + cuentas de merma (residuos), app ESG (energía, Scopes, iniciativas→Proyecto), planes de RSU/RP como documentos + KPIs en indicadores.
- Requisitos legales (19 NOMs): Documentos con tags + fecha de revisión + responsable.

**Criterios de aceptación Fase 3:** una inspección real ejecutando el plan de control con cuarentena automática; una calibración vencida bloqueando visualmente; un PPAP armado 100% con referencias; un ECO aprobado con re-versión de BoM; matriz de competencias cargada con vigencias activas.

### FASE 4 — Piso, automatización y visibilidad

1. **Shop Floor** en tablets por centro de trabajo: bitácoras por máquina (registro con lote, paros con motivo, checks integrados, worksheets = sus IT). Sustituye F-IT-P-P01-xx en papel.
2. **Barcode GS1** por rollo (GTIN+lote+cantidad) con impresoras Zebra vía quality point "Print Label"; tarjeta viajera digital = lote con propiedades custom + traceability report.
3. **IoT**: básculas de piso a recepciones/producción (ojo: cámaras y herramientas de medición NO funcionan con IoT virtual de Windows — usar IoT Box física), impresoras de etiquetas.
4. **WhatsApp nativo**: plantillas aprobadas para alertas de calidad/paros a supervisores; PWA con push para actividades.
5. **Dashboards**: app Dashboards (sobre spreadsheets con datos vivos) — panel por proceso con semáforos, panel de Dirección.
6. **Portal del auditor externo**: carpetas de Documentos compartidas solo-lectura **con fecha de expiración de permiso** para la auditoría de SIDE.
7. **Supabase/Quimibond Intelligence**: agregar al sync de `quimibond_intelligence` los modelos SGI (quality.alert, indicadores/mediciones, scores de proveedor, mediciones de quality checks para cpk/SPC — que no es nativo). Respetar reglas de crons de Odoo.sh (lotes, idempotencia).
8. **Data Cleaning**: deduplicación de partners/productos (mejora también el sync actual a Supabase).

## 5. Diseño del addon `quimibond_sgi` (resumen)

Modelos **nuevos**: `sgi.process`, `sgi.process.flow`, `sgi.document.ack`, `sgi.indicator`, `sgi.indicator.measure`, `sgi.audit.program`, `sgi.audit`, `sgi.audit.finding`, `sgi.risk`, `sgi.management.review`, `sgi.fmea` (+líneas), `sgi.ppap` (+elementos), `sgi.incident`, `sgi.action.line` (líneas de acción de NC), catálogos (`sgi.area`, `sgi.norm`, `sgi.norm.clause`).

Modelos **extendidos** (solo campos + lógica): `documents.document`, `approval.request`/`approval.category`, `quality.alert`, `quality.point`, `maintenance.equipment`, `maintenance.request`, `res.partner`, `hr.job`, `project.task`, `helpdesk.ticket`, `mrp.eco`.

Reportes QWeb: F-P-G05-01 (NC), F-P-G01-16 (NEWS), CoA/reporte de conformidad, acta de Revisión por la Dirección, ficha de proceso.

Crons: vencimientos diarios (acciones, revisiones documentales, pilotos, acuses, calibraciones, certificaciones), cierre mensual de indicadores + NC automáticas, borrador de NEWS mensual.

**Reglas de despliegue:** desarrollo en rama → staging de Odoo.sh → producción; instalar el módulo manualmente desde Apps; nunca tocar la versión del manifest de `quimibond_intelligence`; crons con lotes pequeños e idempotentes (límites FTQ de Odoo.sh).

## 6. Orden de implementación recomendado

Fase 0 → 1.1/1.2 (documental) → 1.3 (NC) → 1.4 (reclamaciones) → 1.6 (mapa) → 1.5 (mejoras) → **go-live 1 y estabilizar** → 2.1 (indicadores, validar 1 mes) → 2.2 auditorías → 2.3 riesgos → 2.4 proveedores → 2.5 revisión dirección → **go-live 2** → 3.1 plan de control → 3.2 calibración → 3.3/3.4 AMEF/PPAP → 3.5 PLM → 3.6 capacitación → 3.7 SST/ambiental → **go-live 3** → Fase 4 progresiva.

## 7. Riesgos y decisiones abiertas

1. **Adopción**: la carga inicial (procesos, documentos vigentes, KPIs con metas, matriz de competencias) es el 50% del éxito. Los datos leídos del Drive permiten precargar casi todo.
2. **Higiene de datos**: los KPIs automáticos (OTD, scrap) dependen de capturar fechas compromiso y motivos reales; validar cada KPI un mes contra el Excel actual antes de apagar el Excel.
3. **Verificar en la instancia**: etapas custom de quality.alert por equipo, opciones exactas de la app Aprobaciones (sin docs oficiales), disponibilidad de PLM/WhatsApp/IoT en la suscripción.
4. **Studio**: si algo ya está hecho con Studio en producción, plan de migración a módulo antes del upgrade a 20.
5. **Multi-compañía**: si algún día separan razones sociales (PNTQ vs Quimibond comercial), decidir branches vs compañías ANTES de crear datos (un padre no puede volverse branch después).
