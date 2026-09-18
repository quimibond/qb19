# Situación de la empresa — diseño (2026-09-18)

Sustituye al módulo `qb_obligation` (v3.0.0). Aprobado por el CEO en la sesión
del 18-sep-2026, sección por sección.

## 1. Propósito

Un **modelo de situación de la empresa** que bots de IA mantienen al día leyendo
Odoo, el SAT, el correo de los 52 buzones y el grafo de la memoria, y que
presentan al CEO ordenado por área: obligaciones (lo que debemos), créditos (lo
que nos deben), problemas (lo roto o atorado), riesgos y oportunidades. El CEO
lo consume por **Claude por MCP** y por **un correo diario**; decide qué delega
y a quién. Los dueños de área **no** lo ven en esta fase.

No es un gestor de tareas para la gente. Es la capa que hace que cualquier bot o
rutina de Claude entienda "qué está pasando" con contexto completo, sin
rehacer el análisis desde cero y sin tropezar con datos viejos, basura o
duplicados.

### Por qué se rediseña

`qb_obligation` nació pensado en cobranza y quedó como bandeja de candidatos:
74 registros, todos de correo, 60 "por confirmar", 0 confirmados, tocados solo
por el CEO y OdooBot. Mientras tanto la empresa tiene abierto (corte 18-sep):
567 actividades vencidas (251 de una sola persona), 549 órdenes de producción
confirmadas sin arrancar, 73 entregas a cliente con fecha vencida, 413 facturas
de cliente vencidas ($53.6M), 175 de proveedor ($4.4M), 1,037 movimientos
bancarios sin conciliar, 119 existencias negativas, 399 conversaciones de correo
abiertas (255 esperando respuesta nuestra), 57 hilos de cliente sin respuesta en
7 días. Nada de eso llega a un solo lugar. Además, el costeo
(`qb_capacidad_costeo`) y el flujo de efectivo (`quimibond_cash_flow`) ya
calculan estados en rojo que no se le avisan a nadie.

## 2. Decisiones de arquitectura

| Decisión | Motivo |
|---|---|
| El mapa vive en **Supabase** (`situaciones`), junto a la memoria de correo y el grafo | Ahí está el contexto rico; los bots (Edge Functions) ya corren ahí; Claude lo lee por MCP |
| Odoo **no guarda** el mapa; lo **muestra** (fase 2) leyendo por REST, como hoy la pestaña Memoria | Sin tablas espejo; una sola verdad |
| Supabase **no copia cifras de Odoo**: guarda **señales derivadas** (una fila por hecho, con `modelo + id` del documento) | Respeta la decisión del 17-sep ("Supabase solo lo que Odoo no tiene"); la cifra viva se consulta en Odoo |
| **SQL hace lo determinístico** (identidad, edad, dedup, agrupación, calidad, reglas); **la IA solo redacta y decide sobre lo que cambió** | Costo bajo, comportamiento predecible, auditable |
| Lo único que el sistema escribe en Odoo es la **actividad nativa** cuando el CEO delega | El sistema propone; no modifica documentos de negocio |
| `qb_obligation` se **retira** en el paso 6 (junto con la app de Odoo); sus 74 registros se migran como situaciones de origen `memoria` en el paso 2 | Evitar dos mapas |

## 3. Modelo de datos (Supabase, esquema `public`)

### 3.1 `senales` — hechos determinísticos, sin IA

Una señal es "esto está así ahora". Se produce cada hora y se identifica por
clave estable.

| Columna | Tipo | Notas |
|---|---|---|
| `id` | bigserial PK | Una fila por **episodio**: si la señal se resuelve y reaparece (factura vencida otra vez tras un abono, OP re-atrasada) se crea una fila nueva; la anterior conserva su `resuelta_en`. Índice único `(clave) WHERE resuelta_en IS NULL` |
| `clave` | text | `entrega_vencida:stock.picking:1234`, `cliente_sin_respuesta:company:6031`. La define la fuente, nunca la IA |
| `episodio` | int | 1, 2, 3… para la misma clave; la situación registra "reapareció (3ª vez)" |
| `senal` | text | Nombre del catálogo (§4) |
| `area` | text | `comercial`, `operaciones`, `compras`, `finanzas`, `calidad_sgi`, `rh`, `sistemas`, `direccion` |
| `tipo` | text | `obligacion`, `credito`, `problema`, `riesgo`, `oportunidad`, `higiene` |
| `fuente` | text | `odoo`, `memoria`, `sat`, `watchdog` |
| `documentos` | jsonb | `[{"modelo":"sale.order","id":1234,"nombre":"SO/2026/0410"}]` |
| `company_id`, `odoo_partner_id` | bigint | Contraparte (nullable) |
| `responsable_odoo_user_id` | int | Dueño natural del documento en Odoo |
| `valor` | numeric | Monto, días, cantidad (según la señal) |
| `valor_texto` | text | Detalle corto legible ("7 facturas, 38 días") |
| `vence` | date | Fecha comprometida si existe |
| `primera_vista`, `vista_en` | timestamptz | Primera y última corrida que la vio |
| `valor_cambio_en` | timestamptz | Última vez que `valor` cambió |
| `resuelta_en` | timestamptz | Se llena cuando una corrida ya no la ve |
| `calidad` | text | `viva`, `antigua`, `zombie`, `dato_malo`, `vencida_memoria`, `ignorada` (§5) |
| `calidad_motivo` | text | Regla que la clasificó |
| `payload` | jsonb | Campos extra de la señal (moneda, etapa, etc.) |

Índices: único `(clave) WHERE resuelta_en IS NULL`, `(senal, resuelta_en)`, `(area, calidad)`, `(company_id)`.

### 3.1.1 `senales_lotes` — un lote por señal y corrida

`id`, `senal`, `fuente`, `corrida` (uuid que Odoo genera por push), `recibido_en`,
`n_claves`, `ok` boolean, `error` text. **La resolución se calcula por señal y
solo cuando llegó su lote completo**: al ingerir el lote de `cartera_vencida`,
las filas abiertas de `cartera_vencida` cuyas claves no vienen en el lote se
marcan `resuelta_en`. Si el lote de una señal no llegó o llegó con `ok=false`,
sus filas no se tocan y `situacion_salud()` la reporta como `sin_datos` con la
edad del último lote bueno. Así un método del push que falle (cada `_push_*`
falla aislado en `_run_push`) o un push atrasado nunca cierra situaciones por
error.

### 3.2 `senales_config` — catálogo y umbrales, editables

`senal` PK, `area`, `tipo`, `fuente`, `activa`, `umbrales` jsonb (p.ej.
`{"dias":7}`), `severidad_base` (1-5) y `severidad_max`, `reglas_calidad` jsonb
(cuándo es `zombie`/`dato_malo`, ver §5), `agrupar_por` (`contraparte`,
`documento`, `responsable`, `ninguno`), `descripcion` (para que el bot sepa qué
significa). Cambiar un umbral es un `UPDATE`, no un despliegue.

### 3.3 `situaciones` — unidades de atención

| Columna | Tipo | Notas |
|---|---|---|
| `id` | bigint PK | |
| `clave` | text UNIQUE | `senal|agrupador`, p.ej. `cartera_vencida|company:1742` |
| `area`, `tipo` | text | Heredados de la señal dominante |
| `titulo` | text | Una línea, escrita por la IA |
| `resumen` | text | Un párrafo: qué pasa, desde cuándo, con quién, cuánto |
| `company_id`, `odoo_partner_id` | bigint | Contraparte |
| `documentos` | jsonb | Unión de los documentos de sus señales |
| `evidencia` | jsonb | `{"senales":[claves], "threads":[ids], "emails":[ids], "facts":[ids]}` |
| `responsable_sugerido_user_id` | int | Inferido: dueño del documento contrastado con `memoria_encargados` y el grafo |
| `responsable_motivo` | text | Por qué esa persona |
| `severidad` | smallint | 1-5, dentro de la banda `[severidad_base, severidad_max]` de la señal |
| `desde`, `vence` | date | Primera detección; compromiso |
| `estado` | text | `abierta`, `empeoro`, `mejoro`, `resuelta`, `descartada`, `delegada` |
| `calidad` | text | Peor calidad entre sus señales vivas (`viva` > `antigua` > …) |
| `recomendacion` | text | Qué haría la IA, con la acción concreta |
| `delegacion` | jsonb | `{"user_id", "fecha", "texto", "mail_activity_id", "estado"}` |
| `historia` | jsonb | Lista de `{fecha, evento, detalle}`: creada, empeoró (+8 días), fusionada con X, delegada, resuelta por evidencia |
| `dias_abierta`, `dias_sin_cambio` | int (generados) | Para que ningún lector infiera nada |
| `ultimo_cambio` | text | "empeoró: +8 días, cliente reclamó el 17-sep" |
| `ia_version`, `ia_modelo`, `updated_at` | | Trazabilidad |

### 3.4 `situacion_reglas` — decisiones del CEO que persisten

`id`, `alcance` (`senal`, `contraparte`, `documento`, `situacion`), `clave_alcance`,
`accion` (`ignorar`, `no_es_problema`, `severidad_fija`, `responsable_fijo`),
`valor` jsonb, `motivo`, `vigente_hasta` (nullable), `creada_por`, `creada_en`.
El bot las aplica **antes** de crear o actualizar; lo ignorado se cuenta, no
desaparece.

### 3.5 `situacion_corridas` — bitácora

Una fila por corrida del bot: cuántas señales, cuántas candidatas, cuántas
situaciones nuevas/actualizadas/resueltas, tokens, errores. `situacion_salud()`
lee de aquí.

## 4. Catálogo de señales (arranque)

Cada señal tiene una consulta en su fuente y una clave. Umbrales en
`senales_config`. "Hoy" = corte del 18-sep-2026.

### Comercial
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `entrega_vencida` | `stock.picking` outgoing, estado no done/cancel, `scheduled_date < hoy`; agrupa por cliente | 73 |
| `pedido_sin_fecha` | `sale.order` state=sale sin `commitment_date` y con líneas por entregar | por medir |
| `cliente_sin_respuesta` | memoria: `threads.status IN ('needs_response','stalled')` con `last_sender_type='external'` y `last_activity < hoy − 3 d` (campos que `memoria_link_recent` ya mantiene); agrupa por empresa | 57 (7 d) |
| `compromiso_correo` | memoria: `memoria_thread_summaries.pendientes` con `quien='nosotros'`; clave por conversación + hash del texto | 452 |
| `cliente_callado` | memoria + facturas: sin correo ni pedido en > 2× su intervalo habitual (ritmo del contacto ya calculado en `memoria_link_recent`) | por medir |
| `oportunidad_demanda` | `customer_demand_signals` cuya `id` aún no es clave de señal (`oportunidad_demanda:demand:<id>`); no requiere columna nueva | 752 |
| `lead_frio` | `crm.lead` abierto sin actividad > 14 d | por medir |
| `venta_margen_negativo` | `sale.order.line` state=sale, `margin < 0`, 90 d; **`dato_malo` si costo = 0 o precio = 0** | 41 líneas |
| `producto_pierde` | `qb.producto.rentabilidad.semaforo = 'rojo'` (computado; el push lo lee por ORM) | por medir |
| `cliente_pierde` | `qb.cliente.rentabilidad.semaforo = 'rojo'` | por medir |
| `cotizacion_bajo_costo` | `qb.cotizacion.semaforo = 'rojo'` en draft/done; `validez_hasta ≤ hoy+15` = por vencer | por medir |

### Operaciones
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `op_atrasada` | `mrp.production` confirmed/progress con `date_start < hoy − 7`; **`zombie` si `date_start` < hoy − 90 y sin movimientos** | 296 |
| `op_sin_componentes` | OPs de esta semana con `stock.move` de componente sin reservar | por medir |
| `tiempos_excepcion` | `qb.workorder.excepcion` de la semana (`lento`, `rapido`, `sin_horas`) | 15 (4 sem) |
| `existencia_negativa` | `stock.quant` interno con `quantity < 0`; agrupa por ubicación | 119 |
| `reorden_pendiente` | `stock.warehouse.orderpoint.qty_to_order > 0` | 26 |
| `transferencia_atorada` | `stock.picking` internal/mrp_operation confirmed/waiting > 7 d; **`zombie` > 90 d** | 2,094 (mayoría zombie) |
| `familia_saturada` | `qb.familia.carga.utilization_pct ≥ 90` | por medir |
| `mantenimiento_abierto` | `maintenance.request` etapa no final > 7 d; preventivo vencido | 5 |

### Compras
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `recepcion_vencida` | `stock.picking` incoming, `scheduled_date < hoy`; agrupa por proveedor | 171 |
| `oc_sin_confirmacion` | `purchase.order` state=purchase sin `date_planned` o sin acuse (actividad/correo) en 5 d | por medir |
| `proveedor_esperando` / `esperando_proveedor` | memoria: `esperando_a` en conversaciones con proveedor | dentro de 452/252 |
| `aprobacion_pendiente` | `approval.request` new/pending > 2 d; `purchase.requisition` abierta | 7 |
| `precio_compra_subio` | `purchase.order.line` último precio vs promedio 6 m > umbral % | por medir |
| `actividad_vencida_oc` | `mail.activity` sobre `purchase.order` vencida | 139 |
| `proveedor_reprobado` | `sgi.supplier.eval.score < 70` | por medir |

### Finanzas
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `cartera_vencida` | `account.move` out_invoice posted, `payment_state` not_paid/partial, `invoice_date_due < hoy`; agrupa por cliente; **`dato_malo` si el RFC del cliente está en la lista de partes relacionadas** (`senales_config.umbrales.rfc_relacionados` de esta señal; arranque con los 5 RFC de `20260426_ap_delay_related_party.sql`) | 413, $53.6M |
| `promesa_pago_vencida` | memoria: `email_pending_actions.tipo='promesa_pago'` con `deadline < hoy`; se une a `cartera_vencida` del mismo cliente | 30 abiertas |
| `cxp_vencida` | `account.move` in_invoice vencida; agrupa por proveedor | 175, $4.4M |
| `factura_proveedor_borrador` | `account.move` in_invoice draft > 3 d | 12 |
| `entregado_sin_facturar` | `sale.order` `invoice_status='to invoice'` | 36 |
| `banco_sin_conciliar` | `account.bank.statement.line.is_reconciled=false`; agrupa por diario, con antigüedad | 1,037 |
| `cfdi_cancelacion_pendiente` | `account.move.l10n_mx_edi_cfdi_state='cancel_requested'` | 8 |
| `sat_discrepancia` | `sat.compare.line.issue in (monto, moneda, cancelado_odoo, cancelado_sat, solo_sat, solo_odoo)` | por medir |
| `sat_complemento` | `sat.pago.compare.issue in (sin_complemento, complemento_duplicado)` | por medir |
| `sat_extraccion_detenida` | `res.company.sat_data_until_*` > 3 d | 0 |
| `nomina_borrador` | `hr.payslip` draft con `date_to < hoy − 3` | 311 (agosto+) |
| `cash_bajo_piso` | `cash.flow.forecast.engine.compute(config)` → semanas con `closing < forecast_min_cash`; runway = primera semana ≤ 0 | por medir |
| `indicador_financiero_rojo` | `sgi.indicator.measure` validada en rojo para DSO, cartera > 60, DPO | por medir |

### Calidad / SGI
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `indicador_rojo` | `sgi.indicator.measure` `semaphore='rojo'`, `state='validado'`, periodo reciente (**no** `sgi.indicator.last_semaphore`: no es consultable) | por medir |
| `accion_correctiva_vencida` | `sgi.action.line.state='vencida'` | 0 |
| `nc_abierta` | `quality.alert` de equipos SGI en etapa no final | 5 |
| `reclamacion_cliente` | memoria: conversación con tono tenso + categoría calidad, o pendiente tipo reclamación | por medir |
| `calibracion_vencida` | `maintenance.equipment.sgi_calibration_state='vencido'` o `sgi_do_not_use` | por medir |
| `legal_incumplido` | `sgi.legal.requirement.compliance_state in (no_cumple, parcial)` o `next_eval_date ≤ hoy` | por medir |
| `riesgo_sin_tratar` | `sgi.risk.attention_level in (inmediata, alto)` con `state='identificado'` | por medir |
| `ppap_rechazado`, `auditoria_pendiente` | `sgi.ppap.state='rechazado'`; `sgi.audit.program.line.state='pendiente'` vencida | por medir |
| `fuente_sgi_apagada` | `sgi.alert.source.enabled=false` con `suppressed_count > 0` (tipo `higiene`: explica silencios) | por medir |

### RH
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `aprobacion_rh` | `approval.request` de categoría RH pendiente; `hr.leave` por aprobar | por medir |
| `pendiente_rh_correo` | memoria: pendientes en conversaciones de RH (finiquitos, IMSS, altas) | por medir |
| `evaluacion_vencida` | `hr.appraisal` vencida; `sgi.competence.gap` abierta | por medir |

### Sistemas
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `ticket_abierto` | `helpdesk.ticket` no resuelto > 7 d | 10 |
| `job_caido` | watchdog: `memoria_cron_health`, `odoo_push_last_events` > 6 h, crons de Odoo sin corrida | 0 |

### Dirección
| Señal | Fuente y regla | Hoy |
|---|---|---|
| `carga_actividades` | `mail.activity` vencidas por usuario (**`zombie` > 180 d**); agrupa por persona | 567 |
| `firma_pendiente` | `sign.request` pendiente > 7 d | 128 |
| `acuse_documento` | `sgi.document.ack.state='pendiente'` | por medir |
| `acuerdo_direccion_vencido` | `sgi.management.review.agreement` vencido | por medir |

**Reglas del catálogo**

1. Cada señal trae `responsable_odoo_user_id` = dueño natural en Odoo. Para las
   señales de **memoria** (que solo conocen buzones) el responsable se resuelve
   en SQL: buzón que más participa en la conversación → `odoo_users.email`; si
   es buzón compartido (rhmexico@, ventas@…), la tabla `buzon_personas` que el
   push de usuarios manda desde la configuración "Buzones (memoria)" de
   `qb_memoria` (nueva columna en `_push_users`).
2. Las señales de Odoo se calculan en Odoo (`_push_senales`, un método por
   señal, todos en `quimibond_intelligence/models/senales/`). **Contrato de
   ingesta:** una llamada por señal al RPC
   `senales_ingestar(p_senal, p_fuente, p_corrida uuid, p_filas jsonb)` con la
   **lista completa** de claves activas y sus valores; el RPC hace upsert por
   clave abierta, abre episodio nuevo para claves que reaparecen, cierra las
   abiertas que no vienen en el lote, y registra el lote en `senales_lotes`.
   Al terminar todos los métodos, Odoo llama `senales_push_terminado(p_corrida)`,
   que dispara la consolidación (`invoke_edge('situacion-consolidar')`): el bot
   corre **después** del push por evento, no por reloj. Las de memoria y SAT-en-
   Supabase se calculan en SQL (`senales_memoria()`) dentro de la misma
   consolidación. Las señales caras (cash flow, costeo, SGI) llevan
   `senales_config.cada_horas` (p.ej. 6) y el push las omite fuera de su turno
   sin cerrar nada (no manda lote, no hay resolución). Nada se copia en masa.
3. Lo que no está en `senales_config` no existe para la IA. Agregar una señal =
   agregar una consulta + una fila de config, sin tocar el bot.
4. Las apps de Studio "Calendario de obligaciones", "Actividades obligatorias",
   "No conformidades" y "Plan de pagos" están vacías: el mapa las sustituye; no
   se integran.

## 5. Calidad: viejo, basura, duplicado

### 5.1 Vida de una señal
`primera_vista`, `vista_en`, `valor_cambio_en`, `resuelta_en`, `episodio`.
Una señal se resuelve solo cuando **llega el lote completo de su señal** y su
clave no viene en él (§3.1.1); nunca por ausencia de push. Si reaparece, es un
episodio nuevo (fila nueva, misma clave). Nunca se borra: la historia es la
evidencia.

### 5.2 Etiquetas de calidad (SQL, reglas en `senales_config.reglas_calidad`)

| Etiqueta | Regla | Qué hace el bot |
|---|---|---|
| `viva` | Vista hoy y con cambio de valor o correo nuevo en < 30 d | La razona y redacta |
| `antigua` | Abierta > 30 d sin cambio de valor ni correo | No la razona cada hora; entra al bloque semanal "rezago" con su edad |
| `zombie` | Estado imposible por edad (OP confirmada > 90 d sin movimientos, transferencia interna confirmada > 90 d, actividad vencida > 180 d, pedido de 2025 sin entregar ni cancelar) | Sale del mapa operativo; se agrupa en **situaciones de higiene** por clase con la limpieza recomendada en Odoo |
| `dato_malo` | El número no puede ser: margen negativo con costo 0 o precio 0; cartera vencida de parte relacionada; contacto no-reply; factura con moneda inconsistente | Situación de higiene con el campo culpable; nunca se mezcla con problemas reales |
| `vencida_memoria` | Compromiso o promesa con `vence` pasado y sin correo nuevo en 21 d | Se cierra como "expiró sin respuesta"; se menciona una vez como riesgo |
| `ignorada` | Cubierta por una regla del CEO | Se cuenta, no se muestra |

Umbrales (30, 90, 180, 21 días) en `senales_config`.

### 5.3 Duplicados, en tres capas
1. **Señal**: clave estable, imposible duplicar.
2. **Situación**: agrupación determinística por `agrupar_por` (contraparte + señal). Siete facturas vencidas del mismo cliente son una situación con siete documentos; la promesa de pago del correo y la factura vencida del mismo cliente caen en la misma por construcción.
3. **Semántica**: el bot recibe, con cada candidata, las situaciones abiertas de la misma contraparte con título parecido (trigram) y decide `fusionar` o `distinta` con motivo. Cada fusión queda en `historia` y es reversible (`situacion_decidir(..., 'separar')`).

### 5.4 Antigüedad visible siempre
Toda fila que devuelve cualquier RPC trae `calidad`, `dias_abierta`,
`dias_sin_cambio` y `ultimo_cambio`. Un bot lector filtra `calidad='viva'` y
no infiere nada.

## 6. El bot de situaciones (`situacion-consolidar`, Edge Function)

Cada hora, después del push de Odoo:

1. `senales_actualizar()` (SQL): recalcula edad y `calidad`, aplica reglas del CEO. (La resolución ya la hizo `senales_ingestar` por lote, §3.1.1; aquí solo se propaga a las situaciones.) El bot arranca por evento (`senales_push_terminado`) y, como respaldo, por pg_cron cada hora si no corrió en los últimos 50 min; si el último lote bueno de una señal tiene más de 2 h, sus situaciones se marcan `sin_datos` en `situacion_salud` y no se cierran.
2. `situacion_candidatas()` (SQL): agrupa señales vivas en candidatas; devuelve solo las **nuevas, empeoradas (valor o severidad subió) o resueltas** desde la corrida anterior, con tope por corrida (config, 40).
3. Por candidata, `situacion_contexto()` arma el contexto: señales y documentos, ficha de memoria de la empresa (`memoria_brief`), últimas conversaciones ligadas (resumen, no correos completos), situaciones hermanas abiertas, posibles duplicados, reglas aplicables, historia previa.
4. Claude (Sonnet, `effort low`, JSON cerrado) devuelve: `titulo`, `resumen`, `severidad` (dentro de la banda), `responsable_sugerido` + `motivo`, `recomendacion`, `estado` (`abierta|empeoro|mejoro`), `duplicados: [{id, decision, motivo}]`, `evento_historia`. Si se corta o no es JSON, la candidata se reintenta en la siguiente corrida y se registra en `situacion_corridas`.
5. `situacion_guardar()` (SQL, transacción): upsert por clave, fusiones, historia, `ultimo_cambio`.
6. Resueltas: sin IA; SQL cierra y escribe "resuelta por evidencia: <señal desapareció>".

Prompt del bot (contrato): habla desde Quimibond; solo lo que está en el
contexto; una situación = una decisión posible del CEO; la recomendación nombra
la acción, al responsable y el documento; severidad justificada con la regla;
español neutro; nunca copia correos completos.

Costo estimado: ~4k tokens de entrada y ~500 de salida por candidata; ≤ 40 por
hora ≈ 1,000/día máximo en Sonnet. `token_usage.endpoint='situacion-consolidar'`.

Lo que el bot **nunca** hace: modificar Odoo; crear situaciones sin señal;
salir de la banda de severidad; borrar historia; ejecutar limpiezas.

## 7. Handoff: consumo

### 7.1 RPCs (Supabase, jsonb)

| RPC | Devuelve |
|---|---|
| `situacion_mapa(p_area text default null, p_calidad text default 'viva', p_min_severidad int default 1, p_limit int default 100)` | Filas compactas: id, área, tipo, título, severidad, contraparte, responsable sugerido, días abierta, último cambio, calidad, estado |
| `situacion_contexto(p_id)` | Todo: resumen, recomendación, documentos, evidencia con resúmenes de conversación y hechos citados, hermanas, historia, reglas aplicables |
| `situacion_cambios(p_desde timestamptz)` | Nuevas, empeoradas, mejoradas, resueltas, delegadas desde `p_desde`, agrupadas por área; más contadores de ignoradas e higiene |
| `situacion_por_persona(p_odoo_user_id)` | Situaciones donde es responsable sugerido o delegado |
| `situacion_decidir(p_id, p_accion, p_payload jsonb)` | `delegar` (`user_id`, `texto`, `vence`), `ignorar` (`alcance`, `motivo`, `vigente_hasta`), `resuelta`, `reabrir`, `separar`, `severidad` (fija) |
| `situacion_higiene()` | Zombis y datos malos por clase con conteo, ejemplos y la limpieza recomendada |
| `situacion_salud()` | Edad del último push por señal, salud de memoria, fuentes SGI apagadas, última corrida del bot y errores |

Todas `SECURITY DEFINER`, solo lectura salvo `situacion_decidir`.

### 7.2 Correo diario (`situacion-digest`, Opus, 06:30 hora de México)

Sustituye a `email-digest`. Contenido: por área, lo que cambió en 24 h y lo
abierto con severidad ≥ 4; una línea por situación (título, contraparte, días,
responsable sugerido, recomendación); los lunes, bloque "rezago" con las
`antigua`; pie: "N ignoradas por tus reglas, M en higiene, salud del mapa". Se
arma desde el mismo JSON de `situacion_cambios` para que rutina y correo digan
lo mismo. Destinatario: el CEO (parámetro).

### 7.3 Delegar

`situacion_decidir(id, 'delegar', {user_id, texto, vence})` deja la situación
`delegada` e inserta en `sync_commands` un comando `crear_actividad` con
**`payload` jsonb** (`situacion_id`, `user_id`, `texto`, `vence`, `modelo`,
`res_id`) — columna nueva; hoy la tabla solo tiene `command`. El pull de 5 min
(`_execute_command(command, payload)`, firma extendida) crea la `mail.activity`
sobre el documento (o el contacto si no hay documento) y confirma con el RPC
`situacion_delegacion_confirmar(p_situacion_id, p_mail_activity_id, p_estado)`,
que escribe `delegacion.mail_activity_id`. Estado de vuelta: el push horario
incluye `_push_actividades_delegadas`, que pide a Supabase la lista de
actividades delegadas abiertas (`situacion_delegaciones_abiertas()`), lee su
estado en Odoo (abierta, hecha con `feedback`, cancelada, reasignada) y lo
manda como lote de la señal `delegacion_estado`. Hecha o señal desaparecida ⇒
situación `resuelta` con historia "cerrada por <usuario>: <feedback>";
cancelada ⇒ vuelve a `abierta` con historia. En el mismo paso se corrige el
bug existente del pull, que escribe `status='error'` cuando el CHECK de
`sync_commands` solo admite `failed`.

### 7.4 Odoo (fase 2)

Módulo `qb_situacion`: app "Situación" visible solo para el grupo
`qb_situacion.group_ceo`. Vista lista por área y ficha, leídas en vivo por REST
(`situacion_mapa`, `situacion_contexto`) como `qb_memoria`; botón "Delegar"
que llama `situacion_decidir`. Sin modelos persistentes salvo la configuración
(URL y llave ya existen en `quimibond_intelligence.*`). `qb_obligation` se
desinstala tras migrar sus registros.

## 8. Orden de construcción y aceptación

| Paso | Entrega | Se acepta cuando |
|---|---|---|
| 1 | Esquema (`senales`, `senales_lotes`, `senales_config`, `situaciones`, `situacion_reglas`, `situacion_corridas`), `senales_ingestar()`, `senales_push_terminado()`, `senales_memoria()`, `senales_actualizar()`, `situacion_candidatas()`, `situacion_mapa`, `situacion_contexto`, `situacion_salud` | `select * from situacion_mapa('comercial')` devuelve las conversaciones sin respuesta y los compromisos de correo como situaciones agrupadas por empresa, con calidad y edad |
| 2 | `quimibond_intelligence._push_senales` (finanzas y comercial primero, luego el resto del catálogo), migración de los 74 `qb.obligation` | Las 413 facturas vencidas aparecen agrupadas por cliente; las de partes relacionadas caen en `dato_malo`; las 296 OPs viejas caen en `zombie`; el push corre en < 60 s |
| 3 | Edge Function `situacion-consolidar` + cron horario | 20 situaciones reales redactadas y revisadas por el CEO por MCP; fusiones correctas en 3 casos preparados; costo dentro de estimación |
| 4 | `situacion_cambios`, `situacion-digest`, retiro de `email-digest` | El correo del día siguiente coincide con `situacion_cambios` |
| 5 | `situacion_decidir`, reglas persistentes, `sync_commands.payload`, comando `crear_actividad` en el pull, `situacion_delegacion_confirmar`, `_push_actividades_delegadas`, cierre por actividad hecha, fix del `status='error'` | Delegar una situación crea la actividad en Odoo en ≤ 5 min y marcarla hecha la cierra |
| 6 | `qb_situacion` (app en Odoo) y desinstalación de `qb_obligation` | El CEO ve el mapa en Odoo y delega desde ahí |

Cada paso se prueba con datos reales de producción (lectura) y se documenta en
`CLAUDE.md` de ambos repos. Se planifica en **dos planes**: plan A = pasos 1–3
(esquema, push, bot: el mapa consultable por MCP), plan B = pasos 4–6 (correo,
delegación, app en Odoo).

## 9. Riesgos y mitigaciones

- **Ruido inicial**: las primeras corridas traerán cientos de zombis. Mitigación: la clasificación de calidad entra desde el paso 2, y la primera sesión de higiene es parte de la aceptación del paso 3.
- **Push pesado**: 40+ consultas por hora en Odoo. Mitigación: cada método con su tiempo en `pipeline_logs`; umbral de 60 s; señales caras (cash flow, costeo) cada 6 h en vez de cada hora (config).
- **Deriva del bot** (títulos distintos para lo mismo cada hora): solo se reescribe cuando la candidata cambió; el título se conserva salvo cambio de estado.
- **Reglas de ignorar que esconden cosas**: el correo y `situacion_cambios` siempre cuentan lo ignorado.
- **Odoo caído o push apagado**: `situacion_salud` lo dice y el digest lo encabeza; los crons de sync ya se reactivan solos en cada update.

## 10. Fuera de alcance (esta fase)

Vista para dueños de área; escritura del bot en Odoo más allá de la actividad
delegada; imágenes de correo (fase 3 de adjuntos); predicción (probabilidad de
cobro, etc.); reemplazo de los digests del SGI y del SAT (siguen; el mapa los
absorbe como señales).
