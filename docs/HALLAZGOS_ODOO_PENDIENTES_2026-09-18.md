# Hallazgos pendientes en Odoo (exportado de Supabase `odoo_pending_actions`, 2026-09-18)

Registro de problemas cuya causa raíz está en la configuración o los datos de Odoo, levantados por las auditorías de 2026 (costeo, inventario, contabilidad, SAT). La tabla de Supabase se borró el 2026-09-18 al limpiar el proyecto; este archivo es su única copia. Los montos son estimaciones mensuales en MXN al momento del hallazgo.

Total: 49 hallazgos.

## Abiertos

### Backlog: 281 CFDIs en SAT sin contrapartida operativa en Odoo ($40M)
`operationalize-cfdi-backlog` · contabilidad · Crítico · creado 2026-05-04 · responsable: Contadora (URGENTE) · impacto estimado: $40,300,000/mes

**Problema.** 281 facturas timbradas en SAT post-2021 NO tienen factura correspondiente en Odoo. Impacto acumulado: $40.3M MXN. Cada CFDI quedó timbrado fiscalmente pero la operación contable nunca se cerró en Odoo. Las facturas más grandes son de $1.5–1.8M c/u (11 días viejas las más recientes). Esto significa: el SAT te tiene como ya facturado y reportaste para impuestos, pero tu contabilidad interna no lo refleja → riesgo de auditoría fiscal + utilidad sub-reportada en libros.

**Arreglo en Odoo.** 1. PRIORIDAD INMEDIATA: descargar lista completa con SQL:
   SELECT canonical_id, uuid_sat, impact_mxn, description
   FROM reconciliation_issues
   WHERE invariant_key='invoice.pending_operationalization' AND resolved_at IS NULL
   ORDER BY impact_mxn DESC;

2. Para cada CFDI: identificar la operación faltante. Típicamente es:
   - Self-billing CFDIs (Quimibond emitió pero no registró)
   - Facturas con cancelación SAT pero Odoo no enteró
   - CFDIs duplicados o de prueba que nunca se cancelaron

3. Decisión por CFDI:
   a) Si la operación SÍ existe pero no se enlazó → vincular en Odoo con UUID correcto.
   b) Si la operación NO existe → crear factura en Odoo retroactiva con la fecha del CFDI.
   c) Si el CFDI es inválido → cancelarlo en SAT.

4. PARA EVITAR EN EL FUTURO: configurar webhook de Syntage que detecte CFDI emitido sin operación Odoo dentro de 24h y dispare alerta automática en /inbox.

5. Configurar tarea programada en qb19: cada lunes correr el invariante y notificar a contadora si hay nuevos no-operacionalizados.

**Cómo se sorteaba en Supabase.** Hoy estos issues viven en `reconciliation_issues` y se ven en /inbox + gold_company_odoo_sat_drift. El sistema los detecta pero no los fixea — requiere acción manual de la contadora caso por caso.

### Revaluar inventario PT contaminado con MOD+gastos pre-1-abril ($6.34M)
`revaluar-inventario-pt-contaminacion-avco` · contabilidad · Crítico · creado 2026-05-04 · responsable: Contadora + CEO · impacto estimado: $6,340,000/mes

**Problema.** Quimibond usa AVCO (Average Cost) para valuación de inventario. Hasta antes del 1 de abril 2026, las BOMs incluían MOD y gastos como componentes (productos RSI56), absorbiéndose al PT al producirse. Esos gastos quedaban dentro del avg_cost del PT en almacén.

A partir del 1 de abril 2026, las BOMs se simplificaron: solo materia prima. Los productos RSI56 fueron archivados, lo que rompe el mecanismo de CAPA mensual que normalizaba la diferencia entre AVCO histórico y costo real.

Diagnóstico actual: ~$6.34M (33.6%) del inventario PT vivo de $18.87M está contaminado con MOD+gastos absorbidos via AVCO de fabricaciones pre-abril. Cada vez que se vende un PT viejo (LIFO se mueve), el COGS contable a 501.01.01 lleva el avg_cost AVCO inflado → reporta sobre-costo en P&L contable y la "ganancia" parece menor.

Esto se cancela parcialmente con que los gastos también están dentro de avg_cost (no se cuentan dos veces en P&L contable). Pero el P&L LIMPIO basado en BOM-recursivo (sólo MP) sub-reporta utilidad porque trata el AVCO inflado como si fuera todo MP.

**Arreglo en Odoo.** 1. Identificar productos PT contaminados:
   SELECT op.internal_ref, op.qty_available, op.standard_price, op.qty_available * op.standard_price AS valor_avco,
          (SELECT get_bom_raw_material_cost_per_unit(op.odoo_product_id)) AS bom_mp_unit,
          op.qty_available * (SELECT get_bom_raw_material_cost_per_unit(op.odoo_product_id)) AS valor_bom_mp
   FROM odoo_products op
   WHERE op.is_storable AND op.qty_available > 0;

2. Para cada PT con diff > 5%, decidir:
   a) Re-valuar via Inventory → Operations → Inventory Adjustment a costo BOM-MP (downstream el AVCO se ajusta).
   b) O dejar contaminado y esperar que el inventario rote (FIFO operativo agota el stock viejo en 6-12 meses).
3. Asiento de revaluación: Dr 501.01.02 COSTO PRIMO o cuenta de ajuste / Cr 115.xx Inventario PT.
4. Coordinar con contadora antes de re-valuar masivo (impacto fiscal).

**Cómo se sorteaba en Supabase.** Hoy /finanzas P&L LIMPIO presenta MP via BOM-recursivo y MOD+overhead aparte. El gap "501.01.01 contable − BOM MP" se reporta como diferencia de absorción (parte AVCO contaminado, parte régimen actual sin absorción). Banner en /contabilidad documenta el régimen.

### Duplicación inventario→501.01.02: TVAR+ENC+SP+REQP ($4.15M YTD)
`refacciones-tvar-doble-conteo-501-01-02` · contabilidad · Crítico · creado 2026-05-07 · impacto estimado: $4,154,994/mes

**Problema.** Audit 2026-05-07: el 100% del 501.01.02 YTD ($4.15M) son movimientos Dr 501.01.02 / Cr 115.* (inventario), repartidos en 4 patrones:

  TVAR     $2,865,800 (69.0%)  Refacciones (agujas, bombas, EPP, laptops, mantenimiento)
  SP/      $  775,057 (18.7%)  Empaque (bolsas naturales, tubos cartón)
  TL/ENC// $  425,056 (10.2%)  Encogimientos textiles (proceso tintorería/acabado)
  TL/REQP/ $   52,012 ( 1.3%)  Requisición producción (MP/PT a línea)
  Otros TL $   37,068 ( 0.9%)  Misc Tlatelolco

Todos siguen el flujo Dr 501.01.02 / Cr 115.* y todos son DOBLE CONTEO bajo el régimen actual:

1. Refacciones/empaque YA se contabilizan al comprar como gasto/inventario.
2. Encogimientos textiles deberían quedarse SOLO en inventario (revaluar AVCO del producto siguiente, no impactar P&L).
3. P&L limpio usa BOM-MP recursivo que YA incluye estas MPs/refacciones/empaque vía AVCO de compras.

Contexto adicional: post-1-abril-2026 RSI56 fue archivado. La cuenta 501.01.02 (cierre histórico para CAPA mensual) debería estar prácticamente vacía. Los $4.15M YTD son 100% duplicación.

Mensual:
  Ene: $1,007,041
  Feb: $1,131,706
  Mar: $1,164,540
  Abr: $  828,706
  YTD: $4,131,993 (los $22k restantes son ruido sin prefijo identificable)

**Arreglo en Odoo.** 1) Crear cuenta de inventario operativo dedicada (ej. 115.05.* "Inventario operativo no-MP") configurada para que su consumo NO impacte 501.* (mandar a cuenta de gasto operativo 605.* o 504.* según naturaleza).
2) Reconfigurar las CATEGORÍAS de productos en Odoo (Stock Output Account):
   • Refacciones (TVAR) → 605.* mantenimiento o 504.01.* overhead
   • Empaque (SP/) → 504.01.* overhead empaque
   • Encogimientos (TL/ENC) → solo movimiento entre 115.* (sin tocar P&L)
   • Requisición producción (TL/REQP) → revisar caso por caso
3) Postear asientos de ajuste por mes para limpiar duplicado YTD ($4.15M):
     Cr 501.01.02                               $4,154,994
     Dr 504.01.0099 / nueva 115.05 / puente     $4,154,994
   (4 asientos de ene, feb, mar, abr según el monto del mes)
4) Una vez configurada la cuenta nueva y reconfiguradas las categorías,
   marcar este pending action como resolved.

**Cómo se sorteaba en Supabase.** 1) RPC silver get_inventory_to_cost_dup_501_01_02(p_from, p_to) detecta los 4 patrones por prefijo de ref/line_name y retorna desglose por mes.
2) pnl.ts expone cogs501_01_02DupInventoryMxn (total) + cogs501_01_02DupBreakdown (por prefijo) + cogs501_01_02CleanMxn (sin duplicación).
3) PnlComparisonTable muestra fila 501.01.02 con contable=full, limpio=clean — la diferencia se ve explícita.
4) PnlAdjustmentEntryCard sugiere asiento de ajuste con desglose por prefijo y plan de remediation.

### qb19 cron de push a Supabase dejó de dispararse el 2026-05-12
`qb19-cron-stopped-2026-05-12` · sync · Crítico · creado 2026-05-14 · responsable: CEO + sistemas

**Problema.** El cron horario del addon quimibond_intelligence en Odoo.sh dejó de correr el 2026-05-12 a las 21:10 UTC. No hubo error de aplicación: el push final cerró limpio y luego silencio total. Resultado: todas las tablas odoo_* en Supabase están 47h+ atrasadas (al 14-may), y odoo_workorders / odoo_account_entries_stock / odoo_workcenters llevan 11-21 días sin sync. Toda la UI (finanzas, contabilidad, cash, P&L) muestra datos del 12-may como si fueran de hoy.

**Arreglo en Odoo.** 1. Conectarse a Odoo.sh shell del proyecto qb19 (branch quimibond).
2. Verificar el estado del cron: en /odoo/web ir a Settings → Technical → Automation → Scheduled Actions; buscar "Quimibond Intelligence - Push a Supabase". Confirmar que Active=true y Next Execution Date está pasado.
3. Si Next Execution Date está stuck en una fecha vieja: click "Run Manually" para forzar una corrida. Si run manual también falla, ver Server Logs en Odoo.sh para el error.
4. Si la corrida manual funciona: verificar que se restablezca la programación (Repeat Every = 1 Hour). Posible causa: alguien desactivó el cron o el worker de cron de Odoo.sh murió.
5. Si el worker murió, restart del servidor en Odoo.sh (Settings → Restart).
6. Verificar éxito desde Supabase: SELECT * FROM odoo_sync_freshness WHERE status != 'fresh'; debe quedar todo fresh.

**Cómo se sorteaba en Supabase.** No hay workaround real: los datos en Supabase NO se actualizan sin el push de qb19. Las MVs y vistas Gold siguen calculando, pero sobre datos del 12-may. Las páginas tienen banners de "Datos al 2026-05-12" hasta que el push se restablezca.

### Realinear cuenta de valuación por categoría de producto (115.01.01 negativa, ventas desde WIP)
`cuentas-valuacion-categoria-realinear` · contabilidad · Crítico · creado 2026-07-02 · responsable: Contadora + Mariano · impacto estimado: $3,140,672/mes

**Problema.** El mapa categoría de producto → cuenta de valuación está roto en dos formas:
1. Productos VENDIBLES viven en categorías "Producto en Proceso / *" (Acabado, Carda, Importación) cuya cuenta de valuación es 115.03.01 Producción en proceso. Al facturarlos, el COGS se alivia desde el WIP: $2.4M de costo vendido salió de 115.03.01 vía facturas de cliente solo abril–junio 2026.
2. En JUNIO 2026 varias categorías (Tejido Circular sin resina, Subproducto, y varias "en Proceso") se cambiaron a la cuenta 115.01.01 "Inventario" SIN asiento de transferencia de saldos: las salidas se acreditan en 115.01.01 pero el stock entró históricamente por 115.03/115.04. Resultado: 115.01.01 nació NEGATIVA (−$2.0M al 30-jun, −$3.14M al 2-jul y bajando). Una cuenta de inventario nunca puede ser negativa.
Además apareció 115.04.03 en mayo (facturas de proveedor) que luego desapareció del GL por reclasificaciones posteriores.

**Arreglo en Odoo.** 1. Definir el mapa OFICIAL (una sola vez, con la contadora):
   - Toda categoría de producto vendible (telas, entretelas, subproductos, importados) → valuación 115.04.01 Productos terminados.
   - Materias primas (hilo, químicos, resina, fibra) → 115.02.01.
   - Refacciones/consumibles → 115.02.02 (ver acción refacciones-fuera-ciclo-textil).
   - 115.03.01 SOLO para WIP automático de MOs (no como cuenta de valuación de ninguna categoría).
   - Decidir si 115.01.01 se depreca (recomendado: dejarla en cero y desactivarla) o se vuelve la única cuenta de PT — pero NO mixto.
2. En Inventario → Configuración → Categorías de producto: corregir cuenta de valuación, cuenta de entrada/salida de stock de CADA categoría según el mapa.
3. Por cada categoría cambiada: asiento de transferencia de saldo (valor del stock on-hand de esa categoría) de la cuenta vieja a la nueva, mismo día del cambio. Esto lleva 115.01.01 a su valor real (positivo) y saca de 115.03/115.04 lo que ya no les corresponde.
4. Regla operativa: cambiar la cuenta de valuación de una categoría REQUIERE su asiento de transferencia el mismo día.

**Cómo se sorteaba en Supabase.** La conciliación física-contable de /inventario/conciliacion y la invariante nueva inventory.negative_bucket (plan fase 4) flaggean saldos 115.x negativos. Silver no puede corregir la causa: es 100% configuración de Odoo.

### Reemplazar el asiento CAPA (costo primo manual) por política de valuación — y depurar los $13.5M en 115.03.01
`capa-valoracion-manual-detener` · contabilidad · Crítico · creado 2026-07-02 · responsable: Contadora + CEO · impacto estimado: $15,068,423/mes

**Problema.** PROPÓSITO REAL (aclarado por CEO 2026-07-02): el asiento mensual en CAPA DE VALORACIÓN existe para que 501.01.01 refleje COSTO PRIMO sin gastos — le quita al COGS los gastos de transformación que el AVCO trae absorbidos (contaminación pre-abril), porque MOD+overhead ya se presentan una vez en 501.06/504.01. El objetivo es legítimo; el MECANISMO tiene 3 problemas medidos:
1. NO LLEGA: incluso después del CAPA, 501.01.01 queda en ~2× el costo primo BOM (residual post-CAPA $4.3–6.2M/mes en 2026). Hoy la cuenta no es ni AVCO ni costo primo — es un híbrido sin calibración auditable.
2. ROMPE EL BALANCE: la contrapartida se debita a inventario (115.03.01, jun a 115.04.01) sin respaldo físico — $13.46M acumulados 2026 (~87% del saldo de WIP; solo hay ~50 MOs abiertas). Los gastos removidos del COGS no pueden vivir en inventario. Histórico: crédito a 501.01.01 de $35.9M (2024) y $48.7M (2025).
3. ES MANUAL Y MENSUAL: cálculo fuera del sistema, destino inestable (WIP ene–may, PT jun).

**Arreglo en Odoo.** El fix de raíz es lograr el MISMO objetivo (501.01.01 = costo primo) por VALUACIÓN, no por asiento mensual:
1. Decidir política con la contadora (Decisión D1 de la auditoría):
   (a) Libros ABSORBENTES (cumple NIF C-4: inventario a MP+gastos de producción) — revaluación a MP+fabricación absorbida; el costo primo sin gastos queda como vista analítica (P&L limpio de la plataforma / subcuentas informativas), o
   (b) Libros a COSTO PRIMO (variable costing) — revaluación a MP-solo: el AVCO del despacho ES costo primo automáticamente. Es la presentación que busca el CEO directamente en libros; la contadora debe resolver el tratamiento NIF/fiscal al cierre.
2. En AMBOS caminos: CONGELAR el journal CAPA (ya no es necesario — la valuación corregida hace el trabajo).
3. Depurar el saldo estacionado: $13.46M de 2026 en 115.03.01 (+ arrastre 2024-25) se reconocen al depurar — eran gastos reales de períodos pasados que no pueden quedarse en el balance (coordinado con la revaluación, Decisión D2).
4. Verificar contra qué cuenta debitaba el CAPA en 2024-2025 (fuera del scope 115/501/504 sincronizado).

**Cómo se sorteaba en Supabase.** El P&L limpio de /finanzas YA entrega la vista de costo primo sin gastos de forma analítica (fila 501.01.01 reemplazada por BOM-recursivo + Δ vs contable) — el CEO la puede usar desde hoy sin el asiento. cogs_monthly_cache trackea cogs_contable_raw / cogs_capa_valoracion / cogs_recursive_mp para auditar la calibración mes a mes. La invariante capa_journal_activity (Fase 4) alertará asientos nuevos.

### Incorporar el conteo físico de junio al corte final (asientos cancelados; 999998 limpia)
`conteo-junio-reclasificado-999998` · contabilidad · Crítico · creado 2026-07-02 · responsable: Contadora + CEO · impacto estimado: $3,571,753/mes

**Problema.** CORRECCIÓN 2026-07-03: el hallazgo original decía que $3.57M del conteo de junio se reclasificaron a equity 999998. Era un FALSO POSITIVO — la fila de 999998 en odoo_account_balances es sintética (utilidad neta mensual que el sync fabrica para el balance sheet); tras extender el sync a cuentas 999% y re-push completo 2026 se verificó que existen CERO asientos reales en 999998. Lo que sí pasó: los asientos del conteo (Cantidad de producto actualizada, $6.4M de cargos) fueron CANCELADOS por el CEO, así que las diferencias físicas del conteo hoy NO están reflejadas en el GL.

**Arreglo en Odoo.** En el corte final, incorporar el resultado del conteo con destino correcto por grupo (evidencia en clasificacion-conteo-evidencia-2026.xlsx): (1) máquinas de tejer y equipo ($4.24M SIN_PRODUCTO) → verificar físicamente y dar de alta como ACTIVO FIJO, no inventario; (2) refacciones fantasma ($3.60M) → ajuste contra resultados acumulados (REA) como corrección de error de períodos anteriores (NIF B-1), no P&L del año; (3) diferencias textiles reales (neto −$520k, sobrante) → 501.01.08; (4) refacciones reales ($69k) → gasto de mantenimiento 504.01.0005.

**Cómo se sorteaba en Supabase.** El fix de qb19 (line_ids.write_date en el incremental de _push_account_entries_stock, 2026-07-02) hace que futuras ediciones de líneas sí se re-sinquen; para el histórico hay que re-push heavy desde 2026-06-01. La invariante nueva equity_manual_posting (plan fase 4) alertará movimientos manuales a 999998.

### Capitalizar flete/aduana al producto importado vía Landed Cost
`capitalize-import-landed-cost` · compras · Alto · creado 2026-05-04 · responsable: Contadora + Elena Delgado · impacto estimado: $210,000/mes

**Problema.** 504.01.0035 "GASTOS DE IMPORTACION" recibe ~$210k/mes pero parece NO estar capitalizándose al avg_cost del producto importado (no encontramos evidencia de uso del módulo Landed Cost). Resultado: el avg_cost_mxn de importados subestima sistemáticamente el costo real, e infla el margen aparente. Sin esto, mi fix de imports en BOM-recursive (que usa avg_cost para sufijo " I") sigue sub-valuado.

**Arreglo en Odoo.** 1. Activar el módulo "Landed Costs" en Odoo (Inventory → Configuration → Settings → Landed Costs ON).\n2. Configurar productos de servicio: "Flete importación", "Aduana", "Agente aduanal" como is_landed_cost=true.\n3. En cada compra de importación, crear una "Landed Costs" entry vinculada al picking de entrada y prorratear el costo al producto.\n4. Validar que post-cierre el avg_cost_mxn del producto importado sube por el monto del landed cost.

**Cómo se sorteaba en Supabase.** Hoy avg_cost_mxn refleja solo costo del proveedor extranjero. Mientras se arregla, podríamos prorratear 504.01.0035 mensual contra los SKUs " I" en silver, pero introduciría complejidad. Mejor esperar fix Odoo.

### Sincronizar cancelaciones SAT → Odoo (85 facturas, $3.6M)
`sync-sat-cancellations-to-odoo` · contabilidad · Alto · creado 2026-05-04 · responsable: Contadora + Mariano · impacto estimado: $3,580,000/mes

**Problema.** 85 facturas están "posted" en Odoo pero "cancelado" en SAT. Esto pasa cuando alguien canceló el CFDI directamente en el portal SAT (no vía Odoo) y la cancelación nunca llegó de vuelta al sistema interno. Resultado: tu Odoo cree que cobraste/debes esas facturas pero en realidad están canceladas fiscalmente. Impacto $3.58M en saldos AR/AP fantasma.

**Arreglo en Odoo.** 1. Activar webhook SAT → Syntage → Odoo para cancelaciones (qb19 ya tiene el endpoint pero falta el config en SAT):
   - Ir a SAT portal → Configuración → Servicios web → Notificaciones cancelación
   - Configurar URL del webhook de Syntage (preguntar a Syntage por URL específica)

2. Para las 85 ya canceladas en SAT pero posted en Odoo:
   - Lista en: SELECT canonical_id, uuid_sat, impact_mxn FROM reconciliation_issues WHERE invariant_key='invoice.state_mismatch_posted_cancelled' AND resolved_at IS NULL;
   - Hacer reverso/cancelación manual en Odoo de cada una (Accounting → Customer Invoices → seleccionar → Cancel).

3. CONTROL FUTURO: mensualmente correr el invariante state_mismatch_posted_cancelled y notificar si > 5 nuevos.

**Cómo se sorteaba en Supabase.** reconciliation_issues con invariant_key=invoice.state_mismatch_posted_cancelled lista todos. Aparecen en /inbox priorizadas por impact_mxn.

### Capturar facturas de proveedores que están en SAT pero faltan en Odoo ($6.4M)
`capture-ap-invoices-from-sat` · compras · Alto · creado 2026-05-04 · responsable: Contadora + Elena Delgado · impacto estimado: $6,360,000/mes

**Problema.** 193 CFDIs de proveedores (recibidos por Quimibond) están en SAT pero NO en Odoo. Quimibond ya los recibió fiscalmente (el SAT te los reporta como AP) pero contabilidad no los capturó como gasto/inventario. Impacto $6.36M en gasto sub-reportado. Significa que el P&L contable está SOBREESTIMADO en utilidad por ese monto, y el inventario o el gasto está sub-reportado.

**Arreglo en Odoo.** 1. Lista de los 193 CFDIs:
   SELECT canonical_id, uuid_sat, impact_mxn, description
   FROM reconciliation_issues
   WHERE invariant_key='invoice.ap_sat_only_drift' AND resolved_at IS NULL
   ORDER BY impact_mxn DESC;

2. Para cada uno, capturar la factura recibida en Odoo:
   - Accounting → Vendor Bills → New
   - Importar XML del CFDI (Odoo 19 acepta CFDI 4.0 import directo)
   - Validar producto, monto, cuenta de gasto
   - Post.

3. AUTOMATIZAR: activar el módulo "l10n_mx_edi_vendor_bills" en Odoo (recepción automática de CFDIs vía PAC o buzón SAT). Pasos:
   - Configurar credenciales del PAC en Odoo
   - Activar polling cada 4 horas
   - Validar mapping automático proveedor → cuenta de gasto

4. CONTROL: invariante semanal — si > 10 CFDIs AP sin Odoo en una semana, alerta automática.

**Cómo se sorteaba en Supabase.** reconciliation_issues con invariant_key=invoice.ap_sat_only_drift lista todos. Aparecen en /empresas/[id]/auditoria-sat-tab para cada proveedor afectado.

### Tracking gap entre stock_moves AVCO y BOM-recursivo en MOs activos
`manufacturing-variance-tracking` · productos · Alto · creado 2026-05-04 · responsable: Guadalupe Ramos + Contadora · impacto estimado: $10,230,000/mes

**Problema.** Bajo régimen AVCO + variable costing implícito, la diferencia entre el value de stock_moves de venta (AVCO al despacho real) y el BOM-recursivo (qty × canonical.avg_cost por hoja) es la métrica relevante — NO un "variance" de Standard.

Ejemplo abril 2026:
- Stock moves de venta value: $6.68M
- COGS posteado a 501.01.01: $6.60M (cuadra al 99% con stock_moves)
- BOM-recursivo (sólo MP): $4.25M
- Gap: ~$2.35M (35%)

El gap NO es un bug. Refleja:
1. Contaminación AVCO histórica del PT producido pre-1-abril (MOD+gastos absorbidos vía RSI56, archivado).
2. Drift entre canonical_products.avg_cost_mxn (snapshot) y costo MP real al producir/despachar.

Lo que sí hay que trackear: si el gap se aleja del esperado mes a mes (bajo régimen estable post-abril, debería tender a 0% conforme se rota el PT viejo).

**Arreglo en Odoo.** 1. Para cada MO con variance alto, investigar root cause:
   - ¿La BOM está outdated? (MP cambió pero no se actualizó la lista)
   - ¿Hay scrap excesivo? (defectos en producción)
   - ¿Sustituciones no documentadas? (operario usó otra MP sin actualizar BOM)

2. Actualizar BOMs vivas en Odoo cuando el consumo real difiera consistentemente:
   Manufacturing → BOMs → seleccionar → ajustar cantidades

3. Capacitar a Guadalupe Ramos (producción) para reportar sustituciones en tiempo real desde el módulo MO.

4. ALERTA: configurar invariante manufacturing.material_cost_variance con threshold de >5% para que dispare a /inbox automáticamente.

5. EN SILVER: agregar el variance al P&L limpio como una línea separada "Variance manufactura no en BOM" — actualmente NO se reporta.

**Cómo se sorteaba en Supabase.** reconciliation_issues con invariant_key=manufacturing.material_cost_variance lista todos los MOs con variance. Existe mv_mo_actual_material_cost pero no se surface en /finanzas.

[2026-05-05] Reframe AVCO: el "variance" original asumía Standard. Bajo AVCO el gap es esperado y se reduce conforme rota el PT pre-abril.

### 82 componentes hoja en BOMs sin standard_price ni avg_cost
`assign-cost-to-bom-leaves` · productos · Alto · creado 2026-05-04 · responsable: Gustavo Delgado + Elena Delgado

**Problema.** De 317 productos que son LEAVES (sin BOM, son MP comprada) usadas en BOMs activas, 100 no tienen avg_cost_mxn en canonical. De esas, 82 tampoco tienen standard_price > 0 en Odoo. El cálculo BOM-recursivo asigna $0 a esas hojas → cualquier producto que las use en su BOM tiene costo subreportado por la parte que viene de esas hojas.

**Arreglo en Odoo.** 1. Listar las 82 hojas sin costo:
   SELECT op.internal_ref, op.name, op.active
   FROM odoo_products op
   WHERE op.odoo_product_id IN (
     SELECT DISTINCT bl.odoo_product_id FROM mrp_bom_lines bl
     JOIN mrp_boms b ON b.odoo_bom_id=bl.odoo_bom_id AND b.active
     WHERE NOT EXISTS (SELECT 1 FROM mrp_boms b2 WHERE b2.active AND b2.odoo_product_id=bl.odoo_product_id)
   ) AND (op.standard_price IS NULL OR op.standard_price=0);

2. Para cada uno, decidir:
   a) Si es producto comprado activo: hacer compra de prueba para que Odoo registre avg_cost. O capturar standard_price manualmente con el último costo de compra conocido.
   b) Si es desarrollo / inactivo: archivar el producto.

3. Configurar regla: cualquier producto con tipo=consumable o storable DEBE tener standard_price > 0 antes de usarse en BOMs.

**Cómo se sorteaba en Supabase.** Hojas sin costo contribuyen 0 al BOM-recursivo. Magnitud del impacto en cálculos depende del uso de cada hoja en BOMs activas; auditar caso por caso.

### Configurar workcenters faltantes: ACABADO, TINTORERÍA, ENTRETELAS, INSP/EMPAQUE
`configure-workcenters-acabado-tintoreria-entretelas` · productos · Alto · creado 2026-05-04 · responsable: Guadalupe Ramos + Mariano + CEO · impacto estimado: $3,300,000/mes

**Problema.** Hoy en Odoo solo está configurado el workcenter de TEJIDO CIRCULAR (40 máquinas, $74.57/hr) con go-live MAYO 2026. Los demás procesos productivos (Acabado, Tintorería, Entretelas, Inspección/Empaque) NO tienen workcenter, lo que significa:
1. No se captura tiempo-máquina por orden de manufactura.
2. No se absorbe MOD+overhead al producto al producirse (variable costing implícito).
3. El P&L LIMPIO de costo primo (BOM-recursivo) reporta solo MP — los gastos de transformación viven en cuentas separadas (501.06 nómina, 504.01 overhead).
4. No es comparable a un sistema absorbing costing tipo manufactura industrial estándar.

Impacto Apr 2026 (calculado en silver via get_overhead_by_cost_center + get_nomina_by_cost_center):
- ACABADO: nómina $265k + overhead $1.59M = $1.86M no absorbido
- TINTORERÍA: $247k + ~$300k = $547k no absorbido
- ENTRETELAS: $209k + ~$400k = $609k no absorbido
- INSP/EMPAQUE: $266k + ~$50k = $316k no absorbido
Total: ~$3.3M/mes que está en gastos pero no absorbido al PT.

**Arreglo en Odoo.** 1. Configurar workcenters en Odoo Manufacturing → Configuration → Work Centers:
   - ACABADO (capacidad por turno, costo/hr basado en nómina+overhead históricos)
   - TINTORERIA
   - ENTRETELAS
   - INSPECCION_EMPAQUE
2. Para cada BOM, asignar operations al workcenter correspondiente (Manufacturing → BOMs → Operations tab).
3. Definir time_cycle_manual o ms/unit por operation.
4. Establecer cost/hr en cada workcenter usando burden rate calculado:
   - TEJIDO ~$14.47/kg (ya configurado, $74.57/hr ÷ producción/hr)
   - ACABADO ~$1.22/mt (calcular hr equivalente)
   - TINTORERIA ~$5.55/kg
   - ENTRETELAS ~$2.05/mt
5. Go-live coordinado por proceso, validar 1 mes con shadow accounting.
6. Una vez en producción, el costo MOD+overhead se absorbe al PT al producirse → Inventario PT refleja costo total → COGS al venderse incluye MP+MOD+OH.

**Cómo se sorteaba en Supabase.** Hoy el silver tiene 3 RPCs (get_nomina_by_cost_center, get_overhead_by_cost_center, get_production_by_cost_center) que calculan burden rate por departamento desde la contabilidad. La página /contabilidad/centros-de-costo (en construcción) muestra el desglose. Mientras no esté workcenter configurado, el P&L LIMPIO presenta variable costing implícito (MP en costo, MOD+OH en gastos separados).

### Reinterpretar P&L LIMPIO con régimen real: AVCO + sin workcenters (variable costing implícito)
`pnl-limpio-rewrite-avco-regimen` · contabilidad · Alto · creado 2026-05-04 · responsable: CEO + Contadora + Mariano

**Problema.** Premisa anterior del P&L LIMPIO (documentada en CLAUDE.md hasta 2026-05-04) era incorrecta:
- Suponía valoración Standard con CAPA mensual inflando 501.01.01.
- Asumía que el "swap" 501.01.01 ↔ BOM-recursivo limpiaba la duplicación.

Realidad confirmada con CEO 2026-05-04:
- Valuación es AVCO, NO Standard.
- Workcenters solo configurados en Tejido Circular, go-live MAYO 2026.
- Para Acabado/Tintorería/Entretelas/Empaque: variable costing implícito (MOD+OH viven en gasto separado, no se absorben al PT).
- Pre-abril 2026: BOMs incluían MOD+gastos via componentes RSI56, ahora archivados.
- 501.01.02 COSTO PRIMO ya NO recibe ajuste mensual (porque RSI56 archivado).
- 501.01.01 NO está inflado por CAPA — es el COGS real AVCO al despacho.

Implicaciones:
1. El "swap" 501.01.01 ↔ BOM-recursivo ya NO es una corrección — es una reformulación: muestra lo que costaría a estructura nueva sin contaminación AVCO histórica.
2. El P&L LIMPIO debe presentarse como "P&L régimen actual": MP via BOM + MOD por depto + Overhead por depto (todo separado, variable costing).
3. El "residual 501.01.01 − BOM" representa contaminación AVCO histórica, no error contable.

**Arreglo en Odoo.** No hay fix Odoo único — es una decisión de estructura contable + reporting:

OPCIÓN ELEGIDA (implementada en silver 2026-05-04):
1. Renombrar P&L LIMPIO a "P&L Régimen Actual" o similar.
2. Presentar 4 bloques claros:
   a) Ingresos (4xx neto)
   b) MP consumido (BOM-recursivo MP only)
   c) MOD por departamento (501.06 splitable via NOMINAS journal ref)
   d) Overhead fábrica por departamento (504.01 splitable via overhead_account_assignment + rent_lot_assignment)
3. Margen contributivo material = ingresos − MP.
4. Costo total = MP + MOD + OH fábrica.
5. Eliminar lenguaje "CAPA inflada" — ya no aplica.
6. Mantener 501.01.08 DIFERENCIAS POR CONTEO como atípico investigable (shrinkage físico).

OPCIÓN futura: configurar workcenters en todos los procesos para absorbing costing real (ver pending action configure-workcenters-acabado-tintoreria-entretelas).

**Cómo se sorteaba en Supabase.** Implementado en silver 2026-05-04: 3 RPCs (get_nomina_by_cost_center, get_overhead_by_cost_center, get_production_by_cost_center) + 3 tablas (cost_center_config, overhead_account_assignment, rent_lot_assignment) + página /contabilidad/centros-de-costo. Pnl-Block usa nuevo desglose por departamento.

### Pull de CFDIs SAT (Syntage) parado desde 2026-05-01
`syntage-pull-stopped-2026-05-01` · sync · Alto · creado 2026-05-14 · responsable: CEO + sistemas

**Problema.** syntage_invoices no recibe rows nuevas desde 2026-05-01 (13+ días) y syntage_invoice_payments desde 2026-04-30. Los webhooks reales-time tampoco están entrando (last syntage_webhook warning hace 6h indica error transient pero el pull diario también falla). Impacto: el cruce Odoo↔SAT en gold_company_odoo_sat_drift y los recon invoice.ap_sat_only_drift ($6.4M abiertos) no reflejan facturas timbradas en mayo. Complementos de pago de la quincena del 1-mayo no entran al cashflow_projection.

**Arreglo en Odoo.** 1. Verificar credenciales Syntage en Vercel env vars (SYNTAGE_API_KEY, SYNTAGE_WEBHOOK_SECRET). Si expiraron: pedir nuevo token al proveedor.
2. Probar endpoint manualmente: POST a /api/syntage/cron-daily con Bearer CRON_SECRET; ver si retorna 200 y cuántos rows ingresó.
3. Revisar Vercel cron logs (Project → Logs → Crons) para últimas corridas de /api/syntage/cron-daily.
4. Si el endpoint falla con 401/403 del lado de Syntage: el token está expirado o suspendido.
5. Verificar webhooks: dashboard de Syntage → Webhooks → revisar Delivery Attempts. Si están todos 4xx/5xx, el endpoint /api/syntage/webhook está fallando — ver Vercel logs.
6. Una vez restaurado, forzar pull de los últimos 14 días: POST /api/syntage/cron-daily?since=2026-04-30.
7. Verificar éxito: SELECT MAX(synced_at) FROM syntage_invoices; debe ser <72h.

**Cómo se sorteaba en Supabase.** El frontend usa canonical_invoices.fiscal_* fields (de Odoo, no de SAT) cuando el match SAT no existe. Las pages de drift Odoo↔SAT muestran data del 29-abr.

### Tiempos de workorders (tejido) no son confiables
`workorder-tiempos-no-confiables` · produccion · Alto · creado 2026-06-05 · responsable: Producción / Sistemas

**Problema.** Las duraciones de los workorders de circular van de 432 kg/h (tiempo sub-registrado, casi 0) a 5 kg/h (tiempo inflado, orden abierta días sin cerrar; una llegó a 578h = 24 días). El duration_expected está en 0 (sin tiempos estándar). El total mensual (~10,201h en mayo) es inservible para valuar producción: Odoo multiplica costo/hora × esa duración basura → absorbe mal cada orden.

**Arreglo en Odoo.** Que los operarios CIERREN el workorder al terminar (o auto-cierre al registrar la producción). Cargar tiempos estándar (duration_expected) por producto/máquina. Revisar las órdenes con duración >72h o <0.1h.

**Cómo se sorteaba en Supabase.** El costo estándar del workcenter deriva las horas-máquina de la PRODUCCIÓN (kg ÷ tasa estándar 11 kg/h), no de las duraciones rotas.

### Saldo/desperdicio: política de costo $0 y parar conversiones que jalan costo de telas
`saldo-desperdicio-costo-cero` · contabilidad · Alto · creado 2026-06-12 · responsable: Gustavo Delgado (almacén) + Guadalupe Ramos + Contadora · impacto estimado: $420,000/mes

**Problema.** El desperdicio ya está cobrado en el costo de la tela buena: las BOMs de acabado consumen +12-18% vs peso teórico (verificado vs plan de capacidades de manufactura, que asume 10% de merma). Sin embargo, las conversiones TL/CONV-ART le transfieren costo pleno AVCO de las telas al saldo (~$1.48M feb-may 2026), duplicando el mismo material en 501.01.01. Las telas usadas en conversiones traen $1.5M de faltantes + $1.1M de sobrantes en conteos 2026 (las cantidades convertidas no son rollos físicos identificados). Hay $364k de saldo en stock valuado con costo duplicado (5,401 kg SALDO TEJIDO D + 411 kg SALDO NO TEJIDO D).

**Arreglo en Odoo.** 1) Mantener cost share 0% en subproductos de BOMs de tejido (ya quedó así en junio — formalizarlo como política). 2) Dejar de crear saldo vía conversiones que consumen telas a costo pleno cuando lo embolsado es desperdicio: el saldo debe entrar a costo $0 (ajuste de inventario sin valor). 3) Conversiones con costo SOLO para rollos de primera identificados que se degradan de verdad. 4) Revaluar a $0 el stock actual de saldo ($364k) — una sola vez, contra cuenta de ajuste, para que las ventas futuras de saldo salgan sin costo. 5) Comunicar a almacén: al juntar desperdicio, alta a $0.

**Cómo se sorteaba en Supabase.** El P&L limpio ya trata SALDO*/DESPERDICIO* como costo $0 (is_byproduct, migration 20260602). El contable seguirá duplicado hasta aplicar el fix en Odoo.

### Activos fijos clasificados como consumibles inflan el inventario
`activo-fijo-clasificado-como-inventario` · Inventario / Categorías · Alto · creado 2026-06-22 · responsable: Operaciones / Contabilidad · impacto estimado: $4,112,215/mes

**Problema.** Máquinas (CALDERA Cleaver Brooks $1.99M, ROPE OPENER/ABRIDOR $2.02M, moto-reductor 20HP, etc.) están en la categoría "Generales-Consumibles-Refacciones-pza" → se valúan como inventario en vez de activo fijo. ~$4.1M de inventario falso.

**Arreglo en Odoo.** Crear/usar categoría de Activo Fijo (no inventario) y reasignar estos 4 productos. Verificar que ya estén capitalizados en 153 Maquinaria; si sí, darlos de baja del inventario o marcarlos no almacenables.

**Cómo se sorteaba en Supabase.** Aislados en inventory_valuation_snapshot con flag; excluidos del "inventario operativo".

### 554 productos activos sin categoría real (vacía / "All")
`categorias-producto-sin-clasificar` · Categorías · Alto · creado 2026-06-22 · responsable: Operaciones

**Problema.** 554 productos activos no tienen categoría (o están en "All" genérico): 441 refacciones/consumibles, 75 servicios/gastos, 37 a revisar, 1 tela. Sin categoría → cuentas contables por defecto incorrectas y caen en "Otros" del costeo.

**Arreglo en Odoo.** Asignar categoría según la columna categoria_sugerida del worklist (product_category_cleanup). La mayoría → Refacciones y Consumibles o Servicios y Gastos. Revisar manualmente los 37+1.

**Cómo se sorteaba en Supabase.** Worklist materializado en product_category_cleanup con sugerencia por producto.

### Maquinaria, activo fijo y refacciones cayendo en "Diferencias por conteo" (501.01.08) dentro del costo de ventas
`conteo-maquinaria-refacciones-en-cogs` · contabilidad · Alto · creado 2026-06-25 · responsable: Elena Delgado Ruiz / Contabilidad · impacto estimado: $1,625,104/mes

**Problema.** El ajuste de inventario por conteo físico está mandando maquinaria y refacciones contra 501.01.08 (costo de ventas). YTD 2026: $8.12M de movimientos NO textiles (98.8%) vs solo $99.8k de merma textil real. El neto que quedó en libros en COGS es $1.625M. Incluye una CALDERA CLEAVER BROOKS de $1,993,000 (activo fijo, NO inventario), BIODIGESTOR $238k, controlador de agujas $368k, bombas/motobombas, controladores, EPP/uniformes (calcetín $472k, zapato, chamarra, chaleco), agujas/platinas y químicos. Esto (1) infla el costo de ventas textil (margen bruto real 22.5% vs 18.3% reportado) y (2) puede subestimar la utilidad hasta ~$1.6M si esos activos son fijos mal gastados o no existen físicamente.

**Arreglo en Odoo.** 1) Validar físicamente cada ítem de la lista: ¿existe el activo / debió capitalizarse como activo fijo, o de verdad se consumió/desechó? 2) La CALDERA y equipos (biodigestor, bombas, controladores) deben estar en activo fijo, no en inventario ni en COGS. 3) Reclasificar las refacciones/EPP/químicos a su cuenta de gasto correcta (no 501.01.08 costo de ventas textil). 4) Configurar categorías de producto separadas para refacciones/maquinaria/EPP con cuentas de inventario y gasto distintas a producto terminado, para que el conteo no las mande a COGS.

**Cómo se sorteaba en Supabase.** El modelo de costo reconstruido y el P&L limpio separan el conteo no-textil del costo de tela vendida (clasificación por SKU textil vs descripción). Se reporta el margen bruto textil real (22.5%) aparte de las bajas de maquinaria/refacciones.

### Sacar refacciones y activo fijo del ciclo de inventario textil (doble vía de capitalización + conteos a COGS)
`refacciones-fuera-ciclo-textil` · contabilidad · Alto · creado 2026-07-02 · responsable: Contadora + Elena Delgado + Gustavo Delgado · impacto estimado: $4,154,994/mes

**Problema.** Las refacciones y activos conviven con el inventario textil valuado y contaminan tanto el COGS como el inventario:
1. DOBLE VÍA de capitalización: (a) asiento manual mensual en "Operaciones varias" cargando 115.02.02 Inventario refacciones ($306k ene, $906k feb, $623k mar, $1.02M abr…) y (b) facturas de proveedor capitalizando directo a 115.04.03 (52 asientos en mayo por $352k; cuenta que luego desapareció del GL por más reclasificaciones). Con las compras también pasando por TVAR, ya existe la duplicación medida en refacciones-tvar-doble-conteo-501-01-02 ($4.15M YTD).
2. Los conteos y consumos de refacciones/maquinaria caen a 501.01.08 dentro del costo de ventas (junio: $663k; abril: $254k; ya flaggeado en conteo-maquinaria-refacciones-en-cogs $1.63M).
3. Activo fijo clasificado como consumible infla inventario $4.11M (acción activo-fijo-clasificado-como-inventario).
Esta acción es el FIX DE RAÍZ de configuración que resuelve las tres anteriores.

**Arreglo en Odoo.** 1. Categoría "Refacciones" en Odoo con: valuación automática → 115.02.02, y cuenta de GASTO al consumir → 504.01.xx mantenimiento (NO 501.01.08, NO 501.01.02).
2. UNA sola vía de entrada: la factura del proveedor capitaliza a 115.02.02 vía la categoría. ELIMINAR el asiento manual mensual de Operaciones varias.
3. Ubicación de inventario separada para refacciones con su propia cuenta de ajuste (mantenimiento) — así los conteos de refacciones nunca tocan 501.01.08.
4. Maquinaria/equipo: ficha de activo fijo (depreciación), no producto almacenable.
5. Revisar el ajuste manual acumulado en 115.02.02 ($3.36M) contra el físico de refacciones (~$2.74M a avg_cost): diferencia ~$0.6M a depurar.

**Cómo se sorteaba en Supabase.** get_refacciones_dup_501_01_08 y get_inventory_to_cost_dup_501_01_02 ya miden los síntomas mes a mes; el P&L limpio los descuenta como workaround. Cuando el fix esté en Odoo, esos RPCs deben tender a cero.

### Programa de revaluación de inventario al costo reconstruido (cuadre GL = físico al centavo)
`revaluacion-inventario-costo-reconstruido` · contabilidad · Alto · creado 2026-07-02 · responsable: CEO + Contadora + Mariano · impacto estimado: $8,500,000/mes

**Problema.** Objetivo del CEO: que el GL de inventario cuadre al centavo con el inventario físico valuado al costo correcto (reconstruido). Estado al 2026-07-02:
- GL 115.* (30-jun): $51.62M = 115.01 −$2.00M + 115.02.01 $20.60M + 115.02.02 $3.36M + 115.03 $15.45M + 115.04 $14.21M.
- Físico Odoo (stock_qty × avg_cost, 1,313 SKUs): $43.12M — el gap contra GL son los asientos manuales (CAPA, refacciones, reclasificaciones), NO la valuación automática.
- Físico a costo RECONSTRUIDO: PT/vendibles en catálogo (522 SKUs): $11.95M AVCO vs $8.03M a MP-último-costo vs $17.76M a MP+fabricación absorbida. MP/otros (791 SKUs): $31.17M AVCO vs ~$39.03M a último costo de compra (457 con compra reciente; validar UoM por SKU antes de usar).
- 4 SKUs con stock negativo (−$492k) y 15 SKUs con stock sin costo deben corregirse antes de revaluar.
PREREQUISITOS: cuentas-valuacion-categoria-realinear + capa-valoracion-manual-detener + refacciones-fuera-ciclo-textil + conteo-junio-reclasificado-999998 (si se revalúa sobre la estructura rota, el cuadre es imposible).

**Arreglo en Odoo.** 1. DECISIÓN DE POLÍTICA (CEO): valuar PT a MP+fabricación absorbida (absorbing, recomendado: +$5.8M vs AVCO actual, consistente con /contabilidad/costo-reconstruido) o a MP-último-costo (variable costing: −$3.9M). MP siempre a último costo de compra. Saldo/desperdicio/subproducto a $0 (política ya definida). Refacciones a costo de compra.
2. Validar costo objetivo por SKU: top ~200 SKUs cubren >90% del valor; el resto por familia. Fuente: product_cost_catalog (mp_unit_mxn, fab_absorbido_unit_mxn) + última compra para MP (cuidado con UoM compra≠stock).
3. Corregir stock negativo (4 SKUs) y SKUs sin costo (15) en Odoo.
4. Ejecutar en Odoo 19 AVCO por producto: Inventario → Valuación → "Actualizar costo" (revaluación) con la cuenta de contrapartida acordada con la contadora (recomendado: subcuenta nueva 501.01.09 "Revaluación de inventario" para el efecto P&L, o directo contra la depuración de CAPA cuando aplique). Hacerlo por lote vía import/script de shell — el sistema puede generar el CSV producto→costo_nuevo desde product_cost_catalog.
5. Cierre: verificar GL 115.x = Σ stock_qty × costo_nuevo por bucket, AL CENTAVO. La invariante inventory.gl_vs_physical_drift (tolerancia $50k, luego $1) queda vigilando.
6. Cadencia: repetir trimestral hasta que los workcenters de Acabado/Tintorería/Entretelas absorban solos (entonces AVCO se auto-mantiene).

**Cómo se sorteaba en Supabase.** La página /inventario/conciliacion compara GL vs físico on-read. El catálogo product_cost_catalog (refresh nocturno) es la fuente del costo objetivo. Falta construir: export CSV de revaluación + invariante de drift diaria (fase 4 del plan).

### Workcenter Tejido: abono de mano de obra a 501.01.01 (debe ser cuenta de absorción)
`workcenter-mano-obra-cuenta-absorcion` · contabilidad · Alto · creado 2026-07-03 · impacto estimado: $500,000/mes

**Problema.** Desde el go-live (may-2026) los asientos "TL/OP-TEJ - Mano de obra" eran CIRCULARES (cargo y abono a 115.03.01, $3.03M mayo + $1.13M junio, efecto cero — la absorción nunca operó). El ~30-jun la cuenta del abono cambió a 501.01.01 COSTO PRIMO: ahora capitaliza WIP pero contamina el costo primo ($33.9k en jul 1-2, ~$500k-1M/mes proyectado) y rompe la calibración costo primo = recetas.

**Arreglo en Odoo.** En el workcenter TEJIDO CIRCULAR (y los que se den de alta después): configurar la cuenta de costo/producción a una cuenta de ABSORCIÓN dedicada nueva (crear 501.06.90 "MOD Y GIF ABSORBIDOS A PRODUCCIÓN", naturaleza acreedora contra-gasto). Resultado correcto: cargo 115.03.01 WIP / abono 501.06.90. El P&L mostrará nómina real menos absorbida = ociosidad visible, y 501.01.01 queda puro.

**Cómo se sorteaba en Supabase.** Guardia inventory.costo_primo_leak (hourly) detecta cada asiento TL/* que toque 501.01.0x. El modelo de costeo no se afecta (usa GL 501.06 completo).

### WK140R70JNT165 (Bowen): BOM con hilo caro y base equivocada
`bowen-wk140-bom-hilo-incorrecto` · Costeo / BOM · Alto · creado 2026-07-29 · responsable: Ingeniería / Costos

**Problema.** La BOM del interlock 140 para Bowen explota a HS0100960100ISN (hilo 100/96 microfibra $73.40/kg) vía un intermedio WK120R70 de 120g×1.70. Da MP $19.63/m ($1.13 USD) = 63% del precio target $1.80 → margen −31% falso. El producto debe ir con hilo 100/36 reciclado (~$43.5/kg) y base 140g×1.65 (o 125g).

**Arreglo en Odoo.** Corregir la BOM: (1) cambiar el hilo a 100/36 reciclado (HP0100360100ESN o equivalente); (2) alinear la base a 140g×1.65 (no el intermedio WK120R70 de 120g×1.70); (3) revisar la merma (consume 0.267 kg de hilo). Con eso el costo baja a ~$1.42 y a $1.80 da +5 a +13% neto.

**Cómo se sorteaba en Supabase.** El cotizador muestra la fila corregida (100/36) al lado de la actual como referencia; NO usar el costo de la BOM actual para decidir Bowen.

### Deduplicar SKUs con múltiples versiones activas (canonical apunta a inactiva)
`dedupe-active-products` · productos · Medio · creado 2026-05-04 · responsable: Mariano + Jessica Francisco

**Problema.** Hay al menos 4 productos vendidos con 2-4 versiones distintas en Odoo (mismo internal_ref, diferentes odoo_product_id, una activa y otras archivadas). Ejemplos: POLYCOTTON 140 GR (IXJ140Q21JNT162), Entretela A60BL155, WM4026NG152, TEJIDO CIRCULAR WC120Q11JNT165. canonical_products tiene la versión INACTIVA, lo que rompía COGS hasta el fix BOM-recursive. La causa raíz es duplicación en Odoo cuando alguien crea un producto sin verificar si ya existe.

**Arreglo en Odoo.** 1. Identificar todos los duplicados con: SELECT internal_ref, COUNT(DISTINCT odoo_product_id) FROM odoo_products WHERE active=true GROUP BY internal_ref HAVING COUNT > 1.\n2. Para cada duplicado: decidir cuál mantener como canonical (la activa más reciente con BOM y avg_cost OK).\n3. Hacer merge en Odoo: Inventory → Products → Merge tool.\n4. Establecer regla de unicidad en internal_ref a nivel Odoo si es posible (puede requerir custom validación en qb19).

**Cómo se sorteaba en Supabase.** En silver, get_bom_raw_material_cost_per_unit ya costea correctamente vía BOM-recursive aunque canonical_products no tenga la versión activa. Pero queda como tech debt para limpiar.

### Distinguir notas de crédito de devolución física vs ajuste de precio
`distinguish-physical-return-vs-price-nc` · ventas · Medio · creado 2026-05-04 · responsable: Contadora + Guadalupe Guerrero · impacto estimado: $100,000/mes

**Problema.** Hoy todas las out_refund se procesan igual en silver: restamos el COGS recursivo asumiendo que la mercancía regresó al inventario. Pero algunas NCs son solo bonificación/descuento sin movimiento físico (no hay stock_move de retorno). En esos casos NO deberíamos revertir el COGS — el bien sigue vendido, solo bajó el precio. Magnitud abril 2026: ~$280k de NCs, mezcla de ambos tipos.

**Arreglo en Odoo.** 1. En Odoo, asegurar que SIEMPRE que se cree una NC con devolución física, se haga via "Reverse" desde la factura original (lo cual genera el stock_move de retorno automáticamente).\n2. Para NCs de bonificación/descuento sin retorno físico, capacitar al equipo de ventas para usar "Credit Note Type: Discount" o etiquetar manualmente.\n3. Idealmente agregar campo custom is_physical_return en account.move (qb19 addon).

**Cómo se sorteaba en Supabase.** En silver, modificar get_cogs_recursive_mp para incluir solo out_refund que tengan stock_move asociado (vía origin LIKE refund_name). Pendiente de implementar — query a stock_moves.origin timeó por tamaño, requiere índice.

### Validar monto CFDI vs Odoo antes de timbrar (213 mismatches, $4.5M)
`validate-cfdi-amount-pre-timbre` · ventas · Medio · creado 2026-05-04 · responsable: Contadora + Mariano · impacto estimado: $4,480,000/mes

**Problema.** 213 facturas tienen UUID matched (mismo CFDI) pero el MONTO en SAT difiere del de Odoo más allá de la tolerancia. Esto pasa cuando se modifica una factura DESPUÉS de timbrar (descuentos, ajustes, líneas extra) y la modificación se hace solo en Odoo sin re-timbrar. Resultado: SAT muestra monto X, Odoo muestra monto Y, contabilidad no cuadra fiscalmente. Impacto $4.48M.

**Arreglo en Odoo.** 1. Configurar regla en Odoo: bloquear cualquier modificación a una factura ya timbrada (state="posted" + tiene cfdi_uuid).
   - Settings → Technical → Server Actions → New
   - Trigger: account.move write
   - Condition: state='posted' AND cfdi_uuid IS NOT NULL
   - Action: raise UserError("Factura ya timbrada — para modificar, cancelar y re-timbrar")

2. Para los 213 mismatch existentes:
   SELECT canonical_id, uuid_sat, impact_mxn FROM reconciliation_issues WHERE invariant_key='invoice.amount_mismatch' AND resolved_at IS NULL;
   Decidir caso por caso: cancelar+retimbrar OR ajustar Odoo al SAT (si SAT es la fuente de verdad).

3. INVARIANTE preventivo: pre-timbre, validar que monto Odoo == monto a timbrar antes de enviar al PAC.

**Cómo se sorteaba en Supabase.** reconciliation_issues con invariant_key=invoice.amount_mismatch. Tolerance configurable en audit_tolerances tabla.

### Productos con 2+ BOMs activas (89 productos)
`merge-multi-active-boms` · productos · Medio · creado 2026-05-04 · responsable: Guadalupe Ramos + Mariano

**Problema.** Hay 89 productos con 2 o más BOMs marcadas active=true en Odoo. La función BOM-recursivo escoge UNA por criterio (después del fix: la primera con líneas, prefiriendo code='' y menor odoo_bom_id). Pero esto es ambiguo si las múltiples BOMs tienen contenido distinto: ¿cuál es la "verdadera"? Riesgo: el cálculo de costo no representa la receta real usada en producción.

**Arreglo en Odoo.** 1. Listar productos con multi-BOM activa:
   SELECT odoo_product_id, COUNT(*) FROM mrp_boms WHERE active=true GROUP BY odoo_product_id HAVING COUNT(*)>1;

2. Para cada producto, decidir cuál BOM es la versión "actual":
   a) Revisar con Guadalupe Ramos / Producción cuál se está usando físicamente.
   b) Las demás → archivar (active=false) en Odoo.

3. Establecer convención: solo 1 BOM activa por producto. Versiones anteriores → archivadas con código de versión en el campo "code".

4. Considerar validación en qb19 que bloquee crear BOM activa si ya hay otra activa para el mismo producto.

**Cómo se sorteaba en Supabase.** Función actual selecciona una con criterio determinístico (líneas > 0, code='', menor id). No falla pero puede no representar la receta real.

### Resync canonical_products: 14 productos vendidos sin entry, 484 apuntando a inactivos
`audit-canonical-products-coverage` · productos · Medio · creado 2026-05-04 · responsable: Mariano + Jessica Francisco

**Problema.** Auditoría detectó 2 problemas de cobertura en canonical_products:
1. 14 productos vendidos en 2026 NO tienen entry en canonical_products (versiones nuevas / activas que no se sincronizaron).
2. 484 entries en canonical_products apuntan a un odoo_product_id que está INACTIVO en Odoo (versiones viejas).

Impacto: el BOM-recursivo busca avg_cost en canonical por odoo_product_id; si el ID activo no está en canonical, falla. La función ya tiene fallback decente pero la data debería estar limpia.

**Arreglo en Odoo.** 1. Para los 14 productos vendidos sin canonical, ejecutar matcher_product manualmente:
   SELECT matcher_product(internal_ref, name) FROM odoo_products WHERE odoo_product_id IN (...);

2. Para los 484 que apuntan a inactivos, decidir caso por caso:
   a) Si el internal_ref tiene una versión ACTIVA distinta → repunte canonical_products.odoo_product_id al activo.
   b) Si el internal_ref ya no existe activo → archive canonical_products entry o mantenerla histórica.

3. CAUSA RAÍZ EN ODOO: cuando alguien duplica un producto en Odoo (en lugar de editar el existente), crea un odoo_product_id nuevo. canonical_products no se actualiza automáticamente al ID nuevo.

4. Configurar trigger en bronze que cuando se inserte un producto en odoo_products con internal_ref existente en canonical, RE-PUNTE canonical al activo.

**Cómo se sorteaba en Supabase.** matcher_product corre cada 2h vía pg_cron silver_sp3_matcher_all_pending. Los 14 faltantes deberían entrar en próximo run. Los 484 que apuntan a inactivos requieren update manual.

### Renta abril 2026 fue 39% menor a marzo: investigar si está completa
`investigate-renta-abril-baja` · contabilidad · Medio · creado 2026-05-04 · responsable: Contadora + Mariano · impacto estimado: $535,000/mes

**Problema.** La cuenta 504.01.0008 RENTA en abril 2026 muestra $677k vs marzo $1.13M (gap de $379k, -33%). El CEO confirmó breakdown de renta total esperado:
- Lote 9 planta tintorería+acabado: $356,934 (50/50 entre acabado y tint)
- Lote 10 planta entretelas: $352,062
- Lote 9,10 oficinas Tejido: $284,269
- Lote 10 oficinas RH+Compras: $219,509
Total esperado: ~$1,212,775/mes

Abril en libros: $677k → faltan ~$535k. Posibles causas:
1. Factura(s) de renta de abril aún no capturadas en Odoo (timing de captura).
2. Acreedor pagó parte en marzo y se contabilizó allá.
3. Reclasificación entre subcuentas.

Esto distorsiona el overhead por departamento en abril (especialmente ACABADO+TINTORERÍA, ENTRETELAS y oficinas Tejido).

**Arreglo en Odoo.** 1. Buscar facturas de renta abril 2026 en Odoo:
   Accounting → Vendor Bills → filtro "Renta" + abril 2026.
2. Cruzar con Lote 9 / Lote 10 contratos.
3. Si falta alguna, capturarla con fecha correcta.
4. Validar que cada lote esté en cuenta correcta (504.01.0008 todo, o subcuentas separadas).
5. Configurar facturas recurrentes en Odoo (Accounting → Configuration → Subscriptions / Recurring) para que no se omita ningún mes.

**Cómo se sorteaba en Supabase.** silver: rent_lot_assignment tiene los 4 lotes con distribución porcentual a cost centers. RPC get_overhead_by_cost_center prorratea según lo que esté en libros. Si los libros faltan, el overhead reportado está bajo en abril (no es bug del cálculo, es data missing).

### BOMs con cantidades infladas (WC090, WJ055)
`bom-cantidades-infladas-wc090-wj055` · costos · Medio · creado 2026-06-05 · responsable: Costos / Producción

**Problema.** WC090Q11JNT170 y WJ055Q23JNT165 tienen recetas que consumen ~10x el peso fisico de la tela -> MP recursivo inflado, margenes falsos. Error de captura en Odoo (cantidad de salida o componentes por lote vs por metro).

**Arreglo en Odoo.** Revisar y corregir la BOM de WC090Q11JNT170 y WJ055Q23JNT165 en Odoo: la cantidad de salida o el consumo de componentes esta capturado por lote en vez de por metro.

**Cómo se sorteaba en Supabase.** El reporte de costo reconstruido marca estos productos; su MP no es confiable hasta corregir la BOM. El factor de fab/op por peso usa el gramaje del ref, no la receta, así que esos no se afectan.

### Gap de merma: BOMs de acabado cargan ~18% vs 10% del plan de manufactura
`merma-bom-acabado-vs-plan` · productos · Medio · creado 2026-06-12 · responsable: Guadalupe Ramos + jefe de manufactura

**Problema.** Las BOMs de acabado consumen +12% a +18% de crudo vs el peso teórico (gramaje × ancho del plan de capacidades), pero manufactura planea con 10% de merma. Los ~8 puntos de diferencia inflan el costo BOM de cada tela o esconden merma real mayor a la planeada. Tejido sí está 1:1 real desde abril (verificado: rendimiento 96-101% abr-jun; el 50% de ene-mar era el artefacto de los tokens RSI56).

**Arreglo en Odoo.** Auditar los coeficientes de consumo de las BOMs de acabado contra el consumo real de las órdenes (kg crudo consumido / metros acabados producidos) por producto. Ajustar las BOM a la merma real medida. Empezar por los SKUs de mayor volumen: WJ042Q22JNT160, WJ038Q22JNT160, WJ045Q22JNT160, WJ053Q22JNT160, XJ140Q21JNT165.

**Cómo se sorteaba en Supabase.** El costo reconstruido y el P&L limpio usan estas BOMs — si sobre-consumen, el costo primo BOM está inflado en la misma proporción.

### Explicar ajustes de inventario de saldo valuados a mano (dic-2024 $1.39M, dic-2025 $0.79M)
`ajustes-inventario-saldo-valuados` · contabilidad · Medio · creado 2026-06-12 · responsable: Contadora + Gustavo Delgado · impacto estimado: $2,174,000/mes

**Problema.** Dos ajustes de conteo de fin de año dieron entrada a saldo CON valor: dic-2024 +16,691 kg de SALDO DE TELA TEJIDO por $1,385,348 y dic-2025 +8,480 kg de SALDO TEJIDO D por $788,940. Ese valor no salió de ninguna orden — si los kg venían de desperdicio cuyo costo ya estaba en la tela buena (BOM +18%), el costo de ventas 2025-2026 carga ese material dos veces (compensado en 501.01.08, pero distorsiona márgenes por producto).

**Arreglo en Odoo.** Pedir a contabilidad el soporte de ambos ajustes: quién los valuó, con qué criterio, y si hubo baja equivalente en las telas de origen. Una vez adoptada la política de saldo a $0, los conteos futuros de saldo entran sin valor automáticamente.

**Cómo se sorteaba en Supabase.** Ninguno — es investigación contable histórica.

### FORMATOIMPRENTA con costo unitario irreal ($1,700/millar) infla inventario $21.5M
`error-costo-formato-imprenta` · Inventario / Costos · Medio · creado 2026-06-22 · responsable: Contabilidad · impacto estimado: $21,510,440/mes

**Problema.** El producto FORMATOIMPRENTA (formatos de imprenta) tiene 12,653 millares × $1,700 = $21.5M de valor de inventario, claramente un error de captura (costo o cantidad).

**Arreglo en Odoo.** Corregir el costo unitario y/o la cantidad en mano del producto FORMATOIMPRENTA. Revisar el movimiento de inventario que lo generó.

**Cómo se sorteaba en Supabase.** Marcado como anomalía en inventory_valuation_snapshot; excluido del inventario sin anomalías.

### La unidad de medida está en el nombre de la categoría → árbol inflado y duplicados
`categorias-uom-en-nombre-y-duplicados` · Categorías · Medio · creado 2026-06-22 · responsable: Operaciones / Sistemas

**Problema.** Las categorías incluyen el uom en el nombre (-kg, -mts, -litro, -pza) lo que genera duplicados y typos: Puntos-mts vs Puntos-mts2 vs Puntos-kgs, Carda-mts vs Carda-mts2, "Tramado -mts" (espacio), Quimico vs Químico-Maquila, "Tac-Producto en proceso" vs "Producto terminado". Rompe la clasificación automática del costeo.

**Arreglo en Odoo.** Quitar el uom del nombre (el uom ya vive en el producto). Consolidar los duplicados/typos en una categoría canónica por proceso. Estandarizar mayúsculas/acentos.

**Cómo se sorteaba en Supabase.** El clasificador del modelo usa ILIKE tolerante, pero los duplicados siguen ensuciando el ruteo.

### Categorías con cuenta de valuación que no es inventario
`categorias-cuentas-valuacion-incorrectas` · Categorías / Contabilidad · Medio · creado 2026-06-22 · responsable: Contabilidad

**Problema.** Algunas categorías tienen property_stock_valuation_account_id apuntando a 107.05.02 Mercancías en Producción o 107.01.01 Deudores Diversos Empleados, que no son cuentas de inventario. Distorsiona el balance de inventario.

**Arreglo en Odoo.** Corregir la cuenta de valuación de inventario por capa: Materia Prima→115.02.01, Producto en Proceso→115.03.01, Producto Terminado→115.04.01, Refacciones→115.04.03.

**Cómo se sorteaba en Supabase.** La valuación reconstruida no usa estas cuentas (calcula por costo), pero el inventario CONTABLE de Odoo sí queda mal.

### TL/EMB y TL/REQP pegan a 501.01.02 (cuenta de salida mal apuntada)
`categorias-salida-501-emb-reqp` · contabilidad · Medio · creado 2026-07-03 · impacto estimado: $450,000/mes

**Problema.** Asientos de embarque (TL/EMB $442,537 el 30-jun) y requisiciones (TL/REQP $7,977 el 1-jul) cargan 501.01.02 COSTO POR AJUSTES A CANTIDAD. Son movimientos de almacén: deben mover cuentas 115 (transferencias) o el gasto específico correcto (consumo de requisición → mantenimiento/gasto del área).

**Arreglo en Odoo.** Revisar los tipos de operación TL/EMB y TL/REQP y las categorías de los productos involucrados: la "cuenta de salida de existencias" apunta a 501.01.02. Corregir a la cuenta 115 correspondiente (transferencia) o al gasto del área (requisiciones).

**Cómo se sorteaba en Supabase.** Guardias inventory.zombie_501_01_02 e inventory.costo_primo_leak ya los levantan como issue.

### WR135Q48JNT165: peso de BOM inflado
`wr135q48-peso-bom-inflado` · Costeo / BOM · Medio · creado 2026-07-29 · responsable: Ingeniería / Costos

**Problema.** El rib WR135Q48 tiene peso de BOM 0.327 kg/m vs ~0.223 físico (135g×1.65) — ~46% más. Sobreestima la fabricación absorbida (se reparte factor por kg) y el costo total.

**Arreglo en Odoo.** Revisar las cantidades de la receta (consumo de hilo por metro) y/o el peso maestro del producto. Debe acercarse a 0.223-0.24 kg/m.

**Cómo se sorteaba en Supabase.** Marcado como [REVISAR] en el libro de cotizaciones; costo total probablemente sobreestimado.

### 45 SKUs con stock físico pero sin avg_cost_mxn
`fix-45-skus-without-avg-cost` · productos · Bajo · creado 2026-05-04 · responsable: Gustavo Delgado

**Problema.** Hay 45 SKUs con stock_qty > 0 pero avg_cost_mxn = NULL o 0. No contribuyen al valor físico calculado, lo que sub-estima el inventario físico vs contable y oculta el drift real. Causas típicas: (a) producto creado pero nunca comprado, (b) producto fabricado sin standard_price asignado, (c) producto importado sin avg_cost actualizado.

**Arreglo en Odoo.** 1. Listar los 45 SKUs: SELECT internal_ref, name, stock_qty FROM canonical_products WHERE stock_qty > 0 AND (avg_cost_mxn IS NULL OR avg_cost_mxn = 0).\n2. Para cada uno, decidir: si fue comprado pero no se reflejó, hacer compra de prueba; si es manufacturado, asignar standard_price; si está obsoleto, archivar y hacer write-off del stock.

**Cómo se sorteaba en Supabase.** Ninguno — son outliers individuales, no systemic.

### Separar overhead corporativo vs fábrica en 504.01
`separate-corp-vs-factory-overhead` · contabilidad · Bajo · creado 2026-05-04 · responsable: Contadora

**Problema.** La cuenta 504.01.0008 "RENTA DEL LOCAL" es renta de TODO (incluye corporativo y fábrica). El P&L limpio asume que 504.01 es 100% fábrica, lo que sobrestima el "costo de ingresos" e infla artificialmente el margen administrativo en 6xx. Magnitud no estimada con precisión.

**Arreglo en Odoo.** 1. Decidir el ratio fábrica/corporativo de la renta (típicamente por m2, 80/20 o similar).\n2. Crear sub-cuenta 504.01.0008 (fábrica) + 613.x (corporativo) si no existe.\n3. Configurar asiento mensual de renta como split entre las dos cuentas.\n4. Aplicar el mismo razonamiento a otras 504.01 que tengan componente corporativo (energía, internet, etc.).

**Cómo se sorteaba en Supabase.** En silver podemos meter un override que prorratea 504.01.0008 80/20 entre fábrica/corpo. Pero introduce un ratio arbitrario.

### Validar fechas CFDI vs Odoo (71 issues con date_drift)
`validate-cfdi-date-drift` · ventas · Bajo · creado 2026-05-04 · responsable: Mariano

**Problema.** 71 facturas tienen fecha distinta entre SAT y Odoo. Suele pasar cuando se timbra al día siguiente de crear la factura en Odoo y la fecha cambia. No tiene impacto monetario directo pero distorsiona reportes mensuales y aging.

**Arreglo en Odoo.** 1. Configurar Odoo para que SIEMPRE timbre el mismo día que se crea la factura (no permitir timbrado diferido).
   - Accounting → Configuration → Journals → Customer Invoices
   - Activar "Auto-timbre on validate"

2. Para las 71 con drift, decidir si ajustar Odoo a la fecha SAT (recomendado por consistencia fiscal) o aceptarlas.

3. CONTROL: si > 5 nuevos drifts por mes, revisar el flujo de timbrado.

**Cómo se sorteaba en Supabase.** reconciliation_issues con invariant_key=invoice.date_drift.

## Resueltos

### WR170Q46JNT162 (Bowen rib 170): sin BOM real
`bowen-wr170-sin-bom` · Costeo / BOM · Alto · creado 2026-07-29 · responsable: Ingeniería / Costos

**Problema.** El rib 170 (100DGB) para Bowen tiene BOM auto-referencial (se apunta a sí mismo, no explota a hilo). MP no se puede calcular; get_bom_weight_per_unit da 0. No hay forma de costearlo desde el modelo.

**Arreglo en Odoo.** Dar de alta la receta real del WR170: hilo 100/36, cantidades por metro, y las operaciones (tejido/teñido/acabado). Verificar galga y ancho.

**Cómo se sorteaba en Supabase.** Costo estimado a mano mientras tanto; no confiable.

### BOMs activas vacías: priorizar las que tienen líneas
`fix-bom-empty-priority` · productos · Medio · creado 2026-05-04 · responsable: Guadalupe Ramos + Mariano

**Problema.** Encontrado: cuando un producto tiene 2+ BOMs activas y la "primera" por criterio de orden está vacía (0 líneas), el cálculo BOM-recursivo devolvía $0 ignorando otras BOMs activas válidas. 7 productos afectados, solo 1 vendido en 2026 (IWR130Q46JAZ155 — $13.5k). RESUELTO 2026-05-04 PM en silver con cambio en función get_bom_raw_material_cost_per_unit (ahora prioriza num_lines > 0).

Subyacente: hay 31 BOMs activas vacías en Odoo en 26 productos distintos. Son placeholders de "DESARROLLOS / GENÉRICO" que deberían archivarse o tener líneas reales.

**Arreglo en Odoo.** 1. En Odoo, ir a Manufacturing → Bills of Materials.
2. Filtro: Active=true AND Components count=0.
3. Para cada BOM vacía:
   a) Si es placeholder de desarrollo → archivar.
   b) Si debería tener líneas → llenarlas con la receta real.
4. Capacitar a Guadalupe Ramos para no dejar BOMs vacías activas (usar Draft state).
5. Considerar regla en qb19: bloquear active=true en BOM con 0 líneas.

**Cómo se sorteaba en Supabase.** Ya RESUELTO en silver — la función ahora ignora BOMs vacías y elige una con líneas si existe. Pero limpiar Odoo evita confusión humana y el bug similar en otros lugares del sistema.

[2026-05-05] RESUELTA: get_bom_raw_material_cost_per_unit ahora prioriza BOMs activas con num_lines > 0 sobre las vacías (commit 0f038f7).

## Descartados (wont_fix)

### Reinterpretar el "P&L limpio": el BOM-recursivo NO incluye MOD+OH absorbido
`reinterpret-pnl-limpio-mod-oh` · contabilidad · Crítico · creado 2026-05-04 · responsable: Contadora + CEO · impacto estimado: $2,350,000/mes

**Problema.** [OBSOLETO 2026-05-04] La premisa que motivó esta acción (Standard valuation con CAPA inflada en 501.01.01) era incorrecta. Quimibond usa AVCO. Workcenters configurados sólo en Tejido Circular (go-live mayo 2026); el resto de los procesos NO absorbe MOD+OH al PT al producirse (variable costing implícito).

Pre-1-abril-2026 las BOMs incluían MOD+gastos vía productos token RSI56 (archivados). El COGS posteado a 501.01.01 NO está "inflado por CAPA" — es el AVCO real al despacho, contaminado por PT producido pre-abril.

Esta acción se mantiene en wont_fix por trazabilidad histórica. El reemplazo bajo la premisa correcta es 'pnl-limpio-rewrite-avco-regimen'.

**Arreglo en Odoo.** Esta NO es acción Odoo per se — es decisión de la contadora sobre cómo presentar el P&L. Tres opciones:

OPCIÓN A: Eliminar el "P&L limpio" — usar solo el contable (501.01.01 post-CAPA mensual).

OPCIÓN B (RECOMENDADA): Renombrar y separar.
- "COGS contable" (501.01.01) sigue siendo el costo total absorbido (MP + MOD + OH).
- "Margen contributivo material" (ventas − BOM-recursivo) se reporta APARTE como métrica de eficiencia material, NO como utilidad.
- Quitar el "swap" entre 501.01.01 y BOM-recursivo en la tabla limpia.

OPCIÓN C: Construir BOM completo con MOD+OH asignado por hora-máquina y por unidad. Requiere data adicional (rates) que no tenemos hoy.

Validar con contadora cuál preferir antes de cambiar UI.

**Cómo se sorteaba en Supabase.** Hoy el reporte mensual y /contabilidad muestran un "P&L limpio" que sub-reporta utilidad. Marcar todas las páginas con banner "interpretación bajo revisión" hasta que se decida.

### 501.01.01 debe ser solo costo de MP, no auto-COGS standard
`reclassify-501-01-01-as-mp` · contabilidad · Alto · creado 2026-05-04 · responsable: Contadora · impacto estimado: $1,820,000/mes

**Problema.** [OBSOLETO 2026-05-04] La premisa que motivó esta acción (Standard valuation con CAPA inflada en 501.01.01) era incorrecta. Quimibond usa AVCO. Workcenters configurados sólo en Tejido Circular (go-live mayo 2026); el resto de los procesos NO absorbe MOD+OH al PT al producirse (variable costing implícito).

Pre-1-abril-2026 las BOMs incluían MOD+gastos vía productos token RSI56 (archivados). El COGS posteado a 501.01.01 NO está "inflado por CAPA" — es el AVCO real al despacho, contaminado por PT producido pre-abril.

Esta acción se mantiene en wont_fix por trazabilidad histórica. El reemplazo bajo la premisa correcta es 'pnl-limpio-rewrite-avco-regimen'.

**Arreglo en Odoo.** 1. Crear cuenta nueva 501.01.99 "Auto-COGS Odoo (standard cost)" en el catálogo.\n2. En cada categoría de producto, cambiar el campo "Income Account: Cost of Goods Sold" para que apunte a 501.01.99 en vez de 501.01.01.\n3. Dejar 501.01.01 reservada exclusivamente para asientos manuales de costo MP real (cuando se hace consumo de inventario via MO).\n4. Validar con un par de facturas de cliente nuevas que el COGS automático cae en 501.01.99.

**Cómo se sorteaba en Supabase.** En el P&L limpio, 501.01.01 ya está siendo SWAP-eada con costo primo BOM (lógica correcta para los datos actuales). Una vez resuelto en Odoo, ajustar la lógica para que 501.01.99 sea la que se SWAP-e, y 501.01.01 quede como costo MP real reportable directamente.

### Workflow mensual: ajuste CAPA en Odoo para alinear 501.01.01 con costo MP real
`monthly-capa-workflow` · contabilidad · Alto · creado 2026-05-04 · responsable: Contadora (mensual) · impacto estimado: $1,820,000/mes

**Problema.** [OBSOLETO 2026-05-04] La premisa que motivó esta acción (Standard valuation con CAPA inflada en 501.01.01) era incorrecta. Quimibond usa AVCO. Workcenters configurados sólo en Tejido Circular (go-live mayo 2026); el resto de los procesos NO absorbe MOD+OH al PT al producirse (variable costing implícito).

Pre-1-abril-2026 las BOMs incluían MOD+gastos vía productos token RSI56 (archivados). El COGS posteado a 501.01.01 NO está "inflado por CAPA" — es el AVCO real al despacho, contaminado por PT producido pre-abril.

Esta acción se mantiene en wont_fix por trazabilidad histórica. El reemplazo bajo la premisa correcta es 'pnl-limpio-rewrite-avco-regimen'.

**Arreglo en Odoo.** PROCESO MENSUAL (último día hábil del mes, después del cierre de facturación):

1. Abrir /contabilidad/cuenta/501.01.01?from=YYYY-MM&to=YYYY-MM en el sistema. La sección "CAPA del mes a aplicar" muestra el monto exacto a remover.

2. En Odoo, ir a Accounting → Journal Entries → New.

3. Crear el asiento con:
   - Journal: "CAPA DE VALORACIÓN"
   - Date: último día del mes (ej. 2026-04-30)
   - Reference: "Ajuste CAPA overhead [Mes] [Año]"
   - Lines (donde X = residual del mes):
     * Línea 1: Cr 501.01.01 Cost of sales       $X
     * Línea 2: Dr 504.01.0099 Overhead absorbido $X
       (si la cuenta no existe, crearla primero — ver paso 0)

4. Validar (Post) el asiento.

5. Verificar en el sistema que /contabilidad/cuenta/501.01.01 ahora muestra residual ≈ $0.

PASO 0 (solo primera vez): crear cuenta 504.01.0099 "Overhead absorbido CAPA"
- Tipo: expense_direct_cost
- Padre: 504.01 OVERHEAD FÁBRICA

ALTERNATIVA — si tu contadora prefiere:
   * Línea 2 alterna: Dr 115.04.01 Productos terminados $X
     (regresa overhead al inventario)
Esto es el patrón que ya usas hoy ($62k abril). Es válido pero crea
inventario "fantasma" de overhead — preferible 504.01.0099 si quieres
visibilidad clara del overhead absorbido en P&L.

¿CUÁL ES EL MONTO EXACTO DE X?
El sistema lo calcula así:
  X = saldo neto 501.01.01 del mes
      − costo MP real recursivo BOM del mes
      − ajustes CAPA ya posteados en el mes

Verlo en /contabilidad/cuenta/501.01.01 (sección "CAPA del mes a aplicar").

**Cómo se sorteaba en Supabase.** El P&L LIMPIO ya muestra el número correcto (swap 501.01.01 ↔ BOM). Pero el P&L CONTABLE oficial sigue inflado hasta que se hagan los CAPA mensuales. Para conciliar libros vs realidad sin esperar el fix Odoo de fondo (acción reclassify-501-01-01-as-mp), este workflow es el puente.

### Confirmar con contadora: método de valuación de inventario (Standard / AVCO / FIFO)
`investigate-real-cost-method` · contabilidad · Alto · creado 2026-05-04 · responsable: Contadora + Mariano

**Problema.** [OBSOLETO 2026-05-04] La premisa que motivó esta acción (Standard valuation con CAPA inflada en 501.01.01) era incorrecta. Quimibond usa AVCO. Workcenters configurados sólo en Tejido Circular (go-live mayo 2026); el resto de los procesos NO absorbe MOD+OH al PT al producirse (variable costing implícito).

Pre-1-abril-2026 las BOMs incluían MOD+gastos vía productos token RSI56 (archivados). El COGS posteado a 501.01.01 NO está "inflado por CAPA" — es el AVCO real al despacho, contaminado por PT producido pre-abril.

Esta acción se mantiene en wont_fix por trazabilidad histórica. El reemplazo bajo la premisa correcta es 'pnl-limpio-rewrite-avco-regimen'.

**Arreglo en Odoo.** PASOS PARA CONFIRMAR:

1. Pedir a contadora confirmación del método de valuación configurado:
   - Odoo: Inventory → Configuration → Settings → Inventory Valuation
   - Posibles valores: Standard Price, Average Cost (AVCO), First In First Out (FIFO)

2. Si es AVCO o FIFO:
   - El campo standard_price es solo informativo, no se usa para COGS.
   - Necesitamos otro campo / cálculo para BOM-recursivo:
     a) Pull el precio de la última compra vía odoo_purchase_orders + lines.
     b) O calcular el moving average dinámico.

3. Si es Standard Price:
   - Validar por qué hay gap entre standard_price y COGS posteado.
   - Posiblemente product_categ_id.property_cost_method es distinto a producto-level.

4. Documentar el método correcto y ajustar canonical_products.avg_cost_mxn para que refleje el "costo real al despachar" en lugar del standard_price snapshot.

**Cómo se sorteaba en Supabase.** BOM-recursivo actualmente usa avg_cost_mxn = standard_price. Si el método real es AVCO/FIFO, este valor está mal. Mientras se valida, todo cálculo de "costo MP real" vía BOM debe leerse con cautela.

### 501.01.02 debe ser solo scrap/encogimientos físicos, no ajustes auto de Odoo
`reclassify-501-01-02-as-scrap` · contabilidad · Medio · creado 2026-05-04 · responsable: Contadora + Gustavo Delgado · impacto estimado: $1,200,000/mes

**Problema.** [OBSOLETO 2026-05-04] La premisa que motivó esta acción (Standard valuation con CAPA inflada en 501.01.01) era incorrecta. Quimibond usa AVCO. Workcenters configurados sólo en Tejido Circular (go-live mayo 2026); el resto de los procesos NO absorbe MOD+OH al PT al producirse (variable costing implícito).

Pre-1-abril-2026 las BOMs incluían MOD+gastos vía productos token RSI56 (archivados). El COGS posteado a 501.01.01 NO está "inflado por CAPA" — es el AVCO real al despacho, contaminado por PT producido pre-abril.

Esta acción se mantiene en wont_fix por trazabilidad histórica. El reemplazo bajo la premisa correcta es 'pnl-limpio-rewrite-avco-regimen'.

**Arreglo en Odoo.** 1. Crear cuenta nueva 501.01.97 "Ajustes valuación auto Odoo" para los asientos automáticos del journal Valoración del inventario.\n2. En la configuración del Stock Valuation Journal de Odoo, cambiar la cuenta de gasto default a 501.01.97.\n3. Dejar 501.01.02 reservada para asientos manuales de scrap/encogimientos identificados (cuando se hace inventory adjustment con motivo "scrap").\n4. Capacitar al equipo de almacén para que use el motivo correcto al hacer ajustes.

**Cómo se sorteaba en Supabase.** En el P&L limpio, 501.01.02 vive en ambos contable Y limpio (asumimos costo legítimo). Cuando se separe, 501.01.97 debe excluirse del limpio O quedar como ajuste explícito.

### Configurar categorías de producto para tracking de variance MP
`configure-product-categories-for-variance` · productos · Medio · creado 2026-05-04 · responsable: Contadora + Guadalupe Ramos

**Problema.** Existe mv_mo_actual_material_cost (consumo real por MO) y mv_bom_standard_cost (BOM teórica), pero no se usa para reportar variance. La razón: las categorías de producto en Odoo no tienen separación clara entre "input variance account" e "output variance account", lo que dificulta auditar si la producción real consumió más/menos MP que la BOM.

**Arreglo en Odoo.** 1. Crear cuentas nuevas 501.01.95 "Variance favorable producción" y 501.01.96 "Variance desfavorable producción".\n2. En cada Product Category, configurar Cost Variance Account = 501.01.95/96 según signo.\n3. Activar reportes de variance en cada cierre mensual.

**Cómo se sorteaba en Supabase.** En silver, podemos ya reportar variance comparando los 2 MVs sin necesidad de cuentas Odoo separadas (sería sólo display). Pero el CEO no podría conciliar contra libros sin la cuenta dedicada.

[2026-05-05] OBSOLETO: bajo régimen AVCO no existe el concepto de "variance" tipo Standard. Las categorías de producto no se necesitan configurar para tracking de variance porque no hay variance — hay gap AVCO/BOM (cubierto por manufacturing-variance-tracking, ahora reframeada).
