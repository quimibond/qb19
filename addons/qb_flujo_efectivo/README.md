# qb_flujo_efectivo — Flujo de efectivo a 13 semanas

Reporte **Contabilidad → Reportes → Flujo de efectivo a 13 semanas** sobre el
motor de reportes de Odoo 19 Enterprise (`account_reports`): una columna por
semana (la semana en curso, hasta el domingo, y 12 semanas de lunes a domingo)
más el total, con PDF y Excel nativos. Método directo, todo en pesos al tipo de
cambio del día del reporte.

## Estructura del reporte

| Renglón | De dónde sale |
|---|---|
| **Saldo inicial de caja** | Cuentas de la pestaña «Cuentas de caja»: último estado de cuenta (`balance_end_real`) o saldo contable de la cuenta del diario. Se despliega por cuenta, con su moneda. |
| **Entradas · Comprometido** | Facturas de cliente abiertas en su fecha esperada (ver «Fechas esperadas»). |
| **Entradas · Estimado** | Pedidos de venta confirmados por facturar (entrega + días que tarda el cliente), ventas sin pedido todavía, compromisos de entrada y ajustes manuales. |
| **Salidas · Comprometido** | Facturas de proveedor abiertas en su fecha esperada. |
| **Salidas · Estimado** | Compromisos recurrentes (nómina, aguinaldo, IMSS, SAT, rentas…), órdenes de compra confirmadas por facturar, compras de las ventas sin pedido y ajustes manuales. |
| **Flujo neto, saldo final** | Saldo final de una semana = saldo inicial de la siguiente. |
| **Sobrante (faltante) contra el mínimo** | Solo si hay saldo mínimo; las semanas debajo se avisan. |
| **No suma** | Cartera incobrable, proveedores por revisar y lo que vence después de la semana 13. |

Cada renglón se despliega por cliente o proveedor y «Ver documentos» abre las
partidas, pedidos u órdenes que lo forman.

## Fechas esperadas de cobro y pago (1.1.0)

1. **«Fecha esperada en el flujo»** en la factura (junto al vencimiento): si se
   captura, manda. En una factura de cliente también la saca de incobrable.
2. Si no, **vencimiento + atraso histórico del contacto**: promedio, ponderado
   por importe, de los días entre vencimiento y pago de lo conciliado en los
   últimos 12 meses (`account.partial.reconcile`). Encendido para clientes y
   apagado para proveedores por omisión. Sin historial: el promedio general.
3. Lo **vencido que ya pasó ese atraso** se reparte en partes iguales en las
   primeras semanas: 4 en clientes y 1 (todo en la semana en curso) en
   proveedores, por omisión.

## Ventas y compras sin documento (1.1.0)

- **Ventas sin pedido todavía**: por cliente y mes, lo que falta para llegar al
  promedio facturado de los últimos 3 meses (o al presupuesto de ventas del
  año: el aprobado, o el más reciente vigente si no hay aprobado), menos lo ya
  facturado en el mes y los pedidos por facturar. Se factura parejo en los días
  que quedan del mes, con el IVA histórico del cliente, y se cobra con los días
  que ese cliente tarda en pagar (o su plazo, sin historial).
- **Compras de esas ventas**: la proporción histórica de facturas de proveedor
  sobre facturas de cliente (6 meses, sin nómina, IMSS, SAT, compromisos,
  proveedores por revisar ni contactos que no son flujo), o la que se capture,
  menos lo que ya está en órdenes de compra del mes; se paga con los días
  promedio de pago a proveedores.

## Foto semanal: proyectado contra real (1.1.0)

Un cron diario guarda la proyección de las 13 semanas si la semana no tiene
foto (o «Guardar foto ahora»), y llena lo real de cada semana que cerró: saldo
real con la misma regla de caja, **neto real = saldo al cierre − saldo al cierre
de la semana anterior** (la cifra confiable) y entradas y salidas de los
estados de cuenta sin los contactos que no son flujo (orientativo).
**Contabilidad → Reportes → Flujo: proyectado contra real.**

## Reglas (supuestos, en Contabilidad → Configuración → Flujo a 13 semanas)

- **Cartera cobrable**: facturas de cliente desde el 1-ene-2026 y vencidas hasta
  90 días. Lo demás es incobrable (no suma). Ambos valores son editables.
- **Proveedores por revisar**: sus facturas no suman hasta aclararse (Grupo
  Industrial Miro, INTRVC).
- **Contactos que no son flujo**: la empresa misma y Mifel (compra-venta de
  dólares, no es deuda). Sus facturas no suman y no cuentan como reales.
- **Compromisos recurrentes**: importe, periodicidad (semanal, quincenal,
  mensual, bimestral, anual, única) y día. «Estimar con lo pagado» promedia, por
  periodo que tuvo pago, los pagos registrados, los movimientos de los estados
  de cuenta o las facturas del contacto, filtrados por texto del memo. El
  contacto incluye a los que tienen su mismo nombre o RFC (el IMSS y el SAT
  están repetidos). Con «Sus facturas no suman» (nómina, IMSS, SAT) las
  facturas del contacto no entran a los pagos; sin marcar (rentas, créditos)
  sí entran y al compromiso de cada mes se le resta lo ya facturado que vence
  ese mes.
- **Pedidos**: solo confirmados en los últimos 90 días (los viejos se suponen
  abandonados).
- **Ajustes manuales**: entradas o salidas de una sola vez (un crédito, una
  venta de dólares, un abono acordado).
- «Cargar valores de Quimibond» agrega la caja (BBVA 8792, 5048, 5098, 5386,
  0546 por estado de cuenta; Mifel 5109 y 7755 por saldo contable; sin
  inversiones, sin 5209), los compromisos de nómina semanal y quincenal,
  aguinaldo (18 de diciembre, estimado con el traspaso de «AGUINALDO» del año
  pasado), IMSS, RCV e Infonavit, SAT y las rentas de Leasing Lepezo (con sus
  facturas), los proveedores por revisar y los contactos que no son flujo.
  El préstamo BBVA y Crediflex quedan **apagados y en cero**: no hay pagos
  periódicos en los estados de cuenta para estimarlos; se capturan importe y
  día y se encienden. No borra nada.

## Pruebas

- Puras (CI): `tests_puros/flujo_efectivo/` (semanas, fechas de compromisos, estimación).
- Odoo.sh: `--test-tags /qb_flujo_efectivo`.
