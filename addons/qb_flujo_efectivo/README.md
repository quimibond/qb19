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
| **Entradas · Comprometido** | Facturas de cliente abiertas por fecha de vencimiento. Lo vencido (cobrable) cae en la semana 1. |
| **Entradas · Estimado** | Pedidos de venta confirmados por facturar (entrega + plazo de pago), compromisos de entrada y ajustes manuales. |
| **Salidas · Comprometido** | Facturas de proveedor abiertas por fecha de vencimiento. Lo vencido cae en la semana 1. |
| **Salidas · Estimado** | Compromisos recurrentes (nómina, IMSS, SAT…), órdenes de compra confirmadas por facturar y ajustes manuales. |
| **Flujo neto, saldo final** | Saldo final de una semana = saldo inicial de la siguiente. |
| **Sobrante (faltante) contra el mínimo** | Solo si hay saldo mínimo; las semanas debajo se avisan. |
| **No suma** | Cartera incobrable, proveedores por revisar y lo que vence después de la semana 13. |

Cada renglón se despliega por cliente o proveedor y «Ver documentos» abre las
partidas, pedidos u órdenes que lo forman.

## Reglas (supuestos, en Contabilidad → Configuración → Flujo a 13 semanas)

- **Cartera cobrable**: facturas de cliente desde el 1-ene-2026 y vencidas hasta
  90 días. Lo demás es incobrable (no suma). Ambos valores son editables.
- **Proveedores por revisar**: sus facturas no suman hasta aclararse (Grupo
  Industrial Miro, INTRVC).
- **Compromisos recurrentes**: importe, periodicidad (semanal, quincenal,
  mensual, bimestral, única) y día. Con contacto, «Estimar con lo pagado»
  promedia los pagos (`account.payment`) de los últimos periodos que tuvieron
  pago, filtrados por texto del memo. Las facturas de proveedor de ese contacto
  no se suman otra vez.
- **Pedidos**: solo confirmados en los últimos 90 días (los viejos se suponen
  abandonados).
- **Ajustes manuales**: entradas o salidas de una sola vez (un crédito, una
  venta de dólares, un abono acordado).
- «Cargar valores de Quimibond» agrega la caja (BBVA 8792, 5048, 5098, 5386,
  0546 por estado de cuenta; Mifel 5109 y 7755 por saldo contable; sin
  inversiones, sin 5209), los compromisos de nómina, IMSS y SAT y los
  proveedores por revisar del reporte diario de pagos y cobros. No borra nada.

## Pruebas

- Puras (CI): `tests_puros/flujo_efectivo/` (semanas, fechas de compromisos, estimación).
- Odoo.sh: `--test-tags /qb_flujo_efectivo`.
