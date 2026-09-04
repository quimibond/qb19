# quimibond_cash_flow — Estado de flujo de efectivo NIF B-2

Addon de Odoo 19 Enterprise que agrega el reporte **Flujo de efectivo NIF B-2**
en el menú de Contabilidad, justo debajo del "Estado de flujo de efectivo" nativo
(mismo padre; se reubica en cada instalación/actualización), sobre el motor de reportes de `account_reports`
(filtros de fecha, comparación de periodos, PDF/XLSX, plegado y drill-down).

## Qué resuelve

El reporte nativo (`account.report` "Estado de flujo de efectivo") solo ve como
efectivo las cuentas ligadas a diarios, ignora cobros/pagos dentro de facturas,
descompone cada factura en sus líneas (IVA, inventario, costo de ventas aparecen
como "flujos") y no presenta el método indirecto. Este addon:

* Define efectivo y equivalentes por **reglas configurables** (tipo de cuenta,
  prefijo, cuenta, exclusiones), incluidas transitorias y recibos/pagos pendientes
  que no están ligadas a un diario.
* Presenta **método indirecto** (NIF B-2) y **método directo resumido** en el mismo
  reporte, con **columnas mensuales + acumulado** y comparación nativa contra
  periodos anteriores (mismo periodo del año anterior).
* Clasifica el método directo por la **contraparte** del movimiento de efectivo,
  con reglas por diario, tipo de asiento, contacto y cuenta. Si la contraparte es
  una cuenta por cobrar/pagar conciliada con una factura, la parte conciliada se
  clasifica por la **cuenta dominante de la factura** (su línea de producto más
  grande, sin impuestos): así una factura de maquinaria pagada vía proveedores es
  "Activo fijo comprado", la venta de una máquina cobrada vía clientes es "Activo
  fijo vendido" y las facturas de arrendamiento o intereses caen en su línea, sin
  descomponer la factura en todas sus líneas. Los cobros/pagos registrados dentro
  de una factura se clasifican por tipo de asiento (toda la póliza es un cobro o un
  pago).
* Sección **Conciliación**: efectivo inicial, incremento neto por cada método,
  efecto cambiario, efectivo final calculado, saldo contable de las cuentas de
  efectivo y diferencia (debe ser 0.00).
* Nada se descarta: lo que no cae en una regla va a **"Sin clasificar"**
  (indirecto) u **"Otros (revisar)"** (directo), con drill-down por cuenta y
  alerta si "Otros" supera el umbral (2 % de las salidas por default).
* Excluye las pólizas de cierre (`l10n_mx_closing_move`) del resultado y de las
  variaciones. Todo en moneda de la compañía (`balance`); nunca `amount_currency`.
* **Snapshots** (`cash.flow.snapshot`, JSON) por cron mensual para dashboards
  externos vía JSON-2.

## Por qué ambos métodos cuadran siempre

Toda póliza registrada suma cero, así que para cualquier periodo

```
variación de efectivo = −(suma de saldos de los apuntes que NO son efectivo)
```

* **Indirecto**: reparte *cada* apunte que no es efectivo (todas las pólizas
  registradas del periodo, sin cierres) en una línea. Las cuentas de resultados
  caen en "Resultado antes de impuestos". Las *partidas virtuales* (depreciación,
  resultado en venta de activo, intereses, arrendamiento financiero, diferencias
  cambiarias sobre efectivo, estimación de incobrables) se presentan con el signo
  con que se suman de vuelta y su efecto real se manda a una **línea espejo**
  (inversión, financiamiento, efecto cambiario, capital de trabajo), de modo que
  la suma total no cambia.
* **Directo**: reparte cada apunte que no es efectivo *de las pólizas que tocan
  efectivo*. Las pólizas que no tocan efectivo no mueven efectivo, así que el total
  es idéntico al del indirecto. Los traspasos entre cuentas de efectivo no tienen
  contraparte y no generan flujo.

Las diferencias cambiarias del diario "Diferencia de cambio" sobre bancos USD (y la
diferencia en ventas de USD a casa de cambio vía 102.09.00) son contrapartes de
pólizas que tocan efectivo: van a **Efecto por cambios en el valor del efectivo** en
ambos métodos. Las diferencias cambiarias sobre clientes/proveedores no tocan
efectivo: quedan en operación (resultado) compensadas por la variación de la cuenta.

Todo se calcula con **una consulta SQL agrupada** (mes, cuenta, diario, tipo de
asiento, contacto de regla, toca-efectivo, lado) y se clasifica en Python sobre esos
grupos. Objetivo: ejercicio completo en menos de 10 s sobre ~2.3 M de apuntes.

## Estructura

```
quimibond_cash_flow/
  __manifest__.py                     depends: account, account_reports, l10n_mx
  models/
    cash_flow_lines.py                catálogo de líneas/secciones (claves estables)
    cash_flow_config.py               cash.flow.config (una por compañía), cash.flow.rule, defaults Quimibond
    cash_flow_engine.py               cash.flow.engine: SQL + clasificación (sin dependencia de account_reports)
    cash_flow_snapshot.py             cash.flow.snapshot + cron
    account_cash_flow_nif_report.py   handler account.report.custom.handler (columnas, líneas, drill-down)
  data/account_report_data.xml        account.report + acción cliente
  data/ir_cron_data.xml               cron mensual (inactivo por default)
  security/ir.model.access.csv
  views/                              configuración (árbol editable), snapshots, menús
  tests/test_cash_flow.py             casos: recibos pendientes, proveedor con IVA, traspaso, USD/Mifel,
                                      activo fijo, préstamo con intereses, nómina, cierre, sin clasificar
  scripts/validate_vs_odoo.py         validación vía JSON-2 contra saldos contables
```

## Configuración

**Contabilidad › Configuración › Flujo de efectivo NIF B-2** → crear el registro de
la compañía → **Cargar defaults Quimibond**. Esto siembra:

* *Efectivo*: `asset_cash` + `liability_credit_card` + 101.01, 102.01.00x,
  102.02.02/03, 103.01.03-05, 102.01.0011, 204.01.02, recibos/pagos pendientes
  102.01.xx, 102.09.00, 102.01.36; excluye 109.23.02 y 107.03.001.
* *Indirecto*: partidas virtuales (504.08/09/10/11/22/23, 613 → depreciación;
  704.23.0003/701.01.0004 → resultado en venta de activo; 701.01.0003 siniestro;
  701.01.0005 incobrables; 701.01.0001/0002 y 702.01.0001/0002 solo en pólizas
  con efectivo → efecto cambiario; 701.04 → intereses; 701.11.0001 →
  arrendamiento), resultado 4xx–7xx, capital de trabajo, inversión por
  movimientos (cargos = adquisiciones, abonos = bajas, 171/183 por lado),
  financiamiento (252.01 por lado, 205.02.02/03, 107.03.001, 205.04.01, 30x) y
  redes de seguridad por prefijo/tipo.
* *Directo*: diarios Nominas / IMSS / Impuestos / IMPUESTOS FEDERALES /
  Diferencia de cambio, tipo de asiento (facturas), contactos SAT / IMSS /
  Gobiernos / arrendadores (ICOMATEX, Fong's, Interlock, Bianco) y cuentas.

Los diarios y contactos se buscan por nombre en la compañía; los que no existen se
omiten y se avisan en la notificación. Las reglas son editables en árbol; la primera
que coincide (por secuencia) gana.

## Instalación en Odoo.sh (staging primero)

1. Rama de desarrollo → PR a `main` → build de staging (la BD es copia de
   producción).
2. En el shell del build de staging:
   ```bash
   odoo-update quimibond_cash_flow      # o instalar desde Apps la primera vez
   odoosh-restart http && odoosh-restart cron
   ```
   Primera instalación: **Apps › Actualizar lista › quimibond_cash_flow › Instalar**
   (requiere `account_reports` y `l10n_mx`, ya instalados).
3. Contabilidad › Configuración › Flujo de efectivo NIF B-2 › Nuevo (compañía
   Quimibond) › **Cargar defaults Quimibond**.
4. Contabilidad › Reportes › **Flujo de efectivo NIF B-2**, rango 01/01/2026 –
   31/08/2026. Verificar en la sección Conciliación que la **Diferencia** es 0.00 y
   revisar "Sin clasificar" / "Otros (revisar)" renglón por renglón (desplegar).
5. Correr los tests en el shell de staging:
   ```bash
   odoo-bin -d <db> --test-enable --test-tags /quimibond_cash_flow --stop-after-init
   ```
6. Validación externa (JSON-2), con una API key de un usuario contador:
   ```bash
   ODOO_URL=https://<staging>.odoo.com ODOO_API_KEY=... python3 addons/quimibond_cash_flow/scripts/validate_vs_odoo.py --by-account
   ```
7. Merge a `quimibond` (producción) siguiendo `docs/RUNBOOK_DESPLIEGUE.md`.
8. Opcional: activar el cron "Flujo de efectivo NIF B-2: snapshot mensual".

## Proyección de flujo de efectivo (13 semanas)

Reporte **Proyección de flujo de efectivo** (menú de Contabilidad, debajo del
NIF B-2): una columna por semana a partir de la fecha del filtro, más el total.

| Renglón | Fuente |
|---|---|
| Saldo inicial | Saldo contable de las cuentas de efectivo (misma definición) |
| Cobros / pagos vencidos | Cuentas por cobrar y por pagar ya vencidas, repartidas en las primeras N semanas (`forecast_overdue_weeks`); las vencidas hace más de `forecast_stale_days` (180) se excluyen y se muestran en un renglón informativo |
| Cobros por vencimiento | Cuentas por cobrar abiertas por vencimiento + atraso promedio real del cliente (cobros conciliados de 12 meses, ponderado por importe, tope `forecast_max_delay`) |
| Pedidos de venta sin facturar | Opcional (`forecast_include_orders`, apagado por default): `sale.order` confirmados no más viejos que `forecast_stale_days`, importe pendiente de facturar, a fecha de entrega + `forecast_order_days` + atraso del cliente |
| Pagos vencidos / por vencimiento | Cuentas por pagar abiertas |
| Órdenes de compra sin factura | `purchase.order` confirmadas, importe pendiente, a `date_planned` + `forecast_order_days` |
| Ritmo histórico | Opcional (`forecast_include_runrate`, encendido): a partir de la semana en que se agota el DSO (días factura→cobro, ponderado, 12 meses) se estima cada semana el promedio semanal de cobros a clientes de los últimos `forecast_history_months` menos lo ya conocido; igual para pagos a proveedores con el DPO. Renglones separados para distinguir conocido de estimado |
| Compromisos | `cash.flow.forecast.item`: nómina, impuestos, préstamos, arrendamientos, intereses, activo fijo, partes relacionadas, otros; recurrencia única / semanal / 14 días / mensual |
| Saldo final | Se resalta cualquier semana por debajo de `forecast_min_cash` |

Cada renglón se despliega por contacto y "Ver origen" abre los apuntes,
pedidos o compromisos que lo alimentan. El botón **Sembrar compromisos del
historial** detecta la periodicidad real de los pagos en los últimos meses
(`forecast_history_months`, 3 por default) del método directo, día por día y
por categoría y contacto: series **semanales** (día de la semana e importe
mediano), series **mensuales por grupo de días cercanos** (a 4 días o menos;
28–31 = fin de mes) presentes en la mayoría de los meses, con el total mensual
mediano del grupo (dos rentas fijas pagadas en días distintos quedan en un
renglón), y un resto **irregular** (al menos dos ocurrencias) como promedio
mensual marcado para revisar. Un pago aislado no se siembra, y las series por
debajo de `forecast_min_item_amount` tampoco. La reclasificación por factura solo
refina clientes y proveedores genéricos: una cuenta por pagar con clasificación
propia (nómina 210, impuestos, préstamos, arrendamiento, partes relacionadas) o
una regla por diario o contacto se respeta (la provisión de nómina facturada por
una prestadora sigue siendo nómina). Cada compromiso lleva su siguiente fecha
y una nota con las fechas e importes observados; los capturados a mano se
conservan.

API: `POST /json/2/cash.flow.config/compute_forecast` con
`{"ids": [<config_id>], "date_from": "2026-09-07"}`.

## API JSON-2

```
POST /json/2/cash.flow.config/compute_summary
Authorization: Bearer <api_key>
{"ids": [<config_id>], "date_from": "2026-01-01", "date_to": "2026-08-31"}

POST /json/2/cash.flow.config/get_cash_account_ids      {"ids": [<config_id>]}
POST /json/2/cash.flow.snapshot/search_read             {"domain": [["company_id","=",1]], "fields": ["date_from","date_to","data"]}
```

## Notas

* Solo se consideran apuntes **registrados**; el filtro "Borradores" del reporte
  está deshabilitado.
* Las columnas mensuales aparecen cuando el rango de fechas está alineado a meses
  completos; si no, hay una sola columna con el rango. Cada periodo de comparación
  se muestra en una columna (total del periodo).
* Multi-compañía: cada compañía se calcula con su propia configuración y se suman
  los importes; las definiciones nunca se mezclan. Sin configuración, el reporte
  lo avisa en la primera línea.
* Método directo: los pagos anticipados sin factura conciliada se quedan en la
  línea de la cuenta por pagar/cobrar (proveedores o clientes) hasta que se
  concilian; la reclasificación por factura usa `account.partial.reconcile`, así
  que un pago conciliado después de generar un snapshot cambia de línea al
  recalcular.
