# qb_tejido_ritmo — Ritmo de tejido desde el pesaje

Cuánto teje de verdad cada circular, cuánto corre, cuánto está parada y
cuánto pierde en cambios de artículo, medido rollo por rollo. La base es el
pesaje (`mrp.weighing.log` de `pesaje_rollos_tejido`): todos los rollos se
pesan al salir de la máquina, así que el tiempo entre un rollo y el
siguiente es el tiempo real de tejido. El cronómetro de las órdenes de
trabajo no sirve porque muchas se quedan abiertas.

Especificación: «Tiempos y ritmos de tejido desde el pesaje» (2026-10-08).

## Qué calcula

- **Intervalo por rollo** (`qb.tejido.intervalo`): horas brutas desde el
  rollo anterior de la misma máquina, menos descanso del calendario y paros
  generales = horas netas. Clase: primer rollo, captura atrasada (grupos de
  3+ rollos con menos de 10 min), cambio de artículo (el exceso sobre la
  referencia va a cambio), pesado junto, sin referencia, paro (netas > 3× la
  referencia; el exceso va a paro) o corrida. El supervisor captura el
  **motivo del paro**; se conserva al recalcular.
- **Paro general** (`qb.tejido.paro.general`): hueco de más de 5 h sin
  pesajes en toda la planta dentro del calendario. Se resta a todas las
  máquinas. Si no fue paro real, se desmarca y el siguiente recálculo lo
  ignora.
- **Ritmo** (`qb.tejido.ritmo`): por máquina, artículo y periodo (semana y
  mes): kg/h en corrida, técnica (P90 de ventanas de 6 rollos), típica,
  lenta (P10), **efectiva** (incluye paros: base del costo por kg), rango de
  confianza (remuestreo por día), estándar (receta), horas por clase, % en
  corrida, paros, ciclo por rollo y peso típico. Filas con artículo vacío =
  total de la máquina en el periodo, con horas programadas y **sin
  explicar**.
- **Centro de costeo** (`qb.centro`, driver «órdenes de trabajo»): capacidad
  medida (h/mes y kg/mes), throughput medido y kilos perdidos de los últimos
  3 meses. El botón **Aplicar** copia la capacidad medida a la normal; no se
  sobrescribe sola.
- **Costeo** (`qb.producto.horas`): fuente **pesaje** = 1 ÷ kg/h efectiva del
  artículo en el centro, ponderada por kilos de los últimos meses. Manda
  sobre las órdenes de trabajo y por debajo del dato manual.

## Descansos y calendario

Las circulares usan en Odoo «Jornada 24/7 3 Turnos» con arranque el sábado a
las 19:00, pero desde el 25-sep-2026 tejido para de viernes 18:00 a domingo
19:00. El descanso se captura aquí con **vigencia** (Manufactura → Costos →
Ritmo de tejido → Descansos), así las semanas viejas se miden con su
descanso de entonces; la regla que aplica a un fin de semana es la vigente
el día en que empieza su ventana (el cambio del viernes 25-sep aplica ese
mismo fin de semana). Los festivos salen de las ausencias globales de los
calendarios de las máquinas. Todo el cálculo es en hora de planta
(`America/Mexico_City`).

## Cuándo se calcula

Acción programada nocturna: últimos 14 días (hay rollos que se capturan
tarde). Botón «Recalcular ritmo de tejido» por rango de fechas para cuando se
corrija un descanso o un paro general. El motor (`models/motor.py`) es
Python puro y se prueba con datos sintéticos.

## Parámetros

En Manufactura → Costos → Configuración → Parámetros, claves `ritmo_*`: peso
máximo de rollo (150 kg), usuarios de captura atrasada, artículos y prefijos
excluidos, centros excluidos (MAQUILA), minutos mínimos (10), rollos de
captura atrasada (3), factor de paro (3), horas de paro general (5),
intervalos mínimos para referencia (8), rollos mínimos para un ritmo (5),
meses de capacidad (3) y si la referencia de paros y cambios cuenta como
corrida (1; en 0 reproduce la prueba de aceptación).

## Prueba de aceptación

Con `ritmo_referencia_cuenta_corrida = 0`, un solo descanso de viernes
18:00 a sábado 19:00 y el rango 18-may a 7-oct-2026, los totales deben
coincidir ±1 % con la prueba externa: 10,442 rollos, 474,937 kg, 11 paros
generales (343 h), corrida 40,502 h, paro 13,433 h, cambio 5,130 h, sin
medir 780 h; CIRCULAR 28 / WJ044 13.62 kg/h en corrida, CIRCULAR 22 / XJ130
24.75, CIRCULAR 17 / WJ047 11.28. Después se regresan los dos parámetros.
