# Asientos que el costeo no debe repartir

Bitácora de las partidas que se descubrieron entrando al costo del producto
sin deber, cómo se detectaron y con qué mecanismo se sacaron. Está aquí para
que la próxima no se re-derive desde cero: todas se veían normales hasta que
alguien preguntó por qué un número no cuadraba.

**La regla que las une:** el modelo reparte un promedio de doce meses sobre
la producción. Cualquier partida **única** —una regularización, un cierre,
una baja de activo— el suavizado la convierte en costo **recurrente** de cada
mes. Y cualquier partida cuya **contrapartida** viva fuera de los buckets de
costeo se ve a medias.

---

## 1. Póliza de cierre anual

`Dr/2025/12/32` · ref `POLIZA DE CIERRE ANUAL` · **$190,684,760**

Reversa las cuentas de resultados del año entero contra un solo asiento de
diciembre. Dos daños:

- La conciliación de diciembre salía sin sentido: −$163M de «ventas» y −$147M
  de «gasto», con una diferencia de ventas de $174,966,216 que es, al peso,
  el total facturado del año.
- Cada pool perdía **diciembre entero**, porque `_smooth` descarta los meses
  negativos. Cada año que se quisiera ver perdía un mes real de los doce —y
  ya estaba pasando en 2026 sin que se notara.

**Cómo se saca:** `refs_fuera_de_costeo`, por referencia del asiento.

## 2. Baja de activo vendido

`Ch/2025/12/08` · ref `REGISTRO ENAJENACIÓN DE ACTIVO MAQUINA` · **$5,827,157**

Depreciación pendiente de dos máquinas (FONGS JET y CIRCULAR INTERLOCK)
cargada a `504.08.0001`, que está en el bucket `depreciacion`. Entraba al
pool fabril: $485,596/mes, el 7.8%.

Tres razones para sacarla, y apuntan al mismo lado:

1. **Ya está compensada.** `704.23.0003 UTILIDAD EN VENTA DE ACTIVO FIJO`
   trae $5,896,997 el mismo mes —diferencia de $69,840, la utilidad real—,
   en una cuenta `income_other` que el costeo no mira. El módulo veía media
   operación.
2. **Es un evento único** y el suavizado a 12 meses lo vuelve recurrente.
3. **Es doble conteo.** Fue una venta con **arrendamiento en reversa**: esas
   máquinas hoy se pagan como renta y la renta ya está en el pool. El
   arrendamiento (`701.11%`) saltó de 10 a 16 contratos y de $600,440 a
   $1,028,398 al mes justo en dic-2025.

**Cómo se saca:** `refs_fuera_de_costeo`, patrón `ENAJENACI`.

> Si siguen migrando maquinaria a arrendamiento habrá más bajas. Se agregan
> al parámetro, sin tocar código.

## 3. Ajustes de cantidad que no son merma

`501.01.02 COSTO POR AJUSTES A CANTIDAD` mezcla naturalezas:

| Etiqueta | Qué es | ¿Costo? |
|---|---|---|
| `SP/10758` | scrap de Odoo | **sí** — merma real |
| `TL/EMB/04840` | embarque | no |
| `TL/ENC//00103` | encogimiento | no |
| `TVAR/ENT-REF/00471` | entrada de refacciones | no |

Un asiento de regularización de dic-2025 —$5,822,686, «Merma no contabilizada
(1,136 scraps sin asiento)»— entró completo y subió `mp_ajuste` de **0.781 a
0.903**: +15.6% de costo de MP en todos los productos.

**Cómo se saca:** `filtro_etiqueta` en la clasificación de la cuenta, con
`SP/`. Es filtro de LÍNEA: la clasificación sigue siendo por cuenta y esto
acota qué parte de ella entra.

---

# Cuando el motor no ve lo que existe

No todo es exceso. Estas son las veces que el módulo **no veía** algo que sí
estaba, y el efecto fue igual de grande.

## 4. Patrones de orden que no cubrían el naming viejo

Los `mo_name_pattern` traían solo el prefijo actual `TL/OP-`. Las órdenes
anteriores al cambio se llaman `OP-TEC-00445`, `OP-ACA-00898`, `OP-V1000421`
—sin `TL/`—, así que el denominador de producción salía partido.

**La producción en Odoo arranca en 2022**, no en 2024:

| Año | Órdenes | Unidades |
|---|---:|---:|
| 2022 | 9,093 | 20,472,510 |
| 2023 | 11,032 | 26,505,091 |
| 2024 | 14,710 | 28,878,853 |

Medido en enero-2024, el caso más visible:

| | Antes | Después |
|---|---:|---:|
| kg producidos (ventana) | 25,188 | 58,269 |
| Utilización kg | 26.6% | 61.6% |
| **Energía $/kg** | **34.22** | **14.79** |

La energía es variable y se divide entre kilos reales, así que el denominador
partido inflaba su $/kg 2.3× y con él el costo unitario. **El margen de 2024
pasó de −$8,612,780 a −$1,855,540.**

> Señal de que es esto y no el negocio: el margen mejoraba **monótonamente**
> a lo largo del año, en lock-step con la producción registrada. Un año real
> oscila.

## 5. Un patrón amplio marcado como renta por una sola cuenta

`marcar_cuentas_de_renta` marcaba una clasificación entera si **alguna** de
sus cuentas era renta. Para un patrón eso saca del pool todo lo que abarca:
`504.01%` incluye a `504.01.0008 RENTA DEL LOCAL`, y `701.11%` a
`701.11.0001 ARRENDAMIENTO FINANCIERO`.

Quedaron marcadas **38 cuentas fabriles, de las cuales una sola lo era**. El
motor sacaba del pool $1,534,140/mes que nada reponía: mantenimientos de
fábrica, herramientas, uniformes, ISO, y el arrendamiento de la maquinaria
con la que se produce. **$12,273,123 entre enero y agosto de 2026.**

## 6. La conciliación filtraba por tipo y el motor por bucket

La conciliación miraba `income`, `expense`, `expense_direct_cost` y
`expense_depreciation`. El motor mira **buckets**. Esa asimetría dejaba fuera
del mayor gasto que el modelo sí cobraba, en cuentas `income_other`:

| Cuenta | 2025-2026 |
|---|---:|
| `701.11.0001` Arrendamiento financiero | +13,907,465 |
| `701.01.0001` Pérdida cambiaria | +6,929,162 |
| Intereses y comisiones | +1,309,819 |
| `704.23.0003` Utilidad en venta de activo fijo | −6,545,551 |
| `702.01.0001` Utilidad cambiaria | −4,527,270 |
| `704.23.0001` Otros ingresos | −2,928,358 |
| **Neto** | **+8,065,226** |

El arrendamiento es el que importa: tiene bucket fabril, el modelo lo cobra
bien, y el mayor no lo contaba. Ahora va en `gl_otros_costeo` y entra al
gasto total. El resto es resultado integral de financiamiento: no es costo de
producto, pero sí resultado de la empresa, y va en `gl_resultado_integral`.

---

## Cómo se detecta el siguiente

1. **Un mes que se sale de la serie.** Ordena el pool por mes y mira los
   extremos. Diciembre es sospechoso por diseño: cierres, aguinaldo,
   provisiones y regularizaciones caen ahí.
2. **Una tendencia monótona.** Si un margen mejora o empeora mes a mes sin
   escalones, casi siempre es un denominador que cambia, no el negocio.
3. **Media operación.** Si un gasto grande no tiene contrapartida visible en
   los buckets, búscala en el resto del mayor antes de concluir. Aquí pasó
   dos veces, y las dos veces la conclusión inicial estaba mal.
4. **El panel.** Revisa «Producción baja en la ventana»: marca los períodos
   cuyo unitario no compara con uno normal.

## Los mecanismos disponibles

| Qué hace | Dónde se configura |
|---|---|
| Sacar asientos enteros por su referencia | `refs_fuera_de_costeo` (parámetro) |
| Dejar pasar solo ciertas líneas de una cuenta | `filtro_etiqueta` (clasificación) |
| Sacar una cuenta del costeo | bucket `no_costeo` |
| Sacar un centro completo del pool | `modo_costeo` = absorción + fecha de corte |

Los cuatro son visibles, reversibles y no requieren tocar código.
