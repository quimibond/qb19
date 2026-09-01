# Revisión del costeo — qb_capacidad_costeo

**Fecha:** agosto 2026 · **Compañía:** PRODUCTORA DE NO TEJIDOS QUIMIBOND (id 1)
**Estado:** los cinco hallazgos prioritarios están arreglados — ver § 3.5
**Datos:** mayor posteado de Odoo (ene–ago 2026) contra `qb.costo.producto`
y `qb.costo.factores` de los mismos meses.

## Resumen en una línea

El modelo le cobra a los productos **más de lo que la empresa gasta**: en
julio 2026 la contabilidad da una utilidad de operación de **+$1,472,394** y
el modelo dice **−$664,296**. Son **$2.14 millones de diferencia en un solo
mes** (12.6% de las ventas), y acumulado enero–agosto la brecha es de
**$8.6 millones**. Nada en el módulo la medía, así que nadie se enteró.

| | Ene–Ago 2026 |
|---|---:|
| Resultado de operación real (mayor) | **+$4,766,429** |
| Resultado según el modelo (Σ margen neto) | **−$3,859,830** |
| **Brecha** | **−$8,626,259** |

Lo bueno: **el lado de las ventas está perfecto**. La facturación que lee el
motor (`aml.balance` de líneas de producto contra cuentas de ingreso) cuadra
al peso con el estado de resultados — julio: $17,010,128.78 en ambos lados —
y las cantidades también (893,131.18 m netos en los dos). El problema está
entero del lado del costo.

---

## 1. Anatomía del mes: a dónde va cada peso (julio 2026)

Costo de ventas real: **$13,317,567**.

| Concepto | Cuentas | $/mes | % | ¿Lo carga algún producto? |
|---|---|---:|---:|---|
| Materia prima consumida | 501.01.01 COSTO PRIMO | 5,885,882 | 44.2% | **No** — `no_costeo`; se sustituye por la receta |
| Mano de obra de fábrica | 501.06.xx (17 cuentas) | 3,486,632 | 26.2% | Sí — bucket `mod` |
| Overhead de planta | 504.01.xx (gas, agua, mantenimiento, agujados…) | 2,486,851 | 18.7% | Casi todo sí |
| Gastos de importación | 504.01.0035 | 548,861 | 4.1% | **No** — `no_costeo` |
| Ajustes de inventario y conteo | 501.01.02 + 501.01.08 | 494,605 | 3.7% | **No** — `no_costeo` |
| Impuestos de importación | 502.03.02 DTA + .03 IGI + .04 PRV | 414,736 | 3.1% | **No** — `no_costeo` |

Más, del lado de gastos: operación y depreciación **$2,220,168**.

### Gasto real que ningún producto carga: **$1,815,137/mes** (10.7% de las ventas)

Descontando el costo primo (que sí es correcto excluir, porque se sustituye
por la explosión de receta), queda esto sin dueño:

| Fuga | $/mes | Dónde debería estar |
|---|---:|---|
| Gastos de importación (fletes, agente aduanal) | 548,861 | Costo del producto importado (landed) |
| Ajustes de inventario + diferencias por conteo | 494,605 | Merma real de producción |
| Impuestos de importación IGI/DTA/PRV | 414,736 | Costo del producto importado (landed) |
| Renta de planta excluida y nunca reintegrada | 356,935 | Pool de fabricación |

El caso de la renta merece explicación porque es un **bug**, no una decisión.
`504.01.0008 RENTA DEL LOCAL` está clasificada `no_costeo` con el argumento
—documentado en el README— de que en su lugar se usa la *renta contractual*
capturada por centro. Pero en `models/costeo.py` la renta contractual solo se
suma al pool dentro del bloque de entretelas:

```python
entretela_pool = (ent_mod
                  + sum(ent_centros.mapped('renta_contractual_mxn'))
                  + Config.get_param('entretela_overhead_extra_mxn', 0.0))
```

Los $641,203/mes de renta contractual de TEJIDO ($284,269), TINTORERÍA
($178,467) y ACABADO ($178,467) están capturados en la configuración y **solo
alimentan la vista de ociosidad — nunca el costo del producto**. La única
renta que llega a la tela es la que se cuela por `603.45.0001 RENTA DEL LOCAL
(PLANTA)` ($286,678, clasificada `overhead_fab`).

### Y por el otro lado, cobra de más: **$2.18M/mes**

| Capa | Modelo (julio) | Realidad (mayor) | Exceso |
|---|---:|---:|---:|
| Variable (MP + energía) | 8,742,962 | 7,728,357 | **+1,014,605** (+13%) |
| Fabricación absorbida | 6,554,370 | 5,607,914 (pool disponible) | **+946,456** (+17%) |
| Operación | 2,437,986 | 2,220,168 | +217,818 |
| **Total** | **17,735,318** | **15,537,735** | **+2,197,583** |

Las dos fugas se compensan parcialmente en el agregado, pero **no en el
producto individual**: el importado paga de menos (no carga sus impuestos ni
sus fletes) y la tela nacional paga de más (MP inflada y fabricación
sobre-absorbida). Es exactamente la mezcla de errores que lleva a subir el
precio de lo que sí deja y a defender lo que no.

---

## 2. Los seis problemas de método

### 2.1 La MP no es la MP — y nada lo revisa

`mp_unit` es la explosión recursiva de la receta al **último precio de
compra**. Eso es un costo de reposición teórico, no lo que se consumió. Le
falta:

- **Merma y rendimiento real.** La receta dice 0.072 kg de hilo por metro; si
  la máquina consume 0.080, el modelo nunca se entera. Los $494,605/mes de
  ajustes de inventario son justamente esa diferencia, y están en `no_costeo`.
- **Variación de precio.** El último precio de compra puede ser de hace seis
  meses o de un pedido atípico. La cuenta 501.01.01 lleva lo que de verdad se
  pagó por lo que de verdad se consumió.
- **Filtro de variante y subproductos.** `_explode_bom` recorre
  `bom.bom_line_ids` sin llamar `_skip_bom_line(product)`, así que en recetas
  con atributos el producto carga componentes que no consume; y no acredita
  `byproduct_ids`, así que la receta que genera subproducto vendible lo carga
  todo al principal.

Resultado medible: **+$1.0M/mes contra el mayor**, sin que ningún indicador
lo señalara — porque `501.01.01` está en `no_costeo` y el bucket `mp` de
`BUCKETS` está declarado pero **nunca se consulta** en el motor.

### 2.2 La fabricación se sobre-absorbe entre 80% y 117%

`cobertura_fab_pct` ya medía esto, y los números son alarmantes:

| Mes | Cobertura |
|---|---:|
| mayo 2026 | 80.2% |
| junio 2026 | 100.5% |
| julio 2026 | **116.6%** |
| agosto 2026 | 84.0% |

El factor se arma con la producción del mes (`kg_denom`, `m_denom`) y se
aplica a lo **vendido**. Si la mezcla vendida pesa distinto que la producida
—que es lo normal—, no cuadra. Un ±18% de swing mes a mes en la capa que
representa el 37% del costo hace que el mismo producto valga distinto según
cuándo se le pregunte.

### 2.3 El costo depende del precio (circular)

```python
op = factores.op_pct * precio
```

La operación se cobra como % del precio de venta. Consecuencia directa: **si
vendes más barato, el modelo te dice que costó menos**. Dos clientes que
compran el mismo metro a distinto precio tienen distinto "costo unitario", y
el producto vendido con descuento se ve artificialmente sano. Para decidir
precios eso es exactamente al revés de lo que se necesita: el piso debe ser
independiente del precio.

Además, un producto **sin ventas en el mes** tiene `precio = 0` ⇒ `op = 0` ⇒
su `costo_absorbido` sale sistemáticamente bajo. Justo los productos que hay
que evaluar para decidir si vale la pena empujarlos.

### 2.4 La ociosidad se le carga al producto

Los 12 centros tienen `capacidad_normal = 0`. El motor entonces divide el
pool fijo entre la **producción real** del mes, no entre la capacidad normal.
Eso significa que **un mes flojo encarece el producto** — y el modelo
recomienda subir el precio justo cuando lo que hace falta es vender más.

Es lo contrario de lo que dice el propio README ("costo fijo ÷ capacidad
normal; la ociosidad va al P&L, no al producto", IAS 2) y de lo que hace la
vista `qb.ociosidad`, que sí usa capacidad normal. Las dos partes del módulo
no están de acuerdo entre sí.

### 2.5 La fabricación se reparte por planta, no por ruta

El split 67/33 entre kg y metros es global: **todo** producto de tela paga
tejido y acabado en la misma proporción, sin importar por dónde pasó de
verdad. Un producto que se vende crudo (sin teñir, sin acabar) paga acabado;
uno que da tres pasadas de acabado paga lo mismo que el que da una.

La información para hacerlo bien **ya existe** en Odoo: cada `mrp.production`
tiene sus `mrp.workorder` con su workcenter y sus horas. El módulo ya liga 37
workcenters al centro TEJIDO. Costear por ruta real es alcanzable sin datos
nuevos.

Lo mismo aplica a la energía: agua está asignada a Tintorería y gas a
Acabado en la clasificación de cuentas, pero `energia_por_kg` es un solo
número de planta — así que **el producto crudo paga el agua de la tintorería
por la que nunca pasó**.

### 2.6 La depreciación no cubre la reposición

`504.08.0001 DEPRECIACIÓN MAQUINARIA Y EQUIPO` = **$79,334/mes** para una
planta con 37 máquinas de tejido circular. Es costo histórico sobre equipo
casi totalmente depreciado. El costo del producto no está reservando lo que
cuesta reponer esas máquinas. No es un error contable —es correcto según la
norma— pero sí es una razón para no usar el costo absorbido como piso de
precio de largo plazo sin un ajuste explícito.

---

## 3. Cosas menores pero reales

| # | Qué | Dónde |
|---|---|---|
| 1 | `_production_month_avg` no filtra `company_id` al leer `mrp_production` / `mrp_workorder`; el resto del motor sí filtra compañía. En multicompañía el denominador se contamina. | `models/costeo.py` |
| 2 | El dedup de cantidad `DISTINCT ON (move_id, product_id, ABS(quantity))` tira líneas legítimas si un mismo producto aparece dos veces en la misma factura con la misma cantidad (dos rollos iguales): la qty se parte a la mitad y el precio promedio se duplica. **Verificado: en julio 2026 no se disparó** (312 líneas, cero duplicados), pero es una bomba de tiempo en un negocio que factura por rollo. | `_sales_by_product` |
| 3 | `std_output_per_hour = 0` en TINTORERÍA, ENTRETELAS e INSP_EMPAQUE ⇒ `_hours_per_unit` los ignora y la "contribución por hora-máquina" se calcula sobre TEJIDO o ACABADO aunque el cuello real esté en otro lado. | `models/config_costeo.py` (datos) |
| 4 | El bucket `mp` existe en `BUCKETS` pero ningún `_pool_by_month` lo consulta: es configuración que no hace nada. | `models/costeo.py` |

---

## 3.5 Estado de cada hallazgo

| # | Hallazgo | $/mes | Estado |
|---|---|---:|---|
| 1 | Renta de planta excluida y no reintegrada | 356,935 | **Arreglado** (v19.0.1.10.0) |
| 2 | Gastos e impuestos de importación en resultados | 352,690 | **Arreglado** (v19.0.1.17.0): el módulo mide, no prorratea |
| 3 | MP de receta sin conciliar contra el costo primo | ~1,014,605 | **Arreglado** (v19.0.1.12.0) |
| 4 | Ociosidad cargada al producto | — | **Arreglado** (v19.0.1.13.0) |
| 5 | El costo depende del precio | — | **Arreglado** (v19.0.1.14.0) |
| 6 | Ajustes de inventario y conteo sin cargar | 494,605 | **Arreglado**: entran por el bucket `mp` (v19.0.1.12.0) |
| 7 | Sobre-absorción de fabricación (80–117%) | ~946,456 | **Mitigado**: el denominador de capacidad normal la estabiliza; lo que no se absorbe queda medido en `fab_ocioso_month` |
| 8 | Dedup que parte a la mitad dos rollos iguales | — | **Arreglado** (v19.0.1.15.0) |
| 9 | Receta con atributos que carga de más | — | **Arreglado** (v19.0.1.15.0) |
| 10 | Denominador sin filtro de compañía | — | **Arreglado** (v19.0.1.10.0) |
| 11 | Energía $/kg sobre capacidad en vez de producción | — | **Arreglado** (v19.0.1.13.0) |
| 12 | Fabricación repartida por planta, no por ruta | — | **Pendiente, bloqueado por datos** |
| 13 | Centros sin throughput nominal | — | **Medido**: el panel los lista |
| 14 | Depreciación a costo histórico | — | **No es un error**: nota para el piso de largo plazo |

### El único bloqueado: costear por ruta

El reparto 67/33 entre kilos y metros es de planta, así que un producto que se
vende crudo paga acabado. La información de ruta ya existe (cada
`mrp.production` tiene sus workorders con su workcenter), pero **repartir por
ruta exige que el gasto fabril esté asignado a un centro de costo**, y hoy casi
todo el pool está clasificado sin centro: solo agua va a Tintorería, gas a
Acabado y energéticos a Tejido.

Implementarlo ahora dejaría la mayor parte del pool sin repartir y rompería el
costo. Por eso este cambio no lo implementa: agrega
`fab_pool_con_centro_pct` a los factores, que mide exactamente cuánto falta.
Cuando esa cifra suba, el costeo por ruta se vuelve un cambio mecánico.

## 4. Qué se hizo en este cambio

Nada de lo anterior se puede arreglar a ciegas: primero hay que **ver** la
brecha. Este cambio hace visible el dinero real y deja el diagnóstico
permanente.

### 4.1 Costo por producto ahora trae el dinero, no solo los $/unidad

Campos nuevos en `qb.costo.producto`:

- **`ventas_total`** — lo realmente facturado en pesos (Σ `aml.balance`).
  Cuadra contra el estado de resultados.
- **Totales por capa** — `mp_total`, `energia_total`, `fab_total`,
  `op_total`, `costo_variable_total`, `costo_produccion_total`,
  `costo_absorbido_total`: el costo de lo vendido, en pesos del mes.
- **Precio en divisa** — `divisa_id`, `precio_prom_divisa`,
  `ventas_total_divisa`, `qty_divisa` y `tc_prom`: el precio tal cual se
  facturó en USD (o la divisa que sea), sin convertir, además del espejo en
  pesos que ya existía. `tc_prom` es el tipo de cambio efectivo de las
  facturas del mes, no el del día.

Los márgenes bruto y neto ya existían en monto unitario, % y total; ahora las
identidades cuadran contra el dinero real y hay test que lo verifica:

```
ventas_total − costo_produccion_total = margen_bruto_total
ventas_total − costo_absorbido_total  = margen_neto_total
ventas_total − costo_variable_total   = contrib_total
```

Todo aditivo: el pivote suma productos y meses sin mentir.

### 4.2 Conciliación vs. contabilidad (nueva)

**Capacidad & Costeo → Análisis → Costos → Conciliación vs. contabilidad.**

Una fila por mes, en vivo, sin cron. Compara lado a lado:

- ventas del modelo vs. ventas del mayor (y su diferencia),
- costo repartido a los productos vs. gasto real (costo de ventas + gastos),
- **gasto fuera de costeo** y **gasto sin clasificar** — el dinero que se
  paga y nadie carga,
- resultado del modelo vs. resultado de operación contable, y la **brecha**
  en monto y como % de las ventas.

Semáforo: verde bajo ±2% de las ventas, ámbar hasta 5%, rojo arriba. Hoy sale
rojo. Ese es el punto: hasta que esa fila esté en verde, el costo unitario no
sirve como piso de precio.

---

## 5. Qué sigue

Los cinco puntos prioritarios de la primera versión de este documento ya están
implementados (ver § 3.5). Lo que queda:

1. **Verificar la conciliación después de desplegar.** Cada arreglo mueve la
   brecha; el número que debe tender a cero es **Brecha sin ociosidad**. Si
   algún factor quedó fuera de su banda de cordura, el log lo dice y el panel
   lo marca.
2. **Revisar las cuentas de aduana que el panel liste como mal ubicadas.** La
   migración solo movió las que estaban fuera de costeo; una cuenta de
   importación clasificada en `operacion` se sigue prorrateando sobre TODAS
   las ventas —incluidas las de producto nacional— y moverla es decisión del
   usuario.
3. **Capturar el throughput nominal** de tintorería, entretelas e inspección.
   Hoy quedan fuera de la contribución por hora-máquina, así que el ranking
   mide el centro equivocado cuando el cuello real está en uno de ellos.
4. **Asignar el gasto fabril a centros de costo.** Es el prerrequisito del
   costeo por ruta; `fab_pool_con_centro_pct` mide el avance.
5. **Decidir si el costo absorbido sirve como piso de largo plazo.** La
   depreciación de maquinaria ($79,334/mes para 37 máquinas de tejido
   circular) es costo histórico sobre equipo casi totalmente depreciado. No es
   un error contable, pero el costo no está reservando la reposición.

---

## Apéndice: correcciones a este documento

Tres cifras de la primera versión estaban mal medidas y quedan corregidas
arriba. Se dejan anotadas porque hubo decisiones que se tomaron con ellas:

| Afirmación original | Medición correcta |
|---|---|
| «Landed cost configurado y sin usar» | Hay **163 landed costs aplicados por $7.85M** — pero 159 son de 2023 y solo uno entre 2024 y agosto 2026. La práctica existe y se detuvo. El problema real es que **además** se gastan importaciones en resultados en paralelo, o sea el mismo pedimento por dos vías. |
| «$963,597/mes de pedimentos en resultados» | **$352,690/mes** (502.03.x + 504.01.0035 = $2.82M en ocho meses de 2026). |
| «~10,000 líneas y 30 parámetros» | **5,837 líneas** de Python (más 1,185 de tests) y **57 campos** de configuración. |

Lo que NO cambia con la corrección: la aduana seguía quedándose en resultados
y ningún producto la cargaba, y el reparto que se implementó primero
(v19.0.1.11.0) estaba mal repartido. El arreglo de v19.0.1.17.0 —medir en vez
de prorratear— sigue siendo el correcto, y con landed costs operando desde
septiembre es además el que se vuelve suficiente.

También conviene dejar por escrito lo que se revisó y resultó estar **bien**,
para no "arreglarlo" después:

- Los 64 BOMs con `cost_share = 0` en el subproducto son correctos: el
  principal absorbe todo el costo y el saldo entra a $0 en producción normal.
- Las conversiones (CONV-ART, RE-TIN, CVU) son neutras en valor.
- El ajuste de metros por encogimiento/estiramiento está bien implementado y
  mueve el costo/metro +1.16%. El primer recálculo debe moverse aproximadamente
  ese porcentaje: si se mueve mucho más o mucho menos, hay que parar y revisar.

---

## Bitácora 31-ago-2026: consumo de BOM inflado ("hilo fantasma")

Este es el error que §2.1 predecía («si la máquina consume 0.080, el modelo
nunca se entera») — ocurrió al revés: la BOM decía MÁS de lo que la máquina
consume, y el modelo tampoco se enteraba.

**Cómo se encontró.** El usuario preguntó por el costo del kg de hilo
policotton del X140 ($65 la última compra) y no le cuadraba el $18.42/m de
MP. La conversión era correcta ($65 × 0.2674 kg/m de la BOM ≈ $17.4 + $1 de
auxiliares), pero al comparar el factor 0.2674 contra las OPs de acabado
reales, éstas consumían **0.2425–0.2474 kg/m**. La BOM cargaba ~8% de hilo
que la planta no gasta: **$1.02/m de MP fantasma** en un producto que el
modelo pintaba vendiendo bajo costo (−3.7%) cuando estaba en equilibrio.

**Barrido completo (12 meses de OPs done, pareo consumo↔producción por
producto).** La inflación NO era generalizada:

| Familia | BOM | Real 12m | Veredicto |
|---|---|---|---|
| XJ140Q21JNT165 (X140) | 0.2674 | 0.2474 | **+8.1% — corregida** |
| XJ140Q21JGO165 | 0.2674 | 0.2481 | **+7.8% — corregida** |
| WN055Q66JNT162 (SCRIM 55) | 0.1032 | 0.0963 | **+7.2% — corregida** |
| WN055Q66JNG172 (2 BOMs alt.) | 0.1094 | 0.0965 | **+13.4% — corregidas** |
| WJ042/045/053, WD038 ×4, WC090 ×2, WJ060 ×3, WN055 BL172, WN075, TJ085 | — | ±2% | correctas, sin tocar |

**Efecto del fix** (recalculado ene–ago 2026): X140 agosto −3.7% → +0.2%
de margen absorbido; margen de productos ene–jul +$228K (11.06M → 11.28M).

**Hallazgos colaterales sin resolver** (necesitan a producción):
- A60BL155 y K40BL155: sus BOMs apuntan a fibras que **nadie consumió en
  12 meses** — las OPs reales usan otros componentes; su MP se costea con
  la fibra equivocada.
- WD038-NG166: entreverada con su reproceso ING163, no separable desde
  fuera.
- Hilo gemelo HPESCO22/16535 (BOM "MP 35% ARANCEL") sin una sola compra
  registrada: unificar con HP65P35A22/1.

**El guard que quedó** (v19.0.1.45.0): check del panel «Consumo de BOM vs
OPs reales» — compara cada receta kg→m con ≥50,000 m producidos en 12
meses contra el consumo real de las OPs done y avisa a ±5%. Con recetas
alternativas manda la más cercana al real. La lección de método: **la BOM
es un parámetro que duplica un dato vivo** (el consumo de las OPs) y toda
la clase de parámetros así se valida contra su fuente — misma regla que ya
cubría pesos (5.11), AVCO de importados (5.13) y luz/energía.

---

## Bitácora 31-ago-2026 (2): la capacidad sale del papel de planta

Dos números del módulo eran estimaciones que nadie podía contrastar, y los
dos estaban mal por razones distintas. La planta mandó sus formatos
(F-IT-P-P01-10-06 rev 02, abr-2026: tiempos de rama, tiempos de tintorería,
capacidad de cargas y horario) y ahora salen de velocidades medidas.

### Acabado: 915,733 → 1,175,313 m/mes (+28%)

Las dos ramas dan 3,015.9 m/h netos —UNITECH 29.0779 m/min menos su 10% de
descuento, BRUCKNER 28.3478 m/min menos su 15%— y el horario declarado es de
90 h/semana en dos turnos, o 389.7 h/mes con la convención del módulo. La
planta declara 1,158,124 en su hoja porque calcula el mes con 384 h; la
diferencia es 1.5%.

Esto es exactamente lo que el check «Capacidad normal vs producción real»
venía pidiendo desde v1.49: con 915,733 capturados, **cinco de los ocho meses
de 2026** (ene–may, 918K–939K m) produjeron arriba de la capacidad y el
costeo tuvo que caer a producción real para no sobre-absorber. La rama nueva
que faltaba reflejar no era nueva: eran las dos que ya estaban corriendo.

La ICOMATEX (RAMA 3) **sigue en montaje** — no produce todavía. Cuando
arranque: máquinas a 3 en los turnos y recapturar la capacidad.

### Tintorería: 195,000 → 216,089 kg/mes (+11%), y las tinas NO son el cuello

El 195,000 era el producto de tres supuestos (5 tinas × 625 h/mes × 90% OEE ÷
9 h de ciclo × 620 kg de baño) y los tres estaban mal, casi cancelándose
entre sí:

| Supuesto | Real |
|---|---|
| 5 tinas | **4** — la HTJ-5 (THEN, 1,200 kg) sigue en pruebas |
| 625 h/mes | **389.7** — 90 h/semana, el mismo horario que acabado |
| Ciclo único de 9 h | **de 2:20 a 10:20** según tina y color |

El ciclo no es un número: va de 2:20 h en naturales a 10:20 h en obscuros.
Ponderado por la mezcla real de las OPs de 12 meses —**82% natural, 13%
blanco, 2.7% obscuro, 2.3% medio**— el ciclo efectivo es de **3.1 h**, un
tercio de lo asumido. Las cuatro tinas dan 554.5 kg/h.

Respuesta al pendiente que la propia nota del centro dejó abierto («con 12 h
serían EL cuello de la casa»): **no lo son**. Corren al 43% de su capacidad
(92,810 kg/mes reales contra 216,089). El cuello sigue siendo acabado, que va
al 89% (1,044,150 m en agosto).

### El guard que quedó

`capacidad_normal`, cuando está capturada, **gana en silencio** sobre el
cálculo de turnos × throughput. Por eso Acabado pudo vivir con un número que
sus propias máquinas contradecían: nada los comparaba. Dos cambios:

1. **Check nuevo «Capacidad capturada vs horario × velocidad»**: contrasta el
   número capturado contra sus turnos por su velocidad nominal y avisa a más
   de ±10%. Mismo patrón que ya cubre pesos (5.11), AVCO de importados (5.13)
   y consumo de BOM (5.14). Para que tenga contra qué comparar, quedaron
   capturados los turnos de acabado (90 h/sem, 2 ramas) y tintorería (90 h/sem,
   4 tinas).
2. **El check de capacidad superada mira una ventana de 12 períodos**, no solo
   el último. Miraba agosto —que cabía— y se pintaba verde mientras enero a
   mayo seguían rojos. Un mes flojo no arregla una capacidad mal capturada;
   solo la esconde.

### Efecto en el costo

Subir el denominador de metros 28% baja el factor de fabricación por metro en
la misma proporción: el costo fijo por metro deja de llevar dentro la
ociosidad, y la ociosidad aparece completa donde va —en el resultado del
período, no en el producto. El par indivisible no se mueve: margen de
productos − ociosidad = el mismo resultado de siempre.

Recálculo: **solo 2026**. Los períodos de 2024 y 2025 quedan como están hasta
que dirección los cierre. Advertencia: hoy los 32 períodos están en
`borrador` —ninguno marcado `cerrado`— así que el guard de períodos cerrados
no los protege todavía; la restricción vive en la migración, no en el modelo.

### Verificación del recálculo de v1.50 (rendimiento vendible)

La cola de recálculo de 2026 drenó completa (config `recalculo_pendiente`
vacía). Lo que el rendimiento vendible destapó, con julio 2026 como corte:

- **WC090Q11JNT165** rinde 0.90: su costo vendible ($17.63/m en agosto) queda
  **arriba** del absorbido ($15.93), y en junio el margen pasa de +10.2%
  contable a **−0.30% real**. El semáforo deja el verde, que era el punto.
- **Ocho productos cambian de signo** en julio al mirar el margen real en vez
  del neto. Los dos que importan por volumen:

  | Producto | m vendidos | Margen neto | Margen real | Rendimiento |
  |---|---:|---:|---:|---:|
  | WN075Q66JBL205 | 63,098 | +0.27% | **−7.99%** | 0.92 |
  | WN055Q66JNT162 | 18,930 | +0.99% | **−16.74%** | 0.85 |
  | A40BL155 | 1,000 | +1.07% | **−27.06%** | 0.78 |
  | WNY4032BL151 | 400 | +10.29% | **−14.52%** | 0.78 |

  Los cuatro se vendían "en equilibrio" y en realidad pagan la merma con el
  margen. WN075 solo, a 63 mil metros al mes, es la decisión de precio más
  cara de la lista.

### Incidente del despliegue de v1.51: el recálculo leyó la capacidad vieja

La migración escribió la capacidad de Acabado (915,733 → 1,175,313) y
recalculó 2026 en la misma transacción. Los ocho períodos salieron con el
denominador **viejo**, y el modo de falla fue el peor: silencioso. Los
factores se veían normales —`m_denom_month` 915,733, utilización 98.8%— y
solo comparándolos contra el centro se notaba que el número ya no
correspondía a lo que decía la tabla.

**Causa.** `qb.ociosidad` es un `_table_query`: lee `qb_costeo_centro` con
SQL crudo. El ORM no tiene cómo saber que ese SELECT depende de un write
pendiente sobre los centros, así que no hace flush y la vista devuelve el
renglón anterior. Cualquier flujo que escriba un centro y recalcule sin
cerrar la transacción cae en lo mismo.

**Arreglo.** `_capacidad_normal_map` hace flush antes de leer la vista
(v1.52), con su test de regresión; el recálculo se movió a la migración
1.52. Producción quedó corregida a mano el mismo día, período por período.

**Efecto real, ya con la capacidad correcta** (ene–ago 2026):

| | Antes | Después |
|---|---:|---:|
| Denominador de metros | 915,733 | **1,175,313** |
| Utilización de acabado | 98.6–100% | **77.0–80.0%** |
| Factor de fabricación $/m | 2.00–2.09 | **1.57–1.64** |
| Ociosidad de fabricación $/mes | 1.80–1.89M | **2.19–2.32M** |

Los meses de enero a mayo dejaron de estar topados al 100%: ya no hay
`capacidad_superada_m` en ningún período de 2026. El costo fijo por metro
bajó ~22% y esos ~400 mil pesos al mes se movieron del producto al
resultado del período, que es donde IAS 2 los quiere. El par no cambia:
margen de productos − ociosidad = el mismo resultado.

**Lo que el check nuevo dice hoy, y es cierto:** Acabado y Tintorería
cuadran contra sus turnos (±0.01%), pero **TEJIDO y ENTRETELAS quedan en
ámbar** porque nadie capturó su horario y su capacidad no tiene contra qué
validarse. El caso de tejido vale una pregunta a planta: los 180,000
kg/mes capturados, contra 37 máquinas a 11 kg/h, implican **102 h/semana
por máquina** — mientras el calendario que traen sus workcenters en Odoo
es «Jornada 24/7 3 Turnos» (168 h). Uno de los dos números describe la
planta y el otro no.

### Tejido: el tercer centro deja de ser estimación (v1.53)

Con «informacion_de_carga_produccion» (hojas CapacidadesProducto y Turnos)
se cierra el último centro fabril cuya capacidad no tenía fuente.

**180,000 → 197,529 kg/mes (+10%).** 27 circulares tejiendo, cada una a su
velocidad documentada, por 623.5 h/mes. El horario real son **144 h/semana**:
doce turnos de 12 h, con la planta parada de viernes 19:00 a sábado 19:00 —
no las 168 h del calendario «Jornada 24/7 3 Turnos» que traen sus workcenters
en Odoo. De paso valida el throughput que el módulo traía a ojo: **11 kg/h
capturados contra 11.73 medidos**, 6% de diferencia.

Se cuentan 27 y no las 37 instaladas por el mismo criterio de acabado (dos
ramas corriendo, la ICOMATEX en montaje no cuenta): 28 workcenters
registraron órdenes en agosto —27 máquinas distintas, porque la CIRCULAR 19
está dada de alta dos veces en Odoo— y las otras diez no están fuera de
servicio, pero tampoco se dotan. Con las 37 la capacidad sería 269,174
kg/mes. Una máquina parada por falta de gente es ociosidad, no capacidad que
el producto deba pagar.

**Lo que la medición dejó claro, y vale más que el número.** Las circulares
corren a velocidad nominal: en agosto registraron 8,660 horas-máquina y
produjeron ~93,000 kg, o sea **10.7 kg/h contra los 11.73 del papel** (91%).
El problema de tejido no es que las máquinas vayan lentas — es que de las
17,458 horas-máquina programadas de esas 27 circulares se usó **la mitad**.
La ociosidad es de horas, no de kilos por hora, y eso es exactamente la
palanca #1: llenar turnos, no apurar máquinas.

Tejido es el denominador de kg, así que el factor de fabricación por peso
baja ~9% para todo lo costeado por kilo.

**Duplicado a limpiar:** la CIRCULAR 19 existe dos veces como
`mrp.workcenter` (ids 386 y 388), las dos con producción. No afecta la
capacidad (se cuenta la máquina, no el registro) pero sí ensucia cualquier
reparto por workcenter.

### La capacidad de un centro no es fungible: familias de máquinas (v1.54)

El número de tejido —197,529 kg/mes contra 93,000 producidos, 47%— invita a
concluir que sobra planta. Es falso, y la columna «Alternos» del formato de
planta lo dice: las circulares se agrupan en familias intercambiables, y un
artículo solo sale en la suya. Los grupos son los componentes conexos de esa
relación, así que **particionan el centro sin traslape**; de 19 artículos
catalogados, **18 solo caben en una familia**.

| Familia | Máquinas dotadas | Capacidad | Carga | Utilización |
|---|---|---:|---:|---:|
| Galga 18 Ø32 | 17,18,28,31–37 (10 de 10) | 47,794 | 37,569 | **79%** |
| Galga 24/28 Ø30 | 6,7,8,9,15,20,27,29,30 (9 de 11) | 58,983 | 24,823 | 42% |
| Galga 18 Ø30 | 19,21,25,26 (4 de 5) | 58,675 | 17,261 | 29% |
| Galga 16 Ø30 | 23 (de 2) | 11,831 | 4,702 | 40% |
| Galga 24 Ø30 | 1 (de 4) | 5,695 | 1,754 | 31% |
| CIRCULAR 38 y 40 | 2 | 14,550 | 0 | **0%** |

La familia galga 18 Ø32 teje el **WJ044 de 235 cm y el WJ035 de 200 cm** —los
dos productos más grandes de la casa, 42 de las 86 toneladas mensuales— y va
al 79%. Un 25% más de demanda de esos dos la satura, con la planta marcando
44%. Al revés, las dos Wellrich (38 y 40) no tejen ninguno de los artículos
catalogados: 14,550 kg/mes de capacidad que el agregado suma como si
sirvieran para todo.

**Lo que se modeló.** `qb.costeo.familia` (grupo de máquinas dentro de un
centro, con su horario, sus máquinas dotadas y su velocidad),
`qb.familia.producto` (qué puede hacer cada familia y a qué velocidad — el
mismo WJ047 da 8.1 kg/h en la galga 18 Ø32 y 18.7 en la Ø30) y
`qb.familia.carga` (capacidad vs carga real vs utilización, con la carga de
un producto repartida entre las familias que pueden hacerlo).

**Dónde cambia una decisión.** El cotizador validaba el volumen contra el
promedio del centro: contestaba que sí a un pedido que la familia capaz de
hacerlo no puede correr. Ahora, cuando el producto está catalogado, valida
contra las máquinas que de verdad lo hacen y lo dice con nombre y apellido.

**Lo que NO cambia: el costo.** La familia es una subdivisión de capacidad,
no de costo. El pool de gasto sigue siendo del centro y se absorbe sobre su
capacidad completa; repartir el gasto fabril por familia es costeo por ruta,
que sigue bloqueado por la asignación del gasto a centros (§3.5). El
denominador de kg no se mueve, así que ningún costo unitario cambia con este
cambio.

**Pendiente de planta:** 2,561 kg/mes (3% de la producción) son artículos que
no aparecen en el catálogo de familias — WT140Q21HNT190, NN053Q66HNT098,
WN052B66HNG099 y seis más. Mientras no estén, el cotizador cae al método
viejo para ellos. Y acabado y tintorería tienen la misma estructura sin
capturar: en las ramas hay artículos que solo corren en la UNITECH, y la hoja
de tintorería marca por artículo qué jets lo pueden teñir.

---

## Los 125 tests corrieron por primera vez (1-sep, v1.59)

Nueve versiones se desplegaron a producción con la suite escrita y nunca
ejecutada: en Odoo.sh no hay paso de tests y el CI de GitHub solo corre
flake8 y `compileall`. Se levantó un Odoo 19 completo —fuente de
`odoo/odoo@19.0`, PostgreSQL 16, base recién creada— y se corrió
`--test-tags /qb_capacidad_costeo`.

**Primer corrido: 29 de 127 fallaron.** Tres eran bugs del módulo, uno de
ellos en producción desde la versión anterior.

### 1. El mixin de flush de la 1.58 era código muerto

`qb.sql.view` se enganchaba a `_where_calc`, que era el embudo de búsqueda
**hasta Odoo 18**. En 19 el ORM se reorganizó (`odoo/orm/`) y ese método ya
no existe: el override no lo llamaba nadie. Peor, la 1.58 había quitado el
`search` ad-hoc de `qb.familia.carga` —que sí funcionaba— para dejarlo en
manos del mixin. O sea que el arreglo estructural del bug más caro del
módulo llevaba una versión entera en producción sin hacer absolutamente
nada, y no había forma de notarlo sin correr el test.

Ahora engancha `_search`, que en 19 es por donde pasan `search`,
`search_fetch`, `search_count` y `_read_group`.

### 2. El flush no bastaba: faltaba tirar la caché

Con el enganche corregido el test seguía fallando. La vista devuelve el
**mismo id** entre consultas, así que sus campos salían de la caché del ORM
sin volver a la base: escribir la capacidad del centro y releer la fila de
ociosidad seguía dando el número viejo. El mixin ahora hace `flush_all()` y
`invalidate_model()`; sin la segunda mitad, el accidente de la 1.51 podía
repetirse igual.

### 3. El mismo bug vivía en 25 lugares más

Los `cr.execute` del motor leen tablas que el ORM administra —centros,
turnos, mapeo de cuentas, familias— y ninguno vaciaba el buffer.
`_pool_by_month`, que es por donde pasan TODOS los pools contables, leía el
mapeo de cuentas en SQL crudo: clasificar una cuenta y calcular factores en
la misma transacción daba pools en cero. Doce tests fallaban por esto solo.
Los 25 sitios ahora vacían antes de consultar.

### 4. La capacidad se validaba con un número inventado

`resolve_m_per_kg` cae a un default de planta (8.0 m/kg) cuando el artículo
no tiene peso propio. El chequeo de capacidad del cotizador lo usaba para
convertir los kilos de la etapa a metros, así que la rama de «no hay peso
capturado — no se puede validar» era inalcanzable: contestaba **OK** sobre
una carga calculada con un promedio que no es de esa tela. Ahora pide
`strict=True` y el default se queda para los reportes aproximados, que es
donde sirve.

### Lo demás: la suite estaba desalineada, no el módulo

Las otras 21 fallas eran tests viejos contra código que ya había cambiado, o
supuestos de una base que no es esta:

| Qué | Por qué |
|---|---|
| 4 tests de MP | `Product Unit` a 2 decimales en una base virgen redondea 0.072 kg/m a 0.07. Producción la tiene en 4; ahora la suite la fija. |
| 3 tests de stock | `stock.move.name` ya no existe en Odoo 19. |
| 2 de aduana | El recargo sigue a la COMPRA, no al producto (refinamiento posterior al test): sin una compra importada de verdad no recarga nada, y eso es lo correcto. |
| 1 de margen | `margen_bruto_total` se deriva del ingreso para que se cumpla `ventas − costo = margen`; el test pedía el 0 de antes de ese arreglo. |
| 1 de semáforo | El comentario daba el costo variable del fixture en ~7.9 cuando es 3.888, así que 10 MXN es ámbar y no rojo. |
| 3 `UPDATE` crudos | Los propios tests escribían por SQL sin vaciar el buffer: el mismo bug del módulo, del otro lado. |
| resto | Ids de producción (tipos de picking 77/147) en una base donde no existen, parámetros ya sembrados, redondeo de campos Monetary. |

**Estado:** 127 tests, 0 fallos, sobre una base instalada desde cero.

**Lo que esto deja claro:** el CI no corre los tests de Odoo, así que la
suite solo vale si alguien la corre a mano antes de desplegar. Mientras eso
siga así, un test escrito y no corrido es documentación, no una red.

---

## Refactor con red: el panel partido y el CTE deduplicado (1-sep, v1.60)

Con los tests corriendo, los tres pendientes de calidad que estaban
bloqueados por no poder verificarlos dejaron de estarlo.

### `_build_estado`: de 677 líneas a 23 métodos

Los 23 checks del semáforo vivían en un solo método. Se partieron en un
método por check más un registro que fija el orden de presentación; agregar
el check 24 ya no obliga a tocar un cuerpo de 677 líneas.

Partirlo destapó dos cosas que el método largo escondía:

**Ocho checks dependían de variables definidas en checks anteriores**
(`Clase`, `Config`, `ultimo`, `absorbidos`). El orden del registro era una
dependencia que nadie había declarado: mover un check de lugar habría roto
otros tres sin decir por qué. Cada uno rederiva ahora lo suyo, y un test los
corre sueltos y en orden inverso para que no vuelva a colarse.

**Un check que truene ya no se lleva el panel.** Cada uno corre aislado; si
falla sale como renglón rojo con su error y los otros 22 siguen. El panel es
la pantalla de entrada del módulo: perder el tablero entero por un dato roto
en un check era el peor canje posible.

También cambió el prefijo de `_check_` a `_estado_`: Odoo usa `_check_*`
para hooks propios del ORM (`_check_company`, `_check_access`,
`_check_recursion`) y un check del panel que cayera en uno de esos nombres
lo habría sobrescrito en silencio.

**Verificación:** la salida HTML del panel es byte a byte idéntica a la de
antes del cambio (3,874 caracteres, 16 renglones).

### El CTE `cfg`: trece copias a mano, una de ellas distinta

Las seis vistas SQL leían sus parámetros del config escribiendo el mismo
subselect a mano, trece veces. No todas iguales: **`weeks_per_month` era el
único sin `NULLIF`**, en cuatro archivos. Nada valida ese campo, así que un
0 tecleado ahí ponía las semanas del mes en cero y con eso la capacidad de
toda la planta — cero horas, cero kilos, y el costo unitario dividiendo
entre cero. En los demás parámetros un 0 caía al default.

Ahora sale de `cfg_sql()`, con la misma guarda para los seis. Cero no es un
valor válido para ninguno.

**Verificación:** las seis vistas devuelven exactamente las mismas filas.
Las cuatro cuyo SQL cambió de texto son justo las que ganaron el `NULLIF`.

### El lint dejó de tapar código muerto

El CI apagaba F401 y F841 en todo el repo por una razón que solo aplica a
los `__init__.py` (en Odoo el import "sin usar" es el punto del archivo).
Apagarlo en 124 archivos para no molestar en esos tapaba **30 hallazgos
reales** en el resto: imports muertos en `quimibond_sgi`,
`quimibond_intelligence` y `quimibond_sgi_plm`, y dos migraciones-stub con
un `env` que dejó de usarse cuando el recálculo se movió.

La configuración pasó a `.flake8` en la raíz, con ignores por archivo y el
motivo de cada excepción escrito. Correr `flake8 addons/` en local da ahora
exactamente lo mismo que el CI.

Uno de los 30 NO se borró: `company_cn` en `sync_push_partners.py` se
calcula con tres ramas deliberadas y nunca se usa. Puede ser una función a
medio terminar (el contacto no lleva el nombre de su empresa a Supabase) o
diez líneas de sobra. Decidirlo es de quien mantiene el sync; queda el
FIXME y la excepción documentada.

**Estado:** 132 tests, 0 fallos, sobre instalación desde cero.

---

## Los tests entran al CI (1-sep)

Correrlos a mano encontró tres bugs el primer día y dos de los tres estaban
en producción. La conclusión obvia: mientras dependan de que alguien se
acuerde, no son una red.

`.github/workflows/ci.yml` gana un job `odoo-tests` que levanta la imagen
oficial `odoo:19.0` con un PostgreSQL 16 de servicio, **instala el módulo en
una base recién creada** y corre `--test-tags /qb_capacidad_costeo`. Se usa
`-i` y no `-u` a propósito: así se prueba también que el módulo instala de
cero, que es como llega a una base nueva y es justo lo que no se probaba.

La etiqueta de la imagen es `19.0` flotante, no una fechada. Producción es
Odoo.sh siguiendo esa misma rama, así que el día que upstream cambie algo
que nos rompa queremos enterarnos en un PR y no en un despliegue — que es
exactamente el aviso que no hubo cuando `_where_calc` dejó de existir. El
costo es que un cambio de upstream puede pintar el CI en rojo sin que nadie
haya tocado el repo; eso es información, no ruido.

**Alcance:** solo `qb_capacidad_costeo`. Los otros módulos del repo tienen
tests que tampoco se han corrido nunca; meterlos todos de golpe pintaría el
CI en rojo y bloquearía todo antes de saber qué falla de verdad. Agregar
cada uno es una línea más en `--test-tags` el día que alguien lo corra a
mano primero y lo deje en verde.

---

## Familias de acabado y tintorería (1-sep, v1.61)

Tejido tenía sus familias desde la 1.54; acabado y tintorería seguían
leyéndose como si cualquier máquina hiciera cualquier producto.

**La subdivisión aquí es distinta.** En tejido las familias salieron de la
columna «Alternos» —qué máquinas son intercambiables— y particionaban el
centro. En acabado y tintorería esa misma columna dice que TODAS las
máquinas de cada centro son intercambiables entre sí, o sea que por esa vía
cada centro sería una sola familia y no habría nada que subdividir. La
restricción real no está en las máquinas sino **por artículo**: hay telas
que solo salen en la UNITECH, y cada teñido declara en qué jets se puede
correr. Así que la familia es la máquina individual y lo que subdivide es el
catálogo.

| Centro | Familias | Capacidad | Catálogo |
|---|---|---|---|
| ACABADO | UNITECH (RAMA 2), BRUCKNER (RAMA 1), ICOMATEX inactiva | 611,954 + 563,448 = 1,175,402 m/mes (centro: 1,175,313, +0.01%) | 74 + 46 artículos |
| TINTORERÍA | HTJ-1 a HTJ-4, HTJ-5 inactiva | 75,820 + 72,028 + 45,494 + 22,747 = 216,089 kg/mes (centro: 216,089) | 40 + 33 + 2 + 17 |

### El catálogo de tintorería NO va en los códigos que da la planta

La hoja lista los **terminados** (J) aunque el jet tiñe el **intermedio**
(I), y el centro produce etapa I. Es el bug de la 1.54 esperando repetirse:
si se captura el código que da el papel, `familias_de()` devuelve vacío en
toda cotización real y el cotizador cae al agregado del centro sin avisar.

El I no se puede derivar del J —cambian gramaje y ancho: `WB038Q47JBL172`
sale de `WB046Q47IBL111`—, así que los 72 códigos se resolvieron uno por uno
por BOM: 64 resueltos a 52 códigos I distintos. Cinco no tienen BOM activa
en Odoo y quedaron fuera, anotados en el archivo. Ahora hay un test que
recorre TODO el catálogo y verifica que cada código sea de la etapa que su
centro produce.

### Lo que el reparto destapó

**HTJ-5 está en pruebas y sería la segunda tina más capaz.** El catálogo de
planta la declara para 29 de los 52 artículos; liberarla suma ~91,000 kg/mes
(+42%) a tintorería. Se dio de alta **inactiva** —una familia activa sin
capacidad se reparte carga que no puede correr y deja a las demás con
holgura falsa—, y ningún artículo depende solo de ella, así que no deja nada
sin ruta. Misma decisión y mismo motivo para la ICOMATEX en acabado.

**HTJ-3 tiene 45,494 kg/mes y solo 2 artículos que la pueden usar.** Es el
21% de la tintorería que casi nadie puede tomar. O falta catalogar artículos
o esa tina está prácticamente parada; en el papel de tiempos aparece con
producción, así que lo más probable es que falte catálogo.

**HTJ-2 rinde 2,500 kg/día en blancos contra 4,000 de la HTJ-1** con carga
casi igual (950 vs 1,000 kg). O el formato mide otra cosa o la tina rinde
por debajo de su carga nominal. El reparto la trata como proporcional a su
carga, que es lo que dice el papel de capacidades; verificarlo es de piso.

**Los dos archivos de planta se contradicen en qué rama es cuál marca.** El
de abridoras dice RAMA 1 = UNITECH; el de tintorería dice RAMA 2 = UNITECH.
Manda el segundo: el encabezado de la propia hoja T RAMA dice «UNITECH RAMA
2», así que la fila de CENTROS DE TRABAJO del primer archivo está vieja.

**La merma de BRUCKNER.** La capacidad del centro se derivó con −15% para
esa rama, pero la columna de merma del formato dice 10% en todos los
renglones. Con 10% daría 1,531 m/h en vez de 1,445.85. Se conservó la base
del centro para que las familias sumen; el 5% de diferencia es pregunta para
planta: ¿es merma o disponibilidad?

### Un modo de falla que casi pasa

La primera generación perdió **7 filas en silencio**. Los códigos
`WD038Q46JNG166` y `WD038Q46JNG166.` son dos productos distintos en Odoo (el
segundo es REPROCESO ACABADO), pero al normalizar el xmlid colapsaban al
mismo y Odoo sobrescribe el registro anterior sin decir nada. Solo se notó
al contar el catálogo cargado contra el de origen: 70 donde debían ser 77.
El generador ahora asevera que no haya xmlid repetido antes de escribir.

**Estado:** 135 tests, 0 fallos, sobre instalación desde cero.

---

## La carga compartida va donde hay lugar (1-sep, v1.62)

Con las familias de acabado ya capturadas, la primera lectura de la carga
real (jun–ago 2026) dio un número imposible: **UNITECH 120%, BRUCKNER 48%**.
No es que la planta esté produciendo más de lo que puede — es que
`qb.familia.carga` repartía la carga compartida **en partes iguales** entre
las familias que pueden hacerla, y la planta obviamente no reparte así.

### Lo que el reparto parejo escondía

| | Capacidad | Carga cautiva | Holgura |
|---|---|---|---|
| UNITECH | 611,954 | **469,314 (77%)** | 142,640 |
| BRUCKNER | 563,448 | 1,635 (0.3%) | 561,813 |
| compartida | | 534,994 m/mes | |

«Cautiva» es lo que **solo** esa familia puede correr. La UNITECH arranca el
mes con el 77% comprometido: los cuatro grandes (WJ042Q22JNT160 con 149,368
m/mes, WJ053Q22JNT160 con 117,200, XJ140Q21JNT165 con 62,165 y
WN075Q66JBL205 con 52,669) suman 381,402 m/mes que no tienen a dónde más ir.

Partir los 534,994 compartidos a la mitad le cargaba 267,497 más a una
máquina a la que solo le quedaban 142,640 — de ahí el 120%. Y al mismo
tiempo le inventaba holgura a la BRUCKNER, que es peor: el cotizador la
habría usado para prometer volumen.

### El reparto nuevo

Primero lo cautivo, que no se reparte porque no hay a dónde. Lo compartido
se distribuye **en proporción a la holgura que le queda a cada candidata**.
Con eso el mismo mes da **UNITECH 94%, BRUCKNER 76%** — factible, y sigue
diciendo cuál es el cuello.

Si ninguna candidata tiene holgura se cae al reparto parejo: cuando todas
están llenas, no hay mejor criterio.

Y lo cautivo por encima de la capacidad **sí** sale arriba del 100%, a
propósito: eso no es artefacto de reparto, es trabajo que solo esa máquina
puede hacer y que no le cabe. Hay un test para cada uno de los dos casos.

### El número es una asignación, no una medición

Vale la pena decirlo en el campo y aquí: Odoo **no registra en qué máquina
corrió cada orden** — las ramas y los jets ni siquiera existen como
`mrp.workcenter`. Así que el reparto se modela. El día que se den de alta
esas máquinas y las órdenes las referencien, esto pasa de modelado a medido.

### De paso: la regla del porcentaje, ahora verificable

El SQL de las vistas puede pasar por formateo estilo printf y un `%` suelto
lo rompe. La regla se venía respetando a mano y ya se había caído una vez
por un ILIKE (por eso `excluir_refs_sql` usa `position(... in ...)`). Estuvo
a punto de caerse otra por un comentario dentro del SQL que decía «120%».
Ahora hay un test que recorre todas las vistas y lo comprueba.

**Estado:** 138 tests, 0 fallos, sobre instalación desde cero.

---

## El panel, rehecho alrededor de decisiones (1-sep, v1.63)

El panel abría con cuatro tarjetas del último mes cerrado y, debajo, cuatro
tablas de doce meses — sin que nada dijera que hablaban de períodos
distintos. Quien leía «margen de productos» y luego «clientes que más
dejan» no tenía forma de notarlo.

Se rehizo con la ventana en **año en curso** y con el orden puesto en las
preguntas que se contestan con él.

### 1. El año

Ventas, margen de productos, ociosidad acumulada y resultado, de enero al
último mes **calculado**. La ventana la manda `qb.costo.factores`, no la
fecha: la conciliación es una vista sobre el mayor y septiembre existe
desde su día 1, con ventas y sin costeo. Filtrar por fecha metía ingresos
sin su costo y el año salía inflado justo el día que alguien lo abría.

Los titulares van compactos ($108.6M) con la cifra exacta en el `title`:
nueve dígitos no se leen de un vistazo, pero hay que poder cuadrarlos.

### 2. La franja de confianza — lo más importante que faltaba

El módulo fija su propio umbral en `brecha_pct`: **bajo ±2% el modelo sirve
para decidir precios; arriba, primero hay que cerrar la brecha.** La brecha
real del año va en **28.8%** — $31.3M sin explicar sobre $108.6M de venta.

El panel enseñaba rentabilidad por cliente y por producto sin decir de qué
lado de esa raya estamos. Eso es invitar a recotizar con números que el
propio modelo declara que todavía no cuadran. Ahora la franja va **arriba**
de los márgenes, y un test comprueba ese orden.

Lo que dice cuando la brecha es alta: los márgenes sirven para **comparar
entre sí** —quién deja más y quién menos— no como cifra absoluta.

### 3. El techo, por máquina y no por centro

Es la lección de la semana, medida: acabado lee 88% y su rama UNITECH va al
94%; tintorería lee 48% y su **HTJ-1 al 81%**. El promedio del centro
invita a prometer volumen que la máquina que hace ESE artículo no puede
correr.

Las familias van ordenadas de más apretada a más libre, con el número del
centro al lado cuando difiere lo bastante como para engañar. La escala es
**de dos lados a propósito**: saturada es un techo y ociosa es dinero
parado, y las dos son malas noticias por razones opuestas.

### El resto

Resultado por mes con la pareja divergente (azul arriba, rojo abajo, cero
en gris), etiqueta directa solo en el mejor y el peor mes. Cobertura de
fijos del año. Y «qué necesita acción» ordenado por el dinero en juego, con
las familias arriba del 90% incluidas.

### Cómo se verificó

Ninguna de estas decisiones es de gusto:

- La **paleta de estado** es la validada (`good/warning/serious/critical`).
  Dos de los cuatro no llegan a 3:1 sobre fondo claro, así que la regla es
  que el color nunca va solo: cada uno viaja con icono y palabra, y quien
  no distinga los tonos lee lo mismo. Los valores van FUERA de las barras,
  en tinta de texto, para no competir con el relleno.
- **Se renderizó y se miró**, que es el paso que no se puede saltar. Ahí
  salió que con todas las máquinas en cero el panel decía «la máquina más
  apretada es TEJ_G24_D30_A al 0%» — señalando una al azar. Ahora hay una
  guardia y su test.
- La barra **acota el relleno al 100% pero dice el exceso aparte**: una
  familia al 150% es trabajo que no cabe, y taparlo sería el error que este
  panel vino a quitar.

**Estado:** 144 tests, 0 fallos, sobre instalación desde cero.
