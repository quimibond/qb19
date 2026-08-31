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
