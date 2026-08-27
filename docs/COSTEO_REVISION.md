# Revisión del costeo — qb_capacidad_costeo

**Fecha:** agosto 2026 · **Compañía:** PRODUCTORA DE NO TEJIDOS QUIMIBOND (id 1)
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

## 5. Qué sigue (en orden de impacto)

1. **Landed cost de importados.** Clasificar 502.03.xx y 504.01.0035 y
   repartirlos sobre el valor importado. Cierra $963,597/mes de fuga y
   corrige el producto que hoy se ve más barato de lo que es.
2. **Conciliar la MP contra 501.01.01.** Comparar mes a mes la MP explotada
   contra el costo primo consumido y aplicar el factor de ajuste (merma +
   variación de precio) al costo unitario. Cierra el +$1.0M/mes.
3. **Capturar `capacidad_normal` por centro** y dividir el pool fijo entre
   ella, mandando la ociosidad al P&L. Estabiliza el costo unitario y alinea
   el motor con la vista de ociosidad y con el README.
4. **Sacar el precio del costo.** Repartir operación por un driver (kg
   vendidos, pedidos, líneas) en vez de % del precio.
5. **Costear por ruta real** usando los workorders que ya existen, en lugar
   del split 67/33 de planta.
