# qb_costeo v2 — Costeo y cotización rediseñados desde cero

**Fecha:** 2026-10-06 · **Estado:** aprobado en lo esencial por el CEO el 2026-10-06 (decisiones en §13) · **Sustituye a:** `qb_capacidad_costeo` (19.0.1.69.4)

## 1. Qué cambia y por qué

`qb_capacidad_costeo` responde dos preguntas: *¿cuánto cuesta este producto?* y
*¿a cuánto lo vendo?* Las responde, pero con 21,000 líneas de Python, 30 modelos,
49 menús, 47 migraciones y más de diez ajustes encadenados sobre el costo
unitario. La auditoría del 2026-10-05 y el trabajo de esta semana dejaron claro
qué sirve y qué no:

| Se usa y funciona | Sin uso o dudoso |
|---|---|
| Motor de costo por capas (MP, energía, fabricación, conversión) | Cotizar desde pedido (0 de 56 cotizaciones ligadas a uno) |
| Cotizador con pisos y semáforo (56 cotizaciones en 2 meses) | Envío de cotización por correo (0 enviados) |
| Períodos cerrables (8 cerrados, 0 reaperturas) | Win/Loss (1 ganada marcada) |
| Conciliación contra el mayor | Snapshots, tendencia por centro, escenarios de turno |
| Pesos y rendimiento por producto | Ranking hora-máquina, programa mensual, producto × cliente, comparador, balance de línea, auditoría de pesos |
| Traza por lotes de la conversión absorbida (cuadra al centavo con 504.01.0099) | Fichas técnicas propias (1,839 generadas de golpe; duplican `quimibond_ficha_tecnica_tela` y DyD del SGI) |
| Historia de 12 meses y crudo hermano | Familias de máquinas (sustituidas por la historia) |

El problema de fondo no es un reporte de más: es que **el módulo calcula un
costo paralelo al de Odoo**. Odoo ya costea las órdenes de tejido (horas ×
tarifa del centro de trabajo, capitalizadas en 504.01.0099) y ahí el modelo y la
orden real empatan al centavo (WJ032: 2.98 vs 2.98). Donde el módulo reparte
una bolsa plana ($3.39/m para tintorería y acabado) es donde fallan las
cotizaciones: las telas negras y pesadas salen baratas y las naturales ligeras
caras (Bowen WK300 cotizada con pérdida; Menchaca sobrecosteada 15 puntos).

**La idea del rediseño cabe en una línea: el módulo calcula la tarifa de cada
centro a partir del mayor y de la capacidad; Odoo aplica esa tarifa en las
órdenes; el módulo solo agrega lo que Odoo no ve (administración y ventas,
capacidad ociosa, merma vendible) y concilia.** Una sola fórmula para todos los
centros, sin casos especiales, con la calidad de cada dato visible.

## 2. Lecciones aprendidas (con evidencia)

1. **Una capa por centro, no una bolsa.** Tejido absorbido por workcenter cuadra;
   la bolsa plana por metro no. Septiembre: 82 % de lo teñido es natural, así
   que el promedio castiga al natural y subsidia al negro (WK300: $3.39/m hoy
   contra ~$15/m con ciclo de color).
2. **El cotizador no puede depender del día.** Tres cotizaciones de septiembre
   tomaron pools de $1.49M, $3.20M y $3.99M según la fecha. Se cotiza con el
   último período cerrado; el período en curso solo informa.
3. **El cierre necesita una compuerta.** La brecha mensual iba de −7.8 % a +5.8 %
   y nadie la miraba; en el año cuadra (−0.28 %). Septiembre se explicó al
   descomponerla: $761k eran metros vendidos de inventario (volumen), no error.
   La conciliación debe descomponerse en *volumen*, *promediado* y *sin
   explicar*, y la meta se mide sobre lo que queda sin explicar.
4. **Un dato dudoso rompe una cotización completa.** WK284R46ING166 lleva 0.632
   kg de colorante por kg de tela (seguramente 0.0632): la MP sale al doble.
   WK300R50HNG165 tiene `standard_price = 0`. NN040 arrastraba un 55 % de
   rendimiento por un lote de desarrollo. WC090 trae un AVCO roto ($17.46 vs
   $6.66). Las recetas, precios y rendimientos necesitan validación automática
   y una marca de calidad que llegue hasta el PDF.
5. **La calidad del costo tiene que verse.** Trece de catorce cotizaciones
   vigentes usan tejido *estimado* (promedio del centro). Hoy eso se lee solo
   en un campo técnico. Cada costo debe decir qué porcentaje es medido,
   estándar o estimado.
6. **El tiempo de orden no es tiempo de proceso.** Las fechas de las órdenes de
   tintorería son de captura; los tiempos reales vendrán de órdenes de trabajo.
   Mientras no existan, el estándar vive en la operación de la receta (BOM), y
   es Odoo quien lo aplica.
7. **Lo que no se mide se pudre.** Los pesos "estimados" (349 productos), los
   crons apagados sin aviso (push de Supabase), las pruebas que no corrían.
   Validación mensual automática modelo vs orden real en los 20 productos que
   más venden, con alerta a > 5 %.
8. **Reglas del repo que cuestan builds:** sin herencias de vistas propias,
   versión + CHANGELOG en cada cambio, tests registrados en `tests/__init__.py`,
   `check_odoo_views.py` y `check_addons.py` en CI, el SGI no se instala en CI
   (Enterprise) así que cualquier puente con el SGI va en un satélite
   `auto_install`.
9. **Nada de tablas espejo ni frontends aparte.** Las cifras viven en Odoo; los
   reportes son vistas y pivotes sobre los modelos almacenados, no tablas
   calculadas por cron que nadie abre.

## 3. Principios de diseño

- **Una fórmula.** `producción = MP + Σ horas_centro × tarifa_centro`, para
  todo producto y todo centro. Entretela, importados e inspección son centros
  con su driver, no excepciones en el código.
- **Odoo es la fuente del costo de fabricar.** El módulo *publica* la tarifa
  ($/h) en `mrp.workcenter.costs_hour` cada período cerrado; las órdenes la
  usan; la traza por lotes ya existente lleva lo capitalizado hasta la venta.
- **Toda cifra lleva fuente y calidad.** `medido` (orden de trabajo real),
  `estandar` (operación de la receta), `hermano`, `estimado` (promedio del
  centro), `manual` (capturado con motivo). El costo del producto trae el
  porcentaje de cada una; el PDF interno lo imprime.
- **Validar antes de usar.** Receta, precios de componentes, pesos y
  rendimientos pasan por reglas de plausibilidad; lo dudoso se marca y, en el
  cotizador, se ve en ámbar aunque el precio sea bueno.
- **Cierre con compuerta.** Un período solo cierra si la conciliación sin
  explicar está dentro de la meta y los 20 productos principales no tienen
  datos dudosos.
- **Pocos modelos, pocos menús, en las apps donde está la gente.** Cotizar en
  Ventas; costos en Manufactura; conciliación también visible en Contabilidad.
- **Cada reporte se justifica con un usuario.** Lo que no tenga dueño no se
  programa.

## 4. Arquitectura: cuatro módulos

```
qb_costeo               motor: centros, tarifas, costo por producto, períodos, conciliación
  depends: mrp, stock_account, account, hr, purchase, uom, mail
qb_cotizador            cotizaciones en Ventas, PDFs, piso en la línea de pedido
  depends: qb_costeo, sale
qb_costeo_sgi           auto_install: indicadores, NC automáticas, proceso, DyD (proyectos)
  depends: qb_costeo, qb_cotizador, quimibond_sgi
qb_costeo_presupuesto   auto_install: margen presupuestado, capacidad comprometida por releases
  depends: qb_cotizador, quimibond_ventas_presupuesto
```

Por qué partirlo: el CI puede instalar `qb_costeo` y `qb_cotizador` (Community);
los puentes con el SGI y con presupuesto dependen de Enterprise y solo se
prueban en el build de Odoo.sh con `--test-tags`. Si el SGI cambia, el motor no
se entera. Mismo patrón que `quimibond_sgi` y sus satélites.

Tamaño objetivo: motor ≤ 2,500 líneas de Python, cotizador ≤ 1,200, puentes
≤ 400 cada uno. Si el motor pasa de 3,000, algo se está volviendo a parchar.

## 5. El modelo de costo

### 5.1 Centros y tarifas (`qb.centro`, `qb.tarifa`)

Un **centro** es un proceso con su capacidad: TEJIDO, TINTORERIA, ACABADO,
ENTRETELAS, INSP_EMPAQUE, más los indirectos (ALMACEN, CALIDAD, MANTENIMIENTO,
LIMPIEZA) que no cargan horas y se reparten a los directos por una llave
(nómina, horas o renta).

Cada centro tiene:
- sus `mrp.workcenter` (uno o varios);
- su **driver de horas**: `workorder` (ya mide tiempos reales), `operacion`
  (tiempo estándar de la receta), `kg_ciclo` (tintorería: kg por carga ×
  ciclo por familia de color), `m_velocidad` (acabado: metros ÷ velocidad de
  rama), `fijo_por_unidad` (inspección de importados);
- su **capacidad normal** en horas/mes (calendario de Odoo × máquinas ×
  eficiencia, o capturada con motivo);
- sus cuentas del mayor (clasificación por bucket + centro + % de asignación,
  heredada de `cuenta_map`) y su parte de nómina (departamentos de RH, salario
  mensual de `hr.version`).

Por período, el módulo calcula para cada centro:

```
pool_fijo_c      = nómina + renta + mantenimiento + depreciación del centro (mayor, suavizado N meses)
pool_variable_c  = energía y consumibles del centro (mayor, mes real)
tarifa_fija_c    = pool_fijo_c ÷ horas_normales_c
tarifa_var_c     = pool_variable_c ÷ horas_reales_c
tarifa_c         = tarifa_fija_c + tarifa_var_c
ociosidad_c      = pool_fijo_c − tarifa_fija_c × horas_reales_c      (IAS 2: al resultado, no al producto)
```

Al cerrar el período, `tarifa_c` se **publica** en `costs_hour` de sus
workcenters. Odoo costea con ella las órdenes del mes siguiente. Eso convierte
el régimen híbrido de hoy (tejido absorbido, el resto en capa) en un solo
régimen: todos los centros con driver `workorder` quedan absorbidos por Odoo;
los que todavía no tienen órdenes de trabajo se costean con la misma tarifa
por el estándar de su receta.

### 5.2 Horas por unidad (`qb.producto.horas`)

Para cada producto y centro de su ruta, horas por unidad con su fuente, en
este orden:

| Fuente | De dónde | Calidad |
|---|---|---|
| `medido` | Órdenes de trabajo reales de 12 meses del producto, dentro de la banda de rendimiento del centro (hoy `rendimiento_min/max`) | alta |
| `estandar` | `mrp.routing.workcenter.time_cycle` de la receta vigente (la de más cantidad en 90 días) | alta si la confirmó Ingeniería |
| `hermano` | Mismo código salvo color o ancho (posiciones 10-13 de la nomenclatura) | media |
| `estimado` | Promedio del centro (horas reales ÷ unidades reales) | baja |
| `manual` | Capturado con motivo y vigencia | la que diga quien lo capturó |

Los semielaborados (crudo H, teñido I) heredan hacia el terminado J por la
receta: `horas_J = Σ horas de sus componentes × cantidad + horas propias`.
Esto sustituye `_conv_unit` / `_conv_rec` / `_conv_bom` y las familias de
máquinas.

Tintorería sin órdenes de trabajo (interino): `horas = (kg ÷ kg_por_carga) ×
ciclo_h(familia_color)` con la tabla de ciclos por familia de color capturada
por planta (natural/claro, medio, obscuro). Acabado: `horas = m ÷
velocidad_rama(producto o familia)`. Ambas se capturan como **operaciones de la
receta** en Odoo, no como tabla propia: así el día que existan órdenes de
trabajo, la fuente pasa de `estandar` a `medido` sin tocar nada.

### 5.3 Materia prima (`qb.producto.mp`)

Explosión recursiva de la receta vigente al precio de componente (AVCO del
producto; última compra si el AVCO es 0 o está fuera de banda), con la
importación capitalizada por landed cost (sin prorrateo propio). Igual que hoy,
pero con **validación**:

| Regla | Marca |
|---|---|
| Componente con precio 0 consumido en receta activa | `precio_cero` |
| AVCO vs última compra fuera de ±30 % | `precio_fuera_banda` |
| Colorante > 12 % del peso de tela, auxiliar > 25 %, agua fuera de la RB del centro (`tintoreria.capacidad.rendimiento.relacion_bano` si está instalado) | `receta_implausible` |
| Peso kg/u vs gramaje × ancho de la ficha fuera de ±5 % | `peso_inconsistente` |
| Rendimiento calculado con < 10,000 m clasificados | `rendimiento_insuficiente` |

Las marcas viven en `qb.producto.validacion` (producto, regla, valor, fecha,
estado: abierta / corregida / aceptada con motivo). Una marca abierta pinta el
costo como **dudoso**; el cotizador lo muestra; el cierre lo cuenta.

### 5.4 Rendimiento vendible (`qb.producto.rendimiento`)

Como hoy: PQ ÷ (PQ + FE + segundas + desperdicio) de los movimientos de
almacén en 12 meses, por producto si clasificó más de 10,000 m, si no planta;
`manual` con motivo y **vigencia** manda sobre ambos (NN040: 0.88 hasta
diciembre, luego se recalcula solo). Nuevo: el FE recuperable (resinado como
entretela) se valora a su precio de salida, no a cero; el margen real deja de
castigar lo que sí se vende.

### 5.5 Costo del producto por período (`qb.costo.producto`)

```
mp_u            = Σ componentes × precio                           (5.3)
variable_u      = mp_u + Σ_c horas_c,u × tarifa_var_c
produccion_u    = mp_u + Σ_c horas_c,u × tarifa_c
vendible_u      = produccion_u ÷ rendimiento
op_u            = op_pct × precio                                   (admin + ventas ÷ ventas, suavizado)
total_u         = vendible_u + op_u
piso_ocioso     = variable_u ÷ rendimiento                          (no bajar de aquí nunca)
piso_lleno      = vendible_u ÷ (1 − op_pct)                         (precio de equilibrio)
calidad         = % de produccion_u que es medido / estandar / hermano / estimado / manual
```

Totales del período: `× qty vendida` para todas las capas, salvo lo que Odoo
capitalizó, que se toma **trazado por lote** hasta la venta (la traza actual se
conserva tal cual). Campos de ventas, divisa, TC y precio promedio igual que
hoy (son hechos contables y cuadran contra el mayor).

Se quitan: `mp_ajuste`, `renta_contractual`, `nomina_a_operacion`,
`inspeccion_share`, `entretela_factor`, `fab_weight_share`, `capacidad_superada`
y los overrides de denominadores. Cada uno fue un parche a la bolsa plana; con
tarifa por centro no hacen falta. Si alguno resulta necesario, vuelve como
regla explícita del centro, no como parámetro global.

### 5.6 Períodos (`qb.periodo`)

`borrador → cerrado`, con reapertura con motivo, como hoy. El **cierre exige**:

1. conciliación del mes con `sin_explicar` ≤ 2 % de ventas (acumulado del año
   ≤ 1 %);
2. ninguna validación abierta en los 20 productos de mayor venta del mes;
3. validación modelo vs órdenes reales (5.8) corrida y sin desviación > 5 % en
   esos 20;
4. tarifas publicadas a los workcenters.

El cron recalcula el mes en curso a diario (solo informa); el cotizador usa el
último cerrado.

### 5.7 Conciliación (`qb.conciliacion`, vista SQL)

Por mes: ventas, gasto del mayor por bucket, costo del modelo por capa,
resultado del mayor vs resultado del modelo, y la brecha **descompuesta**:

```
brecha            = resultado_modelo − resultado_mayor
  − ociosidad     (IAS 2, deliberada)
  − volumen       (unidades vendidas − producidas) × tarifa fija → fabricación en inventario no capitalizada
  − promediado    pool suavizado − gasto real del mes
  = sin_explicar  ← la meta se mide aquí
```

Cuando todos los centros estén absorbidos por Odoo, `volumen` tiende a cero
solo (el costo de ventas ya lo trae). La vista se muestra en Manufactura y en
Contabilidad → Reportes.

### 5.8 Validación mensual modelo vs orden real (`qb.validacion.orden`)

Para los 20 productos de mayor venta del mes: costo del modelo (MP + horas ×
tarifa) contra el costo real de sus órdenes terminadas en Odoo (`mrp.production`
+ workorders). Desviación > 5 % → fila en rojo, correo al responsable del
proceso y, con el SGI, NC automática. Es la prueba de que el modelo dice lo
mismo que la planta.

## 6. Cotizador (`qb_cotizador`)

`qb.cotizacion` vive en **Ventas → Cotizaciones de costo**. Conserva lo que se
usa: producto existente o especificación nueva (gramaje, ancho, galga, familia
→ hermano o estimado), volumen mensual, moneda y TC guardado, precio objetivo y
de mercado, pisos, semáforo, margen de contribución / bruto / neto, escalera de
volumen opcional, supuestos, validez, revisiones encadenadas, PDF interno y PDF
comercial. Agrega:

- **Calidad del costo** en la cabecera: "72 % medido · 28 % estimado · 1 dato
  dudoso (receta WK284R46ING166)". Ámbar si hay dudoso aunque el precio cubra
  el piso.
- **Capacidad**: horas que pide el volumen por centro contra horas libres del
  centro (capacidad normal − utilización real − demanda comprometida por
  releases, ver §8.3). Tres estados: cabe / ajustado / no cabe.
- **Línea de pedido**: en `sale.order.line`, campos `qb_piso_lleno`,
  `qb_margen_pct`, `qb_semaforo` calculados con el último período cerrado.
  Al confirmar un pedido con una línea en rojo, aviso bloqueante salvo para el
  grupo *Autoriza precio bajo piso*; la autorización queda en el chatter con
  quién y por qué. Esto reemplaza al wizard "cotizar desde pedido" que nadie
  usó: el control llega sin que nadie tenga que abrir nada.
- **Ganada / perdida automático**: una cotización vigente cuyo producto y
  cliente aparecen en un pedido confirmado pasa a *ganada* y se liga al pedido;
  vencida sin pedido, a *vencida*. Sin botones.
- **Pendiente comercial**: una cotización presentada sin respuesta en N días
  entra al mapa de situación de la empresa como señal
  `cotizacion_sin_respuesta` (`@senal` en
  `quimibond_intelligence/models/senales/comercial.py`, una fila por
  cotización con modelo + id). `qb_obligation` está obsoleto y no se usa.

Se va: envío por correo (0 uso), wizard por pedido, comparador, Win/Loss como
menú (queda como filtro).

## 7. Menús

```
Ventas
└─ Cotizaciones de costo
   ├─ Nueva cotización                 (asistente)
   ├─ Cotizaciones                     (lista: vigentes / ganadas / perdidas / vencidas)
   └─ Costo por producto               (solo lectura, último período cerrado)

Manufactura
└─ Costos
   ├─ Costo por producto               (pivote por período / producto / cliente)
   ├─ Períodos                         (factores, cierre, reapertura)
   ├─ Tarifas por centro               (por período; botón "publicar a workcenters")
   ├─ Validación modelo vs órdenes
   ├─ Conciliación vs contabilidad
   ├─ Datos del producto
   │  ├─ Pesos y rendimiento
   │  ├─ Horas por centro              (fuente y calidad; captura manual con motivo)
   │  └─ Validaciones abiertas
   └─ Configuración
      ├─ Centros
      ├─ Clasificación de cuentas
      └─ Parámetros

Contabilidad → Reportes → Conciliación de costeo       (misma acción)
Producto (ficha) → botón inteligente "Costo"           (costo vigente, horas, validaciones)
```

De 49 menús a 17. Rentabilidad por cliente y por producto salen del pivote de
*Costo por producto* agrupado por cliente (los totales ya están por producto y
mes; la vista SQL de 438 líneas no hace falta).

## 8. Integraciones

### 8.1 Manufactura (`mrp`)
- Tarifas publicadas a `mrp.workcenter.costs_hour` al cerrar período.
- Tiempos estándar como `mrp.routing.workcenter` de la receta; el módulo los
  lee, no los duplica.
- Órdenes de trabajo reales → fuente `medido`. Planta ya trabaja en las
  órdenes de trabajo de tintorería y acabado, y **desde noviembre de 2026
  acabado pesa rollos en centros de trabajo** (`pesaje_rollos_tejido`,
  `mrp.weigh.roll.wizard`: un lote por rollo con su peso). Eso da dos datos
  medidos a la vez: horas por orden y **kg reales por rollo**, que alimentan
  `qb.producto.peso` con fuente `pesaje` (manda sobre nomenclatura y ficha)
  y el rendimiento vendible con kilos de verdad, no estimados.
- `mrp.eco` (PLM): un ECO aplicado sobre una receta marca el producto para
  recálculo y, si cambia la MP más de 10 %, avisa a las cotizaciones vigentes.
- `tintoreria.capacidad.rendimiento` (módulo de Consolti, raíz del repo): si
  está instalado, da kg por carga por banda de rendimiento y la relación de
  baño; si no, se capturan en el centro.

### 8.2 Ventas (`sale`)
- §6: piso y semáforo en la línea, bloqueo con autorización, ganada/perdida
  automático, liga cotización ↔ pedido.
- `res.partner`: pestaña *Costos y precios* con las cotizaciones vigentes del
  cliente y su margen neto de 12 meses (desde `qb.costo.producto`).

### 8.3 Presupuesto y releases (`quimibond_ventas_presupuesto`)
- `sgi.sales.budget.line` recibe `qb_costo_unit`, `qb_margen_pct` y
  `qb_margen_total` del último período cerrado: el presupuesto y el pronóstico
  muestran margen presupuestado, no solo venta. La revaluación del S2 lo
  recalcula.
- `qb.release` aplicado → demanda neta por producto y semana → **horas
  comprometidas por centro** (`qb.capacidad.comprometida`, vista SQL). El
  cotizador descuenta esas horas de la capacidad libre. Un release que exceda
  la capacidad normal de un centro se marca en el release (`qb_capacidad_ok`).
- Precio de lista vs piso lleno: la brecha de precio que ya calcula presupuesto
  (`price_gap`) gana una tercera columna, *piso lleno*, y una alerta cuando la
  lista queda abajo del piso.
- Proyectos del presupuesto (líneas sin producto, con gramaje y ancho): se
  cotizan con el camino de especificación nueva y el costo vuelve a la línea.

### 8.4 SGI (`quimibond_sgi`)
- **Proceso**: P-Cxx *Costeo y precios* en `sgi.process` con actividades por
  puesto: Contabilidad clasifica cuentas nuevas (mensual), Ingeniería confirma
  tiempos estándar y recetas marcadas (mensual), Dirección cierra el período
  (mensual), Ventas revisa cotizaciones vencidas (semanal). Las actividades se
  ejecutan desde *Mis pendientes* como las demás del SGI.
- **Indicadores** (`calc_mode` propio, patrón `_calc_<modo>`):
  - CO-01 margen neto real del mes (% de ventas);
  - CO-02 conciliación sin explicar (% de ventas);
  - CO-03 pedidos confirmados bajo piso lleno (conteo y $);
  - CO-04 cobertura de costo medido (% de la venta con horas `medido` o
    `estandar`);
  - CO-05 desviación modelo vs órdenes (máxima de los 20 principales).
  Con `nc_on_red` en CO-02 y CO-05.
- **NC automáticas** (`quality.alert.sgi_auto_create`), fuentes
  `costeo_receta_implausible`, `costeo_desviacion_orden`,
  `costeo_cierre_bloqueado`.
- **Diseño y desarrollo (proyectos)**: una cotización de especificación nueva
  se liga al proyecto FT (`project.project.sgi_is_ft`): toma gramaje, ancho,
  precio objetivo y volumen de la solicitud (`sgi_dev_*`), y al transferir a
  producción (etapa 8.3.6) la cotización pasa a producto existente con las
  horas estándar que capturó el desarrollo. Los lotes de desarrollo se marcan
  en la orden (`qb_es_desarrollo`) y **no entran** al rendimiento ni a las
  horas medidas; se acaba el caso NN040.
- **Revisión por la dirección**: los cinco indicadores entran al paquete de
  revisión del SGI sin trabajo adicional.

### 8.5 Contabilidad y RH
- Clasificación de cuentas por bucket y centro, con el cron diario que ya
  existe. Recomendación aparte: distribución analítica por centro en las
  cuentas 501/504 para que el mayor diga el centro sin tabla propia; el módulo
  la usa si existe y cae a la clasificación si no.
- Nómina por centro desde `hr.version` y departamentos, como hoy.

### 8.6 Fichas técnicas
Ninguna propia. Se instala `quimibond_ficha_tecnica_tela` (Consolti; decisión
del CEO 2026-10-06) y el peso y el rendimiento m/kg se leen de ahí
(`ficha.tecnica.acabado.peso_acabado`, `rendimiento_tela_acabada`,
`ficha.tecnica.tejido.velocidad` como velocidad de tejido). Orden de
precedencia del peso: `pesaje` (rollos reales) > `manual` > `ficha` >
`nomenclatura`. `qb.producto.ficha` y sus 1,839 registros se
archivan en la migración; lo que valía (gramaje, ancho, rendimiento) se copia a
`qb.producto.peso` con fuente `ficha_legada`.

## 9. Modelos

| Modelo | Tipo | Sustituye a |
|---|---|---|
| `qb.centro` | Model | `qb.costeo.centro`, `qb.turno.config`, `qb.producto.ruteo`, familias |
| `qb.cuenta.clase` | Model | `qb.costeo.cuenta.class` + `cuenta_map` |
| `qb.parametro` | Model | `qb.costeo.factor.config` (≤ 15 claves, cada una con `help`) |
| `qb.periodo` | Model + mail.thread | `qb.costo.factores` (≈ 25 campos en vez de 70) |
| `qb.tarifa` | Model | nuevo (centro × período) |
| `qb.producto.peso` | Model | el actual, más rendimiento manual con vigencia |
| `qb.producto.horas` | Model | `_conv_*`, familias, ruteo |
| `qb.producto.validacion` | Model | `qb.peso.auditoria`, `qb.workorder.excepcion` |
| `qb.costo.producto` | Model | el actual (≈ 40 campos) |
| `qb.absorcion.traza` | AbstractModel | el actual, sin cambios |
| `qb.conciliacion` | vista SQL | la actual, con la descomposición |
| `qb.validacion.orden` | Model | nuevo |
| `qb.capacidad` | vista SQL | `qb.capacidad`, `qb.ociosidad`, `qb.balance`, `qb.rh.centro` en una |
| `qb.cotizacion`, `qb.cotizacion.tramo` | Model | los actuales |
| `qb.cotizador.wizard` | Transient | el actual, sin especificación duplicada |
| `qb.capacidad.comprometida` | vista SQL | nuevo (puente presupuesto) |

Dieciséis modelos contra treinta. Sin snapshot, panel, ficha, familia, carga,
balance, rentabilidad por cliente/producto, producto × cliente, mensual,
excepciones, escenario de turno, comparador, wizard por pedido ni recálculo
por rango (el recálculo es un botón del período).

## 10. Qué se conserva del código actual

Se copia, con sus pruebas, lo que ya demostró cuadrar:
- `absorcion_vendida.py` completo (traza por lotes, historia de 12 meses,
  `productos_con_conversion`);
- la explosión de MP con receta vigente de 90 días (`_last_mo_bom_map`,
  `_mp_breakdown`);
- el cálculo de rendimiento vendible y su captura manual;
- `para_cotizar`, `_pisos`, `semaforo_for` y `explain_quote_html` (el desglose
  explicado es lo mejor del cotizador);
- `cuenta_map` (clasificación y refresco diario), `excluir_refs_sql`;
- la lógica de ventas por producto desde el mayor (`ventas_total`, divisa, TC);
- los dos PDFs;
- los tests de esas piezas (de los 167, unos 90 aplican; se reescriben contra
  los modelos nuevos).

## 11. Plan de implementación

Sin fechas: cada fase tiene una compuerta y no se pasa a la siguiente sin
cumplirla. Todo en rama de desarrollo → `main` (staging) → `quimibond`.

### Fase 0 — Decisiones y datos (sin código)
- CEO decide los puntos del §13.
- Planta entrega: ciclos por familia de color y kg por carga en tintorería,
  velocidad de rama por producto o familia, y confirma las bandas de
  rendimiento por centro.
- Ingeniería corrige las recetas que ya sabemos mal (WK284R46ING166,
  WK300R50HNG165 sin precio) y Contabilidad el AVCO de WC090.
- Se capturan como operaciones de receta en Odoo (tintorería y acabado) los
  tiempos estándar de los 40 productos que más venden. Como planta ya está
  implementando las órdenes de trabajo en esos dos centros, las operaciones
  de la receta son las mismas que usarán las órdenes: no es trabajo doble.
- Se instala `quimibond_ficha_tecnica_tela` en staging y se cargan las
  fichas de esos 40 productos.
- **Compuerta:** tabla de ciclos firmada por planta; recetas corregidas;
  fichas de los 40 principales cargadas.

### Fase 1 — `qb_costeo` 1.0 en paralelo
- Centros, cuentas, tarifas, horas por producto con sus cinco fuentes, MP con
  validaciones, rendimiento, período con compuerta, conciliación descompuesta,
  validación vs órdenes. Sin cotizador todavía.
- Corre **en paralelo** con `qb_capacidad_costeo` sobre la misma base dos
  meses (septiembre y octubre, ambos cerrados en el viejo). Reporte de
  diferencias por producto: ninguna > 5 % en los 20 principales sin
  explicación escrita.
- **Compuerta:** conciliación `sin_explicar` ≤ 2 % en los dos meses; validación
  vs órdenes ≤ 5 % en los 20 principales; cobertura de costo medido o estándar
  ≥ 80 % de la venta.

### Fase 2 — `qb_cotizador` 1.0
- Cotización, asistente, PDFs, calidad del costo, capacidad, piso en línea de
  pedido con autorización, estados automáticos, pendiente comercial.
- Migración: las 116 cotizaciones actuales se copian como *histórico* (solo
  lectura, con su revisión y factores de origen); las 14 vigentes se recotizan
  en el nuevo y se comparan con las del viejo.
- **Compuerta:** Jessica y el CEO cotizan dos semanas solo en el nuevo; cero
  casos en que falte algo del viejo.

### Fase 3 — Puentes
- `qb_costeo_sgi`: proceso, cinco indicadores, tres fuentes de NC, liga con
  proyectos FT, lotes de desarrollo excluidos.
- `qb_costeo_presupuesto`: margen en presupuesto y pronóstico, horas
  comprometidas por releases, piso en la brecha de precio.
- Pruebas en Odoo.sh con `--test-tags /qb_costeo_sgi,/qb_costeo_presupuesto`.
- **Compuerta:** indicadores con dos meses de medición; un release aplicado
  mostrando capacidad comprometida.

### Fase 4 — Cambio de régimen y retiro del viejo
- Publicar tarifas a los workcenters de tintorería y acabado; Odoo empieza a
  capitalizarlos. La línea *volumen* de la conciliación debe caer.
- Desinstalar `qb_capacidad_costeo`: migración que archiva sus tablas
  (`qb_costeo_legacy_*`) por 12 meses y luego las borra; `tools/no_bump.txt`
  y el README del repo se actualizan.
- **Compuerta:** un cierre completo (período, indicadores, revisión por la
  dirección) sin tocar el módulo viejo.

### Fase 5 — Órdenes de trabajo y pesaje en tintorería y acabado
- Corre en paralelo con las fases 1 a 4, es proyecto de planta: acabado
  arranca con pesaje de rollos en centros de trabajo en noviembre de 2026 y
  tintorería está en implementación. Cuando lleguen tiempos y pesos, la
  fuente pasa sola de `estandar` a `medido` y el indicador CO-04 lo muestra.
- Condición de diseño para que esto funcione sin tocar código: las órdenes
  de trabajo deben capturarse en los mismos workcenters que las operaciones
  de la receta (§5.2), y el pesaje debe dejar el peso en el lote del rollo.

## 12. Pruebas y CI

- `qb_costeo` y `qb_cotizador` con suite completa en CI (`odoo:19.0` local y
  GitHub Actions): tarifas por centro con datos sintéticos, cinco fuentes de
  horas y su orden, validaciones de receta, compuerta de cierre, conciliación
  con los tres componentes, piso en línea de pedido con y sin autorización.
- Puentes: tests en el build de Odoo.sh de la rama con `--test-tags`.
- `tools/check_addons.py` exige CHANGELOG por versión en los cuatro módulos
  desde la 1.0.
- Un test de **no regresión numérica**: fixture con los 20 productos
  principales de septiembre de 2026 y sus costos esperados; cualquier cambio
  del motor que los mueva más de 1 % tiene que actualizar el fixture a
  propósito.

## 13. Decisiones del CEO (2026-10-06)

| # | Pregunta | Decisión | Efecto en el diseño |
|---|---|---|---|
| 1 | ¿Órdenes de trabajo en tintorería y acabado? | **Sí, ya se están implementando.** Acabado pesa rollos en centros de trabajo desde noviembre de 2026 | El estándar de la receta es interino y corto; la Fase 5 corre en paralelo; el pesaje alimenta pesos reales (§8.1) |
| 2 | ¿Alguien abre los diez reportes? | **Nadie** | Se van los diez; no se migran sus tablas |
| 3 | ¿Instalar `quimibond_ficha_tecnica_tela`? | **Sí** | §8.6; las fichas propias se archivan |
| 4 | ¿Centro como distribución analítica en contabilidad? | Pendiente de explicar (ver abajo) | La Fase 1 arranca con la clasificación de cuentas por centro, como hoy; la analítica es mejora de la Fase 4 si Contabilidad la adopta |
| 5 | ¿Quién autoriza precio bajo piso? | **Solo el CEO** | Grupo `qb_cotizador.group_autoriza_bajo_piso` con un solo miembro; la autorización queda en el chatter del pedido |
| 6 | Meta de conciliación | **La recomendada** | ±2 % mensual sobre lo no explicado, ±1 % acumulado del año |
| 7 | Nombre | Indistinto | `qb_costeo`, `qb_cotizador`, `qb_costeo_sgi`, `qb_costeo_presupuesto` |
| — | `qb_obligation` | **Obsoleto** | Sin integración; el pendiente comercial va al mapa de situación (§6) |

**Sobre la pregunta 4, en llano.** Hoy el módulo adivina a qué centro
pertenece cada gasto con una tabla que se mantiene a mano: "la cuenta
504.03 es 60 % tintorería y 40 % acabado". Odoo tiene *cuentas analíticas*:
una etiqueta que Contabilidad pone en cada factura de gasto al capturarla
("esta factura de luz es de tintorería"). Si Contabilidad etiqueta así los
gastos de fábrica, la tabla desaparece y el centro lo dice el mayor. Es más
trabajo de captura para Contabilidad y menos para quien mantiene el costeo.
No bloquea nada: el diseño funciona con la tabla y mejora con la etiqueta.
Decisión pendiente con Contabilidad, no con el CEO.

## 14. Riesgos

| Riesgo | Mitigación |
|---|---|
| Planta no entrega ciclos ni velocidades | Fase 1 arranca con `estimado` para tintorería y acabado (igual que hoy) y CO-04 muestra la cobertura baja; el paralelo sigue valiendo |
| Publicar tarifas a workcenters cambia la valuación de inventario | Se publica primero en staging; Contabilidad revisa el primer mes de asientos 504.01.0099 por centro antes de producción |
| Dos motores en paralelo confunden a Ventas | Ventas no ve el nuevo hasta la Fase 2; el paralelo es de Dirección y Contabilidad |
| Las 116 cotizaciones históricas se pierden | Se migran como histórico de solo lectura con su PDF regenerado |
| El motor vuelve a crecer a parches | Límite de líneas en `check_addons.py` para `qb_costeo` (3,000) y regla: toda excepción al costo es una regla del centro con `help`, nunca un parámetro global |
