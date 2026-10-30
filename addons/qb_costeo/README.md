# qb_costeo — Costeo Quimibond (segunda generación)

Motor de costeo que sustituyó a `qb_capacidad_costeo` (desinstalado y retirado
del repo el 2026-10-22). Diseño completo en
`docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`. Esta es la
**Fase 1**: tarifas por centro, períodos con compuerta y validación de datos.
El costo por producto, la conciliación y el cotizador llegan en las versiones
siguientes; mientras, corre en paralelo con el módulo anterior sobre la misma
base.

## La idea

> El módulo calcula la tarifa ($/h) de cada centro a partir del mayor y de la
> capacidad normal; Odoo la aplica en las órdenes; el módulo solo agrega lo
> que Odoo no ve.

Por cada centro directo y período:

```
pool_fijo     = fijo del centro en el mayor (suavizado N meses)
                + su parte de la nómina de fábrica (por sueldos de RH)
                + su parte de los centros indirectos (por horas, nómina o partes iguales)
pool_variable = energía y consumibles del centro (mes real)
tarifa_fija   = pool_fijo ÷ horas normales        (calendario × eficiencia, o capturada con motivo)
tarifa_var    = pool_variable ÷ horas reales      (por orden: sus órdenes de trabajo; sin ellas, estándar de la receta × producido)
tarifa        = tarifa_fija + tarifa_var
ociosidad     = pool_fijo − tarifa_fija × horas reales       → al resultado del mes (IAS 2)
op_pct        = administración y ventas ÷ ventas (suavizados)
```

Al **cerrar** el período, la tarifa se escribe en «Costo por hora» de los
centros de trabajo marcados con *Publicar tarifa*. Odoo costea con ella las
órdenes del mes siguiente.

## Compuerta de cierre

Un período no cierra si:

- no se ha calculado;
- algún centro directo no tiene horas normales o no tuvo horas reales ni
  estándar en el mes;
- hay más de la tolerancia en cuentas de resultados sin clasificar;
- algún producto de los N principales del mes (por venta) tiene una
  validación abierta.

Las razones se ven en el formulario («Qué impide cerrar»). Reabrir exige
motivo y queda en el chatter.

## Validaciones

`qb.producto.validacion` corre semanalmente (y a mano en Configuración →
*Validar recetas y precios ahora*) sobre los productos con receta activa:

| Regla | Dispara cuando |
|---|---|
| `precio_cero` | un componente con unidad de peso cuesta 0 (el agua se excluye) |
| `precio_fuera_banda` | el costo promedio se aleja más del % de banda de la última compra |
| `receta_implausible` | una línea de colorante/auxiliar pasa del % máximo del peso, o todas juntas pasan del total |

Una fila se cierra sola cuando deja de disparar, se acepta con motivo, y se
reabre si el valor cambia más de 10 %.

## Horas y peso por producto

`qb.producto.horas` dice cuántas horas de cada centro consume una unidad y
de dónde sale el número (orden de preferencia en `models/horas.py`): manual,
pesaje, órdenes de trabajo, estándar de la receta, **ficha técnica**,
hermano, galga, promedio del centro y configuración del centro. La fuente
«ficha técnica» (1.5.0) lee los procesos por artículo de
`quimibond_ficha_tecnica_tela` si está instalado: la velocidad de cada tela
en cada máquina del centro (m/min × 60, con merma), promedio de horas por
metro si corre en varias; cuenta como calidad alta y también dice que el
artículo pasa por ese centro. Un proceso sin velocidad no cuenta.

`qb.producto.kg` convierte metros en kilos: pesaje, manual, ficha de
acabado, legado, «se vende en kg» y, al final, el código (gramaje × ancho
por metro lineal, convertido a yardas si el producto va en yardas; solo el
gramaje si va en m²; sin peso si va en piezas).

## Flujo de tela en dos niveles (1.7.0)

Desde el diseño técnico v5.4 la tela se fabrica en dos órdenes:

```
pedido en m ─► CLASIFICACIÓN FINAL (lista «B», m; tipo 409 o 411)
                 consume IWJ053Q22JNT160 a 1 ÷ rendimiento de la ficha (0.0848 kg/m)
                 operación CLASIFICACION en EMPAQUE
               └─► por MTO: ORDEN EN KG (lista «A», «I» + código, kg; tipo 410)
                     crudo × 1.113 (merma de proceso), engomado, químicos dosificados
                     DEVANADORA → TINTORERÍA (por baño) → ABRIDORA → RAMA → INSPECCIÓN
```

**Qué receta usa el costeo.** La del flujo nuevo (tipo de operación en
`tipos_flujo_nuevo`) y, entre las demás, la de menor secuencia, como Odoo.
La lista vieja en metros con años de historia ya no se escoge por tener más
órdenes: la historia sirve para las horas medidas.

**Horas.** Cada operación se calcula como Odoo: (preparación + limpieza +
tiempo) ÷ unidades por ciclo ÷ eficiencia. Devanado, abridora, rama e
inspección van en min/kg con capacidad 1. La tintorería va **por baño**:
tiempo de ciclo de la ficha > minutos de la fórmula en ese jet > operación de
la lista, entre los kg por baño del jet para ese producto
(`mrp.workcenter.capacity`, p. ej. 600 kg en TINTORERIA 1): 210 min ÷ 600 kg
= 0.00583 h/kg. Es el costo estándar a baño lleno; un baño a medio llenar
cuesta más en las órdenes reales (lo dice la conciliación). La tela en kg
carga tintorería (con devanado), acabado e inspección; la de metros solo su
clasificación, y hereda lo demás × 0.0848 kg/m. Con operaciones en la receta
vigente, un centro sin ninguna no le carga horas propias al producto: así la
rama medida en la ruta vieja no se suma a la que hereda de la tela en kg.

**Qué pérdida va en cada lugar (una sola vez):**

| Pérdida | Dónde va | Quién la captura |
|---|---|---|
| Merma de proceso (crudo → tela en kg: devanado, teñido, rama, orillas) | Factor del crudo en la lista A (1.113 kg de crudo por kg) | Ingeniería en la lista |
| Conversión kg → m de la tela acabada | 1 ÷ rendimiento de la ficha de acabado (1000 ÷ g/m² ÷ ancho m); el costeo usa la ficha aunque la lista B diga otra cosa, y la validación `conversion_clasificacion` avisa si se aleja más de 0.5 % | Ficha técnica de acabado |
| Segundas, FE y desperdicio de la clasificación | `rendimiento` de la tela en metros: lo que sale de la clasificación a primera ÷ lo que sale a primera + merma (`vendible = producción ÷ rendimiento`) | Movimientos de la clasificación |
| Tela en kg (semielaborado) | Ninguna: rendimiento 1 («semielaborado») | — |

Lo que sale de la orden en kg (`tipos_kg`) no cuenta para el rendimiento
vendible aunque caiga en una ubicación de primera.

**Horas reales del período.** Por orden de fabricación: sus órdenes de
trabajo en el centro; una orden sin ellas aporta el estándar de su receta ×
producido (o, si su receta no pasa por el centro, las horas del producto ×
producido). Así la energía de tintorería y acabado entra a la tarifa aunque
el primer mes casi no haya órdenes de trabajo (`horas_reales_completar`).

**Bandas.** `qb.centro.banda`: una por unidad del producto de la orden
(kg/h para la tela en kg, m/h para la ruta vieja) o en horas por orden (en
tintorería cada orden programada es un baño). Sin filas, la velocidad mínima
y máxima del centro.

**Lo que Odoo capitaliza.** Odoo carga a cada orden la duración real de sus
órdenes de trabajo × el costo por hora del centro de trabajo, que es la
tarifa que el período publica al cerrar (*Publicar tarifa* en los cinco
directos). La orden en kg y la de clasificación tienen órdenes de trabajo
distintas, así que la mano de obra no se suma dos veces: la de metros recibe
la de kg dentro del costo del componente. `qb.periodo.conciliacion_mano_de_obra()`
compara, por centro, lo capitalizado contra horas × tarifa del período.

## Rentabilidad y órdenes fuera de rango (1.6.0)

Costos → Rentabilidad: **por cliente** y **por producto** (12 meses de
facturas contra el costo de cada mes; cobertura, costo del mismo mes y
calidad del costo a la vista) y **órdenes con tiempo fuera de rango** (la
banda de velocidad del centro, por unidad desde 1.7.0). Sustituyen a los reportes del módulo
anterior; las señales `cliente_pierde`, `producto_pierde`,
`tiempos_excepcion`, `familia_saturada` y `cotizacion_bajo_costo` de
`quimibond_intelligence` los leen.

## Configuración mínima

1. **Centros** (Manufactura → Costos → Configuración): código, naturaleza,
   driver, centros de trabajo de Odoo, departamentos de RH, *Publicar tarifa*.
2. **Clasificación de cuentas**: cada cuenta de resultados a su bucket; las
   de fábrica con centro y %. *Cuentas sin clasificar* lista lo que falta.
3. **Parámetros**: todos traen ayuda; los defaults sirven.
4. **Períodos**: crear el mes, *Calcular*, revisar tarifas, *Cerrar*.

## Pruebas

```
docker run … odoo -i qb_costeo --test-enable --test-tags /qb_costeo
```

`tests/test_periodo.py` (reparto, capacidad, estándar sin órdenes, compuerta,
publicación, reapertura, refs excluidas, para_cotizar),
`tests/test_validacion.py` (colorante, autocierre, aceptar, precio cero y
banda) y `tests/test_flujo_kg.py` (flujo en dos niveles con el piloto
WJ053Q22JNT160).
