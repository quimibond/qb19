# qb_capacidad_costeo — Capacidad, Costeo, Ociosidad y Cotizador

Módulo **read-only y no invasivo** para Odoo 19 (Odoo.sh). Lee los modelos
nativos (`mrp.*`, `hr.*`, `account.*`, `product.*`, `stock.*`, `sale.*`,
`resource.*`) **en vivo** y, con tablas de configuración propias, calcula
capacidad por máquina, balance de línea, costo real por producto, costo de
ociosidad y cotizaciones. **Nunca escribe ni extiende modelos nativos.**

Es un módulo aparte de `quimibond_intelligence` (el sync a Supabase): no toca
su lógica ni su versión de manifest.

## Modelos

### Configuración (el usuario los llena en la UI — menú Configuración)

| Modelo | Qué captura |
|---|---|
| `qb.costeo.centro` | Catálogo de centros de proceso: naturaleza, driver (peso/largo), **workcenter_ids** (M2M a `mrp.workcenter`), departamentos RH, throughput nominal, capacidad normal (IAS 2), renta contractual, patrón de órdenes (fallback pre-workcenter), y el **régimen de costeo**: capa mensual o absorción por workcenter con su fecha de corte |
| `qb.costeo.cuenta.class` | Clasificación de cuentas: cuenta o patrón LIKE → bucket (mp/energia/mod/overhead_fab/depreciacion/arrend_maquinaria/**importacion**/operacion/ventas/no_costeo), variable/fija, **es_renta**, centro, driver, % |
| `qb.costeo.factor.config` | Parámetros globales por key: `fab_weight_share` (0.67), `smoothing_months` (12), `production_window_months` (3), `m_per_kg_default`, `energia_por_kg` (0=auto), `op_pct_override` (0=auto), `weeks_per_month`, `entretela_overhead_extra_mxn`, `denominador_kg_override`, `denominador_m_override` |
| `qb.producto.peso` | Override de peso/unidad y kg↔m por producto (manual > cvu > ref_gramaje > bom > odoo_weight) |
| `qb.producto.ruteo` | Producto/categoría/patrón → familia de costeo (tela / entretela tejida / entretela carda / importado / subproducto) + centros de su ruta |
| `qb.turno.config` | Turnos manuales por centro (fallback mientras no haya `resource.calendar` vía workcenters) |

### Vistas SQL read-only (siempre en vivo, `_table_query`)

| Modelo | Qué muestra |
|---|---|
| `qb.costeo.cuenta.map` | Cuenta → mejor clasificación (cuenta específica > patrón más largo) |
| `qb.capacidad` | Por workcenter: horas de calendario real × eficiencia, producción prom. (workorders), capacidad, utilización, horas libres, throughput real vs nominal |
| `qb.balance` | Por centro, en **metros-equivalentes**: capacidad vs producción, cuello de botella (= techo de planta) |
| `qb.rh.centro` | Dotación, horas y costo MOD/hora (desde sueldos de `hr.version` y desde GL — ambos) |
| `qb.ociosidad` | Costo fijo del centro × (1 − utilización) = capacidad hundida (IAS 2); fijo unitario a capacidad normal vs a producción real |
| `qb.workorder.excepcion` | Operaciones con rendimiento (cantidad ÷ horas registradas) fuera de la banda sana. Con tarifa por hora activa el tiempo es dinero: un timer desbocado le carga al producto horas que no trabajó. Mira siempre la duración REAL, nunca `duration_expected` — hay rutas con minutos capturados donde van horas |
| `qb.costo.conciliacion` | **El control de calidad del costeo**: mes a mes, ventas y costo del modelo contra el mayor — gasto fuera de costeo, gasto sin clasificar, resultado contable vs. resultado del modelo y la **brecha** en monto y % de ventas. Verde bajo ±2%. Ver `docs/COSTEO_REVISION.md` |

### Motor de costeo (stored, por período)

| Modelo | Qué guarda |
|---|---|
| `qb.costo.factores` | Los factores del mes: pools GL suavizados, denominadores kg/m (capacidad normal) y producción real, utilización, **fabricación no absorbida** (ociosidad IAS 2), factor $/kg y $/m, energía $/kg, op %, factor entretela, renta contractual vs. GL sustituido, ajuste de MP contra el costo primo, factor de importación, **cobertura del pool** — trazabilidad completa |
| `qb.costo.producto` | Costo por capa por producto: MP (BOM recursiva a último costo), energía, fabricación híbrida, operación; márgenes de contribución, bruto y neto en $/u, % y **total del período**; **contribución por hora-máquina**. Trae además el dinero real del mes: `ventas_total` (facturado en pesos, cuadra contra el estado de resultados), los totales por capa (`mp_total` … `costo_absorbido_total` = costo de lo vendido) y el precio **en la divisa original** (`divisa_id`, `precio_prom_divisa`, `ventas_total_divisa`, `tc_prom` = TC efectivo de las facturas) |
| `qb.cotizacion` | Cotizaciones guardadas con supuestos (para comparar antes/después). El wizard es una **calculadora viva**: los resultados (costo por capa, pisos, contribución, capacidad) se recalculan al instante al cambiar producto/volumen/precio/margen; el botón solo guarda el escenario |
| — claridad de términos | **Glosario único** (`models/glosario.py`) visible en el wizard, en la cotización guardada y en el PDF: precio objetivo, precio de mercado, TC, márgenes bruto/neto/contribución, pisos, capacidad, ociosidad, semáforo. Toda cifra indica su moneda (MXN vs divisa); el precio objetivo capturado en divisa muestra su espejo `= en MXN` con el TC del día |
| — comparativa | Pestaña **«¿A cuánto lo vendo hoy?»** (`comparativa_html`, snapshot en la cotización y en el PDF): precio promedio real de los últimos 12 meses **cliente por cliente** (en MXN vía `aml.balance` — las facturas en USD salen en pesos reales) con contribución % y margen neto % al costo VIGENTE, + **otras presentaciones del mismo artículo** por nomenclatura (prefijo `I` = venta en kg, ej. WJ038Q22JNT160 ↔ IWJ038Q22JNT160; sufijo ` I` = importado) con el margen de cada una a su precio actual y el equivalente $/m de la versión en kg |
| — escalera de volumen | **Precios estandarizados por tramo** (`qb.cotizacion.tramo`, tramos ½×/1×/2×/4× del volumen — múltiplos y descuento editables en Configuración: `escalera_multiplos`, `escalera_desc_doble` default 3% por duplicación). Dos reglas duras: el precio nunca baja del **piso a planta llena** y la **contribución total $/mes nunca baja** al crecer el tramo (si el descuento la bajara, el precio se ajusta arriba). Cada tramo trae margen neto, contribución $/mes, semáforo y chequeo de capacidad; el wizard la muestra como tabla visual (tramos en columnas, barra de contribución), la hoja interna la imprime completa y el **PDF del cliente solo ofrece los tramos que caben en capacidad** (tabla «Precios por volumen», sin márgenes) |
| `qb.costeo.snapshot` | Foto mensual de capacidad/ociosidad por centro (tendencia) |

## Fórmulas

```
MP/u        = explosión recursiva de BOM al ÚLTIMO costo de compra (fallback avg)
              × mp_ajuste  (= costo primo del mayor ÷ MP modelada de lo
                vendido, misma ventana). La receta no lleva merma ni
                rendimiento real ni variación de precio; el mayor sí. Solo
                aplica a producto nacional, con banda de cordura [0.5, 1.5]
              aduana: por default NO se prorratea (`importacion_driver` =
                "landed"). El pedimento ya sabe a qué embarque pertenece: se
                captura con el landed cost de Odoo sobre la recepción y cae en
                los productos que lo causaron. El módulo solo MIDE cuánta
                aduana se quedó en resultados. Con driver "compras" se
                prorratea sobre el valor comprado a proveedor extranjero y el
                recargo entra en la HOJA comprada — el hilo importado carga su
                aduana y la receta la lleva a la tela
              importados (' I') sin costo propio → gemelo nacional
              subproductos (SALDO/DESPERDICIO) = $0
energía/u   = energia_por_kg × peso   (importados: 0)
fab/u       = híbrida:  tela en m  → kg/m × factor_kg + factor_m
                        tela en kg → factor_kg + m/kg × factor_m
              factor_kg = ws × pool_fab / kg_producidos     (ws = fab_weight_share)
              factor_m  = (1−ws) × pool_fab / m_producidos
              entretela carda → factor propio $/m (su MOD + renta ÷ sus metros)
              entretela tejida → factor_kg (tejido+tint) + factor entretela
              importados y subproductos NO cargan fabricación
op/u        = op_rate × costo_producción   (REPORTE: op_rate = pool de
                operación ÷ costo de producción de lo vendido). Repartirlo
                sobre el PRECIO hacía que vender con descuento «abaratara» el
                producto, y dejaba en $0 la operación de lo no vendido.
                Reversible a % sobre ventas con el parámetro `op_driver`
              op_pct × precio  (COTIZADOR: para el piso a planta llena sí es
                lo correcto — resuelve qué precio deja cubierta una operación
                que es % de la venta)

costo_variable  = MP + energía            → margen de CONTRIBUCIÓN
costo_absorbido = variable + fab + op     → P&L / piso a planta llena

piso ocioso  = costo variable
piso lleno   = (variable + fab) / (1 − op_pct)
precio de mercado = promedio real facturado 12m (todos los clientes, MXN)

Evaluación (semáforo, márgenes, PDF cliente), en cascada:
  precio objetivo capturado → precio de mercado → piso lleno
Sin "margen meta": el ancla no es una aspiración — son los pisos (debajo
de qué no bajar) y el mercado (qué se está logrando de verdad).
```

## PDFs de cotización — DOS documentos (mejor práctica)

**1. Hoja interna de costo y precio** (`report_cotizacion`) — para decidir,
1 página, cada número aparece UNA sola vez:
- Datos generales + TC del día (si hay divisa).
- La decisión en una línea (semáforo con la acción: no tomar / solo con
  ociosidad / precio sano).
- **«Del costo al precio»**: una sola tabla-cascada MP → +energía →
  =costo variable (piso mínimo) → +fabricación → +operación % →
  =piso a planta llena → precio de mercado (prom. 12m) → ⭐ precio
  objetivo, con columna en divisa para los precios y nota corta por renglón.
- **«Qué deja el precio evaluado»** (objetivo → mercado → piso lleno): una
  línea con contribución $/%, margen bruto, margen neto y $/hora-máquina.
- Capacidad: una línea si cabe; el detalle por centro SOLO si no cabe.
- Comparativa (clientes + presentaciones m/kg/importado).
- **Anexo «¿De dónde sale cada número?»**: el desglose snapshot (BOM
  componente por componente con su última compra, peso y fuente, factores
  del GL con fórmula) + supuestos. La trazabilidad completa, pero en anexo
  — no estorbando la página de decisión.

**2. Cotización para cliente** (`report_cotizacion_cliente`) — comercial:
producto/especificación, volumen estimado, UN precio unitario en la moneda
del cliente (`precio_cliente_*`: objetivo → mercado → piso lleno)
y condiciones (IVA, vigencia). **Cero datos internos** — sin costos, sin
márgenes, sin pisos.

El botón **«✉ Enviar al cliente»** usa la plantilla
`mail_template_cotizacion_cliente`, que adjunta SOLO el PDF comercial. La
plantilla vieja (que adjuntaba la hoja interna) quedó marcada obsoleta y el
código ya no la referencia — la hoja interna con costos jamás debe salir
por correo.

## Ventanas de datos

**Política:** los dos lados de cualquier cociente se miden sobre la MISMA
ventana, y el denominador de un pool son los meses de la VENTANA — no los
meses en que la cuenta tuvo movimiento. La renta y la energía se registran al
pagarse (la renta oscila entre $506k y $1,490k contra un contrato de ~$1,065k;
la energía entre $53k y $173k según cuándo llegó el recibo), así que dividir
entre los meses con factura da el cargo por recibo, no el costo mensual.

- Gastos GL: promedio móvil `smoothing_months` (12m default) sobre los meses
  de la ventana con pólizas posteadas. Los meses **negativos** (reversos del
  cierre anual: diciembre 2025 metió +$163M de débito a cuentas de ingreso)
  se descartan de los dos lados de la división.
- Pool fabril durante una migración: la ventana arranca en la **fecha de
  corte** del centro absorbido más reciente. Promediar meses del régimen
  viejo con meses del nuevo describiría un mes que ya no existe.
- Producción: promedio `production_window_months` (3m default) de meses completos.
- Renta: **contractual fija** por centro, para TODOS los centros fabriles
  (el GL de renta se paga a saltos). Las cuentas de renta de inmueble se
  marcan `es_renta` y el motor las resta del pool mes a mes — sin esa marca
  la renta se contaría dos veces, una por el GL y otra por el contrato. El
  panel lo revisa y `qb.costo.factores` guarda las dos cifras lado a lado.
- Importación: IGI, DTA, PRV, agente aduanal y flete van al bucket
  `importacion`, que sirve para **medirlos**, no para prorratearlos. La forma
  correcta de que lleguen al costo es el **landed cost de Odoo** sobre cada
  recepción: el pedimento cae en los productos que lo causaron y una máquina
  carga el suyo en lugar de cobrárselo al hilo. La conciliación muestra
  cuánta aduana se quedó en resultados y el panel la compara contra lo
  capitalizado.
- MP: último costo de compra por hoja de BOM, convertido a MXN al FX de la
  compra, y conciliada contra el costo primo del mayor (bucket `mp`, que
  incluye los ajustes de inventario: ahí vive la merma que la receta no
  lleva). Sin cuentas en ese bucket el ajuste es 1.0 y no pasa nada.
- Capacidad: `resource.calendar` real × `time_efficiency` — nunca 24/7 asumido.
- Denominador de fabricación: **capacidad NORMAL** del centro (IAS 2), no la
  producción del mes. La misma fuente que usa `qb.ociosidad`, así que el
  motor y la vista dicen lo mismo. Lo que la producción real no alcanza a
  absorber queda en `fab_ocioso_month` y va al resultado del período, no al
  producto. Un centro sin capacidad derivable cae a producción real.
- Denominador de energía: producción **real**. Es un costo variable: con
  capacidad normal en el denominador, un mes al 60% de utilización daría una
  energía por kilo 40% baja.

## Configuración automática desde Supabase (cero manual)

**Configuración → Importar desde Supabase** (y cron semanal que además
recalcula el período): jala toda la configuración curada en la capa silver
de Quimibond Intelligence usando las credenciales que el sync ya tiene en
Odoo.sh (`quimibond_intelligence.supabase_url` / `supabase_service_key`):

| Tabla Supabase | → Modelo del módulo |
|---|---|
| `cost_center_config` (12 centros) | `qb.costeo.centro` + departamentos RH por patrón de nómina |
| `rent_lot_assignment` (5 lotes) | renta contractual por centro (Σ monto × %) |
| `workcenter_cost_config` | throughput nominal (11 kg/h) + **auto-link de workcenters** por patrón `%CIRCULAR%` |
| `overhead_account_assignment` | clasificación cuenta→centro (energía variable / overhead directo; asignadas a admin → fuera del pool fabril) |
| `costing_variable_accounts` | luz/gas/agua como variables |
| `costing_config` | `fab_weight_share` (0.67) y demás parámetros |
| `product_kg_per_unit` (2,758) + `product_uom_conversion` (769) | `qb.producto.peso` (match directo por `odoo_product_id`; overrides locales `manual` no se pisan) |

Idempotente: se puede correr las veces que sea. Workcenters nuevos que
matcheen el patrón entran solos en la corrida semanal.

## Cómo agregar un centro nuevo (cero código)

1. Dar de alta el `mrp.workcenter` en Manufactura (con su `resource.calendar`
   real y `time_efficiency`).
2. En **Capacidad & Costeo → Configuración → Centros de costo**: crear (o
   abrir) el centro y ligarle el workcenter en `workcenter_ids`. Poner su
   `std_output_per_hour` (kg/h o m/h) y driver.
3. Listo: aparece en Capacidad, Balance, Ociosidad y RH. Si sus cuentas de
   gasto ya están clasificadas, su costo fijo entra solo.

Mientras un proceso NO tenga workcenters (hoy: tintorería, acabado):
- capturar sus horas en **Turnos / capacidad manual**, y
- (opcional) su `mo_name_pattern` (ej. `TL/OP-ACA%`) para atribuir producción.
Al darlo de alta como workcenter real, la vía nativa lo sustituye sola.

## Cómo clasificar una cuenta nueva

**Configuración → Clasificación de cuentas**: agregar fila con patrón (ej.
`504.01.0099%`) o cuenta específica → bucket/centro/driver. El matching se
refresca al guardar y cada noche (cron), así las cuentas nuevas de una
familia ya clasificada entran solas. El menú **Cuentas sin clasificar**
lista las 4xx-7xx pendientes.

## Régimen híbrido: capa mensual vs. absorción por workcenter

Un centro puede costearse de dos maneras, y el módulo tiene que saber cuál
para no cobrar el mismo peso dos veces:

- **Capa mensual** — su gasto entra al pool y el módulo lo reparte con sus
  factores. Es el régimen de arranque.
- **Absorción por workcenter** — sus `mrp.workcenter` tienen tarifa por hora y
  cuenta de costos fabriles aplicados, así que **Odoo** capitaliza horas ×
  tarifa al AVCO del producto y la venta lo libera sola.

Cada centro declara su modo y su **fecha de corte**. La fecha se compara
contra el PERÍODO, no contra hoy: un centro que migró en septiembre sigue
siendo de capa en agosto, así que recalcular un mes viejo no lo reescribe con
el régimen nuevo.

Desde su fecha de corte, el centro sale del pool, de la renta contractual y de
los denominadores. Y lo que Odoo capitalizó **se resta del pool medido en la
propia cuenta de costos fabriles aplicados** (su saldo acreedor), no con un
parámetro que haya que mantener al día: si la tarifa absorbe de más o de
menos, el pool se ajusta solo.

Cuando un lado del split peso/largo se queda sin centros en capa, su share se
va a 0 automáticamente — repartirle pool a un factor que ya no tiene
denominador dejaría dinero sin absorber. El panel vigila las dos mitades del
doble conteo: centro absorbido sin cuenta clasificada, y cuenta con saldo sin
centro marcado.

**Corte vigente:** TEJIDO desde 2026-09-01 (37 workcenters CIRCULAR).

## Períodos cerrables

`qb.costo.factores` tiene estado. **Cerrado** congela el período: ni el cron
ni un recálculo manual ni un `write` suelto pueden tocar sus factores ni sus
costos por producto. Reabrir exige motivo, cuenta las reaperturas y queda en
el historial del registro.

Sin esto, el número que se presentó el mes pasado cambiaba solo la próxima vez
que alguien recalculaba, y no había forma de defenderlo.

## Conciliación contra la contabilidad

**Análisis → Costos → Conciliación vs. contabilidad.** El costeo reparte
gasto con un modelo; esta vista lo confronta contra el mayor mes a mes. Si la
brecha no está cerca de cero, el costo unitario todavía no sirve como piso de
precio — por muy detallado que se vea.

Tres caminos por los que el modelo se desvía, y los tres se ven ahí:

1. **Gasto que nunca llega a un producto** — cuentas `no_costeo` o sin
   clasificar. Parte es correcta (el costo primo se sustituye por la receta),
   parte es fuga.
2. **Sobre o sub absorción** — `cobertura_fab_pct` ya lo medía para
   fabricación; la conciliación lo cierra para el gasto completo.
3. **MP modelada ≠ MP consumida** — la receta al último precio de compra no
   lleva merma ni variación de precio. La cuenta de costo primo del mayor es
   el número duro.

El diagnóstico con datos reales de ene–ago 2026 (y la lista priorizada de qué
arreglar) está en **`docs/COSTEO_REVISION.md`**.

## Validaciones (§ pruebas)

- `costo_absorbido = MP + energía + fab + op` exacto (test).
- Totales del período aditivos y cuadrados contra el dinero real (test):
  `ventas_total − costo_produccion_total = margen_bruto_total`,
  `ventas_total − costo_absorbido_total = margen_neto_total`,
  `ventas_total − costo_variable_total = contrib_total`.
- La conciliación cuadra con el motor: su lado "modelo" es exactamente la Σ
  de `qb.costo.producto` del mes (test).
- Un período cerrado no se recalcula, sus filas no se escriben ni se borran, y
  reabrirlo exige motivo (test).
- Un centro absorbido sale del pool por exactamente su renta contractual, la
  fecha de corte respeta el histórico, y el share se apaga cuando su lado
  queda sin centros en capa (tests).
- Importados sin fabricación; subproductos MP $0 (tests).
- La renta contractual de un centro fabril mueve el pool en exactamente esa
  cantidad; el reconocedor distingue renta de inmueble de arrendamiento de
  maquinaria (tests).
- El ajuste de MP es exactamente el cociente GL ÷ modelada, se recorta a la
  banda, y solo toca al producto nacional (tests).
- Con el driver default no hay prorrateo de aduana (test). Con driver
  "compras", la aduana del hilo importado llega a la tela por la receta y no
  se aplica dos veces; `importacion_unit` es informativo y la identidad de
  capas sigue intacta (tests).
- `qb.costo.factores.cobertura_fab_pct`: Σ fab absorbida en vendidos ÷ pool
  (~90% es sano; mucho menos = revisar denominadores/clasificación).
- Parser de gramaje: solo bloques de exactamente 3 dígitos (4 dígitos =
  código de resina, p.ej. 4032/9032).
- Dedup de cantidades por TAMAÑO del grupo: tres líneas iguales son un
  triplete de facturación y cuentan una vez; dos son dos rollos y cuentan
  dos (tests).
- La explosión de receta filtra las líneas por variante: un producto no
  carga los componentes de sus variantes hermanas (test).

## Rendimiento (límites de cron de Odoo.sh)

El recálculo mensual está diseñado para caber en el límite de tiempo de los
crons de Odoo.sh con miles de SKUs:

- **Un solo query** resuelve el último costo de compra de TODAS las hojas de
  BOM (`_last_purchase_line_map`, `DISTINCT ON`) + warm-up del cache ORM.
- Las reglas de ruteo y los pesos se resuelven **una vez por corrida**
  (`_engine_ctx`) y se comparten en todo el loop, incluida la recursión de BOM.
- Los registros nuevos se crean **en lote** (un `create(vals_list)`).
- Un producto con BOM rota (ciclo, UoM inconsistente) se **loggea y se omite**
  — no tumba el cálculo mensual completo; el log reporta el conteo de errores.
- `post_init_hook`: el matching cuenta↔clase queda poblado al instalar, así
  las vistas SQL tienen datos desde el día 1.

## Crons

| Cuándo | Qué |
|---|---|
| Diario 2:30 | Refrescar matching de cuentas (plan contable nuevo entra solo) |
| Día 1 de mes | Snapshot de capacidad/ociosidad + recálculo de costeo del mes cerrado |

## Despliegue (Odoo.sh)

- Instalar con `-i qb_capacidad_costeo` (módulo nuevo — no `-u` masivo).
- No requiere que existan tintorería/acabado como workcenters: degrada con
  gracia (capacidad desde turnos config, "sin datos" en vez de romper).
- Multi-compañía: vistas SQL llevan `company_id`.
- No cambia la versión del manifest de `quimibond_intelligence`.
