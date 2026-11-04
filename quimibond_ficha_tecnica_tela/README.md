# Fichas técnicas de tela (`quimibond_ficha_tecnica_tela`)

Las fichas técnicas de Quimibond en Odoo 19: qué es cada tela (tejido, acabado, especificación), cómo se hace
(máquinas y datos de la tela en cada una) y si sus listas de materiales cuadran con eso. Historia por versión:
[`CHANGELOG.md`](CHANGELOG.md).

**Decisión vigente (Dirección General, 2026-11-03):** la ficha se ve en las pantallas nativas de Odoo (producto,
lista de materiales, operación, centro de trabajo) y la app «Fichas técnicas» es el concentrado. Los datos siguen
guardados en las fichas de tejido y de acabado y en los procesos por máquina, porque los leen el SGI, el costeo y la
Programación de Tintorería.

## Ruta de uso: de la idea a la venta

| # | Quién | Dónde | Qué hace |
|---|---|---|---|
| 1 | Diseño y Desarrollo | SGI, proyecto de desarrollo (C1) | Da de alta el crudo (`WJ044…H…`), el acabado en kg (`IWJ045…J…`) y la tela en metros (`WJ045…J…`); «Fichas de proceso» crea la ficha de tejido y la de acabado; Procesos propone, se corre la muestra y Producto valida. Fuera de un desarrollo: en el producto sin ficha, pestaña «Ficha técnica» → «Crear la ficha de la tela» o «Crear la ficha de tejido». |
| 2 | Todos | Fichas técnicas → Artículos → **Acabado** | La tela aparece con su estado. Se abre el producto, pestaña «Ficha técnica» (solo lectura, cada dato dice de qué ficha sale): peso, ancho y rendimiento, crudo, ruta, especificación y pendientes. |
| 3 | Cada área | Lista de materiales → operación (o máquina → «Telas que corren aquí») | Los datos de la tela en cada máquina: «Agregar los datos de la tela en esta máquina» (velocidad, merma, ancho de rama, tiempo de ciclo, kg por carga). |
| 4 | Ingeniería (Innovación) | Artículos → Rutas por familia; ficha de acabado | La ruta de la familia (una vez por crudo) y, en la ficha, lo propio de la tela: rama o jet si no son los de la familia, lavado o teñido si no es el del código, consumo de crudo. «Generar listas desde la ruta» arma la lista en kg (`I…`) y la de metros (`J`) archivadas, con tiempos, capacidad y conversión desde la ficha. |
| 5 | Cada área | Telas (filtro por área) o Pendientes por área | Corrige lo suyo; «Volver a revisar» (o la revisión de la noche) deja la tela en «Lista para activar». |
| 6 | Ingeniería | Lista de materiales | Visto bueno: activa la lista en kg (y voltea la de metros). El estado pasa a «Activa». Hasta entonces Odoo fabrica con la lista anterior. |
| 7 | Planeación y planta | Fabricación | Orden en kg (`I…`); la Programación de Tintorería asigna el jet y divide en baños; cada rollo pasa por Inspección al salir de rama y entra a «10 INSPECCIÓN». Venta en kg: el mismo `I…`. Venta en metros: la Clasificación final produce la J medida con el mismo lote. |
| 8 | Quien cambie la ficha | Producto o ficha | Si cambia un dato (peso, velocidad…), la revisión lo marca y se vuelve a «Usar los datos de la ficha». |

**Estados de la tela** (columna del concentrado): *sin revisar*; *con pendientes*; *lista para activar* (las listas
cuadran y la lista en kg sigue archivada); *activa con pendientes* (Odoo ya fabrica con ella y algo no cuadra: lo
primero que hay que ver); *activa*.

## Rutas por familia (2.23.0)

Una ruta por crudo (Artículos → Rutas por familia) y la ficha de cada tela bastan para que Odoo arme las listas. Reglas de
la tabla «rutas de proceso rev4» de Innovación (2026-07-15):

| Paso | Regla |
|---|---|
| Devanado | Todas (en WJ050, WJ053, WJ055 y WJ060 también puede ser Preparado: alternativa de la máquina) |
| Tintorería | Lavado si el color del código es NT, NN u OW; teñido en las demás (blanco óptico incluido). Mismo jet; cambian la fórmula y el tiempo. WJ032 sin tintorería (se marca en la ficha) |
| Abridora volteado | Solo la familia WD038 (paso «solo si se tiñe» o «siempre» en su ruta) |
| Abridora | Todas, aunque el tejido salga abierto (sale del jet en cuerda); abierto o tubular solo cambia la velocidad |
| Rama (termofijado) | Todas; la rama es de la familia y la tela puede llevar otra |
| Inspección | Todas |
| Clasificación final | En la lista en metros, con tiempo fijo |

«Generar listas» nunca toca una lista activa; lo generado nace archivado y se activa con el visto bueno de Ingeniería.

## Dónde se captura cada dato (2.24.0)

Cada dato tiene **un dueño**: se captura ahí y en las demás pantallas se ve con la leyenda «Dato de: …» (donde se
puede editar, es el mismo registro, no una copia). **Nada se escribe solo en las listas de materiales ni en los
centros de trabajo:** todo pasa por «Usar los datos de la ficha» (o su botón en la operación, el jet o la lista), y la
revisión de la noche avisa lo que no cuadra.

| Dato | Se captura en | Se ve en | Llega a Odoo (listas, centros de trabajo) |
|---|---|---|---|
| La tela: peso, ancho de venta, especificación y tolerancias | **Ficha de acabado** | Producto | No llega |
| El crudo en cada circular: hilos, ajustes de tejido, tela acondicionada | **Ficha de tejido** (una por circular; «Agregar circular» copia una a otra máquina) | Crudo | No llega |
| Rendimiento de tela acabada | **Nadie: sale de lo fabricado** (metros ÷ kilos) | Ficha de acabado, producto | Conversión kg → m, con botón |
| La tela en cada máquina: velocidad, merma, tiempo de ciclo, kg por carga | **Máquinas → Procesos por artículo** (también con el lápiz en la operación o en la máquina) | Operación, máquina, ficha de acabado | Tiempos de las operaciones, con botón |
| La máquina: galga, capacidad, bandas, relación de baño | **Máquinas → Configuraciones por fecha** | Centro de trabajo | Capacidad de los jets por producto, con botón (las circulares no tienen) |
| La ruta de la familia | **Artículos → Rutas por familia** | Ficha de acabado | Listas nuevas archivadas, con «Generar listas» |

**Para trabajar no hace falta saberse la tabla:** Artículos → **Pendientes por área**, filtrar el área y pulsar
**«Resolver»** en cada pendiente; abre el lugar donde se corrige (los datos de la tela en la máquina, la operación, el
jet, la lista o la ficha). También en la pestaña «Ficha técnica» del producto.

## Dónde se ve cada cosa

| Pantalla | Qué hay |
|---|---|
| **Fichas técnicas → Artículos → Acabado** | Una fila por tela acabada con ficha: peso, ancho, rendimiento, crudo, estado, pendientes y áreas, en verde o rojo; filtros por estado y por área, búsqueda por crudo y «Actualizar la revisión». Abre el producto. |
| **Artículos → Crudos** | Los crudos con ficha de tejido. Abre el producto. |
| **Producto → pestaña «Ficha técnica»** (la primera si tiene ficha) | **Solo lectura** (2.24.0): la ficha de acabado y la de tejido de donde sale cada bloque, con liga; la ayuda de cada campo dice la pestaña y el campo de origen. La tela (peso, ancho de venta y rendimiento), su crudo y cuántas telas lo comparten, la ruta (operaciones de sus listas en kg y en metros, duración de la lista contra la de sus parámetros), la especificación y los pendientes. Botones: «Usar los datos de la ficha», «Volver a revisar», listas, «Ficha de acabado», «Procesos de la tela». En un crudo: circulares, telas que lo usan y «Fichas de tejido». Sin ficha (código de tela, de acabado en kg o de crudo): «Crear la ficha…». |
| **Lista de materiales → operación** | Tiempo de la ficha, su cálculo y «Usar el tiempo de la ficha»; los parámetros de la tela en esa máquina y sus alternativas (lápiz para editar) o «Agregar los datos de la tela en esta máquina». |
| **Lista de materiales** | Aviso si la conversión kg → m no es la de la ficha, con botón; filtro «Conversión kg → m distinta de la ficha». Acción «Usar los datos de la ficha». |
| **Operaciones** | Filtro «Tiempo distinto de la ficha». |
| **Centro de trabajo** | Pestaña «Ficha de la máquina» (tipo, estado, configuración por fecha), «Capacidades» con la capacidad de la ficha en los jets, y «Telas que corren aquí». Acción «Usar las capacidades de la ficha (jets)». |
| **Artículos → Rutas por familia** | Una ruta por crudo con sus pasos y máquinas; «Armar rutas desde las listas» y «Generar listas». |
| **Artículos → Pendientes por área** | Cada pendiente de la revisión con su área (Ingeniería, Tintorería, Acabado, Inspección, Manufactura), operación, máquina y lista. La revisión se rehace cada noche y con «Actualizar la revisión» en Acabado. |
| **Artículos → Fichas de tejido (por circular) / Fichas de acabado (datos de origen)** | Las fichas donde se guardan los datos; el SGI valida sobre ellas. |
| **Tintorería** | Fórmulas y tiempos por fórmula y jet (módulos de José Sacramento) y «Artículos en los jets». |
| **Máquinas** | Máquinas, configuraciones por fecha, procesos por artículo (edición en masa) y comparativo. |
| **Materias primas** | Fichas de hilo, propiedades y hilos por tela y máquina. |
| **Configuración** (jefes de Fabricación) | Tipos de máquina y sus parámetros, tipos de materia prima, fibras, unidades, características, claves de codificación y Herramientas (importar desde Excel, acabados en proceso faltantes, ligado de hilos). |

## Dónde se guarda cada dato

| Dato | Modelo |
|---|---|
| Crudo: máquina, hilos, ajustes, tela acondicionada (una ficha por artículo, revisión y circular) | `ficha.tecnica.tejido` |
| Tela: rendimiento, peso, ancho, encogimiento, elongación, especificación, su producto en kg y en metros | `ficha.tecnica.acabado` (+ `ficha.tecnica.spec`) |
| La tela en cada máquina (velocidad, merma, ancho bajando de rama, tiempo de ciclo, kg por carga…) | `ficha.tecnica.proceso` (por código de artículo y máquina) |
| La máquina por fecha (galga, diámetro, capacidad y bandas de los jets, relación de baño…) | `ficha.tecnica.maquina.config` con los parámetros de su tipo |
| Revisión de las listas | `ficha.tecnica.revision` y `.pendiente` (foto, se rehace) |

## Cálculos

**Tiempo de cada operación desde la ficha** (minutos por ciclo; se pasa a la lista con un botón, nunca solo):

| Operación | Cuenta |
|---|---|
| Rama | rendimiento de tela acabada m/kg ÷ velocidad m/min (2.24.0: el rendimiento de lo fabricado ya trae la merma) |
| Inspección | rendimiento de tela acabada m/kg × 60 ÷ velocidad de inspección m/h (rollos con 1 empalme) |
| Devanado | kg de crudo por kg × rendimiento de tejido ÷ velocidad m/min |
| Abridora | igual, con la velocidad de tela tubular o abierta según sale del tejido (si no, la general) |
| Tintorería | minutos por baño: tiempo de ciclo de la tela en el jet; si no, el de la fórmula en ese jet |

Rama, inspección, devanado y abridora se multiplican por la capacidad del ciclo (1 kg si no está capturada);
tintorería no, porque un ciclo es un baño y por eso pide la capacidad del jet para el producto.

**Capacidad del jet por producto** (`mrp.workcenter._ft_regla_capacidad`, regla única de fichas): kg por carga de la
tela en el jet; si no, la banda de su rendimiento de tejido (A de 3 a menos de 7, B de 7 a menos de 11, C de 11 a 16
m/kg) topada a la capacidad general; un jet sin bandas usa la general; banda N/A o fuera de bandas, no hay. La
Programación de Tintorería tiene su copia de la misma regla (propuesta a José de usar esta).

**Conversión kg → m:** la lista en metros consume de su producto en kg metros ÷ rendimiento de tela acabada ÷ la primera para planear de la tela (la que alcanzan 9 de cada 10 lotes, del costeo; 1 si no hay dato), para que la venta llegue completa (2.24.0). La lista en metros lleva de subproductos los saldos en kg de segundas (`SALDO DE TELA TEJIDO`) y desperdicio (`SALDO TEJIDO D`), con cantidad 0 (2.24.0).

**Qué lista se toma** (revisión, pestaña del producto, ruta de la ficha de acabado): la que tiene operaciones aunque
esté archivada (las listas del flujo en kg esperan el visto bueno de Ingeniería); entre iguales, la activa; las que
dicen «no usar» van al final. Odoo fabrica con la activa.

## Modelo de producción vigente

Las secciones de versiones anteriores cuentan cómo se llegó aquí (2.6.0 y
2.9.0 hablaban de «un solo producto de acabado en kg»; 2.12.0 a 2.13.1 lo
afinaron; 2.13.2 mete Inspección en la orden en kg, como pidió la v5 del
diseño de tintorería y acabado de José Sacramento, 2026-10-21). Lo vigente:

| Orden | Operaciones | Produce | Código | Unidad |
|---|---|---|---|---|
| Tejido | Tejer | Crudo | `WJ044Q22HNT235` (H en la operación) | kg |
| **Acabado en kg** (se cierra con los rollos pesados e inspeccionados, a la ubicación «10 INSPECCIÓN») | Devanado → Tintorería → Abridora → Rama → **Inspección** | Acabado en kg | `IWJ045Q22JNT160` | kg |
| **Clasificación final** (empaque), solo para la venta en metros | Clasificación y empaque | Tela acabada | `WJ045Q22JNT160` (J) | m medidos |

- **Toda la producción va en kg hasta la Clasificación final.** El código en kg
  es el de la tela acabada con la **«I» al frente**: la DAT P-D02-01 rev. 05
  define la posición 1 como unidad (I = kilogramos, sin letra = metros). No hay
  letras nuevas en la operación.
- **Inspección es una operación de la orden en kg**: cada rollo se inspecciona
  al salir de rama, sin esperar a que termine la orden. El «I…» entra a
  inventario ya inspeccionado (ubicación 10); no hay tela sin revisar mezclada
  con la que se vende.
- **Venta en kg:** el mismo «I…» pasa de la ubicación 10 a Producto terminado
  con su empaque y se vende (96 existen; 435 líneas y $5.1 millones vendidos en
  kg en el último año). Sin orden de conversión.
- **Venta en metros:** la Clasificación final consume el «I…» en kg y produce
  la J en metros medidos, con el mismo lote. La «Conversión de unidades» de hoy
  (metros → kg) desaparece: ahora el kg va primero.
- **El teñido** (I en la operación) deja de ser producto cuando la ruta del «I…»
  lleva todas sus operaciones; tintorería es una orden de trabajo de esa orden.
- **Reglas acordadas con José Sacramento (2026-10-21)** para su módulo de
  programación y dosificación:
  - *Relación de baño:* la del artículo en el jet; si no tiene, la de la
    máquina; si no hay ninguna, se detiene la confirmación
    (`mrp.workcenter._ft_relacion_bano(product)`, siempre como número: «1:10»
    → 10). El colorante (% sobre kg) no depende de la RB. Las fichas de los
    jets traen los mismos valores que su tabla (HTJ1 = 10, HTJ2 = 7, HTJ3 = 6,
    HTJ4 = 6, HTJ5 = 7) y las mismas capacidades por banda.
  - *Tiempo de ciclo:* el `tiempo_ciclo` del artículo en el jet; si no hay, la
    fórmula × HTJ.
  - *Bandas de rendimiento de tejido:* A de 3 a menos de 7, B de 7 a menos de
    11, C de 11 a 16 m/kg (el límite pertenece a la banda de arriba). Fuera de
    bandas o banda en N/A: se bloquea y se corrige; una máquina sin bandas
    capturadas (hoy HTJ5) usa su capacidad general con aviso.
  - *Duración de rama:* kg × rendimiento ÷ (velocidad × (1 − merma/100)) con el
    **rendimiento de acabado** (la rama deja la tela en su ancho final y la
    velocidad se mide en esa tela) y, si el artículo no tiene ficha de acabado,
    el de tejido con aviso. Ejemplo real: TL/OP-ACA/06468, 477 kg dieron
    5,099 m (10.7 m/kg); con el de tejido (8.45) saldrían 4,031 m, con el de
    acabado (11.8) y merma 10 %, 5,066 m.
  - *Merma de rama:* 10 % mientras se miden de 10 a 20 corridas.
  - *Alertas de metros:* Inspección contra Clasificación final, 1 m; metros
    finales contra kg × rendimiento de acabado, un porcentaje (tolerancia de
    peso + de ancho de la ficha de acabado; 3 % sin ficha).
- **Listas de materiales a voltear:** hoy las listas de los «I…» consumen la J
  en metros («Conversión de unidades»). Con el flujo nuevo el «I…» consume el
  crudo o el teñido y la J consume el «I…» (Clasificación final); las dos no
  pueden quedar a la vez (círculo). Las voltea Ingeniería, artículo por
  artículo; el reporte «Listas de materiales por voltear» (2026-10-20) trae las
  90 listas y las cantidades nuevas.
- **En el módulo:** la ficha de acabado liga la tela acabada (`product_acabado_id`)
  y el acabado en kg (`product_proceso_id`, lo liga sola por código); los
  artículos nuevos los crea C1 en el SGI (57.160.0) y los que faltan de los
  existentes, Configuración → Herramientas → Acabados en proceso faltantes. Otros módulos leen el
  artículo en kg en la ficha, no lo arman por el código.
- **Configuración de inventario pendiente** (no es código): las rutas para que
  Ventas tome solo de Producto terminado y la operación de Clasificación final.

## Los anchos: cuál es cada campo y de dónde sale (2.24.0)

| Campo (nombre en pantalla) | Qué ancho es | Dónde se captura | Quién lo usa |
|---|---|---|---|
| **Ancho de bastidor** (ficha de tejido → Ajustes de tejido) | El bastidor de la circular (ajuste de la máquina) | Ficha de tejido | Ningún cálculo |
| **Ancho de rollo de crudo** (`ancho_rollo`, ficha de tejido → Ajustes de tejido) | El rollo de crudo que sale de la circular | Ficha de tejido | Ningún cálculo |
| **Ancho de crudo relajado** (`ancho_acondicionado`, ficha de tejido → Tela acondicionada) | El crudo acondicionado, tubular o abierto según la circular | Ficha de tejido | Rendimiento de tejido → devanado, abridora y banda del jet |
| **Ancho de rollo en rama** (parámetro `ancho_rama` «Ancho bajando de rama», Procesos de la tela → rama) | El rollo que sale de la rama | Proceso del artículo `I…` en cada rama | Ningún cálculo desde 2.24.0 |
| **Ancho de venta (especificación)** (`ancho_acabado`, ficha de acabado → Tela acabada) | El de especificación, el mismo del código (`…160` = 160 cm) | Ficha de acabado | Hoy: rendimiento de tela acabada (kg → m), peso por metro del costeo, especificación |

El ancho de rollo en rama ya no entra a ningún cálculo desde 2.24.0: la rama usa el rendimiento de tela acabada.

## El rendimiento de tela acabada (2.24.0)

**Uno solo** (Dirección General, 2026-11-03): metros de la tela acabada fabricados ÷ kilos de tela consumidos (crudo,
teñido o acabado en kg; sin engomado ni químicos) en sus órdenes terminadas de los **últimos 12 meses**. Ya trae la
merma y el peso real. Se recalcula cada noche, antes de la revisión de listas; sin órdenes, se queda el capturado.

- Lo usan la conversión kg → m de la lista en metros (× la primera para planear) y los tiempos de rama (÷ velocidad) e
  inspección (× 60 ÷ velocidad). **Nunca cambia una lista solo**: la revisión marca las que ya no cuadran y se
  corrigen con «Usar los datos de la ficha».
- Ficha de acabado → «Tela acabada»: **Especificación** (peso, ancho de venta, espesor, encogimiento y elongación con
  sus tolerancias) y **Real** (el rendimiento, de cuántas órdenes, y el peso real = 1000 ÷ rendimiento × ancho de
  venta).
- Ejemplo (agosto a octubre de 2026): WJ053Q22JNT160 rinde 10.70 m/kg; su peso real a 160 cm es 58 g/m² contra 53
  de la especificación.

El **rendimiento de tejido** (`rendimiento_tela_tejida`, ficha de tejido) sigue aparte: es el del crudo (otra tela, otro
ancho) y lo usan devanado, abridora y la banda del jet en Tintorería.

## Columnas fijas

Las columnas fijas de `ficha.tecnica.tejido` y `ficha.tecnica.acabado`
(máquina, hilos, poleas, tela acondicionada, peso, ancho, encogimiento,
elongación, rendimientos…) **no se eliminan ni se renombran**: el código de
Consolti las lee. Los renglones de características de 2.1.0 conviven con
ellas.

## Importación masiva desde Excel

Ambos wizards leen la **primera fila como encabezados** y cada fila
siguiente como un artículo distinto. El match de encabezado es tolerante a
mayúsculas/acentos/espacios (ej. "Rendimiento", "rendimiento", "RENDIMIENTO"
matchean igual). Columnas no reconocidas se ignoran — puedes incluir
columnas extra sin que falle la importación.

### Columnas reconocidas — Importar Tejido

`Artículo` (requerida), `Revisión`, `Producto Proceso` (referencia interna
o nombre del producto "Tela en Proceso", opcional), `Máquina`, `Marca
Máquina`, `Galga`, `Diámetro`, `No Agujas`, `No Alimentadores`,
`Velocidad`, `Vueltas por rollo`, `Notas`.

Tabla de especificaciones por polea/hilo: `Longitud Malla Polea1` /
`Polea2` / `Tolerancia` / `Tolerancia Unidad`, igual para `Consumo cm vta`
y `Polea Alimentación`.

Tabla de especificaciones generales: `Tensión`, `Punto Cilindro`, `Punto
Plato`, `Altura Plato`, `Ancho Bastidor`, `Estiraje`, `Ancho Rollo`,
`Peso Promedio Rollo` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad`.

Tela acondicionada: `Peso Acondicionado`, `Ancho Acondicionado`, `Espesor
Acondicionado`, `Columnas`, `Mallas`, `Elongación Carga Largo`,
`Elongación Carga Ancho` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad` (el de Peso admite tolerancia asimétrica en
texto libre, ej. "+12 / -6").

Hasta 2 hilos por columnas `Hilo1 Tipo` / `Hilo1 Título` / `Hilo1
Torsión` / `Hilo1 Pct` / `Hilo1 Lote` / `Hilo1 Proveedor` (e igual para
`Hilo2`). **`Hilo1 Proveedor` / `Hilo2 Proveedor` deben ser el nombre
exacto de un contacto ya existente en Odoo marcado como proveedor**
(`supplier_rank > 0`); si no se encuentra, la fila se importa igual pero
sin vincular el proveedor, y se reporta como aviso.

### Columnas reconocidas — Importar Acabado

`Artículo` (requerida), `Revisión`, `Artículo Tejido` (requerida — debe
existir ya como ficha de tejido), `Producto Acabado` (requerida —
referencia interna o nombre del producto, debe existir ya en Odoo),
`Rendimiento`, `Notas`.

Tabla de datos de tela acabada: `Peso`, `Ancho`, `Encogimiento a lo
Largo`, `Encogimiento a lo Ancho`, `Espesor`, `Elongación Largo`,
`Elongación Ancho` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad` (el de Peso admite tolerancia asimétrica en
texto libre, ej. "+12 / -6").

Si una fila referencia un artículo de tejido o producto que no existe
todavía en Odoo, esa fila se omite y se reporta en el resumen de avisos
al final de la importación — el resto de filas válidas sí se procesan.

## Instalación y actualización

- Depende de `mrp`, `product`, `account` y `hr`; las importaciones piden `openpyxl` en el servidor.
- Al instalar o actualizar se crean los parámetros que usan los cálculos que falten en su tipo de máquina (por clave;
  en producción ya existen). Los demás parámetros los da de alta la planta en Configuración → Tipos de máquina.
- La acción planificada «Fichas técnicas - Revisión de listas en kg» rehace la revisión cada noche; después de
  desplegar conviene pulsar «Actualizar la revisión» (Artículos → Acabado) una vez.
- La tabla `ficha_tecnica_tela` del primer modelo (retirado en 2.1.1) sigue en la base sin uso.

## Pruebas

`tests/`, registradas en `tests/__init__.py`; las corre el job `odoo-tests` del CI. Las de tiempos, revisión y
pantallas nativas comparten los datos de `tests/test_tiempo_ficha.py` (`TiempoFichaCase`, códigos ZZ931 que no existen
en producción).
