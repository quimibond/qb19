# Quimibond - Presupuesto y pronóstico de ventas

Presupuesto anual de ventas por año y revisión con los cinco mercados (F-P-A28-18) y pronóstico
semanal por cliente (F-P-A28-13) del procedimiento P-A28, en Ventas →
Presupuesto y pronóstico. Salió de `quimibond_sgi` en 57.11.0 (auditoría
A-016, A-013, E-014; decisión 5 de Jose: «lo que sale del SGI va a módulos
propios, sin borrar datos»).

## Qué trae

- Modelos `sgi.sales.budget`, `sgi.sales.budget.line`,
  `sgi.sales.budget.import` y el campo `budget.analytic.sgi_sales_budget_id`.
  **Conservan el nombre técnico `sgi.*`** a propósito: cambiarlo obligaba a
  renombrar tablas, columnas, XML IDs, reglas y referencias de 1,291 líneas.
- Vistas, matriz (web_grid), reporte, análisis (4 acciones), menús, 2
  reglas por empresa, 10 accesos, la secuencia `PPV-AAAA-`, el pie de
  formato `format_map_sales_budget`, los 9 parámetros `quimibond_sgi.budget_*`,
  `price_*`, `forecast_*` y `sales_budget_alert_pct` (mismas claves) y sus
  ajustes en Ajustes → SGI → Indicadores automáticos.
- Avisos en el motor de crons del SGI (`sgi.cron`): cierre de mes (gancho
  `_sgi_monthly_close_steps`), cobertura semanal del pronóstico vigente y revaluación
  del S2 (crons `sgi_cron_forecast_coverage` y `sgi_cron_budget_revaluation`).

## Desviación contra el precio presupuestal (19.0.3.5.6, 2026-11-05)

La desviación de precio comparaba lo facturado contra el **precio de lista de
hoy**: en la REV2 2026 los renglones copiados recalcularon la lista y cualquier
cambio de lista del año salía como desviación (89 en la REV1, 342 en la REV2).
Ahora compara contra el **precio presupuestal del renglón**, en su moneda y su
base (unidad, metro o kilo); una factura en otra moneda se convierte con el
tipo de cambio del mes del presupuesto (y la marca «cruza divisas»). Es lo que
mide el VE-05. La migración recalcula el real y la desviación de los renglones.
El precio de lista queda como referencia.

## Cumplimiento al corte (19.0.3.5.4, 2026-11-05)

El «% Cumplimiento» de la ficha dividía lo capturado por los renglones entre el
presupuesto del año entero (con meses que todavía no pasan), y la ficha y la
lista lo mostraban con el widget de porcentaje, que lo multiplica por 100.
Ahora compara **venta contra presupuesto, los dos al corte** (último día del
último mes completo; 31-dic en un año cerrado): la venta es la de los equipos
de los mercados con la regla del VE-02, así que lo vendido fuera de los
renglones sí cuenta. Trae el cumplimiento por mercado. «Capturado por los
renglones» queda aparte, y el aviso de cierre de mes usa la misma venta.
Desde 19.0.3.5.5 un documento obsoleto no mide cumplimiento.

## Traer renglones de otro presupuesto (19.0.3.5.3, 2026-11-05)

Engrane → «Traer renglones de otro presupuesto»: copia a un borrador los
renglones de otros presupuestos del mismo año (de cualquier estado), con su
mercado, cliente, producto o proyecto, mes, unidad, cantidad y precio; lo que
ya existe o no cabe queda anotado en el chatter. Con esto se arma la REV2
2026 con los cinco mercados: «Nueva revisión» del Industrial aprobado y traer
los renglones de Confección (PPV-2026-004) y Especiales (PPV-2026-005).

El anti-doble-conteo («un producto no va con cliente y sin cliente») ahora es
**por mercado base**: Confección presupuesta sin cliente productos que
Industrial presupuesta por cliente, y cada uno mide su propio equipo. La
conciliación también separa por equipo.

## Cantidad facturada sin las líneas de costo y conciliación por partida (19.0.3.5.2, 2026-11-05)

El real de cada renglón sumaba también las dos líneas de costo (cogs) que Odoo
agrega a cada línea de factura: la cantidad facturada salía al triple y el
precio real a un tercio, así que casi todo el Industrial 2026 salía con
desviación de precio «grave» (−70 %). El importe facturado no cambiaba (las
líneas de costo se cancelan). Ahora el real, la precarga y la plantilla solo
leen líneas de producto; la migración recalcula el real de todos los renglones.
Además pone a los documentos que ya existían su fecha de revisión (la de
creación; en 3.5.0 quedó la del update) y su venta del año anterior.

**Conciliación de la venta del año** (engrane → «Conciliar facturado»): la
venta del año es la del VE-02 (solo cuentas de ventas: una venta de activo
fijo, un anticipo u otros ingresos ya no salen como «no presupuestado») y cada
peso cae en una sola partida, con los cinco cliente–producto más grandes de
cada una: mercado sin renglones en el presupuesto, notas de crédito y
servicios sin producto de tela, saldos y desperdicio, mes sin ningún renglón,
mismo cliente y producto en otro mes, producto presupuestado a otro cliente,
producto no presupuestado a un cliente presupuestado y cliente sin presupuesto.
El residuo es 0.

## Equipo de Industrial (19.0.3.5.1, 2026-11-05)

La migración de 3.5.0 buscó los equipos por nombre sin idioma, y el equipo 1
no se llama «Industrial» en ese idioma: los 325 renglones del Industrial 2026
quedaron sin equipo y su real en 0 (el importe y el VE-02 no cambiaron). Ahora
la migración compara el nombre en todos los idiomas; la de 3.5.1 completa el
parámetro y pone el equipo de su mercado a cada renglón, con su real y su
pedido. Al guardar Ajustes con otro equipo de mercado, los renglones lo toman
solos.

## Presupuesto anual (19.0.3.5.0, E2 paso 1, 2026-11-05)

Diseño v3, sección 2.6. **Un documento por año y revisión** (REV0, REV1…) con
los cinco mercados dentro; ya no hay un presupuesto por equipo de ventas.

- **Cabecera:** año, revisión (`revision_label` REV0/REV1, fecha), tipo de
  cambio por mes (`qb.sales.budget.rate`, pesos por dólar; «Aplicar a los 12
  meses»), objetivo (+20 %) y mínimo (+15 %) contra la venta del año anterior
  (la del VE-02, se calcula al enviar a revisión). Estado nuevo
  **reemplazado**: al aprobar una revisión, la aprobada anterior del año queda
  reemplazada (congelada, comparable). Una aprobada y una abierta por año y
  compañía. «Nueva revisión» ya no obsoleta la aprobada: el VE-02 la sigue
  leyendo hasta que se apruebe la nueva.
- **Renglón:** mercado (Industrial, Proyectos industrial, Confección,
  Proyectos confección, Especiales; sin mercado, el del equipo del cliente, y
  sin cliente, Confección); producto **o** proyecto sin código (descripción,
  gramaje, ancho; solo en Proyectos); metros y kilos (`qty_m`, `qty_kg`, con
  el rendimiento del producto o el gramaje y ancho del proyecto).
- **Precio capturable:** `price_input` en USD o MXN, por la unidad del
  renglón, por metro o por kilo. Al crear el renglón (y con «Sugerir precios
  de la lista» o «Actualizar real») se toma el de la lista del cliente con las
  reglas de siempre; sin regla o con precio basura queda vacío. Importe =
  cantidad en la base del precio × precio × tipo de cambio del mes. Un
  renglón sin precio, sin tipo de cambio o sin conversión **cuenta 0** y sale
  en rojo; aprobar lo bloquea (o se aprueba con excepción, con constancia).
  Industrial y Especiales sin cliente no se aprueban. El precio de lista
  queda como referencia y para el control de desviaciones.
- **Equipos de los mercados** en Ajustes (Industrial, Confección,
  Especiales): el real y lo pedido de cada renglón se miden en el equipo de su
  mercado. La migración los pone por nombre.
- **Plantilla e importación:** columnas MERCADO, PRECIO y MONEDA (opcionales).
  Matriz por mercado × cliente × producto × mes.
- **Cierre de mes:** compara el presupuesto aprobado del año contra la venta
  neta del VE-02 y avisa al Administrador de ventas.
- **Migración:** los renglones existentes toman el mercado del equipo de su
  documento y su precio de lista en pesos como precio presupuestal (el importe
  no cambia: el VE-02 del Industrial 2026 aprobado da lo mismo). Los
  presupuestos 2026 en borrador (Confección y Especiales) pasan a obsoletos.

Sigue (E2): proyectos ligados a Desarrollo (`qb.sales.project`), el real
`qb.sales.actual` y los reportes, el presupuesto del mes y la carga de la
REV0/REV1 del Excel.

## Comparación de precios: kilos y prefijos (19.0.3.4.1, 2026-11-05)

Primera corrida en producción (106 renglones): 60 «sin regla» y 6 «sin
cliente» que no eran tales. Corregido: un código con «I» que existe tal cual
se compara como precio por kilo aunque venga en la columna «precio / m»
(Pieles, Lear); un precio por metro cuyo código en metros no tiene regla se
compara con la regla del código en kilos entre el rendimiento (Shawmut tiene
sus reglas en los IWJ…); «FXI» se liga por las hojas «FXI CD JUAREZ» y «FXI
SANTA TERESA» (mismo cliente).

## Comparación de precios del forecast (19.0.3.4.0, 2026-11-05)

Diseño v3, sección 4.4. **Presupuesto → Comparación de precios** →
«Comparar con forecast.xlsx» (Administrador de ventas o del SGI): lee la hoja
**PRECIOS** (cliente, código, precio por m o por kg, rendimiento) y compara
cada renglón con la **lista de precios del cliente** en Odoo, en la moneda de
la lista (el Excel no dice moneda). Resultado por renglón: **igual** (menos de
0.5 %), **diferente** (con la diferencia y el %), **sin regla** (la lista no
tiene regla del producto o el cliente no tiene lista: Odoo caería al precio de
venta, que no se usa), **sin producto**, **sin cliente** (ligarlo en Hojas del
forecast). Si el forecast da precio por metro y solo existe el código en kilos
(«I…»), se compara el precio por kilo entre el rendimiento del artículo. Cada
carga reemplaza la anterior; no toca ninguna lista. Ojo con Lear: su «precio /
m» del Excel es por kilo (diseño 1.4), así que saldrá «diferente» hasta
corregir el Excel. El lector es `forecast_xlsx.parse_prices` (Python puro).

## Lector genérico por número de columna y .xls (19.0.3.3.1, 2026-11-05)

El release de Shawmut Silao («SL 3254 Release for W45 2026.csv») trae todo el
encabezado en una sola celda, así que la parte no se encuentra por nombre: el
perfil acepta la **columna de la parte y la de la descripción por número**
(Silao: 5 «Item» y 6 «Desc», fechas mes/día/año, unidad LY). El lector abre
**.xls** si el servidor tiene `xlrd` (Odoo lo trae). El de Tennessee («Quimibond
TN Release …xls») es una hoja de planeación con consumo, «Tennessee
requirements» y saldos por parte: el genérico no la lee bien; va por captura
hasta tener lector propio.

## Releases más rápidos (19.0.3.3.0, 2026-11-05)

Diseño v3, sección 2.10 (puntos 3 y 4):

- **Lector «Excel / CSV genérico (parte × fecha)»** para los clientes sin lector
  propio. Reconoce la tabla **ancha** (fechas como columnas, una fila por parte)
  y la **larga** (columnas parte, fecha y cantidad), en .xlsx o .csv (separador
  `,` `;` tabulador o `|`; un .xls viejo pide guardarlo como .xlsx). En el perfil
  van la unidad del cliente (obligatoria: MT, LY, KG, M2…), y si hace falta los
  encabezados de la parte, la OC y la descripción, y el orden día/mes de las
  fechas de texto. El Release ID es el nombre del archivo. Python puro:
  `release/generic_table.py` (pruebas en `tests_puros/`).
- **Periodo en Captura semanal y Pronóstico vigente** (filtros del buscador):
  Esta semana, Próximas 4 semanas, Próximas 13 semanas (por omisión en la
  matriz), Este mes, Próximo mes, Este año y el filtro de fecha nativo «Semana
  (periodo)» (mes, trimestre, año). Las columnas de la matriz son las semanas
  del periodo elegido, aunque estén vacías (`group_expand` lee el dominio).
- **«Confirmar sugerencias»** en el release: confirma de una vez las partes que
  ya tienen producto sugerido y anota en el chatter las que no tienen producto.
  En Partes del cliente, **«Confirmar seleccionadas»** en la lista.

## Matriz del presupuesto sin `<range>` (19.0.3.2.1, 2026-11-05)

El build de 3.2.0 falló en la matriz del presupuesto: Odoo 19 no acepta el
elemento `<range>` dentro de la columna de un grid (ni con `step` ni sin él).
La columna queda `<field name="date" type="col"/>` y el rango (semana, mes,
año) se elige en pantalla.

## Matriz semanal con catálogo de semanas (19.0.3.2.0, 2026-11-05)

- **Captura semanal** (Pronóstico → Captura semanal, Administrador de ventas o
  del SGI): vista `grid` con renglón = cliente, planta, producto y unidad del
  cliente; **columna = semana del catálogo** `qb.forecast.week` («2026-S48 ·
  23 nov», una por lunes, se crean solas); celda = cantidad en la unidad del
  cliente. Siempre muestra las próximas 13 semanas aunque estén vacías
  (`group_expand`) y abre con el filtro «Próximas 13 semanas». Al escribir se
  crea, suma o quita la semana (en cero) y queda en Cambios del pronóstico. Un
  producto nuevo para el cliente entra con «Pegar semanas desde Excel».
- Cada renglón del pronóstico lleva `week_id` (lo pone `create`/`write`); la
  migración llena los que ya existían.
- La matriz del presupuesto anual ya no lleva `step` en su `<range>` (rango año
  → columnas por mes), el atributo que Odoo 19 rechaza; no fallaba en
  producción solo porque esa vista no había cambiado.

## Sin la vista de matriz (19.0.3.1.1, 2026-11-05)

La vista `grid` de 3.1.0 tumbó el build de producción: en Odoo 19 el `<range>`
del grid no acepta `step` (la columna sale del `span`: mes → días, año →
meses), así que no hay columnas por semana. Se quita la vista; se quedan
«Pegar semanas desde Excel» y `grid_update_cell` (probado), para una matriz
semanal que se diseñe con otra columna.

## Captura semanal (19.0.3.1.0, 2026-11-05)

Para los clientes sin lector de release (diseño v3, sección 2.3), dos formas de
mantener el pronóstico vigente sin el `forecast.xlsx`:

- **Matriz** (vista «Captura semanal» en Pronóstico vigente): renglón =
  cliente, planta, producto y unidad del cliente; columna = semana (mes o año);
  celda = cantidad en la unidad del cliente. Al escribir se crea la semana, al
  dejarla en cero se quita, y todo queda en Cambios del pronóstico. Una semana
  que vino de un release se puede corregir aquí y pasa a «Captura». Para una
  semana nueva la matriz tiene que estar agrupada por cliente.
- **Pegar semanas desde Excel** (Pronóstico → Pegar semanas desde Excel, solo
  Administrador de ventas o del SGI): se elige cliente, planta y unidad, y se
  pega el bloque copiado de la hoja del cliente (código, parte y semanas; con el
  renglón de los números de semana o indicando la primera semana). El producto
  se busca por código o por la parte confirmada del cliente. No toca lo
  anterior a esta semana ni lo que escribió un release; una semana vacía del
  bloque quita lo que había (se puede apagar). El lector es Python puro
  (`release/forecast_paste.py`, probado en `tests_puros/`).

## Pedidos bloqueados siguen contando (19.0.3.0.1, 2026-11-05)

Producción bloquea solo cada pedido de venta al confirmarlo (36 de 62 pedidos
con entrega futura estaban bloqueados), así que el filtro «sin bloquear» de
3.0.0 sacaba de la Demanda pedidos vigentes (de 50 a 3 en las semanas firmes y
planeadas). Se quita; cuenta solo «con entrega abierta».

## Limpieza: un solo pronóstico, sin MPS (19.0.3.0.0, 2026-11-05)

Decisiones de Jose (2026-11-05): archivar los pronósticos viejos, desconectar
el Programa Maestro (MPS) por ahora y reordenar los menús.

- **Un solo pronóstico**: el vigente (`qb.forecast.line`). El documento por
  cliente y año (`sgi.sales.budget` con `kind = 'pronostico'`) es histórico:
  ya no se crea (solo con el contexto `qb_allow_old_forecast`, que usan las
  pruebas), no se revisa ni se aprueba, y se ve de solo lectura en
  Histórico → Pronósticos anteriores. La migración los pasa a «Obsoleto» con
  una nota en el chatter (21 en producción).
- **Releases**: «Aplicar» escribe en el pronóstico vigente (origen `release`)
  por cliente, dirección de entrega o planta, producto y semana, sin partir por
  año. Desde la primera semana del release, lo que ya no trae se quita (queda en
  Cambios del pronóstico); lo anterior y las otras plantas no se tocan. El
  perfil de release lleva **Planta** (un perfil por cliente y planta).
- **Sin MPS**: se quitó el envío al Programa Maestro (botones, cálculo de
  demanda neta, suma por cliente). La migración borra las 624 celdas que dejó
  el envío del 2026-11-02 (17:04–17:06 UTC); las que Planeación tocó a mano se
  quedan. Planeación ve la demanda en Fabricación → Pronóstico de ventas.
- **Pedidos pendientes = con entrega abierta.** Pronóstico consumido, Demanda
  por cliente y cobertura cuentan una línea de pedido solo si le queda una
  entrega sin hacer ni cancelar. Antes bastaba con «pedido − entregado > 0»:
  273 de los 324 «atrasados» de producción eran faltantes cerrados sin
  backorder o productos cambiados al surtir (2026-11-05).
  En 3.0.0 un pedido **bloqueado** tampoco contaba; **3.0.1 lo quita**: producción
  bloquea cada pedido al confirmarlo, y la Demanda perdió 47 pedidos vigentes.
- **Cobertura del pronóstico** (cron semanal): sobre el pronóstico vigente,
  semanas firmes con pronóstico sin pedido y pedidos firmes sin pronóstico, en
  una actividad al Administrador de ventas sobre el cliente.
- **Presupuesto**: la columna «Pronóstico» de la línea y la precarga leen el
  pronóstico vigente del mes (por artículo). Se quitaron la importación del
  pronóstico por Excel semanal y el campo «Código del cliente» de la línea.
- **Menús** (mismos XML IDs): Pronóstico (vigente, Demanda por cliente,
  Cambios, Releases), Presupuesto (Presupuestos, Análisis), Configuración
  (Perfiles, Partes del cliente, Hojas del forecast, Cargar forecast — solo
  administrador) e Histórico (Pronósticos anteriores). El menú 2435
  (`menu_sale_sgi_sales_forecast`, el que cita el documento F-P-A28-13 del
  SGI) ahora abre el pronóstico vigente.

## Mercado sin `res.partner.team_id` (19.0.2.0.3, 2026-11-05)

En Odoo 19 el contacto ya no tiene equipo de ventas (`res.partner.team_id` no existe; se vio
en producción antes de la primera carga). El mercado del pronóstico y de la Demanda por cliente
sale ahora del último pedido confirmado del cliente, si no de su última factura y si no del
equipo de su vendedor (`res.users.sale_team_id`); sin ninguno, «Sin mercado». Antes de esto,
crear un renglón de pronóstico con cliente fallaba. También: un cliente con varias hojas en el forecast (FXI Cd. Juárez y Santa Teresa; Saltillo y León) carga una planta por hoja en lugar de sumarlas, y entre dos productos activos con el mismo código se toma el que tiene conversión.

## Aviso del build (19.0.2.0.2, 2026-11-05)

`qb.forecast.line.conv_origen` es relacionado del producto y declaraba además su lista de
opciones; Odoo 19 la ignora y dejaba el WARNING «selection attribute will be ignored as the
field is related» en el update de producción. Se quita la lista repetida.

## El Excel se carga una sola vez (19.0.2.0.1, 2026-11-05)

Decisión de Jose: el forecast de Excel es solo la carga inicial; después el
pronóstico vive en Odoo (releases y la matriz de captura de E1b). El menú
«Cargar forecast de Excel» queda solo para el administrador del SGI.

## Pronóstico vigente y Demanda por cliente (19.0.2.0.0, E1a, 2026-11-04)

Primera parte de la entrega 1 del diseño v3
(`docs/superpowers/specs/2026-11-04-presupuesto-pronostico-ventas-v3-design.md`).

- **Artículo y unidades** (`product.product`: `qb_articulo_id`, `qb_m_por_kg`,
  `qb_ancho_m`, `qb_conv_origen`; `models/product_articulo.py`). Nada propio:
  el artículo sale de la ficha de acabado o de la regla de Dirección (con «I»
  es kilos, sin «I» es metros; «M2» al final es la versión en m²); el
  rendimiento, de la ficha (el mismo de la lista en metros), luego de su
  especificación y luego del código; sin dato no se convierte. Los kilos a
  producir son metros ÷ rendimiento ÷ primera para planear, como la lista en
  metros. Cron nocturno «artículo y conversión de productos».
- **Pronóstico vigente** (`qb.forecast.line`): una fila por cliente × planta ×
  producto × semana, sin estados, con la cantidad del cliente, en unidad de
  venta, metros y kilos. Cada cambio queda en `qb.forecast.change`. Los pedidos
  confirmados del mismo cliente, artículo y semana lo consumen (`qty_pedido`,
  `qty_neta`), aunque el pedido venga en el código en kilos.
- **Cargar forecast de Excel** (`qb.forecast.import`, lector puro
  `release/forecast_xlsx.py`): lee las hojas de cliente de `forecast.xlsx`
  (no las de resumen), con planta, OC, parte y la unidad de la hoja
  PRONOSTICO. Cada hoja se liga a su cliente en «Hojas del forecast» (se
  sugiere por nombre). Solo desde la semana elegida; quita las semanas que el
  archivo ya no trae, salvo las de un release. Probado contra el archivo real
  (983 semanas de 2026–2027, mismos totales por cliente que las hojas).
- **Demanda por cliente** (`qb.demand.line`): pedidos pendientes + pronóstico
  sin pedido, en metros, kilos de venta y kilos a producir, con los cortes de
  Planeación «Acabado: 3 semanas» y «Tejido: próximo mes» (ventana móvil). Se
  reconstruye cada mañana, al cargar el Excel y con «Actualizar». Es tabla y
  no vista SQL porque la conversión vive en Fichas (Python).
- **Grupo «Planeación (consulta)»** y menú Fabricación → Pronóstico de ventas.
- Sigue en E1b: releases por planta y lector CSV/XLS, aplicar el release al
  pronóstico nuevo, el MPS desde esta fuente, la matriz de captura y la
  migración de los 21 pronósticos viejos y del MPS del 2-nov.

## Sin el módulo anterior de costeo (19.0.1.4.0, 2026-10-16)

La sugerencia de producto y la conversión a kilos ya no leen
`qb.producto.ficha` de `qb_capacidad_costeo` (desinstalado y retirado del repo
el 2026-10-22; 19.0.1.4.1 solo corrige el comentario de `release/units.py`):
gramaje y ancho
salen del código del artículo y el rendimiento (m/kg) del peso del costeo
para los productos en metros, o del código (1000 ÷ gramaje × ancho) para los
que van en kilos. Sin `qb_costeo`, igual que antes sin la ficha: no se
convierte a kilos y no se inventa.

## Pruebas de staging (19.0.1.3.1, 2026-10-01)

Solo pruebas; el código no cambia.

- `test_ajustes_130` (`TestBudgetMinPriceException`, casos 3 y 4): cada
  línea creaba su propio presupuesto del mismo mercado y año, y el segundo
  chocaba con la regla «un presupuesto no obsoleto por mercado y año». La
  clase usa un solo presupuesto y le agrega líneas. Error de la prueba.
- `test_sales_budget` (`TestSalesBudgetLifecycle55.test_05`): esperaba
  «omitidos 1» y el chatter decía 0. Desde 1.1.0 (N2) el presupuesto se
  omite por producto + cliente + **mes** del pronóstico, y la semana del
  pronóstico era el lunes de 2040-06-03, o sea 2040-05-28 (mayo) contra la
  línea de junio. No es regresión ni dato de producción: la prueba no se
  actualizó con N2. Ahora la semana es la del 2040-06-04.

## Ajustes del CEO (19.0.1.3.0, 2026-10-01)

- **Precio mínimo plausible propio** (producto, pestaña Ventas; categoría,
  vale para sus subcategorías): excepción al umbral general de Ajustes ($5)
  para productos que de verdad cuestan menos (tiras perforadas Perfoquim a
  $0.55–$1.28/m). Manda el del producto, luego el de la categoría más
  cercana y al final el general, que no cambia. Con 0.01 se acepta cualquier
  precio mayor que cero; el origen del precio de la línea dice «mínimo propio
  del producto / categoría '…'». Visible para el Admin de ventas y el Jefe
  MAST.
- **«Actualizar real» en «Revisado»** recalcula también el precio de lista sin
  regresar el documento a borrador: el precio sale de la lista, no de la
  captura, y el gate «sin precio» de la aprobación pide justo corregir la lista
  y refrescar. Si algún precio cambia queda constancia en el chatter (líneas,
  importe antes y después, sin precio antes y después). Los crons y «Conciliar
  facturado» siguen refrescando el precio solo en borrador; lo aprobado sigue
  congelado.

## Pantallas (19.0.1.2.0, revisión de vistas V-M10)

- La lista de líneas y la matriz de la ficha suman al pie la cantidad
  presupuestada y facturada y los importes presupuestado, facturado y pedido
  (este último en la lista, columna opcional).
- En el encabezado del presupuesto o pronóstico solo queda el flujo (enviar a
  revisión o marcar revisado, aprobar, nueva revisión, regresar a borrador y
  enviar demanda al MPS). «Análisis» es botón inteligente; «Actualizar real»,
  «Conciliar facturado» e «Importar desde Excel» están en el engrane
  (Acción) de la ficha, y el PDF en el menú Imprimir.
- Las migas dicen lo mismo que el menú: «Presupuestos», «Pronósticos» y
  «Releases de clientes» (V-M12).

## Releases de clientes (19.0.1.1.0, E1a)

Diseño: `docs/superpowers/specs/2026-09-30-releases-pronostico-presupuesto-e0.md`.

- **Partes del cliente** (`qb.customer.part`, Ventas → Presupuesto y
  pronóstico → Partes del cliente): parte del cliente (+ planta, PO y unidad
  MT/LY/M2/KG) → producto. «Sugerir producto» propone el producto con lo que
  ya está en Odoo:
  - nuestra referencia escrita en la descripción del cliente;
  - pedidos que mencionan la parte o su PO;
  - lo que se le ha vendido al cliente;
  - gramaje y ancho del código del artículo (norma DAT P-D02-01) y
    rendimiento con el peso del costeo (`qb.producto.kg`, si `qb_costeo`
    está instalado; desde 1.4.0, antes `qb.producto.ficha` de
    `qb_capacidad_costeo`).

  Ventas confirma. Solo una parte **confirmada** se usa al aplicar un
  release. La cantidad se convierte con el rendimiento m/kg de la ficha o con
  un factor fijo; sin ese dato la semana no se aplica.
- **Perfiles de release** (`qb.release.profile`): lector, fechas de embarque o
  de entrega (con días de tránsito), zona firme (N semanas o hasta la
  autorización Fab), semanas a pedido, acuse y CUM.
- **Releases** (`qb.release`, con partes y semanas):
  - se sube el archivo y se oprime «Leer», con los lectores de Lear (texto
    AIAG, también dentro de un .eml) y de FXI (Excel SUM);
  - Ventas lo revisa y oprime «Aplicar» (desde 3.0.0 escribe en el
    pronóstico vigente; ver arriba);
  - el release anterior queda «reemplazado».
- **MPS** (retirado en 3.0.0): cada celda producto × fecha lleva la **suma** de todos los
  pronósticos revisados y presupuestos aprobados de la compañía.
  - Antes el último documento enviado pisaba a los demás clientes.
  - El presupuesto solo se omite para el cliente y el mes que un pronóstico
    ya cubre.
  - En un MPS semanal, el mes del presupuesto se reparte en sus lunes.
- La lógica que no necesita Odoo vive en `release/` (`part_match.py`,
  `units.py`, `lear_aiag.py`, `fxi_sum.py`). Se prueba con pytest en el CI
  desde `tests_puros/ventas_presupuesto/`, fuera del addon para que pytest no
  lo importe.

## Qué se queda en el SGI

El KPI VE-02 (`presupuesto_ventas`) y su evidencia leen
`sgi.sales.budget.line` solo si este módulo está (`'sgi.sales.budget.line' in
env`); sin él caen al parámetro `quimibond_sgi.monthly_sales_budget`, igual
que antes sin presupuesto aprobado. El Diagnóstico cuenta los presupuestos en
borrador con la misma condición.

## Instalación y dependencias

- Depende de `quimibond_sgi` (mixins `sgi.base.mixin`/`sgi.format.mixin`,
  grupos, crons), `sale_stock`, `web_grid` y `account_budget`. El núcleo no
  puede depender de este módulo (sería circular).
- `auto_install`: una base nueva con esas dependencias lo instala junto con
  el SGI, como antes.
- En una base que viene de 57.10.0 o anterior (producción),
  `auto_install` **no basta**: Odoo solo instala solo un módulo cuando otra
  de sus dependencias se está *instalando*, no cuando se actualiza
  (`ir.module.module.button_install` / `button_upgrade`). Por eso
  `quimibond_sgi/migrations/19.0.57.11.0/pre-migrate.py` pasa los XML IDs a
  este módulo (`ir_model_data.module`, sin borrar ni recrear) y lo marca para
  instalar en el mismo update; si hay presupuestos y faltara una dependencia,
  detiene el update.

## Verificación después de desplegar

- `sgi.sales.budget` = 4 registros (ids 4, 5, 7, 137) y 1,291 líneas.
- Menús 2434 «Presupuestos» y 2435 (desde 3.0.0 «Pronóstico vigente») con el mismo id.
- Desde 3.0.0: `sgi.sales.budget` con `kind = 'pronostico'` todos en `obsoleto`;
  `mrp.product.forecast` de los programas creados el 2026-11-02 = 0.
- `ir.model.data` con `module = 'quimibond_ventas_presupuesto'` para
  `menu_sale_sgi_sales_budget` (res_id 2434) y `model_sgi_sales_budget`.

## Pruebas

`tests/test_sales_budget.py` (las 117 que vivían en el SGI) y
`tests/test_sales_budget_sgi.py` (multiempresa, Dirección y el cron de
cobertura corrido dos veces, que vivían en otras pruebas del SGI).
`tests/test_ajustes_130.py` (mínimo propio y precio en revisado),
`tests/test_customer_part.py`, `tests/test_release.py` (catálogo de partes y
releases al pronóstico vigente) y `tests/test_forecast_e1.py` (artículo,
pronóstico vigente, carga del Excel, Demanda, cobertura y el candado de los
pronósticos viejos): corren en el build de Odoo.sh con
`--test-tags /quimibond_ventas_presupuesto`. Los lectores, el emparejamiento y
las unidades: `pytest tests_puros/` (sin Odoo, en el CI).
