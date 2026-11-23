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

## Soporte por renglón; desarrollos solo liberados (19.0.3.33.0, 2026-11-23)

Jose (23-nov), sobre la propuesta 2027 con 0 % de crecimiento, desarrollos al 100 % y el tipo de cambio del
día: «así es muy ambiguo. Todo tiene que tener soporte». Cambios:

- **Sin crecimiento por mercado**: la propuesta toma la venta base tal cual (o el pronóstico). Un cambio
  sobre la venta base solo entra como **supuesto de un cliente**, con motivo obligatorio y evidencia (liga o
  archivo). Los supuestos quedan en el presupuesto (pestaña **Supuestos**, `qb.sales.budget.assumption`) y
  cada renglón que tocan lo lleva (`assumption_id`). Por MCP, `growth` distinto de cero da error;
  `partner_growth` acepta `evidence_url`.
- **Soporte de cada renglón** (columnas «Soporte de la cantidad» y «Soporte del precio», botón **Ver
  soporte**): facturas del mismo mes del periodo base (`base_date_from`/`base_date_to` del presupuesto),
  semanas y releases del pronóstico, el desarrollo con su consumo declarado, la regla de lista o el promedio
  facturado. Se guardan la cantidad y el precio propuestos; si se cambian, se escribe el **motivo del
  ajuste**. Un renglón capturado a mano lleva su **soporte** escrito.
- **Desarrollos**: solo los **liberados** suman. Los de Cotización a Pilotaje quedan como **potencial**
  (cantidad 0, con su cantidad e importe potenciales aparte, aviso y botón «Ver potencial» en la ficha); no
  llenan meses ni el plan mensual. Ya no hay porcentaje de consumo.
- **Fuente del tipo de cambio** (`rate_source`, en el asistente y en la pestaña Tipo de cambio).
- **Bloquea con excepción** (Jose, 23-nov): no se aprueba con meses sin renglones, renglones sin soporte o,
  si hay precios en otra moneda, tipo de cambio sin fuente. Dirección puede usar **Aprobar con excepción**,
  que deja la lista en el chatter. Filtros «Sin soporte», «Potencial (no suma)» y «Ajustados a mano» en los
  renglones.

## Propuesta del año con pronóstico y supuestos por cliente; meses vacíos (19.0.3.32.0, 2026-11-23)

Jose (23-nov) preguntó cómo se arma el presupuesto 2027 y eligió tres mejoras antes de armarlo:

- **Pronóstico vigente en la propuesta** (casilla «Usar el pronóstico vigente», encendida): en Industrial y
  Especiales, los meses que el pronóstico de un cliente cubre completos (del primer mes con pronóstico al
  último que su última semana alcanza hasta el último día) salen del pronóstico, sin crecimiento, con origen
  «Pronóstico vigente (releases)», para los productos que su pronóstico trae. Los demás meses, y los
  productos del cliente que no vienen en su pronóstico (se venden por pedido), salen de la venta base. Sin regla en la lista,
  el precio es el real del periodo base del cliente y producto. Confección sigue de la venta base. El
  chatter dice qué clientes y meses vinieron del pronóstico y cuánta venta base reemplazaron.
- **Supuestos por cliente** (tabla en el asistente): crecimiento propio desde un mes, en lugar del del
  mercado (−100 % = deja de comprar), con motivo. Solo en los meses de venta base. Por MCP,
  `action_qb_build_proposal(..., partner_growth=[{'partner_id', 'growth_pct', 'month_from', 'note'}],
  use_forecast=True)`.
- **Meses sin renglones**: aviso en la ficha (borrador o revisado) con cada mes de Industrial, Confección o
  Especiales (los proyectos cuentan para su mercado) sin ningún renglón, y **no se aprueba así**. Si de verdad
  no se vende nada ese mes, Dirección usa «Aprobar con meses vacíos» y queda la lista en el chatter. A la
  REV2 de 2026 le faltaron Industrial y Proyectos en noviembre y diciembre (unos 25 M).

La precarga vieja (`action_preload_from_orders`, el mayor entre pronóstico y venta por celda) no tiene
botón; sigue solo por código.

## Manda la unidad del catálogo (19.0.3.31.1, 2026-11-23)

Jose (23-nov): «del release de Saltillo el PDF lo marca con metros del WJ060 pero todas sus cantidades son
en yardas». El lector toma la unidad impresa en el release («Requirements Unit: MTR») y la ficha del catálogo
nace con ella; la cantidad se convierte con la unidad del catálogo. Ventas la corrigió a yardas en 290524 y
290524L, pero la semana del pronóstico tomaba su unidad del release (metros) y no del catálogo.

- La unidad de la semana del release sale de la **parte del catálogo** (la misma con que se convierte), y
  solo si el catálogo no tiene, de lo que imprime el release. Si un cliente imprime una unidad equivocada,
  se corrige una vez en el catálogo y vale para todos sus releases.

## El release queda en la unidad del cliente (19.0.3.31.0, 2026-11-20)

Jessica (20-nov): «el release de LEAR me transforma las cantidades directamente a kilos, no me deja ver los
metros como con Shawmut». Al aplicar, el release escribía la semana en la unidad de venta (kilos para las
telas con «I») con la unidad del cliente vacía. Para ver metros se le cambió la unidad a «m», pero el número
de kilos se quedaba y se leía como metros: 37 semanas de Lear bajaron 12 veces (de 22,377 kg a 1,858 kg).

- La semana del release queda en la **unidad en que pide el cliente** (Lear en metros, Shawmut y FXI en
  yardas, KG en kilos, M2 en m²), y los metros y kilos siguen en sus columnas. Solo si todas las partes del
  producto piden en la misma unidad, se convierten por unidad (no con factor fijo) y la vuelta a la unidad de
  venta da lo mismo; si no, se queda en la unidad de venta, como antes. «Pide el cliente» y la diferencia
  siguen en la unidad de venta. «Volver al release» respeta la unidad de la semana.
- **Cambiar solo la unidad convierte el número** (ya no lo reinterpreta) y no cuenta como ajuste de Ventas. Si
  falta el rendimiento o el ancho para convertir, avisa. Para corregir una unidad mal capturada se cambian
  cantidad y unidad juntas.

## Yardas y metros del release a kilos con el rendimiento de la ficha (19.0.3.30.1, 2026-11-19)

El release pasaba de yardas a metros bien (× 0.9144), pero de metros a kilos usaba el rendimiento del
código (1000 ÷ gramaje ÷ ancho) en lugar del de la ficha cuando el producto se vende en kilos. Shawmut
CW47, IWJ042Q22JNT160: 10 486 LY = 9 588 m ÷ 14.88 = 644 kg; con la ficha (13.09 m/kg) son 732 kg. Ahora
la conversión del catálogo de partes usa primero el «Rendimiento m/kg» del producto o de su artículo (el
mismo que el pronóstico, los rollos y las congeladas) y el del código solo si no hay ficha. Los releases ya
aplicados se corrigen con «Leer» otra vez.

## Release por correo con aprobación de Ventas (19.0.3.30.0, 2026-11-19)

Jose (19-nov): «necesito que Jessica apruebe cada release nuevo en algún lugar, no que se integre automático,
para comprobar que se haga correctamente y que ella tenga el control» (`models/qb_release_mail.py`).

- **Buzón `releases@quimibond.odoo.com`** (alias de Odoo sobre `qb.release`, `data/qb_release_mail_data.xml`):
  el release se manda o se reenvía ahí (a mano o con un filtro de reenvío en el correo de Ventas). Por cada
  adjunto se busca el perfil con «Dominios del remitente» (también en el cuerpo, para los reenviados),
  «Asunto del correo contiene» y «Archivo del release» (comodines; `release/correo.py`). Se crea el release
  con el archivo y **se lee**; nunca se aplica solo.
- **Aprobación**: el release deja la actividad «Aprobar release …» al Responsable en Ventas del perfil.
  «Marcar revisado» y «Aplicar al pronóstico» solo los hace ese responsable o un administrador.
- Si le tocan varios perfiles o ninguno, queda «Por revisar» sin perfil (se elige el perfil y «Leer»); un
  correo sin archivo de release (firmas, PPAP, OC) queda descartado; el mismo archivo otra vez no crea otro.
- **La planta escrita distinto** (`release/plantas.py`): el perfil decía «Silao», «FXI CD JUAREZ» o nada (Lear)
  y Condiciones de entrega tenía la dirección de entrega o «CIUDAD JUAREZ»; «Cómo pide cada cliente» dice
  «LEÓN» o «SALTILLO». Ahora se comparan por palabras (sin acentos, sin el nombre del cliente, «cd» =
  «ciudad») con o sin dirección, y un cliente con una sola condición la usa aunque el perfil no diga planta.
  Antes los releases de Shawmut Silao, FXI Juárez y Lear se aplicaban sin congeladas, rollos ni días de
  recolección, y Saltillo, León y FXI Juárez sin su OC (Jose, 19-nov).
- **Semanas por pedir**: al aplicar, si hay semanas sin pedido dentro de «Semanas a pedido» del perfil, queda
  la actividad «Crear pedidos …» y el botón «Por pedir» del release abre esas semanas en el Pronóstico
  vigente, donde está «Crear pedido». Filtro nuevo «Por pedir (próximas 4 semanas)».

## Pedido y pronóstico ligados (19.0.3.29.0, 2026-11-19)

Jose (19-nov): «quisiera poder generar el pedido desde ahí […] si hago un cambio de fecha que me mueva la captura
semanal a la fecha que moví el pedido […] que no haya doble captura en pedido y en el forecast y viceversa».
Aprobó este diseño (`models/qb_forecast_orders.py`):

- **Crear pedido** (Pronóstico vigente, lista y ficha): una cotización por cliente, dirección, OC y semana con
  lo que falta por pedir («Por pedir» = cantidad − «Pedido ligado»). La OC sale de la regla del cliente,
  Condiciones de entrega o el release; la fecha, del día de recolección. Se abre la cotización nativa para
  revisarla y confirmarla. Cada renglón queda ligado a su semana (`sale.order.line.qb_forecast_line_id`); la
  ficha de la semana tiene el botón «Pedidos».
- **El pedido manda sobre su semana**: si cambia la fecha de entrega, lo ligado se mueve a la semana nueva (la
  vieja se quita si se queda sin cantidad ni pedidos; si vino de un release queda en cero y ajustada para que el
  siguiente release no la vuelva a llenar). Si cambia la cantidad y el pedido cubría la semana, la semana sigue
  al pedido; si la cubría en parte, solo crece cuando el pedido se pasa. Todo queda en la bitácora.
- **Pedido capturado a mano**: al confirmarse se liga a la semana del cliente con ese artículo en la semana del
  pedido; si no hay, a la más cercana (hasta 4 semanas) con cantidad sin pedir, y la mueve a la semana del pedido.
  Sin pronóstico cerca entra como «Pedido del cliente» (3.21.0).
- **Releases**: una semana con pedido confirmado (también después de las congeladas) se queda con lo pedido; lo
  que el cliente pida de más pasa a la siguiente semana sin pedido y lo de menos se descuenta de las siguientes.
- «Proponer pedidos» también deja la liga, y lo que cambia en los pedidos no se devuelve al pronóstico (ya sale
  de él; contexto `qb_no_forecast_sync`).

## En las congeladas manda el pedido (19.0.3.28.0, 2026-11-19)

Jose (19-nov, Zwisstex CW47): «lo que está congelado de las 3 semanas no lo mueve, eso está mal». La congelada
se quedaba con el pronóstico anterior aunque ya hubiera pedido confirmado (JNT160: 5,500 m pedidos para el 23-nov
y el pronóstico los mandaba al 7-dic). Decisión de Jose: **en las semanas congeladas manda el pedido confirmado**.

- Al aplicar un release, una semana congelada con pedido confirmado (completo, lo entregado también cuenta) se
  queda con lo pedido; la diferencia con lo que pide el cliente pasa a la semana 4 como antes («Congelada con
  pedido»). Sin pedido, se queda el pronóstico anterior. Los pedidos se atribuyen al perfil por su dirección de
  entrega; un cliente con varios perfiles sin dirección (Saltillo y León) sigue con el pronóstico anterior.
- La semana de un pedido es la de embarque con que nació de «Proponer pedidos» (`qb_forecast_week`) o la de su
  fecha compromiso. Ligar un pedido a la semana con pronóstico más cercana se probó y Jose lo rechazó: el pedido
  se va a generar desde el pronóstico y, al moverlo, se mueve la semana (siguiente versión).

## El PDF de COPO se lee con el pypdf de Odoo.sh (19.0.3.27.1, 2026-11-19)

En producción el primer release de COPO quedó «por revisar»: el pypdf de Odoo.sh no tiene el modo `layout`. Sin
ese modo el lector arma el acomodo con las posiciones del content stream (`stream_layout_text`: cada texto en su
x, un renglón por fila de la hoja juntando lo que Excel centra en la vertical, y el segundo renglón de una celda
en su propio renglón). Con el PDF real de COPO da lo mismo que el modo `layout` de pypdf 6.

## Releases en PDF con tabla (19.0.3.27.0, 2026-11-19)

Jose (19-nov): «COPO manda un PDF y eso no hay cómo meterlo en los perfiles de release». Lector nuevo **«PDF
genérico con tabla (parte × fecha: COPO)»** (`release/pdf_table.py`): lee el PDF con el acomodo del texto (pypdf,
`extraction_mode='layout'`), arma la tabla por la posición de las columnas (las semanas vacías no recorren las
cantidades) y la pasa por el lector genérico, con los mismos campos del perfil (unidad, encabezados de parte, OC y
descripción, orden de las fechas). Junta encabezados y descripciones de dos renglones y corta en las notas al pie.
De cada parte toma su OC, la última factura considerada (en «Último packing slip») y lo recibido («Llegadas», en
«CUM recibido»); del encabezado, «Release No.» y la fecha. Fechas con el mes en letra (16-Nov-26, 7-dic-2026).
COPO: sus fechas son de material listo para recolección en nuestra planta (EXW) → «Fecha de embarque».

## La presentación sale de la ficha técnica (19.0.3.26.0, 2026-11-19)

Jose (19-nov): «muchos de los datos de la preparación del producto deberían de salir del módulo de ficha técnica…
y ya en ventas lo hacemos por cliente». Tres niveles:

1. **Especificación** (ancho de venta, peso, espesor y sus tolerancias): la de la ficha de acabado. Condiciones de
   entrega la muestra en «Especificación (ficha)», de solo lectura; se quitó el «Ancho» que se capturaba ahí
   (estaba vacío en todas las fichas).
2. **Presentación estándar**: pestaña **Presentación** nueva en la ficha de acabado (largo de rollo, tubo, empalmes,
   tramo mínimo, peso máximo, rollos por tarima, empaque), agregada desde este módulo
   (`models/qb_ficha_presentacion.py`) para no tocar el de la ficha. Condiciones de entrega la muestra en
   «Presentación de la ficha».
3. **Por cliente**: lo que pide distinto. Vacío (0 o −1) = el del cliente en el encabezado y, si tampoco, el de la
   ficha. Para el redondeo a rollos: renglón → cliente → ficha. «Tolerancia del cliente» abre ancho y peso mínimo y
   máximo cuando el cliente exige otra tolerancia.

## Varios días de recolección (19.0.3.25.0, 2026-11-19)

Jose (19-nov): «qué sucede si un cliente tiene más de una fecha de recolección, como Lear, que recolecta
martes y viernes». Desde septiembre Ventas captura a mano dos pedidos por semana de Lear, martes y viernes,
por partes iguales; la ficha solo aceptaba un día.

- **Recolección** (Jose, 19-nov: «¿no es mejor preguntar si el cliente tiene recolección fija?»): *Sin día
  fijo* (la fecha del pedido es la del cliente menos el colchón, un pedido por semana), *Un día a la semana*
  o *Varios días a la semana*; con día fijo aparecen los **Días de recolección** (etiquetas, `qb.weekday`).
  Las fichas que tenían día quedan en «Un día a la semana» con ese día.
- Con varios días, «Proponer pedidos» reparte la semana **por partes iguales** (por rollos completos si el
  rollo está en la unidad del producto; si no, por unidades enteras; lo que sobra va al primer día) y sale
  un renglón y un pedido por OC, semana y día (`sale.order.qb_pickup_weekday`).
- Al volver a proponer, cada día se compara con su pedido: el que se creó para ese día o, si se capturó a
  mano, el de la fecha compromiso más cercana a ese día.

## Una OC abierta por producto (19.0.3.24.0, 2026-11-19)

Jose (19-nov): «qué pasa si un mismo cliente tiene más de una orden abierta, eso sucede con Zwisstex… para
Shawmut y Saltillo no, me dan una sola OC abierta para todos sus productos y cambia de acuerdo a su planta».
La OC abierta vivía solo en la regla de «Cómo pide el cliente» (una por cliente y planta), y Zwisstex tiene
una OC por producto (14792, 15486, 15862…).

- La presentación por producto de **Condiciones de entrega** trae **OC abierta** y **Vigencia de la OC**.
  Cada cambio queda en el historial de la ficha.
- «Proponer pedidos» toma la OC así: en «OC abierta + release», la del producto y, si no tiene, la de la
  regla (la OC que trae el release se sigue ignorando, como antes); en «OC en el release» y «Release sin
  OC», la de la semana y, si no trae, la del producto. Vencida la del producto, ese producto no entra.
- La regla en «OC abierta + release» ya puede ir sin OC: los productos sin OC propia quedan «No entra» con
  el motivo «Sin OC abierta para el producto en Condiciones de entrega».
- Shawmut y Saltillo no cambian: su OC sigue en la regla de cada planta.

## Ficha del presupuesto anual con vistas nativas (19.0.3.23.0, 2026-11-19)

Jose no quiso el tablero de la 3.22.0 (tablas en HTML hechas a mano): «lo necesito lo más nativo de Odoo
posible; asegura que las vistas y la navegación funcionen perfecto». Se quitaron los campos HTML. La ficha
queda con elementos estándar:

- **Campos** «Al corte» (presupuesto, venta, diferencia, cumplimiento con barra de avance) y «Año»
  (presupuesto del año, lo que pone el presupuesto del mes, cierre estimado y su % con barra), y
  **cumplimiento por mercado** (Industrial, Confección, Especiales y a tipo de cambio presupuestal) con barra.
- **Botones inteligentes:** Renglones (lista agrupada por mercado y cliente, pivote), **Por mercado**,
  **Por mes** (gráfica de presupuesto contra venta por mes) y **Por cliente** (cliente y artículo), que abren
  el comparativo `qb.sales.compare` filtrado a este presupuesto y la venta del año, en pivote, gráfica y
  lista; Variaciones (volumen, precio, tipo de cambio por renglón) y Cierre de ventas.
- **Pestañas:** Renglones (de nuevo en la ficha, como antes), Diferencia, Objetivo, Tipo de cambio y
  Justificación.
- El comparativo gana la fuente **«Presupuesto del mes»** (los renglones del presupuesto del mes en los
  meses y mercados sin renglones del anual, ligados al presupuesto vigente o en captura), la medida
  **Importe** (presupuesto o venta, para graficarlos juntos por fuente), una **lista** y la bajada al detalle
  desde el pivote (antes estaba apagada).
- **Cierre estimado por mercado:** en un mes con presupuesto del mes, cada mercado que ese documento no trae
  (Confección y Especiales mientras los vendedores no capturan) usa el anual.
- **Probado navegando** en el build de la rama con datos de prueba (presupuesto 2026, facturas de enero a
  octubre, presupuesto del mes de noviembre). Se corrigió lo que salió: la gráfica por mes va con barras
  lado a lado (apiladas sumaba presupuesto y venta), el año sin separador de miles («2026», no «2,026»; en Odoo 19 la opción es `enable_formatting`),
  el precio de los renglones no se suma al agrupar, y Renglones abre sin la matriz (en Odoo 19 la vista
  grid sin periodos truena al cambiar a ella).
- **Pruebas:** las del presupuesto anual no corrían en Odoo 19 (el cliente de prueba usaba
  `res.partner.team_id`, que ya no existe; ahora toma el equipo de su vendedor), y la prueba del
  comparativo guarda los renglones antes de leer la vista SQL.

## Ficha del presupuesto anual con tablero (19.0.3.22.0, 2026-11-19)

Reemplazada por la 3.23.0 (arriba). Jose (19-nov) pidió mejorar la vista del presupuesto anual y eligió todas las propuestas. La ficha era una
pared de textos («Industrial 115,318,254 · …») con 854 renglones abajo; ahora (`models/sgi_sales_budget_tablero.py`):

- **Tablero arriba:** presupuesto del año, al corte, vendido al corte, cumplimiento con semáforo (verde desde
  90 %, ámbar desde el mínimo de Ajustes) y **cierre estimado**.
- **Pestañas Por mercado, Por mes y Por cliente** (tablas de solo lectura) con un botón que abre el análisis
  agrupado igual para bajar al renglón. Por mercado separa la diferencia en volumen, precio, tipo de cambio,
  presupuesto del mes y venta sin renglón, con los proyectos debajo de su mercado. Por mes trae presupuesto,
  facturado, pedido de los renglones, acumulados y el estimado: lo vendido en los meses cerrados; en los que
  faltan, el presupuesto del mes (la foto del pronóstico vigente con precio) o, si no hay, el anual; si ya se
  vendió más, lo vendido. Por cliente: los 15 principales, otros, Confección (sin cliente) y la venta a
  clientes sin presupuesto. La venta es la del cumplimiento (regla del VE-02, sin saldos), así que cuadra.
- **Renglones fuera de la ficha:** botón «Renglones» que abre la lista agrupada por mercado y cliente, con la
  matriz y el pivote. Objetivo, cantidades y lo demás pasan a la pestaña «Objetivo y detalle».
- **Meses sin renglones del presupuesto del mes:** la REV2 de 2026 no trae Industrial ni Proyectos en
  noviembre y diciembre. Un mes y mercado sin ningún renglón en el anual toma lo que pone el presupuesto del
  mes de ese mes (borrador o enviado; donde el anual trae algo, manda el anual): suma al presupuesto del año,
  al presupuesto al corte, al cumplimiento, al crecimiento contra el año anterior, al aviso de cierre de mes y
  al cierre de ventas (PDF). En la diferencia aparece como «presupuesto del mes», así que las partes siguen
  sumando. El total de los renglones (`amount_budget_total`) no cambia.

## Pedidos fuera del pronóstico y días hábiles (19.0.3.21.0, 2026-11-19)

Respuestas de Jose del 19-nov:

- **Los pedidos de productos que el cliente no pronostica entran al pronóstico** (producto de línea de
  otro cliente, proyecto, muestra), porque también van al presupuesto. Al actualizar pedidos (cron
  diario, al aplicar un release, al capturar) cada pedido confirmado desde la semana en curso deja una
  semana con origen «Pedido del cliente» (filtro «De pedido»): cliente, producto, semana de compromiso,
  la OC y la cantidad pedida (no la pendiente, para que lo entregado siga en el mes). No se agrega si el
  cliente ya pronostica ese artículo (el pronóstico manda), si el pedido nació de «Proponer pedidos» o
  si el cliente es de Confección (pronostica sin cliente). Si el pedido cambia, la semana cambia; si se
  cancela, se quita; si Ventas la corrige, pasa a «Captura» y el pedido ya no la toca. No cuenta como
  pronóstico sin pedido en la Demanda y «Proponer pedidos» no la propone otra vez. Las semanas pasadas
  no se tocan.
- **Días hábiles de adelanto**: los días de adelanto de las condiciones de entrega se cuentan sin
  sábados ni domingos (5 = una semana) y la fecha de embarque nunca cae en fin de semana. Los días de
  tránsito del perfil del release siguen siendo naturales. Aplica a los releases que se lean desde
  ahora.

## Menús recortados (19.0.3.20.0, 2026-11-19)

Jose (19-nov): «siento que ya hay muchísimos menús… quitar lo que no sirve… no porque tenga datos
significa que funciona». Se revisó cada menú contra su uso en producción (quién lo usó y cuándo, si duplica
otra vista). Ventas → Presupuesto y pronóstico pasa de 36 entradas a 20 (mismos XML IDs):

- **Pronóstico:** Pronóstico vigente (botones arriba de la lista: «Pegar desde Excel», solo gerentes, e
  «Imprimir o descargar»), Captura semanal, Releases (un solo menú; abre con lo pendiente agrupado por
  estado), Proponer pedidos.
- **Presupuesto:** Presupuesto anual (Análisis y Cierre de ventas son botones de la ficha), Presupuesto del
  mes (botón «Armar presupuesto del mes» arriba de la lista), Proyectos comerciales, Armar propuesta del año.
- **Reportes:** Presupuesto contra real (antes «Cierre mensual por mercado»; favoritos compartidos «Por año
  y mercado» y «Por cliente y artículo», que eran los menús «Anual» y «Cliente y artículo»), Cambios del
  pronóstico, Faltantes.
- **Configuración:** Cómo pide cada cliente, Condiciones de entrega, Partes del cliente, Perfiles de release.

Menús quitados (los modelos, acciones y datos se quedan): Comparación de precios, Hojas del forecast y
Cargar forecast de Excel (dependen del forecast.xlsx, que ya no se mantiene; la comparación corrió una vez,
el 4-nov), Pronósticos anteriores y su grupo Histórico (21 documentos obsoletos), Releases por revisar,
Análisis, Anual, Cliente y artículo, Venta real (el comparativo trae lo real en pesos, metros y kilos),
Cierre de ventas (PDF), y en Ventas Capacidad semanal y Demanda por cliente (son de Planeación; en las 56
semanas nadie había capturado capacidad). Fabricación → Pronóstico de ventas queda con Demanda por
cliente, Pronóstico vigente, Capacidad semanal y Cambios del pronóstico.

## Condiciones de entrega por cliente (19.0.3.19.0, 2026-11-19)

Jose (19-nov): «¿no crees que necesitamos una parte en algún lado en donde podamos poner la
presentación de entrega de los clientes?… también agrega número de empalmes y todo eso». Las reglas de
3.18.0 vivían en el perfil de release, que solo tienen los clientes con release; ahora hay una ficha por
cliente y planta, tenga o no release: **Ventas → Presupuesto y pronóstico → Configuración → Condiciones
de entrega** (`qb.delivery.terms`).

- **Cuándo se embarca:** día de recolección, días de adelanto (embarcar antes de la fecha en planta del
  cliente; el release los usa si su perfil no trae días de tránsito), colchón en días (sin día de
  recolección, la fecha del pedido es la del cliente menos el colchón; 1 por omisión) y capacidad
  asignada por semana (informativa).
- **Al aplicar el release:** semanas congeladas, semanas por embarque y su primer embarque, y «Manda
  cantidades cerradas» (no redondea). Las reglas de 3.18.0 leen de aquí.
- **Presentación:** rollo y empalmes máximos del cliente, y por producto (`qb.delivery.terms.line`): largo
  del rollo y si se redondea, ancho, tubo / core, empalmes máximos, tramo mínimo entre empalmes, peso
  máximo, rollos por tarima, empaque, etiqueta y notas. Documentos del embarque y etiquetado en su
  pestaña.
- **Proponer pedidos:** la fecha de entrega de un pedido nuevo es el día de recolección de la semana de
  embarque o, sin él, la fecha del cliente menos el colchón.
- Migración: una ficha por perfil de release con 3 semanas congeladas; en `main` (3.18.0) pasan las
  semanas congeladas, los embarques y el rollo del perfil, y el rollo de cada parte del cliente. Se
  quitan esos campos del perfil y de la parte.

**Siguiente:** que Inspección avise si un rollo final pasa los empalmes máximos del cliente
(`quimibond_rollos_bitacora` ya cuenta los empalmes) y los acumulados de Saltillo.

## Reglas del release por cliente (19.0.3.18.0, 2026-11-19)

Jose (19-nov) describió lo que hacía a mano después de subir cada release, cliente por cliente, y pidió
que fuera **escalable**: nada de código por cliente. Las reglas son configuración del **perfil de
release** («Al aplicar el release») y de la **parte del cliente**; el motor es Python puro
(`release/reglas.py`, pruebas en `tests_puros/ventas_presupuesto/test_reglas.py`).

- **Semanas congeladas** (perfil, 3 por omisión): las semanas desde la semana en curso que Planeación ya
  programó se quedan como estaban. Lo que el cliente sube en ellas entra en la semana siguiente (la 4);
  lo que baja se resta de la 4 y, si no alcanza, de la 5 y las demás, sin dejar ninguna bajo cero.
  0 = sin congelar (Blancos y Pieles, que mueven semanas congeladas, no tienen release).
- **Semanas por embarque** (perfil): la demanda de cada bloque de N semanas se suma en la primera
  semana del bloque (FXI Juárez: 4). Los bloques se cuentan desde «Primer embarque de los bloques»
  para que no se recorran cada semana.
- **Rollo** (perfil, y la parte del cliente gana): cada semana después de las congeladas se sube al
  múltiplo del rollo (505 yd con rollos de 500 → 1000). Shawmut: 500 yd en el perfil; FXI:
  WD038Q46JNT163 1000 yd y WD038Q46JNG163 500 yd en sus partes. 0 = cantidades cerradas.
- «Pide el cliente» guarda lo del release tal cual; la columna **Reglas del release** dice qué hizo
  cada regla en la semana («Congelada: el cliente pide 250, se queda 200», «Recibe +50 de las semanas
  congeladas», «Junta la semana del…», «Redondeado de 450 a 600»). Lo que hace una regla **no** es un
  ajuste de Ventas: el siguiente release lo vuelve a calcular. Un ajuste de Ventas (3.17.0) sigue
  ganando sobre las reglas.
- **Capacidad semanal** (Pronóstico y Fabricación → Capacidad semanal): metros del pronóstico vigente
  de cada semana, de todos los clientes, contra la capacidad de venta que da Producción (parámetro
  `quimibond_ventas_presupuesto.forecast_capacity_m`, 250,000 m), editable por semana porque cambia.
  En rojo si se pasa, en amarillo arriba de 90 %. Solo avisa.
- Al actualizar, todos los perfiles quedan con 3 semanas congeladas, un embarque por semana y sin
  rollo.

**Siguiente:** acumulados (restar lo enviado que el release del cliente todavía no ve: Saltillo), día
de recolección por cliente y el colchón de un día en la fecha de entrega.

## Ventas ajusta las semanas del release (19.0.3.17.0, 2026-11-18)

Jose (18-nov): «necesito poder modificar las semanas una vez que suba los releases de los clientes
[…] así como planeación va metiendo el plan maestro». Las semanas ya se podían cambiar (Captura
semanal, o la semana en Pronóstico vigente), pero el siguiente release volvía a poner lo que pedía el
cliente y la corrección se perdía. Decisión de Jose (18-nov): **conservar el ajuste y avisar**.

- Cada semana del release guarda **Pide el cliente** (`qty_release`, unidad de venta) aparte de la
  cantidad del pronóstico, que es la que Ventas decide enviar.
- Si Ventas cambia la cantidad (matriz, lista o ficha), la semana queda **Ajustada por Ventas**. En la
  matriz, una semana del release en cero **no se quita**: queda en cero («no enviamos esa semana»).
- **Aplicar** un release ya no pisa una semana ajustada: actualiza «Pide el cliente», el release, la
  OC y la fecha del cliente, y si el cliente cambió su número (o ya no trae la semana) la marca **El
  cliente cambió** (en amarillo, con su filtro) y lo anota en Cambios del pronóstico y en el chatter del
  release. Lo no ajustado sigue al release como antes.
- En la lista y en la ficha: **Volver al release** (la semana regresa a lo que pide el cliente; si ya
  no lo pide, se quita) y **Conservar el ajuste** (quita la marca).
- El presupuesto anual, el presupuesto del mes («Volver a armar»), la Demanda de Planeación y
  «Proponer pedidos» leen la cantidad ajustada. Una semana ajustada a cero sin pedido no se propone.
- Si el release trae la misma tela en otro código (metros / kilos) que la semana ajustada, gana el
  release (no se puede conservar una cantidad de otra unidad).
- Migración: las semanas del release toman su cantidad como «Pide el cliente»; las que Ventas ya había
  corregido después de un release quedan ajustadas con lo que pedía ese release (Shawmut, semana del
  9-nov: pide 245.68, ajustada a 5 kg).

## Ventas pone la fecha de entrega (19.0.3.16.7, 2026-11-10)

Jose (10-nov): «en algún punto del proceso Jessica tiene que poder ponerle la fecha de entrega que
nosotros pensamos que le vamos a entregar al cliente».

- En «Proponer pedidos» hay dos columnas: **Pide el cliente** (la entrega del release o el lunes de la
  semana, fija) y **Fecha de entrega**, la que comprometemos, **editable**. Viene la que ya tiene el
  pedido (aunque se haya cambiado a mano) o, si no hay pedido, la que pide el cliente; nunca en el
  pasado. Sale en rojo si es posterior a lo que pide el cliente.
- Cambiarla **marca el renglón**, también uno «Sin cambio» (solo cambia la fecha, no la cantidad). Al
  aplicar, el pedido toma esa fecha; en un pedido confirmado y bloqueado se desbloquea, se cambia y
  se vuelve a bloquear con nota (solo el administrador de ventas).
- La siguiente propuesta **respeta la fecha comprometida**: ya no la regresa a la del release.
- También se puede cambiar directo en el pedido («Otra información» → «Fecha de entrega»); la
  propuesta la toma de ahí.

## Fecha de entrega y bajas a cero en «Proponer pedidos» (19.0.3.16.6, 2026-11-10)

Primera corrida real (10-nov): los pedidos parecían «sin fecha de entrega» y no había cómo aplicar un
release que deja de pedir un producto. (Los duplicados por la fecha de entrega ya los resolvió 3.16.1.)

- **Fecha de entrega visible:** la propuesta muestra la columna «Fecha de entrega», la que lleva el
  pedido: la entrega del release o, si no trae, el lunes de la semana de embarque. **Nunca en el
  pasado:** una semana atrasada se pide para hoy (salían cotizaciones con entrega del 2, 3 y 9 de
  noviembre). Al aplicar, la cotización del pronóstico toma la fecha vigente (el release pudo moverla).
- **Baja a cero:** si el release deja de pedir un producto, la semana se quita del pronóstico y la
  propuesta ya no la veía; ahora propone **bajarlo a cero** en el pedido que nació del pronóstico, con
  las mismas reglas que cualquier baja (en una cotización viene marcada; en un pedido confirmado,
  desmarcada y solo el administrador de ventas; nunca baja de lo entregado o facturado). La cotización
  que se queda sin productos se cancela. Los pedidos capturados a mano no se tocan.

## La lista del cliente es la fuente del precio (19.0.3.16.5, 2026-11-10)

Decisión de Jose (10-nov): el precio del presupuesto del mes sale **del presupuesto anual aprobado o
de la lista de precios del cliente**, nunca de una factura. Lo que la lista no tiene lo da de alta
Ventas:

- Al armar el presupuesto del mes y al pulsar **Completar precios**, cada cliente con productos que
  su lista no tiene recibe **una actividad para su vendedor** (en la lista de precios del cliente;
  si no tiene lista, en el cliente) con los productos, la cantidad del mes y, **solo de
  referencia**, el último precio facturado (del mismo código o de la misma tela en otro código, por
  kilo o por metro). Si ya hay una abierta de ese presupuesto del mes, se actualiza.
- Cuando Ventas da de alta los precios, se pulsa otra vez «Completar precios».
- Un renglón que tomó precio de una factura (3.16.2 a 3.16.4) se vuelve a evaluar: si la lista ya lo
  tiene, toma ese; si no, queda sin precio y entra a la actividad.

Lo de 3.16.2 y 3.16.4 sobre la «última factura» queda solo como referencia para Ventas.

## Precio de la misma tela facturada en otro código (19.0.3.16.4, 2026-11-10)

En el presupuesto de noviembre de 2026 quedaban 9 renglones sin precio: FXI, Lear y Shawmut se
facturan **en kilos** (IWD038…, IWJ045…) y el pronóstico viene **en metros** (WD038…, WJ045…), así
que no había factura del mismo código. Ahora, si no hay factura del mismo producto, el precio es el
de la última factura al cliente de **la misma tela en otro código** (mismo artículo) en los 12 meses
anteriores, con la base de la unidad en que se facturó: por kilo o por metro (el importe usa los
kilos o metros del renglón con el rendimiento de la tela). Origen «Última factura».

## Proyectos comerciales: inicio, revisión y desarrollos repetidos (19.0.3.16.3, 2026-11-10)

Revisión de los proyectos comerciales (3.8.0–3.10.0):

- **Inicio del proyecto:** la bitácora de etapas del desarrollo empezó en octubre de 2026; los
  desarrollos de antes no tenían registro de Cotización y su proyecto tomó la fecha del día en que
  se cargó (52 de 62 en noviembre de 2026), así que su venta anterior contaba como línea. Ahora el
  inicio es la entrada a Cotización o la fecha de alta del desarrollo, la que sea anterior; la
  migración lo corrige en los existentes y reconstruye la Venta real.
- **Desarrollo repetido:** si un desarrollo genera el producto de un cliente y artículo que ya
  tiene otro proyecto vigente, su proyecto no toma el producto y avisa en el chatter (antes el
  desarrollo no podía cambiar de etapa).
- **Pasar a línea:** el aviso dice lo que hace: la venta cuenta en línea en Venta real y en el
  cierre; los renglones del presupuesto no cambian de mercado. Cuando lo gradúa el presupuesto del
  año siguiente, pasa a línea al 31 de diciembre del año anterior (antes, el día de la
  aprobación: el resto del año en curso salía de Proyectos).
- **Presupuestado** del proyecto: un documento por año (el aprobado; si no hay, el abierto). Antes
  sumaba el aprobado y su revisión en borrador.
- **Mercado de un renglón con desarrollo:** solo se pone Proyectos si no viene mercado; una copia de
  un renglón que Ventas pasó a Industrial ya no regresa a Proyectos.
- **Propuesta del año:** un desarrollo de un producto de línea para un cliente que no lo compra sí
  entra (antes se descartaba si el producto se vendía a cualquier cliente), y dos desarrollos del
  mismo cliente y artículo dan un solo juego de renglones (antes la propuesta entera fallaba).

## Precios del presupuesto del mes (19.0.3.16.2, 2026-11-10)

El presupuesto del mes de noviembre de 2026 salía con 11 renglones sin precio: el precio del
presupuesto anual se buscaba solo en el mismo mes, y la REV2 casi no trae noviembre ni diciembre.
Ahora el precio de cada renglón sale, en este orden, de:

1. **El presupuesto anual aprobado**, del mismo cliente y producto, del mes o, si no está ese mes,
   del mes más cercano.
2. **La lista del cliente** (solo con regla del producto, como antes).
3. **La última factura** de ese cliente y producto en los 12 meses anteriores, en la unidad de venta
   y la moneda de la factura (origen «Última factura»).

**Completar precios** (encabezado, en borrador) pone precio a los renglones que siguen en cero sin
tocar cantidades ni los que ya tienen precio. Lo que siga sin precio no tiene presupuesto, lista ni
factura: hay que darlo de alta en la lista del cliente.

## Proponer pedidos sin duplicar (19.0.3.16.1, 2026-11-10)

Revisión de «Proponer pedidos» (3.15.x): había casos en que proponía otra vez lo ya pedido.

- **Semana del pedido:** un pedido se reconoce por la semana del pronóstico (`qb_forecast_week`),
  no por la fecha compromiso, que puede ser la de entrega del cliente días después del embarque
  (perfiles con días de tránsito). Un pedido capturado a mano, sin esa semana, se sigue
  reconociendo por la fecha compromiso dentro de la semana.
- **Release sin OC:** la referencia `REL-…` lleva el número del release, que cambia cada semana; el
  pedido ya hecho con el release anterior se reconoce por el prefijo `REL-`.
- **Plantas:** con dirección de entrega, solo cuentan los pedidos de esa dirección; las cotizaciones
  nuevas de cada planta van con su dirección.
- **Misma tela en otro código:** si la semana ya tiene pedido de la tela en metros y el pronóstico la
  trae en kilos (o al revés), no se pide otra vez: queda «No entra» con el código del pedido.
- **Lo facturado:** una cantidad nunca baja de lo entregado **ni de lo facturado**.
- **Aplicar dos veces:** un renglón aplicado queda marcado y no se vuelve a aplicar; al aplicar se
  vuelven a leer los pedidos, así que dos asistentes armados a la vez no piden dos veces lo mismo.
- **Semanas sin regla** de la planta aparecen como «No entra» con su motivo, y «Exige OC para
  facturar» respeta la regla de la planta del pedido.

## Lector de Woodbridge y bandeja de releases (19.0.3.16.0, 2026-11-09)

Diseño v3, 2.10.4 y 2.10.5.

- **Lector «Woodbridge — PDF»** (`release/woodbridge_pdf.py`, prueba pura en
  `tests_puros/ventas_presupuesto/test_release_woodbridge.py`): el «Supplier Release Planning
  Schedule» de Saltillo Lamination (Saltillo y León). Lee el PDF con el lector de Odoo (pypdf) y
  toma por parte la OC abierta (Blanket P/O 6700 / 6600319), el release, la unidad (MTR, LY, YD),
  el acumulado recibido y las semanas con su autorización (Firm y Fabrication = firme; Forecast =
  planeado). Descarta las páginas viejas que arrastra el PDF de León (más de 180 días antes de la
  más reciente) y **no lee el release de una planta en el perfil de la otra** (el número de
  proveedor del PDF, 130551 o 130551L, tiene que ser el del perfil).
- **Releases por revisar** (Pronóstico): los recibidos, por revisar, leídos o revisados que aún no
  se aplican, agrupados por estado. Filtro «Revisados sin aplicar».
- **Aviso:** cron diario; un release recibido, por revisar o leído hace más de 2 días deja una
  actividad al responsable de Ventas del perfil (una por release).

Después del update: cambiar el lector de los perfiles de Saltillo y León a «Woodbridge — PDF».

## Ajustar pedidos confirmados (19.0.3.15.2, 2026-11-09)

«Crear o actualizar cotizaciones» daba «Está prohibido modificar … en una orden bloqueada:
Cantidad»: producción bloquea el pedido al confirmarlo. Decisión de Jose (opción B, la que Ventas
hace hoy a mano): el **administrador de ventas** ajusta el pedido confirmado de la OC y semana:
se **desbloquea, se cambia la cantidad y se vuelve a bloquear**, con nota en el chatter («Cantidad
de … : antes → después (release …)»). Así sigue habiendo un pedido por OC y semana y Odoo ajusta
la entrega pendiente.

- Los **aumentos** vienen marcados; las **bajas** en un confirmado, **desmarcadas** (puede quedar
  material sin pedido): se marcan a propósito. Columna «Ajusta un pedido confirmado».
- Para quien no es administrador de ventas, quedan **por autorizar**.
- Nunca baja de lo entregado. Si la entrega de la línea ya se surtió completa, lo que se pide de
  más va a una cotización nueva de la misma OC y semana.

## Aplicar un release reemplaza la misma tela en otro código (19.0.3.15.1, 2026-11-09)

La primera aplicación real (Shawmut W45, 9-nov) **duplicó** 62 semanas: el forecast de Excel
tenía a Shawmut en WJ (metros) y el release viene en IWJ (kilos); «Aplicar» buscaba la semana por
producto exacto y creó otra al lado. Ahora busca por **artículo**: la semana de la misma tela en
otro código se reemplaza (toma el producto del release) y las de esa tela que el release ya no
trae se quitan. En producción, las 62 semanas del Excel se archivaron a mano (recuperables) y la
Demanda se reconstruyó.

## Pedidos desde el pronóstico (19.0.3.15.0, E3, 2026-11-09)

Decisiones de Jose (9-nov): **un pedido por OC y semana de embarque**, creado como
**cotización**; los modos por cliente los propone Claude con los datos de producción.

- **Regla única:** una semana del pronóstico se vuelve pedido cuando está dentro de las
  **semanas a pedido** del cliente (desde la semana en curso) y **tiene OC**. Lo demás sigue como
  pronóstico.
- **Cómo pide cada cliente** (Configuración, `qb.order.rule`, por cliente y, si hace falta,
  planta): **OC en el release** (cada semana trae su OC), **OC abierta + release** (la OC de la
  regla, con vigencia), **Release sin OC** (la referencia es `REL-<release>`; con «Exige OC para
  facturar» el pedido no se factura hasta capturar la OC) o **Sin release** (a mano; no se propone).
- **La OC del pronóstico se limpia** (`release/oc.py`): viene del Excel como «6700/PV17173»
  (OC y pedido de Odoo), «PO/14792», «PO 70152» o con el encabezado «PO» / «No.parte cliente»,
  que no son OC.
- **Pronóstico → Proponer pedidos:** por cliente, OC, semana y producto, la cantidad del pronóstico
  contra la de las cotizaciones y pedidos abiertos de esa OC y semana: **nuevo**, **aumenta**,
  **baja**, **sin cambio**, **baja por autorizar** (pedido ya confirmado: no se aplica sola) o
  **no entra** con su motivo (sin OC, OC abierta vencida, sin precio en la lista, artículo en
  desarrollo). «Crear o actualizar cotizaciones» aplica los renglones marcados: un pedido por OC y
  semana (fecha compromiso = la entrega del cliente o el lunes de la semana), la OC en «Referencia
  del cliente», el precio de la lista del cliente, y en cada línea la semana, el release y la parte
  del cliente. Nunca baja de lo entregado.

## Botones de encabezado (19.0.3.14.2, 2026-11-09)

«Actualizar» de la Demanda por cliente y «Comparar con forecast.xlsx» de la Comparación de
precios daban `TypeError ... takes 1 positional argument but 2 were given`: el botón del
encabezado de una lista manda los ids de la lista y los métodos eran `@api.model`. La Demanda
usa ahora `action_qb_rebuild_button` y la comparación ya no es `@api.model`.

## Corrección del build (19.0.3.14.1, 2026-11-09)

El build de `main` se cayó en la migración 3.8.0: el mercado de un proyecto nuevo leía
`res.partner.team_id`, que no existe en Odoo 19. Ahora el equipo del cliente sale de
`team_of_partner` (último pedido confirmado, última factura o el equipo del vendedor), también
en el mercado por omisión de un renglón. Además: un segundo desarrollo del mismo cliente y
artículo ya no choca con «un proyecto vigente por cliente y producto» (se queda con el que
existe), y las migraciones 3.8.0, 3.10.0 y 3.11.0 trabajan registro por registro o con
savepoint: lo que falle queda en el log y no detiene la actualización.

## Presupuesto del mes (19.0.3.14.0, E2, 2026-11-09)

Diseño v3, 2.5. Sustituye el F-P-A28-14 de Excel. Presupuesto → **Presupuesto del mes** y
**Armar presupuesto del mes** (`qb.sales.month.plan`, uno por compañía y mes).

- **Armar** (por omisión, el mes siguiente): una **foto del pronóstico vigente** del mes, un
  renglón por cliente comercial, planta y producto (las semanas del mes sumadas), en la unidad
  de venta, metros y kilos.
  - **Precio:** el del renglón del presupuesto anual aprobado del mismo cliente, producto y mes;
    si no hay, el de la lista del cliente (la regla del anual); si no, cuenta 0 y sale en rojo.
  - **Tipo de cambio del mes:** el del presupuesto anual aprobado (o el de Ajustes), editable.
  - **Mercado:** el del pronóstico; con proyecto comercial vigente, «Proyectos».
- **Ajustes antes de enviar:** cantidad y precio por renglón; no tocan el pronóstico. «Pronóstico
  al armar» guarda la cantidad original. «Volver a armar» rehace los renglones del pronóstico y
  conserva los capturados a mano.
- **Confección:** los vendedores agregan sus renglones (sin cliente) en el mismo documento; una
  regla de registro solo les deja crear, cambiar o borrar renglones de Confección y Proyectos
  confección, y la cabecera (mes, tipo de cambio) es del administrador de ventas.
- **Totales:** por mercado, y **productos de línea / proyectos** (los dos números del correo).
- **Enviar a Dirección:** congela el documento (solo el administrador del SGI lo regresa a
  borrador), genera el PDF F-P-A28-14 (resumen línea / proyectos y detalle por mercado) y abre el
  correo con el PDF y los totales.

**Pendiente:** el comparativo (`qb.sales.compare`) todavía no trae la foto del presupuesto del mes
(diseño 2.8) para medir cuánto se movió el pronóstico contra lo presupuestado en el mes.

## Cierre de ventas general (19.0.3.13.0, E2, 2026-11-09)

Diseño v3, 2.8 y 2.9. Sustituye la hoja MENSUAL del Excel.

- **PDF «Cierre de ventas general <mes>»** (hoja horizontal): por mercado, el presupuesto, la
  venta, la diferencia y el cumplimiento **del mes** y **del acumulado del año**, y la venta del
  mismo periodo del año anterior con su crecimiento; al pie, la venta del año anterior, el
  objetivo y el mínimo, y la justificación del incumplimiento si la hay. La venta es la de
  «Venta real» (sin saldos ni desperdicio; el PDF dice cuánto quedó fuera).
- **Dónde:** botón **Cierre de ventas** en el presupuesto, o Reportes → **Cierre de ventas
  (PDF)**. El asistente pide el presupuesto (por omisión, el aprobado del año) y el mes (por
  omisión, el último cerrado) y tiene **Imprimir PDF** y **Enviar por correo** (abre el
  redactor del presupuesto con el PDF adjunto y el resumen en el cuerpo).
- Antes de calcular se reconstruye la venta real del año hasta el fin del mes y el mismo
  periodo del año anterior: no depende del cron nocturno.
- **Cierre de mes del SGI:** al cerrar cada mes, el PDF queda en el chatter del presupuesto
  aprobado del año. El aviso de cierre por debajo del umbral también mide sin saldos.

## VE-02 sobre la venta real (19.0.3.12.0, E2, 2026-11-09)

Diseño v3, 2.7; decisión de Jose (4-nov): **los saldos y el desperdicio no cuentan en el
cumplimiento**. Van a la misma cuenta 401 que la tela; el VE-02 los sumaba y el cierre del
Excel no (0.19 % de la venta ene–oct 2026).

- El **VE-02** («Cumplimiento de presupuesto de ventas») mide la misma venta que «Venta real»:
  las líneas de venta de siempre (cuentas 401/402, compañía del KPI) **sin los productos de
  saldo o desperdicio**. Se calcula directo de las facturas con la misma regla, no de la tabla,
  así que no depende de que la venta real esté reconstruida. La nota de la medición dice
  cuánto quedó fuera.
- En el presupuesto: **Venta al corte** y **Cumplimiento al corte**, y la **base del año
  anterior** (objetivo y mínimo), también sin saldos.
- Los demás indicadores de venta (VE-01, cartera, DSO…) no cambian: el filtro va por contexto
  (`qb_sin_saldos`) solo desde el VE-02 y el presupuesto (`models/sgi_indicator.py`, herencia
  de `sgi.indicator` en este módulo; el SGI no se toca).
- **Las mediciones ya guardadas no cambian**; las de 2026 se pueden volver a medir para que
  queden sin saldos. La base del año anterior de un presupuesto se recalcula al enviarlo a
  revisión o con «Actualizar base».

## Venta real, comparativo y reportes (19.0.3.11.0, E2, 2026-11-09)

Diseño v3, secciones 2.7 y 2.8. Menú nuevo **Presupuesto y pronóstico → Reportes**.

- **Venta real** (`qb.sales.actual`): una fila por línea de factura de cliente publicada en
  las cuentas de venta del VE-02, con cliente comercial, producto, artículo, mercado, venta en
  pesos (la nota de crédito resta), importe y moneda de la factura, tipo de cambio, cantidad con
  signo, metros y kilos, y las marcas **saldo o desperdicio** y **nota genérica o servicio**.
  - **Mercado:** el del equipo de la factura (el mismo con que el presupuesto mide su real y se
    arma la propuesta); si el cliente y el artículo tienen un proyecto comercial en la fecha de
    la factura, «Proyectos» de ese mercado; sin equipo de mercado, «Sin mercado».
  - **Desviación del diseño:** es una tabla que se reconstruye, no una vista SQL (como la
    Demanda), porque la regla de cuentas del VE-02 y la conversión a metros y kilos viven en
    Python. Cron nocturno (año en curso y anterior; parámetro
    `quimibond_ventas_presupuesto.actual_years`, 2 por omisión); «Actualizar» y los reportes del
    menú refrescan el mes en curso y el anterior al abrirse.
- **Comparativo** (`qb.sales.compare`, vista SQL sobre tablas propias): presupuesto (cada
  revisión no obsoleta), pronóstico vigente y venta real **sin saldos ni desperdicio**, por
  cliente comercial, artículo, mercado y mes. Filtro «Presupuesto aprobado vigente» por omisión
  (sin él se suman revisiones: agrupe por Revisión para comparar REV0 y REV1).
  - **Cierre mensual por mercado:** presupuesto, venta y diferencia por mercado y mes.
  - **Anual:** lo mismo por año; con «Hasta el mes pasado», lo presupuestado a la fecha.
  - **Cliente y artículo:** presupuesto, pronóstico y venta en pesos, metros y kilos.
  - La **variación** por volumen, precio y tipo de cambio sigue en los renglones del
    presupuesto (3.6.2).
- **Faltantes** (`qb.sales.missing`): ventas del año sin mercado, productos vendidos sin
  conversión, renglones que cuentan 0, partes del cliente sin producto, proyectos perdidos con
  presupuesto y proyectos de producto nuevo sin desarrollo, cada uno con su botón para abrir el
  registro. Se recalcula cada noche y con «Actualizar».
- **Migración:** primera carga de la venta real y los faltantes.


## Artículo en desarrollo marcado en el pronóstico (19.0.3.10.0, E2, 2026-11-09)

Diseño v3, 2.11.4. El **pronóstico vigente** y la **Demanda por cliente** traen el **Estado del
artículo** (`dev_state`): el estado de desarrollo del producto (`sgi_dev_state` de Desarrollo de
producto) o, si no lo tiene, el de su artículo (el código en kilos de un desarrollo nuevo puede
no llevarlo).

- **En desarrollo o archivado:** se puede pronosticar (Planeación lo ve venir), pero el pedido
  no se confirma hasta pilotaje. Renglón en amarillo, etiqueta roja y aviso en la ficha.
- **En pilotaje:** se vende con aviso (los tres primeros lotes del primer pedido son el
  pilotaje). Etiqueta amarilla y aviso en la ficha.
- **Liberado o sin estado:** de línea, sin marca.
- Filtros «Artículo en desarrollo» y «Artículo en pilotaje» en los dos. En el pronóstico se
  recalcula solo cuando cambia el estado del artículo; la Demanda lo toma al reconstruirse (la
  migración la reconstruye una vez).
- La propuesta de pedidos de la E3 no propondrá un artículo en desarrollo; eso va con la E3.

## Desarrollos en la propuesta del año (19.0.3.9.0, E2, 2026-11-09)

Diseño v3, 2.11.3. «Armar propuesta del año» trae ahora, con **Traer desarrollos** (activo por
omisión), un renglón de Proyectos por mes para cada desarrollo abierto (de Cotización a
Pilotaje):

- **Cantidad:** el consumo anual del desarrollo entre 12 (las yardas pasan a metros) o, si no
  hay, el volumen mensual de la solicitud, por el **% del consumo** del asistente (100 por
  omisión). En la unidad de venta del producto (con el rendimiento del artículo, así que el
  consumo en kg ya se convierte); sin código, en metros o kilos, con la descripción del
  desarrollo (lleva el folio).
- **Meses:** desde el mes de la fecha requerida si cae en el año; sin fecha, los 12; con fecha
  posterior al año, no entra.
- **Precio:** el de la última cotización abierta (borrador o enviada) al cliente con ese
  producto; si no hay, el precio objetivo de la solicitud, por metro o por kilo; si no, el de
  la lista. Origen del precio «Cotización o precio objetivo del desarrollo».
- **No entra** si su producto ya se vendió en el periodo base (ya viene en la venta) o si sus
  renglones ya vienen del presupuesto de proyectos que se trae (que ahora copia también el
  desarrollo).
- Cada renglón guarda **Origen de la cantidad** (venta del periodo base, presupuesto de
  proyectos o consumo del desarrollo), la **Cantidad propuesta** y la etapa del desarrollo;
  Jessica decide cuánto entra. Filtro «Propuestos desde un desarrollo». El chatter del
  presupuesto resume cuántos desarrollos entraron y por qué no entraron los demás.
- Para MCP: `action_qb_build_proposal(..., include_dev=True, dev_pct=100.0)`.

## Proyectos ligados a Desarrollo de producto (19.0.3.8.0, E2, 2026-11-09)

Diseño v3, secciones 2.6 y 2.11. Presupuesto → **Proyectos comerciales** (`qb.sales.project`):
un cliente y un artículo en arranque. Mientras está vigente, su presupuesto va en «Proyectos
industrial» o «Proyectos confección».

- **Nace del desarrollo:** cuando un desarrollo de producto (C1) llega a Cotización o más
  adelante con cliente y artículo (el acabado del desarrollo, o el artículo de línea si el
  análisis dijo «Producto de línea»), se crea su proyecto comercial, con «Desde» = la fecha en
  que entró a Cotización. El mercado sale del equipo del desarrollo (Confección → Proyectos
  confección; lo demás → Proyectos industrial). Si Ventas o el presupuesto ya lo tenían dado
  de alta sin desarrollo (mismo cliente y producto), se liga en lugar de duplicarse.
- **Se pierde con el desarrollo:** cerrado sin producto o no factible → **perdido**; si el
  desarrollo vuelve a una etapa abierta, se reabre. Sus renglones se ven en los renglones del
  presupuesto con el filtro **Proyecto perdido**.
- **Renglón de Proyectos:** al crearlo (o pasarlo a Proyectos) crea o liga su proyecto
  comercial: el de su desarrollo si lo eligió; si no, el vigente del mismo cliente y producto
  (o descripción). Un renglón con **Desarrollo de producto** toma de él cliente, producto y,
  sin código, descripción, gramaje y ancho (masa y ancho nominales de la tabla de
  características). Filtro **Proyecto sin desarrollo**.
- **El renglón sin código toma el producto** cuando el desarrollo genera el suyo, solo en
  presupuestos en borrador o revisado (uno aprobado es evidencia; el proyecto avisa en el
  chatter qué renglones se quedaron sin producto y por qué).
- **Pasa a línea** a mano («Pasar a línea») o solo al aprobarse un presupuesto de un año
  posterior que lo presupuesta fuera de Proyectos con el mismo cliente y producto.
- **El desarrollo ve su venta:** botón **Presupuesto y venta** en la ficha del desarrollo (se
  ancla en la caja de botones nativa de Proyectos): abre el proyecto comercial con sus
  renglones de presupuesto, las semanas del pronóstico vigente y lo facturado al cliente de
  los códigos del artículo (metros, kilos y m²) desde que arrancó.
- **Migración:** crea los proyectos de los desarrollos que ya están en Cotización o más
  adelante con cliente y artículo, y liga los renglones de Proyectos que ya existen. No cambia
  importes ni productos de renglones aprobados.

Sigue en E2: la carga de la REV0/REV1 (que crea los 33 proyectos de
PRESUPUESTO PROYECTOS). Antes del 1-dic Diseño y Desarrollo liga a su desarrollo los proyectos
de 2026 de producto nuevo y cierra los «FT-» viejos (diseño v3, 2.11.6).

## Propuesta del presupuesto del año siguiente (19.0.3.7.0, 2026-11-05)

Presupuesto → **Armar propuesta del año** arma el borrador del presupuesto (para 2027 hace falta
el 9 de diciembre) con lo vendido en el periodo base, con la regla de venta del VE-02:

- **Periodo base:** los 12 meses cerrados antes de hoy (cada mes va al mismo mes del año nuevo) o
  el año anterior completo.
- **Renglones:** Industrial y Especiales por producto, cliente y mes; Confección por producto y
  mes, sin cliente (se presupuesta por promedios).
- **Cantidad:** la vendida ese mes × (1 + crecimiento del mercado), redondeada.
- **Precio:** el de la lista del cliente (o la presupuestal); si la lista no tiene regla del
  producto, el precio real promedio del periodo base en la moneda en que se facturó (origen
  «Precio real del periodo base»).
- **Tipo de cambio:** el presupuestal de Ajustes en los 12 meses (se puede cambiar en el asistente).
- **Proyectos:** opcionalmente, las líneas de proyecto de otro presupuesto, en los mismos meses.
- **No entran:** saldos y desperdicio, notas de crédito genéricas y servicios, productos de otra
  compañía y, si se pide, lo que vendió menos de un mínimo anual por producto y cliente.

Si ya hay un presupuesto abierto del año con renglones, no lo pisa. Crea la revisión siguiente si
el año ya tiene uno aprobado. El resumen (periodo, renglones e importe por mercado, de dónde salió
cada precio y lo que no entró) queda en el chatter. Para MCP:
`sgi.sales.budget.action_qb_build_proposal(año, growth={'industrial': 5, …}, base, min_amount,
project_source_id, rate)`.

## Poner cliente a varios renglones (19.0.3.6.4, 2026-11-05)

Un producto no puede tener renglones con cliente y sin cliente en el mismo mercado
(anti-doble-conteo), así que cambiar el cliente renglón por renglón chocaba siempre con el
primer renglón. **Poner cliente** (en el aviso rojo de renglones sin cliente, en el engrane de
la ficha y en la acción de la lista de renglones) pone el mismo cliente a todos los elegidos en
una sola escritura; abre con los renglones de Industrial y Especiales sin cliente ya elegidos.
Sin cliente, los renglones quedan para todo el mercado. Deja nota en el chatter; para MCP,
`action_qb_set_partner(line_ids, partner_id)` en el presupuesto.

## Cada mercado suma igual que el total (19.0.3.6.3, 2026-11-05)

«Diferencia por mercado» mostraba el tipo de cambio de los renglones y no la venta sin
presupuesto, así que los mercados no llegaban al total (en la REV2 faltaban 205 mil de tipo de
cambio de venta sin renglón). Ahora cada mercado se lee
`venta − presupuesto = volumen + precio + TC + sin presupuesto`: el TC es el de toda la venta del
mercado y lo que queda es venta sin presupuesto a tipo de cambio del presupuesto. La suma de los
mercados es la del encabezado.

## Diferencia por volumen, precio y tipo de cambio (19.0.3.6.2, 2026-11-05)

Las alertas de precio por renglón y mes (contador «Desviaciones de precio»,
colores, filtro) hacían mucho ruido y no decían por qué la venta no da el
presupuesto; salen de las pantallas (los campos se quedan: el VE-05 los lee).
En su lugar, la práctica de análisis de variaciones: la ficha separa **venta −
presupuesto al corte** en **volumen** ((cantidad real − presupuestada) × precio
presupuestal × TC presupuestal), **precio** ((precio real − presupuestal, en la
moneda de venta) × cantidad real × TC presupuestal), **tipo de cambio** (venta
a TC real − a TC presupuestal) y **venta sin presupuesto** (lo que no cae en un
renglón); suman exacto, también por mercado. Los renglones guardan su efecto
volumen, precio y tipo de cambio y el Análisis los suma por cliente, producto o
mes.

## Precios a la moneda de la lista (19.0.3.6.1, 2026-11-05)

Pestaña Tipo de cambio → «Pasar precios a la moneda de la lista» (solo en
borrador): los renglones con lista en dólares y precio presupuestal en pesos
(los de 2026, desde la migración de 3.5.0) pasan a dólares con «Tipo de cambio
para todos los meses», que queda en los 12 meses. El importe no cambia (salvo
redondeo) y la desviación de precio deja de mezclar el tipo de cambio.

Práctica de presupuesto en varias monedas: precio en la moneda de venta, un
tipo de cambio presupuestal (por mes si hace falta, congelado al aprobar), la
desviación de precio en la moneda de venta, y el cumplimiento de dos formas: a
tipo de cambio real (el contable, el del VE-02) y **a TC presupuestal** (lo
facturado en dólares por el TC del mes del presupuesto). La diferencia es el
**efecto del tipo de cambio**.

## PDF y Excel del presupuesto y del pronóstico (19.0.3.6.0, 2026-11-05)

- **Presupuesto** (ficha → Imprimir, y engrane → «Descargar Excel»): resumen por
  mercado y mes, y detalle por mercado con cliente, producto o proyecto, unidad,
  cantidad por mes, precio presupuestal e importe. El PDF es horizontal (carta)
  y trae el cumplimiento al corte. El Excel tiene las hojas Resumen, Cantidades e
  Importes MXN.
- **Pronóstico vigente** (Pronóstico → «Imprimir o descargar», también en
  Fabricación → Pronóstico de ventas): desde una semana, cuántas semanas, qué
  clientes y en qué unidad (metros, kilos o la de venta); matriz cliente y
  planta × producto × semana, en PDF horizontal o en Excel. Los totales por
  cliente solo salen en metros o kilos.

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
pronósticos viejos) y `tests/test_sales_project.py` (proyectos comerciales y
Desarrollo de producto), `tests/test_sales_actual.py` (venta real, comparativo, faltantes,
VE-02 y cierre) y `tests/test_sales_month_plan.py` (presupuesto del mes) y `tests/test_order_proposal.py` (pedidos desde
el pronóstico): corren en el build de Odoo.sh con
`--test-tags /quimibond_ventas_presupuesto`. Los lectores, el emparejamiento y
las unidades: `pytest tests_puros/` (sin Odoo, en el CI).
