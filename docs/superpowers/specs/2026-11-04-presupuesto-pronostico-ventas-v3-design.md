# Presupuesto, pronóstico y releases de ventas — diseño v3

**Prompt:** v3 de Jose Mizrahi (4-nov-2026), sustituye a la v2 del 30-sep en lo que choque.
**Módulo:** `addons/quimibond_ventas_presupuesto` (19.0.1.4.2 en `main`).
**Antecedente:** `2026-09-30-releases-pronostico-presupuesto-e0.md` (releases, partes, perfiles, lectores: se conservan).
**Estado:** diseño para revisar con Jessica Francisco (Administración de Ventas) y Planeación. **No hay código.**

Datos de producción consultados por MCP el 4-nov-2026 (`Quimibond - Odoo`). Este documento no trae volúmenes ni
precios por cliente; los totales de control de la REV1 están en la sección 1.3 del prompt.

---

## 0. Resumen en una página

1. **El pronóstico deja de ser un documento.** Hoy es un `sgi.sales.budget` por cliente y año, con estados, y solo
   llega al MPS si está «revisado». Pasa a ser **una sola tabla viva** `qb.forecast.line` (cliente × planta × artículo ×
   semana), sin estados, con bitácora de cambios. Planeación la ve completa, por cliente, sin que nadie la «revise».
2. **El presupuesto anual se queda en `sgi.sales.budget`**, pero un documento es **un año + una revisión** (REV0, REV1)
   con los 5 mercados dentro, no un documento por equipo. Precio capturable, TC por mes, renglones de proyecto sin
   código, congelado al aprobar. El KPI VE-02, el folio PPV, el formato F-P-A28-18 y los avisos siguen funcionando.
3. **El real es una sola vista** (`qb.sales.actual`, SQL): líneas de factura por fecha de factura, cliente, artículo y
   mercado, en MXN (`balance`), metros y kilos, con las mismas cuentas (401/402) que ya usa el VE-02. Reportes,
   cumplimiento y KPI leen de ahí: la cifra es una.
4. **W / IW se agrupan en un «artículo»**: la tela en metros. Sale de la ficha técnica de acabado (31 fichas hoy) y, si
   no hay ficha, de la regla de la DAT P-D02-01 (`I` + código de la tela) que ya escribió Fichas. Rendimiento y kilos a
   producir también salen de Fichas (un solo rendimiento, el de la lista en metros): Ventas no tiene factores propios.
   Los proyectos nacen del **Desarrollo de producto** del SGI (C1) y el desarrollo ve su venta (2.11). Shawmut se pronostica en WJ y se factura en
   IWJ: los dos caen en el mismo artículo y el cumplimiento deja de dar 0.
5. **Unidades**: todo renglón guarda la cantidad en su unidad original, en la unidad de venta del producto, en metros y
   en kilos, con el **origen del factor** (ficha, código, sin dato). La decisión de pasar a kilos solo cambia qué
   columna se muestra primero; no cambia el modelo.
6. **Entregas:** E1 pronóstico + Planeación + carga del forecast (producción 25-nov); E2 presupuesto anual + real +
   reportes (producción 9-dic, para armar el 2027 en diciembre); E3 pedidos desde la zona firme (producción 20-ene-2027).

---

## 1. Reporte de lo que hay hoy

### 1.1 Estado real en producción (corrige el prompt en tres puntos)

| Dato | Prompt | Producción 4-nov |
|---|---|---|
| Documentos `sgi.sales.budget` | 23 | **24**: 3 presupuestos + 21 pronósticos |
| Pronósticos en borrador | 20 | **14 en borrador, 7 «revisado»** (FXI 2026 y 2027, Lear 2026 y 2027, Saltillo 2026 y 2027, Shawmut 2026) |
| Releases aplicados | ninguno | **4 aplicados y 2 reemplazados el 2-nov-2026 17:04–17:05, aplicados por Jose J. Mizrahi**; 2 leídos sin aplicar (los del 24 y 25-sep), Zwisstex revisado con 2 partes sin producto |
| Presupuestos | Industrial aprobado 118.9 M | Industrial 2026 **aprobado** (118,880,612.48); Confección 2026 **borrador** (39,087,175, un peso abajo de la REV1); Especiales 2026 **borrador** (4,009,250, igual a la REV1) |

Pregunta 1 (sección 7): ¿las aplicaciones del 2-nov fueron una prueba en producción? Si lo fueron, el MPS ya recibió
esa demanda (se recalcula al aplicar, `models/qb_release.py:416`).

Ventas facturadas ene–sep 2026 en Odoo por equipo (líneas de producto, `balance`): Industrial 101.85 M, Confección
20.16 M, Especiales 3.72 M (igual al Excel al centavo), Distribuidor 0.005 M y **sin equipo 11.35 M** (ahí está la
venta de la rama ICOMATEX que el VE-02 ya excluye por cuenta, `sgi_indicator.py:519`). Industrial + Proyectos industrial
del Excel suman 101.46 M: la diferencia de 0.4 M se explica en la carga (sección 4.3).

### 1.2 Qué sirve y se conserva

| Pieza | Dónde | Por qué sirve |
|---|---|---|
| Lectores de Lear (AIAG) y FXI (SUM), emparejamiento de partes, conversión de unidades en Python puro con pruebas en CI | `release/lear_aiag.py`, `release/fxi_sum.py`, `release/part_match.py`, `release/units.py` | Funcionan y tienen pruebas; la yarda es exacta (`units.py:11`, 0.9144). **El Excel usa yd ÷ 1.0963: es un error de dedo de 1.0936** (0.25 % de más en todo lo que llega en yardas). |
| Catálogo de partes del cliente con sugerencia y confirmación | `models/qb_customer_part.py` | 53 partes, 38 confirmadas. Es la llave parte ↔ producto que pide la sección 2.1. |
| Perfiles de release: base de fecha, días de tránsito, zona firme (N semanas o autorización Fab), CUM | `models/qb_release.py:36-97` | Es justo el «perfil del cliente» de la sección 2.2. |
| Release con partes y semanas (`qb.release.part`, `qb.release.line`), zona por semana | `models/qb_release.py:425-525` | Guarda el release tal cual llegó: es la versión. |
| Anti-doble conteo del MPS: suma de todos los clientes (N1) y presupuesto omitido por producto + cliente + mes cubierto (N2), mes repartido en lunes (N8) | `models/sgi_sales_budget.py:1200-1279` | La regla es correcta; cambia la fuente (tabla nueva) y la llave (artículo en vez de producto). |
| Escritura en el MPS sin duplicar celdas | `models/sgi_sales_budget.py:1291-1321` | Se reusa tal cual. |
| Detección de precio basura (sin regla, regla global sobre precio de venta, mínimo plausible propio) | `models/sgi_sales_budget_line.py:259-334`, `models/product_min_price.py` | Se reusa para **sugerir** el precio y para marcar el hueco de lista. |
| Avisos del SGI: cierre de mes, cobertura semanal, revaluación de junio | `models/sgi_cron.py:25, 51, 190` | Se conservan; cambian de fuente (sección 2.9). |
| Ciclo borrador → revisado → aprobado, candado de aprobado, `action_revise` | `models/sgi_sales_budget.py:558-672` | Sirve para el presupuesto anual y sus revisiones. |

### 1.3 Qué no sirve y por qué

| # | Problema | Dónde | Efecto que se ve |
|---|---|---|---|
| N1 | El pronóstico es un documento por cliente y año con estados; editar una línea lo regresa a borrador | `sgi_sales_budget.py:61-67, 84-89`; `sgi_sales_budget_line.py:874-889` | Nadie lo marca «revisado»; un release que cruza de año crea otro documento (`qb_release.py:343-358`). |
| N2 | Al MPS solo llega lo «revisado» y sumado por producto | `sgi_sales_budget.py:1237-1257, 1341-1344` | Planeación no ve qué cliente pide qué; con 13 de 21 en borrador, al MPS le falta demanda. |
| N3 | El real del presupuesto filtra por **equipo de la factura** y por **producto exacto** | `sgi_sales_budget_line.py:486-493` | Shawmut: pronóstico en WJ (yd) y factura en IWJ (kg) → cumplimiento 0. Facturas sin equipo (11.35 M ene–sep) no cuentan. |
| N4 | El «comprometido» del pronóstico y la demanda neta también van por producto exacto | `sgi_sales_budget_line.py:599-656, 801-803` | Un pedido en IWJ no consume el pronóstico en WJ: **doble conteo** en el MPS. |
| N5 | «Facturado NO presupuestado» no distingue tipo de documento: en un pronóstico resta su comprometido del **facturado anual de todo el equipo** | `sgi_sales_budget.py:336-356` | Cada pronóstico muestra ~110 M. |
| N6 | El precio **no se captura**: siempre sale de la lista | `sgi_sales_budget_line.py:62-66, 211-225` | Lear y FXI sin precio en ninguna línea; Shawmut 240 de 244 en la versión WJ. El presupuesto no puede llevar el precio acordado con Dirección. |
| N7 | Una línea sin precio de lista **igual suma importe** con el precio de venta que la misma línea marca «NO usar» | `sgi_sales_budget_line.py:201-205, 334` | Importes de centavos por yarda en Shawmut (WJ) que entran al total. |
| N8 | Un solo TC presupuestal para todo el año | `sgi_sales_budget_line.py:336-354` (`budget_planning_rate`) | El forecast usa un TC por mes (19.0, 17.48, 17.17…); REV0 y REV1 tienen TC distintos. |
| N9 | Mercado = equipo de ventas obligatorio, un documento por equipo | `sgi_sales_budget.py:55-58, 494-515` | No caben Proyectos industrial ni Proyectos confección sin crear equipos falsos. |
| N10 | `product_id` obligatorio en la línea | `sgi_sales_budget_line.py:32-33` | Un proyecto sin código no se puede presupuestar. |
| N11 | Un perfil por cliente, sin planta; aplicar reemplaza el release anterior **del perfil** y pone en 0 lo que el release nuevo no trae | `qb_release.py:42, 381-388, 408-412` | Shawmut Silao quedó «reemplazado» por Shawmut Tennessee; Saltillo León por Saltillo. Las semanas de la otra planta se borran. |
| N12 | Solo dos lectores; los perfiles de Saltillo, Shawmut y Zwisstex dicen «FXI SUM» y sus releases se cargaron por fuera (versión 0) | `qb_release.py:23-26`; perfiles 3, 5, 7 | 18 de 21 clientes no tienen forma de entrar salvo la matriz del pronóstico. |
| N13 | Gramaje, ancho y rendimiento salen del código y del peso de `qb_costeo`, **no de la ficha técnica** | `qb_customer_part.py:111-137` | Producción ya tiene `ft_peso`, `ft_ancho`, `ft_rendimiento` y la liga W ↔ IW en la ficha de acabado (`quimibond_ficha_tecnica_tela/models/ficha_producto.py:66-82, 127`) y no se usa. |
| N14 | El KPI VE-02 suma `amount_budget` de **todos** los presupuestos aprobados y mide el real por cuentas 401/402; el reporte mide por equipo y producto | `quimibond_sgi/models/sgi_indicator.py:483-513` contra `sgi_sales_budget_line.py:458-530` | Dos cifras de cumplimiento distintas. Con REV0 y REV1 aprobadas a la vez, el KPI sumaría las dos. |
| N15 | No hay presupuesto del mes, ni cierre de mes como reporte, ni comparativo de revisiones, ni objetivo/mínimo, ni armado de la propuesta del año siguiente | — | Jessica sigue con F-P-A28-14 en Excel. |
| N16 | Planeación no tiene grupo ni vista; el menú pide `group_sgi_user` | `views/menus.xml:7-9` | Consulta el Drive. |

### 1.4 Lo que dicen los Excel (leídos el 4-nov)

Fuentes: `forecast.xlsx` vivo (Drive de innovacion@, modificado el 4-nov) y los adjuntos de correo guardados en la
memoria (Supabase): GENERAL 2026 rev01072026, INDUSTRIAL 26102026 (presupuesto de noviembre), CONFECCIÓN noviembre. El
detalle con cifras de clientes quedó fuera del repo; aquí solo estructura y conteos.

**Lo que confirma el diseño**

- **TC de la REV1 = 17.5.** Correo «PRESUPUESTO DE VENTAS GENERAL REV 1 JULIO - DICIEMBRE 2026» (25-jun): «de $19.2 a
  $17.5, aplicado a todo 2026», y la respuesta de Dirección «Si hay que considerar el TC 17.50». La hoja «2026
  industrial» trae 17.5. El 17.2 de la hoja ANUAL no se pudo ver (sección 4.3).
- **El presupuesto Industrial REV1 se reproduce:** la hoja «2026 industrial» (16 clientes, 46 renglones cliente ×
  producto, 4 columnas por mes: cantidad presupuestada, vendida, $ presupuestado, $ vendido) da 139,288,312 contra el
  control 139,288,311.59 (redondeo de celdas). $ = cantidad × precio USD × 17.5; seis clientes tienen precio en MXN. La
  unidad (m o kg) no está escrita: se deduce de si el renglón trae precio por m o por kg. Hay columnas de precio por
  periodo («precio m junio–septiembre», «julio–octubre»): el precio presupuestal cambia dentro del año, por eso va por
  renglón-mes (sección 2.6).
- **El real del Excel es el de Odoo:** septiembre industrial cuadra **por producto al centavo** con las facturas de Odoo
  (el TC por renglón es el de la factura). La diferencia total (27 mil) son líneas de desperdicio, saldos y chatarra que
  el Excel no cuenta, y las notas de crédito. Confección y Especiales de septiembre también cuadran con el correo de
  cierre. Ver pregunta nueva 15.
- **Un proyecto es una combinación producto–cliente, no un producto.** PRESUPUESTO PROYECTOS trae 33 renglones (32
  códigos, 11 clientes): 21 son productos nuevos, varios son productos de línea para un cliente nuevo (el mismo código
  es línea para Saltillo y proyecto para Zwisstex), uno es un cambio de código, y 6 combinaciones ya se vendían antes de
  2026 y siguen como proyecto. 3 códigos no existen en Odoo (proyectos sin código, como prevé 2.6). Ningún producto
  tiene `sgi_dev_state`. Corrige 2.6: la marca va en la combinación, no en el producto.
- **TC por mes en el presupuesto del mes y en el forecast:** 11 meses con TC en el industrial de noviembre (nov 18.67)
  y 12 en PRESUPUESTO MES 2026. Confirma `qb.sales.budget.rate` por mes.
- **Confección va solo por producto** (F-P-A28-17 noviembre: producto, unidad, cantidad y $ presupuestados y reales,
  sin cliente).
- **Las plantas son hojas separadas** en el forecast (FXI Cd. Juárez y Santa Teresa, Saltillo y León) o bloques
  separados (Shawmut Silao y Tennessee dentro de la hoja SHAWMUT). Confirma el perfil por planta (2.10).

**Lo que el Excel hace mal (y Odoo debe no repetir)**

- yd → m con **1.0963** en PRONÓSTICO y con 1.0936 en CONVERSIÓN y PRESUPUESTO: dos factores en el mismo libro.
- PRONÓSTICO 2027 copia los totales de 2026 en 9 clientes que no tienen bloque 2027 (el «pronóstico 2027» de esos
  clientes es el 2026 repetido).
- Rangos corridos en la tabla mensual y en CONVERSIÓN (octubre se salta la semana 40; un mes suma desde la semana 39 en
  adelante; Lear usa la llave de Shawmut; Lear divide entre el precio en lugar del rendimiento).
- PRESUPUESTO SEMANAL 2026 da `#N/A` en todos sus totales (un código que no está en PRECIOS) y casi todas sus semanas
  usan el TC de enero.
- La columna «precio / m» de PRECIOS mezcla USD por m, USD por kg (Lear) y MXN (seis clientes), sin decir moneda.
- Dos rendimientos con el ancho equivocado (Seiren, Contitech). El resto (98) coincide con la especificación o el código.

**PRECIOS (106 renglones) contra las listas de Odoo:** 47 iguales, 26 diferentes, 33 sin regla en Odoo; todos los
productos existen (uno solo inactivo) y todos los clientes tienen lista.

- **Shawmut:** 18 de 19 diferentes, con patrón: el precio por kg del Excel es el de Odoo **+6.0 %** en los 12 renglones
  por kg; los precios por m del Excel salen de dividir el kg entre el rendimiento. Hay además dos reglas duplicadas en
  Odoo sobre la misma plantilla que parecen precios por m² cargados como m.
- **Seiren:** 4 diferentes (−6.5 % a −6.8 %) contra reglas que **vencieron el 31-oct-2026**.
- **Pieles:** 23 sin regla. **Zwisstex:** 3 diferentes (+6 %), 2 sin regla. **Contitech:** 3 sin regla. **TQ-1:** 2 sin
  regla. **FXI:** 2 sin regla (uno es un producto inactivo).
- **Lear:** su «precio / m» es en realidad el precio por kg y coincide con Odoo.
- Copo, Saltillo, Poliuretanos, Maisa, Maclin, Entex, Bowen, Blancos y Woodbridge: todo igual.

Odoo vende la misma tela con **tres códigos**: el base en metros, el de «I» al frente en kilos y el de «M2» al final en
metros cuadrados. El artículo (2.2) tiene que juntar los tres.

**Para el lector del forecast (4.2):** 31 hojas (19 de cliente, ocultas); semanas 1–52 en columnas; bloques de año
apilados hacia abajo (fila del año, «FECHA DE ENTREGA», «PRODUCTO / PO» con la OC, renglones de producto con nuestro
código en A y la parte del cliente en B, TOTAL). La unidad no está en la hoja del cliente, solo en PRONÓSTICO (yd en
FXI, Shawmut y Saltillo; kg en Pieles; m el resto). Seis hojas se salen del layout (semana 1 en otra columna, sin
columna de parte, subbloques por planta, renglones de PO o de conversión intercalados, cantidades escritas como texto
«por confirmar»). Por eso el lector no es genérico: lleva un **mapa por hoja** (fila del año, columna de la semana 1,
columna de parte, unidad) que se revisa con Jessica una vez.

**Hallazgo en la memoria (repo quimibond-intelligence):** el extractor de adjuntos solo guarda las **10 primeras
hojas** de un Excel (`attachments-extract`, `wb.SheetNames.slice(0, 10)`). Por eso la memoria no tiene ANUAL, MENSUAL ni
las hojas de confección, proyectos y especiales del presupuesto general.

### 1.5 Lo que contestaron el correo y Odoo (revisado el 4-nov)

Fuentes: la memoria de correo (Supabase), el Gmail de Jose y Odoo de producción, solo lectura. Detalle fuera del repo.

- **Shawmut +6 %: el Excel está viejo, Odoo tiene el precio bueno.** Fue un recargo temporal del 6 % del 1-jun al
  30-sep-2026 (Jessica a Shawmut, 19-may, autorizado por Jacobo; nota de Jessica en el chatter de «TARIFA SHAWMUT LLC
  (USD)» del 4-jun; Jessica a Shawmut el 18-ago: «The 6% surcharge will finish on September 30th, 2026 … no extension or
  replacement»). Las reglas en kg de esa tarifa se reescribieron el 30-sep a las 23:16 UTC, al terminar el recargo. La
  hoja PRECIOS del forecast sigue con el recargo.
- **Releases aplicados el 2-nov: arranque sin consecuencias en producción, pero dejó datos vivos en el MPS.** Los aplicó
  Jose entre 17:04 y 17:05 UTC; crearon **14 programas nuevos del MPS** (ids 131–144) con **624 celdas** (450 en meses ya
  pasados; 11 programas en kg y 3 en metros). Nadie lanzó reabastecimiento desde ellos y ninguna de las 88 órdenes de
  fabricación desde el 2-nov salió del MPS. La minuta de la 2.ª etapa de Odoo (3-nov) deja pendientes las «pruebas en el
  MPS … con pedidos actuales». Pregunta 1.
- **Planeación (contesta las preguntas 12 y 13):** tejido se programa **en kilos** con una **ventana móvil de un mes
  desde el día del corte** (correos semanales «Progr. de Cantidades por Tejer del 06/Oct al 07/Nov», no mes calendario;
  en diciembre se alarga); acabado se programa **en metros, semanal, en piezas de primera que equivalen a un baño**. La
  demanda sale del forecast semanal de Ventas. Planeación ya se quejó de huecos del forecast (FXI Santa Teresa sin
  pronóstico de agosto, 9-jul) y de semanas que rebasan la capacidad de Rama.
- **Releases por planta (contesta la 10):**

  | Cliente / planta | Día | Formato |
  |---|---|---|
  | Shawmut Silao (planta 12) | martes | .csv |
  | Shawmut Tennessee (planta 50, PO 70152) | martes, correo aparte | .xls |
  | FXI Cd. Juárez (planta 1500) | viernes | .xlsx «SUM» (el lector que ya existe) |
  | FXI Santa Teresa | sin release: órdenes de compra por correo | — |
  | Saltillo Lamination Saltillo | lunes | .pdf (dos archivos) |
  | Saltillo Lamination León | miércoles | .pdf |

  Ningún cliente manda EDI de entrada. En Odoo **las plantas no existen**: Shawmut, FXI y Saltillo facturan y entregan a
  la dirección de la empresa (sin direcciones de entrega hijas); la única pista es la OC del cliente en el pedido.
- **Base del precio por cliente (contesta la 8):** USD por metro (TQ-1, Seiren, Zwisstex, Contitech, Saltillo,
  Woodbridge, Copo, Bowen), USD por kilo (Shawmut, Lear, FXI), MXN por metro (Blancos, Poliuretanos, Maclin, Entex,
  Maisa) y MXN por kilo (Pieles). La moneda de cada lista es la de sus facturas. Pero la tarifa de Shawmut tiene 13 reglas
  en m² y 2 en metros aunque todo se le factura en kg; FXI 3 y Lear 1 en m².
- **Desperdicio y saldos (pregunta 15):** 264 mil pesos facturados ene–oct (0.19 % de la venta), casi todo de Industrial y
  en la misma cuenta 401 que la venta. Los parámetros de saldos de Fichas no están en la base (Fichas usa sus códigos por
  omisión). No hay correo que explique por qué el cierre los excluye; la minuta del 4-nov pide «contemplar la merma de los
  productos de línea» (Jessica, César París).
- **Presupuesto general 2026:** el vigente es el rev01072026 del correo de cierre de septiembre (1-oct). Está en el Gmail
  de Jose y en el almacenamiento de la memoria, pero ninguno de los dos se puede bajar desde aquí (Gmail por MCP no baja
  adjuntos; la red del contenedor no llega a Supabase). **No existe un «GENERAL REV0»:** la REV0 son el «PRESUPUESTO 2026
  INDUSTRIAL rev1» (1 y 11-dic-2025) y el de confección (9 y 15-dic-2025); el primer GENERAL es del 2-mar-2026.
- **Proyecto → línea:** no hay criterio escrito de Ventas. El P-D01 (Diseño y Desarrollo, 13-oct-2026) libera el producto
  tras al menos 3 pilotajes estables con acta firmada por Ventas, Manufactura y Calidad; Jessica corrigió en dic-2025 un
  producto de Shawmut que «debería solo ir [en] proyectos».
- **Desarrollos viejos (para la limpieza de Diseño):** de los 95, **3 ya se venden** al cliente del proyecto y deberían
  pasar a «Liberado»; **22** no tienen venta ni actividad desde enero–marzo y deberían cerrarse sin producto; **6** hay que
  revisarlos con Ventas (ventas chicas, posiblemente muestras); **41** están vivos; **15** no tienen cliente o datos (uno es
  el cliente de prueba de C1). Los 4 liberados no tienen `sgi_dev_state`; solo 12 productos en toda la base lo tienen. La
  lista va para Diseño (fuera del repo).

**Por qué los releases no se usan (sección 2.1 del prompt):** (a) solo Lear y FXI tienen lector; (b) aplicar exige que
cada parte esté confirmada una por una y que el release esté «revisado» (`qb_release.py:323-341`); (c) aplicar escribe en
un documento por cliente-año que Planeación no ve, así que a Jessica no le ahorra nada: sigue necesitando su Excel para
consolidar, valorizar y mandar a Planeación; (d) la falta de planta (N11) rompe a Shawmut y Saltillo; (e) los dos
releases de septiembre se leyeron y nadie los revisó: no hay bandeja que avise que llegó uno.

---

## 2. Diseño

### 2.1 Principios

1. **Tres capas, tres modelos.** Release (lo que mandó el cliente, inmutable) → pronóstico vigente (una tabla) →
   presupuesto anual (documento congelado por revisión). El real es una vista que todos leen.
2. **La llave de medición es el artículo, no el producto.** Un artículo agrupa la tela en metros (W…) y su acabado en
   kilos (IW…). Todo cruce pronóstico ↔ pedido ↔ factura ↔ presupuesto va por cliente + artículo.
3. **Ninguna conversión inventada.** Cada cantidad convertida guarda de dónde salió el factor; sin dato, el renglón
   aparece en «Faltantes» y en metros/kilos vale vacío, no cero.
4. **El precio del presupuesto se captura y se congela; el del pedido sale de la lista.** La lista sugiere el precio
   presupuestal; nunca lo sustituye un precio basura.
5. **Nada de estados en lo vivo.** El pronóstico no se revisa ni se aprueba: se ve, se cambia y queda bitácora.

### 2.2 Artículo y unidades (sección 2.7 y 2.5 del prompt)

**Nada de esto se inventa en este módulo: se lee de Fichas técnicas** (`quimibond_ficha_tecnica_tela`, que el SGI ya
instala). Fichas ya resolvió las tres preguntas de unidades para Producción, y Ventas debe usar las mismas respuestas
para que el pronóstico, la lista de materiales y el MPS den el mismo número:

| Pregunta | Quién la contesta hoy en Fichas | Dónde |
|---|---|---|
| ¿Cuál es la tela en kg de esta tela en metros (W ↔ IW)? | La ficha de acabado: «4. Tela acabada (m)» `product_acabado_id` y «3. Acabado en proceso (kg)» `product_proceso_id`; sin ficha, la regla de código `codigo_proceso()` (14 o 16 posiciones con J en la 9 → «I» al frente) | `models/ficha_tecnica_acabado.py:27`, `models/acabado_ruta.py:30-40`, `ft_tela_m_de_kg_id` en `ficha_producto.py:127` |
| ¿Cuántos metros da un kilo? | **Un solo rendimiento** por tela (decisión de Dirección General, fichas 2.24.0, 3-nov-2026): metros fabricados ÷ kilos de tela consumidos en las órdenes terminadas de 12 meses, sin saldos; sin órdenes, el capturado en la ficha | `rendimiento_tela_acabada`, `models/rendimiento_real.py:1-110` (cron nocturno); `peso_real` = el peso que da ese rendimiento al ancho de venta |
| ¿Cuántos kilos hay que producir para entregar N metros? | La lista en metros: metros ÷ rendimiento ÷ «primera para planear» de la tela (`qb_costeo`, fracción de primera que alcanzan 9 de cada 10 lotes) | `mrp.bom._ft_conversion`, `models/capacidad_conversion.py:125-169`; `primera_para_planear()` línea 36 |

Con eso, en `product.product` (guardados, recalculados por el cron nocturno de Fichas y al cambiar ficha o código):

| Campo | Qué es | De dónde sale, en orden |
|---|---|---|
| `qb_articulo_id` (m2o `product.product`) | La tela en metros que representa al producto en la medición | 1) ficha de acabado (`ft_producto_m_id`: para el IW, su W; para el W, él mismo); 2) `ft_tela_m_de_kg_id` (la regla `codigo_proceso()` de Fichas al revés); 3) código con `M2` al final (la versión en m² que Odoo ya tiene en listas de precios): el mismo código sin `M2`; 4) el desarrollo que lo creó: `sgi_dev_product_id` del proyecto (`sgi_dev_project_id` del producto, sección 2.11); 5) excepción capturada (`qb.articulo.excepcion`, para cambios de código como «WJ045… → WJ050…»); 6) él mismo |
| `qb_m_por_kg` | Rendimiento para convertir venta en m ↔ kg | 1) `rendimiento_tela_acabada` de la ficha (el real); 2) especificación de la ficha `1000 ÷ (ft_peso × ft_ancho/100)`; 3) código (gramaje y ancho de la DAT P-D02-01); 4) vacío |
| `qb_conv_origen` | `ficha_real` / `ficha_especificacion` / `codigo` / `sin_dato` | — |

**Dos kilos distintos, y la vista de Planeación muestra los dos:**

- **Kilos de venta** (`qty_kg`): lo que pesa lo que se vende, con `qb_m_por_kg`. Es lo que se compara contra la factura.
- **Kilos a producir** (`qty_kg_producir`, solo en la Demanda de Planeación): metros ÷ rendimiento ÷ primera para planear,
  **la misma cuenta que hace la lista en metros**. Para un cliente que compra en kilos el acabado en proceso (Shawmut
  compra IWJ), los kilos a producir son los kilos pedidos ÷ primera para planear.

Esto contesta la pregunta 3: para `WJ038Q22JNT160` la especificación da 16.45 m/kg y la ficha 14.19 m/kg; manda la
ficha, porque es el número con el que Producción explota la lista y con el que el MPS pide los kilos. La especificación
queda como respaldo cuando la tela no tiene órdenes ni ficha. El caso WC090 del prompt también se corrige solo:
`WC090Q11JNT168` tiene ficha (90 g/m², 168 cm) y los demás WC090 leen 90 del código.

**Unidades del cliente:** `release/units.py` se queda como lógica pura, pero el catálogo de unidades del cliente (MT, LY,
YD, KG, M2…) pasa al catálogo de unidades de Fichas (`ficha.tecnica.unidad`, con su `uom_id` y su campo «También se
escribe»: `models/unidades.py`): una sola lista de alias para fichas, Excel y releases.

**Conversión:** una función `qb_convertir(producto, cantidad, de_unidad, a_dimension)` en este módulo que solo junta lo
anterior (ningún factor propio). Metros ↔ yardas exacto (0.9144); metros ↔ kilos con `qb_m_por_kg`; metros ↔ m² con el
ancho de venta de la ficha. Si `qb_conv_origen = sin_dato`, devuelve `None`. La misma función la usan el release, la
carga del Excel, el desarrollo (2.11) y el real.

**Cada renglón con cantidad (pronóstico, presupuesto, real, propuesta de pedido) guarda:**

- `qty_origen` + `uom_origen` (lo que escribió el cliente o el usuario, en su unidad),
- `qty_venta` en la unidad de venta del producto del renglón,
- `qty_m` y `qty_kg` (vacíos si no hay dato),
- `conv_origen`.

Así, la decisión de pasar todo a kilos solo cambia qué columna se muestra primero; no cambia tablas ni reportes. El
producto del pedido y del MPS es **el que compra el cliente** (W en metros o IW en kilos): la lista de materiales y Fichas
hacen el resto.

### 2.3 Pronóstico vigente: `qb.forecast.line`

Una fila por **cliente comercial × planta × artículo × semana (lunes)**. No hay cabecera.

| Campo | Notas |
|---|---|
| `partner_id` (comercial), `ship_to_id` (planta, opcional) | Shawmut Tennessee y Silao, Saltillo y León, FXI Cd. Juárez y Santa Teresa son plantas del mismo cliente |
| `product_id` | El producto que pide el cliente (puede ser W o IW); `articulo_id` related guardado |
| `customer_part_id` | Parte del cliente (`qb.customer.part`), opcional (confección no lleva) |
| `week` (lunes de la semana de **embarque**), `month` guardado | La fecha del cliente y los días de tránsito ya los resuelve `qb.release.line._compute_dates` |
| `qty_origen`, `uom_origen`, `qty_venta`, `qty_m`, `qty_kg`, `conv_origen` | Sección 2.2 |
| `mercado` | Sección 2.6 |
| `zona` | `firme` / `materia_prima` / `planeado`, del perfil del cliente (regla de `qb_release.py:503-525`); sin perfil, 3 semanas firmes (parámetro) |
| `origen` | `release` / `captura` / `excel` (carga inicial y pegado) / `anual` (forecast anual tipo Contitech) / `historico` (migrado de los 21 documentos) |
| `release_id`, `customer_po`, `release_ref` | Del release que escribió la semana |
| `qty_pedido`, `qty_neta` | Pedidos confirmados del mismo cliente + planta + artículo con compromiso en esa semana; `qty_neta = max(0, qty_venta − qty_pedido)` |
| `active` | Las semanas pasadas no se borran; se ocultan |

Restricción única: `(company, partner, ship_to, product, week)`.

**Versiones (sección 2.2, «la forma la propones tú»):** dos niveles.

1. **El release es la versión completa** (ya existe, inmutable). Comparar dos releases = comparar sus `qb.release.line`.
2. **`qb.forecast.change`**: cada vez que una semana cambia se escribe una fila (fecha, usuario, release, cantidad
   antes y después, en unidad de venta y en metros, motivo). Desde el release: botón «Cambios contra el anterior»
   (lista agrupada por parte con +/−). Desde Planeación: filtro «Cambió en los últimos 7 días».

Además, al enviar el **presupuesto del mes** se congela una foto del pronóstico de ese mes (sección 2.5): con eso se
puede medir cuánto se movió el pronóstico contra lo que se presupuestó en el mes.

**Captura de los clientes sin lector:** vista matriz (`web_grid`, dependencia que el módulo ya tiene) filtrada por
cliente: filas = parte del cliente (o producto), columnas = semana, celda = cantidad **en la unidad del cliente**. Es la
misma hoja que Jessica llena hoy. Y un asistente **«Pegar semanas desde Excel»** que lee el bloque de 52 semanas de una
hoja del `forecast.xlsx` (mismo lector que la carga inicial, sección 4.2): durante la transición Jessica puede seguir
recibiendo en Excel y pegar.

> **Hecho en 3.1.0 (2026-11-05):** la matriz es una vista `grid` de `qb.forecast.line` (renglón cliente, planta,
> producto y unidad; columna semana; celda `qty_origen`) con `grid_update_cell` propio (crea, suma, quita en cero,
> bitácora). «Pegar semanas desde Excel» es el asistente `qb.forecast.paste`: lee el portapapeles de Excel
> (tabuladores) en vez del archivo, con el lector puro `release/forecast_paste.py`; mismas reglas que la carga
> (lo pasado y lo de release no se tocan). Lo revisan Jessica y Planeación el 11-nov.

**Confección:** la misma tabla con `partner_id` vacío y `mercado = confeccion` (pronóstico por producto). Lo capturan
los vendedores de confección por matriz producto × mes (la semana es el primer lunes del mes; a Planeación le llega
repartida en los lunes, igual que hoy el presupuesto, `sgi_sales_budget.py:1200`).

### 2.4 Vista de Planeación y demanda al MPS

> **3.0.0 (2026-11-05, decisión de Jose): el MPS queda desconectado por ahora.** El pronóstico no escribe en el
> Programa Maestro; Planeación usa Demanda por cliente (Fabricación → Pronóstico de ventas). Se borraron las 624
> celdas del envío del 2-nov (programas 131–144, los programas se quedan). Lo que esta sección dice del MPS queda
> para cuando se vuelva a conectar.

**Modelo de lectura `qb.demand.line`** (en E1a quedó como **tabla que se reconstruye** cada mañana, al cargar el forecast y con «Actualizar», y no como vista SQL: la conversión y la primera para planear viven en Fichas, en Python) que une, por cliente + planta + artículo + semana:

- `tipo = pedido`: líneas de pedido confirmadas pendientes de entregar (`product_uom_qty − qty_delivered`), por semana de
  `commitment_date` (o `expected_date`), y
- `tipo = pronostico`: `qty_neta` del pronóstico (lo que los pedidos no cubren).

Columnas: cliente, planta, mercado, artículo, producto, semana, mes, zona, tipo, cantidad en unidad de venta, metros,
kilos, `conv_origen`. Medidas sumables: metros y kilos (la unidad de venta solo se suma dentro de un producto).

**Acción «Demanda por cliente»** (menú Planeación → Demanda), vista pivote por defecto:

- filas: artículo → cliente; columnas: semana; medida: metros y kilos (las dos);
- filtros guardados: **«Acabado: 3 semanas»** (semana actual y dos siguientes, en metros; con E3, solo pedidos),
  **«Tejido: próximo mes»** (pedido + pronóstico de **hoy a un mes**, ventana móvil como el correo semanal de Planeación,
  en kilos a producir), zona firme / planeada, mercado, cliente, producto;
- lista con las mismas columnas y exportación nativa a Excel; gráfica opcional.

**Permisos:** grupo nuevo `Planeación (consulta)` con lectura de `qb.demand.planning`, `qb.forecast.line`,
`qb.forecast.change`, releases y partes; sin escritura. Menú propio bajo Fabricación → Planeación y bajo Ventas →
Presupuesto y pronóstico.

**MPS:** la demanda que se escribe en `mrp.product.forecast` sale de la misma fuente:

- por artículo y semana: suma de `qty_neta` del pronóstico + pedidos (la regla `max(pronóstico, comprometido)` de hoy,
  pero por cliente + artículo, que cierra el doble conteo N4);
- más el presupuesto anual vigente solo donde ningún pronóstico cubre cliente + artículo + mes (regla N2 de hoy), para
  los meses más allá del horizonte de los releases;
- se escribe en **el producto que compra el cliente**: W en metros (la lista en metros de Fichas explota los kilos del
  IW con el rendimiento y la primera para planear) o IW en kilos (Shawmut). Así el MPS no necesita una conversión propia
  y la decisión de kilos no cambia nada aquí. Si el producto no tiene programa en el MPS (hoy 48 en Toluca), la Demanda
  de Planeación lo marca y ofrece crearlo.
- se recalcula al aplicar un release, al guardar la matriz y en el cron diario; ya no depende de ningún estado.

**Abrir la demanda del MPS por cliente:** el MPS de Odoo es una pantalla OWL de Enterprise. Se agrega un parche de
JavaScript chico al menú de la fila del producto («Demanda por cliente») que abre `qb.demand.planning` filtrado por el
artículo y el horizonte del MPS. Como el CI no instala Enterprise, se prueba en el build de Odoo.sh; si el parche no
pasa, queda el respaldo de un botón en la ficha del producto y en la lista de programas.

### 2.5 Presupuesto del mes: `qb.sales.month.plan`

Documento por mes (folio, mes, TC del mes, estado `borrador` / `enviado`), creado con «Armar presupuesto de
<mes siguiente>» a fin de mes:

- líneas = foto del pronóstico vigente del mes (cliente, planta, artículo, mercado, línea/proyecto, cantidad en metros,
  kilos y unidad de venta) × precio presupuestal (sección 2.6, regla de precio) × TC del mes;
- Jessica puede ajustar cantidades de la foto antes de enviar (no cambian el pronóstico vivo);
- totales por mercado y por productos de línea / proyectos (los dos números del correo de octubre);
- botón «Enviar a Dirección»: PDF con el formato F-P-A28-14 (industrial) y correo desde Odoo con los totales; queda
  `enviado` y congelado.

Confección: los vendedores llenan su parte en el mismo documento (líneas sin cliente), con regla de registro que solo
les deja tocar `mercado in (confeccion, proyectos_confeccion)`.

### 2.6 Presupuesto anual: `sgi.sales.budget` reorganizado

**Por qué se queda en `sgi.sales.budget` y no en un modelo nuevo:** es un documento que se aprueba y se congela, y eso ya
lo hace (ciclo, candado, `action_revise`, folio PPV, formato F-P-A28-18, KPI VE-02, Diagnóstico del SGI, avisos de
cierre y de junio). Renombrar costaría las mismas tablas, XML IDs y referencias que en 57.11.0 se decidió no tocar. Lo
que sí cambia es qué es un documento y qué lleva la línea. El pronóstico, en cambio, sale del modelo (sección 2.3):
`kind = 'pronostico'` queda solo para los 21 documentos históricos, en solo lectura.

**Cabecera = año + revisión** (REV0, REV1…), los 5 mercados dentro. `team_id` deja de ser obligatorio en la cabecera.

| Campo nuevo | Notas |
|---|---|
| `revision_label` (REV0, REV1) y `revision_date` | `revision` numérico se conserva |
| `rate_ids` → `qb.sales.budget.rate` (mes, TC USD/MXN) | Un TC por mes, o el mismo en los 12 con «Aplicar a todos». Se congela al aprobar. Sustituye a `budget_planning_rate` para este documento |
| `objetivo_pct` (20), `minimo_pct` (15) | Default de Ajustes, editables por revisión |
| `base_anio_anterior` | Venta del año anterior por mercado, de `qb.sales.actual` (calculado, guardado al aprobar) |
| estado `reemplazado` | Al aprobar REV1, la REV0 aprobada pasa a `reemplazado` (congelada, comparable). Solo una aprobada por año |

**Línea (`sgi.sales.budget.line`) — cambios:**

| Campo | Notas |
|---|---|
| `mercado` (selección) | `industrial`, `proyectos_industrial`, `confeccion`, `proyectos_confeccion`, `especiales`. Sustituye a `team_id` como dimensión; `team_id` queda calculado del mercado |
| `partner_id` | Obligatorio en industrial y especiales; vacío en confección |
| `product_id` **opcional** + `articulo_id` | Sin producto: renglón de proyecto con `proyecto_descripcion`, `proyecto_gramaje`, `proyecto_ancho_cm`, unidad. «Ligar producto» lo cambia por el código cuando se da de alta y conserva la historia |
| `qty`, `uom_id`, `qty_m`, `qty_kg`, `conv_origen` | Sección 2.2 |
| `price_input`, `price_currency_id` (USD/MXN), `price_basis` (`m` / `kg` / unidad de venta) | **Capturable**. Al crear, se sugiere de la lista del cliente con las reglas de hoy (`_sgi_pricelist_price`); si la lista no tiene regla o el precio es basura, el campo queda vacío y el renglón cuenta en «Sin precio» (corrige N7: un renglón sin precio importa 0, nunca el precio de venta) |
| `price_origen` | `lista` / `captura` / `excel` / `proyecto` |
| `amount_budget` (MXN) | `qty` en la base del precio × precio × TC del mes del documento. Mismo nombre: el VE-02 lo sigue leyendo |
| `qty_origen_propuesta`, `qty_sugerida`, `origen_cantidad` | Sección «Armado de la propuesta» |

`team_id` en la cabecera y la restricción «uno por equipo y año» (`sgi_sales_budget.py:494-515`) se cambian por «una
revisión aprobada por año y compañía».

**Línea o proyecto (sección 2.4 del prompt):** no se puede derivar de lo que hay. `sgi_dev_state` existe en el producto
pero **ningún producto vendido en 2026 lo tiene** (0 de 121.9 M en Industrial + Confección), y `sgi_first_sale_date`
marcaría 102 M como «nuevos» desde julio de 2025, así que no sirve. Y la hoja PRESUPUESTO PROYECTOS muestra que un
proyecto es una **combinación producto–cliente** (sección 1.4): el mismo código es línea para un cliente y proyecto para
otro. Propuesta:

1. **`qb.sales.project`** (cliente comercial, artículo o renglón sin código, `desde`, `hasta`, **proyecto de desarrollo**,
   nota): la combinación en arranque. Mientras esté vigente, su venta y su pronóstico cuentan como proyecto de su mercado
   base. Dos orígenes (decisión de Jose, 4-nov): un **producto nuevo** nace del proyecto de Desarrollo (sección 2.11) y no
   se captura aparte; un **producto de línea para un cliente nuevo** no pasa por Desarrollo y lo da de alta Ventas
   directamente (cliente + artículo existente).
2. El presupuesto la declara: un renglón en `proyectos_industrial` o `proyectos_confeccion` crea o liga su
   `qb.sales.project` para ese año. La carga de la REV1 crea las 33 de PRESUPUESTO PROYECTOS.
3. La combinación «se gradúa» cuando el presupuesto del año siguiente la pone en línea (se cierra `hasta`), o a mano.
4. Base del mercado: el equipo de ventas del **cliente** (`res.partner.team_id`), no el de la factura; si no tiene, el de
   la factura; si tampoco, «Sin mercado» visible en el reporte (sección 2.7).

Sin equipos falsos y sin marcar el producto.

**Armado de la propuesta del año siguiente** (asistente «Armar propuesta <año>», crea la REV0 en borrador):

Por cliente × artículo × mes, la cantidad sale del primer origen disponible, y el renglón dice cuál fue:

1. `release`: semanas del pronóstico vigente que caen en el año (Lear trae 52 semanas);
2. `anual`: el forecast anual del cliente (Contitech), cargado como pronóstico con `origen = anual`;
3. `tendencia`: venta de los últimos 12 meses del mismo mes × factor de tendencia del cliente (venta últimos 6 meses
   contra los mismos 6 del año anterior, acotado a ±30 %, editable por cliente en el asistente);
4. `historico`: venta del mismo mes del año en curso.

Cada renglón guarda `qty_sugerida` y `origen_cantidad`; Jessica escribe `qty` encima. El precio se sugiere de la lista
vigente (sección anterior). El asistente muestra antes de crear el total por mercado contra el año anterior, el objetivo
(+20 %) y el mínimo (+15 %). Dirección aprueba (grupo `quimibond_sgi.group_sgi_director`, como hoy, `sgi_sales_budget.py:593`).

**Revaluación de junio:** el cron de hoy (`sgi_cron.py:190`) avisa; con el diseño nuevo «Nueva revisión» copia la
vigente con TC y precios capturables y deja la anterior en `reemplazado`.

### 2.7 Real: `qb.sales.actual` (vista SQL)

Una fila por línea de factura de cliente publicada (`out_invoice`, `out_refund`) **en las cuentas de ventas del VE-02**
(parámetro `quimibond_sgi.sales_accounts`, 401/402, `sgi_indicator.py:519-528`):

`fecha` (fecha de factura), `mes`, `partner_id` (comercial), `ship_to` (dirección de entrega de la factura),
`product_id`, `articulo_id`, `mercado` (regla de la sección 2.6), `es_proyecto`, `amount_mxn` (`-balance`; la nota de
crédito resta sola), `amount_currency` y moneda, `tc` (`balance / amount_currency`), `qty_venta` con signo, `qty_m`,
`qty_kg`, `conv_origen`, `team_id` de la factura (solo informativo).

**Desperdicio, saldos y chatarra:** el cierre del Excel no los cuenta (27 mil en septiembre industrial) y el VE-02 sí,
porque van a la misma cuenta 401. La vista los marca con `es_saldo` con la lista de saldos que ya tiene Fichas
(`_ft_saldo_products()`, parámetros `quimibond_ficha_tecnica_tela.saldo_segunda` y `.saldo_desperdicio`, más
`DESPERDICIO TEJIDO`: `models/ruta_familia.py:57-61`). **Se excluyen del cumplimiento y del VE-02** (decisión de Jose,
4-nov): el VE-02 cambia su filtro de cuentas por esta misma vista, y así reporte, KPI y cierre de Jessica dan la misma
cifra. Pesan 0.19 % de la venta (264 mil pesos ene–oct 2026). Como los parámetros de saldos de Fichas no están en la
base, E2 los crea con sus códigos actuales para que la lista sea explícita.

Se calcula al leer: no hay foto que refrescar (hoy `amount_real` es una foto que se pone vieja, `sgi_sales_budget_line.py:96-99`).
Metros y kilos se calculan en SQL con los campos guardados del producto (sección 2.2).

**El KPI VE-02** pasa a leer el presupuesto de la revisión aprobada vigente y el real de esta vista
(`_sgi_year_budget_amount` y `_sgi_net_invoiced`, `sgi_indicator.py:483, 563`). Así reporte y KPI dan la misma cifra
por construcción, y la prueba de aceptación lo compara mes por mes de enero a octubre.

### 2.8 Reportes (sustituyen MENSUAL y ANUAL)

Todos sobre un modelo de lectura `qb.sales.compare` (vista SQL que une presupuesto vigente, pronóstico, foto del
presupuesto del mes y real por cliente + artículo + mercado + mes).

| Reporte | Contenido | Forma |
|---|---|---|
| **Mensual por mercado** | presupuesto, venta, % cumplimiento, por mes y acumulado | pivote + PDF «Cierre de ventas general <mes>» y correo desde Odoo (botón en el cierre) |
| **Anual** | venta del año anterior, objetivo, mínimo, presupuesto, vendido, presupuestado a la fecha, diferencia; comparativo REV0 vs REV1 | pivote + PDF |
| **Cliente × artículo** | presupuesto anual vs pronóstico vs real, en $, metros y kilos | pivote, filtros por mercado y línea/proyecto |
| **Variación** | volumen, precio y TC por cliente × artículo × mes | lista con totales |
| **Faltantes** | productos sin gramaje/ancho, partes sin producto, renglones sin precio, ventas «sin mercado», códigos que no cumplen la nomenclatura | lista, desde el cierre y desde la carga |

Fórmula de la variación (cantidad en la base del precio, precio en la moneda de la lista):

- volumen = (Q real − Q ppto) × P ppto × TC ppto
- precio = (P real − P ppto) × Q real × TC ppto
- TC = (TC real − TC ppto) × P real × Q real

La suma da real − presupuesto; para un precio en MXN el efecto TC es 0. Lo vendido sin presupuesto va como volumen.

### 2.9 Avisos del SGI

- **Cierre de mes** (`_sgi_monthly_close_steps`): igual, pero lee `qb.sales.compare` y adjunta el PDF mensual.
- **Cobertura semanal** (`cron_forecast_coverage`): pasa a leer `qb.demand.planning`: semanas firmes sin pedido y pedidos
  sin pronóstico, por cliente + artículo (hoy por producto exacto).
- **Revaluación de junio:** igual (sección 2.6).
- **Nuevo:** «Release sin revisar > 2 días» (actividad a Jessica) y «Cliente con perfil sin release en N semanas».

### 2.10 Releases (lo que falta para que Jessica los aplique cada semana)

1. **Planta en el perfil** y un perfil por cliente + planta (Shawmut ×2, Saltillo ×2; FXI Santa Teresa no manda
   release: se captura de sus órdenes de compra). Las plantas se dan de alta como **direcciones de entrega** del cliente
   (hoy no existen, sección 1.5), para que pronóstico, pedido y factura digan la planta. Aplicar reemplaza solo
   dentro del mismo perfil y pone en 0 solo las semanas de esa planta (corrige N11).
2. **Aplicar escribe en `qb.forecast.line`** y muestra antes el resumen de cambios (`qb.forecast.change` en borrador). Sin
   pronóstico por año ni estado «revisado» de documento.
3. **Confirmación en bloque** de partes sugeridas desde el release («Confirmar sugerencias de este release»); las partes
   nuevas se proponen con la ficha (sección 2.2) además del código.
4. **Lector genérico «Excel/CSV parte × semana»** con mapeo por perfil (fila de encabezado, columna de parte, columnas de
   fecha, unidad): cubre Shawmut Silao (.csv) y Tennessee (.xls), Zwisstex y los que mandan hoja propia. **Saltillo y León
   mandan PDF:** en E1 se capturan en la matriz; un lector de PDF (texto, no imagen) va después si el formato es estable. Los perfiles 3, 5 y 7 dejan de decir
   «FXI SUM».
5. **Bandeja:** menú «Releases por revisar» con el contador; el aviso de la sección 2.9.

---

### 2.11 Proyectos: conexión con Desarrollo de producto (C1) y con Fichas técnicas

**Lo que ya existe.** El SGI tiene el desarrollo de producto completo sobre Proyectos (`quimibond_sgi/models/sgi_dev_*.py`,
procedimiento C1): un proyecto `project.project` con `sgi_is_ft` que nace como **solicitud → análisis → cotización →
aprobación del cliente → muestra → respuesta del cliente → pilotaje → liberado** (o cerrado sin producto). Ya guarda casi
todo lo que el presupuesto necesita de un proyecto:

| Lo que necesita el presupuesto | Campo del desarrollo | Dónde |
|---|---|---|
| Cliente y equipo de ventas | `partner_id`, `sgi_dev_team_id`, `sgi_dev_salesperson_id`, `sgi_dev_customer_status` (actual o prospecto) | `sgi_dev_project.py:283-290` |
| Producto (tela en m y en kg) | `sgi_dev_product_id` (acabado en m) y `sgi_dev_product_proceso_id` (acabado en kg); el generador de código los crea con la DAT P-D02-01 | `sgi_dev_product.py:291-413`, `sgi_dev_product_proceso.py:42` |
| Producto de línea para un cliente nuevo | `sgi_dev_analysis_result = 'linea'` + `sgi_dev_base_product_id`: se cotiza el existente y el proyecto cierra sin folio | `sgi_dev_project.py:331-339` |
| Volumen | `sgi_dev_consumption_annual` en m, yd o kg, `sgi_dev_program_years`, `sgi_dev_required_date`; en la solicitud, `sgi_dev_volume` mensual y `sgi_dev_target_price` | `sgi_dev_project.py:295-310`, `sgi_dev_request.py:47-55` |
| Si ya se puede vender | `product.template.sgi_dev_state`: en desarrollo (el pedido no se confirma), pilotaje (se vende con aviso), liberado, archivado | `sgi_dev_product.py:27-80, 94-110` |
| Su ficha técnica | el asistente de fichas de proceso del desarrollo crea la ficha de tejido y la de acabado de sus productos | `sgi_dev_fabric_sheet.py:342-480` |

**Cómo está en producción (4-nov):** 95 proyectos de desarrollo. Los 17 capturados desde octubre (Tescap, Saltillo,
Shawmut, Bondtex…) traen cliente, equipo, producto W + IW y consumo anual; los ~75 migrados de los «FT-» viejos están casi
todos en «Muestra» sin consumo ni equipo, y ningún producto vendido en 2026 tiene `sgi_dev_state`. De los 33 renglones de
PRESUPUESTO PROYECTOS del Excel, **12 tienen su proyecto de desarrollo** por producto (W o IW), 2 se parecen (código
hermano), 3 traen códigos que no existen en Odoo pero sí hay desarrollos con códigos cercanos (Bowen, Blancos) y el resto
son productos de línea para un cliente nuevo, sin análisis registrado. Lista completa fuera del repo.

**Qué se conecta (todo vive en `quimibond_ventas_presupuesto`, que ya depende del SGI; el SGI no se toca salvo una
vista):**

1. **Todo producto nuevo pasa por Desarrollo; un producto de línea no** (decisión de Jose, 4-nov). El proyecto
   comercial de un producto nuevo nace del desarrollo: cuando llega a «Cotización» con cliente y artículo
   (`sgi_dev_product_id`), se crea su `qb.sales.project`
   (cliente + artículo, `desde` = fecha de cotización, liga al desarrollo). Si el desarrollo cierra sin producto o no
   es factible, el proyecto comercial se cierra como **perdido** y sus renglones del presupuesto y del pronóstico salen
   en Faltantes («proyecto perdido con presupuesto»). Si llega a «Liberado», sigue como proyecto hasta que el
   presupuesto del año siguiente lo pase a línea. Un producto de línea para un cliente nuevo se da de alta en Ventas sin
   desarrollo (si el desarrollo ya existe con análisis «línea», se liga, pero no se exige). Faltantes solo marca un
   proyecto cuyo artículo **no existía** antes (o no existe en Odoo) y no tiene desarrollo.
2. **Renglón de presupuesto sin código = desarrollo sin producto.** El renglón de proyecto (2.6) liga el desarrollo en
   lugar de solo describir gramaje y ancho; cuando el generador de código crea los productos
   (`action_sgi_dev_generate_products`), el renglón toma el producto solo (herencia del método en este módulo, que llama
   a `super()`). Gramaje y ancho del renglón salen de la tabla de características del desarrollo (masa y ancho que ya usa
   el generador) mientras no haya ficha.
3. **Volumen del desarrollo → propuesta del presupuesto.** El armado de la propuesta (2.6) suma un origen más,
   `desarrollo`: para cada desarrollo abierto (de Cotización a Pilotaje) con consumo anual, el volumen mensual desde la
   fecha requerida, convertido con `qb_convertir` (el consumo en kg hoy se queda sin metros: `sgi_dev_project.py:399-404`
   solo convierte m ↔ yd). Cada renglón dice la etapa del desarrollo; Jessica decide cuánto entra. Precio: el de la
   cotización del desarrollo si existe, si no `sgi_dev_target_price`.
4. **Pronóstico y pedidos respetan el estado del artículo.** Un artículo «en desarrollo» puede estar en el pronóstico
   (Planeación necesita verlo venir) pero sale marcado; la propuesta de pedidos de E3 no lo propone (el pedido no se
   confirmaría, `sgi_dev_product.py:94-110`). En «pilotaje» sí, con el mismo aviso que ya pone el pedido.
5. **El desarrollo ve su venta.** En la ficha del proyecto de desarrollo, un botón «Presupuesto y venta» (herencia de la
   vista del SGI desde este módulo, con ancla revisada por `check_odoo_views.py`): presupuesto del año, pronóstico vigente
   y facturado del cliente + artículo. Es la medición que hoy no existe de «¿el desarrollo se vendió?».
6. **Limpieza previa: la hace Diseño y Desarrollo antes del 1-dic-2026** (decisión de Jose, 4-nov), antes de que E2
   entre a producción el 9-dic: ligar a su desarrollo los proyectos de 2026 de producto
   nuevo (los de producto de línea no lo necesitan), cerrar los «FT-» viejos que ya no siguen y poner `sgi_dev_state = liberado` a los artículos de
   desarrollos liberados. Corregir también el acabado en kg del análisis de Saltillo (`WJ030Q22KNT168`, que no sigue la
   regla «I» + código de la tela). Sin eso, la carga de la REV1 crea los `qb.sales.project` sin desarrollo y los marca.

**Y con Fichas técnicas,** además de unidades (2.2): el artículo, el rendimiento y los kilos a producir salen de la
ficha de acabado; los saldos que no cuentan como venta, de la lista de saldos de Fichas (2.7); la revisión de listas de
Fichas (`ft_estado`, «lista para activar») se muestra en la Demanda de Planeación para que Planeación sepa si un artículo
pedido todavía no tiene lista activa.

### 2.12 Prácticas de vistas y código (obligatorias en E1–E3)

**Cómo está hoy el módulo (4-nov):** cumple las reglas del repo: `check_odoo_views.py` y `check_odoo_fields.py` no marcan
nada, usa la sintaxis de Odoo 19 (`<list>`, `invisible`/`readonly` con expresiones, `<chatter/>`, sin `attrs` ni
`states`), no hereda sus propias vistas (sus 4 herencias son de otros módulos: producto, categoría, Ajustes del SGI y
`account_budget`), cada vista de búsqueda tiene filtros y agrupaciones, y todas las acciones tienen ayuda salvo la del
asistente de importación (no la necesita). Lo que sí cambia es el modelo, no la forma.

**Vistas**

1. **Sintaxis de Odoo 19:** `<list>`, expresiones en `invisible`/`readonly`/`required`, `<chatter/>`, `column_invisible`
   en listas incrustadas. Nada de `attrs`, `states`, `tree` ni `oe_chatter`.
2. **Sin herencias propias** (regla del repo): la ficha de cada modelo propio va completa en su `<record>`. Se hereda
   solo lo de otros módulos: la ficha del proyecto de desarrollo (SGI), la del producto, la de Ajustes y el MPS (parche
   OWL). Cada ancla la revisa `check_odoo_views.py` contra el padre tal como queda en el código; el archivo del padre va
   antes en el manifiesto.
3. **Listas:** columnas secundarias con `optional="hide"`; `sum` solo en medidas que se pueden sumar (pesos, metros,
   kilos; **nunca** la cantidad en unidad de venta mezclando productos); `decoration-*` para estado, zona y origen;
   `default_order` explícito; `multi_edit="1"` donde Jessica corrige en bloque (pronóstico, renglones del presupuesto).
4. **Búsqueda:** una por modelo, con los filtros con nombre del proceso («Acabado: 3 semanas», «Tejido: próximo mes»,
   «Zona firme», «Proyectos», «Sin precio», «Sin conversión»), agrupaciones por cliente, artículo, mercado, semana y mes,
   y `<group>` sin atributos (RNG de Odoo 19). Las acciones abren con `search_default_*`, no con dominios fijos, para que
   el usuario pueda quitar el filtro.
5. **Pivote y gráfica** con medidas declaradas (`type="measure"`) y filas por defecto; la Demanda de Planeación abre en
   pivote. La captura semanal y mensual va en `web_grid` (ya es dependencia), no en listas editables de 52 columnas.
6. **Fichas:** encabezado con barra de estado solo donde hay estados (presupuesto anual, release, presupuesto del mes; el
   pronóstico no tiene); botones inteligentes `oe_stat_button` con conteo (cambios, faltantes, facturas, pedidos); acciones
   de flujo en el encabezado y las secundarias en el engrane, como ya decidió V-M10.
7. **Textos:** `help` de acciones y campos en «usted», en español, diciendo de dónde sale el dato; sin etiquetas repetidas
   en el mismo modelo (el checker lo revisa); los nombres de menú y migas iguales.
8. **Accesibilidad:** `<a class="btn">` con `role="button"`; avisos `alert-*` con `role="alert"` o `"status"`.
9. **Campos de otros modelos confirmados en producción** con `get_fields` por MCP antes de ponerlos en una vista
   (Fichas, MPS y Desarrollo son de otros módulos y Enterprise); lo que agregue Enterprise va a `tools/odoo_fields_allow.txt`.
10. **Menús:** solo en `views/menus.xml`, con `groups` por puesto (sección 3.3): Ventas → Presupuesto y pronóstico y
    Fabricación → Planeación → Demanda por cliente. Los modelos técnicos (bitácora, tasas) no llevan menú propio.

**Modelos y código**

11. **Vistas SQL** (`qb.sales.actual`, `qb.demand.planning`, `qb.sales.compare`) con `_auto = False` y `init()` que hace
    `CREATE OR REPLACE VIEW`; filtros por fecha e índices en las tablas base que use; si el pivote anual pasa de 2 s, se
    materializa (riesgo 5).
12. **Sin N+1:** cálculos por lote (`_read_group`, índices por llave), `store=True` solo en lo que se filtra o agrupa. Los
    campos de Fichas no se guardan (`ft_*` son calculados): lo que dependa de ellos (`qb_articulo_id`, `qb_m_por_kg`) se
    recalcula con el cron nocturno de Fichas, no con `@api.depends` sobre campos no guardados.
13. **Seguridad:** un renglón de `ir.model.access.csv` por modelo y grupo; reglas por compañía; regla de confección por
    mercado; Planeación solo lectura; `sudo()` solo donde se lee contabilidad para el real, documentado.
14. **Lógica pura fuera de Odoo** (`release/`: lectores, unidades, lector del forecast) con pruebas en `tests_puros/` y
    fixtures anonimizados; pruebas de Odoo en `tests/` y registradas en `tests/__init__.py` (el checker lo exige), corridas
    en Odoo.sh con `--test-tags /quimibond_ventas_presupuesto`.
15. **Correos y PDF:** `mail.template` con QWeb (no HTML armado en Python); PDF con `web.external_layout` y el pie de
    formato del SGI (`sgi.format.mixin`, F-P-A28-14/17/18).
16. **Datos:** crons y parámetros con xmlid, sembrados sin pisar valores capturados (como `seed_parameters`); los crons
    se vuelven a encender en cada update, igual que en `quimibond_intelligence`.
17. **Despliegue:** versión del manifiesto subida en cada entrega (19.0.2.0.0, 2.1.0, 2.2.0) con su sección en el README;
    migraciones idempotentes con conteos en el log; `check_addons.py`, `check_odoo_views.py` y `check_odoo_fields.py`
    en 0 antes de abrir el PR; después de desplegar, revisar el `update.log` y las consultas del runbook.

## 3. Flujos y permisos

### 3.1 Semana

| Día | Quién | Qué hace en Odoo |
|---|---|---|
| Al llegar | Jessica | Sube el release (Lear jueves, FXI viernes, los demás en su día) → «Leer» → revisa cambios → «Aplicar». Clientes sin lector: matriz o «Pegar semanas desde Excel» |
| Al aplicar | Odoo | Escribe el pronóstico, bitácora, recalcula el MPS |
| Lunes | Planeación | Abre «Demanda por cliente», filtros Acabado 3 semanas y Tejido próximo mes; exporta si lo necesita |
| Lunes | Odoo | Aviso de cobertura (semanas firmes sin pedido) |
| Semana firme | Jessica | Hasta E3: captura los pedidos como hoy. Con E3: «Proponer pedidos» → confirma |

### 3.2 Mes

| Cuándo | Quién | Qué |
|---|---|---|
| Día 1–3 | Odoo + Jessica | Cierre: reporte mensual (presupuesto vs real por mercado), justificación si va bajo el umbral, correo «Cierre de ventas general <mes>» desde Odoo. Ya no se exportan ni pegan facturas |
| Últimos días | Jessica + vendedores de confección | «Armar presupuesto de <mes siguiente>», ajustes, «Enviar a Dirección» |
| Junio | Jessica + Dirección | Nueva revisión (REV1) con TC y precios nuevos → Dirección aprueba |
| Diciembre | Jessica + Dirección | «Armar propuesta <año>» → ajustes → revisión con Dirección → aprobar REV0 |

### 3.3 Permisos por puesto

| Puesto | Grupo | Pronóstico | Releases | Presupuesto del mes | Presupuesto anual | Reportes |
|---|---|---|---|---|---|---|
| Administración de Ventas (Jessica) | `Planeación comercial: administrador` (nuevo, implica `sales_team.group_sale_manager`) | edita todo | lee, aplica | arma y envía | arma, envía a revisión | todos |
| Vendedores y Atención a clientes confección | `Planeación comercial: confección` (nuevo) | edita solo `mercado` de confección (regla de registro) | — | edita su parte | edita renglones de confección en borrador | los de confección |
| Planeación | `Planeación (consulta)` (nuevo) | lee | lee | — | — | Demanda por cliente |
| Dirección | `quimibond_sgi.group_sgi_director` | lee | lee | recibe | **aprueba** cada revisión | todos |
| MAST | `quimibond_sgi.group_sgi_manager` | como hoy (borrar, regresar a borrador) | | | | |

---

## 4. Migración y carga inicial

### 4.1 Datos actuales (no se borra nada)

> **Hecho en 3.0.0 (2026-11-05), con un cambio:** los 21 documentos `kind = pronostico` pasan a `obsoleto` con una
> nota, **sin copiar sus líneas** a `qb.forecast.line` (Jose: «archivarlos»; el vigente salió de `forecast.xlsx`).
> Se ven en Histórico → Pronósticos anteriores, de solo lectura, y ya no se crean. Las 624 celdas del MPS se borraron
> (no se recalculan: MPS desconectado). Los perfiles de release llevan Planta. Migración:
> `migrations/19.0.3.0.0/post-migrate.py`.

| Hoy | Queda como |
|---|---|
| 21 documentos `kind = pronostico` | Pasan a `obsoleto` y solo lectura; sus líneas se copian a `qb.forecast.line` con `origen = historico` y la liga al documento. Lo de la semana actual en adelante lo pisa la carga de `forecast.xlsx` (4.2) y los releases. Las líneas WJ en yardas de Shawmut se convierten por artículo (el WJ y el IWJ quedan en el mismo artículo) |
| Presupuesto Industrial 2026 (aprobado, 118.9 M) | `reemplazado`, como «Industrial 2026 versión Odoo sept-26»: comparable, fuera del KPI |
| Confección y Especiales 2026 (borrador) | `obsoleto`: la REV1 del Excel los sustituye (mismas cifras) |
| Releases, partes, perfiles | Se quedan; los perfiles de Shawmut y Saltillo se parten por planta (con los releases ligados a la planta que dicen) |
| MPS | Se recalcula una vez con la fuente nueva; se guarda antes y después de los programas para compararlos. Los 14 programas que crearon los releases del 2-nov (ids 131–144, 624 celdas) se recalculan o se borran aquí |

Todo en `migrations/19.0.2.0.0/post-migrate.py`, idempotente, con conteos en el log. Versión del manifiesto: 19.0.2.0.0
(modelos nuevos: sin bump el CI marca error).

### 4.2 Carga del `forecast.xlsx` (E1)

Lector en Python puro (`release/forecast_xlsx.py`, pruebas en `tests_puros/` con un **fixture anonimizado** del mismo
layout: clientes «Cliente A…», partes y cantidades inventadas). Lee **las hojas de cliente**, no PRONÓSTICO 2026/2027
(fórmulas con rangos corridos y 2027 copiado de 2026 en 9 clientes, sección 1.4), con un mapa por hoja (fila del año,
columna de la semana 1, columna de parte, unidad tomada de PRONÓSTICO); las celdas de texto («por confirmar») y los
renglones de PO o de conversión intercalados van al reporte de la carga, no al pronóstico. Por cada hoja de cliente y
cada bloque 2026/2027:
semana, fecha de entrega, OC, producto, parte del cliente y cantidad en su unidad → `qb.forecast.line` con
`origen = excel`. Las partes que no existen se crean en el catálogo como «sugerido». La hoja PRECIOS y la de MUESTRAS
no se cargan en E1 (la de precios se compara, 4.4; la de muestras queda como pregunta 9).

Aceptación: por cliente, la suma en su unidad igual a la de sus renglones en PRONÓSTICO 2026 (hoy coinciden salvo dos
renglones de 500 que PRONÓSTICO no toma); la diferencia en metros contra «TOTAL M» (9,992,405 m en 2026) se explica por
el factor de yarda (1.0963 → 0.9144 exacto), los factores fijos de Pieles y los rangos corridos.

### 4.3 Carga del presupuesto general 2026 (E2)

Lector de las 5 hojas de mercado (cliente × producto × mes en industrial; producto × mes en confección) → REV1 con su TC
y precios (`price_origen = excel`), aprobada por Dirección con el botón; REV0 desde su versión, en `reemplazado`.

Aceptación al peso: los totales de la hoja ANUAL por mercado (tabla de control de la sección 1.3 del prompt), tanto el
presupuesto como lo vendido a septiembre recalculado desde `qb.sales.actual`. **Falta el archivo:** la memoria solo
guarda las 10 primeras hojas (sección 1.4); de ahí sale Industrial (reproducido) pero no Proyectos industrial,
Confección, Proyectos confección, Especiales ni ANUAL. La versión «01072026 propuesta» trae Proyectos industrial en
33,005,420, no los 33,866,419.50 del control: hubo un ajuste posterior. Se necesita el .xlsx vigente. Lo vendido no va a cuadrar al peso de
entrada (Odoo ene–sep Industrial por equipo 101.85 M contra 101.46 M del Excel): la carga produce el reporte de
diferencias por cliente y mes (facturas sin equipo, cliente cuyo equipo no es el de la factura, notas de crédito,
fecha) y esa conciliación se revisa con Jessica antes de aprobar.

### 4.4 Comparación PRECIOS del forecast contra listas de Odoo

Hecha el 4-nov (resumen en 1.4; detalle por renglón fuera del repo). Antes de E1 hay que: (a) decidir el +6 % de
Shawmut (¿la lista de Odoo está vieja o el Excel trae un aumento no capturado?); (b) renovar las reglas de Seiren que
vencieron el 31-oct; (c) dar de alta las 33 reglas faltantes (23 de Pieles); (d) borrar las dos reglas duplicadas de
Shawmut que parecen m². Sin eso, E3 no puede proponer pedidos con precio. La carga de E1 repite la comparación como
reporte para que Jessica la vea en Odoo.

---

## 5. Plan por entregas

Mismo orden que el prompt. El artículo (2.2) y la vista del real (2.7) se adelantan a E1 porque el consumo de pronóstico
por pedidos y la Demanda por cliente ya los necesitan.

| Entrega | Contenido | Revisión del diseño | Staging (Odoo.sh) | Producción |
|---|---|---|---|---|
| **E1** | Artículo y unidades; `qb.forecast.line` + bitácora; matriz y «Pegar desde Excel»; releases con planta, lector Excel genérico, confirmación en bloque, aplicar al pronóstico nuevo; `qb.demand.planning`, Demanda por cliente, grupo Planeación; MPS desde la fuente nueva y parche «Demanda por cliente»; migración de los 21 pronósticos; carga de `forecast.xlsx`; comparación de precios | 11-nov (Jessica + Planeación) | 20-nov | **25-nov** |
| **E2** | Presupuesto anual por año + revisión, mercados, proyectos ligados a Desarrollo (2.11), precio y TC capturables; `qb.sales.actual`; VE-02 sobre la vista; reportes mensual, anual, cliente × artículo, variación y faltantes; presupuesto del mes con PDF y correo; armado de la propuesta del año; carga REV0/REV1 | — | 4-dic | **9-dic** (Jessica arma 2027 del 9 al 18-dic; cierre de noviembre en paralelo con el Excel) |
| **E3** | Pedidos desde la zona firme (sección 2.8 del prompt): propuesta, agregar o actualizar por OC, reducciones con autorización, nunca tocar lo embarcado, consumo del pronóstico | — | 13-ene-2027 | **20-ene-2027** (fuera del cierre de año) |

Criterio de salida de E1: Jessica deja de actualizar `forecast.xlsx` y Planeación deja de abrir el Drive (una semana en
paralelo, del 25-nov al 2-dic). De E2: el cierre de noviembre sale de Odoo con las mismas cifras que el Excel (o con
cada diferencia explicada) y el VE-02 de octubre da lo mismo que el reporte mensual.

**E3 en detalle (para que el diseño quede completo):** asistente `qb.order.proposal` que toma la zona firme de
`qb.forecast.line`: una línea por parte del cliente y semana con OC (`customer_po`), Release ID, fecha compromiso
(semana de embarque), producto de facturación del artículo (parámetro por artículo: W o IW), cantidad en la unidad de
facturación y precio de la lista del cliente (sin precio de lista no se propone: va a Faltantes). Busca un pedido
abierto del cliente con `client_order_ref = OC`; agrega la línea o actualiza la cantidad. Campos nuevos en
`sale.order.line`: `qb_customer_part_id`, `qb_release_id`, `qb_forecast_week`. Reglas: una reducción dentro de la zona
firme queda «por autorizar» (grupo administrador comercial); nunca baja de `qty_delivered`; un clic de Jessica confirma
todo; después, «automático» por perfil de cliente. El consumo ya existe desde E1 (`qty_pedido`, `qty_neta`).

---

## 6. Riesgos

1. **Enterprise sin red local.** El MPS (parche JS) y `web_grid` solo se prueban en Odoo.sh. Mitigación: respaldo con
   botones (2.4) y pruebas con `--test-tags /quimibond_ventas_presupuesto`.
2. **Fichas técnicas: 31.** El resto depende del código. Hay códigos que no cumplen la nomenclatura (`WC090Q11ING165.`
   con punto, unidades de kg en códigos W): quedan en Faltantes, no se convierten.
3. **Rendimiento de especificación contra real** (14 % en WJ038): si Planeación compra hilo con el de especificación,
   pide de menos. Ver pregunta 3.
4. **Precios de lista con huecos** (Shawmut, Lear, FXI): no frenan el presupuesto (precio capturable) pero sí E3.
5. **Volumen de la vista del real.** Es SQL sobre `account_move_line` con filtros por fecha y cuenta; si el pivote anual
   tarda, se materializa por mes con refresco nocturno (decisión en E2 con la medición).
6. **Datos sensibles:** fixtures anonimizados, los Excel nunca en el repo, la comparación de precios se entrega fuera.
7. **Transición:** una semana de doble captura (Excel y Odoo). Si Jessica no la puede sostener, E1 entra el lunes 30-nov.

## 7. Preguntas

**Para Jose**

1. ~~Releases del 2-nov~~ (1.5): los 14 programas del MPS (ids 131–144) y sus 624 celdas se quedan hasta la migración de
   E1, que los recalcula con la fuente nueva o los borra (Jose, 4-nov). Mientras, Planeación no debe lanzar
   reabastecimiento desde esos programas.
2. ~~TC de la REV1~~: **17.5**, confirmado en el correo del 25-jun y en la hoja «2026 industrial» (1.4). Queda ver por qué
   la hoja ANUAL dice 17.2 cuando llegue el archivo.
3. ~~Rendimiento de especificación o real~~: el de la ficha (un solo rendimiento, decisión de Dirección General en fichas
   2.24.0); la especificación solo como respaldo (2.2).
4. ¿El equipo «Distribuidor» y las ventas sin equipo de clientes de giro van a algún mercado o se reportan aparte?
5. Objetivo y mínimo: ¿sobre la venta total del año anterior o por mercado?
6. ¿Se acepta que el mercado salga del cliente (no de la factura), con la conciliación de 4.3?

**Para Ventas (Jessica)**

7. Un «proyecto» en el Excel es una combinación producto–cliente en arranque (1.4). ¿Cuándo pasa a «línea»? Hay 6 que
   ya se vendían antes de 2026 y siguen como proyecto. (Jose, 4-nov: todo producto nuevo pasa por Desarrollo; un producto
   de línea para un cliente nuevo, no.)
8. ~~Base del precio~~: la de la lista y la factura de cada cliente (1.5). Falta limpiar las reglas en m² de Shawmut,
   FXI y Lear, que no se usan para facturar.
9. Hoja MUESTRAS: ¿se queda en Excel o la quieres en Odoo (sería otra entrega)?
10. ~~Plantas~~: sí, Shawmut manda dos releases; FXI Santa Teresa ninguno; Saltillo y León dos PDF (1.5). ¿Qué OC es de
    qué planta, para dar de alta las direcciones de entrega?
11. Para el armado del 2027: ¿el factor de tendencia de ±30 % te sirve como tope, o lo pones tú por cliente?

**Para Planeación**

12. ~~Unidad de trabajo~~: kilos para tejido, metros para acabado (1.5).
13. ~~Corte de tejido~~: ventana móvil de un mes desde el día del corte (1.5).
14. En el MPS, ¿los programas deben quedar en el producto en kilos (IW) o en metros (W)?

**Nuevas tras leer los Excel, el correo y Odoo (4-nov)**

15. ~~Desperdicio, saldos y chatarra~~: se excluyen del cumplimiento y del VE-02 (Jose, 4-nov). Pesan 0.19 % (1.5).
16. ~~Shawmut +6 %~~: vale Odoo; el Excel trae el recargo de jun–sep que ya terminó (1.5).
17. ¿Subes el GENERAL 2026 rev01072026 a tu Drive (o me das el enlace)? Por correo y por la memoria no se puede bajar
    desde aquí. La REV0 se arma del industrial y el de confección de diciembre de 2025 (1.5).
18. ¿Se corrige el extractor de la memoria para guardar todas las hojas de un Excel (otro PR, en quimibond-intelligence)?

**Para Diseño y Desarrollo**

19. ~~Limpieza de los FT viejos~~: Diseño y Desarrollo, antes del 1-dic-2026 (Jose, 4-nov). Si no está, la carga de la
    REV1 sigue y marca en Faltantes lo que quede sin ligar.
20. ¿La cotización del desarrollo trae precio en algún campo, o el precio objetivo de la solicitud es lo único?
