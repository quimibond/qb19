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
   no hay ficha, de la regla de la DAT P-D02-01 (`I` + código de la tela). Shawmut se pronostica en WJ y se factura en
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

**Campos nuevos en `product.product` (guardados, recalculados por cron nocturno y al cambiar ficha o código):**

| Campo | Qué es | De dónde sale, en orden |
|---|---|---|
| `qb_articulo_id` (m2o `product.product`) | La tela en metros que representa al producto en la medición | 1) ficha de acabado: `ft_producto_m_id` (para el IW, su W; para el W, él mismo); 2) regla DAT P-D02-01: si el código cumple `NOMEN_RE` con `I` al frente y existe el producto con el mismo código sin `I`, ese; 3) excepción capturada (`qb.articulo.excepcion`); 4) él mismo |
| `qb_gramaje` (g/m²), `qb_ancho_cm` | Especificación | 1) ficha (`ft_peso`, `ft_ancho`); 2) código (`NOMEN_RE` grupos 3 y 8, la misma de `qb_costeo/models/horas.py:64`); 3) vacío |
| `qb_m_por_kg` | Rendimiento para convertir | `1000 ÷ (gramaje × ancho_m)` de la especificación (lo que pide la sección 2.7) |
| `qb_m_por_kg_real` | Rendimiento real de producción, solo informativo | `ft_rendimiento` de la ficha (metros fabricados ÷ kilos consumidos, 12 meses) |
| `qb_conv_origen` | `ficha` / `codigo` / `sin_dato` | — |

El caso WC090 del prompt se corrige solo: `WC090Q11JNT168` tiene ficha con 90 g/m² y 168 cm, y los demás WC090 leen
90 del código. **Ojo:** para `WJ038Q22JNT160` la especificación da 16.45 m/kg y la ficha real 14.19 m/kg (14 % menos).
Cuál usa Planeación para los kilos de tejido es la pregunta 3 de la sección 7; el modelo guarda los dos.

**Conversión:** una función de módulo `qb_convertir(producto, cantidad, de_unidad, a_dimension)` sobre `release/units.py`
(se agrega `de_unidad` de Odoo además del código del cliente). Metros ↔ yardas exacto; metros ↔ kilos con `qb_m_por_kg`;
metros ↔ m² con el ancho. Si `qb_conv_origen = sin_dato`, devuelve `None`.

**Cada renglón con cantidad (pronóstico, presupuesto, real, propuesta de pedido) guarda:**

- `qty_origen` + `uom_origen` (lo que escribió el cliente o el usuario, en su unidad),
- `qty_venta` en la unidad de venta del producto del renglón,
- `qty_m` y `qty_kg` (vacíos si no hay dato),
- `conv_origen`.

Así, la decisión de pasar todo a kilos solo cambia qué columna se muestra primero y en qué producto (W o IW) se hace el
pedido (parámetro por artículo, sección 2.8); no cambia tablas ni reportes.

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

**Confección:** la misma tabla con `partner_id` vacío y `mercado = confeccion` (pronóstico por producto). Lo capturan
los vendedores de confección por matriz producto × mes (la semana es el primer lunes del mes; a Planeación le llega
repartida en los lunes, igual que hoy el presupuesto, `sgi_sales_budget.py:1200`).

### 2.4 Vista de Planeación y demanda al MPS

**Modelo de lectura `qb.demand.planning`** (vista SQL, `_auto = False`) que une, por cliente + planta + artículo + semana:

- `tipo = pedido`: líneas de pedido confirmadas pendientes de entregar (`product_uom_qty − qty_delivered`), por semana de
  `commitment_date` (o `expected_date`), y
- `tipo = pronostico`: `qty_neta` del pronóstico (lo que los pedidos no cubren).

Columnas: cliente, planta, mercado, artículo, producto, semana, mes, zona, tipo, cantidad en unidad de venta, metros,
kilos, `conv_origen`. Medidas sumables: metros y kilos (la unidad de venta solo se suma dentro de un producto).

**Acción «Demanda por cliente»** (menú Planeación → Demanda), vista pivote por defecto:

- filas: artículo → cliente; columnas: semana; medida: metros y kilos (las dos);
- filtros guardados: **«Acabado: 3 semanas»** (semana actual y dos siguientes; con E3, solo pedidos), **«Tejido: próximo
  mes»** (pedido + pronóstico del mes siguiente), zona firme / planeada, mercado, cliente, producto;
- lista con las mismas columnas y exportación nativa a Excel; gráfica opcional.

**Permisos:** grupo nuevo `Planeación (consulta)` con lectura de `qb.demand.planning`, `qb.forecast.line`,
`qb.forecast.change`, releases y partes; sin escritura. Menú propio bajo Fabricación → Planeación y bajo Ventas →
Presupuesto y pronóstico.

**MPS:** la demanda que se escribe en `mrp.product.forecast` sale de la misma fuente:

- por artículo y semana: suma de `qty_neta` del pronóstico + pedidos (la regla `max(pronóstico, comprometido)` de hoy,
  pero por cliente + artículo, que cierra el doble conteo N4);
- más el presupuesto anual vigente solo donde ningún pronóstico cubre cliente + artículo + mes (regla N2 de hoy), para
  los meses más allá del horizonte de los releases;
- se escribe en el producto del artículo **que tenga programa en el MPS** (hoy 48 programas en Toluca): si el programa es
  del IW, la cantidad va en kilos; si es del W, en metros; si los dos tienen programa, en el que diga el parámetro del
  artículo. Esa es la bisagra con la decisión de kilos.
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
marcaría 102 M como «nuevos» desde julio de 2025, así que no sirve. Propuesta:

1. El presupuesto declara qué es proyecto: el mercado del renglón (`proyectos_industrial`, `proyectos_confeccion`) para
   ese **cliente + artículo + año**.
2. Para lo que no está en el presupuesto, el producto lleva `qb_proyecto_hasta` (fecha, opcional): mientras no llegue, su
   venta cuenta como proyecto. Jessica lo marca al dar de alta el código de un proyecto; un proyecto «se gradúa» cuando el
   presupuesto del año siguiente lo pone en línea.
3. Base del mercado: el equipo de ventas del **cliente** (`res.partner.team_id`), no el de la factura; si no tiene, el de
   la factura; si tampoco, «Sin mercado» visible en el reporte (sección 2.7).

Sin equipos falsos.

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

1. **Planta en el perfil** y un perfil por cliente + planta (Shawmut ×2, Saltillo ×2, FXI ×2). Aplicar reemplaza solo
   dentro del mismo perfil y pone en 0 solo las semanas de esa planta (corrige N11).
2. **Aplicar escribe en `qb.forecast.line`** y muestra antes el resumen de cambios (`qb.forecast.change` en borrador). Sin
   pronóstico por año ni estado «revisado» de documento.
3. **Confirmación en bloque** de partes sugeridas desde el release («Confirmar sugerencias de este release»); las partes
   nuevas se proponen con la ficha (sección 2.2) además del código.
4. **Lector genérico «Excel parte × semana»** con mapeo por perfil (fila de encabezado, columna de parte, columnas de
   fecha, unidad): cubre Saltillo, Shawmut, Zwisstex y los que mandan hoja propia. Los perfiles 3, 5 y 7 dejan de decir
   «FXI SUM».
5. **Bandeja:** menú «Releases por revisar» con el contador; el aviso de la sección 2.9.

---

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

| Hoy | Queda como |
|---|---|
| 21 documentos `kind = pronostico` | Pasan a `obsoleto` y solo lectura; sus líneas se copian a `qb.forecast.line` con `origen = historico` y la liga al documento. Lo de la semana actual en adelante lo pisa la carga de `forecast.xlsx` (4.2) y los releases. Las líneas WJ en yardas de Shawmut se convierten por artículo (el WJ y el IWJ quedan en el mismo artículo) |
| Presupuesto Industrial 2026 (aprobado, 118.9 M) | `reemplazado`, como «Industrial 2026 versión Odoo sept-26»: comparable, fuera del KPI |
| Confección y Especiales 2026 (borrador) | `obsoleto`: la REV1 del Excel los sustituye (mismas cifras) |
| Releases, partes, perfiles | Se quedan; los perfiles de Shawmut y Saltillo se parten por planta (con los releases ligados a la planta que dicen) |
| MPS | Se recalcula una vez con la fuente nueva; se guarda antes y después de las 48 programas para compararlos |

Todo en `migrations/19.0.2.0.0/post-migrate.py`, idempotente, con conteos en el log. Versión del manifiesto: 19.0.2.0.0
(modelos nuevos: sin bump el CI marca error).

### 4.2 Carga del `forecast.xlsx` (E1)

Lector en Python puro (`release/forecast_xlsx.py`, pruebas en `tests_puros/` con un **fixture anonimizado** del mismo
layout: clientes «Cliente A…», partes y cantidades inventadas). Por cada hoja de cliente y cada bloque 2026/2027:
semana, fecha de entrega, OC, producto, parte del cliente y cantidad en su unidad → `qb.forecast.line` con
`origen = excel`. Las partes que no existen se crean en el catálogo como «sugerido». La hoja PRECIOS y la de MUESTRAS
no se cargan en E1 (la de precios se compara, 4.4; la de muestras queda como pregunta 9).

Aceptación: por cliente y mes, la suma en la unidad del cliente igual a la hoja «CONVERSIÓN DE UNIDADES VENTA»; la
diferencia en metros contra «TOTAL M» se explica renglón por renglón (factor del Excel contra especificación).

### 4.3 Carga del presupuesto general 2026 (E2)

Lector de las 5 hojas de mercado (cliente × producto × mes en industrial; producto × mes en confección) → REV1 con su TC
y precios (`price_origen = excel`), aprobada por Dirección con el botón; REV0 desde su versión, en `reemplazado`.

Aceptación al peso: los totales de la hoja ANUAL por mercado (tabla de control de la sección 1.3 del prompt), tanto el
presupuesto como lo vendido a septiembre recalculado desde `qb.sales.actual`. Lo vendido no va a cuadrar al peso de
entrada (Odoo ene–sep Industrial por equipo 101.85 M contra 101.46 M del Excel): la carga produce el reporte de
diferencias por cliente y mes (facturas sin equipo, cliente cuyo equipo no es el de la factura, notas de crédito,
fecha) y esa conciliación se revisa con Jessica antes de aprobar.

### 4.4 Comparación PRECIOS del forecast contra listas de Odoo

La entrega la carga de E1 como reporte (cliente, producto, precio Excel por m y por kg, regla de la lista de Odoo,
precio de la lista, diferencia %, «sin regla», «precio basura»). **No la puedo entregar hoy**: el archivo no está en el
repo ni en la sesión (y no debe subirse). En cuanto me lo pases la corro fuera del repo y te mando el resultado.

---

## 5. Plan por entregas

Mismo orden que el prompt. El artículo (2.2) y la vista del real (2.7) se adelantan a E1 porque el consumo de pronóstico
por pedidos y la Demanda por cliente ya los necesitan.

| Entrega | Contenido | Revisión del diseño | Staging (Odoo.sh) | Producción |
|---|---|---|---|---|
| **E1** | Artículo y unidades; `qb.forecast.line` + bitácora; matriz y «Pegar desde Excel»; releases con planta, lector Excel genérico, confirmación en bloque, aplicar al pronóstico nuevo; `qb.demand.planning`, Demanda por cliente, grupo Planeación; MPS desde la fuente nueva y parche «Demanda por cliente»; migración de los 21 pronósticos; carga de `forecast.xlsx`; comparación de precios | 11-nov (Jessica + Planeación) | 20-nov | **25-nov** |
| **E2** | Presupuesto anual por año + revisión, mercados, proyectos, precio y TC capturables; `qb.sales.actual`; VE-02 sobre la vista; reportes mensual, anual, cliente × artículo, variación y faltantes; presupuesto del mes con PDF y correo; armado de la propuesta del año; carga REV0/REV1 | — | 4-dic | **9-dic** (Jessica arma 2027 del 9 al 18-dic; cierre de noviembre en paralelo con el Excel) |
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

1. ¿Las aplicaciones de releases del 2-nov en producción fueron prueba? Si sí, ¿se deshacen antes de E1 o se migran tal
   cual?
2. TC de la REV1: ¿17.5 (correo del 25-jun) o 17.2 (hoja ANUAL)?
3. Para Planeación, ¿kilos con el rendimiento de especificación (gramaje × ancho) o con el real de la ficha?
4. ¿El equipo «Distribuidor» y las ventas sin equipo de clientes de giro van a algún mercado o se reportan aparte?
5. Objetivo y mínimo: ¿sobre la venta total del año anterior o por mercado?
6. ¿Se acepta que el mercado salga del cliente (no de la factura), con la conciliación de 4.3?

**Para Ventas (Jessica)**

7. ¿Qué es hoy un «proyecto» en tu Excel: producto nuevo, cliente nuevo o los dos? ¿Cuándo pasa a «línea»?
8. ¿El precio presupuestal de industrial lo manejas por metro o por kilo, y en qué moneda, por cliente?
9. Hoja MUESTRAS: ¿se queda en Excel o la quieres en Odoo (sería otra entrega)?
10. Plantas: ¿FXI Cd. Juárez y Santa Teresa mandan un release cada una?
11. Para el armado del 2027: ¿el factor de tendencia de ±30 % te sirve como tope, o lo pones tú por cliente?

**Para Planeación**

12. Unidad de trabajo: metros, kilos o las dos (la vista trae las dos; se elige cuál va primero).
13. ¿El corte de tejido es el mes calendario siguiente o las próximas 4–5 semanas?
14. En el MPS, ¿los programas deben quedar en el producto en kilos (IW) o en metros (W)?
