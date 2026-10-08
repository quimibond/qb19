# Releases de clientes — pedidos ligados, lectores por formato, perfil por planta y normalización

Respuesta a la solicitud de José (8-oct-2026) sobre los cuatro huecos que dejó
ver la carga manual de la semana 41 (7-oct-2026). **No se ha programado nada**:
este documento es la propuesta para el visto bueno. Rutas relativas a
`addons/quimibond_ventas_presupuesto/` salvo que se diga otra cosa.

Lo que hay en producción se leyó por MCP, solo lectura, el 8-oct-2026.

---

## 0. Cómo funciona el módulo hoy (lo que entendí)

Versión 19.0.1.3.1 del addon `quimibond_ventas_presupuesto`. Cinco modelos en
`models/qb_customer_part.py` y `models/qb_release.py`; la lógica pura en
`release/` (`lear_aiag.py`, `fxi_sum.py`, `part_match.py`, `units.py`),
probada con pytest en `tests_puros/ventas_presupuesto/`.

**Perfil (`qb.release.profile`).** Uno por `(compañía, cliente, planta)`:
`partner_id` obligatorio, `ship_to_id` opcional, `supplier_code`, `team_id`,
`reader` (Selection obligatoria con dos valores: `lear_aiag`, `fxi_sum`),
`date_basis` (embarque / entrega) + `transit_days`, `firm_rule` (N semanas o
hasta Fab Auth) + `firm_weeks`, `so_weeks`, acuse y CUM (`cum_managed`,
`cum_reset_date`), responsables. Restricción SQL
`unique nulls not distinct (company_id, partner_id, ship_to_id)`.

**Lectores.** `qb.release._qb_parse_file()` despacha por `profile.reader`:
decodifica el archivo (`file` Binary) y llama a la función pura, que devuelve
`{'parts': [...]}`; el modelo normaliza cada parte a un mismo diccionario
(`release_ref`, `release_date`, `customer_part`, `uom`, `customer_po`,
`cum_received`, autorizaciones, `lines: [{date, qty, line_type, cum_req,
net_req}]`). Lear: texto AIAG, acepta `.eml` y lo abre; parsea Prior
(`prior_req_qty`, `prior_cum_req_qty`) **pero el modelo lo descarta**. FXI:
Excel SAP, bloques de tres filas (Date / Gross need / V Cum), 52 fechas de
entrega. Los dos lectores levantan `ValueError` con el motivo y son todo o
nada.

**Lectura (`action_read`).** Solo desde `recibido`, `por_revisar` o `leido`.
Borra las partes, parsea; si falla → `por_revisar` + `error_reason`; si no,
crea `qb.release.part` (busca o crea la parte del catálogo `qb.customer.part`
por cliente + planta + parte, y le sugiere producto sin confirmarlo) y sus
`qb.release.line`, escribe `release_ref`, `release_date` y pasa a `leido`.
Luego `_qb_link_previous()`.

**Versionado.** `_qb_link_previous()` corre **solo dentro de `action_read`**:
`previous_id` = el último release del mismo perfil en `aplicado` o
`reemplazado` (por fecha e id); `version` = máximo de versiones del perfil + 1
si estaba en 0. Consecuencias: (a) un release creado por RPC sin pasar por
«Leer» se queda con `version = 0` y `previous_id` vacío (ids 3 a 9); (b)
mientras nada se haya aplicado, `previous_id` queda vacío aunque haya pasado
por el lector (ids 1 y 2: versión 1, sin anterior).

**Estados.** `recibido → leido → revisado → aplicado`, con `por_revisar`
(lector falló), `reemplazado` (otro release del mismo perfil se aplicó) y
`descartado` (manual; un aplicado no se descarta). «Marcar revisado» y
«Aplicar» son del gerente de ventas.

**Semanas (`qb.release.line`).** `date_customer` tal cual viene;
`date_ship = date_customer − transit_days` si el perfil es de entrega;
`week` = lunes de `date_ship`; `qty_customer` en la unidad del cliente;
`qty` en la unidad del producto vía `qb.customer.part.to_product_qty()`
(`units.py`: MT/LY/M2/KG con rendimiento m/kg y ancho de la ficha, o factor
fijo); `qty_ok` si se pudo convertir; `zone` (`firme` / `materia_prima` /
`planeado`) calculada por la regla del perfil y las autorizaciones de la
parte, **no** por el `line_type` del documento.

**Aplicar (`action_apply`).** Exige `revisado`, todas las partes confirmadas en
el catálogo y todas las semanas convertidas. Suma `qty` por (producto, semana),
toma el pronóstico `sgi.sales.budget` (`kind='pronostico'`) del cliente por
año (lo crea si falta), y desde la primera semana del release: las celdas del
release se escriben con `release_id` + `forecast_source='release'`, las celdas
de esos productos que el release ya no trae quedan en 0, las anteriores no se
tocan. El pronóstico pasa a `revisado`, **todos** los `aplicado` del mismo
perfil pasan a `reemplazado`, el release queda `aplicado` con `forecast_ids`,
y se recalcula el MPS (`_qb_mps_refresh`, suma de todos los clientes).

**Lo que no existe hoy.** Ningún campo une `sale.order` / `sale.order.line`
con `qb.release*` ni con `qb.customer.part`. El único puente pedido ↔
release es indirecto: `sgi.sales.budget.line.qty_ordered` (pedidos del mes por
producto y equipo) y `release_id` en la línea del pronóstico.

### 0.1 Estado real de los registros (producción, 8-oct)

| id | Perfil (reader) | Release | Estado | version | previous | Archivo en Odoo | Semanas |
|---|---|---|---|---|---|---|---|
| 1 | Lear (lear_aiag) | 000174 · 24-sep | leido | 1 | — | sí (.txt) | 53 (incluye ceros) |
| 2 | FXI (fxi_sum) | 21108 · 25-sep | leido | 1 | — | sí («(valores).xlsx», copia editada) | 104 |
| 3 | FXI | 21138 · 2-oct | leido | 0 | — | no | 104 |
| 4 | Lear | 000175 · 2-oct | leido | 0 | — | no | 53 (incluye ceros; la fila «Prior» entró como semana 22-sep con 0) |
| 5 | Saltillo (fxi_sum) | 2026278C · 5-oct | leido | 0 | — | no | 37 (290530: 26 con ceros; 290524 y 290552 solo con cantidad) |
| 6 | Saltillo | 2026280 León 130551L · 7-oct | leido | 0 | — | no | 6 |
| 7 | Shawmut (fxi_sum) | Silao W41 | leido | 0 | — | no | 65 |
| 8 | Shawmut | TN 20261006 PO 70152 | leido | 0 | — | no | 17 |
| 9 | Zwisstex (fxi_sum) | CW41 | leido | 0 | — | no | 10 |

- Los cinco perfiles tienen `ship_to_id` vacío. Ni Saltillo Lamination ni
  Shawmut tienen contactos hijos de tipo «dirección de entrega»: **León,
  Clinton TN y Silao no existen como partners**. Lear sí tiene «ZARAGOZA
  PLANT» (8101) como dirección de entrega.
- Los nueve archivos originales (y los de las semanas 39 y 40) están en la
  memoria de correo (Supabase, bucket `email-attachments`, con texto
  extraído): `130551 - Quimibond 10-05-2026 D.pdf`, `Release Quimibond
  WK41.D.pdf`, `SL 3254 Release for W41 2026.csv`, `Quimibond TN Release
  20261006.xls`, `RELEASE QUIMIBOND CW41.xlsx`, `Quimibond 05_10_2026.pdf`
  (Copo), `SUM Quimibond 10.5.26.xlsx`, `QUIMIBOND.eml` (Lear).
- En el release 4 de Lear, la línea del 22-sep tiene `qty 0`, tipo `F` y
  `cum_req 771,119 = 761,619 (Cum Received) + 9,500`: el «Prior» se perdió como
  cantidad y solo sobrevivió en el acumulado.

### 0.2 Cómo se capturan hoy los pedidos (lo que cambia el diseño)

Pedidos de Saltillo Lamination (partner 5796) desde agosto:

- **Un pedido por camión**, confirmado: `PV16968` (entrega 8-oct) y `PV16969`
  (10-oct) traen la misma mezcla (9,372 m de WJ053Q22JNT160 + 4,572 m de
  WJ032Q22JNT160), PO 6700. La semana del release (43,238 LY ≈ 39,537 m de
  290530 para el 5-oct) se reparte en varios camiones, y un camión puede caer
  entre dos semanas.
- **La planta no está en el pedido como dirección**: Saltillo y León llevan
  `partner_shipping_id = 5796` (la matriz). Lo que distingue la planta es el
  PO (`client_order_ref` 6700 / 7516 / 7508 = Saltillo, 6600319 = León) y una
  nota «SALTILLO» / «LEON» en las líneas.
- **Lo embarcado no es lo pedido**: se embarcan rollos completos y
  `qty_delivered` supera `product_uom_qty` (5,687 m entregados contra 4,572
  pedidos en `PV16931`; 9,623 contra 9,372 en `PV16968`). «Embarcado» tiene
  que salir de los movimientos de almacén hechos (`stock.move` `done` con
  destino cliente), no de la cantidad pedida.
- Lear: un pedido por semana con PO 1030782; FXI: un pedido por PO SAP
  (4500572100, 4500572101…); Shawmut: POs 70152 (TN), 12033, 1759, 1794,
  1856 (Silao); Zwisstex: PO 14792 / 15486; Copo: PO 5721.
- El mismo producto sirve a varias partes: WJ053Q22JNT160 (12032) es 290530
  (Saltillo), 290530L y 290522L (León), M208908 (Zwisstex), 362110 (Copo) y
  SCR56 (TQ-1). **Dentro de León dos partes caen al mismo producto**, así que
  la parte no se puede deducir solo del producto: la línea del pedido tiene
  que decir qué parte es.

### 0.3 Dónde lo que describe la solicitud no coincide con el código

1. «`version` y `previous_id` quedan vacíos»: `version` queda en **0** (Integer
   con default), no en NULL; el efecto es el mismo.
2. «Los releases anteriores (ids 1 y 2) no se marcaron como reemplazados»: hoy
   **eso es por diseño**: `reemplazado` solo se pone al **aplicar** el
   siguiente, y `previous_id` solo apunta a un release aplicado. Mientras
   nada se aplique, ningún release queda reemplazado aunque pase por el lector.
   Propongo cambiarlo (sección 4).
3. El spec E0 decía que Woodbridge es «todo firme ~26 semanas»; el PDF trae
   tipo Firm / Forecast por semana (290552 cargada como Forecast). Propongo
   una regla de zona firme nueva que respete lo que dice el documento.
4. «Cada pedido cubre parte de una semana o de dos» y «la conciliación real
   es CUM contra CUM» coinciden con los datos; la planta por `ship_to` del
   pedido **no**: hoy la planta va en el PO y en una nota.

---

## 1. Ligar pedidos de venta al release

### 1.1 Decisión: la liga vive en la línea del pedido y apunta a la parte del catálogo, no al release

**Campo nuevo `sale.order.line.qb_customer_part_id`** (Many2one a
`qb.customer.part`, almacenado, indexado, opcional) y **`sale.order.qb_release_profile_id`**
(Many2one calculado almacenado: el perfil de la planta de las partes del
pedido). Nada apunta a `qb.release` ni a `qb.release.line` desde el pedido.

Por qué la parte del catálogo y no el release:

- El release se **reemplaza cada semana**; la parte del cliente (290530 en la
  planta Saltillo, PO 6700) es la entidad estable a la que el cliente mismo
  refiere su CUM, sus autorizaciones y sus recibos. Un pedido ligado a la
  parte queda ligado a todas las versiones, pasadas y futuras.
- La parte ya resuelve producto, planta (`ship_to_id`), PO (`customer_po`) y
  unidad; es el único dato que falta en la línea del pedido para saber «este
  camión es de 290530 y no de 290530L».
- Es compatible con E2 (pedidos generados desde la zona firme): esos pedidos
  nacen con la parte puesta y `origin = release`.

Cómo se llena:

- **Al confirmar el pedido** (y al escribir `product_id` / `client_order_ref`
  en un pedido en borrador): si el cliente comercial tiene perfil de release,
  se busca la parte **confirmada** con ese producto; se desempata por
  `customer_po = client_order_ref` y, si el pedido trae `partner_shipping_id`
  hijo del cliente, por `ship_to_id`. Si queda exactamente una, se pone; si
  hay cero o varias, **se deja vacía y se avisa** en el pedido (nunca se
  adivina). Ventas la pone a mano desde la línea (campo visible solo para
  clientes con perfil).
- Backfill: una acción «Ligar partes» en el perfil (y en la migración, para
  los pedidos confirmados desde `cum_reset_date`) corre la misma regla sobre
  los pedidos existentes y lista los que quedaron sin parte.

### 1.2 Pedido, embarcado y faltante por semana: aritmética de acumulados, sin tabla de asignación

El cliente concilia por CUM. Nosotros hacemos lo mismo y de ahí sale lo
semanal, sin que nadie reparta camiones a semanas a mano:

Por parte del catálogo (y por perfil, desde `cum_reset_date`):

- `cum_embarcado` = Σ `stock.move` en `done`, de salida a cliente
  (`picking_type_code = outgoing`, `location_dest_id.usage = customer`),
  cuyo `sale_line_id.qb_customer_part_id` sea la parte, con `date ≥
  cum_reset_date`, en la **unidad del cliente** (`to_customer_qty`, inversa de
  `to_product_qty`). Las devoluciones (moves de entrada desde cliente con
  `origin_returned_move_id`) restan.
- `cum_pedido` = `cum_embarcado` + Σ `qty_to_deliver` de las líneas
  confirmadas de esa parte (lo pedido y aún no embarcado).

Por semana del release (`qb.release.line`), con `cum_req` del documento (o la
suma acumulada de `qty_customer` más el Prior cuando el documento no trae
CUM, como Zwisstex o Shawmut TN):

- `qty_shipped` = `clamp(cum_embarcado − cum_req_anterior, 0, qty_customer)`:
  lo embarcado llena las semanas más viejas primero (FIFO por acumulado).
- `qty_ordered` = `clamp(cum_pedido − cum_req_anterior, 0, qty_customer) − qty_shipped`:
  lo pedido sin embarcar que cae en esa semana.
- `qty_open` = `qty_customer − qty_shipped − qty_ordered`: lo que falta por
  pedir (o, si es negativo, lo sobrepedido contra el release).
- Los tres también en unidad del producto (`qty_shipped_product`…) para
  sumarlos en la pestaña «Semanas» (`sum=`) y leerlos junto a `qty`.

Esto resuelve los dos casos que pides sin asignación manual: un camión que
cubre parte de una semana aparece como `qty_shipped` parcial, y uno que cruza
dos semanas termina de llenar la primera y empieza la segunda. La fecha de
embarque de nuestros camiones **no** decide la semana; decide el acumulado,
igual que lo lee el cliente.

Por parte del release (`qb.release.part`), para la conciliación con lo que
el cliente reporta:

- `cum_shipped_ours` (nuestro CUM embarcado a la fecha del release, unidad del
  cliente), `cum_diff = cum_shipped_ours − (cum_received + in_transit_qty)`,
  `cum_state` (`cuadra` dentro de la tolerancia del perfil / `no_cuadra` /
  `sin_dato` cuando el documento no trae CUM). Son los campos `cum_ours`,
  `cum_diff`, `cum_state` que ya preveía el E0 (§4.2).
- Alineación inicial: `qb.customer.part.cum_base_qty` + `cum_base_date`
  («el cliente tenía recibido X al día D»); nuestro CUM = X + embarcado después
  de D. Default: 0 en `cum_reset_date` del perfil. Hace falta porque Woodbridge
  cuenta YTD, FXI desde el agreement y Lear desde su año modelo, y porque los
  embarques anteriores a Odoo o sin parte ligada no entran a la suma.

Cálculo **no almacenado**, en lote por release (una búsqueda de moves y una de
líneas de pedido por release, no por semana). Se ve en la pestaña «Semanas»
(columnas Pedido / Embarcado / Falta con totales) y en «Partes» (CUM nuestro,
diferencia, estado). Botón «Pedidos» en la parte del release → líneas de
pedido de esa parte desde `cum_reset_date`; botón «Release vigente» en el
pedido → el último release aplicado (o leído) del perfil. Si después hace
falta un pivote por cliente y semana, se almacena y se refresca al validar
entregas y con el cron de la hora; no lo propongo ahora.

### 1.3 Semana de embarque de un pedido

`sale.order.commitment_date` es la entrega en planta del cliente (así lo
captura Ventas: 8-oct para un camión que salió el 7-oct). Para la vista
«Pedidos de la semana» la semana de embarque del pedido = lunes de
`commitment_date − transit_days` del perfil. No se usa para la asignación
(que es por CUM), solo para listar y para la cobertura del pronóstico (N4 del
E0 queda igual: un pedido = una fecha).

### 1.4 Alternativas descartadas

| Alternativa | Por qué no |
|---|---|
| Many2many `sale.order.line` ↔ `qb.release.line` | Se rompe cada semana (el release vigente es otro registro) y no puede decir cuánto de un camión va a cada semana; un camión que cruza dos semanas necesitaría cantidad por liga. |
| Tabla de asignación `qb.release.allocation` (release_line, sale_line, qty) | Da el mismo resultado que el FIFO por acumulado pero mantenido a mano; nadie va a repartir camiones a semanas cada semana, y el cliente nunca lo hace así. Se puede derivar después si hace falta auditar. |
| Many2one en `sale.order` (cabecera) a `qb.release` | Un pedido trae varias partes (WJ053 + WJ032 en el mismo camión) y el release tiene varias; además versión churn. |
| Many2one en `sale.order.line` a `qb.release.line` | Lo mismo que el m2m: cada versión nueva deja las ligas apuntando a un release reemplazado. |
| Detectar la planta por `partner_shipping_id` | En producción las dos plantas de Saltillo comparten dirección (la matriz); el PO es lo que distingue. Se usa como desempate si viene, no como regla. |
| Usar `qty_delivered` de la línea en vez de `stock.move` | `qty_delivered` sí sirve para el total, pero no trae fecha ni distingue devoluciones; los moves dan ambas cosas y son la misma fuente. |
| Almacenar `qty_shipped` / `qty_ordered` | Obliga a recalcular en cada entrega (override de `stock.move._action_done`) o aceptar datos viejos; no hace falta para la ficha del release. |
| Ligar por `origin` / `client_order_ref` (texto) | Queda como traza (E2 pondrá `origin = release`), no como llave: el PO 6700 es un blanket para muchas partes. |

### 1.5 Cambios de datos que esto necesita (los hace Ventas, no la migración)

- Confirmar las partes del catálogo que siguen en `sugerido` para los clientes
  con release (290524, 4002749, M101418, M208902, 244635…): sin parte
  confirmada no hay liga.
- Revisar la liga de los pedidos desde `cum_reset_date` que la regla no pudo
  resolver (lista en el perfil).
- Capturar `cum_base_qty/date` donde nuestro histórico no cuadre con el CUM del
  cliente (se ve en `cum_diff` al leer el siguiente release).

---

## 2. Lectores para más clientes

Un lector por formato, función pura en `release/`, misma salida normalizada
que los dos actuales, fixtures anonimizados en
`tests_puros/ventas_presupuesto/fixtures/` y pruebas pytest; `_qb_parse_file`
pasa a una tabla `reader → función` en vez del `if` encadenado.

| `reader` | Cliente / planta | Archivo | Librería | Qué saca |
|---|---|---|---|---|
| `woodbridge_pdf` | Saltillo Lamination (130551, PO 6700) y León (130551L, PO 6600319) | PDF «Supplier Release Planning Schedule» (`130551 - Quimibond MM-DD-YYYY D.pdf`, `Release Quimibond WKnn.D.pdf`) | `pdfminer.six` (ya la usa Odoo para indexar PDF) por posición | Por parte: Cum YTD Required / Received, Last Receipt (fecha y cantidad), Fab / Material Cum; por semana: Net Quantity, Cum Quantity, Previous Release, Net Change, tipo Firm / Forecast. El número de proveedor (130551 / 130551L) se contrasta con `supplier_code` del perfil. |
| `shawmut_sl_csv` | Shawmut Silao 3254 (y 3150 cuando llegue) | CSV «SL 3254 Release for Wnn» | `csv` | Ordered CYTD, Received CYTD, Mat Auth, Fab Auth, 12 semanas. Planta 3254 / 3150 contra `supplier_code`. |
| `shawmut_tn_xls` | Shawmut Clinton TN (PO 70152) | XLS «Vendor Release Report - Weekly» | `xlrd` (en los requirements de Odoo para `.xls`) | Renglones de requerimiento y balance por fecha de entrega en Clinton; PO. |
| `zwisstex_xlsx` | Zwisstex | XLSX «RELEASE QUIMIBOND CWnn», una fila por material y columnas semanales | `openpyxl` | Material, descripción, cantidad por semana (encabezado de columna → fecha). |
| `copo_pdf` | Copo / CTM (proveedor 410) | PDF `Quimibond DD_MM_2026.pdf`, semanas en columnas | `pdfminer.six` por posición (x de cada celda contra el x del encabezado de su columna; las columnas vacías no se pierden) | Parte, semana, cantidad, Firme / Forecast. |

Cambios al modelo que piden estos formatos:

- `qb.release.part`: `prior_qty` («Vencido (Prior)», Lear) y `prior_cum_req`;
  `cum_required_ytd` (Woodbridge, Shawmut «Ordered CYTD»); `mat_auth_qty`
  (Shawmut) cae en `raw_auth_qty`, `fab_auth_qty` ya existe; `last_receipt_*`
  ya existen.
- `qb.release.line`: `line_type` se normaliza a `F` / `P` (Firm / Forecast,
  Firme / Forecast); `previous_qty` y `net_change` (Woodbridge trae la
  comparación contra su release anterior; sirve para cotejar la nuestra).
- `qb.release.profile.firm_rule` gana `documento` («lo que marque el release»:
  `F` = firme), para Woodbridge y Copo.
- `qb.release.horizon_end`: última fecha que cubre el documento (sección 4).
- Validación de identidad: cada lector devuelve `supplier` / `plant` cuando el
  archivo lo trae; si no coincide con `supplier_code` del perfil, el release
  queda `por_revisar` con «el archivo es del proveedor 130551L y el perfil dice
  130551». Es lo que habría evitado que León quedara bajo Saltillo.
- Los perfiles 3, 5 y 7 cambian de `fxi_sum` a su lector (migración de
  datos, por id de perfil y solo si siguen en `fxi_sum`).

Para escribir y probar los lectores necesito los archivos. Están en el bucket
privado `email-attachments` de Supabase (los nueve de la semana 41 y los de la
39 y 40); desde esta sesión puedo leer su texto extraído por SQL pero no bajar
el binario. Opciones: (a) me pasas los archivos; (b) los bajo del correo con
el conector de Gmail. En cualquier caso **al repo solo entran fixtures
anonimizados** (partes, cantidades y PO inventados con el mismo layout).

---

## 3. Un perfil por planta

**Qué lo impide.** La restricción `_profile_uniq`:
`unique nulls not distinct (company_id, partner_id, ship_to_id)`. Los cinco
perfiles tienen `ship_to_id` vacío; con `nulls not distinct` dos vacíos
cuentan como iguales, así que un segundo perfil del mismo cliente **sin
planta** choca contra el primero. Odoo lo convierte en un error de validación
con el mensaje «Ya hay un perfil de release para ese cliente y planta»; por
MCP salió como «Internal server error» porque el servidor MCP no traduce ese
error. Lo confirmo con una prueba (segundo perfil con planta distinta →
pasa; sin planta → `ValidationError` con mensaje). Si la prueba muestra otra
causa, la reporto antes de seguir.

**El bloqueo real es de datos:** León, Clinton TN y Silao no existen como
contactos. La migración no inventa partners; los crea Ventas (dirección de
entrega hija del cliente) y entonces se crean los perfiles.

Propuesta:

- `ship_to_id` sigue siendo la llave de la planta; la restricción se queda.
  Se agrega una restricción Python legible: si el cliente ya tiene otro
  perfil activo, los dos deben tener planta.
- Perfil por planta con su `supplier_code` (130551 / 130551L; 3254 / TN),
  `transit_days` y `date_basis` propios; `qb.release.ship_to_id` related
  almacenado para filtrar; el nombre ya incluye la planta.
- Releases 6 (León) y 8 (TN) se mueven a su perfil nuevo en el script de
  normalización (sección 4), no en la migración, porque dependen de los
  partners que creará Ventas. Hasta entonces aplicar 5 o 6 marcaría
  reemplazado al otro: **no aplicar ninguno de los dos** mientras compartan
  perfil.
- Las partes del catálogo de León (290524L, 290530L, 290522L) y de TN
  (239361, 240976, 244635, 245646 con PO 70152) reciben su `ship_to_id`; la
  búsqueda de parte al leer ya prefiere la de la planta.
- `_qb_link_previous` y `action_apply` ya trabajan por perfil; con perfiles
  separados dejan de cruzarse.

Partners que hacen falta (Ventas, en producción): «Saltillo Lamination —
Planta León» (hija de 5796), «Shawmut — Clinton TN» y «Shawmut — Silao 3254»
(hijas de 1780). Lear ya tiene Zaragoza (8101); si el release 5500 es de esa
planta, el perfil 1 toma `ship_to_id = 8101`.

---

## 4. Lo que la carga manual dejó incompleto

### 4.1 Versión y release anterior

- `version` se asigna **al crear** (siguiente consecutivo del perfil) y no
  solo al leer; restricción: única por perfil.
- `previous_id` = el release anterior del mismo perfil por (`release_date`,
  `id`) en cualquier estado salvo `descartado`, se asigna al crear y al leer.
- **Reemplazo al leer:** cuando un release pasa a `leido`, los anteriores del
  perfil que **nunca se aplicaron** (`recibido`, `por_revisar`, `leido`,
  `revisado`) pasan a `reemplazado` (hay un documento más nuevo; no tiene
  sentido aplicar uno viejo). Los `aplicado` siguen pasando a `reemplazado`
  solo cuando se aplica el nuevo, porque el pronóstico aún refleja al viejo.
  Esto deja a 000174 y 21108 en `reemplazado`, como esperas.
- Migración: a los releases existentes se les asigna `version` por orden
  (`release_date`, `id`) dentro de su perfil y `previous_id` con la misma
  regla; los estados los deja el script (porque antes hay que separar
  perfiles).

### 4.2 Prior (vencido)

`qb.release.part.prior_qty` y `prior_cum_req` (ya los parsea `lear_aiag`;
Woodbridge los trae como diferencia Cum YTD Required − Received cuando hay
semanas pasadas). Se muestra en la pestaña Partes como «Vencido» y entra al
cálculo de `qty_open` de la primera semana (lo vencido es lo primero que
hay que embarcar). No se crea una semana artificial con fecha pasada; la línea
del 22-sep del release 4 desaparece al releer.

### 4.3 Semanas en cero: sí existen cuando el documento las imprime

Regla: **el lector guarda cada semana que el documento imprime, con su
cantidad, cero incluido; no inventa semanas**. Lear y FXI imprimen 52 fechas
(con ceros); Woodbridge, Shawmut, Zwisstex y Copo imprimen solo semanas con
cantidad (o columnas fijas con celdas vacías, que son ceros). Para que «no
está» signifique «fuera del horizonte» y no «el cliente dice cero», el
release guarda `horizon_end` (última fecha del documento); dentro del
horizonte, una semana sin línea vale 0 para la aplicación y para comparar
versiones. Por eso 290530 con sus 26 semanas (ceros incluidos) está bien y
290524 / 290552 con solo las semanas con cantidad también, siempre que el
release traiga `horizon_end`. La aplicación al pronóstico no cambia: las
semanas que el release no trae desde la primera quedan en 0.

### 4.4 Cómo normalizar los ids 1 a 9: releer desde el archivo original, con un script

No un script de corrección campo por campo: los datos a mano traen
diferencias que no vale la pena cazar (Prior perdido, ceros omitidos, release
2 cargado desde una copia «(valores)» del Excel). Los nueve originales
existen, así que:

1. Despliegue del módulo (migración: campos nuevos, versiones, lectores en
   los perfiles 3/5/7).
2. Ventas crea los partners de planta; se crean los perfiles León y TN.
3. Script `tools/releases_normalizar_2026_10.py` por JSON-RPC (como el resto
   de integraciones del repo), **idempotente y con modo `--dry-run`**, que para
   cada release 1–9: adjunta el archivo original (`file` + `filename`), mueve 6
   y 8 a su perfil, corre `action_read` (borra y recrea partes y semanas; el
   id del release, su chatter y sus `forecast_ids` se conservan), y verifica
   contra lo cargado a mano (total por parte y CUM) dejando una nota en el
   chatter con las diferencias. Al final aplica la regla de reemplazo (1 y 2 →
   `reemplazado`).
4. Verificación: los totales de la sección 0.1 contra los nuevos; `version`
   1..n por perfil sin huecos; ningún release con `version 0`.

Si algún original no se pudiera leer con el lector nuevo, ese release queda
`por_revisar` con el motivo y se corrige el lector, no el dato.

---

## 5. Orden de trabajo, pruebas y despliegue

Todo en el mismo addon, versión `19.0.1.4.0`, una migración
`migrations/19.0.1.4.0/post-migrate.py` (versiones y `previous_id`, lector por
perfil, liga de partes en pedidos confirmados desde `cum_reset_date`).

1. Punto 1 (liga y acumulados) con pruebas de Odoo: pedido de dos camiones
   contra un release de tres semanas, camión que cruza semana, devolución,
   parte ambigua que no se liga sola, CUM con base inicial.
2. Punto 3 (perfil por planta) con prueba de la restricción y del contraste
   `supplier_code`.
3. Punto 2 (cinco lectores) con pytest sobre fixtures anonimizados y una
   prueba de Odoo por lector (leer → partes → semanas → zona).
4. Punto 4 (versión al crear, reemplazo al leer, Prior, `horizon_end`) con
   pruebas; script de normalización probado contra una copia (Odoo.sh build
   de desarrollo con base de producción) antes de correrlo en producción.

Pruebas de Odoo en el build de desarrollo de Odoo.sh con
`--test-tags /quimibond_ventas_presupuesto`; pytest en el CI. Despliegue por
el runbook (`main` → `quimibond`), y la normalización **después** de verificar
el update en producción.

## 6. Preguntas abiertas (no bloquean el punto 1)

1. ¿`cum_reset_date` por perfil: Woodbridge 1-ene-2026 (YTD), FXI inicio del
   agreement 5500000269, Lear inicio de año modelo, Shawmut 1-ene (CYTD)?
   Mientras no esté, `cum_base_qty/date` por parte lo cubre.
2. ¿Quién crea los partners de planta y con qué nombre? (Propuesta arriba.)
3. Tolerancia de CUM para `cum_state = cuadra`: propongo 1 % o un rollo,
   configurable en el perfil (`cum_tolerance`).
4. Archivos originales: ¿me los pasas o los bajo de Gmail?
