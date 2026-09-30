# E0 — Releases de clientes → pronóstico → pedidos → presupuesto

Respuesta a «PROMPT_presupuesto_y_pronostico_ventas.md» v2 (José Mizrahi,
30-sep-2026), sección 9. **No se ha programado nada**: este documento es lo que
pide el E0 para dar el visto bueno.

Contenido:

1. Cuestionario por cliente (prellenado con la evidencia del correo; falta la sesión con Jessica y Berenice)
2. Reporte de auditoría del módulo actual (sección 4 del prompt, confirmada o corregida con archivo:línea, más hallazgos nuevos)
3. Faltantes de datos maestros (medidos en producción)
4. Diseño técnico
5. Plan por entregas con horas y fechas
6. Riesgos y preguntas

Rutas relativas a `addons/quimibond_ventas_presupuesto/` salvo que se diga otra cosa.

---

## 1. Cuestionario por cliente

**Estado:** prellenado con la evidencia del correo 2026 (memoria de Supabase
y Gmail, conteo de correos originales del cliente sin RE/FW ni duplicados por
buzón, jul-sep = «3m»). **Falta la sesión con Jessica Francisco y Berenice
Vázquez** para confirmar las casillas marcadas «?» y conseguir los 2 releases
anonimizados por cliente. Las celdas vienen del correo, no de ellas.

Hallazgo que cambia el alcance: **no son dos clientes con release, son ocho**
(Lear, FXI, Woodbridge Saltillo, Woodbridge León, Shawmut, Zwisstex, Copo/CTM
y TQ-1), más Contitech y Seiren con forecast mensual o trimestral. Y **Lear ya
manda EDI** por GXS/OpenText iExchangeWeb (39 avisos «Document Received …
Release» a innovacion@ en 2026); el .eml de texto es la impresión de esa misma
transacción.

| Cliente | ¿Release / forecast / solo PO? | Canal y formato | Frecuencia y día | Horizonte | Zona firme / autorizaciones | ¿Embarque o entrega? | Unidad y partes del cliente | CUM | Acuse y plazo | Le llega a / contesta | ¿EDI o portal conectable? |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Lear MTO** (ship-to 5500, proveedor 6PIN0010) | Release semanal | EDI (GXS/OpenText iExchangeWeb) + correo con `QUIMIBOND.eml` de texto AIAG «SUPPLIER SCHEDULE / MATERIAL RELEASE» | Semanal, jueves (a veces miércoles); 13 en 3m | 52 semanas | Fab Auth thru +3 sem, Raw Auth thru +6 sem; líneas Q=P | Ship date | **MT**; L002790184NCPAA (BACK SCRIM 62"), PO 1030782 | Sí: Cum Received, In Transit, Cum Req; Packing Slip = nuestra factura | «Please confirm when you receive»; pide cotejar CUM y promesa de embarque. Plazo ? | berenice.vazquez, innovacion, ventasindustrial; contesta innovacion | **Sí: EDI en iExchangeWeb.** ? si se puede sacar el 830 en archivo o por AS2 |
| **FXI** (Cd. Juárez / Sta. Teresa) | Release semanal («Delivery Releases») | Correo + Excel `SUM Quimibond MM.DD.YY.xlsx` (reporte SAP «Summary Report» volcado) | Semanal, viernes noche (fechado al lunes); 13 en 3m | 52 semanas + total sem. 27-52 | «High authorization» dentro del lead time; Vendor Authorization y Raw Accum Rcvd | **Entrega en FXI** | **LY (yardas lineales)**; 4002749 (WD3846NT163M2, 163 cm), 4010670 (WD038Q46JNG163); vendor 105925, planta 1500, agreement 5500000269 | Sí: V Cum y Raw Accum Rcvd | Acuse semanal; **acumulados al día siguiente** (si no, 100 % de coincidencia); plan UTS si aumenta; corrida mínima al martes | gilberto, innovacion, berenice.vazquez; cc dl_Expeditadores@fxi.com (siempre) | Mencionan «Manual or EDI»; ? |
| **Woodbridge – Saltillo Lamination** | Release semanal + PO blanket 6700 (y PO puntual 7516 para T1XX-2 GM) | Correo + 2 PDF de sistema: «Vendor Planning Schedule» (PO064R) y «SUPPLIER RELEASE PLANNING SCHEDULE» (`130551 - Quimibond MM-DD-YYYY.pdf`) | Semanal, lunes a miércoles; 18 en 3m | ~26 semanas, todo firme | Fab Cum y Material Cum | **Due-in (entrega)**, no embarque | **MTR y LY** según parte; 290524, 290530 (WJ053Q22JNT160), 290552; vendor 130551 | Sí: Cum YTD Required / Received | **24 h** (sin respuesta = sin problema) | innovacion, gilberto, ventasindustrial | ? |
| **Woodbridge – León Lamination** | Release semanal | Mismo PDF (`Release Quimibond WKnn.pdf` + `.D.pdf`) | Semanal, martes a jueves; 21 en 3m | ~26 semanas | Igual que Saltillo | Due-in | 290524L, 290530L; vendor 130551L, PO 6600319 | Sí | 24 h | innovacion, logistica, ventasindustrial | ? |
| **Shawmut** (Clinton TN; Silao plantas 3150 y 3254) | 3 releases semanales | Correo: `Quimibond TN Release YYYYMMDD.xls` y `SL 3254 Release for Wnn.csv` / «SL 3150». Quimibond devuelve un ASN «EDI 856» en Excel | Semanal, lunes a miércoles; 37 en 3m | TN 16 sem; SL ~13 sem | SL: Mat Auth, Fab Auth, Program End Date | **Delivery** en Clinton | Yardas (inferido); 239361, 244101, 245370, 245646, 252113, 240395, 242803, 240976; PO 70152 | SL: Ordered/Received CYTD | Confirmar embarque de la semana; «no pasar la autorización» | innovacion (Jessica) | Piden 856: ? si tienen portal |
| **Zwisstex** | Release semanal | Correo + `RELEASE QUIMIBOND CWnn.xlsx` (Material, Description, semanas) | Semanal, martes a jueves; 10 en 3m | ~20 semanas | ? | ? | ? (probablemente m); M208908, M208893, M208899, M101418, M208902 | ? | Sin plazo explícito | innovacion; cc ventasindustrial, logistica | ? |
| **Copo / CTM** (Silao) | Release semanal o quincenal | Correo + PDF `Quimibond DD_MM_2026.pdf` | Irregular; 4 en 3m | 20 semanas, «Forecast» vs «Firme» | Periodo firme marcado | **Listo para recolección en Quimibond (EXW)**; penalidad si no está | Metros lineales; 362110 (PES 53 g 1600 mm), 362121 (PA 40 g 1630 mm); proveedor 410 | ? | **24 h** (si no, aceptado) | innovacion, logistica | ? |
| **TQ-1** | Forecast + PO + plan semanal de recolección | Excel `TQ1MX-PCP-05-27 Quimibond soportes.xlsx` (no se pudo leer: muy grande); POs por correo | Irregular; 11 en 3m (último forecast 1-sep) | ? | ? | Recolección | Ambigua («10,000k»); SCR56 / CKS | ? | Confirmar recolecciones | innovacion, ventasindustrial, gilberto, logistica | ? — hoy es el único pronóstico en Odoo (id 7) |
| **Contitech** (SLP) | Forecast mensual + PO SAP | Tabla en el cuerpo del correo + PDF de PO | ~mensual, irregular; nada desde 22-jul | 6 meses, cubetas mensuales | Por PO | PO con fecha de entrega, FCA Lerma, USD | **M**; XR27028/1640, XR27009/1620, XR27015/1640, XR27037/1680 | No | «Confirmar de recibido» | innovacion, gilberto | ? |
| **Seiren Viscotec** | Forecast trimestral + PO aparte | Imagen pegada en el correo (sin texto) | ~mensual | 3 meses | PO | ? | ? | No | Solo visibilidad | innovacion, gilberto | ? |
| **World Emblem** (Especiales) | Solo PO mensual | PDF de PO | Mensual | — | PO | Recolección | m (con equivalencia en yd); PELLON 6315 | No | Confirmar recibido | innovacion | — |
| **Blancos Milenium** | Solo OC + 15,000 m/semana pactados | PDF de OC | — | — | — | — | m | No | — | gilberto, dirección | — |
| **Pieles Sintéticas** | Solo pedidos | Pedido en el cuerpo del correo | Esporádico | — | — | — | — | No | Confirmación de pedido | innovacion | — |
| **IUSA** | Solo PO | Portal Coupa | Esporádico | — | — | — | — | No | — | Coupa | Coupa |
| **Bader** | Sin evidencia de release ni PO por correo | — | — | — | — | — | — | — | — | — | ? |

Notas:
- **Quién atiende hoy:** innovacion@ (Jessica) recibe y contesta casi todo;
  logistica@ confirma fechas y citas; ventasindustrial@ va en copia pero su
  último correo es del 8-jul; berenice.vazquez recibe Lear y FXI pero no
  encontré respuestas suyas. **Pregunta para la sesión:** ¿quién es el
  responsable por cliente en el perfil?
- **Tres bases de fecha distintas** (Lear embarque, FXI/Woodbridge/Shawmut
  entrega en planta del cliente, Copo recolección en Quimibond): el perfil
  necesita `date_basis` y días de tránsito por planta.
- **Tres unidades** (MT, LY, M) sobre productos que Quimibond vende en metros o
  kilos: el catálogo de partes necesita el factor por parte.
- Lear también manda «TOTAL CUM / SHIPPING PLAN WKnn» para cotejar CUM, y
  Penske avisa las recolecciones de Lear.

---

## 2. Reporte de auditoría

Leí el módulo completo (`models/`, `views/sgi_sales_budget_views.xml`, el KPI
en `quimibond_sgi/models/sgi_indicator.py` y el mixin de candados
`quimibond_sgi/models/sgi_base.py`). Las 121 pruebas son las de
`tests/test_sales_budget.py` (119) y `tests/test_sales_budget_sgi.py` (2).

### 2.1 Sección 4.2 del prompt («críticos»)

| # | Hallazgo del prompt | Veredicto | Evidencia |
|---|---|---|---|
| C1 | Precio basura en el importe | **Confirmado**, y hay un segundo camino | `models/sgi_sales_budget_line.py:289-313`: con `fell_to_sale` pone `has_price=False` pero **devuelve `price`** (el precio de venta convertido); `_compute_price` (l. 210-217) lo guarda en `price_unit_budget` y `_compute_amount_budget` (l. 193-196) lo multiplica. Segundo camino: cliente **sin lista** (l. 265-273) devuelve `product.list_price` completo. Con `implausible` (regla < $5) también devuelve el precio. Los tres casos deben valer 0. |
| C2 | Precio no capturable; el importador ignora `$` | **Confirmado** | Campo compute sin `inverse` en l. 54-63; importador: `models/sgi_sales_budget_import.py:322-338` salta las columnas `amount` y avisa «se ignoraron». |
| C3 | TC global y flotante | **Confirmado** | `_sgi_planning_factor` (l. 315-333) lee `quimibond_sgi.budget_planning_rate`; si es 0 cae a `_convert(..., day)` con `day = hoy` (l. 264, 347). No hay TC por presupuesto ni por semestre ni congelado al aprobar: el precio sí queda congelado (el refresco salta aprobados, `models/sgi_sales_budget.py:939-948`), pero con el TC del día en que se calculó por última vez. |
| C4 | La venta real depende del equipo de la factura | **Confirmado**, afecta a 7 lugares | `_sgi_compute_real_invoiced` filtra `move_id.team_id` (l. 469); lo mismo `_compute_ordered` (l. 751, `order_id.team_id`), `action_view_month_invoices` (l. 719), `_sgi_team_year_real` y la conciliación (`sgi_sales_budget.py:343`, `397`), la plantilla (`sgi_sales_budget.py:878`), la precarga desde el histórico (`sgi_sales_budget.py:1060`) y el cierre de mes (`models/sgi_cron.py:38-49`). La conciliación ya **mide** lo que se pierde (`no_team`, `sgi_sales_budget.py:425-433`) pero solo como informativo. |
| C5 | La matriz solo sirve sin cliente | **Confirmado** | `grid_update_cell` fuerza `partner_id = False` (l. 947, 953); vista grid con fila = producto (`views/sgi_sales_budget_views.xml:129-141`). |
| C6 | m ↔ kg no se convierten | **Confirmado** | `_convert_qty` (`sgi_sales_budget.py:29-37`) devuelve `None` entre categorías; el real las cuenta en importe pero no en cantidad (`unconverted_count`). El dato para convertir **ya existe**: `qb.producto.ficha` (addon `qb_capacidad_costeo`, `models/ficha.py:35-68`) tiene `gramaje_g_m2`, `ancho_m` y `rendimiento_m_kg` por producto. Ver sección 3 para su cobertura. |
| C7 | `product_id` obligatorio | **Confirmado** | l. 32-33 `required=True`; además la unicidad (l. 175-177) y el anti-doble-conteo (l. 378-404) se basan en el producto. |

### 2.2 Sección 4.3 del prompt («graves»)

| # | Hallazgo del prompt | Veredicto | Evidencia |
|---|---|---|---|
| G1 | «Regresar a borrador» des-aprueba | **Corregido en parte.** Un gerente de ventas **no** puede: el mixin del SGI bloquea cualquier escritura (incluido `state`) sobre un registro en `aprobado` si no es MAST (`quimibond_sgi/models/sgi_base.py:166-180`), y el modelo no define `_sgi_decision_states`. Lo que sí está mal: (a) el botón es **visible** en `aprobado` para `group_sale_manager` (`views/sgi_sales_budget_views.xml:283-285`) y le da un error; (b) **MAST sí puede** des-aprobar, y el prompt dice que de aprobado solo se sale con nueva revisión; (c) `action_set_borrador` (`sgi_sales_budget.py:632-634`) no valida nada por sí mismo. |
| G2 | `action_revise` solo MAST y obsoleta sin comparar | **Confirmado** | `sgi_sales_budget.py:650-651` exige `group_sgi_manager`; botón con el mismo grupo (vista l. 280-282). La revisión anterior pasa a `obsoleto` (l. 657) y ningún reporte compara Rev.N contra Rev.N-1. Además el KPI VE-02 solo lee `aprobado`, así que al revisar en junio **el año se queda sin presupuesto** hasta aprobar la nueva revisión. |
| G3 | Precio de la lista de hoy | **Confirmado** | `day = fields.Date.context_today(self)` (l. 264) se pasa a `_get_product_price_rule(..., date=day)` (l. 280-281) en vez de `line.date`. Una regla con vigencia desde julio no se aplica a la línea de julio. |
| G4 | Plantilla e importador con layout propio | **Confirmado** | Mensual: pares «<mes> m / <mes> $» (`sgi_sales_budget_import.py`, `_parse_header`); forecast: fila «SEMANA» 1-52. Ninguno lee el F-P-A28-14 real (mercados, proyectos, precio) ni el release. |

### 2.3 Sección 4.4 del prompt («menores»)

| # | Hallazgo del prompt | Veredicto | Evidencia |
|---|---|---|---|
| M1 | `_compute_unbudgeted` busca facturas en cada lectura | **Confirmado** | Compute no almacenado (`sgi_sales_budget.py:350-354`) que llama `_sgi_team_year_real` (l. 334-348, `search` de todo el año del equipo). Se dispara en cada ficha y en cada lista que muestre el campo. |
| M2 | KPI VE-02 ≠ reporte | **Confirmado, y son tres cifras distintas** | KPI: facturado **de toda la compañía** (todos los equipos, con fletes y servicios) ÷ presupuesto aprobado (`quimibond_sgi/models/sgi_indicator.py:457-469`, `477-489`). Reporte (`fulfillment_pct`, `sgi_sales_budget.py:173-183`): real **solo de los productos presupuestados** ÷ presupuesto. Cierre de mes (`sgi_cron.py:105-160`): facturado **del equipo**. Además el denominador del KPI **no filtra compañía** (`sgi_indicator.py:450-454`): con un presupuesto aprobado de otra empresa del grupo se sumaría. |
| M3 | Aprobación doble (código vs Studio 63) | **Por confirmar en producción** | El código deja aprobar a `group_sgi_director` **o** `group_sgi_manager` (`sgi_sales_budget.py:590-591`). La regla Studio 63 no vive en el repo. En producción la regla 63 (creada el 29-sep) exige la aprobación de **Jacobo Mizrahi** (usuario 9, Director Estratégico) en `action_approve` solo para `kind='presupuesto'`. El grupo `group_sgi_director` es «Dirección de Operaciones» y su único miembro es **Jorge Ortiz**, no Jacobo: el código deja apretar el botón a Jorge y a los Jefes MAST (Blanca Ballesteros, Sergio Gonzales), y la regla de Studio detiene hasta que Jacobo apruebe. Funciona, pero con dos candados que dicen cosas distintas. **Propuesta:** el código valida un grupo nuevo «Aprueba presupuesto de ventas» (Jacobo) sin la salida de MAST y la regla de Studio se retira; o al revés. Decide José. |
| M4 | El pronóstico exige cliente | **Confirmado** | Cabecera `sgi_sales_budget.py:513-519` y línea `sgi_sales_budget_line.py:369-376`. |
| M5 | F-P-A28-13 no está en Documentos | No verificable desde el código (es dato de Documentos). Lo reviso en el E1. |
| M6 (nuevo) | 30 líneas de Confección en unidad «Actividad» | Producción: no es unidad de venta; se corrige en la carga 2026. |

### 2.4 Hallazgos nuevos

**Críticos** (rompen el piloto de releases si no se corrigen antes):

| # | Hallazgo | Evidencia | Consecuencia |
|---|---|---|---|
| N1 | **El envío al MPS pisa la demanda de otros clientes.** Cada pronóstico escribe `forecast_qty = qty` en la celda producto × semana (`sgi_sales_budget.py:1165-1167`). Si Lear y FXI (o TQ-1) comparten producto y semana, el último pronóstico enviado borra al anterior. El presupuesto mensual también pisa. | `_sgi_push_forecast_cells` | Con dos clientes con release, el MPS subestima la demanda. Hay que mandar la **suma de todos los pronósticos vigentes** por producto y periodo, no la de un documento. |
| N2 | **Anti-doble-conteo demasiado grueso.** El presupuesto omite del MPS **todo** el producto si **cualquier** cliente lo tiene en un pronóstico revisado, sin importar cliente ni mes (`sgi_sales_budget.py:1129-1140`, `1224`). | `_sgi_forecast_covered_products` | Si TQ-1 pronostica WC090, la demanda presupuestada de WC090 para los demás clientes desaparece del MPS. Debe ser por producto + cliente + periodo. |
| N3 | **El pronóstico no cruza de año.** Las líneas deben caer en el año del documento (`sgi_sales_budget_line.py:351-367`) y hay un solo pronóstico activo por cliente-año (`sgi_sales_budget.py:491-511`). | constraints | El release de Lear (sep-26 → sep-27) no cabe en un documento; hay que repartirlo en los de 2026 y 2027. |

**Graves:**

| # | Hallazgo | Evidencia | Consecuencia |
|---|---|---|---|
| N4 | La semana comprometida sale de la fecha **del pedido**, no de la línea: `commitment_date or expected_date or date_order` (`sgi_sales_budget_line.py:552-560`). | `_sgi_effective_monday` | Una PO abierta con entregas semanales (Lear 1030782) cuenta todo en una sola semana; la cobertura y la demanda neta quedan mal. Define cómo se generan los pedidos en E2 (un pedido por semana de embarque). |
| N5 | La cotización por faltante no lleva precio de lista ni PO del cliente (`sgi_sales_budget_line.py:665-709`) y agrupa por `origin` = folio del pronóstico. | `action_create_draft_quotation` | Se reemplaza en E2 por la propuesta de pedido desde la zona firme. |
| N6 | El importador en modo «reemplazar» borra **todas** las líneas del producto, también las de clientes que no vienen en el archivo (`sgi_sales_budget_import.py:319-320` y `374-375`). | importador | Reimportar una hoja de un cliente borra lo capturado para otros clientes del mismo producto. |
| N7 | Almacén del MPS = el primero de la compañía (`sgi_sales_budget.py:1142-1148`, `limit=1` sin orden). | `_sgi_mps_warehouse` | Con más de un almacén la demanda puede caer en el equivocado. En producción la compañía 1 tiene 3 almacenes activos (Toluca, Centro, Toluca Varios) y los 34 programas del MPS están en Toluca. |
| N8 | Periodo del MPS: el pronóstico manda lunes y el presupuesto día 1 del mes; si el MPS de la compañía está en meses, las celdas semanales caen en fechas que el MPS no muestra. En producción el MPS es **semanal** (`manufacturing_period = week`): el pronóstico cuadra, pero el presupuesto mensual manda todo el mes a la semana del día 1. Hoy no ha pasado porque las 122 celdas de 2026 están en 0: nadie ha enviado demanda. | `_send_forecast_to_mps` | Hay que agrupar al periodo configurado del MPS. |

**Menores:**

| # | Hallazgo | Evidencia |
|---|---|---|
| N9 | La semana 1 del forecast es «el primer lunes del año» (`sgi_sales_budget_import.py:202-207`), no la semana ISO; en 2026 la semana 1 del importador empieza el 5-ene y la ISO el 29-dic-2025. El release trae fechas, así que el lector nuevo no usa números de semana. | `_week_monday` |
| N10 | `_sgi_forecast_sols` (usado por el botón «Ver pedidos de la semana») trae **todo** el histórico de pedidos del producto y cliente y filtra en Python (`sgi_sales_budget_line.py:562-576`). | rendimiento |
| N11 | `action_set_obsoleto` existe sin botón ni validación de grupo (`sgi_sales_budget.py:636-638`); el mixin lo bloquea desde `aprobado`, pero desde borrador cualquiera con escritura lo puede llamar por RPC. | seguridad menor |

---

## 3. Faltantes de datos maestros

Medido en producción el 30-sep-2026 por MCP, solo lectura, compañía 1.

### 3.1 Estado real de los presupuestos (confirma y corrige la sección 4.1 del prompt)

| id | Documento | Total MXN | Líneas | Sin precio de lista | Meses | Observación |
|---|---|---|---|---|---|---|
| 137 | Presupuesto Industrial 2026 | 116,354,445.79 | 325 (131 en kg, 194 en m) | 30 (valen $123,243) | ene–oct | Confirmado. 239 desviaciones de precio |
| 4 | Presupuesto Confección 2026 | 22,465,924.76 | 915 (30 en unidad «Actividad») | **874, que suman $9,796,715** | ene–dic | **Corrección:** las líneas «sin precio» no valen 0; están valuadas al precio de venta «(NO usar)». Es el hallazgo C1 en producción. 517 líneas sin cliente sí valen 0 (no hay lista presupuestal) |
| 5 | Presupuesto Especiales 2026 | 0 | 12 | 12 | meses nones | Confirmado |
| 7 | Pronóstico TQ-1 2026 | 11,803,920 | 39 | 0 | 5-ene a 14-sep | Muestra $91.7 M de «facturado no presupuestado»: sospechoso, se revisa en E3 |

- **TQ-1 está presupuestado tres veces:** Industrial $14.97 M (28 líneas),
  Confección $11.34 M (25 líneas, aunque TQ-1 factura como Industrial) y su
  pronóstico $11.80 M. Sin TQ-1, a Confección le quedan $11.1 M, de los que
  solo ~$1.3 M salen de una lista real.
- **Tres tipos de cambio distintos** porque cada documento tomó el del día en
  que se recalculó: Confección 17.12-17.14, Industrial 17.63-17.65, pronóstico
  17.83-17.85 (hallazgo C3 en producción).
- Parámetros: `budget_planning_rate = 0`, `budget_pricelist_id = 0`,
  `monthly_sales_budget = 0`, umbrales de cumplimiento y aviso en 80 %.
  TC del día: 18.071 (1-oct), 17.8413 (30-sep).
- Ninguna línea tiene `customer_code`.

### 3.2 Faltantes por dato maestro

| Dato | Qué hay | Faltante | Qué propongo |
|---|---|---|---|
| **Gramaje y ancho** | Nada en el producto: `x_ancho` (Studio) con 0 productos llenos; `weight` en 42 de 284 vendidos. El dato vive en **`qb.producto.ficha`** (`qb_capacidad_costeo`): 271 de los 284 productos vendidos desde 2025 tienen ficha y **271 tienen rendimiento m/kg**, 203 tienen ancho y **solo 88 tienen gramaje** (el parser deja 0 cuando la referencia trae 2 dígitos: W38, W55; 263 fichas W* en 0). 189 de las 271 fichas no tienen estado. `quimibond_ficha_tecnica_tela` **no está instalado**. | 196 productos vendidos sin gramaje; 13 sin ficha | Para m ↔ kg usar **`rendimiento_m_kg`** de la ficha, que ya cubre 271/284; gramaje × ancho solo como respaldo. Corregir el parser para referencias de 2 dígitos (W38 = 38 g/m²) en `qb_capacidad_costeo`. WC090Q11JNT165: ficha 90 g/m², 1.65 m, 6.54 m/kg; confirma que 90 es el gramaje (el Excel lo calculó con 140). |
| **Parte del cliente → producto** | No existe `product.customerinfo` en esta base. `L002790184NCPAA` solo aparece en una **nota** de PV11796 (2024). Hoy la PO 1030782 está en **89 pedidos** (uno por semana) con `IWJ045Q22JNT160` en **kg** a 9.725 USD. | Todo el catálogo (Lear, FXI, Woodbridge, Shawmut, Zwisstex, Copo, Contitech: ~25 partes vistas en el correo) | Modelo `qb.customer.part` y una hoja de carga que llenan Jessica y Berenice en la sesión del cuestionario. Primera fila: Lear L002790184NCPAA → IWJ045Q22JNT160 (por confirmar), MT → kg con la ficha. |
| **Mercado del cliente** | `res.partner` no tiene equipo. `industry_id`: 256 de 266 clientes con venta 2026 sin industria ($55.7 M); solo 10 «Automotriz» ($83.0 M). Etiquetas INDUSTRIAL / CONFECCION en `category_id`. | Mercado presupuestal de 266 clientes | Campo `budget_market_id` (crm.team) propuesto por el equipo de sus facturas 2025-2026 y confirmado por Ventas. World Emblem: 2026 facturado como **Confección** ($3.41 M), 2025 sin equipo ($3.23 M): hay que decidir si es Especiales. |
| **Listas de precios** | 104 listas activas (88 MXN, 16 USD), 245 reglas, todas de precio fijo. 58 clientes con tarifa propia (~$105 M); 71 con Lista pública; **135 sin lista específica ($29.0 M)**. Ninguna lista se llama «NO usar»: el texto es del presupuesto. | Lista presupuestal (parámetro en 0) y tarifa de 135 clientes | Con precio capturable por línea (E3) la lista deja de ser bloqueante; configurar la lista presupuestal para Confección sin cliente. |
| **Facturas sin equipo** | **1** en 2026: INV/2026/03/0173 a LEASING LEPEZO, 25-mar, **$11,348,207.32** sin IVA. Es la venta de la **RAMA ICOMATEX IC10** (activo), no tela. | 1 | Confirmado el monto. No es venta de producto: debe quedar fuera del presupuesto de ventas, no asignársele equipo. La validación al publicar tiene que exceptuar ventas de activo (propuesta: aviso configurable, no bloqueo). |
| **Pedidos del piloto** | Lear 38 pedidos 2026 con 1 PO, en kg USD; FXI 50 pedidos en kg; Saltillo 78 en m; Shawmut 71 en kg; TQ-1 54 en m; Contitech 74 en m. Casi todos con `commitment_date` (un pedido por entrega). Vendedora: Jessica Francisco. | — | Confirma el diseño de E2 de un pedido por PO y semana. **Los clientes piden en MT/LY y nosotros facturamos en kg**: la conversión por parte es obligatoria desde E1. |
| **Alias y MPS** | No existe el alias `releases@`. MPS: 34 programas, todos en almacén Toluca, periodo **semanal**; 122 celdas de pronóstico 2026, **todas en 0** (nadie ha enviado demanda). | Alias | Se crea en E1. |

---

## 4. Diseño técnico

### 4.1 Principios

- **Se construye sobre el módulo actual**, en el mismo addon
  `quimibond_ventas_presupuesto` (el pronóstico, el MPS y el KPI ya viven ahí;
  un addon aparte duplicaría dependencias y permisos). Los modelos existentes
  conservan su nombre técnico; los nuevos usan el prefijo `qb.release.*`.
- **Los lectores son funciones puras** (`models/release_readers/lear_aiag.py`,
  `fxi_sum.py`): reciben texto o bytes y devuelven un diccionario. Así se
  prueban con pytest en el CI (el módulo depende del SGI, que el CI no instala)
  además de las pruebas de Odoo en el build de Odoo.sh.
- **Nada se aplica a medias.** Un release se lee completo o queda «por revisar»
  con el motivo; se aplica completo o no se aplica (una transacción).
- **Fixtures anonimizados** en `tests/fixtures/` con el mismo layout (partes,
  cantidades y PO inventados). Ningún release real ni el Excel de presupuesto
  entra al repo.

### 4.2 Modelos

**Nuevos**

| Modelo | Para qué | Campos principales |
|---|---|---|
| `qb.release.profile` (Perfil de release) | Cómo manda cada cliente/planta | `partner_id` (empresa), `ship_to_id` (dirección de entrega), `supplier_code` (6PIN0010), `sender_ids`/`sender_domains` (para reconocer el correo), `channel` (correo / portal / EDI), `reader` (`lear_aiag`, `fxi_sum`, `edi_830`, `ia`, `manual`), `date_basis` (embarque / entrega en planta) y `transit_days`, `firm_rule` (N semanas / hasta autorización fab), `firm_weeks`, `ack_required`, `ack_hours`, `ack_template_id`, `cum_managed`, `cum_reset_date`, `cum_tolerance`, `so_mode` (propuesta / automático), `sales_user_id`, `logistics_user_id`, `active` |
| `qb.customer.part` (Catálogo de partes del cliente) | Parte del cliente → producto | `partner_id`, `ship_to_id` (opcional), `customer_part` (L002790184NCPAA), `customer_description`, `product_id`, `customer_uom` (MT, YD, KG, M2…), `conversion` (`fija` con `factor` / `ficha`: `rendimiento_m_kg` de `qb.producto.ficha`, con gramaje × ancho como respaldo), `factor`, `active`. Único por cliente + planta + parte. **Migración**: siembra filas desde los `customer_code` que ya tienen las líneas de pronóstico. |
| `qb.release` (Release, con chatter) | Un documento recibido = una versión | `profile_id`, `partner_id`, `ship_to_id`, `release_ref` (000174), `release_date`, `version` (consecutivo por perfil), `previous_id`, `message_id`/`attachment_ids`, `reader_used`, `state` (`recibido` → `leido` → `revisado` → `aplicado`; `por_revisar`, `reemplazado`, `descartado`), `error_reason`, `ack_sent_at`, totales de cambio |
| `qb.release.part` | Encabezado por parte (en Lear, PO, CUM y autorizaciones son **por parte**, no por release) | `release_id`, `customer_part`, `part_id` (catálogo), `product_id`, `customer_po` (1030782), `cum_received`, `in_transit_qty`, `last_receipt_date/qty`, `last_packing_slip`, `fab_auth_qty/date`, `raw_auth_qty/date`, `cum_ours`, `cum_diff`, `cum_state` (cuadra / no cuadra / sin dato) |
| `qb.release.line` | Detalle semanal | `release_part_id`, `date_customer` (la fecha del release), `date_ship` (embarque = entrega − tránsito), `week` (lunes), `qty_customer` (unidad del cliente), `qty` (unidad del producto), `line_type` (firme / planeado según el release), `zone` (calculada: `firme` / `materia_prima` / `planeado`), `cum_req`, `net_req` |
| `qb.release.change` | Diferencias contra el release vigente (se guardan: son la base de la métrica de estabilidad) | `release_id`, `product_id`, `week`, `qty_prev`, `qty_new`, `delta`, `kind` (aumento / reducción / nueva / desaparece / movimiento de fecha), `in_firm`, `in_lead_time` |
| `sgi.sales.budget.assumption` (Supuesto) | Lo que Ventas ajusta sobre el presupuesto armado | `budget_id`, `kind` (aumento de precio % / precio fijo / baja de cliente / TC / volumen %), `partner_id`, `product_id`, `team_id`, `date_from` (mes), `value`, `note` |

**Cambios a los existentes**

| Modelo | Cambio |
|---|---|
| `sgi.sales.budget` | `kind` agrega `estimado` (estimado de cierre mensual, uno por mercado-año, se regenera cada mes); `forecast_scope` (`cliente` / `producto`) para permitir el pronóstico de Confección sin cliente; `fx_rate_s1`, `fx_rate_s2` (TC por semestre, obligatorios para aprobar, congelados al aprobar); `origin_budget_id` (la revisión anterior, para comparar); estado `reemplazado` en vez de `obsoleto` para la revisión que queda como histórico comparable. |
| `sgi.sales.budget.line` | `product_id` opcional + `project_name`, `project_gramaje`, `project_ancho` (proyectos sin artículo); `price_mode` (`lista` / `capturado`), `price_input`, `price_currency_id`, `price_uom` (m / kg) — el importe usa el capturado si lo hay, si no la lista **a la fecha de la línea**, y **0 si no hay regla** (nunca el precio de venta); `release_id`/`release_line_ids` (de qué release salió la cantidad); `source` (`release` / `historico` / `proyecto` / `manual` / `excel`); `qty_kg` y `qty_m` calculadas con el rendimiento m/kg de la ficha; `partner_shipping_id` opcional. |
| `res.partner` | `budget_market_id` (mercado presupuestal: crm.team) para medir el real sin depender del equipo de la factura. |
| `account.move` | Validación al publicar una factura de cliente sin equipo de ventas (sección 5 del prompt). Se propone como **aviso bloqueante configurable** (parámetro), porque bloquear de golpe detendría la facturación si hay flujos que hoy no ponen equipo (SAT, anticipos, notas). |

### 4.3 Cómo se integra con lo actual

```
correo → mail.alias → qb.release (recibido)
          │ lector según perfil (determinístico / IA)
          ▼
       leido ──► acuse al remitente (mail.template del perfil, queda en el chatter)
          │     ► conciliación CUM por parte (alerta el mismo día si no cuadra)
          │     ► diferencias contra el vigente (qb.release.change)
          ▼
       revisado (Ventas; se salta por perfil cuando haya confianza)
          ▼
       aplicado ─► pronóstico del cliente (sgi.sales.budget kind=pronostico, uno por año que toque)
                 ├► zona firme: propuesta de líneas de pedido (E2)
                 ├► zona planeada: demanda neta al MPS (suma de todos los clientes)
                 └► el release anterior pasa a «reemplazado»
```

- **Pronóstico vigente.** Aplicar un release reescribe las líneas del
  pronóstico de ese cliente **para las semanas que cubre el release** (y deja
  en 0 las partes que desaparecen), con `source='release'` y el vínculo al
  release. Las semanas pasadas no se tocan. Si el release cruza de año, escribe
  en el pronóstico de cada año (se crea el del año siguiente si no existe).
  Las versiones anteriores son los `qb.release` en `reemplazado`: ahí se
  comparan y se mide la precisión.
- **MPS.** Al aplicar, se recalcula la celda de cada producto × periodo del
  MPS como **la suma de la demanda neta de todos los pronósticos vigentes** más
  la demanda del presupuesto de los productos-cliente-mes que no tienen
  pronóstico (corrige N1 y N2). Se agrupa al periodo configurado del MPS.
  Sigue sin crear órdenes de producción.
- **Pedidos (E2).** Por perfil, las semanas en zona firme generan **un pedido
  por PO del cliente y semana de embarque** (`client_order_ref` = PO,
  `origin` = Release ID, `commitment_date` = embarque, precio de la lista del
  cliente). Un pedido por semana resuelve N4 sin tocar el modelo de Odoo
  (`sale.order.line` no tiene fecha por línea). Nunca se baja una cantidad por
  debajo de lo entregado; las reducciones en zona firme quedan como propuesta
  con motivo y requieren a Ventas. Modo `propuesta` al inicio.
- **Presupuesto (E3).** Un asistente «Armar presupuesto <año>» crea el
  borrador por mercado: meses de los releases vigentes (clientes con perfil),
  histórico de 12 meses con estacionalidad simple para los demás (marcado
  «estimado por Quimibond»), líneas de proyecto y luego aplica los supuestos.
  Jessica solo edita supuestos, proyectos y bajas; Dirección aprueba y queda
  inmutable. El **estimado de cierre** se regenera cada mes: meses cerrados =
  real, meses futuros = releases vigentes + histórico + supuestos.
- **Real.** Con cliente: cliente + producto en **cualquier** equipo. Sin
  cliente: producto + mercado presupuestal del cliente de la factura, excluyendo
  lo que ya midió una línea con cliente (sin doble conteo).
- **Una sola cifra de cumplimiento.** Un método del módulo de ventas
  (`_qb_fulfillment(date_from, date_to, company)`) que usan el reporte, el
  cierre de mes y el KPI VE-02. Denominador filtrado por compañía.
- **Explicación de la diferencia.** Con P = precio y Q = cantidad por
  producto-cliente-mes, a TC presupuestal y TC real:
  volumen = (Q<sub>real total</sub> − Q<sub>ppto total</sub>) × P̄<sub>ppto</sub>;
  mezcla = Σ (Q<sub>real</sub> − Q<sub>real total</sub> × participación<sub>ppto</sub>) × (P<sub>ppto</sub> − P̄<sub>ppto</sub>);
  precio = Σ Q<sub>real</sub> × (P<sub>real, divisa</sub> − P<sub>ppto, divisa</sub>) × TC<sub>ppto</sub>;
  TC = Σ Q<sub>real</sub> × P<sub>real, divisa</sub> × (TC<sub>real</sub> − TC<sub>ppto</sub>).
  La suma de los cuatro efectos = real − presupuesto (se prueba al centavo).
  Se calcula en metros y en kilos.

### 4.4 El buzón

- Alias de Odoo `releases@` sobre `qb.release` (`mail.thread` +
  `message_new`). Para que `releases@quimibond.com` llegue a Odoo hay dos
  caminos: (a) un grupo de Google Workspace `releases@quimibond.com` que
  reenvía al alias del dominio de Odoo.sh; (b) apuntar un subdominio
  (`releases@odoo.quimibond.com`) al catchall de Odoo. **Recomiendo (a)**: no
  toca el MX del dominio y el grupo conserva copia en Gmail (y en la memoria).
- Mientras los clientes no cambien su lista de distribución, Berenice, Jessica y
  ventasindustrial ponen un filtro de Gmail que **reenvía** los correos con el
  asunto del perfil («Lear-MTO 5500 Release», «RELEASE QUIMIBOND»). El
  remitente original se toma del encabezado del reenvío y del .eml adjunto.
- Identificación: dominio y remitente del perfil, número de proveedor y asunto.
  Sin perfil que coincida → «por revisar» con el motivo, nunca se adivina.
- Lear manda el release como **.eml adjunto**: el lector abre el .eml y lee su
  cuerpo de texto.
- Duplicados: mismo perfil + `release_ref` + `release_date` → se liga al
  existente y no crea versión nueva.

### 4.5 Qué lector usa cada cliente

| Cliente | Lector | Entrega | Por qué |
|---|---|---|---|
| Lear | `lear_aiag`: texto de ancho fijo del `.eml` adjunto. Encabezado por parte (Release ID, fecha, PO, Item, UM, In Transit, Cum Received, Packing Slip), filas «fecha · Q · Req · Cum Req · Net Req» debajo de «Weekly»/«Prior», pies «Fab/Raw Authorization Cum Qty … Thru». Tolera el salto de página `\f` que repite el encabezado y varias partes por release. | E1 | Determinístico; el layout es AIAG estándar. **Después (E4):** leer el 830 directo de iExchangeWeb si Lear/OpenText dan salida de archivo o AS2; mismo modelo, otro lector. |
| FXI | `fxi_sum`: Excel de una hoja, bloques de 3 filas por parte (encabezado con Raw Material No., Vendor Authorization, Raw Accum Rcvd, Release #, Release Date AAAAMMDD, Agreement #, UOM; luego `Date`, `Gross need`, `V Cum` con 52 columnas). Números con y sin coma de miles. | E1 | Determinístico (reporte SAP). |
| Woodbridge (Saltillo y León) | `woodbridge_pdf`: el PDF «SUPPLIER RELEASE PLANNING SCHEDULE» es de sistema (texto extraíble) con Cum YTD, Fab/Material Cum y semanas. | E4 (o E1 si sobra tiempo) | Es determinístico, **no** requiere IA como suponía el prompt. |
| Shawmut | `shawmut_tn` (.xls) y `shawmut_sl` (.csv) | E4 | Dos layouts fijos. |
| Zwisstex | `excel_semanas`: lector genérico configurable (fila de encabezado, columna de parte, columnas de fecha). | E4 | Excel simple; el genérico sirve a otros clientes chicos. |
| Copo / CTM | `copo_pdf` o IA | E4 | PDF de 20 semanas con zona firme marcada; hay que ver si es texto o imagen. |
| TQ-1 | `excel_semanas` configurado para su hoja, o captura en la matriz | E4 | Excel grande y de uso interno del cliente; primero hay que verlo. |
| Contitech | IA sobre la tabla del cuerpo, con revisión | E4 | Mensual y en el cuerpo del correo. |
| Seiren | Captura manual (es una imagen) o IA con visión, con revisión | E4 | Trimestral y sin texto. |
| World Emblem, Blancos, Pieles, IUSA, Bader | Sin release: pronóstico «estimado por Quimibond» desde el histórico | E3 | — |

### 4.6 Dónde entra la IA y con qué revisión humana

- **Solo** en perfiles con `reader='ia'` (tabla en el cuerpo del correo o
  imagen: Contitech, Seiren, quizá Copo, en E4). Nunca en Lear, FXI, Woodbridge
  ni Shawmut, que tienen layout fijo.
- Odoo llama a la API de Claude con salida JSON cerrada (partes, fechas,
  cantidades, unidad, PO) y guarda el JSON y el texto de origen en el release.
  La llave va en un parámetro del sistema, no en el código.
- El release leído por IA queda **siempre** en `leido` con la marca «extraído
  por IA»; no se aplica sin que Ventas lo pase a `revisado`, y el perfil IA no
  admite modo automático. Toda parte que la IA no pueda ligar al catálogo
  detiene esa línea.
- `quimibond_intelligence` hoy solo empuja contactos, usuarios y señales a
  Supabase y no tiene IA propia; no lo uso para esto. La memoria de correo sí
  sirve para una señal «release sin recibir» (el cliente mandó y Odoo no lo
  tiene) en E4.

### 4.7 Métricas (E4)

- **Estabilidad:** Σ|delta| en zona firme ÷ cantidad firme, por cliente y
  semana, desde `qb.release.change`.
- **Precisión:** para cada semana embarcada, lo planeado a N semanas (1, 4, 8,
  13) en el release de hace N semanas contra lo embarcado; MAPE y % de acierto
  por cliente y mes para el S&OP.
- **A tiempo y completo contra el release:** entregas validadas contra la
  cantidad y fecha del release vigente al embarcar.
- **Cobertura:** la actual, con el pronóstico alimentado por el release.

---

## 5. Plan por entregas

Hoy es miércoles 30-sep. La meta dura es que **Ventas presente el
presupuesto 2027 antes del 31-oct**, así que Jessica necesita el armado listo
hacia el **23-oct** para tener una semana de ajustes.

**No caben E1 y E3 completos antes del 31-oct** (~220 h entre los dos). Por
eso propongo partir E1: su núcleo (perfiles, catálogo, lectores de Lear y FXI,
aplicar al pronóstico y corregir el MPS) es lo que el presupuesto 2027 necesita
para salir «de los releases»; el buzón, el acuse, las diferencias y el CUM
pueden llegar en noviembre. E3 va antes que E2, como pide la sección 7 del
prompt.

| PR | Entrega | Contenido | Horas | Fechas |
|---|---|---|---|---|
| — | **E0** (este documento) | Sesión del cuestionario con Jessica y Berenice (2 h) y hoja del catálogo de partes; visto bueno de José | 12 | 30-sep → 2-oct |
| 1 | **E1a — Núcleo de releases** | Perfiles y catálogo de partes (con migración de `customer_code`); `qb.release*`; lectores **Lear** (texto AIAG) y **FXI** (Excel SUM) como funciones puras con pytest; carga manual del archivo; aplicar al pronóstico repartiendo por año (N3); MPS con la suma de todos los pronósticos y agrupado al periodo del MPS (N1, N2, N8) | 60 | 5-oct → 14-oct |
| 2 | **E3 — Presupuesto y estimado** | Correcciones C1–C7 y G1–G4: precio 0 sin regla, precio capturable, TC por semestre congelado, real por cliente en cualquier equipo, proyectos sin producto, m ↔ kg con la ficha, gobierno de aprobado. Asistente «Armar presupuesto 2027» (releases + histórico + proyectos), supuestos, estimado de cierre, volumen / precio / TC / mezcla, reportes en m y kg, una sola cifra de cumplimiento (KPI VE-02 = reporte) | 110 | 12-oct → 23-oct |
| 3 | **Carga única 2026** | Importador del F-P-A28-14 con precios; totales de control al peso (sección 6 del prompt); los 4 borradores actuales quedan como histórico | 12 | 21-oct → 27-oct (necesita el Excel) |
| 4 | **E1b — Buzón y control** | Alias `releases@` y reenvío desde Google Workspace, perfiles por remitente, acuse automático, diferencias contra el vigente con avisos a Planeación y Compras, conciliación CUM con alerta el mismo día, MPS al aplicar | 50 | 2-nov → 13-nov |
| 5 | **E2 — Pedidos desde la zona firme** | Propuesta de un pedido por PO y semana de embarque, reducciones controladas, modo propuesta / automático por cliente | 40 | 16-nov → 27-nov |
| 6+ | **E4 — Resto y métricas** | Lectores Woodbridge (PDF de sistema), Shawmut (xls/csv), Zwisstex y lector genérico de Excel; IA para Contitech y Seiren; EDI de Lear si OpenText da salida; pronóstico de Confección por producto; validación de factura sin equipo; estabilidad, precisión y entregas contra release | 100 | dic → ene |
| | **Total** | | **≈ 384 h** | |

Supuestos del calendario:
- Un programador (o su agente) dedicado.
- José revisa cada PR en 1-2 días.
- Cada PR sigue el flujo desarrollo → main → quimibond.
- Las pruebas de Odoo corren en el build de desarrollo de Odoo.sh con
  `--test-tags /quimibond_ventas_presupuesto`, porque el CI no instala el
  SGI. Los lectores y la aritmética de la varianza sí corren en pytest del CI.
- Cada PR sube la versión del manifest y agrega su migración.

---

## 6. Riesgos y preguntas

### 6.1 Riesgos

| # | Riesgo | Mitigación |
|---|---|---|
| R1 | **Calendario del 31-oct.** E1a + E3 suman ~170 h en 3.5 semanas y dependen de insumos externos: el visto bueno, el catálogo de partes y el Excel 2026. | Arrancar E1a con el visto bueno del E0; si el 14-oct E1a no está en `main`, el asistente 2027 arma el presupuesto solo con histórico + proyectos y el release entra después. |
| R2 | **Unidades.** Los clientes piden en MT, LY o M; Lear, FXI y Shawmut se facturan en **kg**. El CUM, la cobertura y el presupuesto en kg dependen del `rendimiento_m_kg` de la ficha, que sale de un parser y **nadie ha validado**. Además 196 de 284 productos vendidos no tienen gramaje. | Validar la ficha de los ~15 productos del piloto con Calidad antes de E1a; factor fijo por parte en el catálogo como alternativa. |
| R3 | **Catálogo de partes inexistente.** Hoy la liga Lear → `IWJ045Q22JNT160` solo la sabe la gente. Sin catálogo no se aplica ninguna línea. | Hoja de carga en la sesión del cuestionario; el lector nunca adivina. |
| R4 | **Pruebas solo en Odoo.sh.** El módulo depende del SGI (Enterprise) y el CI no lo instala; cada ronda de build cuesta tiempo. | Lectores y cálculos como funciones puras con pytest en CI; pruebas de Odoo en el build de la rama antes de cada PR. |
| R5 | **Migración sobre datos de producción.** `product_id` opcional, estados nuevos y campos de precio sobre las 1,291 líneas actuales. | Migraciones que solo agregan columnas; los 4 borradores quedan intactos como histórico; se verifica con las consultas del runbook. |
| R6 | **Correo.** El alias necesita un grupo o reenvío en Google Workspace (TI), y Lear podría dejar de mandar el .eml y quedarse solo con EDI. | Carga manual del archivo desde E1a; EDI de Lear en E4. |
| R7 | **Procedimiento.** El estimado de cierre, el TC por semestre y el cambio de quién revisa modifican el P-A28 y sus formatos (documentos controlados del SGI). | Actualizar el P-A28 en paralelo con E3. |
| R8 | **Confidencialidad.** Releases y Excel traen volúmenes y precios. | Fixtures anonimizados y nada real en el repo (regla del prompt). |

### 6.2 Preguntas para José

1. **Orden:** ¿apruebas partir E1 (E1a antes del 31-oct, E1b en noviembre) y hacer E3 antes que E2?
2. **Aprobación del presupuesto:** hoy hay dos candados, el código (grupo Dirección de Operaciones = Jorge Ortiz, o MAST) y la regla Studio 63 (Jacobo). ¿Quién aprueba? Propongo un solo candado en código para Jacobo y retirar la regla 63.
3. **Revisión del aprobado:** si los ajustes van al estimado de cierre, ¿se conserva el botón «Nueva revisión»? Propongo: solo Dirección y como excepción; el KPI VE-02 mide siempre contra el **aprobado original**.
4. **Factura sin equipo:** la única de 2026 es la venta de la rama ICOMATEX ($11.35 M) a Leasing Lepezo, no tela. ¿La validación bloquea o avisa, y exceptúa ventas de activo?
5. **World Emblem:** ¿es Especiales (el prompt) o Confección (sus facturas 2026)?
6. **TQ-1** está en Industrial, en Confección y en su pronóstico. En la carga 2026 manda el Excel; ¿en 2027 va solo en Industrial?
7. **Acuse:** ¿automático al recibir, diciendo solo «recibido» y sin comprometer cantidades? ¿Firma Ventas o una cuenta genérica?
8. **Zona firme para pedidos (E2):** Woodbridge manda 26 semanas «firmes». ¿Se crean pedidos para todo el horizonte firme o solo N semanas por cliente?
9. **IA (E4):** ¿de acuerdo con que Odoo llame a la API de Claude con la llave en un parámetro del sistema?
10. **Excel F-P-A28-14:** mándalo fuera del repo (Drive) para el PR 3.

### 6.3 Preguntas para Ventas (Jessica y Berenice)

1. La hoja del catálogo: parte del cliente → producto → unidad → factor, para Lear, FXI, Woodbridge, Shawmut, Zwisstex, Copo, Contitech y TQ-1.
2. ¿Quién es el responsable de cada cliente? Berenice recibe Lear y FXI pero no contesta en el correo; ventasindustrial@ no escribe desde julio.
3. Días de tránsito por planta (Juárez, Saltillo, León, Clinton, Silao) y si FXI, Woodbridge y Shawmut aceptan fecha de embarque en vez de entrega.
4. Fecha de reinicio del CUM por cliente (¿1-ene? ¿año modelo?).
5. Lear: ¿tenemos usuario en iExchangeWeb y se puede exportar el 830?
6. TQ-1 y Seiren: ¿qué traen su Excel y su imagen, y cada cuándo?
7. Plazos reales de acuse (FXI al día siguiente, Woodbridge y Copo 24 h, Lear ?).
8. Dos releases reales por cliente para armar los fixtures anonimizados.

---

## 7. Decisiones de José (30-sep-2026)

José aprobó el E0. **E1a arranca el 5-oct.**

| # | Pregunta | Decisión |
|---|---|---|
| 1 | Orden | Antes del 31-oct: E1a → E3 → carga 2026. E1b en noviembre, luego E2 y E4. Vale el plan B de R1 (si E1a no está en `main` el 14-oct, el 2027 se arma con histórico + proyectos). |
| 2 | Aprobación del presupuesto | Un solo candado, en código: grupo nuevo **«Aprueba presupuesto de ventas»** con Jacobo Mizrahi y José J. Mizrahi como suplente. **Sin salida** por MAST ni por Dirección de Operaciones. Se retira la regla Studio 63. |
| 3 | «Nueva revisión» | Solo el grupo de aprobación, como excepción. El KPI VE-02 mide **siempre contra el aprobado original**; los ajustes van al estimado de cierre. |
| 4 | Factura sin equipo | **Bloquea** solo si trae producto terminado (tela). Activos, servicios y anticipos pasan con aviso. La venta de la rama ICOMATEX (INV/2026/03/0173) queda fuera del presupuesto. |
| 5 | World Emblem | **Pendiente**: la respuesta llegó sin elegir entre Especiales y Confección. Si es Especiales, se pone Especiales como equipo del cliente para que sus facturas nuevas caigan ahí. |
| 6 | TQ-1 en 2027 | Solo Industrial. |
| 7 | Acuse | Automático, solo «recibido», sin comprometer cantidades, a nombre de la vendedora responsable del perfil. |
| 8 | Pedidos desde la zona firme (E2) | Solo N semanas por perfil (`so_weeks`, 4 por defecto). El resto del horizonte firme se queda como pronóstico / autorización. |
| 9 | IA | Sí, solo en perfiles `ia` (Contitech, Seiren). La llave va en una **variable de entorno de Odoo.sh**, no en `ir.config_parameter`. |
| 10 | Carga 2026 | José manda el Excel por Drive. La carga queda como **presupuesto aprobado 2026**, lo aprueba Jacobo, para que el KPI de este año tenga base. |

Pendientes de José:
- agendar la sesión con Jessica y Berenice esta semana (cuestionario, catálogo de partes y releases de muestra);
- pedir a Calidad que valide el rendimiento m/kg de los productos del piloto (R2).

Ajustes al diseño que salen de estas decisiones:
- `qb.release.profile` agrega `so_weeks` (default 4).
- `ack_template_id` se firma con `sales_user_id`.
- El lector IA toma la llave de `os.environ`.
- `sgi.sales.budget` guarda `approved_original_amount` por mes al aprobar la Rev.1; el KPI lee eso y no la revisión vigente.
- La carga 2026 crea los documentos por mercado ya aprobados (aprobación registrada a nombre de Jacobo).
