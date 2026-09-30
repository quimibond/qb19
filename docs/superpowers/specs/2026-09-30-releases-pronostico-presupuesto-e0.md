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
| M3 | Aprobación doble (código vs Studio 63) | **Por confirmar en producción** | El código deja aprobar a `group_sgi_director` **o** `group_sgi_manager` (`sgi_sales_budget.py:590-591`). La regla Studio 63 no vive en el repo. <!-- STUDIO63 --> |
| M4 | El pronóstico exige cliente | **Confirmado** | Cabecera `sgi_sales_budget.py:513-519` y línea `sgi_sales_budget_line.py:369-376`. |
| M5 | F-P-A28-13 no está en Documentos | No verificable desde el código (es dato de Documentos). Lo reviso en el E1. |

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
| N7 | Almacén del MPS = el primero de la compañía (`sgi_sales_budget.py:1142-1148`, `limit=1` sin orden). | `_sgi_mps_warehouse` | Con más de un almacén la demanda puede caer en el equivocado. <!-- ALMACENES --> |
| N8 | Periodo del MPS: el pronóstico manda lunes y el presupuesto día 1 del mes; si el MPS de la compañía está en meses, las celdas semanales caen en fechas que el MPS no muestra. <!-- MPS_PERIODO --> | `_send_forecast_to_mps` | Hay que agrupar al periodo configurado del MPS. |

**Menores:**

| # | Hallazgo | Evidencia |
|---|---|---|
| N9 | La semana 1 del forecast es «el primer lunes del año» (`sgi_sales_budget_import.py:202-207`), no la semana ISO; en 2026 la semana 1 del importador empieza el 5-ene y la ISO el 29-dic-2025. El release trae fechas, así que el lector nuevo no usa números de semana. | `_week_monday` |
| N10 | `_sgi_forecast_sols` (usado por el botón «Ver pedidos de la semana») trae **todo** el histórico de pedidos del producto y cliente y filtra en Python (`sgi_sales_budget_line.py:562-576`). | rendimiento |
| N11 | `action_set_obsoleto` existe sin botón ni validación de grupo (`sgi_sales_budget.py:636-638`); el mixin lo bloquea desde `aprobado`, pero desde borrador cualquiera con escritura lo puede llamar por RPC. | seguridad menor |

---

## 3. Faltantes de datos maestros

<!-- DATOS_MAESTROS -->

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
| `qb.customer.part` (Catálogo de partes del cliente) | Parte del cliente → producto | `partner_id`, `ship_to_id` (opcional), `customer_part` (L002790184NCPAA), `customer_description`, `product_id`, `customer_uom` (MT, YD, KG, M2…), `conversion` (`fija` con `factor` / `ficha`: gramaje × ancho de `qb.producto.ficha`), `factor`, `active`. Único por cliente + planta + parte. **Migración**: siembra filas desde los `customer_code` que ya tienen las líneas de pronóstico. |
| `qb.release` (Release, con chatter) | Un documento recibido = una versión | `profile_id`, `partner_id`, `ship_to_id`, `release_ref` (000174), `release_date`, `version` (consecutivo por perfil), `previous_id`, `message_id`/`attachment_ids`, `reader_used`, `state` (`recibido` → `leido` → `revisado` → `aplicado`; `por_revisar`, `reemplazado`, `descartado`), `error_reason`, `ack_sent_at`, totales de cambio |
| `qb.release.part` | Encabezado por parte (en Lear, PO, CUM y autorizaciones son **por parte**, no por release) | `release_id`, `customer_part`, `part_id` (catálogo), `product_id`, `customer_po` (1030782), `cum_received`, `in_transit_qty`, `last_receipt_date/qty`, `last_packing_slip`, `fab_auth_qty/date`, `raw_auth_qty/date`, `cum_ours`, `cum_diff`, `cum_state` (cuadra / no cuadra / sin dato) |
| `qb.release.line` | Detalle semanal | `release_part_id`, `date_customer` (la fecha del release), `date_ship` (embarque = entrega − tránsito), `week` (lunes), `qty_customer` (unidad del cliente), `qty` (unidad del producto), `line_type` (firme / planeado según el release), `zone` (calculada: `firme` / `materia_prima` / `planeado`), `cum_req`, `net_req` |
| `qb.release.change` | Diferencias contra el release vigente (se guardan: son la base de la métrica de estabilidad) | `release_id`, `product_id`, `week`, `qty_prev`, `qty_new`, `delta`, `kind` (aumento / reducción / nueva / desaparece / movimiento de fecha), `in_firm`, `in_lead_time` |
| `sgi.sales.budget.assumption` (Supuesto) | Lo que Ventas ajusta sobre el presupuesto armado | `budget_id`, `kind` (aumento de precio % / precio fijo / baja de cliente / TC / volumen %), `partner_id`, `product_id`, `team_id`, `date_from` (mes), `value`, `note` |

**Cambios a los existentes**

| Modelo | Cambio |
|---|---|
| `sgi.sales.budget` | `kind` agrega `estimado` (estimado de cierre mensual, uno por mercado-año, se regenera cada mes); `forecast_scope` (`cliente` / `producto`) para permitir el pronóstico de Confección sin cliente; `fx_rate_s1`, `fx_rate_s2` (TC por semestre, obligatorios para aprobar, congelados al aprobar); `origin_budget_id` (la revisión anterior, para comparar); estado `reemplazado` en vez de `obsoleto` para la revisión que queda como histórico comparable. |
| `sgi.sales.budget.line` | `product_id` opcional + `project_name`, `project_gramaje`, `project_ancho` (proyectos sin artículo); `price_mode` (`lista` / `capturado`), `price_input`, `price_currency_id`, `price_uom` (m / kg) — el importe usa el capturado si lo hay, si no la lista **a la fecha de la línea**, y **0 si no hay regla** (nunca el precio de venta); `release_id`/`release_line_ids` (de qué release salió la cantidad); `source` (`release` / `historico` / `proyecto` / `manual` / `excel`); `qty_kg` y `qty_m` calculadas con la ficha; `partner_shipping_id` opcional. |
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

<!-- PLAN -->

---

## 6. Riesgos y preguntas

<!-- RIESGOS -->
