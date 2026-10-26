# quimibond_sat — SAT (Syntage) dentro de Odoo

Trae a Odoo los CFDI que el SAT tiene para cada compañía (emitidos y recibidos)
usando Syntage como proveedor de descarga masiva, y los cruza por folio fiscal
(UUID) contra las facturas de Odoo. Reemplaza la capa `syntage_*` /
`canonical_invoices` que vivía en Supabase.

## Menú

`Contabilidad → SAT (Syntage)` tiene dos entradas de trabajo y un cajón técnico:
**Conciliar** (lo que no cuadra, con botones para resolverlo), **Pagos (REP)**
(complementos de pago vs pagos de Odoo) y **Datos** (CFDI crudos, complementos,
Traer CFDI del SAT, bitácora y eventos de webhook; solo contadores).

## Qué hace

| Pieza | Dónde | Qué |
|---|---|---|
| Webhook | `POST /quimibond_sat/webhook` | Recibe eventos `invoice.*` de Syntage. Firma HMAC (`X-Satws-Signature`), idempotente por id de evento. |
| Descarga por API | `sat.syntage.client.pull_invoices` | GET paginado de `/entities/{id}/invoices` por periodo. Upsert por UUID. Cron cada 6 h (últimos 7 días) y asistente manual. |
| Extracción | `sat.syntage.client.request_extraction` | `POST /extractions`: Syntage va al SAT por el periodo. Cron diario 05:00 UTC (últimos 4 días). Tiene costo en Syntage. |
| Cruce | `sat.cfdi._match_move` | UUID contra `l10n_mx_edi.document` (documento CFDI) y luego contra `account.move.l10n_mx_edi_cfdi_uuid`. Entre varias facturas con el mismo UUID gana la publicada más reciente. Cron horario para las pendientes. |
| Conciliar | Contabilidad → SAT (Syntage) → **Conciliar** | La pantalla de trabajo. Abre en lista con el filtro *Por conciliar*: CFDI sin factura, facturas sin CFDI o sin UUID, montos o monedas distintas y cancelaciones cruzadas. Cada fila trae *Conciliar*, *Aceptar* (sugerencia), *CFDI* y *Factura*. Cubetas *en ambos / solo SAT / solo Odoo / sin UUID / ignorado / aún no extraído del SAT* y pivot al centavo en la vista Pivot. **Horizonte del SAT:** una factura de Odoo con UUID fechada el día del timbrado más reciente que Syntage entregó, o después, no es "solo Odoo": es *Aún no extraído del SAT* (fuera de conciliación hasta la siguiente extracción). Las compañías sin *Sincronizar CFDI con Syntage* no aparecen del lado Odoo (no hay contra qué comparar). La fecha hasta la que hay datos se ve en la compañía (*Datos del SAT hasta*) y en la alerta diaria, que avisa si lleva más de 3 días sin timbrados nuevos. |

| Sin ruido (1.19.0) | comparación | Fuera de *Por conciliar*: las facturas de Odoo anteriores a **Datos del SAT desde** (compañía; en PNTQ enero 2025, lo fijó la migración) van a *Antes de los datos del SAT*; los CFDI cancelados en el SAT sin factura van a *Cancelado en el SAT, sin factura* (los dos lados coinciden). Hallazgos nuevos: *Ligado a una factura en borrador* (antes salía como *Monto distinto* con total Odoo 0), *Redondeo* (de 2 centavos a menos de $1, factura capturada a mano) y *Cuadra sin los pagos a terceros* (la diferencia son exactamente líneas de productos marcados **Pago a terceros (no va en el CFDI)**, p. ej. los impuestos de pedimento en la factura del agente aduanal). Filtro *Diferencias explicadas*. |
| RFC por póliza | parámetro `quimibond_sat.rfc_poliza` | RFC que se registran por póliza aunque no exista su contacto en Odoo (default SAT, IMSS, Infonavit): sus CFDI quedan ignorados solos. El parámetro reemplaza la lista completa. |
| Lista 69-B (1.20.0) | contacto, factura, comparación, alerta | El estado del proveedor en la lista 69-B del SAT (EFOS: *presunto*, *definitivo*, *desvirtuado*, *sentencia favorable*) sale de su CFDI recibido más reciente (`issuer.blacklistStatus` de Syntage) y queda en el contacto. Una factura de proveedor **definitivo no se publica** hasta que Contabilidad marque *69-B: operaciones documentadas* en el contacto; presunto o definitivo ponen un aviso rojo en la factura. Columna y filtro *Proveedor en 69-B* en Conciliar, y sección propia en la alerta diaria (sus CFDI no son deducibles salvo que se acredite la operación). |
| Factura desde el XML (1.21.0) | cron diario, botón *Crear factura desde el XML* en el CFDI | Se enciende por compañía eligiendo el diario en **Facturas de proveedor desde el SAT en** (vacío = apagado). Cada CFDI recibido vigente, de hace 3 a 45 días (`quimibond_sat.auto_bill_min_days` / `auto_bill_max_days`, tope `auto_bill_limit` = 50 por corrida), sin factura en Odoo, sin sugerencia y sin factura del mismo RFC y monto (publicada o en borrador, ±45 días), se vuelve una **factura en borrador** con el importador de XML de Odoo (el decodificador de CFDI es de `l10n_mx_edi`): nace con folio fiscal, proveedor e importes del CFDI y queda ligada (*Ligado a una factura en borrador*). Nunca se publica sola. El CFDI guarda en *Factura desde el XML* si se creó, por qué no o el error. Sin decodificador no crea nada. El importador de Odoo hace commit: cada CFDI se confirma por separado. |
| Folio fiscal en la factura (1.22.0) | Conciliar, aceptar sugerencia, factura desde el XML; botón *Escribir folio fiscal en la factura* | Cuando una persona liga un CFDI recibido a una factura de proveedor, el folio fiscal se escribe en la factura como lo hace Odoo al subir el XML: documento CFDI (`l10n_mx_edi.document`, estado *Recibido*) con el XML del SAT adjunto, de donde sale `l10n_mx_edi_cfdi_uuid` (DIOT, contabilidad electrónica). Si la factura traía el XML de otro CFDI (cruzado), ese documento se quita (su XML queda en el chatter) y se anota el cambio. Facturas de cliente no se tocan. Para las ligas anteriores: `sat.cfdi.action_write_fiscal_folio()` sobre los CFDI ligados (botón o MCP). |
| Varios CFDI / varias facturas (1.23.0) | comparación, Conciliar | **Factura con varios CFDI** (el proveedor timbró dos y Odoo los capturó juntos, «F-1237/1238»): se ligan todos a la factura y el total se reparte entre ellos según su monto; ligar el segundo ya no desliga el primero como «XML cruzado» si juntos suman la factura, y el asistente propone la factura por lo que le falta. **CFDI en varias facturas** (mismo UUID en dos facturas, p. ej. una por entrada de almacén): se compara contra la suma de las facturas publicadas, y un XML capturado dos veces sale como *Monto distinto* (antes la segunda factura no aparecía). El folio fiscal no borra los documentos de los otros CFDI de la misma factura. |
| CFDI relacionados (1.23.0) | cruce | Se guardan el tipo de relación y los UUID relacionados (`relations` de Syntage). **Sustitución (04):** el CFDI nuevo toma la factura del cancelado al que sustituye (y su folio fiscal); el cancelado queda *Cancelado en el SAT, sin factura*. **Compensado (01/03):** un ingreso que notas de crédito anulan completo, sin factura en Odoo de ninguno, se ignora con el motivo (neto cero). |
| Otra moneda que cuadra (1.23.0) | comparación | Compra de divisas: el banco timbra en pesos y la factura de Odoo va en dólares. Si en pesos cuadran (±$1) es *Cuadra en pesos (otra moneda)*, fuera de los hallazgos; si no, sigue *Moneda distinta*. |
| Pagos (REP) sin falsos (1.24.0) | Pagos (REP) | Lo pagado según Odoo se compara como **fracción pagada de la factura × total del CFDI** (no estorban pagos a terceros, varios CFDI, otra moneda ni centavos; tolerancia $1). Un pago de Odoo cuyo complemento aún está en plazo (día 5 del mes siguiente + 4 días de extracción) es *complemento aún en plazo*; una factura revertida con nota de crédito en Odoo es *revertida*, no un pago. Columna *Último pago en Odoo*. |
| Al publicar (1.24.0) | factura de proveedor | **No se publica** una factura de proveedor cuyo CFDI (ligado o por su folio fiscal) está cancelado en el SAT y ninguno vigente: hay que pedir el que lo sustituye, que se liga solo al llegar (relación 04). Aviso rojo cuando el folio fiscal de la factura **no aparece en el SAT** aunque ya hay datos del SAT de 5 días después de su fecha. La nota de crédito de un proveedor 69-B definitivo sí se publica (reduce lo deducido). |
| Segunda pasada | `sat.cfdi.action_suggest` (cron horario) | Para los CFDI "solo en el SAT": busca la factura de la misma contraparte (RFC), mismo total (±0.5%) y fecha ±45 días. Motivos: *sin XML en Odoo*, *mismo RFC/monto/fecha* y *XML cruzado* (la factura trae ligado otro CFDI del mismo proveedor que no cuadra en monto). Se acepta o rechaza a mano; nunca liga solo. Al aceptar un XML cruzado, el CFDI equivocado vuelve a "solo en el SAT" con nota. |
| Al centavo | pivot de la comparación | `SAT vigente` − `Odoo publicado` = `Δ` por mes, sentido y hallazgo, **en MXN** (egresos restan; cancelados no cuentan). La suma de Δ por hallazgo explica la diferencia del mes peso por peso. Los totales por documento van en su moneda: se tolera un centavo de redondeo, de dos centavos a menos de $1 es *redondeo* y desde $1 es *monto distinto*; si la factura de Odoo está en otra moneda que el CFDI es *moneda distinta*. Las columnas MXN usan el tipo de cambio del CFDI en los dos lados cuando la moneda coincide (el Δ no carga diferencias de tipo de cambio); si no coincide, el lado Odoo va en moneda de la compañía según Odoo. |
| Política por contacto | ficha del contacto, campo "CFDI del SAT" | *Factura con CFDI* (default, se concilia); *Se registra por póliza* (banco, IMSS, SAT, Infonavit: sus CFDI quedan ignorados solos y sus facturas sin XML van a la cubeta *por póliza*); *No emite CFDI* (nómina; los contactos con país distinto de México cuentan igual sin marcarlos). Las cubetas *ignorado / por póliza / sin CFDI esperado* van en cero en las columnas de conciliación: fuera del Δ. |
| Aceptación automática | cron horario | Una sugerencia *sin XML* se liga sola si el total es exacto al centavo, la moneda coincide, la fecha está a ±10 días y ninguna otra sugerencia o CFDI apunta a esa factura. Queda con nota "Ligado automáticamente" y `match_method = sugerido`, desligable. Todo lo demás sigue esperando a una persona. |
| Alerta diaria | cron 13:00 UTC, parámetro `quimibond_sat.alert_email` | Correo con los hallazgos abiertos (cancelado en Odoo / en el SAT, monto, moneda): totales por hallazgo y detalle de los nuevos de los últimos 7 días. Sin destinatarios o sin hallazgos no manda nada. |
| Conciliar | Botón **Conciliar** en la comparación, en el CFDI y en la factura (*Conciliar con el SAT* cuando no tiene CFDI) | Asistente: lista todas las facturas del mismo RFC que cuadran en monto (publicadas y en borrador, ±180 días y ±0.5 % por default, ajustables) con Δ monto y Δ días, o al revés, los CFDI que cuadran con una factura. Un clic liga el CFDI, deja constancia en el chatter de la factura y adjunta el XML del SAT (Syntage) como nota del chatter, **sin pasar por el importador de adjuntos de Odoo** (en una factura publicada ese camino dispara digitalización / cruce con órdenes de compra, hace `commit` a medias y aborta la transacción: error 500 con la liga grabada y sin constancia). El folio fiscal de una factura publicada no lo cambia Odoo por un adjunto: si está mal, se corrige a mano; el UUID del SAT queda en `sat_uuid` por la liga. Si el XML no está disponible, la liga vale igual. Si la factura tenía otro CFDI (XML cruzado), ese vuelve a *solo SAT* con nota. XML en Syntage: la ruta documentada es `GET /invoices/{id}/cfdi` con `Accept: text/xml` (se prueba primero y queda guardada en el parámetro `quimibond_sat.syntage_xml_path`). Como respaldo, el archivo que registró el webhook `file.created` (`sat.syntage.file`, `GET /files/{fileId}/download`; histórico importable con `sat.syntage.file.action_import_from_supabase()` desde la tabla `syntage_files` de Supabase) y las rutas alternas de `XML_PATH_CANDIDATES`. |
| Pagos (REP) | Contabilidad → SAT (Syntage) → Pagos (REP) | Por factura del SAT (tipo I): pagado según los complementos de pago vigentes vs pagado según Odoo (total − residual). Hallazgos *pagado en Odoo sin complemento* (falta emitir/recibir el REP), *complemento sin pago en Odoo*, *complementos exceden el total* (el mismo pago timbrado más de una vez: sobran REP por cancelar en el SAT), *PUE sin complemento* (no lo requiere), *sin factura*, *cancelado*. Pivot en MXN. |
| Complementos de pago | `sat.cfdi.pago` | Un renglón por documento relacionado (`InvoicePayment` de Syntage): factura pagada por UUID, parcialidad, importe, saldos. Llegan por webhook `invoice_payment.*`, por descarga (modo pagos de `action_pull_period`, `/invoices/payments` por meses: el endpoint es frágil con paginación profunda) o por `action_import_payments(rows)` (CSV de Syntage). Si el pago llega antes que la factura, se liga cuando ésta entra. |
| Estado SAT en la factura | formulario de factura | Botón "CFDI SAT", campo *Estado SAT* y aviso rojo cuando la factura está cancelada en el SAT y publicada en Odoo, cancelada en Odoo y vigente en el SAT, o con total/moneda distintos. |
| Cola robusta | cron "Procesar cola" | Presupuesto de tiempo por corrida (`quimibond_sat.queue_budget_seconds`, default 600 s; Odoo.sh mata el worker a los ~15 min) y re-disparo si queda cola; una corrida en *running* sin avanzar 30 min se re-encola sola. |
| Holgura de fechas | descarga | Syntage filtra `issuedAt` en UTC con límite a las 00:00 del día, así que el último día del mes se perdía (una factura del 31 a las 18:00 de México es el 1 a las 00:00 UTC). La descarga pide un día de holgura por cada lado; los CFDI de más se upsertan sin efecto. |

Los CFDI de bancos, casa de bolsa, IMSS, SAT e Infonavit se registran en Odoo
por póliza, no como factura: márcalos **Ignorar** desde el CFDI para que no
aparezcan como "solo en el SAT" (siguen contando en Δ, en su propia columna).

## Configuración

1. Parámetros del sistema (Ajustes → Técnico → Parámetros del sistema):
   - `quimibond_sat.api_key`: API key de Syntage.
   - `quimibond_sat.webhook_secret`: secreto de firma del webhook.
   - `quimibond_sat.api_base` (opcional): default `https://api.syntage.com`.
2. En la compañía (Ajustes → Compañías, junto al RFC): activar **Sincronizar
   CFDI con Syntage**. La entidad de Syntage se resuelve sola por RFC.
3. En Syntage, apuntar el webhook a `https://<odoo>/quimibond_sat/webhook`.
   Un `GET` a esa URL responde `{"ok": true}` para probar alcance.
4. Carga inicial: Contabilidad → SAT (Syntage) → Datos → **Traer CFDI del SAT**, modo *Descargar*,
   o por MCP / acción de servidor: `sat.cfdi.action_pull_period('2026-09-01',
   '2026-09-30')` (los asistentes no se pueden exponer por MCP; habilitar
   `sat.cfdi` con *allow method calls*). Para meses completos usar
   `background=True`: encola y un cron lo corre con commit por página; el
   avance se ve en la bitácora (En cola → Corriendo → OK). Mismo asistente,
   por meses (Syntage ya tiene el histórico extraído). Si un periodo no está en
   Syntage, usar modo *Extraer* y descargar después.

## Notas

- `l10n_mx_edi` no es dependencia dura: en producción está y el cruce por
  UUID lo usa; sin él el módulo instala igual y solo queda el ligado manual
  (así corre en la imagen community del CI).
- Solo metadatos del CFDI; los XML/PDF se quedan en Syntage.
- Un XML de proveedor capturado dos veces en Odoo aparece como dos facturas con
  el mismo UUID: el cruce liga la publicada más reciente y la otra queda en
  "solo Odoo" para cancelarla.
- Las pruebas corren en CI (`odoo-tests`): firma, ingesta y cruce, comparación,
  y el endpoint del webhook con `HttpCase`.
