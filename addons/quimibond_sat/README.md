# quimibond_sat — SAT (Syntage) dentro de Odoo

Trae a Odoo los CFDI que el SAT tiene para cada compañía (emitidos y recibidos)
usando Syntage como proveedor de descarga masiva, y los cruza por folio fiscal
(UUID) contra las facturas de Odoo. Reemplaza la capa `syntage_*` /
`canonical_invoices` que vivía en Supabase.

## Qué hace

| Pieza | Dónde | Qué |
|---|---|---|
| Webhook | `POST /quimibond_sat/webhook` | Recibe eventos `invoice.*` de Syntage. Firma HMAC (`X-Satws-Signature`), idempotente por id de evento. |
| Descarga por API | `sat.syntage.client.pull_invoices` | GET paginado de `/entities/{id}/invoices` por periodo. Upsert por UUID. Cron cada 6 h (últimos 7 días) y asistente manual. |
| Extracción | `sat.syntage.client.request_extraction` | `POST /extractions`: Syntage va al SAT por el periodo. Cron diario 05:00 UTC (últimos 4 días). Tiene costo en Syntage. |
| Cruce | `sat.cfdi._match_move` | UUID contra `l10n_mx_edi.document` (documento CFDI) y luego contra `account.move.l10n_mx_edi_cfdi_uuid`. Entre varias facturas con el mismo UUID gana la publicada más reciente. Cron horario para las pendientes. |
| Comparación | menú SAT (Syntage) → Comparación SAT vs Odoo | Vista SQL con cubetas *en ambos / solo SAT / solo Odoo / sin UUID / ignorado* y hallazgos *monto distinto*, *cancelado en el SAT pero publicado en Odoo*, *cancelado en Odoo pero vigente en el SAT*. |

| Segunda pasada | `sat.cfdi.action_suggest` (cron horario) | Para los CFDI "solo en el SAT": busca la factura de la misma contraparte (RFC), mismo total (±0.5%) y fecha ±45 días. Motivos: *sin XML en Odoo*, *mismo RFC/monto/fecha* y *XML cruzado* (la factura trae ligado otro CFDI del mismo proveedor que no cuadra en monto). Se acepta o rechaza a mano; nunca liga solo. Al aceptar un XML cruzado, el CFDI equivocado vuelve a "solo en el SAT" con nota. |
| Al centavo | pivot de la comparación | `SAT vigente` − `Odoo publicado` = `Δ` por mes, sentido y hallazgo, **en MXN** (egresos restan; cancelados no cuentan). La suma de Δ por hallazgo explica la diferencia del mes peso por peso. Los totales por documento van en su moneda: se tolera un centavo de redondeo y desde dos centavos es *monto distinto*; si la factura de Odoo está en otra moneda que el CFDI es *moneda distinta*. Las columnas MXN usan el tipo de cambio del CFDI en los dos lados cuando la moneda coincide (el Δ no carga diferencias de tipo de cambio); si no coincide, el lado Odoo va en moneda de la compañía según Odoo. |
| Política por contacto | ficha del contacto, campo "CFDI del SAT" | *Factura con CFDI* (default, se concilia); *Se registra por póliza* (banco, IMSS, SAT, Infonavit: sus CFDI quedan ignorados solos y sus facturas sin XML van a la cubeta *por póliza*); *No emite CFDI* (nómina; los contactos con país distinto de México cuentan igual sin marcarlos). Las cubetas *ignorado / por póliza / sin CFDI esperado* van en cero en las columnas de conciliación: fuera del Δ. |
| Aceptación automática | cron horario | Una sugerencia *sin XML* se liga sola si el total es exacto al centavo, la moneda coincide, la fecha está a ±10 días y ninguna otra sugerencia o CFDI apunta a esa factura. Queda con nota "Ligado automáticamente" y `match_method = sugerido`, desligable. Todo lo demás sigue esperando a una persona. |
| Alerta diaria | cron 13:00 UTC, parámetro `quimibond_sat.alert_email` | Correo con los hallazgos abiertos (cancelado en Odoo / en el SAT, monto, moneda): totales por hallazgo y detalle de los nuevos de los últimos 7 días. Sin destinatarios o sin hallazgos no manda nada. |
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
4. Carga inicial: menú SAT (Syntage) → **Traer CFDI del SAT**, modo *Descargar*,
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
