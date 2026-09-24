# qb_obligation — Obligaciones vivas

Una obligación es un compromiso con **cinco datos**: qué hay que hacer, de
quién es (usuario de Odoo), sobre qué documento, cómo se prueba que ya se
hizo y cuándo vence. Si no tiene los cinco, no entra. Misma mecánica en
comercial, operaciones, compras, finanzas, SGI y RH: cambia la regla de
cierre, no el sistema. El cierre por evidencia es una consulta, nunca el
juicio de un modelo.

> **Desde 3.2.0 el espejo en actividades está apagado** (decisión del CEO,
> 2026-09-24): las obligaciones no crean actividades en contactos ni
> documentos y las que había se quitaron en el update (migración). Viven solo
> en la app Obligaciones. Se enciende por compañía en Ajustes → Compañías →
> Obligaciones → *Reflejar en actividades de Odoo*; el cron horario crea o
> quita las actividades según el interruptor. Lo que sigue describe el espejo
> encendido.

**No es una app aparte.** Cada obligación abierta es una actividad nativa de
Odoo (`mail.activity`, tipo *Obligación*) sobre su documento ancla (pedido,
compra, factura) o, si no tiene, sobre el contacto. Se ve en el reloj de
actividades y en el chatter del documento, asignada al dueño con la fecha:

| En la actividad | En la obligación |
|---|---|
| Marcar hecha (con o sin comentario) | Cumplida por **acuse del dueño** (`owner_ack`) |
| Cancelar | **Descartada** (pegajoso: el mismo origen no la vuelve a crear) |
| — | Cerrada por evidencia (Odoo o memoria) → la actividad se marca hecha sola |
| — | Cambia dueño o fecha → la actividad sigue |

**La cobranza no vive aquí.** Las facturas vencidas ya están en Contabilidad;
en 3.0.0 se retiró la generación de obligaciones desde facturas vencidas y la
reasignación por complementos del SAT (migración: borra el piloto).

## De dónde nace y cómo se cierra

| Origen | Cómo entra | Dueño | Cierre |
|---|---|---|---|
| **Correo** (memoria en Supabase, `email_pending_actions`) | Cron horario: cada pendiente abierto detectado en el correo entra como **candidata** por área: compromiso de entrega, cotización, documento solicitado → Comercial; RFQ → Compras; promesa de pago → Finanzas. Idempotente por id. | Ver abajo | `email_resolved`: la memoria marca el pendiente resuelto → cumplida con evidencia; expirado → cancelada. |
| **Manual / MCP / gabinete** | `qb.obligation.create_candidate(vals)` | El que se indique o el default | Según `evidence_rule_key`: `invoice_paid_or_credited` (saldo de la factura en cero), `so_delivered`, `po_received`, `email_resolved` u `owner_ack`. |

**Un pendiente, una obligación.** El mismo hilo suele llegar a varios buzones
(CC) y la memoria detecta un pendiente por buzón. Se agrupan por clave
(`dedupe_key`: contraparte + tipo + montos y referencias del texto); nace una
sola obligación, con los ids duplicados en `detection_payload.duplicates`, y
cualquiera de ellos la cierra. Dueño del grupo: el encargado aprendido si lo
hay, si no el primer buzón que sea usuario. Las candidatas viejas sin clave se
deduplican en cada corrida.

Si Supabase no responde, la corrida sigue con lo de Odoo.

## Quién es el dueño

En orden:

1. El **buzón que recibió el correo**, si es usuario de Odoo (`login`/`email`).
2. Lo que la **memoria aprendió**: `partner.memoria_owner_for(area)` de
   `qb_memoria` (encargado por área con señal, si no el general).
3. Finanzas: `collection_user_id` del contacto o el default de la compañía.
4. El dueño del área configurado en la compañía. Sin dueño no se crea.

## Flujo y registro

El trabajo diario pasa por las actividades. El registro completo (evidencia,
escalación, métricas) vive en la app **Obligaciones**: *Mis obligaciones*,
*Por confirmar*, *Todas*, *Métricas* y *Configuración* (buzones de la memoria,
dueños por área). Botones: *Sí, es mía*, *Ya se cumplió*,
*Descartar*, *Cancelar*. Escalación a Dirección tras `N` días desde la
confirmación (default 3) cuando rebasa el umbral (saldo o días vencida). Correo
diario por dueño con lo vencido o por vencer en 48 h y las candidatas; otro a
Dirección solo con lo escalado.

## Configuración (Ajustes → Compañías → Obligaciones)

Dueños por área (Comercial, Operaciones, Compras, Finanzas/cobranza, SGI, RH,
otros), escalar a, tolerancia de saldo, días para escalar, umbral de saldo y
de días vencida. En el contacto, `collection_user_id` (finanzas por cliente) y
la pestaña Memoria con el encargado aprendido.

## Tres capas de memoria

1. Hechos duros: Odoo (facturas, pedidos, compras).
2. Obligaciones abiertas: este módulo, visibles como actividades.
3. Relación y comportamiento (quién atiende a quién, quién se atrasa): se
   **calcula** a partir de 1 y 2 y de la memoria de correo (`qb_memoria`).
