# qb_obligation — Obligaciones vivas

Una obligación es un compromiso con **cinco datos**: qué hay que hacer, de
quién es (usuario de Odoo), sobre qué documento, cómo se prueba que ya se
hizo y cuándo vence. Si no tiene los cinco, no entra. Misma mecánica en
comercial, operaciones, compras, finanzas, SGI y RH: cambia la regla de
cierre, no el sistema. El cierre por evidencia es una consulta, nunca el
juicio de un modelo.

## De dónde nace y cómo se cierra

| Origen | Cómo entra | Dueño | Cierre |
|---|---|---|---|
| **Odoo** (hechos) | Cron horario. Hoy: factura de cliente publicada, vencida, con saldo → `collection.overdue_invoice`, nace **confirmada**. | `collection_user_id` del contacto o el default de Finanzas | Saldo ≤ tolerancia o estado de pago pagado / en proceso / revertido. Cancelada o bloqueada → cancelada. Si el SAT ya ve el pago (complementos vigentes > cobrado en Odoo) pasa a `collection.apply_payment` con el dueño de aplicar pagos (requiere `quimibond_sat`). |
| **Correo** (memoria en Supabase, `email_pending_actions`) | Mismo cron: cada pendiente abierto detectado en el correo entra como **candidata** por área: compromiso de entrega, cotización, documento solicitado → Comercial; RFQ → Compras; promesa de pago → Finanzas; otro → Otro. Idempotente por id. | El **buzón que recibió el correo** si es usuario de Odoo (`login`/`email`); si no, el dueño del área en la compañía. Sin dueño no se crea. | `email_resolved`: la memoria marca el pendiente resuelto → cumplida con evidencia; expirado → cancelada. |
| **Manual / MCP / gabinete** | `qb.obligation.create_candidate(vals)` | El que se indique o el default del área | Según `evidence_rule_key`: `so_delivered` (pedido entregado por completo), `po_received` (compra recibida por completo), `invoice_paid_or_credited`, `email_resolved` u `owner_ack`. |

El cliente de la memoria viene de `qb_memoria` (llave de Supabase ya configurada).
Si Supabase no responde, la corrida sigue con lo de Odoo.

## Flujo del dueño

App **Obligaciones**: *Mis obligaciones*, *Por confirmar* (lo que llega del
correo, agrupado por área), *Todas*, *Métricas* (pivot por semana, estado,
área). Botones: *Sí, es mía*, *Ya se cumplió* (acuse), *Descartar*
(pegajoso: el mismo origen no la vuelve a crear), *Cancelar*. Chatter con el
historial. Escalación a Dirección tras `N` días desde la confirmación (default
3) cuando rebasa el umbral (saldo o días vencida). Un correo diario por dueño
agrupado por cliente con lo vencido o por vencer en 48 h y las candidatas por
confirmar; otro a Dirección solo con lo escalado.

## Configuración (Ajustes → Compañías → Obligaciones)

Dueños por área (Comercial, Operaciones, Compras, Finanzas/cobranza, SGI, RH,
otros), dueño de aplicar pagos, escalar a, tolerancia de saldo, días de gracia,
días para escalar, umbral de saldo y de días vencida. En el contacto,
`collection_user_id` para cobranza por cliente.

## Tres capas de memoria

1. Hechos duros: Odoo (facturas, pedidos, compras).
2. Obligaciones abiertas: este módulo.
3. Relación y comportamiento (quién se atrasa, qué cliente se calla): se
   **calcula** a partir de 1 y 2 y de la memoria de correo (pestaña Memoria
   del contacto, `qb_memoria`); no es una entidad aparte.

Oleadas: 1) comercial + operaciones + compras (correo + Odoo), 2) finanzas,
3) SGI y RH. Hoy viven la 1 (correo) y la 2 (cobranza de Odoo).
