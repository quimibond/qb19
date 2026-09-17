# qb_obligation — Obligaciones vivas (piloto: cobranza)

Una obligación es un compromiso con **cinco datos**: qué hay que hacer, de
quién es (usuario de Odoo), sobre qué documento, cómo se prueba que ya se
hizo y cuándo vence. Si no tiene los cinco, no entra. El cierre por evidencia
es una consulta, nunca el juicio de un modelo.

| Qué | Dónde | Cómo |
|---|---|---|
| Obligaciones | App **Obligaciones** (menú principal) → Mis obligaciones / Todas | Lista agrupada por cliente; botones *Sí, es mía*, *Ya se cumplió*, *Descartar*, *Cancelar*; chatter con el historial. *Descartar* es pegajoso: la factura no vuelve a generar obligación aunque siga vencida (cartera histórica, disputa). |
| Cobrar factura vencida | Cron cada hora `Obligaciones - Cobranza` | Nace **confirmada** de cada factura de cliente publicada, vencida (menos días de gracia) y con saldo. Dueño: `collection_user_id` del contacto o el default de la compañía. **Sin dueño configurado no crea nada.** |
| Cierre por evidencia | Mismo cron | `abs(saldo) ≤ tolerancia` (default $1) o estado de pago pagado / en proceso / revertido → cumplida con la factura como evidencia. Factura cancelada o bloqueada → obligación cancelada. |
| Aplicar pago que el SAT ya ve | Mismo cron (requiere `quimibond_sat`) | Complementos de pago vigentes por más de lo que Odoo registra cobrado → la obligación cambia a `collection.apply_payment` y pasa al dueño de aplicar pagos de la compañía. No cierra: el cliente pagó, falta aplicarlo. |
| Promesa de pago (correo) | API `qb.obligation.create_candidate(vals)` | Candidata idempotente por `source_ref`; el dueño confirma o descarta. Sin fecha en el origen: vence en 3 días y queda marcada *sin fecha firme*. |
| Escalación | Mismo cron | Confirmada, abierta `N` días **desde que se confirmó** (default 3) y por encima del umbral (saldo ≥ monto configurado **o** más de X días vencida) → escalada al usuario de Dirección. Sin usuario de Dirección no escala. |
| Recordatorio diario | Cron `Obligaciones - Recordatorio diario` (7:30 CDMX) | Un correo por dueño con lo vencido o por vencer en 48 h **agrupado por cliente** y las candidatas por confirmar; otro a Dirección solo con lo escalado. No se crean actividades de Odoo: un solo recordatorio. |
| Métricas | Obligaciones → Métricas | Pivot por semana de creación y estado: cobrado, días para cerrar. Descartadas / creadas es la tasa de ruido del origen de correo. |

Configuración por compañía (Ajustes → Compañías → pestaña Obligaciones):
dueño de cobranza, dueño de aplicar pagos, escalar a, tolerancia de saldo,
días de gracia, días para escalar, umbral de saldo y de días vencida.

Fuera del piloto: releases, órdenes de compra, SGI y RH usan la misma
plantilla cambiando la regla de cierre. El canal desde Supabase
(`obligation_candidates` → `create_candidate`) se conecta en el siguiente paso.
