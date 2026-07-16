# Syntage Backfill Gap — 2026-04-20

**Contexto:** 3,452 CFDIs en Odoo sin registro en Syntage (18% gap). Auditoría Fase 1 §3.3.

## Meses afectados

| Mes | Odoo sin Syntage |
|---|---|
| 2021-12-01 | 1 |
| 2024-06-01 | 9 |
| 2024-07-01 | 13 |
| 2024-08-01 | 14 |
| 2024-09-01 | 13 |
| 2024-10-01 | 11 |
| 2024-11-01 | 12 |
| 2024-12-01 | 13 |
| 2025-01-01 | 232 |
| 2025-02-01 | 228 |
| 2025-03-01 | 245 |
| 2025-04-01 | 207 |
| 2025-05-01 | 258 |
| 2025-06-01 | 226 |
| 2025-07-01 | 241 |
| 2025-08-01 | 175 |
| 2025-09-01 | 252 |
| 2025-10-01 | 198 |
| 2025-11-01 | 200 |
| 2025-12-01 | 201 |
| 2026-01-01 | 206 |
| 2026-02-01 | 195 |
| 2026-03-01 | 234 |
| 2026-04-01 | 76 |

**Total: 3,452 CFDIs** — el gap es sistemático desde ene-2025 (~200/mes), no un evento puntual.

## Acciones para cerrar el gap (user manual)

1. Autenticarse en portal Syntage con RFC Quimibond.
2. Para cada mes de la tabla (priorizar 2025-01 en adelante por volumen), disparar extracción manual:
   - CFDI Emitidos (Income)
   - CFDI Recibidos (Expense)
   - Complementos de Pago (Payment)
3. Monitorear el procesamiento en tiempo real:
   ```sql
   SELECT date_trunc('hour', received_at) AS hour, COUNT(*) AS events
   FROM public.syntage_webhook_events
   WHERE received_at > now() - interval '4 hours'
   GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
   ```
4. Validar gap reducido:
   ```sql
   SELECT COUNT(*) AS gap_remaining FROM public.odoo_invoices
   WHERE cfdi_uuid IS NOT NULL AND NOT EXISTS (
     SELECT 1 FROM public.syntage_invoices si
     WHERE si.uuid = odoo_invoices.cfdi_uuid AND si.uuid IS NOT NULL
   );
   ```

## Nota

La automatización vía API de Syntage se consideró, pero requiere credenciales externas
(SYNTAGE_API_KEY). Dejar como acción manual del admin. Alternativa es UI del portal Syntage.
