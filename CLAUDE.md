# qb19 — Quimibond Odoo 19 Addon

## Que es

Addon de Odoo 19 que sincroniza datos operativos a Supabase para Quimibond Intelligence.

**Frontend:** `quimibond/quimibond-intelligence` (Vercel)
**Supabase:** `tozqezmivpblmcubmnpi`

## Estructura

```
addons/quimibond_intelligence/
  __manifest__.py          # v19.0.30.0.0 (NO cambiar — ver nota abajo)
  models/
    sync_push.py           # Push Odoo → Supabase (21 modelos)
    sync_pull.py           # Pull Supabase → Odoo
    supabase_client.py     # REST client HTTP
    sync_log.py            # Modelo de log
  views/sync_status_views.xml
  data/ir_cron_data.xml
  security/ir.model.access.csv
```

## Modelos sincronizados (21)

| Metodo | Odoo Model | Supabase Table |
|---|---|---|
| `_push_contacts` | res.partner | contacts + companies (incluye RFC/vat) |
| `_push_products` | product.product | odoo_products |
| `_push_order_lines` | sale/purchase.order.line | odoo_order_lines |
| `_push_users` | res.users + hr.employee | odoo_users |
| `_push_invoices` | account.move | odoo_invoices |
| `_push_invoice_lines` | account.move.line | odoo_invoice_lines |
| `_push_payments` | account.move (paid) | odoo_payments |
| `_push_deliveries` | stock.picking | odoo_deliveries |
| `_push_crm_leads` | crm.lead | odoo_crm_leads |
| `_push_activities` | mail.activity | odoo_activities |
| `_push_manufacturing` | mrp.production | odoo_manufacturing |
| `_push_employees` | hr.employee | odoo_employees |
| `_push_departments` | hr.department | odoo_departments |
| `_push_sale_orders` | sale.order | odoo_sale_orders |
| `_push_purchase_orders` | purchase.order | odoo_purchase_orders |
| `_push_orderpoints` | stock.warehouse.orderpoint | odoo_orderpoints |
| `_push_account_payments` | account.payment | odoo_account_payments |
| `_push_chart_of_accounts` | account.account | odoo_chart_of_accounts |
| `_push_account_balances` | account.move.line (aggregated) | odoo_account_balances |
| `_push_bank_balances` | account.journal (bank/cash) | odoo_bank_balances |
| `_push_currency_rates` | res.currency.rate | odoo_currency_rates |

## Campos clave de Odoo

- **`default_code`** = Referencia Interna del producto → se guarda como `internal_ref` en odoo_products y `product_ref` en order/invoice lines. **SIEMPRE usar para display en frontend.**
- **`commercial_partner_id`** = Empresa padre en Odoo → se resuelve via `_commercial_partner_id()` para linkear a companies.
- **`vat`** = RFC fiscal → se guarda como `rfc` en companies.
- **`salesperson_user_id`** en sale_orders = vendedor real → se usa para asignar insights.
- **`buyer_user_id`** en purchase_orders = comprador real → se usa para insights de proveedores.

Ver mapeo completo de campos en `quimibond-intelligence/CLAUDE.md`.

## Crons

- **Cada 1 hora:** `push_to_supabase()` — sync completo
- **Cada 5 min:** `pull_from_supabase()` — comandos + contactos

## Deploy a produccion

Procedimiento completo con verificaciones: **`docs/RUNBOOK_DESPLIEGUE.md`**. Resumen:

1. `main` al dia con `quimibond` (PR `quimibond` → `main`)
2. PR `main` → `quimibond`
3. Shell Odoo.sh: `odoo-update <modulos sin bump> && odoosh-restart http && odoosh-restart cron`
4. Verificar (el runbook trae las consultas)

**Las ramas se mantienen como superconjuntos:** `main` ⊇ `quimibond`, y `qbtesting` ⊇ `quimibond`. Una rama de desarrollo a la que le faltan modulos que produccion SI tiene revienta al rebuildear, porque la BD es copia de produccion y el codigo no esta (`KeyError: 'sgi.indicator'`).

## Version del manifest: subela, salvo excepcion escrita

Odoo.sh ejecuta `-u` **solo cuando cambia la version**. Si no la subes, tu cambio se queda en el repo y **nunca llega a la base de datos**: vistas viejas, datos sin sembrar, restricciones sin crear, modelos borrados que siguen en `ir_model`.

`qb_capacidad_costeo` acumulo 43 commits sin bump y de ahi salieron, todas juntas: `qb.cotizacion.tramo` y `qb.producto.ficha` sin tabla, el modelo huerfano `qb.costeo.supabase.import`, y 7 restricciones de unicidad declaradas que nunca se aplicaron.

**Excepciones:** solo las de `tools/no_bump.txt`, cada una con su motivo escrito. Hoy: `quimibond_intelligence`, porque el update automatico destapa errores pre-existentes de Odoo Studio y pinta el build en rojo. Es una deuda, no una politica — al limpiar las vistas de Studio invalidas se saca de la lista.

El CI lo verifica (`tools/check_addons.py`): cambiar archivos de un modulo sin bump y sin exencion es error.

## Odoo.sh config

```
quimibond_intelligence.supabase_url = https://tozqezmivpblmcubmnpi.supabase.co
quimibond_intelligence.supabase_service_key = (service key)
```

## Modelos pendientes de sincronizar

| Modelo | Prioridad | Valor |
|---|---|---|
| stock.warehouse.orderpoint | High | Deteccion de desabasto |
| account.payment.term | Medium | Prediccion de pago |
| res.partner.category | Medium | Segmentacion |
| mail.message | Medium | Comunicacion interna |
| mrp.bom | Medium | Costos produccion |
| quality.check | Medium | Calidad |
