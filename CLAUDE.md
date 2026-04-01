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
    sync_push.py           # Push Odoo → Supabase (16 modelos)
    sync_pull.py           # Pull Supabase → Odoo
    supabase_client.py     # REST client HTTP
    sync_log.py            # Modelo de log
  views/sync_status_views.xml
  data/ir_cron_data.xml
  security/ir.model.access.csv
```

## Modelos sincronizados (16)

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

## Crons

- **Cada 1 hora:** `push_to_supabase()` — sync completo
- **Cada 5 min:** `pull_from_supabase()` — comandos + contactos

## Deploy a produccion

1. Push a `main`
2. Merge `main` → `quimibond` en GitHub
3. Shell Odoo.sh: `odoo-update quimibond_intelligence && odoosh-restart http && odoosh-restart cron`

## IMPORTANTE: NO cambiar version del manifest

Odoo.sh ejecuta `-u` cuando detecta cambio de version. El update detecta errores pre-existentes de Odoo Studio y marca el build como Failed. Dejar en `19.0.30.0.0` y hacer `odoo-update` manual.

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
