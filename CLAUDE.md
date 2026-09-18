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

## Otros módulos del repo

- `quimibond_sat`: CFDI y complementos de pago del SAT (Syntage) dentro de Odoo, comparación al centavo, alerta diaria. README propio.
- `qb_obligation`: obligaciones vivas (qué, quién, sobre qué documento, cómo se prueba, cuándo vence). Nacen del correo (memoria) o a mano, viven como **actividades nativas** de Odoo sobre su documento o contacto (hecha = acuse, cancelada = descarte, evidencia las cierra sola), escalan a Dirección y mandan un recordatorio diario. La cobranza NO vive aquí (v3.0.0). App Obligaciones (registro, métricas y configuración); el trabajo diario es en Actividades. API `create_candidate`. README propio.
- `qb_build_limpio`: corrige **en la base** lo que pinta de naranja el build de Odoo.sh (etiquetas duplicadas y dependencias no buscables de campos de Studio, vistas por defecto de Studio inválidas, grupos inexistentes en vistas, columnas requeridas sin NOT NULL). Corre solo al terminar cualquier instalación/actualización de módulos y a mano en Ajustes → Técnico → Build limpio. El único aviso que venía del repo (`qb.obligation` "Currency" ×2) se corrigió en código. README propio.
- Módulos de Consolti en la raíz del repo (venían solo en `qbtesting`; desde 2026-09-18 viven en `main`/`quimibond` y se instalan a mano desde Apps): `quimibond_ficha_tecnica_tela` (fichas técnicas de tejido y acabado, importación desde Excel), `quimibond_tintoreria_rendimiento` (capacidad por rendimiento y relación de baño por centro de trabajo de tintorería), `mantenimiento_surtido_refacciones` (refacciones en solicitudes de mantenimiento con surtido desde almacén). README propio en los dos primeros.
- `qb_memoria`: pestaña Memoria del contacto (Supabase). Desde 1.2.0 muestra la **ficha consolidada** (RPC `memoria_brief`): quién atiende a la empresa, hechos con vigencia y conversaciones resumidas por Claude con estado y pendientes. Además **dueños aprendidos**: cron nocturno que lee la vista `memoria_encargados` (buzón que atiende a cada empresa / área) y lo escribe en el contacto; personas detrás de buzones compartidos en Contactos → Configuración → Buzones (memoria). README propio.

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

- **Cada 1 hora:** `push_to_supabase()` — por default **solo `contacts`** (contactos + empresas), que es lo único que la memoria de correo en Supabase consume (decisión CEO 2026-09-17: Supabase solo guarda lo que Odoo no tiene). Para volver a empujar todo: parámetro `quimibond_intelligence.push_models = all` (o lista con comas). El push pesado (`push_to_supabase_heavy`) respeta la misma lista.
- **Cada 5 min:** `pull_from_supabase()` — comandos + contactos

## Deploy a produccion

Procedimiento completo con verificaciones: **`docs/RUNBOOK_DESPLIEGUE.md`**. Resumen:

1. `main` al dia con `quimibond` (PR `quimibond` → `main`)
2. PR `main` → `quimibond`
3. Shell Odoo.sh: `odoo-update <modulos sin bump> && odoosh-restart http && odoosh-restart cron`
4. Verificar (el runbook trae las consultas)

**Las ramas se mantienen como superconjuntos:** `main` ⊇ `quimibond`, y `qbtesting` ⊇ `quimibond`. Una rama de desarrollo a la que le faltan modulos que produccion SI tiene revienta al rebuildear, porque la BD es copia de produccion y el codigo no esta (`KeyError: 'sgi.indicator'`).

## Version del manifest: subela por default

Subir la version hace el despliegue determinista y rastreable. **Cuando exactamente Odoo.sh corre `-u` no esta confirmado**, y conviene no asumirlo: se observo un build de rama que NO actualizo un modulo con archivos cambiados y version congelada (`Model X has no table` sobre los modelos nuevos), y tambien un deploy a produccion que SI lo actualizo sin bump.

Por eso la regla practica no es "el bump garantiza el update", sino: **sube la version, y verifica despues de desplegar** (el runbook trae las consultas). Lo que si se sabe seguro es que sin verificar no te enteras: cuando una restriccion no se puede crear, Odoo la registra en el log y se la salta.

**Excepciones:** anotalas en `tools/no_bump.txt` con su motivo. Hoy: `quimibond_intelligence`, porque el update automatico destapa errores pre-existentes de Odoo Studio y pinta el build en rojo. Es una deuda, no una politica — al limpiar las vistas de Studio invalidas se saca de la lista.

El CI lo revisa (`tools/check_addons.py`): cambiar archivos sin bump es **advertencia**; agregar un **modelo nuevo** sin bump es error, porque ahi si hay evidencia directa de tablas sin crear.

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
