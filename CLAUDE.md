# qb19 — Quimibond Odoo 19 Addon

## Que es

Addons de Odoo 19 de Quimibond. `quimibond_intelligence` es el puente mínimo
Odoo ↔ Supabase: empuja **contactos, empresas y usuarios** para que la memoria
de correo (Supabase) pueda ligar correos con Odoo, y trae de vuelta comandos y
contactos nuevos.

> **2026-09-18 — Supabase quedó solo con la memoria de correo.** Se borraron
> de Supabase todas las tablas que duplicaban Odoo (`odoo_*` salvo
> `odoo_users`, `canonical_*`, `gold_*`, `mv_*`, `syntage_*`, `mrp_bom*`,
> `reconciliation_issues`, `audit_*`, el esquema `ingestion`). Todo lo demás
> vive en Odoo: SAT en `quimibond_sat`, costeo en `qb_capacidad_costeo`,
> indicadores en el SGI. El módulo se recortó ese día a `_push_contacts` +
> `_push_users` (PR "quimibond_intelligence: solo contactos y usuarios a
> Supabase"). Las cifras se toman de Odoo por MCP, no de Supabase.

**Supabase:** `tozqezmivpblmcubmnpi` (repo `quimibond/quimibond-intelligence`
guarda las Edge Functions y migraciones; el frontend de Vercel está retirado).

## Estructura

```
addons/quimibond_intelligence/
  __manifest__.py          # v19.0.30.0.0 (NO cambiar — ver nota abajo)
  models/
    sync_push.py           # quimibond.sync: cron push_to_supabase(), helpers, _run_push
    sync_push_partners.py  # _push_contacts (contacts + companies) y _push_users (odoo_users)
    sync_pull.py           # quimibond.sync.pull: sync_commands + contactos nuevos → Odoo
    supabase_client.py     # REST client HTTP (upsert, insert, fetch, patch, rpc)
    sync_log.py            # quimibond.sync.log (Historial de Sync)
  views/sync_status_views.xml   # historial + acciones "Forzar Push/Pull"
  data/ir_cron_data.xml         # push 1h + pull 5min (noupdate)
  data/cleanup_2026_09_18.xml   # borra los crons viejos (noupdate no se limpia solo)
  security/ir.model.access.csv
  tests/                        # pytest puro sobre supabase_client (no corre en CI)
```

Tabla huérfana en la base de Odoo tras el recorte: `quimibond_sync_audit`
(modelo transitorio `quimibond.sync.audit`, eliminado). Odoo no borra tablas
de modelos que desaparecen del código.

## Otros módulos del repo

- `quimibond_sat`: CFDI y complementos de pago del SAT (Syntage) dentro de Odoo, comparación al centavo, alerta diaria. README propio.
- `qb_obligation`: obligaciones vivas (qué, quién, sobre qué documento, cómo se prueba, cuándo vence). Nacen del correo (memoria) o a mano, viven como **actividades nativas** de Odoo sobre su documento o contacto (hecha = acuse, cancelada = descarte, evidencia las cierra sola), escalan a Dirección y mandan un recordatorio diario. La cobranza NO vive aquí (v3.0.0). App Obligaciones (registro, métricas y configuración); el trabajo diario es en Actividades. API `create_candidate`. README propio.
- `qb_build_limpio`: corrige **en la base** lo que pinta de naranja el build de Odoo.sh (etiquetas duplicadas y dependencias no buscables de campos de Studio, vistas por defecto de Studio inválidas, grupos inexistentes en vistas, columnas requeridas sin NOT NULL). Corre solo al terminar cualquier instalación/actualización de módulos y a mano en Ajustes → Técnico → Build limpio. El único aviso que venía del repo (`qb.obligation` "Currency" ×2) se corrigió en código. README propio.
- Módulos de Consolti en la raíz del repo (venían solo en `qbtesting`; desde 2026-09-18 viven en `main`/`quimibond` y se instalan a mano desde Apps): `quimibond_ficha_tecnica_tela` (fichas técnicas de tejido y acabado, importación desde Excel), `quimibond_tintoreria_rendimiento` (capacidad por rendimiento y relación de baño por centro de trabajo de tintorería), `mantenimiento_surtido_refacciones` (refacciones en solicitudes de mantenimiento con surtido desde almacén). README propio en los dos primeros.
- `qb_memoria`: pestaña Memoria del contacto (Supabase). Desde 1.2.0 muestra la **ficha consolidada** (RPC `memoria_brief`): quién atiende a la empresa, hechos con vigencia y conversaciones resumidas por Claude con estado y pendientes. Además **dueños aprendidos**: cron nocturno que lee la vista `memoria_encargados` (buzón que atiende a cada empresa / área) y lo escribe en el contacto; personas detrás de buzones compartidos en Contactos → Configuración → Buzones (memoria). README propio.

## Modelos sincronizados (2)

| Metodo | Odoo Model | Supabase Table | Para qué |
|---|---|---|---|
| `_push_contacts` | res.partner (con email y rank, más los partners con facturas del último año) | contacts + companies (incluye RFC/vat, dominio, totales) | ligar remitentes y empresas de los correos con Odoo |
| `_push_users` | res.users + hr.employee | odoo_users | grafo nocturno de la memoria (`kg_refresh_deterministic`: quién atiende a quién) |

## Campos clave de Odoo

- **`commercial_partner_id`** = Empresa padre en Odoo → se resuelve via `_commercial_partner_id()` para linkear contactos a `companies`.
- **`vat`** = RFC fiscal → se guarda como `rfc` en companies.
- **`email`** del partner puede traer varios (separados por `;,`); cada uno es una fila de `contacts` y solo la primera lleva `odoo_partner_id`.

## Crons

- **Cada 1 hora:** `push_to_supabase()` — `contacts` y `users` (parámetro `quimibond_intelligence.push_models`, default `contacts,users`; `all` significa esos dos; cualquier otro nombre se ignora con aviso). Incremental por `write_date` (`last_sync_date`); `users` siempre completo. Para re-mandar todo una vez: `quimibond_intelligence.force_full_sync = 1`.
- **Cada 5 min:** `pull_from_supabase()` — comandos de `sync_commands` (`force_push`, `force_push_full`, `sync_contacts`) + contactos de Supabase sin `odoo_partner_id` → se crean en Odoo.
- Cada corrida del push escribe una fila por método en `pipeline_logs` (`phase='odoo_push'`); la vista `odoo_push_last_events` que lee el watchdog de la memoria sale de ahí.
- **Los crons se vuelven a encender en cada `odoo-update`** (`data/cleanup_2026_09_18.xml`). Odoo 19 apaga solo un cron tras 5 fallos en más de 7 días y no lo vuelve a encender; así murieron el push (3-jul-2026) y el pull (1-jun-2026) sin que nadie lo viera. Si el Historial de Sync (Ajustes → Técnico → Quimibond Sync) no muestra corridas de OdooBot en la última hora, el cron está apagado o fallando: revisar Acciones planificadas. `ir.cron` no está expuesto por MCP.

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

