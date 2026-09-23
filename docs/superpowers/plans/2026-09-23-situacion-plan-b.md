# Situación de la empresa — Plan B (correo diario, decisiones y delegación, app en Odoo) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Que el mapa de situación (plan A, en producción desde el 20-sep-2026) le llegue al director cada mañana por correo, que sus decisiones (delegar, ignorar, cerrar, reabrir, separar, fijar severidad) persistan y se ejecuten en Odoo como actividades nativas, y que pueda ver y delegar desde una app en Odoo; al final se retiran `email-digest` y `qb_obligation`.

**Architecture:** Todo lo determinístico sigue en SQL (Supabase): `situacion_cambios` es la única fuente del correo; `situacion_decidir` escribe reglas y estados y deja un comando en `sync_commands` con `payload`; el pull de Odoo (cada 5 min) crea la `mail.activity` y confirma por RPC; los hooks de `mail.activity` (hecha/cancelada) dejan eventos que el push horario manda como lote de la señal `delegacion_estado`, y el ciclo del bot los aplica. Odoo **no guarda** situaciones: la app `qb_situacion` las lee por REST en un modelo transitorio (como `qb_memoria` lee la memoria). Todo el código nuevo de Odoo vive en el módulo nuevo `qb_situacion` (manifest propio, con bump), no en `quimibond_intelligence` (manifest congelado, `tools/no_bump.txt`: agregar un modelo ahí es error del CI).

**Tech Stack:** Postgres/plpgsql en Supabase (migraciones idempotentes, pruebas en seco con `RAISE EXCEPTION 'PRUEBA_OK'`), Edge Functions en Deno (`_shared/env.ts`, `claude.ts`, `mailer.ts`), vitest para módulos puros, pg_cron; Odoo 19 (`TransactionCase`, `TransientModel`, `mail.activity`), `SupabaseClient` de `quimibond_intelligence`.

**Spec:** `docs/superpowers/specs/2026-09-18-situacion-empresa-design.md` §3.3, §3.4, §7 y §8 (pasos 4–6). **Plan A (hecho):** `docs/superpowers/plans/2026-09-18-situacion-plan-a.md`.

---

## Contexto para quien ejecuta (léelo antes de tocar nada)

- **Dos repos, misma rama de sesión.** Supabase: `quimibond/quimibond-intelligence` (`supabase/migrations`, `supabase/functions`, `supabase/tests/situacion`, `src/__tests__/pipeline`). Odoo: `quimibond/qb19` (`addons/*`). Flujo de cada parte: PR borrador → CI verde → ready → squash-merge → reconstruir la rama desde `origin/main`. En qb19, además, PR `main` → `quimibond` con merge commit y el CEO corre en Odoo.sh `odoo-update <módulos> && odoosh-restart http && odoosh-restart cron`.
- **Migraciones** se aplican en producción con el MCP de Supabase (`apply_migration`, nombre = nombre del archivo sin `.sql`); las Edge Functions con `deploy_edge_function` (`verify_jwt: false`, incluir cada archivo `_shared/*` que importe la función). **PostgREST carga `safeupdate`:** todo `UPDATE`/`DELETE` sin `WHERE` falla al llamarse por RPC aunque esté dentro de una función (también en tablas temporales; el `WHERE` de una subconsulta no cuenta). Los tests SQL corren como `postgres` y NO lo detectan: poner `WHERE true` cuando no hay condición.
- **Pruebas SQL en seco:** archivo en `supabase/tests/situacion/`, bloque `DO $t$ … RAISE EXCEPTION 'PRUEBA_OK'; END $t$;`. Se corre con `execute_sql`; el error esperado es `PRUEBA_OK` y deshace todo. Los ids de partner de prueba empiezan en `990000001`; la señal de prueba se llama `_prueba` y se inserta en `senales_config` dentro del bloque.
- **Odoo:** los tests de `TransactionCase` corren en el CI de qb19 (`--test-tags` por módulo, ver `.github/workflows/ci.yml`). No hay Odoo local: escribe el test, empuja, y lee el log del job `odoo-tests`. Trampas conocidas (plan A): `assertRaises` de Odoo envuelve el bloque en un savepoint y deshace lo escrito; un `write` pendiente del ORM se escribe encima de un `UPDATE` crudo si no haces `flush_recordset()` antes; `stock.move` ya no tiene `name`.
- **Secretos:** nunca en el repo, PR ni chat. Service key en `ir.config_parameter` (`quimibond_intelligence.supabase_url` / `supabase_service_key`), `cron_secret` y `anthropic_api_key` en Vault.
- **Hoy en producción (23-sep):** 18.7k señales, ~1,080 situaciones abiertas, bot a 40 por corrida con 4 llamadas paralelas, `situacion_respaldo` a `:20`, `memoria_email_digest` a las 12:45 UTC. **El push horario de Odoo corre una vez al día (04:48 UTC):** el registro `ir.cron` de producción es `noupdate` y conserva un intervalo viejo; la Tarea 5.9 lo corrige por código.

## Mapa de archivos

### quimibond-intelligence (Supabase)

| Archivo | Responsabilidad |
|---|---|
| `supabase/migrations/20260924a_situacion_cambios.sql` | `senales_config.en_mapa`; RPC `situacion_cambios(p_desde)`; `situacion_guardar` conserva `delegada`; `situacion_mapa` expone la delegación; tabla `situacion_digests` |
| `supabase/tests/situacion/05_cambios.sql` | Prueba en seco de `situacion_cambios` |
| `supabase/functions/_shared/email-html.ts` | Helpers HTML compartidos (`esc`, `inlineMd`, `mdToHtml`, `layout`) sacados de `digest-email-html.ts` |
| `supabase/functions/_shared/situacion-digest-html.ts` | Render puro del correo de situación a partir del JSON de `situacion_cambios` + narrativa |
| `src/__tests__/pipeline/situacion-digest-html.test.ts` | vitest del render |
| `tsconfig.json` | `allowImportingTsExtensions` (los `_shared` con imports `.ts` entran a `tsc` vía los tests) |
| `supabase/functions/situacion-digest/index.ts` | Edge Function: `situacion_cambios` → Opus (narrativa) → HTML → correo → `situacion_digests` |
| `supabase/migrations/20260924b_situacion_digest_cron.sql` | Job `situacion_digest` 12:30 UTC; desprograma `memoria_email_digest` |
| `supabase/functions/health/index.ts` | Vigila `situacion_digest` (diario) |
| `supabase/migrations/20260925a_situacion_decidir.sql` | `sync_commands.payload`, `situacion_decidir`, `situacion_delegacion_confirmar`, `situacion_delegaciones_abiertas`, `situacion_delegaciones_aplicar` (en `situacion_ciclo`), `situacion_redactar` respeta reglas fijas y `no_fusionar`, `situacion_guardar` marca delegaciones pendientes en error a los 15 min |
| `supabase/tests/situacion/06_decidir.sql` | Prueba en seco de decidir / confirmar / aplicar |
| `supabase/migrations/20260926a_retiro_email_digest.sql` | Desprograma y borra lo del digest viejo (RPCs `get_unanswered_client_threads`, `get_silent_customers`) |
| `supabase/migrations/20260926b_obligacion_legado_inactiva.sql` | `obligacion_legado` inactiva, cuando el CEO ya desinstaló `qb_obligation` (Tarea 6.5) |
| `CLAUDE.md` | Sección "Situación de la empresa": correo, decisiones, delegación; jobs; deuda |

### qb19 (Odoo)

| Archivo | Responsabilidad |
|---|---|
| `addons/quimibond_intelligence/models/sync_pull.py` | Lee `payload`, pasa `(command, payload)` a `_execute_command`, escribe `failed` (no `error`) — sin modelos nuevos, sin bump |
| `addons/quimibond_intelligence/models/sync_push.py` | `_push_metodos()`: la lista de métodos del push, extensible por `qb_situacion` |
| `addons/quimibond_intelligence/models/senales/direccion.py` | `obligacion_legado` devuelve `[]` (no `None`) cuando el modelo no está: último lote vacío antes de desinstalar |
| `addons/quimibond_intelligence/data/cleanup_2026_09_18.xml` | Además de encender los crons, fija el intervalo del push a 1 hora |
| `addons/qb_situacion/__manifest__.py` | Módulo nuevo `qb_situacion` 19.0.1.0.0 (paso 5) → 19.0.2.0.0 (paso 6); depende de `mail`, `quimibond_intelligence`, `qb_memoria` (cliente REST) |
| `addons/qb_situacion/models/mail_activity.py` | `mail.activity.situacion_id`; hooks `_action_done` / `unlink` → `qb.delegacion.evento` |
| `addons/qb_situacion/models/delegacion_evento.py` | Modelo `qb.delegacion.evento` (situacion_id, mail_activity_id, evento, feedback, user_id, fecha, enviado) |
| `addons/qb_situacion/models/sync_pull.py` | Comando `crear_actividad` (hereda `quimibond.sync.pull._execute_command`) + `situacion_delegacion_confirmar` |
| `addons/qb_situacion/models/sync_push.py` | `_push_actividades_delegadas` (hereda `quimibond.sync`) → lote `delegacion_estado` |
| `addons/qb_situacion/models/situacion_client.py` | Cliente REST de lectura/decisión (`situacion_mapa`, `situacion_contexto`, `situacion_decidir`) sobre `SupabaseClient` |
| `addons/qb_situacion/models/situacion.py` | `TransientModel` `qb.situacion` (mapa) y `qb.situacion.delegar` (wizard) |
| `addons/qb_situacion/views/*.xml`, `security/*` | App "Situación", grupo `group_ceo`, lista por área, ficha HTML, botón Delegar |
| `addons/qb_situacion/tests/*` | Tests de Odoo: hooks, comando, push, cliente con fake |
| `addons/qb_situacion/README.md`, `CLAUDE.md`, `docs/RUNBOOK_DESPLIEGUE.md` | Docs |

## Orden y dependencias

1. **Parte 1 (paso 4, Supabase):** Tareas 4.1–4.6. No depende de Odoo. Se acepta cuando el correo de la mañana siguiente coincide con `situacion_cambios`.
2. **Parte 2 (paso 5):** Tareas 5.1–5.3 (Supabase) primero; 5.4–5.10 (qb19) después, porque el pull necesita `sync_commands.payload` y los RPCs. Se acepta cuando delegar crea la actividad en ≤ 5 min y marcarla hecha cierra la situación.
3. **Parte 3 (paso 6, qb19 + retiro):** Tareas 6.1–6.6. La desinstalación de `qb_obligation` la hace el CEO en Apps; el borrado del código va en un PR posterior a esa desinstalación (una rama sin un módulo que producción SÍ tiene revienta el build).

**Cosas que hace el CEO** (no se automatizan): correr `odoo-update` tras cada merge a `quimibond`; leer el primer correo y dar el visto bueno; hacer la primera delegación real; desinstalar `qb_obligation` desde Apps; borrar del dashboard de Supabase las Edge Functions `email-digest`, `syntage-daily`, `syntage-webhook` y `query-intelligence`.

**Desviaciones conscientes respecto al spec (dilo así en los PRs y en CLAUDE.md):**
- §7.3 dice que los hooks de `mail.activity` y `_push_actividades_delegadas` viven en `quimibond_intelligence`. Van en **`qb_situacion`** (módulo nuevo con manifest propio) porque `quimibond_intelligence` no puede recibir modelos nuevos sin bump y su manifest está congelado. `quimibond_intelligence` solo cambia el pull (payload y `failed`) y `obligacion_legado`.
- §3.1/§4: la señal `delegacion_estado` **no crea situaciones propias** (`senales_config.en_mapa = false`); su estado se muestra en la situación delegada (`delegacion` jsonb, columnas nuevas de `situacion_mapa`).
- `situacion_decidir('separar')` deja `evidencia.no_fusionar` en la situación madre para que el bot no vuelva a fusionar lo que el CEO separó.
- **Cierre humano pegajoso** (el spec solo dice "hecha ⇒ resuelta"): `situacion_guardar` reabre las `resuelta` cuya señal sigue viva ("reapareció"), así que una actividad hecha con las facturas aún vencidas volvería a `abierta` en la misma corrida. Decisión: `hecha` y `resuelta` por el director dejan `evidencia.cerrada_manual = {n, valor, fecha, por}`; mientras la señal no crezca (más señales o valor > 105 %) la situación sigue `resuelta`; si crece, reabre como `empeoro` con historia "reapareció tras cierre manual" y la marca se quita. `reabrir` también la quita. `descartada` sigue como en plan A (nunca reabre sola).

---

## Parte 1 — Paso 4: `situacion_cambios` y el correo diario

### Task 4.1: RPC `situacion_cambios`, `delegada` pegajosa y delegación visible en el mapa

**Files:**
- Create: `supabase/migrations/20260924a_situacion_cambios.sql`
- Create: `supabase/tests/situacion/05_cambios.sql`

- [ ] **Step 1: Escribir la prueba en seco** (`supabase/tests/situacion/05_cambios.sql`):

```sql
-- Prueba en seco de situacion_cambios: nueva, empeorada, resuelta, delegada, grave, rezago, contadores. Termina en PRUEBA_OK.
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); sid bigint; sid2 bigint; sid3 bigint; a jsonb; uid int;
BEGIN
  SELECT odoo_user_id INTO uid FROM odoo_users ORDER BY odoo_user_id LIMIT 1;
  ASSERT uid IS NOT NULL, 'hace falta al menos un odoo_users';
  INSERT INTO senales_config (senal, titulo, area, tipo, fuente, agrupar_por, agregar, severidad_base, severidad_max)
  VALUES ('_prueba', 'Prueba', 'finanzas', 'credito', 'odoo', 'contraparte', 'suma', 4, 5);
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:990000003","odoo_partner_id":990000003,"valor":7,"documentos":[{"modelo":"account.move","id":13,"nombre":"F/3"}]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  SELECT id INTO sid  FROM situaciones WHERE clave = '_prueba|partner:990000001';
  SELECT id INTO sid2 FROM situaciones WHERE clave = '_prueba|partner:990000002';
  SELECT id INTO sid3 FROM situaciones WHERE clave = '_prueba|partner:990000003';

  -- sid2: delegada (a mano, como lo dejará situacion_decidir en el paso 5). sid3: vieja y sin cambio → rezago; además resuelta hoy.
  UPDATE situaciones SET estado = 'delegada', delegacion = jsonb_build_object('user_id', uid, 'fecha', now(), 'estado', 'creada', 'texto', 'cobrar'),
    historia = historia || jsonb_build_object('fecha', now(), 'evento', 'delegada', 'detalle', 'a Ana') WHERE id = sid2;
  UPDATE situaciones SET desde = current_date - 60, calidad = 'antigua' WHERE id = sid3;
  -- sid nació "ayer": así el empeoro de abajo cuenta como empeorada y no como nueva (una situación sale en una sola lista).
  UPDATE situaciones SET created_at = now() - interval '2 days', desde = current_date - 2 WHERE id = sid;
  -- sid: empeoró en una segunda corrida (más valor).
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000001b","odoo_partner_id":990000001,"valor":80,"documentos":[{"modelo":"account.move","id":14,"nombre":"F/4"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  ASSERT (SELECT estado FROM situaciones WHERE id = sid) = 'empeoro', 'sid empeoró';
  ASSERT (SELECT estado FROM situaciones WHERE id = sid2) = 'delegada', 'delegada se conserva cuando no cambia';
  ASSERT (SELECT estado FROM situaciones WHERE id = sid3) = 'resuelta', 'sid3 resuelta por evidencia';

  -- delegada pegajosa: si empeora, sigue delegada y queda en historia.
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000001b","odoo_partner_id":990000001,"valor":80,"documentos":[{"modelo":"account.move","id":14,"nombre":"F/4"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:990000002b","odoo_partner_id":990000002,"valor":500,"documentos":[{"modelo":"account.move","id":15,"nombre":"F/5"}]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  ASSERT (SELECT estado FROM situaciones WHERE id = sid2) = 'delegada', 'delegada pegajosa al empeorar';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT historia FROM situaciones WHERE id = sid2)) e WHERE e->>'evento' = 'empeoro'), 'historia registra el empeoro de la delegada';

  -- mapa: la delegada muestra a quién y en qué estado.
  -- límite alto: en producción hay ~1,000 situaciones y el mapa ordena por severidad; con el default (100) la fila de prueba puede quedar fuera.
  ASSERT (SELECT delegacion_estado FROM situacion_mapa('finanzas', 'viva', 1, 10000) WHERE id = sid2) = 'creada', 'mapa expone delegacion_estado';
  ASSERT (SELECT delegada_a FROM situacion_mapa('finanzas', 'viva', 1, 10000) WHERE id = sid2) IS NOT NULL, 'mapa expone delegada_a';

  -- cambios de las últimas 24 h.
  r := situacion_cambios(now() - interval '1 day');
  ASSERT r ? 'areas' AND r ? 'rezago' AND r ? 'ignoradas' AND r ? 'higiene' AND r ? 'salud' AND r ? 'totales', 'llaves: ' || (SELECT string_agg(k, ',') FROM jsonb_object_keys(r) k);
  SELECT x INTO a FROM jsonb_array_elements(r->'areas') x WHERE x->>'area' = 'finanzas';
  ASSERT a IS NOT NULL, 'área finanzas presente';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(a->'empeoradas') e WHERE (e->>'id')::bigint = sid), 'sid en empeoradas: ' || (a->'empeoradas')::text;
  ASSERT NOT EXISTS (SELECT 1 FROM jsonb_array_elements(a->'nuevas') e WHERE (e->>'id')::bigint = sid), 'sid NO en nuevas (nació hace 2 días; una situación sale en una sola lista)';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(a->'nuevas') e WHERE (e->>'id')::bigint = sid2) OR EXISTS (SELECT 1 FROM jsonb_array_elements(a->'delegadas') e WHERE (e->>'id')::bigint = sid2), 'sid2 sale (delegada hoy)';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(a->'delegadas') e WHERE (e->>'id')::bigint = sid2), 'sid2 en delegadas';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(a->'resueltas') e WHERE (e->>'id')::bigint = sid3), 'sid3 en resueltas';
  ASSERT (SELECT count(*) FROM jsonb_array_elements(a->'graves')) >= 0, 'graves es lista';
  ASSERT (r->'totales'->>'empeoradas')::int >= 1 AND (r->'totales'->>'delegadas')::int >= 1, 'totales: ' || (r->'totales')::text;
  -- cada fila trae lo que el correo imprime.
  ASSERT (SELECT e FROM jsonb_array_elements(a->'empeoradas') e WHERE (e->>'id')::bigint = sid) ?& ARRAY['titulo','contraparte','severidad','dias_abierta','responsable','recomendacion','ultimo_cambio','redactada'], 'campos de la fila';
  -- ventana vacía: nada.
  r := situacion_cambios(now() + interval '1 hour');
  ASSERT (r->'totales'->>'nuevas')::int = 0 AND (r->'totales'->>'empeoradas')::int = 0, 'ventana futura vacía';
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correrla y ver que falla** con `execute_sql` (contenido del archivo). Esperado: falla antes de `PRUEBA_OK`. Contra el `situacion_guardar` de hoy el primer error es el ASSERT `delegada pegajosa al empeorar` (hoy una delegada pasa a `empeoro`); si lo comentas, sigue `situacion_mapa` sin la columna `delegacion_estado`, y después `function situacion_cambios(timestamp with time zone) does not exist`. Los tres son lo que la migración arregla; la prueba no está mal.

- [ ] **Step 3: Escribir la migración** `supabase/migrations/20260924a_situacion_cambios.sql`:

```sql
-- 2026-09-24a — Situación plan B, paso 4: situacion_cambios (spec §7.1), delegada pegajosa en
-- situacion_guardar, delegación visible en situacion_mapa, tabla situacion_digests.
BEGIN;

-- 1. senales_config.en_mapa: señales que informan pero no forman situaciones (delegacion_estado, paso 5).
ALTER TABLE public.senales_config ADD COLUMN IF NOT EXISTS en_mapa boolean NOT NULL DEFAULT true;
UPDATE public.senales_config SET en_mapa = false WHERE senal = 'delegacion_estado';
COMMENT ON COLUMN public.senales_config.en_mapa IS 'false: la señal se ingiere y se ve en situacion_salud pero no agrupa situaciones (delegacion_estado se refleja en la situación delegada).';

-- 2. situacion_guardar: una situación delegada sigue delegada aunque empeore o mejore (la actividad vive en Odoo);
--    el cambio queda en historia y en ultimo_cambio. Solo cambia aquí el CASE del estado y el evento de historia.
--    (Cuerpo completo = 20260919h_situacion_safeupdate_guardar.sql con estas dos líneas cambiadas.)
CREATE OR REPLACE FUNCTION public.situacion_guardar(p_corrida uuid DEFAULT gen_random_uuid())
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  g record; sit record; n_nuevas int := 0; n_act int := 0; n_res int := 0; n_ign int := 0; n_sin int := 0;
  v_estado text; v_cambio text; v_evento jsonb; v_sin_datos text[];
BEGIN
  SELECT coalesce(array_agg(c.senal), '{}') INTO v_sin_datos
  FROM senales_config c
  WHERE c.activa AND EXISTS (SELECT 1 FROM senales s WHERE s.senal = c.senal AND s.resuelta_en IS NULL)
    AND NOT EXISTS (SELECT 1 FROM senales_lotes l WHERE l.senal = c.senal AND l.ok
                    AND l.recibido_en > now() - make_interval(hours => coalesce(c.sin_datos_horas, 2 * c.cada_horas)));
  n_sin := coalesce(array_length(v_sin_datos, 1), 0);
  SELECT count(*) INTO n_ign FROM senales WHERE resuelta_en IS NULL AND calidad = 'ignorada';

  DROP TABLE IF EXISTS _grupos;
  CREATE TEMP TABLE _grupos AS
  SELECT s.senal, c.area, c.tipo, c.titulo AS titulo_senal, c.severidad_base,
         CASE WHEN s.calidad IN ('zombie', 'dato_malo') THEN 'higiene:' || s.calidad ELSE s.agrupador END AS agrupador,
         count(*) AS n,
         CASE c.agregar WHEN 'cuenta' THEN count(*)::numeric WHEN 'maximo' THEN max(s.valor) ELSE coalesce(sum(s.valor), count(*)::numeric) END AS valor,
         min(s.primera_vista)::date AS desde, min(s.vence) AS vence, max(s.vista_en) AS vista_en,
         mode() WITHIN GROUP (ORDER BY s.company_id) AS company_id,
         mode() WITHIN GROUP (ORDER BY s.odoo_partner_id) AS odoo_partner_id,
         mode() WITHIN GROUP (ORDER BY s.responsable_odoo_user_id) AS responsable,
         (array_agg(s.calidad ORDER BY situacion_calidad_rango(s.calidad)))[1] AS calidad,
         jsonb_path_query_array(jsonb_agg(s.documentos), '$[*][*]') AS docs_flat,
         NULL::jsonb AS documentos, NULL::jsonb AS evidencia,
         jsonb_agg(s.clave ORDER BY s.clave) AS claves, max(s.episodio) AS episodio_max,
         string_agg(DISTINCT s.valor_texto, '; ') FILTER (WHERE s.valor_texto IS NOT NULL) AS valor_texto
  FROM senales s JOIN senales_config c ON c.senal = s.senal
  WHERE s.resuelta_en IS NULL AND s.calidad <> 'ignorada' AND NOT (s.senal = ANY (v_sin_datos))
    AND coalesce(c.en_mapa, true)                                   -- plan B: delegacion_estado no forma situaciones
  GROUP BY s.senal, c.area, c.tipo, c.titulo, c.severidad_base, c.agregar,
           CASE WHEN s.calidad IN ('zombie', 'dato_malo') THEN 'higiene:' || s.calidad ELSE s.agrupador END;
  UPDATE _grupos gr SET
    documentos = (SELECT coalesce(jsonb_agg(d), '[]') FROM (SELECT d FROM jsonb_array_elements(gr.docs_flat) d LIMIT 40) q),
    evidencia = jsonb_build_object(
      'senales', gr.claves,
      'threads', coalesce((SELECT jsonb_agg(DISTINCT (d->>'id')::bigint) FROM jsonb_array_elements(gr.docs_flat) d WHERE d->>'modelo' = 'thread'), '[]'::jsonb),
      'episodio_max', gr.episodio_max)
  WHERE true;

  FOR g IN SELECT * FROM _grupos LOOP
    SELECT * INTO sit FROM situaciones WHERE clave = g.senal || '|' || g.agrupador;
    IF NOT FOUND THEN
      INSERT INTO situaciones (clave, senal, agrupador, area, tipo, titulo, company_id, odoo_partner_id, documentos, evidencia,
                               responsable_sugerido_user_id, severidad, desde, vence, estado, calidad, n_senales, valor, valor_texto,
                               ultimo_cambio, historia)
      VALUES (g.senal || '|' || g.agrupador, g.senal, g.agrupador, g.area,
              CASE WHEN g.agrupador LIKE 'higiene:%' THEN 'higiene' ELSE g.tipo END,
              g.titulo_senal || ' · ' || situacion_nombre_agrupador(g.agrupador, g.company_id, g.odoo_partner_id, g.documentos),
              g.company_id, g.odoo_partner_id, g.documentos, g.evidencia, g.responsable, g.severidad_base, g.desde, g.vence,
              'abierta', g.calidad, g.n, g.valor, left(g.valor_texto, 600),
              'creada: ' || g.n || ' señal(es)',
              jsonb_build_array(jsonb_build_object('fecha', now(), 'evento', 'creada', 'detalle', g.n || ' señal(es), valor ' || coalesce(g.valor::text, '-'), 'corrida', p_corrida)));
      n_nuevas := n_nuevas + 1;
    ELSE
      v_estado := NULL; v_cambio := NULL;
      IF sit.estado IN ('resuelta', 'descartada') THEN
        IF sit.estado = 'descartada' THEN CONTINUE; END IF;
        -- plan B: un cierre humano (hecha por el delegado, o resuelta por el director) es pegajoso mientras la señal
        -- no crezca (evidencia.cerrada_manual guarda n y valor al cerrar). Si crece, reabre como 'empeoro' y la marca se quita.
        IF sit.evidencia ? 'cerrada_manual' THEN
          IF NOT (g.n > coalesce((sit.evidencia->'cerrada_manual'->>'n')::int, 0)
                  OR (g.valor IS NOT NULL AND (sit.evidencia->'cerrada_manual'->>'valor') IS NOT NULL
                      AND g.valor > (sit.evidencia->'cerrada_manual'->>'valor')::numeric * 1.05)) THEN
            CONTINUE;
          END IF;
          v_estado := 'empeoro';
          v_cambio := format('reapareció tras cierre manual: empeoró %s → %s documentos, valor %s → %s',
                             sit.evidencia->'cerrada_manual'->>'n', g.n, coalesce(sit.evidencia->'cerrada_manual'->>'valor', '-'), coalesce(g.valor::text, '-'));
        ELSE
          v_estado := 'abierta'; v_cambio := 'reapareció: ' || g.n || ' señal(es), valor ' || coalesce(g.valor::text, '-');
        END IF;
      ELSIF g.n > sit.n_senales OR (g.valor IS NOT NULL AND sit.valor IS NOT NULL AND g.valor > sit.valor * 1.05) THEN
        v_estado := 'empeoro'; v_cambio := format('empeoró: %s → %s documentos, valor %s → %s', sit.n_senales, g.n, coalesce(sit.valor::text, '-'), coalesce(g.valor::text, '-'));
      ELSIF g.n < sit.n_senales OR (g.valor IS NOT NULL AND sit.valor IS NOT NULL AND g.valor < sit.valor * 0.95) THEN
        v_estado := 'mejoro'; v_cambio := format('mejoró: %s → %s documentos, valor %s → %s', sit.n_senales, g.n, coalesce(sit.valor::text, '-'), coalesce(g.valor::text, '-'));
      END IF;
      UPDATE situaciones SET
        documentos = g.documentos,
        evidencia = CASE WHEN sit.estado = 'resuelta' THEN (sit.evidencia || g.evidencia) - 'cerrada_manual' ELSE sit.evidencia || g.evidencia END,
        calidad = g.calidad, n_senales = g.n, valor = g.valor,
        valor_texto = left(g.valor_texto, 600), vence = g.vence, company_id = coalesce(g.company_id, sit.company_id),
        odoo_partner_id = coalesce(g.odoo_partner_id, sit.odoo_partner_id),
        responsable_sugerido_user_id = coalesce(sit.responsable_sugerido_user_id, g.responsable),
        -- plan B: delegada es pegajosa (la actividad vive en Odoo); el cambio se ve en historia y ultimo_cambio.
        estado = CASE WHEN sit.estado = 'delegada' AND v_estado IN ('empeoro', 'mejoro') THEN 'delegada' ELSE coalesce(v_estado, sit.estado) END,
        resuelta_en = CASE WHEN sit.estado = 'resuelta' THEN NULL ELSE sit.resuelta_en END,
        version = CASE WHEN v_estado IS NOT NULL THEN sit.version + 1 ELSE sit.version END,
        ultimo_cambio = coalesce(v_cambio, sit.ultimo_cambio),
        ultimo_cambio_en = CASE WHEN v_estado IS NOT NULL THEN now() ELSE sit.ultimo_cambio_en END,
        historia = CASE WHEN v_estado IS NOT NULL THEN sit.historia || jsonb_build_object('fecha', now(), 'evento', v_estado, 'detalle', v_cambio || CASE WHEN sit.estado = 'delegada' THEN ' (sigue delegada)' ELSE '' END, 'corrida', p_corrida) ELSE sit.historia END,
        updated_at = now()
      WHERE id = sit.id;
      IF v_estado IS NOT NULL THEN n_act := n_act + 1; END IF;
    END IF;
  END LOOP;

  UPDATE situaciones s SET
    estado = 'resuelta', resuelta_en = now(), version = s.version + 1,
    ultimo_cambio = 'resuelta por evidencia: la señal ' || s.senal || ' desapareció', ultimo_cambio_en = now(),
    historia = s.historia || jsonb_build_object('fecha', now(), 'evento', 'resuelta', 'detalle', 'por evidencia: ' || s.senal || ' desapareció', 'corrida', p_corrida),
    updated_at = now()
  WHERE s.estado NOT IN ('resuelta', 'descartada')
    AND NOT (s.senal = ANY (v_sin_datos))
    AND NOT EXISTS (SELECT 1 FROM _grupos gr WHERE gr.senal || '|' || gr.agrupador = s.clave);
  GET DIAGNOSTICS n_res = ROW_COUNT;
  DROP TABLE IF EXISTS _grupos;

  RETURN jsonb_build_object('nuevas', n_nuevas, 'actualizadas', n_act, 'resueltas', n_res, 'ignoradas', n_ign,
                            'sin_datos', to_jsonb(v_sin_datos), 'corrida', p_corrida);
END $$;

-- 3. situacion_mapa: delegación visible. Misma firma + dos columnas al final.
DROP FUNCTION IF EXISTS public.situacion_mapa(text, text, integer, integer);
CREATE OR REPLACE FUNCTION public.situacion_mapa(p_area text DEFAULT NULL, p_calidad text DEFAULT 'viva', p_min_severidad integer DEFAULT 1, p_limit integer DEFAULT 100)
RETURNS TABLE (id bigint, area text, tipo text, senal text, titulo text, severidad smallint, estado text, calidad text,
               contraparte text, responsable text, responsable_user_id integer, dias_abierta integer, dias_sin_cambio integer,
               ultimo_cambio text, n_documentos integer, valor numeric, valor_texto text, vence date, redactada boolean, recomendacion text,
               delegada_a text, delegacion_estado text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT s.id, s.area, s.tipo, s.senal, s.titulo, s.severidad, s.estado, s.calidad,
         coalesce(c.name, situacion_nombre_agrupador(s.agrupador, s.company_id, s.odoo_partner_id, s.documentos)) AS contraparte,
         u.name AS responsable, s.responsable_sugerido_user_id,
         (current_date - s.desde) AS dias_abierta,
         extract(day FROM now() - s.ultimo_cambio_en)::int AS dias_sin_cambio,
         s.ultimo_cambio, jsonb_array_length(s.documentos) AS n_documentos, s.valor, s.valor_texto, s.vence,
         (s.ia_version >= s.version) AS redactada, s.recomendacion,
         d.name AS delegada_a, s.delegacion->>'estado' AS delegacion_estado
  FROM situaciones s
  LEFT JOIN companies c ON c.id = s.company_id
  LEFT JOIN odoo_users u ON u.odoo_user_id = s.responsable_sugerido_user_id
  LEFT JOIN odoo_users d ON d.odoo_user_id = (s.delegacion->>'user_id')::int
  WHERE s.estado NOT IN ('resuelta', 'descartada') AND s.fusionada_en IS NULL
    AND (p_area IS NULL OR s.area = p_area)
    AND (p_calidad IS NULL OR s.calidad = p_calidad)
    AND s.severidad >= coalesce(p_min_severidad, 1)
  ORDER BY s.severidad DESC, s.ultimo_cambio_en DESC
  LIMIT greatest(coalesce(p_limit, 100), 1)
$$;
COMMENT ON FUNCTION public.situacion_mapa(text, text, integer, integer) IS 'El mapa (spec §7.1): situaciones abiertas por área, calidad (default viva; NULL = todas) y severidad mínima, con a quién está delegada y en qué estado. Ejemplo MCP: select * from situacion_mapa(''finanzas'').';

-- 4. situacion_cambios: lo que cambió desde p_desde, por área, más lo grave que sigue abierto, el rezago y los contadores.
--    Es la ÚNICA fuente del correo diario (situacion-digest) para que rutina y correo digan lo mismo.
--    Una situación aparece en una sola lista, en este orden de prioridad: resueltas, delegadas, empeoradas, mejoradas, nuevas, graves.
--    VOLATILE a propósito: crea una tabla temporal (plpgsql no permite DROP/CREATE TABLE en funciones STABLE).
CREATE OR REPLACE FUNCTION public.situacion_cambios(p_desde timestamptz DEFAULT now() - interval '24 hours')
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE out jsonb; v_hasta timestamptz := now(); v_areas jsonb := '[]'; a record; v_area jsonb; l text;
BEGIN
  DROP TABLE IF EXISTS _cambios;
  CREATE TEMP TABLE _cambios AS
  SELECT s.id, s.area, s.tipo, s.senal, s.titulo, s.severidad, s.estado, s.calidad,
         coalesce(c.name, situacion_nombre_agrupador(s.agrupador, s.company_id, s.odoo_partner_id, s.documentos)) AS contraparte,
         u.name AS responsable, d.name AS delegada_a, s.delegacion->>'estado' AS delegacion_estado,
         (current_date - s.desde) AS dias_abierta, s.recomendacion, s.ultimo_cambio, s.valor_texto,
         (s.ia_version >= s.version) AS redactada, s.ultimo_cambio_en, s.resuelta_en,
         CASE
           WHEN s.estado = 'resuelta' AND s.resuelta_en >= p_desde THEN 'resueltas'
           WHEN s.estado = 'delegada' AND (s.delegacion->>'fecha')::timestamptz >= p_desde THEN 'delegadas'
           WHEN s.estado IN ('empeoro', 'delegada') AND s.ultimo_cambio_en >= p_desde AND s.ultimo_cambio LIKE 'empeor%' THEN 'empeoradas'
           WHEN s.estado IN ('mejoro', 'delegada') AND s.ultimo_cambio_en >= p_desde AND s.ultimo_cambio LIKE 'mejor%' THEN 'mejoradas'
           WHEN s.estado IN ('abierta', 'empeoro', 'mejoro', 'delegada') AND s.created_at >= p_desde THEN 'nuevas'
           WHEN s.estado IN ('abierta', 'empeoro', 'mejoro', 'delegada') AND s.severidad >= 4 AND s.calidad = 'viva' THEN 'graves'
           ELSE NULL END AS lista
  FROM situaciones s
  LEFT JOIN companies c ON c.id = s.company_id
  LEFT JOIN odoo_users u ON u.odoo_user_id = s.responsable_sugerido_user_id
  LEFT JOIN odoo_users d ON d.odoo_user_id = (s.delegacion->>'user_id')::int
  WHERE s.fusionada_en IS NULL AND s.tipo <> 'higiene' AND s.calidad IN ('viva', 'antigua', 'vencida_memoria')
    AND (s.estado NOT IN ('resuelta', 'descartada') OR s.resuelta_en >= p_desde);

  FOR a IN SELECT DISTINCT area FROM _cambios WHERE lista IS NOT NULL ORDER BY area LOOP
    v_area := jsonb_build_object('area', a.area, 'abiertas', (SELECT count(*) FROM _cambios x WHERE x.area = a.area AND x.estado NOT IN ('resuelta', 'descartada')));
    FOREACH l IN ARRAY ARRAY['nuevas', 'empeoradas', 'mejoradas', 'resueltas', 'delegadas', 'graves'] LOOP
      v_area := v_area || jsonb_build_object(l, (
        SELECT coalesce(jsonb_agg(jsonb_build_object('id', x.id, 'titulo', x.titulo, 'senal', x.senal, 'tipo', x.tipo, 'contraparte', x.contraparte,
                 'severidad', x.severidad, 'estado', x.estado, 'calidad', x.calidad, 'dias_abierta', x.dias_abierta, 'responsable', x.responsable,
                 'delegada_a', x.delegada_a, 'delegacion_estado', x.delegacion_estado, 'recomendacion', x.recomendacion, 'ultimo_cambio', x.ultimo_cambio,
                 'valor_texto', x.valor_texto, 'redactada', x.redactada) ORDER BY x.severidad DESC, x.dias_abierta DESC), '[]')
        FROM (SELECT * FROM _cambios x WHERE x.area = a.area AND x.lista = l ORDER BY x.severidad DESC, x.dias_abierta DESC LIMIT 25) x));
    END LOOP;
    v_areas := v_areas || v_area;
  END LOOP;

  SELECT jsonb_build_object(
    'desde', p_desde, 'hasta', v_hasta, 'areas', v_areas,
    'rezago', (SELECT coalesce(jsonb_agg(jsonb_build_object('id', x.id, 'area', x.area, 'titulo', x.titulo, 'contraparte', x.contraparte, 'severidad', x.severidad,
                 'dias_abierta', x.dias_abierta, 'responsable', x.responsable, 'recomendacion', x.recomendacion, 'ultimo_cambio', x.ultimo_cambio) ORDER BY x.dias_abierta DESC), '[]')
               FROM (SELECT * FROM _cambios WHERE calidad = 'antigua' AND estado NOT IN ('resuelta', 'descartada') ORDER BY dias_abierta DESC LIMIT 25) x),
    'ignoradas', (SELECT count(*) FROM senales WHERE resuelta_en IS NULL AND calidad = 'ignorada'),
    'reglas_vigentes', (SELECT count(*) FROM situacion_reglas WHERE vigente_hasta IS NULL OR vigente_hasta > now()),
    'higiene', jsonb_build_object(
      'zombie', (SELECT count(*) FROM senales WHERE resuelta_en IS NULL AND calidad = 'zombie'),
      'dato_malo', (SELECT count(*) FROM senales WHERE resuelta_en IS NULL AND calidad = 'dato_malo')),
    'salud', jsonb_build_object(
      'odoo_push_edad_h', (SELECT round(extract(epoch FROM now() - max(created_at)) / 3600, 1) FROM odoo_push_last_events WHERE method = 'senales' AND status = 'success'),
      'bot_terminada_en', (SELECT max(terminada_en) FROM situacion_corridas),
      'sin_datos', (SELECT coalesce(jsonb_agg(c.senal), '[]') FROM senales_config c WHERE c.activa AND c.fuente = 'odoo'
                    AND EXISTS (SELECT 1 FROM senales s WHERE s.senal = c.senal AND s.resuelta_en IS NULL)
                    AND NOT EXISTS (SELECT 1 FROM senales_lotes l WHERE l.senal = c.senal AND l.ok AND l.recibido_en > now() - make_interval(hours => coalesce(c.sin_datos_horas, 2 * c.cada_horas))))),
    'totales', (SELECT jsonb_build_object(
      'nuevas', count(*) FILTER (WHERE lista = 'nuevas'), 'empeoradas', count(*) FILTER (WHERE lista = 'empeoradas'),
      'mejoradas', count(*) FILTER (WHERE lista = 'mejoradas'), 'resueltas', count(*) FILTER (WHERE lista = 'resueltas'),
      'delegadas', count(*) FILTER (WHERE lista = 'delegadas'), 'graves', count(*) FILTER (WHERE lista = 'graves'),
      'abiertas', count(*) FILTER (WHERE estado NOT IN ('resuelta', 'descartada')),
      'rezago', count(*) FILTER (WHERE calidad = 'antigua' AND estado NOT IN ('resuelta', 'descartada')))
      FROM _cambios)
  ) INTO out;
  DROP TABLE IF EXISTS _cambios;
  RETURN out;
END $$;
COMMENT ON FUNCTION public.situacion_cambios(timestamptz) IS 'Lo que cambió desde p_desde (spec §7.1): por área nuevas, empeoradas, mejoradas, resueltas, delegadas y lo grave (sev ≥ 4) que sigue abierto; rezago (antiguas); ignoradas, reglas vigentes, higiene, salud y totales. Única fuente del correo diario. Ejemplo MCP: select situacion_cambios(now() - interval ''1 day'').';

-- 5. Bitácora del correo diario de situación (sustituye a email_digests para este correo).
CREATE TABLE IF NOT EXISTS public.situacion_digests (
  id            bigserial PRIMARY KEY,
  fecha         date NOT NULL,
  desde         timestamptz NOT NULL,
  hasta         timestamptz NOT NULL,
  cambios       jsonb NOT NULL,
  narrativa_md  text,
  emailed       boolean NOT NULL DEFAULT false,
  email_error   text,
  trigger       text NOT NULL DEFAULT 'cron',
  modelo        text,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS situacion_digests_fecha_idx ON public.situacion_digests (fecha DESC);
REVOKE ALL ON public.situacion_digests FROM public, anon, authenticated;

INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Situación plan B paso 4: situacion_cambios, delegada pegajosa, cierre humano pegajoso (evidencia.cerrada_manual), delegación en situacion_mapa, en_mapa, situacion_digests',
        jsonb_build_object('migration', '20260924a_situacion_cambios'));
COMMIT;
```

**Nota para el ejecutor:** `situacion_cambios` va `VOLATILE` porque crea y borra una tabla temporal; no la declares `STABLE` (plpgsql corre las funciones no volátiles en modo solo lectura y falla con "DROP TABLE is not allowed in a non-volatile function"). Los helpers `situacion_cambios_fila`/`_lista` no existen a propósito: una función no puede declararse sobre el tipo de una tabla temporal de otra sesión.

- [ ] **Step 4: Aplicar la migración en producción** con `apply_migration` (`name: 20260924a_situacion_cambios`). Luego correr `05_cambios.sql` y también `04_lectura.sql` (regresión del mapa). Esperado en ambos: `PRUEBA_OK`.

- [ ] **Step 5: Probar con datos reales por MCP:** `select situacion_cambios(now() - interval '1 day');` — debe devolver áreas con listas, `totales` coherentes con `select estado, count(*) from situaciones where fusionada_en is null and updated_at > now() - interval '1 day' group by 1`, y `select * from situacion_mapa('finanzas', 'viva', 3, 5)` con las dos columnas nuevas (NULL hoy: nadie ha delegado).

- [ ] **Step 6: Commit** — "Situación: situacion_cambios, delegada pegajosa y delegación en el mapa (plan B, paso 4)".


### Task 4.2: Helpers HTML compartidos y render puro del correo de situación

**Files:**
- Create: `supabase/functions/_shared/email-html.ts`
- Create: `supabase/functions/_shared/situacion-digest-html.ts`
- Create: `supabase/functions/situacion-digest/prompt.ts`
- Test: `src/__tests__/pipeline/situacion-digest-html.test.ts`

- [ ] **Step 1: Escribir el test** (`src/__tests__/pipeline/situacion-digest-html.test.ts`):

```ts
import { describe, it, expect } from "vitest";
import { renderSituacionDigestHtml, renderSituacionDigestText, type Cambios } from "../../../supabase/functions/_shared/situacion-digest-html";
import { mdToHtml, esc } from "../../../supabase/functions/_shared/email-html";
import { SYSTEM, entradaParaClaude } from "../../../supabase/functions/situacion-digest/prompt";

const item = (id: number, extra: Partial<Cambios["areas"][0]["nuevas"][0]> = {}) => ({
  id, titulo: `Cartera vencida · ACME ${id}`, senal: "cartera_vencida", tipo: "credito", contraparte: "ACME", severidad: 4, estado: "abierta", calidad: "viva",
  dias_abierta: 12, responsable: "Ana", delegada_a: null, delegacion_estado: null, recomendacion: "Cobrar F/1 <hoy>", ultimo_cambio: "empeoró: 5 → 7 documentos",
  valor_texto: "7 facturas", redactada: true, ...extra,
});
const cambios: Cambios = {
  desde: "2026-09-23T12:30:00Z", hasta: "2026-09-24T12:30:00Z",
  areas: [
    { area: "finanzas", abiertas: 40, nuevas: [item(1)], empeoradas: [item(2)], mejoradas: [], resueltas: [item(3, { estado: "resuelta" })], delegadas: [item(4, { estado: "delegada", delegada_a: "Luis", delegacion_estado: "creada" })], graves: [item(5, { redactada: false })] },
    { area: "comercial", abiertas: 3, nuevas: [], empeoradas: [], mejoradas: [item(6)], resueltas: [], delegadas: [], graves: [] },
  ],
  rezago: [{ ...item(7), area: "compras", dias_abierta: 80 }],
  ignoradas: 12, reglas_vigentes: 3, higiene: { zombie: 1500, dato_malo: 4 },
  salud: { odoo_push_edad_h: 0.7, bot_terminada_en: "2026-09-24T12:20:00Z", sin_datos: [] },
  totales: { nuevas: 1, empeoradas: 1, mejoradas: 1, resueltas: 1, delegadas: 1, graves: 1, abiertas: 43, rezago: 1 },
};

describe("situacion-digest-html", () => {
  it("imprime cada lista con título, contraparte, días, responsable y recomendación, escapando HTML", () => {
    const html = renderSituacionDigestHtml({ dateLabel: "miércoles 24 de septiembre de 2026", narrativaMd: "## Lo que decidiría hoy\n- Cobrar a **ACME**", cambios, esLunes: false });
    expect(html).toContain("Cartera vencida · ACME 1");
    expect(html).toContain("Cobrar F/1 &lt;hoy&gt;");
    expect(html).toContain("Luis");                       // delegada a
    expect(html).toContain("<strong>ACME</strong>");      // narrativa en HTML
    expect(html).toContain("12 ignoradas");
    expect(html).toContain("1,500");                      // higiene con separador de miles
    expect(html).not.toContain("Rezago");                 // solo lunes
    expect(html).toContain("sin redactar");               // graves sin redacción se marcan
  });
  it("los lunes agrega el bloque de rezago y el texto plano dice lo mismo", () => {
    const html = renderSituacionDigestHtml({ dateLabel: "lunes", narrativaMd: "", cambios, esLunes: true });
    expect(html).toContain("Rezago");
    expect(html).toContain("80 días");
    const txt = renderSituacionDigestText({ dateLabel: "lunes", narrativaMd: "", cambios, esLunes: true });
    expect(txt).toContain("Cartera vencida · ACME 1");
    expect(txt).toContain("Rezago");
    expect(txt).toContain("Cobrar F/1 <hoy>");   // texto plano: sin escapar
    expect(txt).not.toContain("&lt;");
  });
  it("un día sin cambios lo dice en una línea", () => {
    const vacio: Cambios = { ...cambios, areas: [], rezago: [], totales: { nuevas: 0, empeoradas: 0, mejoradas: 0, resueltas: 0, delegadas: 0, graves: 0, abiertas: 43, rezago: 0 } };
    const html = renderSituacionDigestHtml({ dateLabel: "x", narrativaMd: "", cambios: vacio, esLunes: false });
    expect(html).toContain("Sin cambios");
  });
});

describe("email-html", () => {
  it("mdToHtml: encabezados, bullets, negritas y escape", () => {
    const html = mdToHtml("## Título\n- uno **fuerte** <b>\n- dos\n\nPárrafo");
    expect(html).toContain("<h2");
    expect(html).toContain("<li>uno <strong>fuerte</strong> &lt;b&gt;</li>");
    expect(html).toContain("<p");
    expect(esc('a<b>&"')).toBe("a&lt;b&gt;&amp;&quot;");
  });
});

describe("situacion-digest/prompt", () => {
  it("la entrada para Claude cabe en el presupuesto y conserva ids y títulos", () => {
    const grande: Cambios = { ...cambios, areas: cambios.areas.map((a) => ({ ...a, graves: Array.from({ length: 200 }, (_, i) => item(1000 + i, { recomendacion: "x".repeat(400) })) })) };
    const txt = entradaParaClaude(grande, 20_000);
    expect(txt.length).toBeLessThanOrEqual(20_500);
    expect(txt).toContain("Cartera vencida · ACME 1");
    expect(txt).toContain('"id":1');
    expect(SYSTEM.toLowerCase()).toContain("json");
    expect(SYSTEM).toContain("situacion_cambios");
  });
});
```

- [ ] **Step 2: Correr y ver que falla:** `cd /home/user/quimibond-intelligence && npx vitest run src/__tests__/pipeline/situacion-digest-html.test.ts`. Esperado: FAIL, "Cannot find module … situacion-digest-html".

- [ ] **Step 3: Escribir `supabase/functions/_shared/email-html.ts`:**

```ts
/**
 * Helpers HTML para correos (Gmail, Outlook, Apple Mail): escape, markdown mínimo
 * (##, -, **, párrafos) y el layout de 600 px con estilos inline. Puro: sin Deno.
 */
export const FONT = "-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif";
export const C = {
  bg: "#f1f3f6", card: "#ffffff", ink: "#111827", body: "#374151", muted: "#6b7280", faint: "#9ca3af",
  line: "#e5e7eb", lineSoft: "#f3f4f6", accent: "#2563eb", dangerBg: "#fef2f2", dangerInk: "#b91c1c", warnBg: "#fffbeb", warnInk: "#b45309",
  okBg: "#ecfdf5", okInk: "#047857",
};

export function esc(s: string): string {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/** Negritas e itálicas dentro de una línea ya escapada. */
export function inlineMd(s: string): string {
  return esc(s).replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>").replace(/(^|[^*])\*([^*]+)\*/g, "$1<em>$2</em>");
}

/** Markdown mínimo → HTML de correo: `## `, `### `, `- ` y párrafos. Todo lo demás se trata como texto. */
export function mdToHtml(md: string): string {
  const out: string[] = [];
  let enLista = false;
  const cerrar = () => { if (enLista) { out.push("</ul>"); enLista = false; } };
  for (const raw of (md ?? "").split(/\r?\n/)) {
    const line = raw.trimEnd();
    if (!line.trim()) { cerrar(); continue; }
    const h = /^(#{1,3})\s+(.*)$/.exec(line);
    if (h) {
      cerrar();
      const tag = h[1].length === 1 ? "h1" : h[1].length === 2 ? "h2" : "h3";
      const size = tag === "h1" ? 20 : tag === "h2" ? 16 : 14;
      out.push(`<${tag} style="margin:18px 0 8px;font:600 ${size}px/1.3 ${FONT};color:${C.ink}">${inlineMd(h[2])}</${tag}>`);
      continue;
    }
    const li = /^[-*]\s+(.*)$/.exec(line);
    if (li) {
      if (!enLista) { out.push(`<ul style="margin:6px 0 10px 20px;padding:0;font:14px/1.5 ${FONT};color:${C.body}">`); enLista = true; }
      out.push(`<li>${inlineMd(li[1])}</li>`);
      continue;
    }
    cerrar();
    out.push(`<p style="margin:6px 0;font:14px/1.5 ${FONT};color:${C.body}">${inlineMd(line)}</p>`);
  }
  cerrar();
  return out.join("\n");
}

/** Marco del correo: fondo gris, tarjeta blanca de 600 px, pie. */
export function layout(title: string, bodyHtml: string, footerHtml = ""): string {
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${esc(title)}</title></head>
<body style="margin:0;padding:0;background:${C.bg}">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${C.bg}"><tr><td align="center" style="padding:24px 12px">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:${C.card};border-radius:10px;border:1px solid ${C.line}">
<tr><td style="padding:24px 28px">${bodyHtml}</td></tr>
${footerHtml ? `<tr><td style="padding:14px 28px;border-top:1px solid ${C.lineSoft};font:12px/1.5 ${FONT};color:${C.faint}">${footerHtml}</td></tr>` : ""}
</table></td></tr></table></body></html>`;
}

export function fmtInt(n: number | null | undefined): string {
  return Number(n ?? 0).toLocaleString("en-US");
}
```

- [ ] **Step 4: Escribir `supabase/functions/_shared/situacion-digest-html.ts`:**

```ts
/**
 * Render del correo diario "Situación" a partir del JSON de situacion_cambios (única fuente,
 * spec §7.2) y la narrativa de Claude. Determinístico: lo que sale aquí es lo que devuelve la RPC.
 */
import { C, FONT, esc, fmtInt, layout, mdToHtml } from "./email-html.ts";

export interface CambiosItem {
  id: number; titulo: string; senal?: string; tipo?: string; contraparte?: string | null; severidad: number; estado: string; calidad?: string;
  dias_abierta: number; responsable?: string | null; delegada_a?: string | null; delegacion_estado?: string | null;
  recomendacion?: string | null; ultimo_cambio?: string | null; valor_texto?: string | null; redactada: boolean;
}
export interface CambiosArea {
  area: string; abiertas: number;
  nuevas: CambiosItem[]; empeoradas: CambiosItem[]; mejoradas: CambiosItem[]; resueltas: CambiosItem[]; delegadas: CambiosItem[]; graves: CambiosItem[];
}
export interface Cambios {
  desde: string; hasta: string; areas: CambiosArea[]; rezago: (CambiosItem & { area: string })[];
  ignoradas: number; reglas_vigentes: number; higiene: { zombie: number; dato_malo: number };
  salud: { odoo_push_edad_h: number | null; bot_terminada_en: string | null; sin_datos: string[] };
  totales: Record<string, number>;
}
export interface DigestInput { dateLabel: string; narrativaMd: string; cambios: Cambios; esLunes: boolean }

const AREAS: Record<string, string> = { finanzas: "Finanzas", comercial: "Comercial", operaciones: "Operaciones", compras: "Compras", calidad_sgi: "Calidad / SGI", rh: "RH", sistemas: "Sistemas", direccion: "Dirección" };
const LISTAS: [keyof CambiosArea, string][] = [["empeoradas", "Empeoraron"], ["nuevas", "Nuevas"], ["graves", "Graves que siguen abiertas"], ["delegadas", "Delegadas"], ["mejoradas", "Mejoraron"], ["resueltas", "Resueltas"]];

const sevColor = (s: number) => (s >= 5 ? C.dangerInk : s === 4 ? C.warnInk : C.muted);
const sinCambios = (c: Cambios) => ["nuevas", "empeoradas", "mejoradas", "resueltas", "delegadas", "graves"].every((k) => !(c.totales?.[k] ?? 0));

function linea(x: CambiosItem): string {
  const meta = [x.contraparte, `${x.dias_abierta} días`, x.delegada_a ? `delegada a ${x.delegada_a}${x.delegacion_estado && x.delegacion_estado !== "creada" ? ` (${x.delegacion_estado})` : ""}` : x.responsable ? `→ ${x.responsable}` : null]
    .filter(Boolean).map((s) => esc(String(s))).join(" · ");
  const rec = x.redactada ? (x.recomendacion ? `<div style="color:${C.body};margin-top:2px">${esc(x.recomendacion)}</div>` : "")
    : `<div style="color:${C.faint};margin-top:2px"><em>sin redactar aún</em>${x.valor_texto ? ` · ${esc(x.valor_texto)}` : ""}</div>`;
  return `<tr><td style="padding:6px 0;border-bottom:1px solid ${C.lineSoft};font:13px/1.45 ${FONT};color:${C.ink}">
    <span style="display:inline-block;min-width:18px;font-weight:700;color:${sevColor(x.severidad)}">${x.severidad}</span> <strong>${esc(x.titulo)}</strong>
    <div style="color:${C.muted};font-size:12px">${meta}${x.ultimo_cambio ? ` · ${esc(x.ultimo_cambio)}` : ""}</div>${rec}</td></tr>`;
}

function bloqueArea(a: CambiosArea): string {
  const partes = LISTAS.filter(([k]) => (a[k] as CambiosItem[]).length).map(([k, titulo]) =>
    `<div style="margin:10px 0 2px;font:600 12px/1.3 ${FONT};color:${C.muted};text-transform:uppercase;letter-spacing:.04em">${titulo} (${(a[k] as CambiosItem[]).length})</div>
     <table role="presentation" width="100%" cellpadding="0" cellspacing="0">${(a[k] as CambiosItem[]).map(linea).join("")}</table>`);
  if (!partes.length) return "";
  return `<h2 style="margin:22px 0 4px;font:600 16px/1.3 ${FONT};color:${C.ink}">${esc(AREAS[a.area] ?? a.area)} <span style="font-weight:400;color:${C.faint};font-size:13px">· ${fmtInt(a.abiertas)} abiertas</span></h2>${partes.join("")}`;
}

function stat(n: number, label: string, tono: "danger" | "warn" | "ok" | "plain" = "plain"): string {
  const ink = tono === "danger" ? C.dangerInk : tono === "warn" ? C.warnInk : tono === "ok" ? C.okInk : C.ink;
  return `<td align="center" style="padding:8px 4px"><div style="font:700 20px/1 ${FONT};color:${ink}">${fmtInt(n)}</div><div style="font:11px/1.3 ${FONT};color:${C.muted};margin-top:3px">${esc(label)}</div></td>`;
}

export function renderSituacionDigestHtml(input: DigestInput): string {
  const { cambios: c, esLunes } = input;
  const t = c.totales ?? {};
  const head = `<div style="font:12px/1.3 ${FONT};color:${C.faint};text-transform:uppercase;letter-spacing:.06em">Situación de la empresa</div>
    <h1 style="margin:4px 0 14px;font:700 22px/1.25 ${FONT};color:${C.ink}">${esc(input.dateLabel)}</h1>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${C.lineSoft};border-radius:8px"><tr>
      ${stat(t.empeoradas ?? 0, "empeoraron", "danger")}${stat(t.nuevas ?? 0, "nuevas", "warn")}${stat(t.graves ?? 0, "graves abiertas", "warn")}${stat(t.delegadas ?? 0, "delegadas")}${stat(t.resueltas ?? 0, "resueltas", "ok")}${stat(t.abiertas ?? 0, "abiertas")}
    </tr></table>`;
  const narrativa = input.narrativaMd?.trim() ? `<div style="margin-top:16px">${mdToHtml(input.narrativaMd)}</div>` : "";
  const cuerpo = sinCambios(c)
    ? `<p style="margin:18px 0;font:14px/1.5 ${FONT};color:${C.body}">Sin cambios en las últimas 24 horas. ${fmtInt(t.abiertas ?? 0)} situaciones siguen abiertas.</p>`
    : c.areas.map(bloqueArea).join("");
  const rezago = esLunes && c.rezago?.length
    ? `<h2 style="margin:22px 0 4px;font:600 16px/1.3 ${FONT};color:${C.ink}">Rezago <span style="font-weight:400;color:${C.faint};font-size:13px">· sin cambio en más de 30 días</span></h2>
       <table role="presentation" width="100%" cellpadding="0" cellspacing="0">${c.rezago.map((x) => linea({ ...x, titulo: `${AREAS[x.area] ?? x.area}: ${x.titulo}`, dias_abierta: x.dias_abierta })).join("")}</table>`
    : "";
  const salud = c.salud ?? { odoo_push_edad_h: null, bot_terminada_en: null, sin_datos: [] };
  const alertas: string[] = [];
  if (salud.odoo_push_edad_h == null || salud.odoo_push_edad_h > 3) alertas.push(`push de Odoo ${salud.odoo_push_edad_h == null ? "sin registro" : `hace ${salud.odoo_push_edad_h} h`}`);
  if (salud.sin_datos?.length) alertas.push(`${salud.sin_datos.length} señal(es) sin datos: ${salud.sin_datos.slice(0, 6).join(", ")}`);
  const pie = `${fmtInt(c.ignoradas)} ignoradas por tus reglas (${fmtInt(c.reglas_vigentes)} reglas) · higiene: ${fmtInt(c.higiene?.zombie)} zombis, ${fmtInt(c.higiene?.dato_malo)} datos malos` +
    (alertas.length ? `<br><span style="color:${C.dangerInk}">Salud del mapa: ${esc(alertas.join("; "))}</span>` : `<br>Salud del mapa: en orden`) +
    `<br>Ventana: ${esc(c.desde)} → ${esc(c.hasta)}. Detalle por MCP: <code>select situacion_contexto(&lt;id&gt;)</code>.`;
  return layout(`Situación — ${input.dateLabel}`, head + narrativa + cuerpo + rezago, pie);
}

/** Versión texto plano (alternativa MIME y lo que se guarda). Dice lo mismo, sin HTML. */
export function renderSituacionDigestText(input: DigestInput): string {
  const { cambios: c, esLunes } = input;
  const t = c.totales ?? {};
  const out: string[] = [`SITUACIÓN — ${input.dateLabel}`, `${t.empeoradas ?? 0} empeoraron · ${t.nuevas ?? 0} nuevas · ${t.graves ?? 0} graves abiertas · ${t.delegadas ?? 0} delegadas · ${t.resueltas ?? 0} resueltas · ${t.abiertas ?? 0} abiertas`, ""];
  if (input.narrativaMd?.trim()) out.push(input.narrativaMd.trim(), "");
  const fila = (x: CambiosItem) => `  [${x.severidad}] ${x.titulo} — ${[x.contraparte, `${x.dias_abierta} días`, x.delegada_a ? `delegada a ${x.delegada_a}` : x.responsable ? `→ ${x.responsable}` : null].filter(Boolean).join(" · ")}` +
    (x.redactada ? (x.recomendacion ? `\n      ${x.recomendacion}` : "") : `\n      (sin redactar aún${x.valor_texto ? `: ${x.valor_texto}` : ""})`);
  if (sinCambios(c)) out.push(`Sin cambios en las últimas 24 horas. ${t.abiertas ?? 0} situaciones siguen abiertas.`);
  for (const a of c.areas) {
    const partes = LISTAS.filter(([k]) => (a[k] as CambiosItem[]).length);
    if (!partes.length) continue;
    out.push(`${(AREAS[a.area] ?? a.area).toUpperCase()} · ${a.abiertas} abiertas`);
    for (const [k, titulo] of partes) { out.push(`  ${titulo}:`); for (const x of a[k] as CambiosItem[]) out.push(fila(x)); }
    out.push("");
  }
  if (esLunes && c.rezago?.length) { out.push("REZAGO (sin cambio en más de 30 días)"); for (const x of c.rezago) out.push(fila({ ...x, titulo: `${AREAS[x.area] ?? x.area}: ${x.titulo}` })); out.push(""); }
  out.push(`${c.ignoradas} ignoradas por tus reglas (${c.reglas_vigentes} reglas) · higiene: ${c.higiene?.zombie ?? 0} zombis, ${c.higiene?.dato_malo ?? 0} datos malos`);
  return out.join("\n");
}
```

- [ ] **Step 5: Escribir `supabase/functions/situacion-digest/prompt.ts`** (puro):

```ts
/** Prompt de la narrativa del correo de situación (Opus). Solo resume el JSON de situacion_cambios; no inventa. */
import type { Cambios, CambiosItem } from "../_shared/situacion-digest-html.ts";

export const SYSTEM = `Eres el asistente ejecutivo del director general de Quimibond (textil, México). Recibes el JSON de situacion_cambios: lo que cambió en el mapa de situación en las últimas 24 horas (por área: nuevas, empeoradas, mejoradas, resueltas, delegadas y lo grave que sigue abierto), más rezago y salud.

Escribe en español, en markdown, máximo 180 palabras, SOLO esta sección:

## Lo que decidiría hoy
3 a 6 bullets. Cada bullet: una decisión concreta del director (delegar a alguien, llamar a un cliente, cerrar algo, pedir una limpieza), con el título de la situación tal cual viene, la contraparte, la cifra o los días, y a quién. Primero lo que empeoró y lo grave; luego lo nuevo. Si hay delegaciones en error o el push de Odoo lleva más de 3 h, dilo en el primer bullet.

Reglas: solo hechos del JSON, nada inventado; no repitas las listas (el correo ya las trae debajo); no uses JSON ni tablas; si no hubo cambios, un solo bullet que lo diga.`;

const recorta = (x: CambiosItem) => ({ id: x.id, titulo: x.titulo, contraparte: x.contraparte, severidad: x.severidad, dias: x.dias_abierta, responsable: x.responsable, delegada_a: x.delegada_a, delegacion_estado: x.delegacion_estado, cambio: x.ultimo_cambio?.slice(0, 120), recomendacion: x.recomendacion?.slice(0, 220), valor: x.valor_texto?.slice(0, 120) });

/** JSON compacto para Claude con presupuesto de caracteres: cada lista se recorta a los N de mayor severidad hasta caber. */
export function entradaParaClaude(c: Cambios, presupuesto = 40_000): string {
  const listas = ["empeoradas", "nuevas", "graves", "delegadas", "mejoradas", "resueltas"] as const;
  let tope = 25;
  for (;;) {
    const areas = c.areas.map((a) => {
      const o: Record<string, unknown> = { area: a.area, abiertas: a.abiertas };
      for (const l of listas) o[l] = a[l].slice(0, tope).map(recorta);
      return o;
    });
    const txt = JSON.stringify({ desde: c.desde, hasta: c.hasta, totales: c.totales, areas, rezago: c.rezago.slice(0, Math.min(tope, 10)).map(recorta), ignoradas: c.ignoradas, higiene: c.higiene, salud: c.salud });
    if (txt.length <= presupuesto || tope <= 2) return txt.slice(0, presupuesto);
    tope = Math.max(2, Math.floor(tope / 2));
  }
}
```

- [ ] **Step 6: `tsconfig.json`:** agregar `"allowImportingTsExtensions": true` en `compilerOptions` (válido porque ya hay `noEmit: true`). Motivo: `situacion-digest-html.ts` importa `./email-html.ts` con extensión (Deno lo exige) y el test de vitest lo importa, así que `npx tsc --noEmit` (que corre el CI) lo sigue y fallaría con TS5097. Ningún `_shared` importado desde vitest tenía imports relativos hasta hoy.

- [ ] **Step 7: Correr el test:** `npx vitest run src/__tests__/pipeline/situacion-digest-html.test.ts`. Esperado: 5 tests PASS. Luego `npx eslint supabase/functions/_shared/email-html.ts supabase/functions/_shared/situacion-digest-html.ts supabase/functions/situacion-digest && npx tsc --noEmit`: limpios.

- [ ] **Step 8: Commit** — "situacion-digest: render puro del correo de situación y helpers HTML compartidos".

### Task 4.3: Edge Function `situacion-digest`

**Files:**
- Create: `supabase/functions/situacion-digest/index.ts`

- [ ] **Step 1: Escribir la función:**

```ts
/**
 * situacion-digest (Edge Function) — el correo diario de situación al director (spec §7.2).
 * Sustituye a email-digest. Una sola fuente: situacion_cambios(p_desde). Claude (Opus) solo
 * escribe "Lo que decidiría hoy"; las listas salen del JSON tal cual (situacion-digest-html.ts).
 *
 * Disparo: pg_cron `situacion_digest` 12:30 UTC (06:30 CDMX). Body opcional:
 *   { "manual": true }       genera y guarda sin mandar correo
 *   { "desde": "<iso>" }     ventana desde esa hora (default: el `hasta` del último digest, o 24 h)
 *   { "sin_ia": true }       sin narrativa (prueba barata)
 */
import { serviceClient, authorizeCron, json, pipelineLog, readBody } from "../_shared/env.ts";
import { anthropicClient, claudeText, MODEL_MAIN } from "../_shared/claude.ts";
import { sendMail } from "../_shared/mailer.ts";
import { renderSituacionDigestHtml, renderSituacionDigestText, type Cambios } from "../_shared/situacion-digest-html.ts";
import { SYSTEM, entradaParaClaude } from "./prompt.ts";

const TZ = "America/Mexico_City";

Deno.serve(async (req: Request) => {
  const supabase = serviceClient();
  const denied = await authorizeCron(req, supabase);
  if (denied) return denied;
  const body = await readBody(req);
  const manual = body.manual === true;
  const started = Date.now();

  try {
    // 1. Ventana: desde el último digest (o 24 h), para que nada se pierda si un día falló.
    // Regla: desde el `hasta` del último correo del cron si tiene menos de 60 h (cubre un día fallido); si no, 24 h.
    let desde: string | null = typeof body.desde === "string" ? body.desde : null;
    if (!desde) {
      const { data: ult } = await supabase.from("situacion_digests").select("hasta").eq("trigger", "cron").order("hasta", { ascending: false }).limit(1).maybeSingle();
      const ultimo = ult?.hasta ? new Date(ult.hasta).getTime() : 0;
      const reciente = ultimo > started - 60 * 3600_000;
      desde = new Date(reciente ? ultimo : started - 24 * 3600_000).toISOString();
    }
    const { data: cambios, error } = await supabase.rpc("situacion_cambios", { p_desde: desde });
    if (error) throw new Error(`situacion_cambios: ${error.message}`);
    const c = cambios as Cambios;

    // 2. Narrativa (Opus) solo si hubo cambios.
    const hayCambios = ["nuevas", "empeoradas", "mejoradas", "resueltas", "delegadas", "graves"].some((k) => (c.totales?.[k] ?? 0) > 0);
    let narrativa = "";
    let modelo: string | null = null;
    if (hayCambios && body.sin_ia !== true) {
      const client = await anthropicClient(supabase);
      if (!client) throw new Error("anthropic_api_key no configurado (env ni Vault)");
      modelo = MODEL_MAIN;
      narrativa = await claudeText(client, supabase, { model: MODEL_MAIN, system: SYSTEM, user: entradaParaClaude(c), max_tokens: 1200, effort: "medium" }, "situacion-digest");
    }

    // 3. Render y correo.
    const ahora = new Date();
    const dateLabel = ahora.toLocaleDateString("es-MX", { timeZone: TZ, weekday: "long", day: "numeric", month: "long", year: "numeric" });
    const fecha = ahora.toLocaleDateString("sv-SE", { timeZone: TZ });
    const esLunes = new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short" }).format(ahora) === "Mon";
    const html = renderSituacionDigestHtml({ dateLabel, narrativaMd: narrativa, cambios: c, esLunes });
    const texto = renderSituacionDigestText({ dateLabel, narrativaMd: narrativa, cambios: c, esLunes });
    let emailed = false, emailError: string | null = null;
    if (!manual) {
      const t = c.totales ?? {};
      const asunto = hayCambios ? `🗺️ Situación — ${fecha}: ${t.empeoradas ?? 0} empeoraron, ${t.nuevas ?? 0} nuevas, ${t.graves ?? 0} graves` : `🗺️ Situación — ${fecha}: sin cambios`;
      const r = await sendMail(supabase, asunto, texto, html);
      emailed = r.ok; emailError = r.error ?? null;
    }

    // 4. Bitácora y log.
    await supabase.from("situacion_digests").insert({ fecha, desde, hasta: c.hasta, cambios: c, narrativa_md: narrativa || null, emailed, email_error: emailError, trigger: manual ? "manual" : "cron", modelo });
    const elapsed_s = Math.round((Date.now() - started) / 1000);
    await pipelineLog(supabase, "situacion_digest", emailError ? "warning" : "info",
      `Situación digest (${manual ? "manual" : "cron"}): ${JSON.stringify(c.totales)} emailed=${emailed}${emailError ? ` — ${emailError}` : ""} (${elapsed_s}s)`,
      { desde, hasta: c.hasta, totales: c.totales, emailed, email_error: emailError, modelo, elapsed_s });
    return json({ ok: true, desde, hasta: c.hasta, totales: c.totales, emailed, email_error: emailError, narrativa, texto });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await pipelineLog(supabase, "situacion_digest", "error", `Situación digest falló: ${message.slice(0, 300)}`);
    return json({ error: message }, 500);
  }
});
```

- [ ] **Step 2: Lint y tipos:** `npx eslint supabase/functions/situacion-digest && npx tsc --noEmit` limpios.

- [ ] **Step 3: Desplegar** con `deploy_edge_function` (`name: situacion-digest`, `entrypoint_path: situacion-digest/index.ts`, `verify_jwt: false`, files: `situacion-digest/index.ts`, `situacion-digest/prompt.ts`, `_shared/env.ts`, `_shared/claude.ts`, `_shared/mailer.ts`, `_shared/gmail.ts`, `_shared/email-html.ts`, `_shared/situacion-digest-html.ts`).

- [ ] **Step 4: Probar sin correo:** `select invoke_edge('situacion-digest', '{"manual":true}'::jsonb);` → esperar 30 s → `select id, fecha, desde, hasta, cambios->'totales' totales, left(narrativa_md, 400), emailed, modelo from situacion_digests order by id desc limit 1;` y `select level, message from pipeline_logs where phase = 'situacion_digest' order by created_at desc limit 2;`. Revisa que `totales` coincida con `select situacion_cambios(desde)` (mismo `desde`). Luego un envío real: `select invoke_edge('situacion-digest', '{}'::jsonb);` y confirmar con el CEO que llegó el correo y se lee bien en Gmail (móvil incluido).

- [ ] **Step 5: Commit** — "situacion-digest: Edge Function del correo diario de situación".

### Task 4.4: Job `situacion_digest`, retiro del job viejo y vigilancia

**Files:**
- Create: `supabase/migrations/20260924b_situacion_digest_cron.sql`
- Modify: `supabase/functions/health/index.ts` (`JOB_INTERVALS`)

- [ ] **Step 1: Migración:**

```sql
-- 2026-09-24b — Situación plan B, paso 4: correo diario de situación a las 06:30 CDMX (12:30 UTC).
-- Sustituye al resumen de correo (memoria_email_digest, 12:45 UTC): se desprograma aquí; la Edge
-- Function email-digest y sus RPCs se retiran en 20260926a, tras la aceptación del CEO.
DO $do$ DECLARE j record; BEGIN
  FOR j IN SELECT jobid FROM cron.job WHERE jobname IN ('situacion_digest', 'memoria_email_digest') LOOP PERFORM cron.unschedule(j.jobid); END LOOP;
END $do$;
SELECT cron.schedule('situacion_digest', '30 12 * * *', $cmd$SELECT public.invoke_edge('situacion-digest', '{"origen":"cron"}'::jsonb)$cmd$);

-- El watchdog lee memoria_cron_health(), que hoy solo devuelve jobs memoria_%: sin esto, `situacion_digest: job no existe` cada hora.
-- (Cuerpo de 20260916g con el WHERE ampliado a situacion_%.)
CREATE OR REPLACE FUNCTION public.memoria_cron_health()
RETURNS TABLE (jobname text, active boolean, schedule text, last_ok timestamptz, last_run timestamptz, failures_3h bigint)
LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT j.jobname, j.active, j.schedule,
         max(d.start_time) FILTER (WHERE d.status = 'succeeded') AS last_ok,
         max(d.start_time) AS last_run,
         count(*) FILTER (WHERE d.status = 'failed' AND d.start_time > now() - interval '3 hours') AS failures_3h
  FROM cron.job j
  LEFT JOIN cron.job_run_details d ON d.jobid = j.jobid AND d.start_time > now() - interval '3 days'
  WHERE j.jobname LIKE 'memoria_%' OR j.jobname LIKE 'situacion_%'
  GROUP BY j.jobname, j.active, j.schedule
  ORDER BY j.jobname;
$$;
COMMENT ON FUNCTION public.memoria_cron_health() IS 'Watchdog: última corrida exitosa y fallos recientes de los jobs pg_cron memoria_* y situacion_*.';

INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Situación plan B paso 4: job situacion_digest 12:30 UTC; memoria_email_digest desprogramado; memoria_cron_health ve situacion_*', jsonb_build_object('migration', '20260924b_situacion_digest_cron'));
```

- [ ] **Step 2: Aplicar** y verificar: `select jobname, schedule, active from cron.job where jobname in ('situacion_digest','memoria_email_digest');` → solo `situacion_digest`, `30 12 * * *`, activo; `select * from memoria_cron_health() where jobname like 'situacion%'` → dos filas (`situacion_digest`, `situacion_respaldo`).

- [ ] **Step 3: `health/index.ts`:** en `JOB_INTERVALS` agregar `situacion_digest: 1440,` (diario; el umbral es `interval * 2.5` = 60 h, así que un día fallido avisa al siguiente). Actualizar el comentario de cabecera (punto 5) con "y el correo diario de situación". Desplegar `health` (files `health/index.ts`, `_shared/env.ts`, `_shared/mailer.ts`, `_shared/gmail.ts`) y probar `select invoke_edge('health');` → `pipeline_logs` phase `watchdog` sin `situacion_digest` entre los problemas (recién programado: `last_run` nulo se salta).

- [ ] **Step 4: Commit** — "Situación: job situacion_digest (12:30 UTC), memoria_email_digest desprogramado, watchdog".

### Task 4.5: Docs, PR y aceptación del paso 4

- [ ] **Step 1: `CLAUDE.md` (quimibond-intelligence):** en "Qué es" punto 3 → "El correo diario de situación (`situacion-digest`, 12:30 UTC) al CEO; el resumen de correo `email-digest` se retiró el 2026-09-2x". Tabla de jobs: reemplazar la fila `memoria_email_digest` por `situacion_digest | 12:30 | situacion-digest (Opus, solo la narrativa) | Correo diario del mapa desde situacion_cambios → situacion_digests + correo HTML`. Tabla de tablas: `situacion_digests`. Sección "Situación de la empresa": subsección **Correo diario** (qué trae, `situacion_cambios`, cómo probar `{"manual":true}`, cómo re-mandar un día `{"desde":"…"}`), agregar `situacion_cambios` a "Leer por MCP" y `en_mapa` a la descripción de `senales_config`, y nota de `delegada` pegajosa. Deuda: quitar "correo diario desde el mapa" de la lista del plan B.

- [ ] **Step 2: PR** en quimibond-intelligence: "Situación plan B, paso 4: situacion_cambios y correo diario situacion-digest" (borrador → CI `check` → ready → squash-merge → reconstruir la rama). Cuerpo: qué trae, verificación (vitest, tsc, pruebas SQL `04`/`05` en PRUEBA_OK, digest manual y real), desviaciones (`en_mapa`, delegada pegajosa).

- [ ] **Step 3: Aceptación (spec §8 paso 4):** a la mañana siguiente, comparar el correo recibido con `select cambios->'totales', narrativa_md from situacion_digests order by id desc limit 1` y con `select situacion_cambios('<desde del registro>')`: mismas listas. El CEO confirma que el correo le sirve; anota ajustes de formato en el PR del paso 5. Hasta que confirme, `email-digest` sigue desplegado pero sin job (rollback = volver a programar `memoria_email_digest`).


---

## Parte 2 — Paso 5: decidir y delegar

### Task 5.1: `situacion_decidir`, confirmación de delegaciones y reglas que el bot respeta (Supabase)

**Files:**
- Create: `supabase/migrations/20260925a_situacion_decidir.sql`
- Create: `supabase/tests/situacion/06_decidir.sql`

**Diseño (léelo antes del código):**
- `sync_commands.payload jsonb` (columna nueva). El pull de Odoo la lee (Tarea 5.3).
- `situacion_decidir(p_id, p_accion, p)`:
  - `delegar {user_id, texto?, vence?}`: valida el usuario en `odoo_users`, exige situación abierta y no delegada en curso; elige documento = primer `documentos[]` con `modelo <> 'thread'` e `id` numérico, si no `res.partner`/`odoo_partner_id`, si no nada (Odoo la pone sobre el usuario); inserta `sync_commands('crear_actividad', payload)`; `estado='delegada'`, `delegacion={user_id, fecha, texto, vence, estado:'pendiente', comando_id}`; historia.
  - `ignorar` / `no_es_problema {alcance?='situacion', clave_alcance?, motivo?, vigente_hasta?}`: inserta la regla en `situacion_reglas` y **descarta** esta situación y las abiertas que la regla cubra (`situacion_regla_aplica`). En el siguiente ciclo las señales quedan `ignorada` y `situacion_guardar` ya salta las descartadas.
  - `resuelta {motivo?}`: `estado='resuelta'`, `resuelta_en`, historia "cerrada por el director". Si estaba delegada, la delegación queda `cerrada_por_director` (la actividad de Odoo sigue viva; el push la reporta y `situacion_delegaciones_aplicar` la ignora porque la situación ya no está delegada).
  - `reabrir {motivo?}`: `estado='abierta'`, `resuelta_en=NULL`, `version+1`; vence las reglas `ignorar`/`no_es_problema` con `alcance='situacion'` de esa clave.
  - `separar {id?}`: la(s) absorbida(s) por `p_id` vuelven al mapa (`fusionada_en=NULL`) y sus ids se guardan en `evidencia.no_fusionar` de la madre; `situacion_redactar` no vuelve a fusionarlas.
  - `severidad {severidad, motivo?}`: regla `severidad_fija` (alcance situación) + escribe la severidad; `situacion_redactar` la respeta.
- `situacion_delegacion_confirmar(p_situacion_id, p_mail_activity_id, p_estado, p_detalle)`: `p_estado ∈ creada|error|hecha|cancelada`. `hecha` ⇒ `resuelta` con historia "cerrada por <usuario>: <feedback>"; `cancelada` ⇒ `abierta` con historia; `creada`/`error` solo actualizan `delegacion`.
- `situacion_delegaciones_abiertas()` → `(situacion_id, mail_activity_id, user_id, vence, fecha)` de delegaciones `creada`. Odoo la consulta antes de mandar el lote `delegacion_estado`.
- `situacion_delegaciones_aplicar()` (lo llama `situacion_ciclo` después de `senales_memoria`): por cada señal abierta `delegacion_estado` con `payload.evento ∈ hecha|cancelada` y sin `payload.aplicado`, llama a `situacion_delegacion_confirmar` y marca `aplicado`. También marca `error` las delegaciones `pendiente` con más de 15 min sin actividad (spec §7.3).
- `situacion_redactar`: respeta `severidad_fija` y `responsable_fijo` (reglas vigentes de alcance situación/señal/contraparte/documento) y no fusiona ids en `evidencia.no_fusionar`.

- [ ] **Step 1: Prueba en seco** `supabase/tests/situacion/06_decidir.sql`:

```sql
-- Prueba en seco de situacion_decidir / situacion_delegacion_confirmar / situacion_delegaciones_aplicar. Termina en PRUEBA_OK.
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); sid bigint; sid2 bigint; sid3 bigint; cmd record; n int; uid int;
BEGIN
  -- Un usuario real de odoo_users para delegar (el primero que haya).
  SELECT odoo_user_id INTO uid FROM odoo_users ORDER BY odoo_user_id LIMIT 1;
  ASSERT uid IS NOT NULL, 'hace falta al menos un odoo_users';
  INSERT INTO senales_config (senal, titulo, area, tipo, fuente, agrupar_por, agregar, severidad_base, severidad_max)
  VALUES ('_prueba', 'Prueba', 'finanzas', 'credito', 'odoo', 'contraparte', 'suma', 3, 5);
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"thread","id":5,"nombre":"hilo"},{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:990000003","odoo_partner_id":990000003,"valor":7,"documentos":[]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  SELECT id INTO sid  FROM situaciones WHERE clave = '_prueba|partner:990000001';
  SELECT id INTO sid2 FROM situaciones WHERE clave = '_prueba|partner:990000002';
  SELECT id INTO sid3 FROM situaciones WHERE clave = '_prueba|partner:990000003';

  -- 1. delegar: comando con payload, documento = el account.move (no el thread), estado delegada/pendiente.
  r := situacion_decidir(sid, 'delegar', jsonb_build_object('user_id', uid, 'texto', 'Cobrar F/1', 'vence', current_date + 3));
  ASSERT (r->>'ok')::bool AND r->>'modelo' = 'account.move' AND (r->>'res_id')::int = 11, 'delegar: ' || r::text;
  SELECT * INTO cmd FROM sync_commands WHERE id = (r->>'comando_id')::bigint;
  ASSERT cmd.command = 'crear_actividad' AND cmd.status = 'pending' AND (cmd.payload->>'situacion_id')::bigint = sid AND (cmd.payload->>'user_id')::int = uid
     AND cmd.payload->>'texto' = 'Cobrar F/1' AND cmd.payload->>'modelo' = 'account.move', 'comando: ' || row_to_json(cmd)::text;
  ASSERT (SELECT estado FROM situaciones WHERE id = sid) = 'delegada' AND (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid) = 'pendiente', 'estado tras delegar';
  -- delegar dos veces: error.
  BEGIN
    PERFORM situacion_decidir(sid, 'delegar', jsonb_build_object('user_id', uid));
    RAISE EXCEPTION 'debió fallar la segunda delegación';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE 'debió%' THEN RAISE; END IF;
  END;
  -- sin documento de Odoo: va al contacto.
  r := situacion_decidir(sid3, 'delegar', jsonb_build_object('user_id', uid));
  ASSERT r->>'modelo' = 'res.partner' AND (r->>'res_id')::int = 990000003, 'delegar al contacto: ' || r::text;

  -- 2. confirmar creada → hecha: la situación se resuelve con historia.
  r := situacion_delegacion_confirmar(sid, 777, 'creada', NULL);
  ASSERT (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid) = 'creada' AND (SELECT (delegacion->>'mail_activity_id')::int FROM situaciones WHERE id = sid) = 777, 'creada';
  ASSERT EXISTS (SELECT 1 FROM situacion_delegaciones_abiertas() WHERE situacion_id = sid AND mail_activity_id = 777), 'abiertas lista la creada';
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_delegaciones_abiertas() WHERE situacion_id = sid3), 'abiertas NO lista la pendiente';
  r := situacion_delegacion_confirmar(sid, 777, 'hecha', 'Ya pagó');
  ASSERT (SELECT estado FROM situaciones WHERE id = sid) = 'resuelta' AND (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid) = 'hecha', 'hecha resuelve';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT historia FROM situaciones WHERE id = sid)) e WHERE e->>'evento' = 'resuelta' AND e->>'detalle' LIKE 'cerrada por%Ya pagó%'), 'historia de hecha';
  -- 2b. el cierre humano es pegajoso: el mismo lote no la reabre; un lote mayor la reabre como 'empeoro' y quita la marca.
  ASSERT (SELECT evidencia ? 'cerrada_manual' FROM situaciones WHERE id = sid), 'marca cerrada_manual';
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"thread","id":5,"nombre":"hilo"},{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:990000003","odoo_partner_id":990000003,"valor":7,"documentos":[]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  ASSERT (SELECT estado FROM situaciones WHERE id = sid) = 'resuelta', 'cierre humano pegajoso con la misma señal';
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:990000001","odoo_partner_id":990000001,"valor":100,"documentos":[{"modelo":"thread","id":5,"nombre":"hilo"},{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:990000001b","odoo_partner_id":990000001,"valor":80,"documentos":[{"modelo":"account.move","id":14,"nombre":"F/4"}]},
    {"clave":"_prueba:partner:990000002","odoo_partner_id":990000002,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:990000003","odoo_partner_id":990000003,"valor":7,"documentos":[]}
  ]'::jsonb);
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  ASSERT (SELECT estado FROM situaciones WHERE id = sid) = 'empeoro' AND NOT (SELECT evidencia ? 'cerrada_manual' FROM situaciones WHERE id = sid)
     AND (SELECT resuelta_en FROM situaciones WHERE id = sid) IS NULL, 'reabre como empeoro si la señal crece';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT historia FROM situaciones WHERE id = sid)) e WHERE e->>'detalle' LIKE 'reapareció tras cierre manual%'), 'historia del reapareció';

  -- 3. cancelada reabre. (sid3: pendiente → creada → cancelada)
  PERFORM situacion_delegacion_confirmar(sid3, 778, 'creada', NULL);
  PERFORM situacion_delegacion_confirmar(sid3, 778, 'cancelada', 'no me toca');
  ASSERT (SELECT estado FROM situaciones WHERE id = sid3) = 'abierta' AND (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid3) = 'cancelada', 'cancelada reabre';

  -- 4. aplicar desde la señal delegacion_estado (lo que manda Odoo): hecha por señal.
  PERFORM situacion_decidir(sid3, 'delegar', jsonb_build_object('user_id', uid));
  PERFORM situacion_delegacion_confirmar(sid3, 779, 'creada', NULL);
  PERFORM senales_ingestar('delegacion_estado', 'odoo', c, jsonb_build_array(jsonb_build_object(
    'clave', 'delegacion_estado:situacion:' || sid3, 'valor', 1, 'valor_texto', 'hecha',
    'payload', jsonb_build_object('situacion_id', sid3, 'evento', 'hecha', 'feedback', 'listo', 'user_id', uid, 'mail_activity_id', 779))));
  r := situacion_delegaciones_aplicar();
  ASSERT (r->>'aplicadas')::int = 1, 'aplicar: ' || r::text;
  ASSERT (SELECT estado FROM situaciones WHERE id = sid3) = 'resuelta', 'hecha por señal resuelve';
  ASSERT (SELECT bool_and((payload->>'aplicado')::bool) FROM senales WHERE senal = 'delegacion_estado' AND clave = 'delegacion_estado:situacion:' || sid3 AND resuelta_en IS NULL), 'marcada aplicada';
  r := situacion_delegaciones_aplicar();
  ASSERT (r->>'aplicadas')::int = 0, 'no se aplica dos veces';
  -- delegacion_estado no forma situaciones (en_mapa=false).
  PERFORM situacion_guardar(c);
  ASSERT NOT EXISTS (SELECT 1 FROM situaciones WHERE senal = 'delegacion_estado'), 'delegacion_estado sin situación propia';

  -- 5. pendiente vieja → error.
  PERFORM situacion_decidir(sid2, 'delegar', jsonb_build_object('user_id', uid));
  UPDATE situaciones SET delegacion = delegacion || jsonb_build_object('fecha', now() - interval '20 minutes') WHERE id = sid2;
  PERFORM situacion_delegaciones_aplicar();
  ASSERT (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid2) = 'error', 'pendiente > 15 min → error';
  -- reabrir/resuelta a mano.
  r := situacion_decidir(sid2, 'resuelta', '{"motivo":"ya se cobró"}');
  ASSERT (SELECT estado FROM situaciones WHERE id = sid2) = 'resuelta' AND (SELECT delegacion->>'estado' FROM situaciones WHERE id = sid2) = 'cerrada_por_director', 'resuelta a mano';
  r := situacion_decidir(sid2, 'reabrir', '{}');
  ASSERT (SELECT estado FROM situaciones WHERE id = sid2) = 'abierta', 'reabrir';

  -- 6. severidad fija: el bot no la cambia.
  r := situacion_decidir(sid2, 'severidad', '{"severidad": 5, "motivo": "cliente estratégico"}');
  ASSERT (SELECT severidad FROM situaciones WHERE id = sid2) = 5, 'severidad fija escrita';
  r := situacion_redactar(sid2, '{"titulo":"T","resumen":"R","recomendacion":"Rec","severidad":3,"evento_historia":"redactada"}'::jsonb, 'modelo-x', NULL);
  ASSERT (SELECT severidad FROM situaciones WHERE id = sid2) = 5, 'redactar respeta severidad_fija';

  -- 7. ignorar con alcance contraparte: descarta esta y deja regla; reabrir la vence.
  r := situacion_decidir(sid2, 'ignorar', '{"alcance":"contraparte","motivo":"parte relacionada"}');
  ASSERT (r->>'regla_id') IS NOT NULL AND (r->>'descartadas')::int >= 1, 'ignorar: ' || r::text;
  ASSERT (SELECT estado FROM situaciones WHERE id = sid2) = 'descartada', 'ignorar descarta';
  ASSERT EXISTS (SELECT 1 FROM situacion_reglas WHERE alcance = 'contraparte' AND clave_alcance = 'partner:990000002' AND accion = 'ignorar'), 'regla creada';
  PERFORM senales_actualizar();
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:990000002' AND resuelta_en IS NULL) = 'ignorada', 'señal ignorada por la regla';
  r := situacion_decidir(sid2, 'reabrir', '{}');
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_reglas WHERE clave_alcance = 'partner:990000002' AND accion = 'ignorar' AND (vigente_hasta IS NULL OR vigente_hasta > now())), 'reabrir vence la regla de esa contraparte';

  -- 8. separar: la absorbida vuelve y no se vuelve a fusionar.
  INSERT INTO situaciones (clave, senal, agrupador, area, tipo, titulo, odoo_partner_id, severidad, estado)
  VALUES ('_prueba|partner:990000001x', '_prueba', 'partner:990000001x', 'finanzas', 'credito', 'Prueba · dup', 990000001, 3, 'abierta') RETURNING id INTO sid2;
  UPDATE situaciones SET estado = 'abierta', resuelta_en = NULL WHERE id = sid;
  r := situacion_redactar(sid, format('{"titulo":"T","resumen":"R","recomendacion":"Rec","severidad":3,"evento_historia":"e","duplicados":[{"id":%s,"decision":"fusionar","motivo":"igual"}]}', sid2)::jsonb, 'modelo-x', NULL);
  ASSERT (SELECT fusionada_en FROM situaciones WHERE id = sid2) = sid, 'fusionada';
  r := situacion_decidir(sid, 'separar', jsonb_build_object('id', sid2));
  ASSERT (SELECT fusionada_en FROM situaciones WHERE id = sid2) IS NULL, 'separada';
  r := situacion_redactar(sid, format('{"titulo":"T","resumen":"R","recomendacion":"Rec","severidad":3,"evento_historia":"e","duplicados":[{"id":%s,"decision":"fusionar","motivo":"igual"}]}', sid2)::jsonb, 'modelo-x', NULL);
  ASSERT (SELECT fusionada_en FROM situaciones WHERE id = sid2) IS NULL AND (r->>'fusiones')::int = 0, 'no_fusionar respetado';

  -- 9. acción desconocida.
  BEGIN
    PERFORM situacion_decidir(sid, 'volar', '{}');
    RAISE EXCEPTION 'debió fallar la acción desconocida';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE 'debió%' THEN RAISE; END IF;
  END;
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correr y ver que falla** (`function situacion_decidir … does not exist`).

- [ ] **Step 3: Migración** `supabase/migrations/20260925a_situacion_decidir.sql`:

```sql
-- 2026-09-25a — Situación plan B, paso 5: decisiones del director y delegación (spec §7.1, §7.3).
BEGIN;

ALTER TABLE public.sync_commands ADD COLUMN IF NOT EXISTS payload jsonb;
COMMENT ON COLUMN public.sync_commands.payload IS 'Datos del comando (crear_actividad: situacion_id, user_id, texto, vence, modelo, res_id, partner_id, titulo). Los comandos viejos no lo usan.';

-- Nombre de usuario de Odoo (para historia y textos).
CREATE OR REPLACE FUNCTION public.situacion_nombre_usuario(p_uid integer)
RETURNS text LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT coalesce((SELECT name FROM odoo_users WHERE odoo_user_id = p_uid LIMIT 1), 'usuario ' || p_uid)
$$;

CREATE OR REPLACE FUNCTION public.situacion_decidir(p_id bigint, p_accion text, p jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  s situaciones%ROWTYPE; v_user int; v_doc jsonb; v_modelo text; v_res_id bigint; v_cmd bigint; v_regla bigint;
  v_alcance text; v_clave text; v_texto text; v_vence date; v_sev int; n int := 0; v_nombre text; v_motivo text; v_ids bigint[];
BEGIN
  SELECT * INTO s FROM situaciones WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'situación % no existe', p_id; END IF;
  v_motivo := nullif(p->>'motivo', '');

  IF p_accion = 'delegar' THEN
    v_user := (p->>'user_id')::int;
    IF v_user IS NULL OR NOT EXISTS (SELECT 1 FROM odoo_users WHERE odoo_user_id = v_user) THEN RAISE EXCEPTION 'user_id % no está en odoo_users', coalesce(v_user::text, 'NULL'); END IF;
    IF s.estado IN ('resuelta', 'descartada') THEN RAISE EXCEPTION 'situación % está %; reábrela antes', p_id, s.estado; END IF;
    IF s.delegacion->>'estado' IN ('pendiente', 'creada') THEN RAISE EXCEPTION 'situación % ya está delegada a % (%)', p_id, situacion_nombre_usuario((s.delegacion->>'user_id')::int), s.delegacion->>'estado'; END IF;
    v_nombre := situacion_nombre_usuario(v_user);
    v_texto := coalesce(nullif(p->>'texto', ''), s.recomendacion, s.titulo);
    v_vence := situacion_fecha(p->>'vence');
    SELECT d INTO v_doc FROM jsonb_array_elements(s.documentos) d WHERE d->>'modelo' <> 'thread' AND (d->>'id') ~ '^[0-9]+$' LIMIT 1;
    IF v_doc IS NOT NULL THEN v_modelo := v_doc->>'modelo'; v_res_id := (v_doc->>'id')::bigint;
    ELSIF s.odoo_partner_id IS NOT NULL THEN v_modelo := 'res.partner'; v_res_id := s.odoo_partner_id;
    END IF;
    INSERT INTO sync_commands (command, status, requested_by, payload)
    VALUES ('crear_actividad', 'pending', coalesce(p->>'creada_por', 'ceo'),
            jsonb_build_object('situacion_id', p_id, 'user_id', v_user, 'texto', v_texto, 'vence', v_vence, 'modelo', v_modelo, 'res_id', v_res_id,
                               'partner_id', s.odoo_partner_id, 'titulo', s.titulo))   -- partner_id: respaldo de Odoo si el documento ya no existe
    RETURNING id INTO v_cmd;
    UPDATE situaciones SET
      estado = 'delegada',
      delegacion = jsonb_build_object('user_id', v_user, 'nombre', v_nombre, 'fecha', now(), 'texto', v_texto, 'vence', v_vence, 'estado', 'pendiente',
                                      'comando_id', v_cmd, 'modelo', v_modelo, 'res_id', v_res_id, 'mail_activity_id', NULL, 'error', NULL),
      historia = historia || jsonb_build_object('fecha', now(), 'evento', 'delegada', 'detalle', 'a ' || v_nombre || coalesce(' hasta ' || v_vence, '') || ': ' || left(v_texto, 160)),
      ultimo_cambio = 'delegada a ' || v_nombre, ultimo_cambio_en = now(), updated_at = now()
    WHERE id = p_id;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', 'delegar', 'comando_id', v_cmd, 'user_id', v_user, 'modelo', v_modelo, 'res_id', v_res_id);

  ELSIF p_accion IN ('ignorar', 'no_es_problema') THEN
    v_alcance := coalesce(nullif(p->>'alcance', ''), 'situacion');
    v_clave := CASE v_alcance
      WHEN 'situacion' THEN s.clave
      WHEN 'senal' THEN s.senal
      WHEN 'contraparte' THEN coalesce('company:' || s.company_id, 'partner:' || s.odoo_partner_id)
      WHEN 'documento' THEN p->>'clave_alcance'
      ELSE NULL END;
    IF v_clave IS NULL THEN RAISE EXCEPTION 'alcance % sin clave (situación % no tiene contraparte/documento)', v_alcance, p_id; END IF;
    INSERT INTO situacion_reglas (alcance, clave_alcance, accion, valor, motivo, vigente_hasta, creada_por)
    VALUES (v_alcance, v_clave, p_accion, coalesce(p->'valor', '{}'::jsonb), v_motivo, (p->>'vigente_hasta')::timestamptz, coalesce(p->>'creada_por', 'ceo'))
    RETURNING id INTO v_regla;
    UPDATE situaciones x SET
      estado = 'descartada', resuelta_en = now(), updated_at = now(), ultimo_cambio = 'descartada por el director', ultimo_cambio_en = now(),
      historia = x.historia || jsonb_build_object('fecha', now(), 'evento', 'descartada', 'detalle', p_accion || ' (regla #' || v_regla || ', ' || v_alcance || '): ' || coalesce(v_motivo, ''))
    WHERE x.estado NOT IN ('resuelta', 'descartada') AND x.fusionada_en IS NULL
      AND (x.id = p_id OR situacion_regla_aplica(ARRAY[p_accion], x.senal, x.agrupador, x.company_id, x.odoo_partner_id, x.documentos) LIKE 'regla #' || v_regla || ' %');
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', p_accion, 'regla_id', v_regla, 'alcance', v_alcance, 'clave_alcance', v_clave, 'descartadas', n);

  ELSIF p_accion = 'resuelta' THEN
    UPDATE situaciones SET
      estado = 'resuelta', resuelta_en = now(), version = version + 1, updated_at = now(),
      -- cierre humano pegajoso (ver situacion_guardar): no reabre mientras la señal no crezca.
      evidencia = coalesce(evidencia, '{}'::jsonb) || jsonb_build_object('cerrada_manual', jsonb_build_object('n', n_senales, 'valor', valor, 'fecha', now(), 'por', 'director')),
      ultimo_cambio = 'cerrada por el director' || coalesce(': ' || v_motivo, ''), ultimo_cambio_en = now(),
      delegacion = CASE WHEN delegacion IS NOT NULL AND delegacion->>'estado' IN ('pendiente', 'creada', 'error') THEN delegacion || jsonb_build_object('estado', 'cerrada_por_director', 'fecha_estado', now()) ELSE delegacion END,
      historia = historia || jsonb_build_object('fecha', now(), 'evento', 'resuelta', 'detalle', 'cerrada por el director' || coalesce(': ' || v_motivo, ''))
    WHERE id = p_id;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', 'resuelta');

  ELSIF p_accion = 'reabrir' THEN
    UPDATE situaciones SET
      evidencia = coalesce(evidencia, '{}'::jsonb) - 'cerrada_manual',   -- reabrir a mano quita la marca del cierre humano
      estado = 'abierta', resuelta_en = NULL, version = version + 1, updated_at = now(),
      ultimo_cambio = 'reabierta por el director' || coalesce(': ' || v_motivo, ''), ultimo_cambio_en = now(),
      historia = historia || jsonb_build_object('fecha', now(), 'evento', 'reabierta', 'detalle', coalesce(v_motivo, 'por el director'))
    WHERE id = p_id;
    UPDATE situacion_reglas r SET vigente_hasta = now()
    WHERE r.accion IN ('ignorar', 'no_es_problema') AND (r.vigente_hasta IS NULL OR r.vigente_hasta > now())
      AND ((r.alcance = 'situacion' AND r.clave_alcance = s.clave)
        OR (r.alcance = 'contraparte' AND r.clave_alcance IN ('company:' || s.company_id, 'partner:' || s.odoo_partner_id)));
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', 'reabrir', 'reglas_vencidas', n);

  ELSIF p_accion = 'separar' THEN
    WITH sep AS (
      UPDATE situaciones h SET fusionada_en = NULL, version = h.version + 1, updated_at = now(),
        historia = h.historia || jsonb_build_object('fecha', now(), 'evento', 'separada', 'detalle', 'de #' || p_id || ' por el director' || coalesce(': ' || v_motivo, ''))
      WHERE h.fusionada_en = p_id AND (p->>'id' IS NULL OR h.id = (p->>'id')::bigint)
      RETURNING h.id)
    SELECT coalesce(array_agg(id), '{}') INTO v_ids FROM sep;
    n := coalesce(array_length(v_ids, 1), 0);
    IF n = 0 THEN RAISE EXCEPTION 'situación % no tiene fusionada %', p_id, coalesce(p->>'id', '(ninguna)'); END IF;
    UPDATE situaciones SET
      evidencia = evidencia || jsonb_build_object('no_fusionar', (SELECT coalesce(jsonb_agg(DISTINCT x), '[]') FROM jsonb_array_elements(coalesce(evidencia->'no_fusionar', '[]'::jsonb) || to_jsonb(v_ids)) x)),
      historia = historia || jsonb_build_object('fecha', now(), 'evento', 'separacion', 'detalle', n || ' situación(es) separada(s) por el director: ' || array_to_string(v_ids, ', ')),
      updated_at = now()
    WHERE id = p_id;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', 'separar', 'separadas', to_jsonb(v_ids));

  ELSIF p_accion = 'severidad' THEN
    v_sev := (p->>'severidad')::int;
    IF v_sev IS NULL OR v_sev NOT BETWEEN 1 AND 5 THEN RAISE EXCEPTION 'severidad % fuera de 1..5', coalesce(p->>'severidad', 'NULL'); END IF;
    INSERT INTO situacion_reglas (alcance, clave_alcance, accion, valor, motivo, creada_por)
    VALUES ('situacion', s.clave, 'severidad_fija', jsonb_build_object('severidad', v_sev), v_motivo, coalesce(p->>'creada_por', 'ceo')) RETURNING id INTO v_regla;
    UPDATE situaciones SET severidad = v_sev, updated_at = now(),
      historia = historia || jsonb_build_object('fecha', now(), 'evento', 'severidad_fija', 'detalle', v_sev || coalesce(': ' || v_motivo, '') || ' (regla #' || v_regla || ')')
    WHERE id = p_id;
    RETURN jsonb_build_object('ok', true, 'id', p_id, 'accion', 'severidad', 'severidad', v_sev, 'regla_id', v_regla);

  ELSE
    RAISE EXCEPTION 'acción % desconocida (delegar, ignorar, no_es_problema, resuelta, reabrir, separar, severidad)', p_accion;
  END IF;
END $$;
REVOKE ALL ON FUNCTION public.situacion_decidir(bigint, text, jsonb) FROM public, anon, authenticated;
COMMENT ON FUNCTION public.situacion_decidir(bigint, text, jsonb) IS 'Decisiones del director (spec §7.1): delegar {user_id, texto, vence} → sync_commands crear_actividad; ignorar/no_es_problema {alcance, motivo, vigente_hasta} → regla + descarta; resuelta; reabrir (vence reglas de ignorar); separar {id}; severidad {severidad}. Ejemplo MCP: select situacion_decidir(1295, ''delegar'', ''{"user_id":22,"texto":"Cobrar INV/2025/08/0145","vence":"2026-09-30"}'').';

-- Odoo confirma la delegación (pull) y sus eventos (push → señal → aplicar).
CREATE OR REPLACE FUNCTION public.situacion_delegacion_confirmar(p_situacion_id bigint, p_mail_activity_id bigint, p_estado text, p_detalle text DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE s situaciones%ROWTYPE; v_nombre text;
BEGIN
  IF p_estado NOT IN ('creada', 'error', 'hecha', 'cancelada') THEN RAISE EXCEPTION 'p_estado % inválido', p_estado; END IF;
  SELECT * INTO s FROM situaciones WHERE id = p_situacion_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'situación % no existe', p_situacion_id; END IF;
  IF s.delegacion IS NULL THEN RAISE EXCEPTION 'situación % no está delegada', p_situacion_id; END IF;
  v_nombre := coalesce(s.delegacion->>'nombre', situacion_nombre_usuario((s.delegacion->>'user_id')::int));
  IF p_estado IN ('hecha', 'cancelada') AND s.estado <> 'delegada' THEN
    -- El director ya la cerró/reabrió a mano: solo se anota, no se cambia el estado.
    UPDATE situaciones SET delegacion = delegacion || jsonb_build_object('estado_odoo', p_estado, 'fecha_estado', now(), 'feedback', p_detalle), updated_at = now() WHERE id = p_situacion_id;
    RETURN jsonb_build_object('ok', true, 'id', p_situacion_id, 'estado', s.estado, 'nota', 'situación ya no estaba delegada');
  END IF;
  UPDATE situaciones SET
    delegacion = delegacion || jsonb_build_object('estado', p_estado, 'fecha_estado', now(),
                   'mail_activity_id', coalesce(p_mail_activity_id, (delegacion->>'mail_activity_id')::bigint),
                   'error', CASE WHEN p_estado = 'error' THEN left(p_detalle, 300) ELSE NULL END,
                   'feedback', CASE WHEN p_estado IN ('hecha', 'cancelada') THEN left(p_detalle, 500) ELSE delegacion->>'feedback' END),
    estado = CASE p_estado WHEN 'hecha' THEN 'resuelta' WHEN 'cancelada' THEN 'abierta' ELSE estado END,
    resuelta_en = CASE p_estado WHEN 'hecha' THEN now() WHEN 'cancelada' THEN NULL ELSE resuelta_en END,
    -- cierre humano pegajoso: situacion_guardar no la reabre mientras la señal no crezca (n / valor de hoy).
    evidencia = CASE WHEN p_estado = 'hecha'
                     THEN coalesce(evidencia, '{}'::jsonb) || jsonb_build_object('cerrada_manual', jsonb_build_object('n', n_senales, 'valor', valor, 'fecha', now(), 'por', v_nombre))
                     ELSE evidencia END,
    version = CASE WHEN p_estado IN ('hecha', 'cancelada') THEN version + 1 ELSE version END,
    ultimo_cambio = CASE p_estado WHEN 'hecha' THEN 'cerrada por ' || v_nombre || coalesce(': ' || left(p_detalle, 120), '')
                                  WHEN 'cancelada' THEN 'delegación cancelada por ' || v_nombre || coalesce(': ' || left(p_detalle, 120), '')
                                  WHEN 'error' THEN 'delegación en error: ' || coalesce(left(p_detalle, 120), '?')
                                  ELSE ultimo_cambio END,
    ultimo_cambio_en = CASE WHEN p_estado = 'creada' THEN ultimo_cambio_en ELSE now() END,
    historia = historia || jsonb_build_object('fecha', now(),
                 'evento', CASE p_estado WHEN 'hecha' THEN 'resuelta' WHEN 'cancelada' THEN 'reabierta' WHEN 'error' THEN 'delegacion_error' ELSE 'delegacion_creada' END,
                 'detalle', CASE p_estado WHEN 'hecha' THEN 'cerrada por ' || v_nombre || coalesce(': ' || left(p_detalle, 300), '')
                                          WHEN 'cancelada' THEN 'delegación cancelada por ' || v_nombre || coalesce(': ' || left(p_detalle, 300), '')
                                          WHEN 'error' THEN coalesce(left(p_detalle, 300), 'sin detalle')
                                          ELSE 'actividad ' || p_mail_activity_id || ' en Odoo' END),
    updated_at = now()
  WHERE id = p_situacion_id;
  RETURN jsonb_build_object('ok', true, 'id', p_situacion_id, 'estado', (SELECT estado FROM situaciones WHERE id = p_situacion_id), 'delegacion_estado', p_estado);
END $$;
REVOKE ALL ON FUNCTION public.situacion_delegacion_confirmar(bigint, bigint, text, text) FROM public, anon, authenticated;

CREATE OR REPLACE FUNCTION public.situacion_delegaciones_abiertas()
RETURNS TABLE (situacion_id bigint, mail_activity_id bigint, user_id integer, vence date, fecha timestamptz, titulo text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT s.id, (s.delegacion->>'mail_activity_id')::bigint, (s.delegacion->>'user_id')::int, situacion_fecha(s.delegacion->>'vence'), (s.delegacion->>'fecha')::timestamptz, s.titulo
  FROM situaciones s WHERE s.estado = 'delegada' AND s.delegacion->>'estado' = 'creada' AND s.delegacion->>'mail_activity_id' IS NOT NULL
  ORDER BY s.id
$$;
REVOKE ALL ON FUNCTION public.situacion_delegaciones_abiertas() FROM public, anon, authenticated;

-- Aplica los eventos que mandó Odoo (señal delegacion_estado) y marca en error las pendientes viejas. La llama situacion_ciclo.
CREATE OR REPLACE FUNCTION public.situacion_delegaciones_aplicar()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE x record; n int := 0; n_err int := 0;
BEGIN
  FOR x IN SELECT s.id, s.payload FROM senales s
           WHERE s.senal = 'delegacion_estado' AND s.resuelta_en IS NULL
             AND s.payload->>'evento' IN ('hecha', 'cancelada') AND coalesce((s.payload->>'aplicado')::bool, false) = false
           ORDER BY s.id LOOP
    BEGIN
      PERFORM situacion_delegacion_confirmar((x.payload->>'situacion_id')::bigint, (x.payload->>'mail_activity_id')::bigint, x.payload->>'evento', x.payload->>'feedback');
      UPDATE senales SET payload = payload || jsonb_build_object('aplicado', true, 'aplicado_en', now()) WHERE id = x.id;
      n := n + 1;
    EXCEPTION WHEN OTHERS THEN
      UPDATE senales SET payload = payload || jsonb_build_object('aplicado', true, 'aplicado_en', now(), 'error', left(SQLERRM, 200)) WHERE id = x.id;
      n_err := n_err + 1;
    END;
  END LOOP;
  UPDATE situaciones SET
    delegacion = delegacion || jsonb_build_object('estado', 'error', 'fecha_estado', now(), 'error', 'Odoo no creó la actividad en 15 min (¿pull apagado?)'),
    ultimo_cambio = 'delegación en error: Odoo no creó la actividad', ultimo_cambio_en = now(), updated_at = now(),
    historia = historia || jsonb_build_object('fecha', now(), 'evento', 'delegacion_error', 'detalle', 'sin actividad en Odoo 15 min después del comando')
  WHERE estado = 'delegada' AND delegacion->>'estado' = 'pendiente' AND (delegacion->>'fecha')::timestamptz < now() - interval '15 minutes';
  RETURN jsonb_build_object('aplicadas', n, 'errores', n_err);
END $$;
REVOKE ALL ON FUNCTION public.situacion_delegaciones_aplicar() FROM public, anon, authenticated;

-- situacion_ciclo: aplicar delegaciones antes de calidad/situaciones. (Mismo cuerpo que 20260919d + una línea.)
CREATE OR REPLACE FUNCTION public.situacion_ciclo(p_corrida uuid DEFAULT gen_random_uuid(), p_origen text DEFAULT 'manual')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE v_id bigint; r_mem jsonb; r_cal jsonb; r_sit jsonb; r_del jsonb; n_sen int;
BEGIN
  INSERT INTO situacion_corridas (corrida, origen) VALUES (p_corrida, p_origen) RETURNING id INTO v_id;
  r_mem := senales_memoria(p_corrida);
  r_del := situacion_delegaciones_aplicar();
  r_cal := senales_actualizar();
  r_sit := situacion_guardar(p_corrida);
  SELECT count(*) INTO n_sen FROM senales WHERE resuelta_en IS NULL;
  UPDATE situacion_corridas SET sql_lista_en = now(), n_senales = n_sen,
    n_nuevas = (r_sit->>'nuevas')::int, n_actualizadas = (r_sit->>'actualizadas')::int, n_resueltas = (r_sit->>'resueltas')::int,
    n_ignoradas = (r_sit->>'ignoradas')::int,
    detalle = jsonb_build_object('memoria', r_mem, 'delegaciones', r_del, 'calidad', r_cal, 'situaciones', r_sit)
  WHERE id = v_id;
  RETURN jsonb_build_object('corrida_id', v_id, 'corrida', p_corrida, 'senales', n_sen, 'delegaciones', r_del, 'calidad', r_cal, 'situaciones', r_sit);
END $$;

-- situacion_redactar: respeta severidad_fija / responsable_fijo y evidencia.no_fusionar. (Cuerpo de 20260919e con tres cambios marcados.)
CREATE OR REPLACE FUNCTION public.situacion_redactar(p_id bigint, p jsonb, p_modelo text DEFAULT NULL, p_corrida_id bigint DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE s situaciones%ROWTYPE; cfg record; v_sev int; d jsonb; n_fus int := 0; v_user int; v_evento text; v_regla record;
BEGIN
  SELECT * INTO s FROM situaciones WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'situación % no existe', p_id; END IF;
  SELECT * INTO cfg FROM senales_config WHERE senal = s.senal;
  v_sev := least(greatest(coalesce((p->>'severidad')::int, s.severidad), cfg.severidad_base), cfg.severidad_max);
  v_user := (p->>'responsable_sugerido_user_id')::int;
  IF v_user IS NOT NULL AND NOT EXISTS (SELECT 1 FROM odoo_users WHERE odoo_user_id = v_user) THEN v_user := NULL; END IF;
  -- plan B: reglas fijas del director mandan sobre la IA.
  FOR v_regla IN SELECT r.accion, r.valor FROM situacion_reglas r
                 WHERE r.accion IN ('severidad_fija', 'responsable_fijo') AND (r.vigente_hasta IS NULL OR r.vigente_hasta > now())
                   AND ((r.alcance = 'situacion' AND r.clave_alcance = s.clave) OR (r.alcance = 'senal' AND r.clave_alcance = s.senal)
                     OR (r.alcance = 'contraparte' AND r.clave_alcance IN ('company:' || s.company_id, 'partner:' || s.odoo_partner_id))
                     OR (r.alcance = 'documento' AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.documentos) dd WHERE (dd->>'modelo') || ':' || (dd->>'id') = r.clave_alcance)))
                 ORDER BY r.creada_en DESC LOOP
    IF v_regla.accion = 'severidad_fija' AND (v_regla.valor->>'severidad') IS NOT NULL THEN v_sev := (v_regla.valor->>'severidad')::int; END IF;
    IF v_regla.accion = 'responsable_fijo' AND (v_regla.valor->>'user_id') IS NOT NULL THEN v_user := (v_regla.valor->>'user_id')::int; END IF;
  END LOOP;
  v_evento := coalesce(nullif(p->>'evento_historia', ''), 'redactada');

  FOR d IN SELECT * FROM jsonb_array_elements(coalesce(p->'duplicados', '[]'::jsonb)) LOOP
    IF d->>'decision' = 'fusionar' AND (d->>'id')::bigint <> p_id
       AND NOT coalesce(s.evidencia->'no_fusionar', '[]'::jsonb) @> to_jsonb((d->>'id')::bigint) THEN   -- plan B: lo que el director separó no se refusiona
      UPDATE situaciones h SET fusionada_en = p_id, updated_at = now(),
        historia = h.historia || jsonb_build_object('fecha', now(), 'evento', 'fusionada', 'detalle', 'en #' || p_id || ': ' || coalesce(d->>'motivo', ''), 'corrida', p_corrida_id)
      WHERE h.id = (d->>'id')::bigint AND h.fusionada_en IS NULL AND h.estado NOT IN ('resuelta', 'descartada')
        AND h.company_id IS NOT DISTINCT FROM s.company_id;
      IF FOUND THEN
        n_fus := n_fus + 1;
        UPDATE situaciones SET
          documentos = (SELECT coalesce(jsonb_agg(x), '[]') FROM (SELECT DISTINCT x FROM jsonb_array_elements(s.documentos || (SELECT documentos FROM situaciones WHERE id = (d->>'id')::bigint)) x LIMIT 60) q),
          evidencia = s.evidencia || jsonb_build_object('fusionadas', coalesce(s.evidencia->'fusionadas', '[]'::jsonb) || to_jsonb((d->>'id')::bigint)),
          historia = historia || jsonb_build_object('fecha', now(), 'evento', 'fusion', 'detalle', 'absorbe #' || (d->>'id') || ': ' || coalesce(d->>'motivo', ''), 'corrida', p_corrida_id)
        WHERE id = p_id;
        SELECT * INTO s FROM situaciones WHERE id = p_id;
      END IF;
    END IF;
  END LOOP;

  UPDATE situaciones SET
    titulo = coalesce(nullif(left(p->>'titulo', 160), ''), titulo),
    resumen = coalesce(nullif(p->>'resumen', ''), resumen),
    recomendacion = coalesce(nullif(p->>'recomendacion', ''), recomendacion),
    severidad = v_sev,
    responsable_sugerido_user_id = coalesce(v_user, responsable_sugerido_user_id),
    responsable_motivo = coalesce(nullif(p->>'responsable_motivo', ''), responsable_motivo),
    ultimo_cambio = CASE WHEN v_evento <> 'redactada' THEN left(v_evento, 300) ELSE ultimo_cambio END,
    historia = historia || jsonb_build_object('fecha', now(), 'evento', 'redactada', 'detalle', left(v_evento, 300), 'modelo', p_modelo, 'corrida', p_corrida_id),
    ia_version = version, ia_modelo = p_modelo, updated_at = now()
  WHERE id = p_id;
  RETURN jsonb_build_object('ok', true, 'id', p_id, 'severidad', v_sev, 'fusiones', n_fus, 'ia_version', s.version);
END $$;

INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Situación plan B paso 5: sync_commands.payload, situacion_decidir, situacion_delegacion_confirmar, delegaciones abiertas/aplicar en el ciclo, reglas fijas y no_fusionar en situacion_redactar',
        jsonb_build_object('migration', '20260925a_situacion_decidir'));
COMMIT;
```

- [ ] **Step 4: Aplicar y correr** `06_decidir.sql`, `05_cambios.sql`, `04_lectura.sql` y `03_*` (regresión del ciclo): todas `PRUEBA_OK`. Si `situacion_regla_aplica … LIKE 'regla #N %'` no casa (formato del texto), ajusta el LIKE al formato real de esa función (`'regla #' || r.id || ' ('…`).

- [ ] **Step 5: Probar por MCP con una situación real de prueba baja** (no delegar nada real todavía: el pull de Odoo aún no entiende `crear_actividad`): `select situacion_decidir(<id de una situación de sat_discrepancia · moneda>, 'severidad', '{"severidad":2,"motivo":"prueba"}');` y luego `select situacion_decidir(<mismo id>, 'reabrir', '{}')` no aplica; en su lugar vence la regla a mano: `update situacion_reglas set vigente_hasta = now() where clave_alcance = '<clave>' and accion = 'severidad_fija';` y `update situaciones set severidad = 3 where id = <id>;` — deja el mapa como estaba.

- [ ] **Step 6: Commit** — "Situación: situacion_decidir, delegación confirmada desde Odoo y reglas fijas en el bot (plan B, paso 5, Supabase)".

### Task 5.2: Docs de Supabase y PR del paso 5 (parte Supabase)

- [ ] **Step 1: `CLAUDE.md`:** en "Situación de la empresa" agregar **Decidir** (las 7 acciones con ejemplos MCP), **Delegación** (flujo: `situacion_decidir` → `sync_commands.payload` → pull de Odoo crea `mail.activity` → `situacion_delegacion_confirmar('creada')` → hooks de Odoo → push `delegacion_estado` → `situacion_delegaciones_aplicar` en el ciclo → `resuelta`/`abierta`; estados `pendiente|creada|error|hecha|cancelada|cerrada_por_director`; 15 min → `error`), tabla `sync_commands` con `payload`, RPCs nuevas en la lista.
- [ ] **Step 2: PR** "Situación plan B, paso 5 (Supabase): situacion_decidir y delegación" → CI → merge → rama.

### Task 5.3: `quimibond_intelligence`: pull con `payload` y `failed`, `obligacion_legado` vacía, intervalo del cron

**Files:**
- Modify: `addons/quimibond_intelligence/models/sync_pull.py:82-131`
- Modify: `addons/quimibond_intelligence/models/sync_push.py:249-255` (`_push_metodos`)
- Modify: `addons/quimibond_intelligence/models/senales/direccion.py` (función `obligacion_legado`)
- Modify: `addons/quimibond_intelligence/data/cleanup_2026_09_18.xml`
- Test: `addons/quimibond_intelligence/tests/test_pull_commands.py` (nuevo; agregar a `tests/__init__.py` y a `collect_ignore_glob` de `tests/conftest.py`)

- [ ] **Step 1: Test** `tests/test_pull_commands.py`:

```python
# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


class ClientePull:
    """Cliente falso del pull: devuelve comandos y guarda los patch."""
    def __init__(self, comandos):
        self.comandos, self.patches, self.rpcs = comandos, [], []

    def fetch(self, table, params=None):
        return self.comandos if table == 'sync_commands' else []

    def patch(self, table, filters, data):
        self.patches.append((table, filters, data))

    def rpc_strict(self, fn, params, timeout=120.0):
        self.rpcs.append((fn, params))
        return {'ok': True}

    def close(self):
        pass


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestPullCommands(TransactionCase):

    def test_execute_command_recibe_payload_y_error_es_failed(self):
        Pull = self.env['quimibond.sync.pull']
        c = ClientePull([{'id': 1, 'command': 'desconocido', 'payload': {'x': 1}}, {'id': 2, 'command': '_revienta', 'payload': None}])
        vistos = []

        def fake_execute(command, payload=None):
            vistos.append((command, payload))
            if command == '_revienta':
                raise ValueError('boom')
            return 'ok'
        self.patch(type(Pull), '_execute_command', lambda _self, command, payload=None: fake_execute(command, payload))
        n = Pull._process_commands(c)
        self.assertEqual(n, 1)
        self.assertEqual(vistos, [('desconocido', {'x': 1}), ('_revienta', None)])
        estados = [d['status'] for _t, f, d in c.patches if f == 'id=eq.2']
        self.assertEqual(estados, ['running', 'failed'], 'el CHECK de sync_commands no admite error')

    def test_comando_desconocido_no_revienta(self):
        self.assertIn('Unknown command', self.env['quimibond.sync.pull']._execute_command('nada', {}))
```

- [ ] **Step 2: Cambios en `sync_pull.py`:** en `_process_commands`, leer `payload = cmd.get('payload') or None` (pedir la columna: el `fetch` sin `select` trae todas) y llamar `self._execute_command(command, payload)`; en el `except`, `'status': 'failed'`. Firma nueva: `def _execute_command(self, command: str, payload=None) -> str:` (los comandos viejos ignoran `payload`). Actualiza el docstring del módulo: "comandos: force_push, force_push_full, sync_contacts; `crear_actividad` lo agrega qb_situacion".

- [ ] **Step 3: `_push_metodos` en `sync_push.py`:** sacar la lista `methods` de `push_to_supabase` a un método que `qb_situacion` pueda extender (Tarea 5.6) sin copiar `push_to_supabase`:

```python
    def _push_metodos(self):
        """[(etiqueta, método)] en orden de ejecución. Los módulos que agregan un push
        (qb_situacion: actividades_delegadas) lo extienden aquí."""
        return [
            ('contacts', self._push_contacts),
            ('users', self._push_users),
            ('senales', self._push_senales),
        ]
```

  y en `push_to_supabase`: `methods = self._push_metodos()` (borrar la lista inline). En `tests/test_push_senales.py::test_push_to_supabase_incluye_senales` agregar:

```python
        etiquetas = [l for l, _ in self.sync._push_metodos()]
        self.assertEqual(etiquetas[:2], ['contacts', 'users'])
        self.assertEqual(etiquetas[-1], 'senales')   # senales siempre al final: el bot se dispara al terminar
```

  **No** afirmes la lista exacta aquí: el CI instala `qb_situacion` en la misma base (`-i …,qb_situacion`, tests `post_install`) y la Tarea 5.6 inserta `actividades_delegadas` en esa lista; la afirmación exacta vive solo en `qb_situacion/tests/test_push_delegadas.py`. Sin modelo nuevo ni bump.

- [ ] **Step 4: `obligacion_legado`:** hoy devuelve `None` si `qb.obligation` no está instalado (= "no aplica", no manda lote, las señales quedan `sin_datos`). Cambiar a `return []` en ese caso, con comentario: "plan B paso 6: al desinstalar qb_obligation el siguiente push manda lote vacío y resuelve todo lo abierto (spec: último lote vacío antes de desinstalar)". Ajustar el test `test_obligacion_legado_solo_abiertas` (o el que cubra el caso "no instalado") a `[]`.

- [ ] **Step 5: `cleanup_2026_09_18.xml`:** después del `<function model="ir.cron" name="write">` que enciende los crons, agregar otro bloque que fije el intervalo del push (el registro de producción es `noupdate` y hoy corre cada día, no cada hora):

```xml
    <!-- 2026-09-23: en producción el push corría UNA vez al día (04:48 UTC): el registro es
         noupdate y conserva un intervalo viejo. El intervalo se fija por código en cada update.
         Solo el push; el pull ya está en 5 minutos. -->
    <function model="ir.cron" name="write">
        <function model="ir.cron" name="search">
            <value eval="[('name', '=', 'Quimibond Sync - Push to Supabase (every 1h)'), '|', ('interval_number', '!=', 1), ('interval_type', '!=', 'hours')]"/>
            <value name="context" eval="{'active_test': False}"/>
        </function>
        <value eval="{'interval_number': 1, 'interval_type': 'hours'}"/>
    </function>
```

  **Ojo:** `ir.cron.write` sobre un cron que está corriendo en ese instante aborta el update ("This cron task is currently being executed"); el push dura < 60 s y el `odoo-update` se corre a mano, así que el riesgo es bajo; si pasa, repetir el `odoo-update`.

- [ ] **Step 6: Verificación local:** `flake8 addons/ && python3 -m pytest addons/quimibond_intelligence/tests -q` (los 9 de pytest siguen), `python3 tools/check_addons.py --base-ref origin/main` (WARN por archivos sin bump, 0 errores: no hay modelo nuevo aquí). XML: `python3 -c "import xml.dom.minidom as m; m.parse('addons/quimibond_intelligence/data/cleanup_2026_09_18.xml')"`.

- [ ] **Step 7: Commit** — "quimibond_intelligence: pull con payload y failed, _push_metodos, obligacion_legado vacía sin módulo, intervalo del push fijo".

### Task 5.4: Módulo `qb_situacion` (esqueleto, modelo de eventos y hooks de `mail.activity`)

**Files:**
- Create: `addons/qb_situacion/__init__.py`, `__manifest__.py`, `models/__init__.py`, `models/delegacion_evento.py`, `models/mail_activity.py`, `security/qb_situacion_security.xml`, `security/ir.model.access.csv`, `data/mail_activity_type_data.xml`, `tests/__init__.py`, `tests/test_hooks.py`, `README.md`
- Modify: `.github/workflows/ci.yml:132-133` (agregar `qb_situacion` a `-i` y `/qb_situacion` a `--test-tags`)

- [ ] **Step 1: `__manifest__.py`:**

```python
{
    'name': 'Quimibond Situación',
    'version': '19.0.1.0.0',
    'license': 'LGPL-3',
    'category': 'Productivity',
    'summary': 'Delegación del mapa de situación: actividades nativas desde Supabase y su estado de vuelta.',
    'description': """
        Plan B del mapa de situación (spec docs/superpowers/specs/2026-09-18-situacion-empresa-design.md, §7.3):
          * comando `crear_actividad` del pull de quimibond_intelligence → mail.activity con situacion_id
          * hooks de mail.activity (hecha / cancelada) → qb.delegacion.evento
          * push horario `_push_actividades_delegadas` → señal delegacion_estado en Supabase
        La app "Situación" (lectura del mapa y botón Delegar) llega en 19.0.2.0.0.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['mail', 'quimibond_intelligence', 'qb_memoria'],
    'data': [
        'security/qb_situacion_security.xml',
        'security/ir.model.access.csv',
        'data/mail_activity_type_data.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
```

`__init__.py`: `from . import models`. `models/__init__.py`: `from . import delegacion_evento, mail_activity, sync_pull, sync_push` (los dos últimos llegan en 5.5 y 5.6: créalos vacíos ahora o agrégalos al import cuando existan).

- [ ] **Step 2: `security/qb_situacion_security.xml`** (grupo del director; se usa desde el paso 6 para la app, aquí solo se crea):

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo>
    <record id="group_ceo" model="res.groups">
        <field name="name">Situación: director</field>
        <field name="comment">Ve el mapa de situación de la empresa y delega desde Odoo.</field>
        <field name="implied_ids" eval="[(4, ref('base.group_user'))]"/>
        <field name="user_ids" eval="[(4, ref('base.user_admin'))]"/>
    </record>
</odoo>
```

`security/ir.model.access.csv`:

```
id,name,model_id:id,group_id:id,perm_read,perm_write,perm_create,perm_unlink
access_qb_delegacion_evento_user,qb.delegacion.evento user,model_qb_delegacion_evento,base.group_user,1,0,1,0
access_qb_delegacion_evento_system,qb.delegacion.evento system,model_qb_delegacion_evento,base.group_system,1,1,1,1
```

`data/mail_activity_type_data.xml` (noupdate):

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo>
    <data noupdate="1">
        <record id="mail_activity_type_situacion" model="mail.activity.type">
            <field name="name">Situación delegada</field>
            <field name="icon">fa-map-marker</field>
            <field name="sequence">46</field>
            <field name="res_model" eval="False"/>
            <field name="chaining_type">suggest</field>
            <field name="delay_count">0</field>
            <field name="summary">Situación</field>
            <!-- Odoo 17+: _action_done ARCHIVA solo las actividades cuyo tipo tiene keep_done; las demás las BORRA.
                 Sin esto, test_hecha_deja_evento_y_no_cancelada falla y el push nunca ve la actividad archivada. -->
            <field name="keep_done" eval="True"/>
        </record>
    </data>
</odoo>
```

- [ ] **Step 3: `models/delegacion_evento.py`:**

```python
# -*- coding: utf-8 -*-
"""Eventos de las actividades delegadas desde el mapa de situación (spec §7.3).

Odoo 19 archiva la actividad al marcarla hecha (active=False, feedback) y la borra al
cancelarla, así que el evento se escribe en el momento (hooks de mail.activity) y el push
horario lo manda a Supabase como lote de la señal delegacion_estado."""
from odoo import fields, models


class QbDelegacionEvento(models.Model):
    _name = 'qb.delegacion.evento'
    _description = 'Evento de actividad delegada (situación)'
    _order = 'fecha desc, id desc'

    situacion_id = fields.Integer(string='Situación (Supabase)', required=True, index=True)
    mail_activity_id = fields.Integer(string='Actividad', required=True, index=True)
    evento = fields.Selection([('hecha', 'Hecha'), ('cancelada', 'Cancelada')], required=True)
    feedback = fields.Text()
    user_id = fields.Many2one('res.users', string='Quién', ondelete='set null')
    fecha = fields.Datetime(required=True, default=fields.Datetime.now)
    enviado = fields.Boolean(default=False, index=True, help='Ya llegó a Supabase (lote delegacion_estado con respuesta ok).')
    enviado_en = fields.Datetime()
```

- [ ] **Step 4: `models/mail_activity.py`:**

```python
# -*- coding: utf-8 -*-
"""mail.activity con situacion_id: hecha → evento 'hecha' (Odoo archiva la actividad);
cancelada (unlink) → evento 'cancelada', salvo que en esta misma transacción ya se haya
marcado hecha (guardia por contexto, spec §7.3: en Odoo 19 _action_done solo borra las
actividades cuyo documento ya no existe, pero la guardia cubre justo ese caso)."""
from odoo import fields, models

CTX_HECHAS = 'qb_situacion_hechas'


class MailActivity(models.Model):
    _inherit = 'mail.activity'

    situacion_id = fields.Integer(string='Situación (Supabase)', index=True, copy=False,
                                  help='Id de la situación del mapa que delegó esta actividad.')

    def _qb_situacion_evento(self, evento, feedback=False):
        Evento = self.env['qb.delegacion.evento'].sudo()
        for act in self.filtered('situacion_id'):
            Evento.create({
                'situacion_id': act.situacion_id, 'mail_activity_id': act.id, 'evento': evento,
                'feedback': (feedback or '')[:1000] or False, 'user_id': self.env.user.id, 'fecha': fields.Datetime.now(),
            })

    def _action_done(self, feedback=False, attachment_ids=None):
        con = self.filtered(lambda a: a.situacion_id and a.active)
        if con:
            con._qb_situacion_evento('hecha', feedback)
            hechas = set(self.env.context.get(CTX_HECHAS, ())) | set(con.ids)
            return super(MailActivity, self.with_context(**{CTX_HECHAS: tuple(hechas)}))._action_done(feedback=feedback, attachment_ids=attachment_ids)
        return super()._action_done(feedback=feedback, attachment_ids=attachment_ids)

    def unlink(self):
        hechas = set(self.env.context.get(CTX_HECHAS, ()))
        # Solo actividades vivas: una archivada (hecha) que se borra después no es una cancelación.
        canceladas = self.filtered(lambda a: a.situacion_id and a.active and a.id not in hechas)
        if canceladas:
            canceladas._qb_situacion_evento('cancelada', self.env.context.get('qb_situacion_motivo'))
        return super().unlink()
```

- [ ] **Step 5: Test `tests/test_hooks.py`:**

```python
# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install', 'qb_situacion')
class TestHooks(TransactionCase):

    def _actividad(self, situacion_id=101):
        partner = self.env['res.partner'].create({'name': 'Cliente Situación'})
        return partner.activity_schedule('qb_situacion.mail_activity_type_situacion', summary='Cobrar', user_id=self.env.user.id,
                                         situacion_id=situacion_id)

    def _eventos(self, situacion_id=101):
        return self.env['qb.delegacion.evento'].search([('situacion_id', '=', situacion_id)], order='id')

    def test_hecha_deja_evento_y_no_cancelada(self):
        act = self._actividad()
        act.action_feedback(feedback='Ya pagó')
        ev = self._eventos()
        self.assertEqual([e.evento for e in ev], ['hecha'])
        self.assertEqual(ev.feedback, 'Ya pagó')
        self.assertEqual(ev.user_id, self.env.user)
        self.assertFalse(ev.enviado)
        # archivada, no borrada (keep_done=True en el tipo); borrarla después no genera 'cancelada'
        self.assertTrue(act.exists() and not act.active)
        act.unlink()
        self.assertEqual([e.evento for e in self._eventos()], ['hecha'])

    def test_cancelada_deja_evento(self):
        act = self._actividad(102)
        act.unlink()
        self.assertEqual([e.evento for e in self._eventos(102)], ['cancelada'])

    def test_documento_borrado_cancela(self):
        # Borrar el documento ancla borra sus actividades (mail.activity.mixin.unlink) → evento 'cancelada':
        # la situación se reabre en Supabase y, si la señal ya no viene de Odoo, el siguiente push la resuelve.
        act = self._actividad(103)
        self.env['res.partner'].browse(act.res_id).unlink()
        self.assertEqual([e.evento for e in self._eventos(103)], ['cancelada'])

    def test_sin_situacion_no_hay_evento(self):
        partner = self.env['res.partner'].create({'name': 'Otro'})
        act = partner.activity_schedule('mail.mail_activity_data_todo', summary='x', user_id=self.env.user.id)
        act.action_feedback()
        self.assertFalse(self.env['qb.delegacion.evento'].search([]))
```

  `tests/__init__.py`: `from . import test_hooks` (y después `test_pull_delegar`, `test_push_delegadas`).

- [ ] **Step 6: CI:** en `.github/workflows/ci.yml` línea 132 agregar `,qb_situacion` a la lista de `-i`; línea 133 agregar `,/qb_situacion` a `--test-tags`.

- [ ] **Step 7: Verificación local:** `flake8 addons/qb_situacion && python -m compileall -q addons/qb_situacion && python3 tools/check_addons.py --base-ref origin/main` (módulo nuevo con versión: 0 errores) y los XML parseados. Commit — "qb_situacion 19.0.1.0.0: eventos de actividades delegadas y hooks de mail.activity". Push y leer el job `odoo-tests`. Decisión tomada: borrar el documento ancla cuenta como **cancelada** (`test_documento_borrado_cancela`); va en el README del módulo.

### Task 5.5: Comando `crear_actividad` en el pull

**Files:**
- Create: `addons/qb_situacion/models/sync_pull.py`
- Test: `addons/qb_situacion/tests/test_pull_delegar.py`

- [ ] **Step 1: Test:**

```python
# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


class ClienteRpc:
    def __init__(self, fallar=False):
        self.calls, self.fallar = [], fallar

    def rpc_strict(self, fn, params, timeout=120.0):
        self.calls.append((fn, params))
        if self.fallar:
            from odoo.addons.quimibond_intelligence.models.supabase_client import SupabaseError
            raise SupabaseError('HTTP 500 simulado')
        return {'ok': True}

    def close(self):
        pass


@tagged('post_install', '-at_install', 'qb_situacion')
class TestPullDelegar(TransactionCase):

    def setUp(self):
        super().setUp()
        self.Pull = self.env['quimibond.sync.pull']
        self.partner = self.env['res.partner'].create({'name': 'ACME'})
        self.user = self.env.ref('base.user_admin')
        self.cliente = ClienteRpc()
        self.patch(type(self.Pull), '_qb_situacion_client', lambda _self: self.cliente)

    def _payload(self, **kw):
        base = {'situacion_id': 501, 'user_id': self.user.id, 'texto': 'Cobrar F/1', 'vence': '2026-09-30', 'modelo': 'res.partner', 'res_id': self.partner.id, 'titulo': 'Cartera vencida · ACME'}
        base.update(kw)
        return base

    def test_crea_actividad_sobre_el_documento_y_confirma(self):
        r = self.Pull._execute_command('crear_actividad', self._payload())
        act = self.env['mail.activity'].search([('situacion_id', '=', 501)])
        self.assertEqual(len(act), 1)
        self.assertEqual((act.res_model, act.res_id, act.user_id), ('res.partner', self.partner.id, self.user))
        self.assertEqual(str(act.date_deadline), '2026-09-30')
        self.assertIn('Cobrar F/1', act.note)
        self.assertEqual(act.activity_type_id, self.env.ref('qb_situacion.mail_activity_type_situacion'))
        fn, params = self.cliente.calls[-1]
        self.assertEqual((fn, params['p_situacion_id'], params['p_mail_activity_id'], params['p_estado']), ('situacion_delegacion_confirmar', 501, act.id, 'creada'))
        self.assertIn(str(act.id), r)

    def test_documento_inexistente_cae_al_contacto_y_sin_contacto_al_del_usuario(self):
        self.Pull._execute_command('crear_actividad', self._payload(situacion_id=502, modelo='account.move', res_id=999999999, partner_id=self.partner.id))
        act = self.env['mail.activity'].search([('situacion_id', '=', 502)])
        self.assertEqual((act.res_model, act.res_id), ('res.partner', self.partner.id))
        self.Pull._execute_command('crear_actividad', self._payload(situacion_id=503, modelo=None, res_id=None))
        act = self.env['mail.activity'].search([('situacion_id', '=', 503)])
        self.assertEqual((act.res_model, act.res_id), ('res.partner', self.user.partner_id.id))

    def test_usuario_invalido_confirma_error(self):
        try:
            self.Pull._execute_command('crear_actividad', self._payload(situacion_id=504, user_id=999999))
            self.fail('debió fallar')
        except Exception:
            pass
        fn, params = self.cliente.calls[-1]
        self.assertEqual((fn, params['p_situacion_id'], params['p_estado']), ('situacion_delegacion_confirmar', 504, 'error'))
        self.assertFalse(self.env['mail.activity'].search([('situacion_id', '=', 504)]))

    def test_idempotente_por_situacion(self):
        self.Pull._execute_command('crear_actividad', self._payload(situacion_id=505))
        self.Pull._execute_command('crear_actividad', self._payload(situacion_id=505))
        self.assertEqual(len(self.env['mail.activity'].search([('situacion_id', '=', 505)])), 1)
```

- [ ] **Step 2: `models/sync_pull.py`:**

```python
# -*- coding: utf-8 -*-
"""Comando `crear_actividad` (spec §7.3): la delegación del director se vuelve una mail.activity
nativa sobre el documento (o el contacto, o el propio usuario) y se confirma a Supabase con
situacion_delegacion_confirmar. Si algo falla, se confirma 'error' con el motivo y se relanza
(el pull marca el comando failed)."""
import logging

from markupsafe import Markup, escape

from odoo import _, fields, models

from odoo.addons.quimibond_intelligence.models.sync_pull import _get_client

_logger = logging.getLogger(__name__)


class QuimibondSyncPullSituacion(models.TransientModel):
    _inherit = 'quimibond.sync.pull'

    def _qb_situacion_client(self):
        return _get_client(self.env)

    def _execute_command(self, command, payload=None):
        if command != 'crear_actividad':
            return super()._execute_command(command, payload)
        return self._crear_actividad_delegada(payload or {})

    def _qb_confirmar(self, client, situacion_id, activity_id, estado, detalle=None):
        client.rpc_strict('situacion_delegacion_confirmar', {
            'p_situacion_id': situacion_id, 'p_mail_activity_id': activity_id, 'p_estado': estado, 'p_detalle': detalle and str(detalle)[:500]})

    def _qb_destino(self, payload):
        """Documento (modelo + id existente con actividades), si no el contacto, si no el usuario."""
        modelo, res_id = payload.get('modelo'), payload.get('res_id')
        if modelo and res_id and modelo in self.env and 'activity_ids' in self.env[modelo]._fields:
            rec = self.env[modelo].sudo().browse(int(res_id)).exists()
            if rec:
                return rec
        partner_id = payload.get('partner_id') or (res_id if modelo == 'res.partner' else None)
        if partner_id:
            partner = self.env['res.partner'].sudo().browse(int(partner_id)).exists()
            if partner:
                return partner
        # res.users no lleva mail.activity.mixin en Odoo 19: el último recurso es el contacto del propio usuario.
        return self.env['res.users'].sudo().browse(int(payload['user_id'])).partner_id

    def _crear_actividad_delegada(self, payload):
        client = self._qb_situacion_client()
        situacion_id = int(payload.get('situacion_id') or 0)
        if not client or not situacion_id:
            raise ValueError('crear_actividad: sin cliente o sin situacion_id')
        Activity = self.env['mail.activity'].sudo()
        existente = Activity.with_context(active_test=False).search([('situacion_id', '=', situacion_id)], limit=1)
        if existente:
            self._qb_confirmar(client, situacion_id, existente.id, 'creada')
            return 'Actividad %s ya existía' % existente.id
        try:
            user = self.env['res.users'].sudo().browse(int(payload.get('user_id') or 0)).exists()
            if not user or user.share:
                raise ValueError(_('user_id %s no es un usuario interno') % payload.get('user_id'))
            destino = self._qb_destino(payload)
            texto = payload.get('texto') or payload.get('titulo') or 'Situación delegada'
            vence = payload.get('vence') or fields.Date.to_string(fields.Date.add(fields.Date.context_today(self), days=3))
            note = Markup('<p>%s</p><p class="text-muted">Situación #%s del mapa: %s</p>') % (escape(texto), situacion_id, escape(payload.get('titulo') or ''))
            act = destino.with_context(mail_activity_automation_skip=False).activity_schedule(
                'qb_situacion.mail_activity_type_situacion', date_deadline=vence, summary=(payload.get('titulo') or texto)[:100],
                note=note, user_id=user.id, situacion_id=situacion_id, automated=False)
            if not act:
                raise ValueError('activity_schedule no devolvió actividad')
        except Exception as exc:  # noqa: BLE001 — se confirma el error y se relanza para que el comando quede failed
            _logger.warning('crear_actividad %s: %s', situacion_id, exc)
            try:
                self._qb_confirmar(client, situacion_id, None, 'error', exc)
            except Exception as exc2:  # noqa: BLE001
                _logger.warning('confirmar error %s: %s', situacion_id, exc2)
            raise
        self._qb_confirmar(client, situacion_id, act.id, 'creada')
        return 'Actividad %s creada en %s,%s para %s' % (act.id, act.res_model, act.res_id, user.name)
```

  **Nota:** el último recurso es el contacto del propio usuario (`res.users` no lleva `mail.activity.mixin` en Odoo 19). Documentarlo en el README.

- [ ] **Step 3: `tests/__init__.py` += `test_pull_delegar`. flake8, compileall, push, leer `odoo-tests`.** Commit — "qb_situacion: comando crear_actividad (delegación → mail.activity) y confirmación a Supabase".

### Task 5.6: `_push_actividades_delegadas` → señal `delegacion_estado`

**Files:**
- Create: `addons/qb_situacion/models/sync_push.py`
- Test: `addons/qb_situacion/tests/test_push_delegadas.py`

- [ ] **Step 1: Test:**

```python
# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


class ClientePush:
    def __init__(self, abiertas, fallar_ingesta=False, fallar_abiertas=False):
        self.abiertas, self.calls, self.fallar_ingesta, self.fallar_abiertas = abiertas, [], fallar_ingesta, fallar_abiertas

    def rpc_strict(self, fn, params, timeout=120.0):
        from odoo.addons.quimibond_intelligence.models.supabase_client import SupabaseError
        self.calls.append((fn, params))
        if fn == 'situacion_delegaciones_abiertas':
            if self.fallar_abiertas:
                raise SupabaseError('caído')
            return self.abiertas
        if fn == 'senales_ingestar':
            if self.fallar_ingesta:
                raise SupabaseError('HTTP 500')
            return {'ok': True, 'n': len(params['p_filas'])}
        return {'ok': True}

    def insert(self, table, rows, batch_size=200):
        return len(rows)

    def close(self):
        pass


@tagged('post_install', '-at_install', 'qb_situacion')
class TestPushDelegadas(TransactionCase):

    def setUp(self):
        super().setUp()
        self.Sync = self.env['quimibond.sync']
        self.partner = self.env['res.partner'].create({'name': 'ACME'})
        self.act = self.partner.activity_schedule('qb_situacion.mail_activity_type_situacion', summary='Cobrar', user_id=self.env.user.id, situacion_id=601)

    def _filas(self, c):
        return [p['p_filas'] for f, p in c.calls if f == 'senales_ingestar'][0]

    def test_abierta_hecha_y_desaparecida(self):
        # 601 abierta con actividad viva; 602 hecha (evento); 603 abierta en Supabase pero la actividad ya no existe y no hay evento.
        act2 = self.partner.activity_schedule('qb_situacion.mail_activity_type_situacion', summary='x', user_id=self.env.user.id, situacion_id=602)
        act2.action_feedback(feedback='listo')
        c = ClientePush([{'situacion_id': 601, 'mail_activity_id': self.act.id}, {'situacion_id': 602, 'mail_activity_id': act2.id}, {'situacion_id': 603, 'mail_activity_id': 999999}])
        n = self.Sync._push_actividades_delegadas(c)
        filas = {f['payload']['situacion_id']: f for f in self._filas(c)}
        self.assertEqual(set(filas), {601, 602, 603})
        self.assertEqual(filas[601]['payload']['evento'], 'abierta')
        self.assertEqual((filas[602]['payload']['evento'], filas[602]['payload']['feedback']), ('hecha', 'listo'))
        self.assertEqual(filas[603]['payload']['evento'], 'cancelada')
        self.assertTrue(all(f['clave'] == 'delegacion_estado:situacion:%s' % f['payload']['situacion_id'] for f in filas.values()))
        self.assertEqual(n, 3)
        ev = self.env['qb.delegacion.evento'].search([('situacion_id', '=', 602)])
        self.assertTrue(ev.enviado and ev.enviado_en)

    def test_si_falla_abiertas_no_manda_lote(self):
        c = ClientePush([], fallar_abiertas=True)
        try:
            self.Sync._push_actividades_delegadas(c)
            self.fail('debió fallar')
        except Exception:
            pass
        self.assertFalse([1 for f, _p in c.calls if f == 'senales_ingestar'])

    def test_si_falla_la_ingesta_el_evento_sigue_pendiente(self):
        self.act.action_feedback(feedback='ok')
        c = ClientePush([{'situacion_id': 601, 'mail_activity_id': self.act.id}], fallar_ingesta=True)
        try:
            self.Sync._push_actividades_delegadas(c)
            self.fail('debió fallar')
        except Exception:
            pass
        self.assertFalse(self.env['qb.delegacion.evento'].search([('situacion_id', '=', 601)]).enviado)

    def test_push_to_supabase_lo_incluye_antes_de_senales(self):
        self.assertIn('actividades_delegadas', self.Sync.PUSH_MODELS)
        self.assertIn('actividades_delegadas', self.Sync.FULL_PUSH_METHODS)
        etiquetas = [l for l, _ in self.Sync._push_metodos()]
        self.assertEqual(etiquetas, ['contacts', 'users', 'actividades_delegadas', 'senales'])
```

- [ ] **Step 2: `models/sync_push.py`:**

```python
# -*- coding: utf-8 -*-
"""Push de las actividades delegadas (spec §7.3): un lote de la señal delegacion_estado con
TODAS las delegaciones que Supabase cree abiertas (situacion_delegaciones_abiertas) y el estado
real en Odoo: 'abierta' (la actividad sigue viva), 'hecha'/'cancelada' (evento pendiente de
enviar, o la actividad ya no está). Si la consulta de abiertas falla NO se manda lote: un lote
vacío cerraría delegaciones por error. Los eventos se marcan enviados solo si el RPC respondió ok."""
import logging
import uuid

from odoo import fields, models

from odoo.addons.quimibond_intelligence.models.supabase_client import SupabaseError

_logger = logging.getLogger(__name__)


class QuimibondSyncSituacion(models.TransientModel):
    _inherit = 'quimibond.sync'

    PUSH_MODELS = ('contacts', 'users', 'actividades_delegadas', 'senales')
    PUSH_MODELS_DEFAULT = 'contacts,users,actividades_delegadas,senales'
    FULL_PUSH_METHODS = frozenset(['users', 'senales', 'actividades_delegadas'])

    def _push_metodos(self):
        """ANTES de senales: _push_senales termina con senales_push_terminado, que dispara el bot
        (situacion_ciclo → situacion_delegaciones_aplicar). Si el lote de delegaciones llegara
        después, hecha/cancelada se aplicarían hasta la corrida siguiente (una hora)."""
        metodos = super()._push_metodos()
        i = next((k for k, (etiqueta, _fn) in enumerate(metodos) if etiqueta == 'senales'), len(metodos))
        metodos.insert(i, ('actividades_delegadas', self._push_actividades_delegadas))
        return metodos

    def _push_actividades_delegadas(self, client, last_sync=None) -> int:
        abiertas = client.rpc_strict('situacion_delegaciones_abiertas', {})
        if not isinstance(abiertas, list):
            raise SupabaseError('situacion_delegaciones_abiertas no devolvió lista: %r' % (abiertas,))
        Evento = self.env['qb.delegacion.evento'].sudo()
        Activity = self.env['mail.activity'].sudo().with_context(active_test=False)
        pendientes = Evento.search([('enviado', '=', False)], order='id')
        por_situacion = {}
        for ev in pendientes:
            por_situacion.setdefault(ev.situacion_id, ev)   # el primero (más viejo) manda; los demás se marcan enviados igual
        filas, enviados = [], Evento
        hoy = fields.Date.context_today(self)
        for d in abiertas:
            sid = int(d['situacion_id'])
            ev = por_situacion.pop(sid, None)
            act = Activity.browse(int(d.get('mail_activity_id') or 0)).exists()
            if ev is not None:
                payload = {'situacion_id': sid, 'evento': ev.evento, 'feedback': ev.feedback or None, 'user_id': ev.user_id.id or None,
                           'fecha': fields.Datetime.to_string(ev.fecha), 'mail_activity_id': ev.mail_activity_id}
                enviados |= Evento.search([('situacion_id', '=', sid), ('enviado', '=', False)])
            elif act and act.active:
                payload = {'situacion_id': sid, 'evento': 'abierta', 'user_id': act.user_id.id, 'vence': fields.Date.to_string(act.date_deadline),
                           'dias_vencida': max((hoy - act.date_deadline).days, 0), 'mail_activity_id': act.id}
            elif act and not act.active:
                payload = {'situacion_id': sid, 'evento': 'hecha', 'feedback': act.feedback or 'hecha en Odoo (sin evento)', 'user_id': act.user_id.id,
                           'fecha': fields.Datetime.to_string(act.write_date), 'mail_activity_id': act.id}
            else:
                payload = {'situacion_id': sid, 'evento': 'cancelada', 'feedback': 'la actividad ya no existe en Odoo', 'mail_activity_id': d.get('mail_activity_id')}
            filas.append({'clave': 'delegacion_estado:situacion:%s' % sid, 'valor': payload.get('dias_vencida', 1) or 1,
                          'valor_texto': payload['evento'], 'responsable_odoo_user_id': payload.get('user_id'), 'payload': payload})
        # Eventos de situaciones que Supabase ya no lista (cerradas por el director): se dan por enviados.
        for sid, ev in por_situacion.items():
            enviados |= Evento.search([('situacion_id', '=', sid), ('enviado', '=', False)])
        res = client.rpc_strict('senales_ingestar', {'p_senal': 'delegacion_estado', 'p_fuente': 'odoo', 'p_corrida': str(uuid.uuid4()), 'p_filas': filas})
        if not isinstance(res, dict) or not res.get('ok'):
            raise SupabaseError('senales_ingestar delegacion_estado: %s' % ((res or {}).get('error') if isinstance(res, dict) else res))
        if enviados:
            enviados.write({'enviado': True, 'enviado_en': fields.Datetime.now()})
        return len(filas)
```

  **Depende de `_push_metodos`** (Tarea 5.3, Step 3): sin él este mixin no se engancha al push.

- [ ] **Step 3: `tests/__init__.py` += `test_push_delegadas`. flake8, compileall, push, leer `odoo-tests`.** Commit — "qb_situacion: push de actividades delegadas como señal delegacion_estado".

### Task 5.7: Docs, PR de qb19, despliegue y aceptación del paso 5

- [ ] **Step 1: `addons/qb_situacion/README.md`:** qué hace (delegación ida y vuelta), estados (incluido: si se borra el documento ancla, la actividad se borra con él y cuenta como cancelada; **hecha** o **cerrada por el director** es pegajoso: la situación no reabre mientras la señal no crezca, y si crece vuelve como "empeoró"), dónde se ve cada cosa (Historial de Sync, `pipeline_logs`, `situacion_salud`), cómo probar a mano (`select situacion_decidir(<id>,'delegar',…)` y esperar 5 min), decisiones (módulo aparte por el manifest congelado; último recurso al usuario/contacto).
- [ ] **Step 2: `CLAUDE.md` (qb19):** estructura (+`qb_situacion`), "Otros módulos" (+`qb_situacion`), "Modelos sincronizados" (+`_push_actividades_delegadas` → `senales` vía `delegacion_estado`), Crons (push incluye `actividades_delegadas`; pull entiende `crear_actividad`; nota del intervalo fijado por código). `docs/RUNBOOK_DESPLIEGUE.md`: sección "Delegar una situación" con la verificación (`select situacion_delegaciones_abiertas()`, actividad en Odoo, `select delegacion from situaciones where id = …`).
- [ ] **Step 3: PR** "Situación plan B, paso 5: qb_situacion (delegación ida y vuelta) y pull con payload" (borrador → CI `check` + `odoo-tests` → ready → squash-merge → rama) y PR "Merge main into quimibond" (merge commit). Cuerpo con la plantilla del repo. Luego el CEO: `odoo-update quimibond_intelligence,qb_situacion && odoosh-restart http && odoosh-restart cron` — **`qb_situacion` es módulo nuevo: primero instalarlo desde Apps** (o `odoo-update` no lo instala; alternativa: `odoo-bin -i qb_situacion` en la shell). Después, por MCP de Odoo: `search_records ir.config_parameter [('key','=','quimibond_intelligence.push_models')]`; si existe con un valor explícito (`contacts,users,senales`), borrarlo o dejarlo en `all`: si no, `actividades_delegadas` se omite en silencio y la aceptación del Step 4 falla sin error.
- [ ] **Step 4: Aceptación (spec §8 paso 5), con el CEO:** `select situacion_decidir(<id real, p.ej. una cartera vencida>, 'delegar', '{"user_id": <odoo_user_id>, "texto": "…", "vence": "2026-10-01"}');` → en ≤ 5 min la actividad aparece en Odoo sobre la factura/contacto (`select delegacion from situaciones where id = <id>` → `estado: creada`, `mail_activity_id`). Marcarla hecha en Odoo → tras el siguiente push horario, `estado: hecha`, situación `resuelta`, historia "cerrada por …", y **sigue `resuelta` en el push siguiente aunque las facturas sigan vencidas** (`evidencia.cerrada_manual`; solo vuelve, como `empeoro`, si la señal crece). Repetir con cancelar → `abierta`. Anota ids y tiempos en el PR. Comprueba `select * from situacion_salud()` → señal `delegacion_estado` con lote ok.


---

## Parte 3 — Paso 6: la app "Situación" en Odoo y los retiros

### Task 6.1: Modelo transitorio `qb.situacion` (el mapa leído en vivo)

**Files:**
- Create: `addons/qb_situacion/models/situacion.py`
- Create: `addons/qb_situacion/models/wizards.py`
- Modify: `addons/qb_situacion/models/__init__.py` (+ `situacion`, `wizards`)
- Test: `addons/qb_situacion/tests/test_app.py`

**Diseño:** sin tablas persistentes. `qb.situacion` es `TransientModel`: al abrir el menú, un `ir.actions.server` llama `action_cargar_mapa()`, que lee `situacion_mapa(null, 'viva', 1, 500)` por REST (cliente `qb.memoria.client` de `qb_memoria`, que ya tiene `rpc` con la service key de `ir.config_parameter`), borra las filas transitorias del usuario y crea una por situación; la vista lista se agrupa por área. La ficha muestra `situacion_contexto` como HTML computado (como la pestaña Memoria). Botones: **Delegar** (wizard), **Ignorar** (wizard), **Cerrar** (directo). Todo escribe por `situacion_decidir`; Odoo no guarda nada.

- [ ] **Step 1: Test** `tests/test_app.py`:

```python
# -*- coding: utf-8 -*-
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged

# responsable_user_id de la primera fila lo pone el test (base.user_admin): la base del CI es fresca.
MAPA = [
    {'id': 1295, 'area': 'finanzas', 'tipo': 'credito', 'senal': 'cartera_vencida', 'titulo': 'Cartera vencida · FXI INC', 'severidad': 5, 'estado': 'abierta', 'calidad': 'viva',
     'contraparte': 'FXI INC', 'responsable': 'Jessica', 'responsable_user_id': None, 'dias_abierta': 3, 'dias_sin_cambio': 0, 'ultimo_cambio': 'creada', 'n_documentos': 14,
     'valor': 1350828, 'valor_texto': '14 facturas', 'vence': None, 'redactada': True, 'recomendacion': 'Cobrar', 'delegada_a': None, 'delegacion_estado': None},
    {'id': 766, 'area': 'comercial', 'tipo': 'obligacion', 'senal': 'cliente_sin_respuesta', 'titulo': 'Cliente sin respuesta · Daños Broker', 'severidad': 4, 'estado': 'delegada', 'calidad': 'viva',
     'contraparte': 'Daños Broker', 'responsable': 'Irma', 'responsable_user_id': 68, 'dias_abierta': 60, 'dias_sin_cambio': 1, 'ultimo_cambio': 'delegada a Irma', 'n_documentos': 1,
     'valor': 1, 'valor_texto': '1 hilo', 'vence': '2026-10-01', 'redactada': True, 'recomendacion': 'Responder', 'delegada_a': 'Irma Luna', 'delegacion_estado': 'creada'},
]
CONTEXTO = {'situacion': {'id': 1295, 'titulo': 'Cartera vencida · FXI INC', 'resumen': 'Debe 1.35 M.', 'recomendacion': 'Cobrar', 'severidad': 5, 'estado': 'abierta',
                          'calidad': 'viva', 'dias_abierta': 3, 'valor_texto': '14 facturas', 'responsable_motivo': 'dueña', 'delegacion': None, 'ultimo_cambio': 'creada'},
            'senal_config': {'titulo': 'Cartera vencida', 'descripcion': 'x', 'severidad_base': 3, 'severidad_max': 5},
            'senales': [{'clave': 'cartera_vencida:partner:1', 'valor': 100, 'valor_texto': 'F/1 vencida 30 días', 'calidad': 'viva', 'documentos': [{'modelo': 'account.move', 'id': 11, 'nombre': 'INV/2025/08/0145'}]}],
            'documentos': [{'modelo': 'account.move', 'id': 11, 'nombre': 'INV/2025/08/0145'}],
            'contraparte': {'empresa': {'name': 'FXI INC', 'rfc': 'X'}, 'memoria': {'encargados': [], 'hechos': [{'hecho': 'paga a 60 días'}], 'stats': {}}},
            'conversaciones': [{'tema': 'Cobro', 'resumen': 'Prometió pagar', 'estado': 'abierto'}], 'hermanas': [], 'posibles_duplicados': [], 'reglas': [],
            'personas': [{'odoo_user_id': 22, 'name': 'Jessica', 'motivo': 'dueña'}], 'historia': [{'fecha': '2026-09-20', 'evento': 'creada', 'detalle': '14 señales'}]}


def fake_rpc(respuestas):
    calls = []

    def rpc(_self, name, params):
        calls.append((name, dict(params or {})))
        r = respuestas.get(name)
        if isinstance(r, Exception):
            raise r
        return r
    rpc.calls = calls
    return rpc


@tagged('post_install', '-at_install', 'qb_situacion')
class TestApp(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        icp = cls.env['ir.config_parameter'].sudo()
        icp.set_param('quimibond_intelligence.supabase_url', 'https://x.supabase.co')
        icp.set_param('quimibond_intelligence.supabase_service_key', 'k')
        cls.Client = type(cls.env['qb.memoria.client'])

    def _con(self, respuestas):
        rpc = fake_rpc(respuestas)
        patcher = patch.object(self.Client, 'rpc', rpc)
        patcher.start()
        self.addCleanup(patcher.stop)
        return rpc

    def test_cargar_mapa_crea_filas_y_abre_la_lista(self):
        admin = self.env.ref('base.user_admin')   # la base del CI es fresca: no hay usuario 22
        mapa = [dict(MAPA[0], responsable_user_id=admin.id), MAPA[1]]
        rpc = self._con({'situacion_mapa': mapa})
        action = self.env['qb.situacion'].action_cargar_mapa()
        self.assertEqual(rpc.calls[0][0], 'situacion_mapa')
        filas = self.env['qb.situacion'].search([('create_uid', '=', self.env.uid)], order='severidad desc')
        self.assertEqual([f.supabase_id for f in filas], [1295, 766])
        self.assertEqual((filas[0].area, filas[0].titulo, filas[0].severidad, filas[0].responsable_user_id), ('finanzas', 'Cartera vencida · FXI INC', 5, admin))
        self.assertEqual((filas[1].estado, filas[1].delegada_a, filas[1].delegacion_estado), ('delegada', 'Irma Luna', 'creada'))
        self.assertEqual(action['res_model'], 'qb.situacion')
        # segunda carga: reemplaza, no duplica
        self.env['qb.situacion'].action_cargar_mapa()
        self.assertEqual(len(self.env['qb.situacion'].search([('create_uid', '=', self.env.uid)])), 2)

    def test_contexto_html(self):
        self._con({'situacion_mapa': MAPA, 'situacion_contexto': CONTEXTO})
        self.env['qb.situacion'].action_cargar_mapa()
        fila = self.env['qb.situacion'].search([('supabase_id', '=', 1295)])
        html = fila.contexto_html
        for esperado in ('Debe 1.35 M.', 'INV/2025/08/0145', 'paga a 60 días', 'Prometió pagar', 'Jessica', '14 señales'):
            self.assertIn(esperado, html)

    def test_delegar_llama_situacion_decidir(self):
        rpc = self._con({'situacion_mapa': MAPA, 'situacion_decidir': {'ok': True, 'comando_id': 9}})
        self.env['qb.situacion'].action_cargar_mapa()
        fila = self.env['qb.situacion'].search([('supabase_id', '=', 1295)])
        wiz = self.env['qb.situacion.delegar'].with_context(active_id=fila.id).create({'user_id': self.env.ref('base.user_admin').id, 'texto': 'Cobrar ya', 'vence': '2026-10-01'})
        self.assertEqual(wiz.situacion_id, fila)
        wiz.action_confirmar()
        name, params = rpc.calls[-1]
        self.assertEqual(name, 'situacion_decidir')
        self.assertEqual((params['p_id'], params['p_accion'], params['p']['user_id'], params['p']['texto'], params['p']['vence']), (1295, 'delegar', self.env.ref('base.user_admin').id, 'Cobrar ya', '2026-10-01'))
        self.assertEqual((fila.estado, fila.delegacion_estado), ('delegada', 'pendiente'))

    def test_ignorar_y_cerrar(self):
        rpc = self._con({'situacion_mapa': MAPA, 'situacion_decidir': {'ok': True}})
        self.env['qb.situacion'].action_cargar_mapa()
        fila = self.env['qb.situacion'].search([('supabase_id', '=', 1295)])
        wiz = self.env['qb.situacion.ignorar'].with_context(active_id=fila.id).create({'alcance': 'contraparte', 'motivo': 'parte relacionada'})
        wiz.action_confirmar()
        name, params = rpc.calls[-1]
        self.assertEqual((name, params['p_accion'], params['p']['alcance'], params['p']['motivo']), ('situacion_decidir', 'ignorar', 'contraparte', 'parte relacionada'))
        self.assertFalse(fila.exists(), 'ignorada sale de la lista')
        fila2 = self.env['qb.situacion'].search([('supabase_id', '=', 766)])
        fila2.action_cerrar()
        self.assertEqual(rpc.calls[-1][1]['p_accion'], 'resuelta')
        self.assertFalse(fila2.exists())

    def test_error_de_supabase_es_usererror(self):
        from odoo.exceptions import UserError
        self._con({'situacion_mapa': UserError('La memoria respondió 500')})
        with self.assertRaises(UserError):
            self.env['qb.situacion'].action_cargar_mapa()
```

- [ ] **Step 2: `models/situacion.py`:**

```python
# -*- coding: utf-8 -*-
"""El mapa de situación en Odoo (spec §7.4): modelo transitorio que se llena al abrir el menú
leyendo situacion_mapa por REST y una ficha que lee situacion_contexto. Nada persiste: Odoo
muestra y decide (situacion_decidir); Supabase es la única verdad."""
import logging

from markupsafe import Markup, escape

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

AREAS = [('finanzas', 'Finanzas'), ('comercial', 'Comercial'), ('operaciones', 'Operaciones'), ('compras', 'Compras'),
         ('calidad_sgi', 'Calidad / SGI'), ('rh', 'RH'), ('sistemas', 'Sistemas'), ('direccion', 'Dirección')]
ESTADOS = [('abierta', 'Abierta'), ('empeoro', 'Empeoró'), ('mejoro', 'Mejoró'), ('delegada', 'Delegada'), ('resuelta', 'Resuelta'), ('descartada', 'Descartada')]


class QbSituacion(models.TransientModel):
    _name = 'qb.situacion'
    _description = 'Situación de la empresa (mapa, lectura en vivo)'
    _order = 'severidad desc, dias_sin_cambio asc, id'
    _transient_max_hours = 12

    supabase_id = fields.Integer(string='Id', required=True, index=True)
    area = fields.Selection(AREAS, required=True)
    tipo = fields.Char()
    senal = fields.Char(string='Señal')
    titulo = fields.Char(string='Situación', required=True)
    severidad = fields.Integer()
    estado = fields.Selection(ESTADOS)
    calidad = fields.Char()
    contraparte = fields.Char()
    responsable = fields.Char(string='Responsable sugerido')
    responsable_user_id = fields.Many2one('res.users', string='Responsable (usuario)')
    dias_abierta = fields.Integer(string='Días abierta')
    dias_sin_cambio = fields.Integer(string='Días sin cambio')
    ultimo_cambio = fields.Char(string='Último cambio')
    n_documentos = fields.Integer(string='Documentos')
    valor_texto = fields.Char(string='Valor')
    vence = fields.Date()
    redactada = fields.Boolean()
    recomendacion = fields.Text(string='Recomendación')
    delegada_a = fields.Char(string='Delegada a')
    delegacion_estado = fields.Char(string='Estado de la delegación')
    contexto_html = fields.Html(string='Contexto', compute='_compute_contexto_html', sanitize=False)

    # ── carga ────────────────────────────────────────────────────────
    @api.model
    def _client(self):
        return self.env['qb.memoria.client']

    @api.model
    def _fila_desde(self, m):
        user = self.env['res.users'].sudo().browse(int(m['responsable_user_id'])).exists() if m.get('responsable_user_id') else False
        return {
            'supabase_id': m['id'], 'area': m['area'] if m.get('area') in dict(AREAS) else 'direccion', 'tipo': m.get('tipo'), 'senal': m.get('senal'),
            'titulo': m.get('titulo') or '?', 'severidad': int(m.get('severidad') or 0), 'estado': m.get('estado') if m.get('estado') in dict(ESTADOS) else 'abierta',
            'calidad': m.get('calidad'), 'contraparte': m.get('contraparte'), 'responsable': m.get('responsable'),
            'responsable_user_id': user.id if user else False, 'dias_abierta': int(m.get('dias_abierta') or 0), 'dias_sin_cambio': int(m.get('dias_sin_cambio') or 0),
            'ultimo_cambio': m.get('ultimo_cambio'), 'n_documentos': int(m.get('n_documentos') or 0), 'valor_texto': m.get('valor_texto'),
            'vence': m.get('vence') or False, 'redactada': bool(m.get('redactada')), 'recomendacion': m.get('recomendacion'),
            'delegada_a': m.get('delegada_a'), 'delegacion_estado': m.get('delegacion_estado'),
        }

    @api.model
    def action_cargar_mapa(self, area=None, min_severidad=1):
        """Lee el mapa (viva, hasta 500) y abre la lista agrupada por área. Reemplaza las filas del usuario."""
        mapa = self._client().rpc('situacion_mapa', {'p_area': area, 'p_calidad': 'viva', 'p_min_severidad': min_severidad, 'p_limit': 500})
        if not isinstance(mapa, list):
            raise UserError(_('El mapa no respondió una lista: %s') % str(mapa)[:200])
        self.search([('create_uid', '=', self.env.uid)]).unlink()
        filas = self.create([self._fila_desde(m) for m in mapa])
        return {
            'type': 'ir.actions.act_window', 'name': _('Situación de la empresa'), 'res_model': 'qb.situacion',
            'view_mode': 'list,form', 'domain': [('id', 'in', filas.ids)],
            'context': {'search_default_group_area': 1, 'create': False, 'edit': False},
        }

    # ── ficha ────────────────────────────────────────────────────────
    def _compute_contexto_html(self):
        for rec in self:
            try:
                ctx = rec._client().rpc('situacion_contexto', {'p_id': rec.supabase_id}) if rec.supabase_id else None
                rec.contexto_html = rec._render_contexto(ctx) if ctx else Markup('<p class="text-muted">Sin contexto.</p>')
            except UserError as exc:
                rec.contexto_html = Markup('<p class="text-danger">%s</p>') % str(exc)

    @staticmethod
    def _lista(items, fmt):
        if not items:
            return Markup('')
        return Markup('<ul>%s</ul>') % Markup('').join(Markup('<li>%s</li>') % fmt(i) for i in items)

    def _render_contexto(self, ctx):
        s, cfg = ctx.get('situacion') or {}, ctx.get('senal_config') or {}
        contra = ctx.get('contraparte') or {}
        mem = contra.get('memoria') or {}
        h = Markup('')
        h += Markup('<h4>%s</h4><p><b>Severidad %s</b> · %s · %s · %s días abierta · %s</p>') % (
            s.get('titulo', ''), s.get('severidad', ''), s.get('estado', ''), s.get('calidad', ''), s.get('dias_abierta', ''), s.get('ultimo_cambio') or '')
        if s.get('resumen'):
            h += Markup('<h5>Qué pasa</h5><p>%s</p>') % s['resumen']
        if s.get('recomendacion'):
            h += Markup('<h5>Recomendación</h5><p>%s</p>') % s['recomendacion']
        if s.get('responsable_motivo'):
            h += Markup('<p class="text-muted">Responsable sugerido: %s</p>') % s['responsable_motivo']
        d = s.get('delegacion')
        if d:
            h += Markup('<h5>Delegación</h5><p>%s · %s · vence %s%s</p>') % (d.get('nombre') or d.get('user_id'), d.get('estado'), d.get('vence') or '-', (' · ' + d['error']) if d.get('error') else '')
        h += Markup('<h5>Señal: %s</h5><p class="text-muted">%s (banda %s–%s)</p>') % (cfg.get('titulo') or s.get('senal', ''), cfg.get('descripcion') or '', cfg.get('severidad_base', ''), cfg.get('severidad_max', ''))
        h += Markup('<h5>Documentos</h5>') + self._lista(ctx.get('documentos') or [], lambda x: Markup('%s · %s') % (x.get('modelo', ''), x.get('nombre') or x.get('id')))
        h += Markup('<h5>Señales</h5>') + self._lista(ctx.get('senales') or [], lambda x: Markup('%s — %s (%s)') % (x.get('clave', ''), x.get('valor_texto') or x.get('valor'), x.get('calidad', '')))
        if contra.get('empresa'):
            e = contra['empresa']
            h += Markup('<h5>Contraparte</h5><p>%s%s</p>') % (e.get('name', ''), (' · RFC ' + e['rfc']) if e.get('rfc') else '')
            h += self._lista(mem.get('hechos') or [], lambda x: escape(x.get('hecho', '')))
        h += Markup('<h5>Conversaciones</h5>') + self._lista(ctx.get('conversaciones') or [], lambda x: Markup('<b>%s</b>: %s') % (x.get('tema') or '', x.get('resumen') or ''))
        h += Markup('<h5>Personas</h5>') + self._lista(ctx.get('personas') or [], lambda x: Markup('%s — %s') % (x.get('name', ''), x.get('motivo', '')))
        h += Markup('<h5>Historia</h5>') + self._lista(ctx.get('historia') or [], lambda x: Markup('%s · %s: %s') % (str(x.get('fecha', ''))[:10], x.get('evento', ''), x.get('detalle') or ''))
        return h

    # ── decisiones ───────────────────────────────────────────────────
    def _decidir(self, accion, payload):
        self.ensure_one()
        r = self._client().rpc('situacion_decidir', {'p_id': self.supabase_id, 'p_accion': accion, 'p': payload})
        if not isinstance(r, dict) or not r.get('ok'):
            raise UserError(_('Supabase no aceptó la decisión: %s') % str(r)[:300])
        return r

    def action_cerrar(self):
        for rec in self:
            rec._decidir('resuelta', {'motivo': 'cerrada desde Odoo', 'creada_por': self.env.user.login})
            rec.unlink()
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {'title': _('Situación cerrada'), 'type': 'success', 'next': {'type': 'ir.actions.act_window_close'}}}

    def action_abrir_delegar(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'qb.situacion.delegar', 'view_mode': 'form', 'target': 'new', 'name': _('Delegar'),
                'context': {'active_id': self.id, 'default_situacion_id': self.id, 'default_texto': self.recomendacion or self.titulo, 'default_user_id': self.responsable_user_id.id}}

    def action_abrir_ignorar(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'qb.situacion.ignorar', 'view_mode': 'form', 'target': 'new', 'name': _('Ignorar'),
                'context': {'active_id': self.id, 'default_situacion_id': self.id}}
```

- [ ] **Step 3: `models/wizards.py`:**

```python
# -*- coding: utf-8 -*-
"""Wizards de decisión: delegar (usuario, texto, vence) e ignorar (alcance, motivo). Ambos llaman situacion_decidir."""
from odoo import _, api, fields, models


class QbSituacionDelegar(models.TransientModel):
    _name = 'qb.situacion.delegar'
    _description = 'Delegar una situación'

    situacion_id = fields.Many2one('qb.situacion', required=True, ondelete='cascade', default=lambda self: self.env.context.get('active_id'))
    user_id = fields.Many2one('res.users', string='A quién', required=True, domain=[('share', '=', False)])
    texto = fields.Text(string='Qué tiene que hacer', required=True)
    vence = fields.Date(string='Para cuándo', default=lambda self: fields.Date.add(fields.Date.context_today(self), days=3))

    def action_confirmar(self):
        self.ensure_one()
        self.situacion_id._decidir('delegar', {'user_id': self.user_id.id, 'texto': self.texto, 'vence': fields.Date.to_string(self.vence) if self.vence else None,
                                               'creada_por': self.env.user.login})
        self.situacion_id.write({'estado': 'delegada', 'delegada_a': self.user_id.name, 'delegacion_estado': 'pendiente'})
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'title': _('Delegada'), 'message': _('%s recibirá la actividad en Odoo en menos de 5 minutos.') % self.user_id.name, 'type': 'success',
            'next': {'type': 'ir.actions.act_window_close'}}}


class QbSituacionIgnorar(models.TransientModel):
    _name = 'qb.situacion.ignorar'
    _description = 'Ignorar una situación'

    situacion_id = fields.Many2one('qb.situacion', required=True, ondelete='cascade', default=lambda self: self.env.context.get('active_id'))
    alcance = fields.Selection([('situacion', 'Solo esta situación'), ('contraparte', 'Todo lo de esta contraparte'), ('senal', 'Toda esta señal')],
                               required=True, default='situacion')
    motivo = fields.Char(required=True)
    vigente_hasta = fields.Date(string='Hasta (vacío = para siempre)')

    def action_confirmar(self):
        self.ensure_one()
        self.situacion_id._decidir('ignorar', {'alcance': self.alcance, 'motivo': self.motivo,
                                               'vigente_hasta': fields.Date.to_string(self.vigente_hasta) if self.vigente_hasta else None, 'creada_por': self.env.user.login})
        self.situacion_id.unlink()
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {'title': _('Ignorada'), 'type': 'success', 'next': {'type': 'ir.actions.act_window_close'}}}
```

- [ ] **Step 4: `security/ir.model.access.csv`** += filas para `qb.situacion`, `qb.situacion.delegar`, `qb.situacion.ignorar` (`group_ceo`: 1,1,1,1). `models/__init__.py` += `situacion, wizards`. `tests/__init__.py` += `test_app`.

- [ ] **Step 5: Correr** flake8/compileall, push, leer `odoo-tests`. Commit — "qb_situacion: mapa transitorio, ficha con contexto y wizards de delegar/ignorar".

### Task 6.2: Vistas, acción y menú de la app

**Files:**
- Create: `addons/qb_situacion/views/situacion_views.xml`, `addons/qb_situacion/static/description/icon.png` (cualquier PNG 128×128; copia el de `qb_obligation`)
- Modify: `__manifest__.py` (`version` 19.0.2.0.0; `data` += `views/situacion_views.xml`)

- [ ] **Step 1: `views/situacion_views.xml`:**

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo>
    <record id="view_qb_situacion_list" model="ir.ui.view">
        <field name="name">qb.situacion.list</field>
        <field name="model">qb.situacion</field>
        <field name="arch" type="xml">
            <list create="false" edit="false" delete="false" default_order="severidad desc, dias_sin_cambio asc"
                  decoration-danger="severidad >= 5" decoration-warning="severidad == 4" decoration-info="estado == 'delegada'" decoration-muted="not redactada">
                <field name="severidad" string="Sev."/>
                <field name="titulo"/>
                <field name="contraparte" optional="show"/>
                <field name="estado" widget="badge"/>
                <field name="dias_abierta" string="Días"/>
                <field name="responsable" optional="show"/>
                <field name="delegada_a" optional="show"/>
                <field name="delegacion_estado" optional="hide"/>
                <field name="ultimo_cambio" optional="show"/>
                <field name="recomendacion" optional="hide"/>
                <field name="area" column_invisible="1"/>
                <field name="redactada" column_invisible="1"/>
                <button name="action_abrir_delegar" type="object" string="Delegar" icon="fa-user-plus" invisible="estado == 'delegada'"/>
            </list>
        </field>
    </record>

    <record id="view_qb_situacion_form" model="ir.ui.view">
        <field name="name">qb.situacion.form</field>
        <field name="model">qb.situacion</field>
        <field name="arch" type="xml">
            <form create="false" edit="false" delete="false">
                <header>
                    <button name="action_abrir_delegar" type="object" string="Delegar" class="btn-primary" invisible="estado == 'delegada'"/>
                    <button name="action_abrir_ignorar" type="object" string="Ignorar"/>
                    <button name="action_cerrar" type="object" string="Cerrar" confirm="¿Cerrar esta situación? Queda cerrada aunque la señal siga igual; vuelve a aparecer solo si empeora."/>
                    <field name="estado" widget="statusbar"/>
                </header>
                <sheet>
                    <div class="oe_title"><h1><field name="titulo" readonly="1"/></h1></div>
                    <group>
                        <group>
                            <field name="severidad"/><field name="area"/><field name="contraparte"/><field name="responsable"/>
                        </group>
                        <group>
                            <field name="dias_abierta"/><field name="dias_sin_cambio"/><field name="vence"/><field name="valor_texto"/>
                            <field name="delegada_a"/><field name="delegacion_estado"/>
                        </group>
                    </group>
                    <field name="recomendacion" readonly="1" placeholder="Sin redactar todavía"/>
                    <notebook><page string="Contexto" name="contexto"><field name="contexto_html" nolabel="1" readonly="1"/></page></notebook>
                </sheet>
            </form>
        </field>
    </record>

    <record id="view_qb_situacion_search" model="ir.ui.view">
        <field name="name">qb.situacion.search</field>
        <field name="model">qb.situacion</field>
        <field name="arch" type="xml">
            <search>
                <field name="titulo"/><field name="contraparte"/><field name="responsable"/>
                <filter name="graves" string="Graves (4-5)" domain="[('severidad', '>=', 4)]"/>
                <filter name="delegadas" string="Delegadas" domain="[('estado', '=', 'delegada')]"/>
                <filter name="sin_redactar" string="Sin redactar" domain="[('redactada', '=', False)]"/>
                <group expand="1" string="Agrupar"><filter name="group_area" string="Área" context="{'group_by': 'area'}"/><filter name="group_estado" string="Estado" context="{'group_by': 'estado'}"/></group>
            </search>
        </field>
    </record>

    <record id="view_qb_situacion_delegar_form" model="ir.ui.view">
        <field name="name">qb.situacion.delegar.form</field>
        <field name="model">qb.situacion.delegar</field>
        <field name="arch" type="xml">
            <form>
                <group><field name="situacion_id" readonly="1"/><field name="user_id" widget="many2one_avatar_user"/><field name="vence"/><field name="texto"/></group>
                <footer><button name="action_confirmar" type="object" string="Delegar" class="btn-primary"/><button string="Cancelar" special="cancel"/></footer>
            </form>
        </field>
    </record>

    <record id="view_qb_situacion_ignorar_form" model="ir.ui.view">
        <field name="name">qb.situacion.ignorar.form</field>
        <field name="model">qb.situacion.ignorar</field>
        <field name="arch" type="xml">
            <form>
                <group><field name="situacion_id" readonly="1"/><field name="alcance"/><field name="motivo"/><field name="vigente_hasta"/></group>
                <footer><button name="action_confirmar" type="object" string="Ignorar" class="btn-primary"/><button string="Cancelar" special="cancel"/></footer>
            </form>
        </field>
    </record>

    <!-- El menú llama a un server action que carga el mapa y abre la lista. -->
    <record id="action_qb_situacion_cargar" model="ir.actions.server">
        <field name="name">Situación de la empresa</field>
        <field name="model_id" ref="model_qb_situacion"/>
        <field name="state">code</field>
        <field name="code">action = model.action_cargar_mapa()</field>
    </record>

    <menuitem id="menu_qb_situacion_root" name="Situación" sequence="30" web_icon="qb_situacion,static/description/icon.png" groups="qb_situacion.group_ceo"/>
    <menuitem id="menu_qb_situacion_mapa" name="Mapa" parent="menu_qb_situacion_root" action="action_qb_situacion_cargar" sequence="10"/>
</odoo>
```

- [ ] **Step 2: XML parseado, flake8, `tools/check_addons.py` (bump 19.0.2.0.0 ✓), push, `odoo-tests`.** Commit — "qb_situacion 19.0.2.0.0: app Situación (mapa por área, ficha, delegar, ignorar, cerrar)".

### Task 6.3: Docs, PR, despliegue y aceptación del paso 6

- [ ] **Step 1: README de `qb_situacion`** (app: qué ve el director, qué hace cada botón, que nada se guarda en Odoo, TTL 12 h de las filas transitorias, cómo dar el grupo "Situación: director" a un usuario). `CLAUDE.md` (qb19): `qb_situacion` en "Otros módulos". `docs/RUNBOOK_DESPLIEGUE.md`: verificación (abrir Situación → Mapa; delegar una; ver la actividad).
- [ ] **Step 2: PR** "Situación plan B, paso 6: app Situación en Odoo" → CI → merge → "Merge main into quimibond" → CEO: `odoo-update qb_situacion && odoosh-restart http`.
- [ ] **Step 3: Aceptación (spec §8 paso 6):** el CEO abre Situación → Mapa, ve las áreas, abre una ficha, delega desde el botón y la actividad llega a la persona en ≤ 5 min. Anotar en el PR.

### Task 6.4: Retiro de `email-digest`

**Files:**
- Create: `supabase/migrations/20260926a_retiro_email_digest.sql`
- Delete: `supabase/functions/email-digest/`, `supabase/functions/_shared/digest-email-html.ts`

- [ ] **Step 1** (solo tras la aceptación del paso 4): migración

```sql
-- 2026-09-26a — Retiro de email-digest (sustituido por situacion-digest el 2026-09-24, aceptado por el CEO).
-- El job memoria_email_digest ya se desprogramó en 20260924b. La Edge Function se borra del dashboard a mano.
-- email_digests se conserva (histórico); analyst_query sigue (email-extract la usa).
DROP FUNCTION IF EXISTS public.get_unanswered_client_threads(integer, integer);
DROP FUNCTION IF EXISTS public.get_silent_customers(integer, integer, integer);
INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Retiro de email-digest: RPCs get_unanswered_client_threads y get_silent_customers borradas', jsonb_build_object('migration', '20260926a_retiro_email_digest'));
```

  Antes de aplicar, confirma las firmas reales: `select proname, pg_get_function_identity_arguments(oid) from pg_proc where proname in ('get_unanswered_client_threads','get_silent_customers');` y que nadie más las use: `grep -rn "get_unanswered_client_threads\|get_silent_customers" supabase/ src/ --include=*.ts --include=*.sql`.

- [ ] **Step 2:** borrar `supabase/functions/email-digest/` y `_shared/digest-email-html.ts`; `npx tsc --noEmit && npm test`; `CLAUDE.md` (RPCs, "Cómo desplegar" lista de `_shared`, deuda: agregar `email-digest` a la lista de funciones que el CEO borra del dashboard). Commit — "Retiro de email-digest (sustituido por situacion-digest)". PR → merge.

### Task 6.5: Retiro de `qb_obligation`

- [ ] **Step 1 (CEO, en producción):** Apps → `qb_obligation` → Desinstalar. Antes: revisar en Odoo las obligaciones abiertas que todavía importan y delegarlas desde el mapa (aparecen como situaciones `obligacion_legado`); el resto se pierde con el módulo (sus actividades nativas también se borran al desinstalar).
- [ ] **Step 2:** en el siguiente push horario, `obligacion_legado` manda lote vacío (Tarea 5.3) y resuelve sus 138 situaciones. Verificar: `select count(*) from situaciones where senal = 'obligacion_legado' and estado not in ('resuelta','descartada');` = 0. Luego migración `20260926b_obligacion_legado_inactiva.sql`: `UPDATE senales_config SET activa = false WHERE senal = 'obligacion_legado';` + pipeline_logs.
- [ ] **Step 3 (qb19, PR aparte, después de la desinstalación):** borrar `addons/qb_obligation/`, quitarlo de `-i` y `--test-tags` en `.github/workflows/ci.yml`, borrar la función `obligacion_legado` de `models/senales/direccion.py` y su test, actualizar `CLAUDE.md` ("Otros módulos", nota histórica) y `tools/no_bump.txt` si lo menciona. `grep -rn "qb_obligation\|qb.obligation" addons/ docs/ CLAUDE.md` debe quedar solo en docs históricos. PR → CI → merge → "Merge main into quimibond". **Nunca antes de que producción lo tenga desinstalado** (regla de superconjuntos).

---

## Riesgos de este plan y cómo se mitigan

- **Delegar sin el pull vivo:** `situacion_delegaciones_aplicar` marca `error` a los 15 min y el correo de la mañana lo dice en el primer bullet (`salud`). El watchdog ya vigila el push; el pull se ve en el Historial de Sync.
- **Cierre humano y señal viva:** sin `evidencia.cerrada_manual`, cada "hecha" o "cerrada por el director" se reabriría en la siguiente corrida del bot (`situacion_guardar` la ve como "reapareció"). Con la marca, reabre solo si la señal crece. Si el CEO prefiere que reabra siempre, basta quitar el `IF sit.evidencia ? 'cerrada_manual'` de `situacion_guardar`.
- **Odoo 19 archiva la actividad hecha solo si su tipo tiene `keep_done=True`** (el nuestro lo tiene; sin eso la borra): el hook de `_action_done` deja el evento antes de archivar; el push además lee la actividad archivada como respaldo (`hecha en Odoo (sin evento)`). Cancelar = `unlink` = evento `cancelada`.
- **`qb_situacion` nuevo en producción:** `odoo-update` no instala módulos nuevos; el CEO lo instala desde Apps la primera vez. El módulo no toca `quimibond_intelligence` más que por herencia.
- **Un lote vacío de `delegacion_estado` cerraría delegaciones:** por eso el push aborta sin lote si `situacion_delegaciones_abiertas` falla, y `senales_ingestar` solo resuelve señales de ESA señal (las delegaciones viven en `situaciones.delegacion`, no en `senales`): un lote vacío por error deja las delegaciones intactas.
- **El correo llega y no gusta:** el render es puro y con test; cambiar formato no toca la RPC. La narrativa se apaga con `sin_ia`.
