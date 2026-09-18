# Situación de la empresa — Plan A (pasos 1–3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Que `select * from situacion_mapa('finanzas')` en Supabase devuelva, cada hora, las situaciones reales de la empresa (facturas vencidas por cliente, OPs atrasadas, conversaciones sin respuesta…) con calidad, edad, título y recomendación redactados por Claude, consultables por MCP.

**Architecture:** Tres piezas en el orden del spec (§8): (1) esquema + RPCs en Supabase (SQL determinístico: ingesta por lote, episodios, calidad, agrupación en situaciones, lectura); (2) `quimibond_intelligence._push_senales` en Odoo (una consulta por señal → `senales_ingestar`, `rpc_strict`, turnos por `cada_horas`) y `buzon_personas`; (3) Edge Function `situacion-consolidar` (Sonnet redacta solo lo que cambió, fusiona duplicados) disparada por `senales_push_terminado` con cron de respaldo. El plan B (correo diario, delegación, app en Odoo) es otro documento.

**Tech Stack:** Supabase Postgres 15 (plpgsql, pg_cron, pg_net, pg_trgm), Edge Functions en Deno (`npm:@anthropic-ai/sdk`, `claude-sonnet-5`), Odoo 19 (Python 3.11, httpx), CI de qb19 (flake8 + Odoo tests en contenedor `odoo:19.0`).

**Spec:** `docs/superpowers/specs/2026-09-18-situacion-empresa-design.md` (qb19). Léelo entero antes de empezar; este plan cita secciones (§) de ahí.

---

## 0. Contexto para quien no conoce el proyecto

### Repos, ramas y cómo se entrega

| Repo | Ruta local | Rama de trabajo | Qué toca este plan |
|---|---|---|---|
| `quimibond/quimibond-intelligence` | `/home/user/quimibond-intelligence` | `claude/awesome-franklin-odolbj` | `supabase/migrations/*.sql`, `supabase/functions/situacion-consolidar/`, `supabase/functions/health/index.ts`, `supabase/tests/situacion/*.sql`, `CLAUDE.md` |
| `quimibond/qb19` | `/home/user/qb19` | `claude/awesome-franklin-odolbj` | `addons/quimibond_intelligence/` (cliente, push, `models/senales/`, tests), `CLAUDE.md`, `docs/RUNBOOK_DESPLIEGUE.md` |

- **Commits:** mensaje en español, imperativo corto; cuerpo con el porqué. Cada commit termina con las dos líneas de atribución de la sesión (`Co-Authored-By: …` y `Claude-Session: …`, ver el system-reminder de la sesión). Escribe el mensaje en un archivo del scratchpad y usa `git -c user.name="Jose J. Mizrahi" -c user.email="jose.mizrahi@quimibond.com" commit -F <archivo>`.
- **PRs:** draft → CI verde → ready → squash-merge con título `… (#N)`. En qb19, después del merge a `main` se abre y mergea (merge commit) el PR "Merge main into quimibond (#N)". En quimibond-intelligence el deploy preview de Vercel **falla siempre** (frontend retirado); no es check requerido: un comentario de standing-down por PR y se mergea.
- **Secretos:** nunca en repo, PR ni chat. La service key de Supabase y la URL viven en `ir.config_parameter` de Odoo (`quimibond_intelligence.supabase_url`, `quimibond_intelligence.supabase_service_key`); `cron_secret` y `anthropic_api_key` en Vault de Supabase. No los imprimas en logs.
- **No subir la versión** del manifest de `quimibond_intelligence` (`tools/no_bump.txt`). El módulo se despliega a mano con `odoo-update quimibond_intelligence && odoosh-restart http && odoosh-restart cron` en la shell de Odoo.sh; eso lo corre el CEO. Como **no hay modelo nuevo** en este plan (solo mixins y módulos Python), `tools/check_addons.py` solo avisa (WARN), no falla.
- **Supabase** proyecto `tozqezmivpblmcubmnpi`. No hay entorno de pruebas: las migraciones se aplican a producción con el MCP `apply_migration` (o `supabase db push`). Por eso cada migración es idempotente y cada prueba SQL corre **en seco** (termina con `RAISE EXCEPTION 'PRUEBA_OK'`, que deshace todo lo que insertó).
- **Edge Functions** se despliegan con el MCP `deploy_edge_function` (files: `<fn>/index.ts` + los `_shared/*.ts` que importe, `verify_jwt=false`) y se prueban con `select invoke_edge('<fn>', '{...}'::jsonb)`; el resultado queda en `pipeline_logs`.

### Cómo correr las verificaciones locales

```bash
# qb19 — lint y compilación (lo mismo que el job `check` del CI)
cd /home/user/qb19 && flake8 addons/ && python -m compileall -q addons/quimibond_intelligence
# qb19 — pytest puro (cliente HTTP; no necesita Odoo)
cd /home/user/qb19/addons && python3 -m pytest quimibond_intelligence/tests -q
# quimibond-intelligence — tipos y tests del frontend retirado (lo que corre el CI; no cubre supabase/functions)
cd /home/user/quimibond-intelligence && npx tsc --noEmit && npm test
```

Los tests de Odoo (`TransactionCase`) **solo corren en el CI** (job `odoo-tests`, contenedor `odoo:19.0`); localmente no hay Odoo. El job instala `quimibond_intelligence` pero hoy no lo incluye en `--test-tags`: la Tarea 2.2 lo agrega.

Las pruebas SQL corren contra producción con el MCP `execute_sql` (o `psql "$SUPABASE_DB_URL" -f archivo.sql` si alguien tiene la URL): el resultado esperado de cada archivo es **el error `PRUEBA_OK`**. Cualquier otro error o un éxito silencioso es un fallo.

### Vocabulario (del spec)

- **Señal** (`senales`): hecho determinístico con clave estable (`cartera_vencida:partner:1742`). Una fila por episodio; `resuelta_en` la cierra. Nunca se borra.
- **Lote** (`senales_lotes`): la lista completa de claves de UNA señal en UNA corrida. La resolución se calcula por lote: lo abierto que no viene en el lote se cierra. Sin lote bueno, nada se cierra.
- **Situación** (`situaciones`): agrupación determinística de señales (`senal|agrupador`) que el CEO puede decidir. SQL la crea con título provisional; la IA solo escribe título, resumen, recomendación, severidad (en banda), responsable sugerido y fusiones.
- **Calidad**: `viva`, `antigua`, `zombie`, `dato_malo`, `vencida_memoria`, `ignorada` (§5.2). Zombis y datos malos se agrupan en situaciones de higiene (`senal|higiene:zombie`).
- **Corrida** (`situacion_corridas`): una ejecución del ciclo (memoria → calidad → situaciones → IA).

---

## 1. Estructura de archivos

### quimibond-intelligence (Supabase)

| Archivo | Responsabilidad |
|---|---|
| `supabase/migrations/20260919a_situacion_esquema.sql` | Tablas, índices, comentarios, catálogo inicial (`senales_config`), `buzon_personas_reemplazar` |
| `supabase/migrations/20260919b_situacion_ingesta.sql` | `senales_ingestar`, `senales_push_terminado`, helpers de responsable |
| `supabase/migrations/20260919c_situacion_memoria.sql` | `senales_memoria` (señales de correo, SQL puro) |
| `supabase/migrations/20260919d_situacion_calidad_y_situaciones.sql` | `senales_actualizar`, `situacion_guardar`, `situacion_ciclo`, `situacion_candidatas`, `situacion_corrida_cerrar` |
| `supabase/migrations/20260919e_situacion_lectura.sql` | `situacion_mapa`, `situacion_contexto`, `situacion_por_persona`, `situacion_higiene`, `situacion_salud`, `situacion_redactar` |
| `supabase/migrations/20260920a_situacion_bot_cron.sql` | Job `situacion_respaldo` (solo cuando la Edge Function ya existe) |
| `supabase/tests/situacion/01_ingesta.sql` … `05_lectura.sql` | Pruebas en seco (terminan en `PRUEBA_OK`) |
| `supabase/functions/situacion-consolidar/index.ts` | El bot: ciclo SQL → candidatas → contexto → Claude → `situacion_redactar` |
| `supabase/functions/situacion-consolidar/prompt.ts` | Prompt del sistema, armado del contexto con presupuesto, validación del JSON (puro, testeable) |
| `supabase/functions/situacion-consolidar/prompt_test.ts` | Tests Deno de `prompt.ts` (se corren si hay `deno`; el CI no los corre) |
| `supabase/functions/health/index.ts` | + lote `job_caido` (`fuente='watchdog'`) |
| `CLAUDE.md` | Inventario: tablas, RPCs, job, cómo preguntar por MCP |

### qb19 (Odoo)

| Archivo | Responsabilidad |
|---|---|
| `addons/quimibond_intelligence/models/supabase_client.py` | + `SupabaseError`, `rpc_strict()` |
| `addons/quimibond_intelligence/tests/test_supabase_client_details.py` | + tests de `rpc_strict` (pytest) |
| `addons/quimibond_intelligence/models/senales/__init__.py` | Importa los módulos por área (registra las señales) |
| `addons/quimibond_intelligence/models/senales/base.py` | Registro `@senal`, helpers `doc`, `fila`, `hoy`, `dias`, `umbral`, `tiene_modelo`, `agrupar` |
| `addons/quimibond_intelligence/models/senales/finanzas.py` … `direccion.py` | Una función por señal, devuelve la lista completa de filas |
| `addons/quimibond_intelligence/models/sync_push_senales.py` | Mixin `_push_senales` (turnos, corrida, `senales_ingestar`, `senales_push_terminado`, errores al Historial) |
| `addons/quimibond_intelligence/models/sync_push.py` | `PUSH_MODELS` + `senales`, orden de métodos |
| `addons/quimibond_intelligence/models/sync_push_partners.py` | `_push_users` + `buzon_personas` |
| `addons/quimibond_intelligence/models/__init__.py` | + `senales`, `sync_push_senales` |
| `addons/quimibond_intelligence/tests/__init__.py`, `tests/test_senales.py` | Tests de Odoo (`TransactionCase`) de las señales |
| `.github/workflows/ci.yml` | `--test-tags` + `/quimibond_intelligence` |
| `CLAUDE.md`, `docs/RUNBOOK_DESPLIEGUE.md` | Documentación del push de señales y verificación post-deploy |

---

## Parte 1 — Supabase: esquema y RPCs (paso 1 del spec)

Todo en `/home/user/quimibond-intelligence`. Cada migración se aplica con el MCP `apply_migration` (`name` = nombre del archivo sin `.sql`) **después** de escribirla en el repo, y se prueba con el archivo de `supabase/tests/situacion/` que le corresponde.

### Task 1.1: Esquema y catálogo

**Files:**
- Create: `supabase/migrations/20260919a_situacion_esquema.sql`
- Create: `supabase/tests/situacion/00_esquema.sql`

- [ ] **Step 1: Escribir la migración**

```sql
-- 2026-09-19a — Situación de la empresa (plan A, paso 1): esquema y catálogo.
-- Spec: qb19/docs/superpowers/specs/2026-09-18-situacion-empresa-design.md §3–§5.
-- Supabase guarda SEÑALES DERIVADAS (una fila por hecho, con modelo+id del
-- documento de Odoo), nunca cifras copiadas. Idempotente.
BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA extensions;

-- 3.2 Catálogo: lo que no está aquí no existe para la IA.
CREATE TABLE IF NOT EXISTS public.senales_config (
  senal           text PRIMARY KEY,
  titulo          text NOT NULL,
  area            text NOT NULL CHECK (area IN ('comercial','operaciones','compras','finanzas','calidad_sgi','rh','sistemas','direccion')),
  tipo            text NOT NULL CHECK (tipo IN ('obligacion','credito','problema','riesgo','oportunidad','higiene')),
  fuente          text NOT NULL CHECK (fuente IN ('odoo','memoria','watchdog')),
  activa          boolean NOT NULL DEFAULT true,
  umbrales        jsonb NOT NULL DEFAULT '{}'::jsonb,
  severidad_base  smallint NOT NULL DEFAULT 2 CHECK (severidad_base BETWEEN 1 AND 5),
  severidad_max   smallint NOT NULL DEFAULT 4 CHECK (severidad_max BETWEEN 1 AND 5),
  reglas_calidad  jsonb NOT NULL DEFAULT '{}'::jsonb,
  agrupar_por     text NOT NULL DEFAULT 'contraparte' CHECK (agrupar_por IN ('contraparte','documento','responsable','situacion','payload','ninguno')),
  agregar         text NOT NULL DEFAULT 'suma' CHECK (agregar IN ('suma','cuenta','maximo')),
  cada_horas      integer NOT NULL DEFAULT 1 CHECK (cada_horas >= 1),
  sin_datos_horas integer,
  descripcion     text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE public.senales_config IS 'Catálogo de señales (spec §3.2/§4). umbrales: parámetros de la consulta en la fuente (p.ej. {"dias":7}); reglas_calidad: {"antigua_dias":30,"zombie_dias":90,"vencida_dias":21,"limpieza":"…"}; agrupar_por decide qué señales forman una situación; agregar decide el valor agregado (suma de valor, cuenta de filas o máximo); cada_horas: turno del push; sin_datos_horas: edad del último lote bueno a partir de la cual la señal se reporta sin_datos (default 2×cada_horas).';

-- 3.1 Señales: una fila por episodio.
CREATE TABLE IF NOT EXISTS public.senales (
  id                        bigserial PRIMARY KEY,
  clave                     text NOT NULL,
  episodio                  integer NOT NULL DEFAULT 1,
  senal                     text NOT NULL REFERENCES public.senales_config(senal),
  area                      text NOT NULL,
  tipo                      text NOT NULL,
  fuente                    text NOT NULL,
  agrupador                 text NOT NULL DEFAULT 'todas',
  documentos                jsonb NOT NULL DEFAULT '[]'::jsonb,
  company_id                bigint,
  odoo_partner_id           integer,
  responsable_odoo_user_id  integer,
  valor                     numeric,
  valor_texto               text,
  vence                     date,
  primera_vista             timestamptz NOT NULL DEFAULT now(),
  vista_en                  timestamptz NOT NULL DEFAULT now(),
  valor_cambio_en           timestamptz NOT NULL DEFAULT now(),
  resuelta_en               timestamptz,
  calidad                   text NOT NULL DEFAULT 'viva' CHECK (calidad IN ('viva','antigua','zombie','dato_malo','vencida_memoria','ignorada')),
  calidad_motivo            text,
  payload                   jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS senales_abierta_uniq ON public.senales (clave) WHERE resuelta_en IS NULL;
CREATE INDEX IF NOT EXISTS senales_senal_idx    ON public.senales (senal, resuelta_en);
CREATE INDEX IF NOT EXISTS senales_area_idx     ON public.senales (area, calidad) WHERE resuelta_en IS NULL;
CREATE INDEX IF NOT EXISTS senales_company_idx  ON public.senales (company_id);
CREATE INDEX IF NOT EXISTS senales_clave_idx    ON public.senales (clave);
COMMENT ON TABLE public.senales IS 'Hechos determinísticos con clave estable (spec §3.1). Una fila por episodio: si la clave reaparece tras resolverse, fila nueva con episodio+1. agrupador lo calcula senales_ingestar según senales_config.agrupar_por (company:<id>, doc:<modelo>:<id>, user:<id>, grupo:<payload.grupo>, todas). payload.fecha_base (date) alimenta la regla zombie; payload.dato_malo (texto) la etiqueta dato_malo; payload.ultimo_correo (timestamptz) las reglas antigua/vencida_memoria.';

-- 3.1.1 Lotes: un lote por señal y corrida. La resolución solo ocurre con lote completo.
CREATE TABLE IF NOT EXISTS public.senales_lotes (
  id              bigserial PRIMARY KEY,
  senal           text NOT NULL REFERENCES public.senales_config(senal),
  fuente          text NOT NULL,
  corrida         uuid NOT NULL,
  recibido_en     timestamptz NOT NULL DEFAULT now(),
  n_claves        integer NOT NULL DEFAULT 0,
  n_nuevas        integer NOT NULL DEFAULT 0,
  n_actualizadas  integer NOT NULL DEFAULT 0,
  n_resueltas     integer NOT NULL DEFAULT 0,
  ok              boolean NOT NULL DEFAULT true,
  error           text
);
CREATE INDEX IF NOT EXISTS senales_lotes_senal_idx ON public.senales_lotes (senal, recibido_en DESC);
CREATE INDEX IF NOT EXISTS senales_lotes_corrida_idx ON public.senales_lotes (corrida);

-- 3.3 Situaciones: unidades de atención.
CREATE TABLE IF NOT EXISTS public.situaciones (
  id                            bigserial PRIMARY KEY,
  clave                         text NOT NULL UNIQUE,
  senal                         text NOT NULL REFERENCES public.senales_config(senal),
  agrupador                     text NOT NULL,
  area                          text NOT NULL,
  tipo                          text NOT NULL,
  titulo                        text NOT NULL,
  resumen                       text,
  company_id                    bigint,
  odoo_partner_id               integer,
  documentos                    jsonb NOT NULL DEFAULT '[]'::jsonb,
  evidencia                     jsonb NOT NULL DEFAULT '{}'::jsonb,
  responsable_sugerido_user_id  integer,
  responsable_motivo            text,
  severidad                     smallint NOT NULL DEFAULT 2 CHECK (severidad BETWEEN 1 AND 5),
  desde                         date NOT NULL DEFAULT current_date,
  vence                         date,
  estado                        text NOT NULL DEFAULT 'abierta' CHECK (estado IN ('abierta','empeoro','mejoro','resuelta','descartada','delegada')),
  calidad                       text NOT NULL DEFAULT 'viva',
  recomendacion                 text,
  delegacion                    jsonb,
  historia                      jsonb NOT NULL DEFAULT '[]'::jsonb,
  n_senales                     integer NOT NULL DEFAULT 0,
  valor                         numeric,
  valor_texto                   text,
  ultimo_cambio                 text,
  ultimo_cambio_en              timestamptz NOT NULL DEFAULT now(),
  version                       integer NOT NULL DEFAULT 1,
  ia_version                    integer NOT NULL DEFAULT 0,
  ia_modelo                     text,
  fusionada_en                  bigint REFERENCES public.situaciones(id),
  resuelta_en                   timestamptz,
  created_at                    timestamptz NOT NULL DEFAULT now(),
  updated_at                    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS situaciones_abiertas_idx ON public.situaciones (area, severidad DESC) WHERE estado NOT IN ('resuelta','descartada') AND fusionada_en IS NULL;
CREATE INDEX IF NOT EXISTS situaciones_company_idx ON public.situaciones (company_id);
CREATE INDEX IF NOT EXISTS situaciones_titulo_trgm ON public.situaciones USING gin (titulo extensions.gin_trgm_ops);
COMMENT ON TABLE public.situaciones IS 'Agrupación determinística de señales (clave senal|agrupador; spec §3.3). SQL crea/actualiza/resuelve; la IA solo escribe titulo, resumen, recomendacion, severidad (en banda), responsable sugerido y fusiones (ia_version = version cuando la redacción está al día). dias_abierta y dias_sin_cambio se calculan en las RPCs (Postgres no admite columnas generadas con now()). fusionada_en: absorbida por otra situación (sale del mapa; reversible).';

-- 3.4 Reglas del CEO.
CREATE TABLE IF NOT EXISTS public.situacion_reglas (
  id             bigserial PRIMARY KEY,
  alcance        text NOT NULL CHECK (alcance IN ('senal','contraparte','documento','situacion')),
  clave_alcance  text NOT NULL,
  accion         text NOT NULL CHECK (accion IN ('ignorar','no_es_problema','severidad_fija','responsable_fijo')),
  valor          jsonb NOT NULL DEFAULT '{}'::jsonb,
  motivo         text,
  vigente_hasta  timestamptz,
  creada_por     text NOT NULL DEFAULT 'ceo',
  creada_en      timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE public.situacion_reglas IS 'Decisiones del CEO que persisten (spec §3.4). clave_alcance: senal → nombre de la señal; contraparte → company:<id> o partner:<odoo_partner_id>; documento → <modelo>:<id>; situacion → clave de la situación.';

-- 3.5 Bitácora de corridas.
CREATE TABLE IF NOT EXISTS public.situacion_corridas (
  id             bigserial PRIMARY KEY,
  corrida        uuid NOT NULL,
  origen         text NOT NULL DEFAULT 'manual',
  iniciada_en    timestamptz NOT NULL DEFAULT now(),
  sql_lista_en   timestamptz,
  terminada_en   timestamptz,
  n_senales      integer,
  n_candidatas   integer,
  n_nuevas       integer,
  n_actualizadas integer,
  n_resueltas    integer,
  n_redactadas   integer,
  n_fusiones     integer,
  n_ignoradas    integer,
  tokens_in      integer,
  tokens_out     integer,
  modelo         text,
  errores        jsonb NOT NULL DEFAULT '[]'::jsonb,
  detalle        jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS situacion_corridas_inicio_idx ON public.situacion_corridas (iniciada_en DESC);

-- §4 regla 1: personas detrás de buzones compartidos (lo manda _push_users desde qb.memoria.mailbox).
CREATE TABLE IF NOT EXISTS public.buzon_personas (
  buzon         text NOT NULL,
  odoo_user_id  integer NOT NULL,
  area          text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (buzon, odoo_user_id)
);

CREATE OR REPLACE FUNCTION public.buzon_personas_reemplazar(p_filas jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE n integer;
BEGIN
  DELETE FROM buzon_personas;
  INSERT INTO buzon_personas (buzon, odoo_user_id, area)
  SELECT lower(f->>'buzon'), (f->>'odoo_user_id')::int, f->>'area'
  FROM jsonb_array_elements(coalesce(p_filas, '[]'::jsonb)) f
  WHERE coalesce(f->>'buzon', '') <> '' AND (f->>'odoo_user_id') IS NOT NULL
  ON CONFLICT (buzon, odoo_user_id) DO UPDATE SET area = EXCLUDED.area, updated_at = now();
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$;
REVOKE ALL ON FUNCTION public.buzon_personas_reemplazar(jsonb) FROM public, anon, authenticated;

-- Permisos: solo service_role (Odoo, Edge Functions y el MCP usan la service key).
REVOKE ALL ON public.senales_config, public.senales, public.senales_lotes, public.situaciones,
              public.situacion_reglas, public.situacion_corridas, public.buzon_personas FROM anon, authenticated;

-- Catálogo inicial (spec §4). Los umbrales se cambian con UPDATE, no con despliegue.
INSERT INTO public.senales_config (senal, titulo, area, tipo, fuente, umbrales, severidad_base, severidad_max, reglas_calidad, agrupar_por, agregar, cada_horas, descripcion) VALUES
-- Comercial
('entrega_vencida', 'Entrega vencida', 'comercial', 'obligacion', 'odoo', '{}', 3, 5, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Salidas a cliente (stock.picking outgoing) no hechas ni canceladas con fecha programada pasada, por cliente.'),
('pedido_sin_fecha', 'Pedido sin fecha comprometida', 'comercial', 'riesgo', 'odoo', '{}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Pedidos confirmados sin commitment_date y con líneas por entregar.'),
('cliente_sin_respuesta', 'Cliente sin respuesta', 'comercial', 'obligacion', 'memoria', '{"dias":3}', 3, 5, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Conversaciones donde el último correo es del cliente y llevan más de N días sin respuesta nuestra, por empresa.'),
('compromiso_correo', 'Compromiso por correo', 'comercial', 'obligacion', 'memoria', '{}', 2, 4, '{"antigua_dias":30,"vencida_dias":21}', 'contraparte', 'cuenta', 1, 'Pendientes de la memoria (memoria_thread_summaries.pendientes) donde quien=nosotros, uno por conversación y texto.'),
('cliente_callado', 'Cliente callado', 'comercial', 'riesgo', 'memoria', '{"factor":2,"min_dias":14,"min_correos":6}', 2, 4, '{"antigua_dias":45}', 'contraparte', 'cuenta', 24, 'Clientes de Odoo sin correo entrante en más de 2× su intervalo habitual (mediana de días entre correos entrantes, 12 meses).'),
('oportunidad_demanda', 'Demanda detectada en correo', 'comercial', 'oportunidad', 'memoria', '{"dias":60}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Señales de demanda (customer_demand_signals) de los últimos N días, una por señal.'),
('lead_frio', 'Oportunidad fría', 'comercial', 'oportunidad', 'odoo', '{"dias":14}', 1, 3, '{"antigua_dias":30,"zombie_dias":120}', 'responsable', 'cuenta', 6, 'crm.lead abierta sin cambio de etapa ni actividad en N días.'),
('venta_margen_negativo', 'Venta con margen negativo', 'comercial', 'problema', 'odoo', '{"dias":90}', 3, 4, '{"antigua_dias":30}', 'contraparte', 'suma', 6, 'Líneas de pedido confirmadas con margen < 0 (últimos N días). Costo 0 o precio 0 = dato_malo.'),
('producto_pierde', 'Producto que pierde', 'comercial', 'problema', 'odoo', '{}', 3, 4, '{"antigua_dias":60}', 'payload', 'suma', 6, 'qb.producto.rentabilidad con semáforo rojo (12 meses).'),
('cliente_pierde', 'Cliente que pierde', 'comercial', 'problema', 'odoo', '{}', 3, 4, '{"antigua_dias":60}', 'contraparte', 'suma', 6, 'qb.cliente.rentabilidad con semáforo rojo (12 meses).'),
('cotizacion_bajo_costo', 'Cotización bajo costo', 'comercial', 'riesgo', 'odoo', '{"dias_vigencia":15}', 3, 4, '{"antigua_dias":30}', 'contraparte', 'cuenta', 6, 'qb.cotizacion en borrador o presentada con semáforo rojo; por vencer si validez_hasta ≤ hoy+N.'),
-- Operaciones
('op_atrasada', 'Orden de producción atrasada', 'operaciones', 'problema', 'odoo', '{"dias":7}', 3, 4, '{"antigua_dias":30,"zombie_dias":90,"limpieza":"Cancelar o cerrar en Fabricación las OPs confirmadas hace más de 90 días sin movimientos."}', 'responsable', 'cuenta', 1, 'mrp.production confirmada/en progreso con inicio planeado hace más de N días, por responsable.'),
('op_sin_componentes', 'OP de la semana sin componentes', 'operaciones', 'riesgo', 'odoo', '{"dias":7}', 3, 4, '{"antigua_dias":14}', 'responsable', 'cuenta', 1, 'OPs que arrancan en los próximos N días con componentes sin reservar (reservation_state != assigned).'),
('tiempos_excepcion', 'Tiempo de máquina fuera de rango', 'operaciones', 'problema', 'odoo', '{"dias":7}', 2, 3, '{"antigua_dias":14}', 'payload', 'cuenta', 6, 'qb.workorder.excepcion de la última semana (lento, rápido, sin horas), por tipo.'),
('existencia_negativa', 'Existencia negativa', 'operaciones', 'problema', 'odoo', '{}', 2, 4, '{"antigua_dias":30,"limpieza":"Ajustar inventario en la ubicación: la cantidad negativa es un error de captura o un consumo sin recepción."}', 'payload', 'cuenta', 1, 'stock.quant en ubicaciones internas con quantity < 0, por ubicación.'),
('reorden_pendiente', 'Reorden pendiente', 'operaciones', 'obligacion', 'odoo', '{}', 2, 3, '{"antigua_dias":14}', 'ninguno', 'cuenta', 1, 'Reglas de reabastecimiento con qty_to_order > 0.'),
('transferencia_atorada', 'Transferencia atorada', 'operaciones', 'problema', 'odoo', '{"dias":7}', 2, 3, '{"antigua_dias":30,"zombie_dias":90,"limpieza":"Cancelar en Inventario las transferencias internas en espera de más de 90 días."}', 'payload', 'cuenta', 1, 'stock.picking internas/de producción confirmadas o en espera hace más de N días, por tipo de operación.'),
('familia_saturada', 'Familia de máquinas saturada', 'operaciones', 'riesgo', 'odoo', '{"pct":90}', 3, 4, '{"antigua_dias":30}', 'documento', 'maximo', 6, 'qb.familia.carga con utilización ≥ N %.'),
('mantenimiento_abierto', 'Mantenimiento abierto', 'operaciones', 'obligacion', 'odoo', '{"dias":7}', 2, 4, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 1, 'maintenance.request en etapa no final hace más de N días, o preventivo con fecha pasada.'),
-- Compras
('recepcion_vencida', 'Recepción vencida', 'compras', 'credito', 'odoo', '{}', 3, 4, '{"antigua_dias":30,"zombie_dias":120}', 'contraparte', 'cuenta', 1, 'Entradas de proveedor (stock.picking incoming) no hechas con fecha programada pasada, por proveedor.'),
('oc_sin_confirmacion', 'Orden de compra sin acuse', 'compras', 'riesgo', 'odoo', '{"dias":5}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'purchase.order confirmada hace más de N días sin referencia del proveedor ni recepción.'),
('proveedor_esperando', 'Proveedor espera respuesta nuestra', 'compras', 'obligacion', 'memoria', '{}', 2, 4, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Conversaciones abiertas con proveedor donde esperando_a=nosotros.'),
('esperando_proveedor', 'Esperamos al proveedor', 'compras', 'credito', 'memoria', '{}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Conversaciones abiertas con proveedor donde esperando_a=ellos.'),
('aprobacion_pendiente', 'Aprobación pendiente', 'compras', 'obligacion', 'odoo', '{"dias":2}', 2, 4, '{"antigua_dias":30}', 'responsable', 'cuenta', 1, 'approval.request nueva/pendiente hace más de N días y purchase.requisition abiertas.'),
('precio_compra_subio', 'Precio de compra subió', 'compras', 'riesgo', 'odoo', '{"pct":15,"meses":6,"dias":30}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 6, 'Último precio de compra de un producto (N días) vs promedio de los M meses previos > pct %.'),
('actividad_vencida_oc', 'Actividad vencida en compras', 'compras', 'obligacion', 'odoo', '{}', 2, 3, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 1, 'mail.activity vencida sobre purchase.order, por usuario.'),
('proveedor_reprobado', 'Proveedor reprobado', 'compras', 'riesgo', 'odoo', '{"score":70}', 2, 3, '{"antigua_dias":60}', 'contraparte', 'maximo', 24, 'Última sgi.supplier.eval por proveedor con score < N.'),
-- Finanzas
('cartera_vencida', 'Cartera vencida', 'finanzas', 'credito', 'odoo', '{"rfc_relacionados":["GQU920609JNA","MITJ991130TV7","MIDJ4003178X9","MIPJ691003QJ1","AOMS630418PP1"]}', 3, 5, '{"antigua_dias":30}', 'contraparte', 'suma', 1, 'Facturas de cliente publicadas, no pagadas o parciales, con vencimiento pasado, por cliente. RFC en rfc_relacionados = dato_malo (parte relacionada).'),
('promesa_pago_vencida', 'Promesa de pago vencida', 'finanzas', 'credito', 'memoria', '{}', 3, 5, '{"antigua_dias":30,"vencida_dias":21}', 'contraparte', 'cuenta', 1, 'email_pending_actions tipo promesa_pago abiertas con deadline pasado; cae en la situación de cartera_vencida del mismo cliente por contraparte.'),
('cxp_vencida', 'Cuentas por pagar vencidas', 'finanzas', 'obligacion', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'contraparte', 'suma', 1, 'Facturas de proveedor publicadas, no pagadas o parciales, vencidas, por proveedor.'),
('factura_proveedor_borrador', 'Factura de proveedor en borrador', 'finanzas', 'obligacion', 'odoo', '{"dias":3}', 2, 3, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'account.move in_invoice en borrador hace más de N días.'),
('entregado_sin_facturar', 'Entregado sin facturar', 'finanzas', 'credito', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'contraparte', 'suma', 1, 'sale.order con invoice_status = to invoice, por cliente.'),
('banco_sin_conciliar', 'Banco sin conciliar', 'finanzas', 'obligacion', 'odoo', '{}', 2, 4, '{"antigua_dias":30}', 'payload', 'cuenta', 1, 'account.bank.statement.line no conciliadas, por diario, con antigüedad.'),
('cfdi_cancelacion_pendiente', 'CFDI con cancelación pendiente', 'finanzas', 'obligacion', 'odoo', '{}', 2, 3, '{"antigua_dias":14}', 'contraparte', 'cuenta', 1, 'account.move con l10n_mx_edi_cfdi_state = cancel_requested.'),
('sat_discrepancia', 'Discrepancia Odoo ↔ SAT', 'finanzas', 'problema', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'payload', 'cuenta', 6, 'sat.compare.line con issue en (monto, moneda, cancelado_odoo, cancelado_sat, solo_sat, solo_odoo), por tipo de discrepancia.'),
('sat_complemento', 'Complemento de pago faltante o duplicado', 'finanzas', 'problema', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'payload', 'cuenta', 6, 'sat.pago.compare con issue en (sin_complemento, complemento_duplicado).'),
('sat_extraccion_detenida', 'Extracción del SAT detenida', 'finanzas', 'problema', 'odoo', '{"dias":3}', 4, 5, '{}', 'ninguno', 'maximo', 1, 'res.company.sat_data_until_* con más de N días de atraso.'),
('nomina_borrador', 'Nómina en borrador', 'finanzas', 'obligacion', 'odoo', '{"dias":3}', 2, 3, '{"antigua_dias":30,"zombie_dias":120,"limpieza":"Confirmar o cancelar en Nómina los recibos en borrador de periodos ya pagados."}', 'payload', 'cuenta', 6, 'hr.payslip en borrador con date_to < hoy − N, por lote/periodo.'),
('cash_bajo_piso', 'Efectivo bajo el piso', 'finanzas', 'riesgo', 'odoo', '{}', 4, 5, '{}', 'ninguno', 'maximo', 6, 'Semanas de la proyección de flujo con saldo final < saldo mínimo; runway = primera semana ≤ 0.'),
('indicador_financiero_rojo', 'Indicador financiero en rojo', 'finanzas', 'problema', 'odoo', '{"nombres":["DSO","cartera","DPO"],"dias":60}', 3, 4, '{"antigua_dias":45}', 'payload', 'cuenta', 6, 'sgi.indicator.measure validadas en rojo cuyo indicador se llama como alguno de umbrales.nombres.'),
-- Calidad / SGI
('indicador_rojo', 'Indicador en rojo', 'calidad_sgi', 'problema', 'odoo', '{"dias":60}', 2, 4, '{"antigua_dias":45}', 'payload', 'cuenta', 6, 'sgi.indicator.measure con semaphore=rojo y state=validado, periodo reciente, por indicador.'),
('accion_correctiva_vencida', 'Acción correctiva vencida', 'calidad_sgi', 'obligacion', 'odoo', '{}', 3, 4, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 1, 'sgi.action.line con state=vencida, por responsable.'),
('nc_abierta', 'No conformidad abierta', 'calidad_sgi', 'problema', 'odoo', '{"dias":7}', 2, 4, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 1, 'quality.alert en etapa no final hace más de N días.'),
('reclamacion_cliente', 'Reclamación de cliente', 'calidad_sgi', 'problema', 'memoria', '{}', 3, 5, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Conversaciones abiertas con cliente en tono tenso.'),
('calibracion_vencida', 'Calibración vencida', 'calidad_sgi', 'obligacion', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'ninguno', 'cuenta', 6, 'maintenance.equipment con sgi_calibration_state=vencido o sgi_do_not_use.'),
('legal_incumplido', 'Requisito legal incumplido', 'calidad_sgi', 'obligacion', 'odoo', '{}', 3, 5, '{"antigua_dias":45}', 'ninguno', 'cuenta', 6, 'sgi.legal.requirement con compliance_state en (no_cumple, parcial) o next_eval_date ≤ hoy.'),
('riesgo_sin_tratar', 'Riesgo sin tratar', 'calidad_sgi', 'riesgo', 'odoo', '{}', 3, 4, '{"antigua_dias":45}', 'ninguno', 'cuenta', 6, 'sgi.risk con attention_level en (inmediata, alto) y state=identificado.'),
('ppap_rechazado', 'PPAP rechazado', 'calidad_sgi', 'problema', 'odoo', '{}', 3, 4, '{"antigua_dias":30}', 'contraparte', 'cuenta', 6, 'sgi.ppap con state=rechazado.'),
('auditoria_pendiente', 'Auditoría pendiente', 'calidad_sgi', 'obligacion', 'odoo', '{}', 2, 3, '{"antigua_dias":45}', 'responsable', 'cuenta', 24, 'sgi.audit.program.line pendiente cuyo mes planeado ya pasó.'),
('fuente_sgi_apagada', 'Fuente de alertas SGI apagada', 'calidad_sgi', 'higiene', 'odoo', '{}', 1, 2, '{}', 'ninguno', 'cuenta', 6, 'sgi.alert.source con enabled=false y suppressed_count > 0: explica silencios.'),
-- RH
('aprobacion_rh', 'Aprobación de RH pendiente', 'rh', 'obligacion', 'odoo', '{"dias":2}', 2, 3, '{"antigua_dias":30}', 'responsable', 'cuenta', 1, 'approval.request de categoría RH pendiente y hr.leave por aprobar.'),
('pendiente_rh_correo', 'Pendiente de RH por correo', 'rh', 'obligacion', 'memoria', '{}', 2, 4, '{"antigua_dias":30}', 'contraparte', 'cuenta', 1, 'Pendientes (quien=nosotros) en conversaciones de los buzones de RH.'),
('evaluacion_vencida', 'Evaluación vencida', 'rh', 'obligacion', 'odoo', '{}', 1, 3, '{"antigua_dias":45}', 'responsable', 'cuenta', 24, 'hr.appraisal vencida y sgi.competence.gap abierta.'),
-- Sistemas
('ticket_abierto', 'Ticket abierto', 'sistemas', 'obligacion', 'odoo', '{"dias":7}', 2, 3, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 1, 'helpdesk.ticket no resuelto hace más de N días.'),
('job_caido', 'Proceso automático caído', 'sistemas', 'problema', 'watchdog', '{}', 4, 5, '{}', 'ninguno', 'cuenta', 1, 'Lo que el watchdog (Edge Function health) detecta: jobs pg_cron atrasados, push de Odoo viejo, lotes de señales sin llegar.'),
-- Dirección
('carga_actividades', 'Actividades vencidas', 'direccion', 'obligacion', 'odoo', '{}', 2, 4, '{"antigua_dias":30,"zombie_dias":180,"limpieza":"Cerrar o cancelar en Odoo las actividades vencidas hace más de 180 días; no describen trabajo real."}', 'responsable', 'suma', 1, 'mail.activity vencidas por usuario (fila aparte para las de más de 180 días, que caen en zombie).'),
('firma_pendiente', 'Firma pendiente', 'direccion', 'obligacion', 'odoo', '{"dias":7}', 2, 3, '{"antigua_dias":30,"zombie_dias":180}', 'responsable', 'cuenta', 6, 'sign.request enviada hace más de N días sin completar.'),
('acuse_documento', 'Acuse de documento pendiente', 'direccion', 'obligacion', 'odoo', '{}', 1, 2, '{"antigua_dias":45}', 'responsable', 'cuenta', 24, 'sgi.document.ack con state=pendiente, por persona.'),
('acuerdo_direccion_vencido', 'Acuerdo de dirección vencido', 'direccion', 'obligacion', 'odoo', '{}', 3, 4, '{"antigua_dias":45}', 'responsable', 'cuenta', 24, 'sgi.management.review.agreement con deadline pasado y sin cerrar.'),
('obligacion_legado', 'Obligación (qb_obligation)', 'direccion', 'obligacion', 'odoo', '{}', 2, 3, '{"antigua_dias":30}', 'documento', 'cuenta', 1, 'Puente hasta retirar qb_obligation (plan B, paso 6): sus registros abiertos, uno por obligación.'),
('delegacion_estado', 'Estado de delegación', 'direccion', 'obligacion', 'odoo', '{}', 1, 2, '{}', 'situacion', 'cuenta', 1, 'Eventos hecha/cancelada de actividades delegadas (plan B, paso 5). Sin consulta en este plan.')
ON CONFLICT (senal) DO NOTHING;

INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Situación plan A paso 1: esquema (senales, senales_lotes, senales_config, situaciones, situacion_reglas, situacion_corridas, buzon_personas) y catálogo inicial',
        jsonb_build_object('migration', '20260919a_situacion_esquema'));
COMMIT;
```

- [ ] **Step 2: Escribir la prueba en seco del esquema**

`supabase/tests/situacion/00_esquema.sql`:

```sql
-- Prueba en seco: termina SIEMPRE con el error PRUEBA_OK (deshace todo).
DO $t$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM senales_config WHERE activa;
  ASSERT n >= 60, 'catálogo incompleto: ' || n;
  ASSERT (SELECT count(*) FROM senales_config WHERE fuente = 'memoria') = 9, 'señales de memoria';
  -- FK: una señal fuera del catálogo no entra.
  BEGIN
    INSERT INTO senales (clave, senal, area, tipo, fuente) VALUES ('x:1', 'no_existe', 'finanzas', 'problema', 'odoo');
    RAISE EXCEPTION 'la FK a senales_config no está';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  -- Único parcial: dos abiertas con la misma clave no caben; una resuelta sí.
  INSERT INTO senales (clave, senal, area, tipo, fuente, resuelta_en) VALUES ('cartera_vencida:partner:0', 'cartera_vencida', 'finanzas', 'credito', 'odoo', now());
  INSERT INTO senales (clave, senal, area, tipo, fuente) VALUES ('cartera_vencida:partner:0', 'cartera_vencida', 'finanzas', 'credito', 'odoo');
  BEGIN
    INSERT INTO senales (clave, senal, area, tipo, fuente) VALUES ('cartera_vencida:partner:0', 'cartera_vencida', 'finanzas', 'credito', 'odoo');
    RAISE EXCEPTION 'el índice único parcial no está';
  EXCEPTION WHEN unique_violation THEN NULL; END;
  PERFORM buzon_personas_reemplazar('[{"buzon":"Ventas@Quimibond.com","odoo_user_id":7,"area":"comercial"}]'::jsonb);
  ASSERT (SELECT odoo_user_id FROM buzon_personas WHERE buzon = 'ventas@quimibond.com') = 7, 'buzon_personas en minúsculas';
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 3: Aplicar la migración a producción**

MCP `apply_migration` (project `tozqezmivpblmcubmnpi`, name `20260919a_situacion_esquema`, query = contenido del archivo). Luego `execute_sql` con el contenido de `00_esquema.sql`.
Expected: el resultado es un error cuyo mensaje contiene `PRUEBA_OK`. Si el error es otro, la migración tiene un bug: corrige y vuelve a aplicar (es idempotente).

- [ ] **Step 4: Commit**

```bash
cd /home/user/quimibond-intelligence && git add supabase/migrations/20260919a_situacion_esquema.sql supabase/tests/situacion/00_esquema.sql
git -c user.name="Jose J. Mizrahi" -c user.email="jose.mizrahi@quimibond.com" commit -F /tmp/claude-0/-home-user/06f8bbe6-40ff-532e-9377-375a032eba82/scratchpad/msg.txt
# msg.txt: "Situación: esquema, catálogo de 62 señales y buzon_personas (plan A, paso 1)" + porqué + atribución
```

### Task 1.2: Ingesta por lote (`senales_ingestar`, `senales_push_terminado`)

**Files:**
- Create: `supabase/migrations/20260919b_situacion_ingesta.sql`
- Create: `supabase/tests/situacion/01_ingesta.sql`

- [ ] **Step 1: Escribir la prueba en seco (falla porque las funciones no existen)**

`supabase/tests/situacion/01_ingesta.sql`:

```sql
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); s record;
BEGIN
  INSERT INTO senales_config (senal, titulo, area, tipo, fuente, agrupar_por, agregar)
  VALUES ('_prueba', 'Prueba', 'finanzas', 'credito', 'odoo', 'contraparte', 'suma');

  -- 1. Lote con dos claves → dos nuevas, lote ok.
  r := senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":100,"valor_texto":"cien","documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}],"payload":{"rfc":"XAXX010101000"}},
    {"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":5}
  ]'::jsonb);
  ASSERT (r->>'ok')::bool AND (r->>'nuevas')::int = 2 AND (r->>'resueltas')::int = 0, 'lote 1: ' || r;
  ASSERT (SELECT n_claves FROM senales_lotes WHERE corrida = c AND senal = '_prueba') = 2, 'senales_lotes';
  SELECT * INTO s FROM senales WHERE clave = '_prueba:partner:1' AND resuelta_en IS NULL;
  ASSERT s.agrupador = 'partner:1' AND s.area = 'finanzas' AND s.episodio = 1, 'agrupador/area/episodio: ' || s.agrupador;

  -- 2. Mismo valor → actualizada sin cambio de valor; valor nuevo → valor_cambio_en avanza.
  PERFORM pg_sleep(0.01);
  r := senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[{"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":100},{"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":9}]'::jsonb);
  ASSERT (r->>'actualizadas')::int = 2 AND (r->>'nuevas')::int = 0, 'lote 2: ' || r;
  ASSERT (SELECT valor_cambio_en = primera_vista FROM senales WHERE clave = '_prueba:partner:1' AND resuelta_en IS NULL), 'valor igual no mueve valor_cambio_en';
  ASSERT (SELECT valor_cambio_en > primera_vista FROM senales WHERE clave = '_prueba:partner:2' AND resuelta_en IS NULL), 'valor distinto mueve valor_cambio_en';

  -- 3. Lote sin la clave 2 → se resuelve; la 1 sigue abierta.
  r := senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[{"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":100}]'::jsonb);
  ASSERT (r->>'resueltas')::int = 1, 'lote 3: ' || r;
  ASSERT (SELECT resuelta_en IS NOT NULL FROM senales WHERE clave = '_prueba:partner:2'), 'clave 2 resuelta';

  -- 4. Reaparece → episodio 2, fila nueva, la vieja conserva resuelta_en.
  r := senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[{"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":100},{"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":1}]'::jsonb);
  ASSERT (SELECT episodio FROM senales WHERE clave = '_prueba:partner:2' AND resuelta_en IS NULL) = 2, 'episodio 2';
  ASSERT (SELECT count(*) FROM senales WHERE clave = '_prueba:partner:2') = 2, 'dos filas de la clave 2';

  -- 5. Lote vacío = todo resuelto (lista completa). Lote malformado = ok=false y fila de lote con error, sin tocar señales.
  r := senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[]'::jsonb);
  ASSERT (r->>'resueltas')::int = 2, 'lote vacío resuelve todo: ' || r;
  r := senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[{"clave":"_prueba:partner:3","valor":"no-es-numero"}]'::jsonb);
  ASSERT NOT (r->>'ok')::bool AND r->>'error' IS NOT NULL, 'lote malo: ' || r;
  ASSERT (SELECT ok = false AND error IS NOT NULL FROM senales_lotes WHERE senal = '_prueba' ORDER BY id DESC LIMIT 1), 'lote malo registrado';
  ASSERT (SELECT count(*) FROM senales WHERE clave = '_prueba:partner:3') = 0, 'lote malo no inserta';

  -- 6. Fuente equivocada o señal desconocida: excepción (error de configuración, no de datos).
  BEGIN
    PERFORM senales_ingestar('_prueba', 'memoria', gen_random_uuid(), '[]'::jsonb);
    RAISE EXCEPTION 'debió rechazar la fuente';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM = 'debió rechazar la fuente' THEN RAISE; END IF;
  END;

  -- 7. push_terminado nunca falla aunque la Edge Function no exista.
  ASSERT senales_push_terminado(c, 'odoo') IS NOT NULL, 'push_terminado';
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correrla y ver que falla**

MCP `execute_sql` con el archivo. Expected: error `function senales_ingestar(...) does not exist` (no `PRUEBA_OK`).

- [ ] **Step 3: Escribir la migración**

```sql
-- 2026-09-19b — Situación plan A: ingesta por lote (spec §3.1.1, §4 regla 2).
-- senales_ingestar: UNA llamada por señal con la lista COMPLETA de claves
-- activas. Upsert por clave abierta, episodio nuevo si reaparece, cierre de
-- lo que no viene, y una fila en senales_lotes. Un lote malformado devuelve
-- ok=false (y su fila con error) sin tocar señales: así la resolución nunca
-- ocurre por un lote roto. Señal desconocida o fuente equivocada = excepción.
BEGIN;

-- Fecha tolerante: texto inválido → NULL, nunca error (los pendientes de la memoria traen fechas libres).
CREATE OR REPLACE FUNCTION public.situacion_fecha(p text)
RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN p::date;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END $$;

-- Persona detrás de un buzón: catálogo buzon_personas (compartidos) y, si no, el usuario con ese correo.
CREATE OR REPLACE FUNCTION public.situacion_user_de_buzon(p_buzon text)
RETURNS integer LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT coalesce(
    (SELECT odoo_user_id FROM buzon_personas WHERE buzon = lower(p_buzon) ORDER BY updated_at DESC LIMIT 1),
    (SELECT odoo_user_id FROM odoo_users WHERE lower(email) = lower(p_buzon) LIMIT 1))
$$;

CREATE OR REPLACE FUNCTION public.senales_ingestar(p_senal text, p_fuente text, p_corrida uuid, p_filas jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  cfg record; n_nuevas int := 0; n_act int := 0; n_res int := 0; n_in int := 0;
BEGIN
  SELECT * INTO cfg FROM senales_config WHERE senal = p_senal;
  IF NOT FOUND THEN RAISE EXCEPTION 'senal % no está en senales_config', p_senal; END IF;
  IF cfg.fuente <> p_fuente THEN RAISE EXCEPTION 'senal % es de fuente %, no %', p_senal, cfg.fuente, p_fuente; END IF;
  IF p_corrida IS NULL THEN RAISE EXCEPTION 'p_corrida es obligatorio'; END IF;

  BEGIN
    IF jsonb_typeof(p_filas) <> 'array' THEN RAISE EXCEPTION 'p_filas debe ser un array'; END IF;
    DROP TABLE IF EXISTS _lote;
    CREATE TEMP TABLE _lote AS
    SELECT DISTINCT ON (f->>'clave')
           f->>'clave'                              AS clave,
           coalesce(f->'documentos', '[]'::jsonb)   AS documentos,
           (f->>'company_id')::bigint               AS company_id,
           (f->>'odoo_partner_id')::int             AS odoo_partner_id,
           (f->>'responsable_odoo_user_id')::int    AS responsable_odoo_user_id,
           (f->>'valor')::numeric                   AS valor,
           left(f->>'valor_texto', 300)             AS valor_texto,
           situacion_fecha(f->>'vence')             AS vence,
           coalesce(f->'payload', '{}'::jsonb)      AS payload,
           NULL::text                               AS agrupador
    FROM jsonb_array_elements(p_filas) f
    WHERE coalesce(f->>'clave', '') <> '';
    GET DIAGNOSTICS n_in = ROW_COUNT;

    -- Contraparte: Odoo manda odoo_partner_id; la empresa de la memoria se resuelve aquí.
    UPDATE _lote l SET company_id = (SELECT c.id FROM companies c WHERE c.odoo_partner_id = l.odoo_partner_id ORDER BY c.id LIMIT 1)
    WHERE l.company_id IS NULL AND l.odoo_partner_id IS NOT NULL;

    UPDATE _lote SET agrupador = CASE cfg.agrupar_por
      WHEN 'contraparte' THEN coalesce('company:' || company_id, 'partner:' || odoo_partner_id, 'sin_contraparte')
      WHEN 'documento'   THEN coalesce('doc:' || (documentos->0->>'modelo') || ':' || (documentos->0->>'id'), 'doc:' || clave)
      WHEN 'responsable' THEN 'user:' || coalesce(responsable_odoo_user_id::text, '0')
      WHEN 'situacion'   THEN 'situacion:' || coalesce(payload->>'situacion_id', '0')
      WHEN 'payload'     THEN 'grupo:' || coalesce(payload->>'grupo', 'todas')
      ELSE 'todas' END;

    -- Abiertas que vienen en el lote: se actualizan (valor_cambio_en solo si el valor cambió).
    UPDATE senales s SET
      vista_en = now(),
      valor_cambio_en = CASE WHEN s.valor IS DISTINCT FROM l.valor THEN now() ELSE s.valor_cambio_en END,
      valor = l.valor, valor_texto = l.valor_texto, vence = l.vence, documentos = l.documentos,
      company_id = coalesce(l.company_id, s.company_id),
      odoo_partner_id = coalesce(l.odoo_partner_id, s.odoo_partner_id),
      responsable_odoo_user_id = coalesce(l.responsable_odoo_user_id, s.responsable_odoo_user_id),
      payload = s.payload || l.payload, agrupador = l.agrupador
    FROM _lote l
    WHERE s.clave = l.clave AND s.senal = p_senal AND s.resuelta_en IS NULL;
    GET DIAGNOSTICS n_act = ROW_COUNT;

    -- Claves sin fila abierta: episodio nuevo.
    INSERT INTO senales (clave, episodio, senal, area, tipo, fuente, agrupador, documentos, company_id, odoo_partner_id,
                         responsable_odoo_user_id, valor, valor_texto, vence, payload)
    SELECT l.clave, 1 + (SELECT count(*) FROM senales p WHERE p.clave = l.clave), p_senal, cfg.area, cfg.tipo, p_fuente,
           l.agrupador, l.documentos, l.company_id, l.odoo_partner_id, l.responsable_odoo_user_id, l.valor, l.valor_texto, l.vence, l.payload
    FROM _lote l
    WHERE NOT EXISTS (SELECT 1 FROM senales s WHERE s.clave = l.clave AND s.resuelta_en IS NULL);
    GET DIAGNOSTICS n_nuevas = ROW_COUNT;

    -- Lo abierto de ESTA señal que no vino: resuelto (lista completa).
    UPDATE senales s SET resuelta_en = now()
    WHERE s.senal = p_senal AND s.resuelta_en IS NULL
      AND NOT EXISTS (SELECT 1 FROM _lote l WHERE l.clave = s.clave);
    GET DIAGNOSTICS n_res = ROW_COUNT;

    INSERT INTO senales_lotes (senal, fuente, corrida, n_claves, n_nuevas, n_actualizadas, n_resueltas, ok)
    VALUES (p_senal, p_fuente, p_corrida, n_in, n_nuevas, n_act, n_res, true);
    DROP TABLE IF EXISTS _lote;
    RETURN jsonb_build_object('ok', true, 'senal', p_senal, 'n', n_in, 'nuevas', n_nuevas, 'actualizadas', n_act, 'resueltas', n_res);
  EXCEPTION WHEN OTHERS THEN
    -- Todo lo anterior del bloque se deshace; solo queda el registro del lote fallido.
    INSERT INTO senales_lotes (senal, fuente, corrida, n_claves, ok, error)
    VALUES (p_senal, p_fuente, p_corrida, coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(p_filas) = 'array' THEN p_filas END), 0), false, left(SQLERRM, 500));
    RETURN jsonb_build_object('ok', false, 'senal', p_senal, 'error', left(SQLERRM, 500));
  END;
END $$;
REVOKE ALL ON FUNCTION public.senales_ingestar(text, text, uuid, jsonb) FROM public, anon, authenticated;
COMMENT ON FUNCTION public.senales_ingestar(text, text, uuid, jsonb) IS
  'Ingesta por lote (spec §3.1.1). Una llamada por señal con TODAS sus claves activas: upsert por clave abierta, episodio nuevo si reaparece, cierra lo que no viene, registra el lote. Lote malformado → ok=false, nada se toca. Filas: {clave, documentos[], company_id?, odoo_partner_id?, responsable_odoo_user_id?, valor?, valor_texto?, vence?, payload{}}.';

-- Fin del push: dispara la consolidación por evento. Tolera que la Edge Function no exista (pasos 2→3 del plan).
CREATE OR REPLACE FUNCTION public.senales_push_terminado(p_corrida uuid, p_origen text DEFAULT 'odoo')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, extensions, pg_temp AS $$
DECLARE v_req bigint; v_lotes int; v_err text;
BEGIN
  SELECT count(*) INTO v_lotes FROM senales_lotes WHERE corrida = p_corrida;
  BEGIN
    v_req := invoke_edge('situacion-consolidar', jsonb_build_object('corrida', p_corrida, 'origen', p_origen));
  EXCEPTION WHEN OTHERS THEN
    v_err := left(SQLERRM, 300);
  END;
  INSERT INTO pipeline_logs (level, phase, message, details)
  VALUES (CASE WHEN v_err IS NULL THEN 'info' ELSE 'warning' END, 'situacion',
          format('Push terminado (%s): %s lotes; consolidación %s', p_origen, v_lotes, CASE WHEN v_err IS NULL THEN 'disparada' ELSE 'NO disparada: ' || v_err END),
          jsonb_build_object('corrida', p_corrida, 'origen', p_origen, 'lotes', v_lotes, 'request_id', v_req, 'error', v_err));
  RETURN jsonb_build_object('ok', true, 'corrida', p_corrida, 'lotes', v_lotes, 'request_id', v_req, 'error', v_err);
END $$;
REVOKE ALL ON FUNCTION public.senales_push_terminado(uuid, text) FROM public, anon, authenticated;

COMMIT;
```

- [ ] **Step 4: Aplicar y correr la prueba**

`apply_migration` (`20260919b_situacion_ingesta`) y `execute_sql` con `01_ingesta.sql`. Expected: error `PRUEBA_OK`.
Si el paso 5 de la prueba (lote malformado) no devuelve `ok=false`: revisa que el cast `(f->>'valor')::numeric` esté dentro del bloque `BEGIN … EXCEPTION`.

- [ ] **Step 5: Commit**

`git add supabase/migrations/20260919b_situacion_ingesta.sql supabase/tests/situacion/01_ingesta.sql` y commit: "Situación: senales_ingestar por lote con episodios y senales_push_terminado".

### Task 1.3: Señales de la memoria (`senales_memoria`)

Las 9 señales `fuente='memoria'` salen de SQL puro sobre `threads`, `memoria_thread_summaries`, `email_pending_actions`, `customer_demand_signals` y `emails`, y **pasan por el mismo `senales_ingestar`** (una llamada por señal, `fuente='memoria'`). Columnas que existen hoy (verificado 2026-09-18): `threads(status, last_sender_type, last_sender, last_activity, company_id, account, subject, conv_key, message_count)`, `memoria_thread_summaries(thread_id, conv_key, company_id, account, tema, estado, esperando_a, tono, pendientes jsonb, summarized_through)`, `email_pending_actions(id, thread_id, tipo, descripcion, deadline, company_id, account, status)`, `customer_demand_signals(id, thread_id, company_id, product_ref, product_desc, qty, uom, demand_date, detected_at)`, `emails(thread_id, company_id, sender_type, email_date)`, `companies(id, name, odoo_partner_id, is_customer, is_supplier, domain)`, `gmail_accounts(email, department)` (department es NULL en las 52).

**Files:**
- Create: `supabase/migrations/20260919c_situacion_memoria.sql`
- Create: `supabase/tests/situacion/02_memoria.sql`

- [ ] **Step 1: Prueba en seco**

```sql
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); n int;
BEGIN
  r := senales_memoria(c);
  ASSERT (r->'cliente_sin_respuesta'->>'ok')::bool, 'cliente_sin_respuesta: ' || (r->'cliente_sin_respuesta');
  ASSERT (r->'compromiso_correo'->>'ok')::bool, 'compromiso_correo: ' || (r->'compromiso_correo');
  ASSERT (SELECT count(*) FROM senales_lotes WHERE corrida = c AND fuente = 'memoria' AND ok) >= 8, 'ocho o nueve lotes de memoria (cliente_callado solo en su turno de 24 h)';
  -- Con datos reales hay conversaciones sin respuesta (57 el 18-sep) y compromisos (452).
  SELECT count(*) INTO n FROM senales WHERE senal = 'cliente_sin_respuesta' AND resuelta_en IS NULL;
  ASSERT n > 0, 'cliente_sin_respuesta sin filas';
  ASSERT (SELECT bool_and(clave LIKE 'cliente_sin_respuesta:company:%' AND company_id IS NOT NULL AND agrupador LIKE 'company:%') FROM senales WHERE senal = 'cliente_sin_respuesta' AND resuelta_en IS NULL), 'claves por empresa';
  SELECT count(*) INTO n FROM senales WHERE senal = 'compromiso_correo' AND resuelta_en IS NULL;
  ASSERT n > 0, 'compromiso_correo sin filas';
  ASSERT (SELECT bool_and(payload ? 'ultimo_correo' AND payload ? 'que') FROM senales WHERE senal = 'compromiso_correo' AND resuelta_en IS NULL), 'payload del compromiso';
  -- Idempotente: segunda corrida no crea filas nuevas.
  r := senales_memoria(gen_random_uuid());
  ASSERT (r->'compromiso_correo'->>'nuevas')::int = 0, 'segunda corrida: ' || (r->'compromiso_correo');
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correrla → falla con `function senales_memoria(uuid) does not exist`**

- [ ] **Step 3: Escribir la migración**

```sql
-- 2026-09-19c — Situación plan A: señales de la memoria (spec §4, fuente=memoria).
-- SQL puro, sin Claude. Cada señal manda su lista completa por senales_ingestar.
BEGIN;

-- Buzón que más participa en una conversación (conv_key) → persona.
CREATE OR REPLACE FUNCTION public.situacion_responsable_conv(p_conv_key text)
RETURNS integer LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT situacion_user_de_buzon(t.account)
  FROM threads t
  WHERE coalesce(t.conv_key, t.gmail_thread_id) = p_conv_key
  ORDER BY t.message_count DESC, t.id LIMIT 1
$$;

-- Buzón que más atiende a una empresa (vista memoria_encargados, rank 1) → persona.
CREATE OR REPLACE FUNCTION public.situacion_responsable_empresa(p_company_id bigint)
RETURNS integer LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT situacion_user_de_buzon(mailbox) FROM memoria_encargados
  WHERE company_id = p_company_id AND area IS NULL ORDER BY rank LIMIT 1
$$;

-- Turno de una señal: sin lote bueno más reciente que cada_horas (menos 5 min de holgura).
CREATE OR REPLACE FUNCTION public.senales_en_turno(p_senal text)
RETURNS boolean LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT NOT EXISTS (
    SELECT 1 FROM senales_lotes l JOIN senales_config c ON c.senal = l.senal
    WHERE l.senal = p_senal AND l.ok
      AND l.recibido_en > now() - make_interval(hours => c.cada_horas) + interval '5 minutes')
$$;

CREATE OR REPLACE FUNCTION public.senales_memoria(p_corrida uuid DEFAULT gen_random_uuid())
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  out jsonb := '{}'::jsonb; filas jsonb; u jsonb;
  v_dias int; v_factor numeric; v_min_dias int; v_min_correos int;
  noise CONSTANT text := '(no-?reply|postmaster|mailer-daemon|notificacion|notification|newsletter|digest|automated|donotreply)';
BEGIN
  -- 1. cliente_sin_respuesta: por empresa, hilos donde el último correo es del cliente.
  SELECT coalesce((umbrales->>'dias')::int, 3) INTO v_dias FROM senales_config WHERE senal = 'cliente_sin_respuesta';
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', 'cliente_sin_respuesta:company:' || c.id,
      'company_id', c.id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', situacion_user_de_buzon(mode() WITHIN GROUP (ORDER BY t.account)),
      'valor', count(*),
      'valor_texto', count(*) || ' conversación(es) sin respuesta; la más vieja lleva ' || max(extract(day FROM now() - t.last_activity))::int || ' días',
      'documentos', to_jsonb((array_agg(jsonb_build_object('modelo', 'thread', 'id', t.id, 'nombre', left(coalesce(t.subject, '(sin asunto)'), 120), 'fecha', t.last_activity::date) ORDER BY t.last_activity))[1:5]),
      'payload', jsonb_build_object('ultimo_correo', max(t.last_activity), 'dias_max', max(extract(day FROM now() - t.last_activity))::int)
    ) AS x
    FROM threads t
    JOIN companies c ON c.id = t.company_id AND c.odoo_partner_id IS NOT NULL
    WHERE t.status IN ('needs_response', 'stalled') AND t.last_sender_type = 'external'
      AND t.last_activity < now() - make_interval(days => v_dias) AND t.last_activity > now() - interval '90 days'
      AND NOT memoria_generic_domain(coalesce(nullif(c.domain, ''), 'sin-dominio.x'))
      AND coalesce(t.last_sender, '') !~* noise
    GROUP BY c.id, c.odoo_partner_id) q;
  out := out || jsonb_build_object('cliente_sin_respuesta', senales_ingestar('cliente_sin_respuesta', 'memoria', p_corrida, filas));

  -- 2. compromiso_correo: pendientes quien=nosotros de conversaciones abiertas (clave por conversación + hash del texto).
  --    Se dejan de emitir las que ya están etiquetadas vencida_memoria (§5.2): así se resuelven por lote.
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', k.clave,
      'company_id', s.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', coalesce(situacion_user_de_buzon(s.account), situacion_responsable_conv(s.conv_key)),
      'valor', extract(day FROM now() - t.last_activity)::int,
      'valor_texto', left(p.value->>'que', 200),
      'vence', situacion_fecha(p.value->>'vence'),
      'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', s.thread_id, 'nombre', left(coalesce(s.tema, t.subject, ''), 120), 'fecha', t.last_activity::date)),
      'payload', jsonb_build_object('que', left(p.value->>'que', 300), 'ultimo_correo', t.last_activity, 'conv_key', s.conv_key, 'buzon', s.account)
    ) AS x
    FROM memoria_thread_summaries s
    JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(s.pendientes) = 'array' THEN s.pendientes ELSE '[]'::jsonb END) p ON true
    JOIN threads t ON t.id = s.thread_id
    JOIN companies c ON c.id = s.company_id AND c.odoo_partner_id IS NOT NULL
    CROSS JOIN LATERAL (SELECT 'compromiso_correo:conv:' || md5(coalesce(s.conv_key, s.thread_id::text) || '|' || lower(trim(coalesce(p.value->>'que', '')))) AS clave) k
    WHERE s.estado = 'abierto' AND p.value->>'quien' = 'nosotros' AND coalesce(p.value->>'que', '') <> ''
      AND s.summarized_through > now() - interval '120 days'
      AND NOT EXISTS (SELECT 1 FROM senales z WHERE z.clave = k.clave AND z.resuelta_en IS NULL AND z.calidad = 'vencida_memoria')) q;
  out := out || jsonb_build_object('compromiso_correo', senales_ingestar('compromiso_correo', 'memoria', p_corrida, filas));

  -- 3. promesa_pago_vencida: email_pending_actions tipo promesa_pago, abiertas, deadline pasado.
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', 'promesa_pago_vencida:epa:' || a.id,
      'company_id', a.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', situacion_user_de_buzon(a.account),
      'valor', (current_date - a.deadline), 'valor_texto', left(a.descripcion, 200), 'vence', a.deadline,
      'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', a.thread_id, 'nombre', left(coalesce(t.subject, ''), 120), 'fecha', t.last_activity::date)),
      'payload', jsonb_build_object('ultimo_correo', t.last_activity, 'epa_id', a.id)
    ) AS x
    FROM email_pending_actions a
    JOIN companies c ON c.id = a.company_id AND c.odoo_partner_id IS NOT NULL
    LEFT JOIN threads t ON t.id = a.thread_id
    WHERE a.tipo = 'promesa_pago' AND a.status = 'open' AND a.deadline < current_date
      AND NOT EXISTS (SELECT 1 FROM senales z WHERE z.clave = 'promesa_pago_vencida:epa:' || a.id AND z.resuelta_en IS NULL AND z.calidad = 'vencida_memoria')) q;
  out := out || jsonb_build_object('promesa_pago_vencida', senales_ingestar('promesa_pago_vencida', 'memoria', p_corrida, filas));

  -- 4/5. proveedor_esperando (nosotros debemos) y esperando_proveedor (ellos deben): conversaciones abiertas con proveedor.
  FOR u IN SELECT value FROM jsonb_array_elements('[{"senal":"proveedor_esperando","esperando":"nosotros"},{"senal":"esperando_proveedor","esperando":"ellos"}]'::jsonb) LOOP
    SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
      SELECT jsonb_build_object(
        'clave', (u->>'senal') || ':conv:' || md5(coalesce(s.conv_key, s.thread_id::text)),
        'company_id', s.company_id, 'odoo_partner_id', c.odoo_partner_id,
        'responsable_odoo_user_id', coalesce(situacion_user_de_buzon(s.account), situacion_responsable_conv(s.conv_key)),
        'valor', extract(day FROM now() - t.last_activity)::int, 'valor_texto', left(coalesce(s.tema, t.subject, ''), 200),
        'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', s.thread_id, 'nombre', left(coalesce(s.tema, t.subject, ''), 120), 'fecha', t.last_activity::date)),
        'payload', jsonb_build_object('ultimo_correo', t.last_activity, 'conv_key', s.conv_key, 'pendientes', s.pendientes)
      ) AS x
      FROM memoria_thread_summaries s
      JOIN threads t ON t.id = s.thread_id
      JOIN companies c ON c.id = s.company_id AND c.odoo_partner_id IS NOT NULL AND c.is_supplier AND NOT c.is_customer
      WHERE s.estado = 'abierto' AND s.esperando_a = (u->>'esperando') AND t.last_activity > now() - interval '90 days') q;
    out := out || jsonb_build_object(u->>'senal', senales_ingestar(u->>'senal', 'memoria', p_corrida, filas));
  END LOOP;

  -- 6. reclamacion_cliente: conversación abierta con cliente en tono tenso.
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', 'reclamacion_cliente:conv:' || md5(coalesce(s.conv_key, s.thread_id::text)),
      'company_id', s.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', coalesce(situacion_user_de_buzon(s.account), situacion_responsable_conv(s.conv_key)),
      'valor', extract(day FROM now() - t.last_activity)::int, 'valor_texto', left(coalesce(s.tema, t.subject, ''), 200),
      'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', s.thread_id, 'nombre', left(coalesce(s.tema, t.subject, ''), 120), 'fecha', t.last_activity::date)),
      'payload', jsonb_build_object('ultimo_correo', t.last_activity, 'conv_key', s.conv_key, 'esperando_a', s.esperando_a)
    ) AS x
    FROM memoria_thread_summaries s
    JOIN threads t ON t.id = s.thread_id
    JOIN companies c ON c.id = s.company_id AND c.odoo_partner_id IS NOT NULL AND c.is_customer
    WHERE s.estado = 'abierto' AND s.tono = 'tenso' AND t.last_activity > now() - interval '90 days') q;
  out := out || jsonb_build_object('reclamacion_cliente', senales_ingestar('reclamacion_cliente', 'memoria', p_corrida, filas));

  -- 7. oportunidad_demanda: una por señal de demanda reciente.
  SELECT coalesce((umbrales->>'dias')::int, 60) INTO v_dias FROM senales_config WHERE senal = 'oportunidad_demanda';
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', 'oportunidad_demanda:demand:' || d.id,
      'company_id', d.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', situacion_responsable_empresa(d.company_id),
      'valor', d.qty, 'valor_texto', left(concat_ws(' ', d.qty, d.uom, coalesce(d.product_ref, d.product_desc), d.period_label), 200),
      'vence', d.demand_date,
      'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', d.thread_id, 'nombre', left(coalesce(t.subject, ''), 120), 'fecha', d.detected_at::date)),
      'payload', jsonb_build_object('ultimo_correo', d.detected_at, 'product_ref', d.product_ref, 'product_desc', d.product_desc)
    ) AS x
    FROM customer_demand_signals d
    JOIN companies c ON c.id = d.company_id AND c.odoo_partner_id IS NOT NULL
    LEFT JOIN threads t ON t.id = d.thread_id
    WHERE d.detected_at > now() - make_interval(days => v_dias)) q;
  out := out || jsonb_build_object('oportunidad_demanda', senales_ingestar('oportunidad_demanda', 'memoria', p_corrida, filas));

  -- 8. cliente_callado: sin correo entrante en > factor × su mediana de días entre correos (12 meses).
  --    (El spec dice "memoria + facturas"; la parte de pedidos queda para cuando la señal se cruce con Odoo.)
  --    Es la única cara (barre 12 meses de emails): cada_horas=24 en config, y aquí se respeta el turno.
  IF senales_en_turno('cliente_callado') THEN
  SELECT coalesce((umbrales->>'factor')::numeric, 2), coalesce((umbrales->>'min_dias')::int, 14), coalesce((umbrales->>'min_correos')::int, 6)
    INTO v_factor, v_min_dias, v_min_correos FROM senales_config WHERE senal = 'cliente_callado';
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    WITH dias AS (
      SELECT e.company_id, e.email_date::date AS d
      FROM emails e
      WHERE e.email_date > now() - interval '12 months' AND e.sender_type = 'external' AND e.company_id IS NOT NULL
      GROUP BY 1, 2
    ), gaps AS (
      SELECT company_id, d, d - lag(d) OVER (PARTITION BY company_id ORDER BY d) AS gap FROM dias
    ), stats AS (
      SELECT company_id, count(*) AS n, max(d) AS ultimo,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY gap) AS mediana
      FROM gaps GROUP BY company_id HAVING count(*) >= v_min_correos
    )
    SELECT jsonb_build_object(
      'clave', 'cliente_callado:company:' || s.company_id,
      'company_id', s.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', situacion_responsable_empresa(s.company_id),
      'valor', (current_date - s.ultimo),
      'valor_texto', (current_date - s.ultimo) || ' días sin correo; escribía cada ' || round(s.mediana) || ' días',
      'payload', jsonb_build_object('ultimo_correo', s.ultimo, 'mediana_dias', round(s.mediana, 1), 'correos_12m', s.n)
    ) AS x
    FROM stats s
    JOIN companies c ON c.id = s.company_id AND c.odoo_partner_id IS NOT NULL AND c.is_customer
    WHERE (current_date - s.ultimo) > greatest(v_factor * s.mediana, v_min_dias)) q;
  out := out || jsonb_build_object('cliente_callado', senales_ingestar('cliente_callado', 'memoria', p_corrida, filas));
  ELSE
    out := out || jsonb_build_object('cliente_callado', jsonb_build_object('ok', true, 'saltada', 'fuera de turno'));
  END IF;

  -- 9. pendiente_rh_correo: pendientes (quien=nosotros) en buzones de RH (buzon_personas.area='rh' o nombre del buzón).
  SELECT coalesce(jsonb_agg(x), '[]') INTO filas FROM (
    SELECT jsonb_build_object(
      'clave', 'pendiente_rh_correo:conv:' || md5(coalesce(s.conv_key, s.thread_id::text) || '|' || lower(trim(coalesce(p.value->>'que', '')))),
      'company_id', s.company_id, 'odoo_partner_id', c.odoo_partner_id,
      'responsable_odoo_user_id', situacion_user_de_buzon(s.account),
      'valor', extract(day FROM now() - t.last_activity)::int, 'valor_texto', left(p.value->>'que', 200), 'vence', situacion_fecha(p.value->>'vence'),
      'documentos', jsonb_build_array(jsonb_build_object('modelo', 'thread', 'id', s.thread_id, 'nombre', left(coalesce(s.tema, t.subject, ''), 120), 'fecha', t.last_activity::date)),
      'payload', jsonb_build_object('que', left(p.value->>'que', 300), 'ultimo_correo', t.last_activity, 'buzon', s.account)
    ) AS x
    FROM memoria_thread_summaries s
    JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(s.pendientes) = 'array' THEN s.pendientes ELSE '[]'::jsonb END) p ON true
    JOIN threads t ON t.id = s.thread_id
    LEFT JOIN companies c ON c.id = s.company_id
    WHERE s.estado = 'abierto' AND p.value->>'quien' = 'nosotros' AND coalesce(p.value->>'que', '') <> ''
      AND t.last_activity > now() - interval '90 days'
      AND (s.account ~* '^(rh|rrhh|recursoshumanos|nomina|capitalhumano)' OR EXISTS (SELECT 1 FROM buzon_personas b WHERE b.buzon = lower(s.account) AND b.area = 'rh'))) q;
  out := out || jsonb_build_object('pendiente_rh_correo', senales_ingestar('pendiente_rh_correo', 'memoria', p_corrida, filas));

  RETURN out;
END $$;
REVOKE ALL ON FUNCTION public.senales_memoria(uuid) FROM public, anon, authenticated;
COMMENT ON FUNCTION public.senales_memoria(uuid) IS 'Señales fuente=memoria (spec §4): SQL puro sobre threads, resúmenes, pendientes y demanda; cada una pasa por senales_ingestar con la lista completa. Devuelve el resultado de cada lote.';

COMMIT;
```

- [ ] **Step 4: Aplicar, correr la prueba (`PRUEBA_OK`) y medir**

Además de la prueba, corre `explain analyze select senales_memoria()` una vez (en una transacción que hagas ROLLBACK, o acepta que inserta señales reales: son las que queremos). Expected: < 10 s. `cliente_callado` (barre 12 meses de `emails`) solo corre en su turno de 24 h (`senales_en_turno`); las demás corren cada ciclo.

- [ ] **Step 5: Commit** — "Situación: señales de la memoria en SQL (9 señales por senales_ingestar)".

### Task 1.4: Calidad, situaciones y ciclo (`senales_actualizar`, `situacion_guardar`, `situacion_ciclo`, `situacion_candidatas`)

**Files:**
- Create: `supabase/migrations/20260919d_situacion_calidad_y_situaciones.sql`
- Create: `supabase/tests/situacion/03_situaciones.sql`

Decisión de implementación sobre §3.3 ("peor calidad entre sus señales"): como zombis y datos malos se apartan en situaciones de higiene, entre las calidades restantes (`viva`, `antigua`, `vencida_memoria`) la situación toma la **mejor**: una señal viva basta para que se razone. Si el CEO prefiere lo literal del spec, es un `DESC` en `situacion_guardar`.

Reglas de calidad (spec §5.2), evaluadas en SQL sobre `senales_config.reglas_calidad` y `senales.payload`, en este orden de precedencia: `ignorada` (regla del CEO) > `dato_malo` (`payload.dato_malo` o RFC en `umbrales.rfc_relacionados`) > `zombie` (`payload.fecha_base` más vieja que `zombie_dias`) > `vencida_memoria` (fuente memoria, `vence` pasado y `payload.ultimo_correo` con más de `vencida_dias`) > `antigua` (abierta más de `antigua_dias` sin cambio de valor ni correo) > `viva`.

- [ ] **Step 1: Prueba en seco**

```sql
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); sid bigint; s record;
BEGIN
  INSERT INTO senales_config (senal, titulo, area, tipo, fuente, agrupar_por, agregar, umbrales, reglas_calidad)
  VALUES ('_prueba', 'Prueba', 'finanzas', 'credito', 'odoo', 'contraparte', 'suma',
          '{"rfc_relacionados":["RELA010101AAA"]}', '{"antigua_dias":30,"zombie_dias":90}');
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":100,"documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:1b","odoo_partner_id":1,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":5,"payload":{"rfc":"RELA010101AAA"}},
    {"clave":"_prueba:partner:3","odoo_partner_id":3,"valor":7,"payload":{"fecha_base":"2025-01-01"}},
    {"clave":"_prueba:partner:4","odoo_partner_id":4,"valor":1,"payload":{"dato_malo":"costo 0"}}
  ]'::jsonb);
  INSERT INTO situacion_reglas (alcance, clave_alcance, accion, motivo) VALUES ('contraparte', 'partner:4', 'ignorar', 'prueba');

  -- Calidad.
  r := senales_actualizar();
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:2' AND resuelta_en IS NULL) = 'dato_malo', 'RFC relacionado → dato_malo';
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:3' AND resuelta_en IS NULL) = 'zombie', 'fecha_base vieja → zombie';
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:4' AND resuelta_en IS NULL) = 'ignorada', 'regla del CEO gana a dato_malo';
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:1' AND resuelta_en IS NULL) = 'viva', 'viva';
  UPDATE senales SET primera_vista = now() - interval '40 days', valor_cambio_en = now() - interval '40 days' WHERE clave = '_prueba:partner:1b';
  r := senales_actualizar();
  ASSERT (SELECT calidad FROM senales WHERE clave = '_prueba:partner:1b' AND resuelta_en IS NULL) = 'antigua', 'antigua';

  -- Situaciones: partner 1 (dos señales, una viva y una antigua) = una situación; zombie y dato_malo = higiene; ignorada no aparece.
  r := situacion_guardar(c);
  ASSERT (r->>'nuevas')::int = 3, 'tres situaciones nuevas (partner 1, higiene:zombie, higiene:dato_malo): ' || r;
  SELECT * INTO s FROM situaciones WHERE clave = '_prueba|partner:1';
  ASSERT s.estado = 'abierta' AND s.n_senales = 2 AND s.valor = 150 AND s.calidad = 'viva' AND s.titulo LIKE 'Prueba · %', 'situación partner 1 (una señal viva basta para que la situación sea viva): ' || row_to_json(s);
  ASSERT jsonb_array_length(s.documentos) = 2 AND (s.evidencia->'senales') @> '["_prueba:partner:1"]', 'documentos y evidencia';
  ASSERT (SELECT tipo FROM situaciones WHERE clave = '_prueba|higiene:zombie') = 'higiene', 'higiene zombie';
  ASSERT NOT EXISTS (SELECT 1 FROM situaciones WHERE clave = '_prueba|partner:4'), 'ignorada no crea situación';
  ASSERT (r->>'ignoradas')::int = 1, 'ignoradas contadas';

  -- Empeora: sube el valor → estado empeoro, version 2, historia.
  PERFORM senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[
    {"clave":"_prueba:partner:1","odoo_partner_id":1,"valor":300,"documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:1b","odoo_partner_id":1,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":5,"payload":{"rfc":"RELA010101AAA"}},
    {"clave":"_prueba:partner:3","odoo_partner_id":3,"valor":7,"payload":{"fecha_base":"2025-01-01"}}
  ]'::jsonb);
  PERFORM senales_actualizar();
  r := situacion_guardar(gen_random_uuid());
  SELECT * INTO s FROM situaciones WHERE clave = '_prueba|partner:1';
  ASSERT s.estado = 'empeoro' AND s.version = 2 AND s.ultimo_cambio LIKE 'empeoró%', 'empeoró: ' || row_to_json(s);
  ASSERT jsonb_array_length(s.historia) = 2, 'historia con dos eventos';
  -- Candidata: nueva/empeorada sin redacción vigente.
  ASSERT EXISTS (SELECT 1 FROM situacion_candidatas(40) WHERE id = s.id), 'es candidata';
  -- Lote sin partner 1 → sus señales se resuelven → situación resuelta, y ya no es candidata.
  PERFORM senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[{"clave":"_prueba:partner:2","odoo_partner_id":2,"valor":5,"payload":{"rfc":"RELA010101AAA"}}]'::jsonb);
  PERFORM senales_actualizar();
  r := situacion_guardar(gen_random_uuid());
  SELECT * INTO s FROM situaciones WHERE clave = '_prueba|partner:1';
  ASSERT s.estado = 'resuelta' AND s.resuelta_en IS NOT NULL, 'resuelta por evidencia: ' || s.estado;
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_candidatas(40) WHERE id = s.id), 'resuelta no es candidata';
  -- Sin datos: si el último lote bueno de la señal es viejo, sus situaciones no se tocan.
  UPDATE senales_lotes SET recibido_en = now() - interval '5 hours' WHERE senal = '_prueba';
  PERFORM senales_ingestar('_prueba', 'odoo', gen_random_uuid(), '[]'::jsonb);  -- lote vacío pero…
  UPDATE senales_lotes SET ok = false WHERE senal = '_prueba' AND n_claves = 0;  -- …marcado malo: no cuenta
  UPDATE senales SET resuelta_en = NULL WHERE clave = '_prueba:partner:2';       -- reabrimos a mano para la prueba
  r := situacion_guardar(gen_random_uuid());
  ASSERT (r->'sin_datos') @> '["_prueba"]', 'señal sin datos reportada: ' || r;
  ASSERT (SELECT estado FROM situaciones WHERE clave = '_prueba|higiene:dato_malo') <> 'resuelta', 'sin datos no resuelve';

  -- Ciclo completo y cierre de corrida.
  r := situacion_ciclo(gen_random_uuid(), 'prueba');
  ASSERT (r->>'corrida_id') IS NOT NULL AND (SELECT sql_lista_en IS NOT NULL FROM situacion_corridas WHERE id = (r->>'corrida_id')::bigint), 'ciclo registra corrida';
  PERFORM situacion_corrida_cerrar((r->>'corrida_id')::bigint, '{"n_candidatas":3,"n_redactadas":2,"tokens_in":100,"tokens_out":10,"modelo":"x"}'::jsonb);
  ASSERT (SELECT terminada_en IS NOT NULL AND n_redactadas = 2 FROM situacion_corridas WHERE id = (r->>'corrida_id')::bigint), 'corrida cerrada';
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correrla → falla con `function senales_actualizar() does not exist`**

- [ ] **Step 3: Escribir la migración**

```sql
-- 2026-09-19d — Situación plan A: calidad (§5), situaciones determinísticas (§6.2) y ciclo.
BEGIN;

-- Orden de "peor calidad" para agregar en la situación.
CREATE OR REPLACE FUNCTION public.situacion_calidad_rango(p text)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p WHEN 'viva' THEN 0 WHEN 'antigua' THEN 1 WHEN 'vencida_memoria' THEN 2 WHEN 'zombie' THEN 3 WHEN 'dato_malo' THEN 4 WHEN 'ignorada' THEN 5 ELSE 0 END
$$;

-- ¿Hay una regla del CEO vigente que cubra esta señal? (alcance senal / contraparte / documento / situacion)
CREATE OR REPLACE FUNCTION public.situacion_regla_aplica(p_accion text[], p_senal text, p_agrupador text, p_company_id bigint, p_partner integer, p_documentos jsonb)
RETURNS text LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT 'regla #' || r.id || ' (' || r.alcance || ' ' || r.clave_alcance || '): ' || coalesce(r.motivo, r.accion)
  FROM situacion_reglas r
  WHERE r.accion = ANY (p_accion) AND (r.vigente_hasta IS NULL OR r.vigente_hasta > now())
    AND ((r.alcance = 'senal' AND r.clave_alcance = p_senal)
      OR (r.alcance = 'contraparte' AND r.clave_alcance IN ('company:' || p_company_id, 'partner:' || p_partner))
      OR (r.alcance = 'situacion' AND r.clave_alcance = p_senal || '|' || p_agrupador)
      OR (r.alcance = 'documento' AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_documentos) d
            WHERE (d->>'modelo') || ':' || (d->>'id') = r.clave_alcance)))
  ORDER BY r.creada_en DESC LIMIT 1
$$;

CREATE OR REPLACE FUNCTION public.senales_actualizar()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE out jsonb;
BEGIN
  -- Un CTE calcula cada condición una sola vez; los dos CASE (etiqueta y motivo) usan las mismas banderas.
  WITH base AS (
    SELECT s.id, s.fuente, s.vence, s.primera_vista, s.valor_cambio_en, s.payload,
      situacion_regla_aplica(ARRAY['ignorar','no_es_problema'], s.senal, s.agrupador, s.company_id, s.odoo_partner_id, s.documentos) AS regla,
      coalesce(s.payload->>'rfc', comp.rfc) AS rfc,
      (c.umbrales ? 'rfc_relacionados') AND (c.umbrales->'rfc_relacionados') ? coalesce(s.payload->>'rfc', comp.rfc, '') AS rfc_relacionado,
      coalesce((c.reglas_calidad->>'antigua_dias')::int, 30) AS antigua_dias,
      (c.reglas_calidad->>'zombie_dias')::int AS zombie_dias,
      coalesce((c.reglas_calidad->>'vencida_dias')::int, 21) AS vencida_dias,
      situacion_fecha(s.payload->>'fecha_base') AS fecha_base,
      coalesce(nullif(s.payload->>'ultimo_correo', '')::timestamptz, s.primera_vista) AS ultimo_correo
    FROM senales s
    JOIN senales_config c ON c.senal = s.senal
    LEFT JOIN companies comp ON comp.id = s.company_id
    WHERE s.resuelta_en IS NULL
  ), calc AS (
    SELECT id, regla, rfc, vence, antigua_dias, zombie_dias, vencida_dias, fecha_base,
      regla IS NOT NULL                                                                              AS es_ignorada,
      coalesce(payload->>'dato_malo', '') <> ''                                                      AS es_dato_malo_fuente,
      rfc_relacionado                                                                                AS es_relacionada,
      zombie_dias IS NOT NULL AND fecha_base IS NOT NULL AND fecha_base < current_date - zombie_dias AS es_zombie,
      fuente = 'memoria' AND vence IS NOT NULL AND vence < current_date
        AND ultimo_correo < now() - make_interval(days => vencida_dias)                              AS es_vencida,
      primera_vista < now() - make_interval(days => antigua_dias)
        AND valor_cambio_en < now() - make_interval(days => antigua_dias)
        AND ultimo_correo < now() - make_interval(days => antigua_dias)                              AS es_antigua,
      payload->>'dato_malo'                                                                          AS dato_malo_fuente
    FROM base
  ), etiqueta AS (
    SELECT id,
      CASE WHEN es_ignorada THEN 'ignorada' WHEN es_dato_malo_fuente OR es_relacionada THEN 'dato_malo'
           WHEN es_zombie THEN 'zombie' WHEN es_vencida THEN 'vencida_memoria' WHEN es_antigua THEN 'antigua' ELSE 'viva' END AS calidad,
      CASE WHEN es_ignorada THEN regla
           WHEN es_dato_malo_fuente THEN dato_malo_fuente
           WHEN es_relacionada THEN 'parte relacionada (RFC ' || rfc || ')'
           WHEN es_zombie THEN 'fecha base ' || fecha_base || ' (> ' || zombie_dias || ' días)'
           WHEN es_vencida THEN 'venció el ' || vence || ' sin correo nuevo en ' || vencida_dias || ' días'
           WHEN es_antigua THEN 'sin cambio de valor ni correo en ' || antigua_dias || ' días'
           ELSE NULL END AS motivo
    FROM calc
  )
  UPDATE senales s SET calidad = e.calidad, calidad_motivo = e.motivo
  FROM etiqueta e
  WHERE s.id = e.id AND (s.calidad IS DISTINCT FROM e.calidad OR s.calidad_motivo IS DISTINCT FROM e.motivo);

  SELECT coalesce(jsonb_object_agg(calidad, n), '{}'::jsonb) INTO out
  FROM (SELECT calidad, count(*) AS n FROM senales WHERE resuelta_en IS NULL GROUP BY calidad) z;
  RETURN out;
END $$;
REVOKE ALL ON FUNCTION public.senales_actualizar() FROM public, anon, authenticated;
COMMENT ON FUNCTION public.senales_actualizar() IS 'Recalcula calidad y calidad_motivo de las señales abiertas (spec §5.2): ignorada > dato_malo > zombie > vencida_memoria > antigua > viva. Devuelve conteo por calidad.';

-- Nombre legible del agrupador para el título provisional.
CREATE OR REPLACE FUNCTION public.situacion_nombre_agrupador(p_agrupador text, p_company_id bigint, p_partner integer, p_documentos jsonb)
RETURNS text LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
  SELECT CASE
    WHEN p_agrupador LIKE 'higiene:%' THEN CASE split_part(p_agrupador, ':', 2) WHEN 'zombie' THEN 'zombis' ELSE 'datos malos' END
    WHEN p_agrupador LIKE 'company:%' OR p_agrupador LIKE 'partner:%' THEN
      coalesce((SELECT name FROM companies WHERE id = p_company_id), (SELECT name FROM companies WHERE odoo_partner_id = p_partner ORDER BY id LIMIT 1), 'contraparte ' || p_agrupador)
    WHEN p_agrupador LIKE 'user:%' THEN coalesce((SELECT name FROM odoo_users WHERE odoo_user_id = split_part(p_agrupador, ':', 2)::int LIMIT 1), 'sin responsable')
    WHEN p_agrupador LIKE 'doc:%' THEN coalesce(p_documentos->0->>'nombre', p_agrupador)
    WHEN p_agrupador LIKE 'grupo:%' THEN substr(p_agrupador, 7)
    WHEN p_agrupador LIKE 'situacion:%' THEN 'situación #' || substr(p_agrupador, 11)
    ELSE 'general' END
$$;

CREATE OR REPLACE FUNCTION public.situacion_guardar(p_corrida uuid DEFAULT gen_random_uuid())
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  g record; s record; n_nuevas int := 0; n_act int := 0; n_res int := 0; n_ign int := 0; n_sin int := 0;
  v_estado text; v_cambio text; v_evento jsonb; v_sin_datos text[];
BEGIN
  -- Señales cuyo último lote bueno es más viejo que sin_datos_horas: no se tocan sus situaciones.
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
         -- Calidad de la situación: la MEJOR de sus señales (zombis y datos malos ya se apartaron en higiene;
         -- entre viva/antigua/vencida_memoria, una señal viva basta para que la situación se razone).
         (array_agg(s.calidad ORDER BY situacion_calidad_rango(s.calidad)))[1] AS calidad,
         jsonb_path_query_array(jsonb_agg(s.documentos), '$[*][*]') AS docs_flat,   -- todos los documentos, aplanados
         NULL::jsonb AS documentos, NULL::jsonb AS evidencia,
         jsonb_agg(s.clave ORDER BY s.clave) AS claves, max(s.episodio) AS episodio_max,
         string_agg(DISTINCT s.valor_texto, '; ') FILTER (WHERE s.valor_texto IS NOT NULL) AS valor_texto
  FROM senales s JOIN senales_config c ON c.senal = s.senal
  WHERE s.resuelta_en IS NULL AND s.calidad <> 'ignorada' AND NOT (s.senal = ANY (v_sin_datos))
  GROUP BY s.senal, c.area, c.tipo, c.titulo, c.severidad_base, c.agregar,
           CASE WHEN s.calidad IN ('zombie', 'dato_malo') THEN 'higiene:' || s.calidad ELSE s.agrupador END;
  -- Segundo paso (los agregados no pueden ir dentro de subconsultas): primeros 40 documentos y evidencia.
  UPDATE _grupos g SET
    documentos = (SELECT coalesce(jsonb_agg(d), '[]') FROM (SELECT d FROM jsonb_array_elements(g.docs_flat) d LIMIT 40) q),
    evidencia = jsonb_build_object(
      'senales', g.claves,
      'threads', coalesce((SELECT jsonb_agg(DISTINCT (d->>'id')::bigint) FROM jsonb_array_elements(g.docs_flat) d WHERE d->>'modelo' = 'thread'), '[]'::jsonb),
      'episodio_max', g.episodio_max);

  FOR g IN SELECT * FROM _grupos LOOP
    SELECT * INTO s FROM situaciones WHERE clave = g.senal || '|' || g.agrupador;
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
      IF s.estado IN ('resuelta', 'descartada') THEN
        IF s.estado = 'descartada' THEN CONTINUE; END IF;  -- el CEO la descartó: no reabrir (una regla la esconde; aquí solo se respeta)
        v_estado := 'abierta'; v_cambio := 'reapareció: ' || g.n || ' señal(es), valor ' || coalesce(g.valor::text, '-');
      ELSIF g.n > s.n_senales OR (g.valor IS NOT NULL AND s.valor IS NOT NULL AND g.valor > s.valor * 1.05) THEN
        v_estado := 'empeoro'; v_cambio := format('empeoró: %s → %s documentos, valor %s → %s', s.n_senales, g.n, coalesce(s.valor::text, '-'), coalesce(g.valor::text, '-'));
      ELSIF g.n < s.n_senales OR (g.valor IS NOT NULL AND s.valor IS NOT NULL AND g.valor < s.valor * 0.95) THEN
        v_estado := 'mejoro'; v_cambio := format('mejoró: %s → %s documentos, valor %s → %s', s.n_senales, g.n, coalesce(s.valor::text, '-'), coalesce(g.valor::text, '-'));
      END IF;
      UPDATE situaciones SET
        documentos = g.documentos, evidencia = s.evidencia || g.evidencia, calidad = g.calidad, n_senales = g.n, valor = g.valor,
        valor_texto = left(g.valor_texto, 600), vence = g.vence, company_id = coalesce(g.company_id, s.company_id),
        odoo_partner_id = coalesce(g.odoo_partner_id, s.odoo_partner_id),
        responsable_sugerido_user_id = coalesce(s.responsable_sugerido_user_id, g.responsable),
        estado = coalesce(v_estado, CASE WHEN s.estado = 'delegada' THEN 'delegada' ELSE s.estado END),
        resuelta_en = CASE WHEN v_estado = 'abierta' THEN NULL ELSE s.resuelta_en END,
        version = CASE WHEN v_estado IS NOT NULL THEN s.version + 1 ELSE s.version END,
        ultimo_cambio = coalesce(v_cambio, s.ultimo_cambio),
        ultimo_cambio_en = CASE WHEN v_estado IS NOT NULL THEN now() ELSE s.ultimo_cambio_en END,
        historia = CASE WHEN v_estado IS NOT NULL THEN s.historia || jsonb_build_object('fecha', now(), 'evento', v_estado, 'detalle', v_cambio, 'corrida', p_corrida) ELSE s.historia END,
        updated_at = now()
      WHERE id = s.id;
      IF v_estado IS NOT NULL THEN n_act := n_act + 1; END IF;
    END IF;
  END LOOP;

  -- Abiertas cuyo grupo desapareció (y cuya señal sí tuvo lote bueno): resueltas por evidencia.
  UPDATE situaciones s SET
    estado = 'resuelta', resuelta_en = now(), version = s.version + 1,
    ultimo_cambio = 'resuelta por evidencia: la señal ' || s.senal || ' desapareció', ultimo_cambio_en = now(),
    historia = s.historia || jsonb_build_object('fecha', now(), 'evento', 'resuelta', 'detalle', 'por evidencia: ' || s.senal || ' desapareció', 'corrida', p_corrida),
    updated_at = now()
  WHERE s.estado NOT IN ('resuelta', 'descartada')
    AND NOT (s.senal = ANY (v_sin_datos))
    AND NOT EXISTS (SELECT 1 FROM _grupos g WHERE g.senal || '|' || g.agrupador = s.clave);
  GET DIAGNOSTICS n_res = ROW_COUNT;
  DROP TABLE IF EXISTS _grupos;

  RETURN jsonb_build_object('nuevas', n_nuevas, 'actualizadas', n_act, 'resueltas', n_res, 'ignoradas', n_ign,
                            'sin_datos', to_jsonb(v_sin_datos), 'corrida', p_corrida);
END $$;
REVOKE ALL ON FUNCTION public.situacion_guardar(uuid) FROM public, anon, authenticated;
COMMENT ON FUNCTION public.situacion_guardar(uuid) IS 'Agrupa señales abiertas por senal|agrupador (zombis y datos malos → higiene:<calidad>) y crea/actualiza/resuelve situaciones sin IA (spec §6.2). Señales sin lote bueno reciente (sin_datos) no se tocan.';

-- Candidatas a redacción: nuevas/empeoradas/mejoradas sin redacción vigente, vivas, no de higiene.
CREATE OR REPLACE FUNCTION public.situacion_candidatas(p_limit integer DEFAULT 40)
RETURNS TABLE (id bigint, clave text, titulo text, estado text, version integer, ia_version integer, severidad smallint, calidad text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT id, clave, titulo, estado, version, ia_version, severidad, calidad
  FROM situaciones
  WHERE estado IN ('abierta', 'empeoro', 'mejoro') AND fusionada_en IS NULL AND calidad = 'viva' AND tipo <> 'higiene'
    AND ia_version < version
  ORDER BY (ia_version = 0) DESC, severidad DESC, ultimo_cambio_en DESC
  LIMIT greatest(p_limit, 1)
$$;
REVOKE ALL ON FUNCTION public.situacion_candidatas(integer) FROM public, anon, authenticated;

-- Ciclo SQL completo (lo llama el bot al arrancar y también sirve a mano): memoria → calidad → situaciones.
CREATE OR REPLACE FUNCTION public.situacion_ciclo(p_corrida uuid DEFAULT gen_random_uuid(), p_origen text DEFAULT 'manual')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE v_id bigint; r_mem jsonb; r_cal jsonb; r_sit jsonb; n_sen int;
BEGIN
  INSERT INTO situacion_corridas (corrida, origen) VALUES (p_corrida, p_origen) RETURNING id INTO v_id;
  r_mem := senales_memoria(p_corrida);
  r_cal := senales_actualizar();
  r_sit := situacion_guardar(p_corrida);
  SELECT count(*) INTO n_sen FROM senales WHERE resuelta_en IS NULL;
  UPDATE situacion_corridas SET sql_lista_en = now(), n_senales = n_sen,
    n_nuevas = (r_sit->>'nuevas')::int, n_actualizadas = (r_sit->>'actualizadas')::int, n_resueltas = (r_sit->>'resueltas')::int,
    n_ignoradas = (r_sit->>'ignoradas')::int,
    detalle = jsonb_build_object('memoria', r_mem, 'calidad', r_cal, 'situaciones', r_sit)
  WHERE id = v_id;
  RETURN jsonb_build_object('corrida_id', v_id, 'corrida', p_corrida, 'senales', n_sen, 'calidad', r_cal, 'situaciones', r_sit);
END $$;
REVOKE ALL ON FUNCTION public.situacion_ciclo(uuid, text) FROM public, anon, authenticated;

CREATE OR REPLACE FUNCTION public.situacion_corrida_cerrar(p_id bigint, p jsonb)
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $$
  UPDATE situacion_corridas SET terminada_en = now(),
    n_candidatas = coalesce((p->>'n_candidatas')::int, n_candidatas), n_redactadas = coalesce((p->>'n_redactadas')::int, n_redactadas),
    n_fusiones = coalesce((p->>'n_fusiones')::int, n_fusiones), tokens_in = coalesce((p->>'tokens_in')::int, tokens_in),
    tokens_out = coalesce((p->>'tokens_out')::int, tokens_out), modelo = coalesce(p->>'modelo', modelo),
    errores = coalesce(p->'errores', errores)
  WHERE id = p_id
$$;
REVOKE ALL ON FUNCTION public.situacion_corrida_cerrar(bigint, jsonb) FROM public, anon, authenticated;

COMMIT;
```

- [ ] **Step 4: Aplicar y correr la prueba (`PRUEBA_OK`)**

Después, con datos reales: `select situacion_ciclo();` y luego `select clave, estado, calidad, n_senales, titulo from situaciones order by severidad desc limit 20;`. Expected: situaciones `cliente_sin_respuesta|company:<id>` y `compromiso_correo|company:<id>` con título "Cliente sin respuesta · <empresa>" (criterio de aceptación del paso 1, spec §8).

- [ ] **Step 5: Commit** — "Situación: calidad, agrupación determinística en situaciones y ciclo SQL".

### Task 1.5: Lectura por MCP y escritura de la IA (`situacion_mapa`, `situacion_contexto`, `situacion_por_persona`, `situacion_higiene`, `situacion_salud`, `situacion_redactar`)

**Files:**
- Create: `supabase/migrations/20260919e_situacion_lectura.sql`
- Create: `supabase/tests/situacion/04_lectura.sql`

Toda fila que sale de aquí trae `calidad`, `dias_abierta`, `dias_sin_cambio` y `ultimo_cambio` (spec §5.4).

- [ ] **Step 1: Prueba en seco**

```sql
DO $t$
DECLARE r jsonb; c uuid := gen_random_uuid(); sid bigint; sid2 bigint; m record;
BEGIN
  INSERT INTO senales_config (senal, titulo, area, tipo, fuente, agrupar_por, agregar) VALUES ('_prueba', 'Prueba', 'finanzas', 'credito', 'odoo', 'contraparte', 'suma');
  PERFORM senales_ingestar('_prueba', 'odoo', c, '[
    {"clave":"_prueba:partner:1","odoo_partner_id":1,"responsable_odoo_user_id":2,"valor":100,"valor_texto":"cien","documentos":[{"modelo":"account.move","id":11,"nombre":"F/1"}]},
    {"clave":"_prueba:partner:1b","odoo_partner_id":1,"valor":50,"documentos":[{"modelo":"account.move","id":12,"nombre":"F/2"}]},
    {"clave":"_prueba:partner:3","odoo_partner_id":3,"valor":7,"payload":{"fecha_base":"2025-01-01"}}
  ]'::jsonb);
  UPDATE senales_config SET reglas_calidad = '{"zombie_dias":90,"limpieza":"borrar"}' WHERE senal = '_prueba';
  PERFORM senales_actualizar(); PERFORM situacion_guardar(c);
  SELECT id INTO sid FROM situaciones WHERE clave = '_prueba|partner:1';

  -- mapa: la situación viva sale; la de higiene no (calidad zombie) salvo p_calidad = NULL.
  SELECT * INTO m FROM situacion_mapa('finanzas') WHERE id = sid;
  ASSERT m.id IS NOT NULL AND m.dias_abierta = 0 AND m.calidad = 'viva' AND m.ultimo_cambio LIKE 'creada%', 'mapa: ' || row_to_json(m);
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_mapa('finanzas') WHERE senal = '_prueba' AND calidad = 'zombie'), 'zombie fuera del mapa por defecto';
  ASSERT EXISTS (SELECT 1 FROM situacion_mapa('finanzas', NULL) WHERE senal = '_prueba' AND calidad = 'zombie'), 'p_calidad NULL trae todas';
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_mapa('comercial') WHERE senal = '_prueba'), 'filtro por área';

  -- contexto: señales, documentos, contraparte, hermanas, reglas, historia.
  r := situacion_contexto(sid);
  ASSERT jsonb_array_length(r->'senales') = 2 AND (r->'situacion'->>'clave') = '_prueba|partner:1', 'contexto señales: ' || left(r::text, 300);
  ASSERT r ? 'contraparte' AND r ? 'hermanas' AND r ? 'posibles_duplicados' AND r ? 'reglas' AND r ? 'historia' AND r ? 'personas', 'llaves del contexto: ' || (SELECT string_agg(k, ',') FROM jsonb_object_keys(r) k);
  ASSERT (r->'senales'->0) ? 'calidad' AND (r->'senales'->0) ? 'episodio', 'señales con calidad y episodio';

  -- por persona.
  ASSERT EXISTS (SELECT 1 FROM situacion_por_persona(2) WHERE id = sid), 'por persona (responsable de la señal)';

  -- higiene y salud.
  r := situacion_higiene();
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(r->'clases') x WHERE x->>'senal' = '_prueba' AND x->>'calidad' = 'zombie' AND (x->>'n')::int = 1 AND x->>'limpieza' = 'borrar'), 'higiene: ' || r;
  r := situacion_salud();
  ASSERT r ? 'senales' AND r ? 'bot' AND r ? 'memoria' AND r ? 'odoo_push', 'salud llaves';
  ASSERT EXISTS (SELECT 1 FROM jsonb_array_elements(r->'senales') x WHERE x->>'senal' = '_prueba' AND (x->>'edad_h')::numeric < 1 AND NOT (x->>'sin_datos')::bool), 'salud señal reciente: ' || (r->'senales');

  -- redactar: escribe solo lo suyo, respeta la banda, sube ia_version, fusiona.
  INSERT INTO situaciones (clave, senal, agrupador, area, tipo, titulo, company_id, odoo_partner_id, severidad, estado)
  VALUES ('_prueba|partner:1x', '_prueba', 'partner:1x', 'finanzas', 'credito', 'Prueba · duplicada', (SELECT company_id FROM situaciones WHERE id = sid), 1, 2, 'abierta') RETURNING id INTO sid2;
  r := situacion_redactar(sid, '{"titulo":"Cartera de prueba","resumen":"Debe 150.","recomendacion":"Cobrar.","severidad":9,"responsable_sugerido_user_id":2,"responsable_motivo":"dueño","evento_historia":"redactada","duplicados":[{"id":' || sid2 || ',"decision":"fusionar","motivo":"misma cartera"}]}'::jsonb, 'modelo-x', NULL);
  ASSERT (r->>'ok')::bool AND (r->>'fusiones')::int = 1, 'redactar: ' || r;
  SELECT * INTO m FROM situaciones WHERE id = sid;
  ASSERT m.titulo = 'Cartera de prueba' AND m.severidad = 4 AND m.ia_version = m.version AND m.ia_modelo = 'modelo-x' AND m.estado = 'abierta' AND m.clave = '_prueba|partner:1', 'redactar escribe solo lo suyo y recorta severidad a la banda: ' || row_to_json(m);
  ASSERT (SELECT fusionada_en FROM situaciones WHERE id = sid2) = sid, 'fusionada_en';
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_mapa('finanzas') WHERE id = sid2), 'la fusionada sale del mapa';
  ASSERT NOT EXISTS (SELECT 1 FROM situacion_candidatas(40) WHERE id = sid), 'ya redactada no es candidata';
  RAISE EXCEPTION 'PRUEBA_OK';
END $t$;
```

- [ ] **Step 2: Correrla → falla con `function situacion_mapa(unknown) does not exist`**

- [ ] **Step 3: Escribir la migración**

```sql
-- 2026-09-19e — Situación plan A: lectura (spec §7.1) y escritura de la IA (§6.5).
BEGIN;

CREATE OR REPLACE FUNCTION public.situacion_mapa(p_area text DEFAULT NULL, p_calidad text DEFAULT 'viva', p_min_severidad integer DEFAULT 1, p_limit integer DEFAULT 100)
RETURNS TABLE (id bigint, area text, tipo text, senal text, titulo text, severidad smallint, estado text, calidad text,
               contraparte text, responsable text, responsable_user_id integer, dias_abierta integer, dias_sin_cambio integer,
               ultimo_cambio text, n_documentos integer, valor numeric, valor_texto text, vence date, redactada boolean, recomendacion text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT s.id, s.area, s.tipo, s.senal, s.titulo, s.severidad, s.estado, s.calidad,
         coalesce(c.name, situacion_nombre_agrupador(s.agrupador, s.company_id, s.odoo_partner_id, s.documentos)) AS contraparte,
         u.name AS responsable, s.responsable_sugerido_user_id,
         (current_date - s.desde) AS dias_abierta,
         extract(day FROM now() - s.ultimo_cambio_en)::int AS dias_sin_cambio,
         s.ultimo_cambio, jsonb_array_length(s.documentos) AS n_documentos, s.valor, s.valor_texto, s.vence,
         (s.ia_version >= s.version) AS redactada, s.recomendacion
  FROM situaciones s
  LEFT JOIN companies c ON c.id = s.company_id
  LEFT JOIN odoo_users u ON u.odoo_user_id = s.responsable_sugerido_user_id
  WHERE s.estado NOT IN ('resuelta', 'descartada') AND s.fusionada_en IS NULL
    AND (p_area IS NULL OR s.area = p_area)
    AND (p_calidad IS NULL OR s.calidad = p_calidad)
    AND s.severidad >= coalesce(p_min_severidad, 1)
  ORDER BY s.severidad DESC, s.ultimo_cambio_en DESC
  LIMIT greatest(coalesce(p_limit, 100), 1)
$$;
COMMENT ON FUNCTION public.situacion_mapa(text, text, integer, integer) IS 'El mapa (spec §7.1): situaciones abiertas por área, calidad (default viva; NULL = todas) y severidad mínima. Ejemplo MCP: select * from situacion_mapa(''finanzas'').';

CREATE OR REPLACE FUNCTION public.situacion_contexto(p_id bigint)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE s situaciones%ROWTYPE; brief jsonb; out jsonb;
BEGIN
  SELECT * INTO s FROM situaciones WHERE id = p_id;
  IF NOT FOUND THEN RETURN NULL; END IF;
  brief := CASE WHEN s.company_id IS NOT NULL THEN memoria_brief(p_company_id => s.company_id) ELSE NULL END;
  IF brief IS NOT NULL THEN
    brief := (brief - 'hilos') || jsonb_build_object('hilos', coalesce(jsonb_path_query_array(brief->'hilos', '$[0 to 4]'), '[]'));
  END IF;
  SELECT jsonb_build_object(
    'situacion', jsonb_build_object('id', s.id, 'clave', s.clave, 'senal', s.senal, 'area', s.area, 'tipo', s.tipo, 'titulo', s.titulo,
        'resumen', s.resumen, 'recomendacion', s.recomendacion, 'severidad', s.severidad, 'estado', s.estado, 'calidad', s.calidad,
        'desde', s.desde, 'vence', s.vence, 'dias_abierta', current_date - s.desde, 'dias_sin_cambio', extract(day FROM now() - s.ultimo_cambio_en)::int,
        'ultimo_cambio', s.ultimo_cambio, 'valor', s.valor, 'valor_texto', s.valor_texto, 'n_senales', s.n_senales, 'version', s.version, 'ia_version', s.ia_version,
        'responsable_sugerido_user_id', s.responsable_sugerido_user_id, 'responsable_motivo', s.responsable_motivo, 'delegacion', s.delegacion),
    'senal_config', (SELECT jsonb_build_object('titulo', titulo, 'descripcion', descripcion, 'severidad_base', severidad_base, 'severidad_max', severidad_max, 'umbrales', umbrales) FROM senales_config WHERE senal = s.senal),
    'senales', (SELECT coalesce(jsonb_agg(jsonb_build_object('clave', x.clave, 'episodio', x.episodio, 'calidad', x.calidad, 'calidad_motivo', x.calidad_motivo,
                  'valor', x.valor, 'valor_texto', x.valor_texto, 'vence', x.vence, 'primera_vista', x.primera_vista, 'valor_cambio_en', x.valor_cambio_en,
                  'documentos', x.documentos, 'responsable_odoo_user_id', x.responsable_odoo_user_id, 'payload', x.payload - 'pendientes') ORDER BY x.valor DESC NULLS LAST), '[]')
                FROM senales x WHERE x.resuelta_en IS NULL AND x.clave IN (SELECT jsonb_array_elements_text(s.evidencia->'senales'))),
    'documentos', s.documentos,
    'contraparte', CASE WHEN s.company_id IS NULL THEN NULL ELSE jsonb_build_object(
        'company_id', s.company_id, 'odoo_partner_id', s.odoo_partner_id,
        'empresa', (SELECT jsonb_build_object('name', name, 'rfc', rfc, 'is_customer', is_customer, 'is_supplier', is_supplier, 'domain', domain) FROM companies WHERE id = s.company_id),
        'memoria', brief) END,
    'conversaciones', (SELECT coalesce(jsonb_agg(jsonb_build_object('thread_id', m.thread_id, 'tema', m.tema, 'resumen', m.resumen, 'estado', m.estado, 'esperando_a', m.esperando_a, 'tono', m.tono, 'pendientes', m.pendientes, 'summarized_through', m.summarized_through)), '[]')
                       FROM (SELECT * FROM memoria_thread_summaries WHERE thread_id IN (SELECT (jsonb_array_elements_text(coalesce(s.evidencia->'threads', '[]')))::bigint) ORDER BY summarized_through DESC LIMIT 5) m),
    'hermanas', (SELECT coalesce(jsonb_agg(jsonb_build_object('id', h.id, 'titulo', h.titulo, 'senal', h.senal, 'severidad', h.severidad, 'estado', h.estado, 'dias_abierta', current_date - h.desde) ORDER BY h.severidad DESC), '[]')
                 FROM situaciones h WHERE h.id <> s.id AND h.company_id IS NOT NULL AND h.company_id = s.company_id AND h.estado NOT IN ('resuelta', 'descartada') AND h.fusionada_en IS NULL),
    'posibles_duplicados', (SELECT coalesce(jsonb_agg(jsonb_build_object('id', h.id, 'titulo', h.titulo, 'senal', h.senal, 'similitud', round(extensions.similarity(h.titulo, s.titulo)::numeric, 2), 'documentos_comunes', dc.n) ORDER BY dc.n DESC), '[]')
                            FROM situaciones h
                            CROSS JOIN LATERAL (SELECT count(*) AS n FROM jsonb_array_elements(h.documentos) a JOIN jsonb_array_elements(s.documentos) b ON a->>'modelo' = b->>'modelo' AND a->>'id' = b->>'id') dc
                            WHERE h.id <> s.id AND h.estado NOT IN ('resuelta', 'descartada') AND h.fusionada_en IS NULL
                              AND ((h.company_id IS NOT NULL AND h.company_id = s.company_id AND extensions.similarity(h.titulo, s.titulo) > 0.3) OR dc.n > 0)),
    'reglas', (SELECT coalesce(jsonb_agg(to_jsonb(r)), '[]') FROM situacion_reglas r
               WHERE (r.vigente_hasta IS NULL OR r.vigente_hasta > now())
                 AND ((r.alcance = 'senal' AND r.clave_alcance = s.senal) OR (r.alcance = 'situacion' AND r.clave_alcance = s.clave)
                   OR (r.alcance = 'contraparte' AND r.clave_alcance IN ('company:' || s.company_id, 'partner:' || s.odoo_partner_id)))),
    'personas', (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('odoo_user_id', u.odoo_user_id, 'name', u.name, 'department', u.department, 'motivo', p.motivo)), '[]')
                 FROM (SELECT x.responsable_odoo_user_id AS uid, 'dueño del documento en Odoo' AS motivo FROM senales x WHERE x.resuelta_en IS NULL AND x.clave IN (SELECT jsonb_array_elements_text(s.evidencia->'senales'))
                       UNION SELECT situacion_user_de_buzon(e.mailbox), 'buzón que más atiende a la empresa (' || e.mailbox || ', ' || e.share || '%)' FROM memoria_encargados e WHERE e.company_id = s.company_id AND e.area IS NULL AND e.rank <= 2) p
                 JOIN odoo_users u ON u.odoo_user_id = p.uid),
    'historia', (SELECT coalesce(jsonb_agg(e ORDER BY i), '[]') FROM (SELECT e, i FROM jsonb_array_elements(s.historia) WITH ORDINALITY t(e, i) ORDER BY i DESC LIMIT 10) q)
  ) INTO out;
  RETURN out;
END $$;
COMMENT ON FUNCTION public.situacion_contexto(bigint) IS 'Todo lo que un bot o el CEO necesita sobre una situación (spec §7.1): señales con calidad y episodio, documentos, ficha de memoria de la empresa, conversaciones ligadas (resúmenes), hermanas, posibles duplicados (trigram + documentos comunes), reglas, personas candidatas, historia.';

CREATE OR REPLACE FUNCTION public.situacion_por_persona(p_odoo_user_id integer)
RETURNS TABLE (id bigint, area text, tipo text, titulo text, severidad smallint, estado text, calidad text, rol text, dias_abierta integer, dias_sin_cambio integer, ultimo_cambio text, recomendacion text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT s.id, s.area, s.tipo, s.titulo, s.severidad, s.estado, s.calidad,
         CASE WHEN (s.delegacion->>'user_id')::int = p_odoo_user_id THEN 'delegada' ELSE 'responsable sugerido' END AS rol,
         current_date - s.desde, extract(day FROM now() - s.ultimo_cambio_en)::int, s.ultimo_cambio, s.recomendacion
  FROM situaciones s
  WHERE s.estado NOT IN ('resuelta', 'descartada') AND s.fusionada_en IS NULL
    AND (s.responsable_sugerido_user_id = p_odoo_user_id OR (s.delegacion->>'user_id')::int = p_odoo_user_id
         OR EXISTS (SELECT 1 FROM senales x WHERE x.resuelta_en IS NULL AND x.responsable_odoo_user_id = p_odoo_user_id AND x.clave IN (SELECT jsonb_array_elements_text(s.evidencia->'senales'))))
  ORDER BY s.severidad DESC, s.ultimo_cambio_en DESC
$$;

CREATE OR REPLACE FUNCTION public.situacion_higiene()
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT jsonb_build_object(
    'generado', now(),
    'clases', coalesce((SELECT jsonb_agg(jsonb_build_object('senal', q.senal, 'titulo', c.titulo, 'area', c.area, 'calidad', q.calidad, 'n', q.n, 'valor', q.valor,
                          'motivos', q.motivos, 'ejemplos', q.ejemplos, 'limpieza', c.reglas_calidad->>'limpieza',
                          'situacion_id', (SELECT id FROM situaciones WHERE clave = q.senal || '|higiene:' || q.calidad)) ORDER BY q.n DESC)
      FROM (SELECT s.senal, s.calidad, count(*) AS n, sum(s.valor) AS valor,
                   (SELECT jsonb_agg(m) FROM (SELECT DISTINCT calidad_motivo m FROM senales z WHERE z.senal = s.senal AND z.calidad = s.calidad AND z.resuelta_en IS NULL LIMIT 5) mm) AS motivos,
                   (SELECT jsonb_agg(jsonb_build_object('clave', z.clave, 'valor_texto', z.valor_texto, 'documento', z.documentos->0, 'motivo', z.calidad_motivo))
                      FROM (SELECT * FROM senales z WHERE z.senal = s.senal AND z.calidad = s.calidad AND z.resuelta_en IS NULL ORDER BY z.valor DESC NULLS LAST LIMIT 5) z) AS ejemplos
            FROM senales s WHERE s.resuelta_en IS NULL AND s.calidad IN ('zombie', 'dato_malo') GROUP BY s.senal, s.calidad) q
      JOIN senales_config c ON c.senal = q.senal), '[]'),
    'ignoradas', (SELECT count(*) FROM senales WHERE resuelta_en IS NULL AND calidad = 'ignorada'))
$$;
COMMENT ON FUNCTION public.situacion_higiene() IS 'Zombis y datos malos por señal con conteo, motivos, ejemplos y la limpieza recomendada en Odoo (spec §7.1).';

CREATE OR REPLACE FUNCTION public.situacion_salud()
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT jsonb_build_object(
    'generado', now(),
    'senales', (SELECT coalesce(jsonb_agg(jsonb_build_object(
                  'senal', c.senal, 'fuente', c.fuente, 'activa', c.activa, 'cada_horas', c.cada_horas,
                  'ultimo_lote_ok', l.recibido_en, 'edad_h', round(extract(epoch FROM now() - l.recibido_en) / 3600, 1),
                  'sin_datos', l.recibido_en IS NULL OR l.recibido_en < now() - make_interval(hours => coalesce(c.sin_datos_horas, 2 * c.cada_horas)),
                  'n_abiertas', (SELECT count(*) FROM senales s WHERE s.senal = c.senal AND s.resuelta_en IS NULL),
                  'ultimo_error', (SELECT jsonb_build_object('en', e.recibido_en, 'error', e.error) FROM senales_lotes e WHERE e.senal = c.senal AND NOT e.ok ORDER BY e.recibido_en DESC LIMIT 1)
                ) ORDER BY c.fuente, c.senal), '[]')
                FROM senales_config c
                LEFT JOIN LATERAL (SELECT recibido_en FROM senales_lotes x WHERE x.senal = c.senal AND x.ok ORDER BY recibido_en DESC LIMIT 1) l ON true
                WHERE c.activa),
    'bot', (SELECT to_jsonb(r) FROM (SELECT id, corrida, origen, iniciada_en, sql_lista_en, terminada_en, n_senales, n_candidatas, n_nuevas, n_actualizadas, n_resueltas, n_redactadas, n_fusiones, n_ignoradas, tokens_in, tokens_out, modelo, errores FROM situacion_corridas ORDER BY iniciada_en DESC LIMIT 1) r),
    'memoria', (SELECT coalesce(jsonb_agg(to_jsonb(h)), '[]') FROM memoria_cron_health() h WHERE h.jobname IN ('memoria_consolidar', 'memoria_sync_emails', 'memoria_ligas')),
    'odoo_push', (SELECT to_jsonb(r) FROM (SELECT method, status, created_at, round(extract(epoch FROM now() - created_at) / 3600, 1) AS edad_h FROM odoo_push_last_events WHERE method IN ('contacts', 'senales') ORDER BY created_at DESC LIMIT 1) r),
    'sgi_fuentes_apagadas', (SELECT count(*) FROM senales WHERE senal = 'fuente_sgi_apagada' AND resuelta_en IS NULL),
    'situaciones', (SELECT jsonb_object_agg(estado, n) FROM (SELECT estado, count(*) AS n FROM situaciones WHERE fusionada_en IS NULL GROUP BY estado) z))
$$;
COMMENT ON FUNCTION public.situacion_salud() IS 'Edad del último lote bueno por señal (sin_datos), última corrida del bot, salud de la memoria, último push de Odoo, fuentes SGI apagadas (spec §7.1).';

-- La IA escribe SOLO esto (spec §6.5). Severidad recortada a la banda; estado, clave, documentos y evidencia nunca se tocan.
CREATE OR REPLACE FUNCTION public.situacion_redactar(p_id bigint, p jsonb, p_modelo text DEFAULT NULL, p_corrida_id bigint DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE s situaciones%ROWTYPE; cfg record; v_sev int; d jsonb; n_fus int := 0; v_user int; v_evento text;
BEGIN
  SELECT * INTO s FROM situaciones WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'situación % no existe', p_id; END IF;
  SELECT * INTO cfg FROM senales_config WHERE senal = s.senal;
  v_sev := least(greatest(coalesce((p->>'severidad')::int, s.severidad), cfg.severidad_base), cfg.severidad_max);
  v_user := (p->>'responsable_sugerido_user_id')::int;
  IF v_user IS NOT NULL AND NOT EXISTS (SELECT 1 FROM odoo_users WHERE odoo_user_id = v_user) THEN v_user := NULL; END IF;
  v_evento := coalesce(nullif(p->>'evento_historia', ''), 'redactada');

  -- Fusiones: solo hermanas abiertas de la misma contraparte (o sin contraparte ambas), nunca la propia.
  FOR d IN SELECT * FROM jsonb_array_elements(coalesce(p->'duplicados', '[]'::jsonb)) LOOP
    IF d->>'decision' = 'fusionar' AND (d->>'id')::bigint <> p_id THEN
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
REVOKE ALL ON FUNCTION public.situacion_redactar(bigint, jsonb, text, bigint) FROM public, anon, authenticated;
COMMENT ON FUNCTION public.situacion_redactar(bigint, jsonb, text, bigint) IS 'Escribe lo que devuelve Claude (spec §6.5): titulo, resumen, recomendacion, severidad (recortada a [severidad_base, severidad_max]), responsable sugerido + motivo, fusiones (duplicados[].decision=fusionar) y el evento de historia. Nunca clave, documentos, evidencia ni estado.';

COMMIT;
```

- [ ] **Step 4: Aplicar, correr la prueba (`PRUEBA_OK`) y el criterio de aceptación del paso 1**

```sql
select situacion_ciclo();
select * from situacion_mapa('comercial');
```
Expected: filas `cliente_sin_respuesta` y `compromiso_correo` agrupadas por empresa, con `contraparte`, `calidad`, `dias_abierta`, título provisional "Cliente sin respuesta · <empresa>". Anota los conteos en el PR.

- [ ] **Step 5: Commit** — "Situación: RPCs de lectura (mapa, contexto, persona, higiene, salud) y situacion_redactar".

### Task 1.6: PR de la Parte 1 y documentación

**Files:**
- Modify: `CLAUDE.md` (quimibond-intelligence): sección nueva "Situación de la empresa" con tablas, RPCs, cómo preguntar por MCP (`situacion_mapa`, `situacion_contexto`, `situacion_higiene`, `situacion_salud`, `situacion_ciclo` a mano), y las pruebas en seco.

- [ ] **Step 1: Documentar en `CLAUDE.md`** (inventario: 7 tablas nuevas, RPCs nuevas en la tabla "Consumidas por Claude por MCP" y "Consumidas por Edge Functions", el job `situacion_respaldo` como pendiente hasta el paso 3).
- [ ] **Step 2: `npx tsc --noEmit && npm test`** (no debe cambiar nada; sigue verde).
- [ ] **Step 3: Commit, push y PR draft** en `quimibond/quimibond-intelligence` desde `claude/awesome-franklin-odolbj` ("Situación de la empresa: esquema, señales de memoria, situaciones y RPCs (plan A, paso 1)"), `subscribe_pr_activity`, esperar CI (`check` verde; Vercel falla por el frontend retirado: un comentario de standing-down), ready + squash-merge, `git fetch origin main && git checkout -B claude/awesome-franklin-odolbj origin/main && git push --force-with-lease`.

---

## Parte 2 — qb19: `_push_senales` (paso 2 del spec)

Todo en `/home/user/qb19/addons/quimibond_intelligence/`. Orden de las señales: finanzas y comercial primero (spec §8), luego el resto. Cada método de señal devuelve **la lista completa** de filas activas (`[]` = "no hay ninguna": resuelve todo) o `None` = "no aplica en esta base" (modelo no instalado: no se manda lote y `situacion_salud` la reporta `sin_datos`).

Formato de fila (lo que `senales_ingestar` espera):

```python
{'clave': 'cartera_vencida:partner:1742', 'documentos': [{'modelo': 'account.move', 'id': 9981, 'nombre': 'INV/2026/0412', 'monto': 12345.6, 'vence': '2026-08-01'}],
 'odoo_partner_id': 1742, 'responsable_odoo_user_id': 7, 'valor': 53400.0, 'valor_texto': '3 facturas, la más vieja 48 días', 'vence': '2026-08-01',
 'payload': {'rfc': 'ABC010101XYZ', 'dias_max': 48, 'fecha_base': '2026-08-01'}}
```

### Task 2.1: `rpc_strict` en el cliente

**Files:**
- Modify: `addons/quimibond_intelligence/models/supabase_client.py`
- Modify: `addons/quimibond_intelligence/tests/test_supabase_client_details.py`

- [ ] **Step 1: Tests (pytest, mock de httpx)** — agregar al final del archivo de tests:

```python
import pytest
from quimibond_intelligence.models.supabase_client import SupabaseError


def test_rpc_strict_returns_json_on_2xx():
    mock = MagicMock()
    resp = MagicMock(status_code=200, content=b'{"ok": true, "nuevas": 2}')
    resp.json.return_value = {'ok': True, 'nuevas': 2}
    mock.post.return_value = resp
    c = _make_client(mock)
    assert c.rpc_strict('senales_ingestar', {'p_senal': 'x'}) == {'ok': True, 'nuevas': 2}
    assert mock.post.call_args[0][0].endswith('/rest/v1/rpc/senales_ingestar')


def test_rpc_strict_raises_on_http_error():
    mock = MagicMock()
    mock.post.return_value = MagicMock(status_code=400, content=b'{"message":"bad"}', text='{"message":"bad"}')
    c = _make_client(mock)
    with pytest.raises(SupabaseError) as exc:
        c.rpc_strict('senales_ingestar', {})
    assert 'HTTP 400' in str(exc.value) and 'bad' in str(exc.value)


def test_rpc_strict_raises_on_network_error():
    mock = MagicMock()
    mock.post.side_effect = httpx.ConnectError('down')
    c = _make_client(mock)
    with pytest.raises(SupabaseError):
        c.rpc_strict('senales_ingestar', {})
```

- [ ] **Step 2: Correr → falla** (`cd /home/user/qb19/addons && python3 -m pytest quimibond_intelligence/tests -q`): `ImportError: cannot import name 'SupabaseError'`.

- [ ] **Step 3: Implementar** en `supabase_client.py` (después de `_logger`):

```python
class SupabaseError(Exception):
    """Error de Supabase que el llamador SÍ quiere ver (rpc_strict)."""
```

y, en la clase, después de `rpc()`:

```python
    def rpc_strict(self, function: str, params: dict, timeout: float = 120.0):
        """RPC que NO traga errores: HTTP != 2xx o red caída → SupabaseError.

        Para el push de señales (spec §4 regla 2): un lote solo cuenta si
        Supabase respondió bien, y un error deja el método en error en el
        Historial de Sync. `rpc()` sigue siendo el tolerante para lo demás.
        """
        try:
            resp = self._http.post(
                f'{self.url}/rest/v1/rpc/{function}',
                content=json.dumps(params or {}, default=str),
                headers=self.headers, timeout=timeout,
            )
        except (httpx.HTTPError, OSError) as exc:
            raise SupabaseError(f'rpc {function}: {exc}') from exc
        if resp.status_code >= 300:
            raise SupabaseError(f'rpc {function} HTTP {resp.status_code}: {(resp.text or "")[:300]}')
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()
```

- [ ] **Step 4: Correr → pasan los 3 nuevos y los viejos.** `flake8 addons/` limpio.
- [ ] **Step 5: Commit** — "quimibond_intelligence: rpc_strict (errores visibles) para el push de señales".

### Task 2.2: Registro de señales, helpers y andamiaje de tests de Odoo

**Files:**
- Create: `addons/quimibond_intelligence/models/senales/__init__.py`
- Create: `addons/quimibond_intelligence/models/senales/base.py`
- Modify: `addons/quimibond_intelligence/models/__init__.py`
- Modify: `addons/quimibond_intelligence/tests/__init__.py`
- Create: `addons/quimibond_intelligence/tests/common.py`, `tests/test_senales_base.py`
- Modify: `.github/workflows/ci.yml` (`--test-tags … ,/quimibond_intelligence`)

- [ ] **Step 1: Test de Odoo** `tests/test_senales_base.py`:

```python
# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged

from ..models.senales import base


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesBase(TransactionCase):

    def test_registro_tiene_las_senales_de_odoo(self):
        # Todas las señales fuente=odoo del catálogo (spec §4) menos delegacion_estado (plan B).
        esperadas = {
            'entrega_vencida', 'pedido_sin_fecha', 'lead_frio', 'venta_margen_negativo', 'producto_pierde', 'cliente_pierde', 'cotizacion_bajo_costo',
            'op_atrasada', 'op_sin_componentes', 'tiempos_excepcion', 'existencia_negativa', 'reorden_pendiente', 'transferencia_atorada', 'familia_saturada', 'mantenimiento_abierto',
            'recepcion_vencida', 'oc_sin_confirmacion', 'aprobacion_pendiente', 'precio_compra_subio', 'actividad_vencida_oc', 'proveedor_reprobado',
            'cartera_vencida', 'cxp_vencida', 'factura_proveedor_borrador', 'entregado_sin_facturar', 'banco_sin_conciliar', 'cfdi_cancelacion_pendiente',
            'sat_discrepancia', 'sat_complemento', 'sat_extraccion_detenida', 'nomina_borrador', 'cash_bajo_piso', 'indicador_financiero_rojo',
            'indicador_rojo', 'accion_correctiva_vencida', 'nc_abierta', 'calibracion_vencida', 'legal_incumplido', 'riesgo_sin_tratar', 'ppap_rechazado', 'auditoria_pendiente', 'fuente_sgi_apagada',
            'aprobacion_rh', 'evaluacion_vencida', 'ticket_abierto',
            'carga_actividades', 'firma_pendiente', 'acuse_documento', 'acuerdo_direccion_vencido', 'obligacion_legado',
        }
        self.assertEqual(esperadas - set(base.REGISTRO), set(), 'señales del catálogo sin consulta')

    def test_helpers(self):
        p = self.env['res.partner'].create({'name': 'Cliente Prueba', 'vat': 'XAXX010101000'})
        d = base.doc(p, monto=10.5)
        self.assertEqual(d, {'modelo': 'res.partner', 'id': p.id, 'nombre': 'Cliente Prueba', 'monto': 10.5})
        f = base.fila('x:1', [d], valor=3, partner=p, payload={'a': 1})
        self.assertEqual(f['odoo_partner_id'], p.commercial_partner_id.id)
        self.assertEqual(f['payload'], {'a': 1, 'rfc': 'XAXX010101000'})
        self.assertIsNone(f['responsable_odoo_user_id'])
        self.assertTrue(base.tiene_modelo(self.env, 'res.partner'))
        self.assertFalse(base.tiene_modelo(self.env, 'no.existe'))
        self.assertEqual(base.umbral({'umbrales': {'dias': '9'}}, 'dias', 3), 9)
        self.assertEqual(base.umbral({'umbrales': {}}, 'dias', 3), 3)
        self.assertEqual(base.umbral({}, 'dias', 3), 3)
```

`tests/__init__.py` (hoy vacío para pytest): `from . import test_senales_base` (y después los demás `test_senales_*`). Los archivos de pytest (`conftest.py`, `test_supabase_client_details.py`) no se importan aquí: pytest los descubre solo y Odoo solo importa lo que lista `__init__.py`.

`tests/common.py`:

```python
# -*- coding: utf-8 -*-
"""Base común de los tests de señales: fecha fija, partner y helpers de documentos."""
from datetime import date, timedelta

from odoo.tests import TransactionCase


class SenalesCommon(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.hoy = date.today()
        cls.cliente = cls.env['res.partner'].create({'name': 'Cliente Señal', 'customer_rank': 1, 'vat': 'ABC010101XYZ'})
        cls.proveedor = cls.env['res.partner'].create({'name': 'Proveedor Señal', 'supplier_rank': 1})
        cls.cfg = {'umbrales': {}}

    def hace(self, dias):
        return self.hoy - timedelta(days=dias)

    def filas(self, senal, cfg=None):
        from ..models.senales import base
        return base.REGISTRO[senal](self.env, cfg or self.cfg)

    def fila_de(self, filas, clave):
        for f in filas:
            if f['clave'] == clave:
                return f
        self.fail('no hay fila %s en %s' % (clave, [f['clave'] for f in filas]))
```

- [ ] **Step 2: Implementar `base.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de Odoo para el mapa de situación (Supabase `senales`).

Una señal = una función registrada con @senal('nombre') que devuelve la lista
COMPLETA de filas activas (spec §4 regla 2): `[]` resuelve todo lo abierto de
esa señal; `None` = no aplica en esta base (modelo no instalado), no se manda
lote. Nunca cifras en masa: una fila por hecho con modelo+id del documento.
"""
import logging
from datetime import datetime, timedelta

from odoo import fields

_logger = logging.getLogger(__name__)

REGISTRO = {}   # nombre de señal → fn(env, cfg) -> list[dict] | None


def senal(nombre):
    def deco(fn):
        REGISTRO[nombre] = fn
        fn.senal = nombre
        return fn
    return deco


def hoy():
    return fields.Date.today()


def hace(n):
    """Fecha de hace n días (para dominios `('campo', '<', hace(7))`)."""
    return hoy() - timedelta(days=n)


def dias(desde, hasta=None):
    """Días entre una fecha (date o datetime) y hoy; None si no hay fecha."""
    if not desde:
        return None
    if isinstance(desde, datetime):
        desde = desde.date()
    return ((hasta or hoy()) - desde).days


def umbral(cfg, clave, default):
    try:
        v = (cfg or {}).get('umbrales') or {}
        v = v.get(clave, default)
        return type(default)(v) if default is not None and not isinstance(v, type(default)) else v
    except (TypeError, ValueError):
        return default


def tiene_modelo(env, nombre):
    return nombre in env


def companias(env):
    return env['quimibond.sync']._get_company_ids()


def doc(rec, nombre=None, **extra):
    d = {'modelo': rec._name, 'id': rec.id, 'nombre': nombre or rec.display_name}
    d.update({k: v for k, v in extra.items() if v not in (None, False)})
    return d


def fila(clave, documentos, valor=None, valor_texto=None, vence=None, partner=None, user=None, payload=None):
    """Arma una fila para senales_ingestar. `partner` se resuelve a su empresa
    comercial (así 7 facturas de 7 contactos del mismo cliente son una
    situación); el RFC viaja en payload para la regla de partes relacionadas."""
    p = partner.commercial_partner_id if partner else None
    pl = dict(payload or {})
    if p is not None and p.vat and 'rfc' not in pl:
        pl['rfc'] = p.vat
    return {
        'clave': clave,
        'documentos': documentos or [],
        'odoo_partner_id': p.id if p else None,
        'responsable_odoo_user_id': user.id if user else None,
        'valor': float(valor) if valor is not None else None,
        'valor_texto': (valor_texto or '')[:300] or None,
        'vence': str(vence) if vence else None,
        'payload': pl,
    }


def agrupar(records, key):
    """{clave: [records]} conservando el orden de aparición."""
    out = {}
    for r in records:
        out.setdefault(key(r), []).append(r)
    return out


def texto_monto(m, moneda='MXN'):
    return f'${m:,.0f} {moneda}'
```

`__init__.py` del paquete:

```python
from . import base  # noqa: F401
from . import finanzas, comercial, operaciones, compras, calidad_sgi, rh, sistemas, direccion  # noqa: F401,E401
```
(Los módulos de área se crean en las tareas siguientes; hasta entonces importa solo los que existan.)

`models/__init__.py`: agrega `from . import senales` y `from . import sync_push_senales` (este último en la Tarea 2.11).

- [ ] **Step 3: CI** — en `.github/workflows/ci.yml` cambia `--test-tags /qb_capacidad_costeo,/quimibond_sat,/qb_obligation,/qb_memoria,/qb_build_limpio` por `…,/qb_build_limpio,/quimibond_intelligence`.
- [ ] **Step 4: `flake8 addons/ && python -m compileall -q addons/quimibond_intelligence`** limpio. Commit — "quimibond_intelligence: registro de señales, helpers y tests de Odoo en CI". Push y abre el PR draft de la Parte 2 ya (el CI de Odoo tarda ~10 min; mejor verlo temprano).
- [ ] **Step 5: Verifica en el PR que el job `odoo-tests` corre `test_senales_base`** (busca `quimibond_intelligence` en el log). El test de registro fallará hasta terminar las tareas 2.3–2.10: es la lista de pendientes.

### Task 2.3: Señales de finanzas

**Files:**
- Create: `addons/quimibond_intelligence/models/senales/finanzas.py`
- Create: `addons/quimibond_intelligence/tests/test_senales_finanzas.py` (+ import en `tests/__init__.py`)

- [ ] **Step 1: Tests**

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesFinanzas(SenalesCommon):

    def _factura(self, partner, tipo, monto, vence_hace, publicar=True):
        inv = self.env['account.move'].create({
            'move_type': tipo, 'partner_id': partner.id, 'invoice_date': self.hace(vence_hace + 30),
            'invoice_date_due': self.hace(vence_hace),
            'invoice_line_ids': [(0, 0, {'name': 'x', 'quantity': 1, 'price_unit': monto})],
        })
        if publicar:
            inv.action_post()
        return inv

    def test_cartera_vencida_agrupa_por_cliente_y_manda_rfc(self):
        contacto = self.env['res.partner'].create({'name': 'Contacto', 'parent_id': self.cliente.id})
        a = self._factura(self.cliente, 'out_invoice', 1000, 40)
        b = self._factura(contacto, 'out_invoice', 500, 10)
        self._factura(self.cliente, 'out_invoice', 700, -5)          # no vencida
        self._factura(self.cliente, 'out_invoice', 900, 20, publicar=False)  # borrador
        filas = self.filas('cartera_vencida')
        f = self.fila_de(filas, 'cartera_vencida:partner:%d' % self.cliente.id)
        self.assertEqual({d['id'] for d in f['documentos']}, {a.id, b.id})
        self.assertAlmostEqual(f['valor'], 1500.0)
        self.assertEqual(f['payload']['rfc'], 'ABC010101XYZ')
        self.assertEqual(f['payload']['dias_max'], 40)
        self.assertEqual(f['vence'], str(self.hace(40)))
        self.assertIn('2 facturas', f['valor_texto'])
        self.assertEqual(len(filas), 1)

    def test_cxp_vencida_es_positiva_y_por_proveedor(self):
        self._factura(self.proveedor, 'in_invoice', 300, 3)
        f = self.fila_de(self.filas('cxp_vencida'), 'cxp_vencida:partner:%d' % self.proveedor.id)
        self.assertAlmostEqual(f['valor'], 300.0)

    def test_factura_proveedor_borrador_respeta_umbral(self):
        inv = self._factura(self.proveedor, 'in_invoice', 100, 0, publicar=False)
        self.env.cr.execute('UPDATE account_move SET create_date = %s WHERE id = %s', (self.hace(5), inv.id))
        inv.invalidate_recordset(['create_date'])
        self.assertTrue(self.fila_de(self.filas('factura_proveedor_borrador', {'umbrales': {'dias': 3}}), 'factura_proveedor_borrador:account.move:%d' % inv.id))
        self.assertEqual([f for f in self.filas('factura_proveedor_borrador', {'umbrales': {'dias': 10}}) if f['clave'].endswith(':%d' % inv.id)], [])

    def test_senales_de_modelos_no_instalados_devuelven_none(self):
        # En Community no hay hr_payroll, sign ni SGI: None = sin lote (situacion_salud lo reporta), nunca [] (que resolvería todo).
        for s in ('nomina_borrador', 'sat_discrepancia', 'cash_bajo_piso', 'indicador_financiero_rojo'):
            if not self.env['ir.module.module'].search([('name', 'in', ('hr_payroll', 'quimibond_sat', 'quimibond_cash_flow', 'quimibond_sgi')), ('state', '=', 'installed')]):
                self.assertIsNone(self.filas(s), s)
```

- [ ] **Step 2: Correr en CI → fallan** (`KeyError: 'cartera_vencida'` en el registro).

- [ ] **Step 3: Implementar `finanzas.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de finanzas (spec §4): cartera, CxP, facturación, banco, CFDI/SAT, nómina, flujo, indicadores."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar, texto_monto


def _vencidas(env, tipo):
    Move = env['account.move'].sudo()
    return Move.search([
        ('company_id', 'in', companias(env)), ('move_type', '=', tipo), ('state', '=', 'posted'),
        ('payment_state', 'in', ('not_paid', 'partial')), ('invoice_date_due', '<', hoy()),
    ], order='invoice_date_due')


def _cartera(env, tipo, prefijo, signo):
    filas = []
    for partner, moves in agrupar(_vencidas(env, tipo), lambda m: m.partner_id.commercial_partner_id).items():
        total = sum(signo * m.amount_residual_signed for m in moves)
        mas_vieja = max(dias(m.invoice_date_due) for m in moves)
        filas.append(fila(
            f'{prefijo}:partner:{partner.id}',
            [doc(m, m.name, monto=round(signo * m.amount_residual_signed, 2), vence=str(m.invoice_date_due), moneda=m.currency_id.name) for m in moves[:30]],
            valor=round(total, 2),
            valor_texto=f'{len(moves)} facturas, la más vieja {mas_vieja} días, {texto_monto(total)}',
            vence=min(m.invoice_date_due for m in moves), partner=partner,
            user=next((m.invoice_user_id for m in moves if m.invoice_user_id), None),
            payload={'dias_max': mas_vieja, 'n': len(moves), 'fecha_base': str(min(m.invoice_date_due for m in moves))}))
    return filas


@senal('cartera_vencida')
def cartera_vencida(env, cfg):
    return _cartera(env, 'out_invoice', 'cartera_vencida', 1)


@senal('cxp_vencida')
def cxp_vencida(env, cfg):
    return _cartera(env, 'in_invoice', 'cxp_vencida', -1)


@senal('factura_proveedor_borrador')
def factura_proveedor_borrador(env, cfg):
    n = umbral(cfg, 'dias', 3)
    Move = env['account.move'].sudo()
    moves = Move.search([('company_id', 'in', companias(env)), ('move_type', '=', 'in_invoice'), ('state', '=', 'draft'),
                         ('create_date', '<', hace(n))])
    return [fila(f'factura_proveedor_borrador:account.move:{m.id}', [doc(m, m.name or m.ref or f'#{m.id}', monto=m.amount_total)],
                 valor=dias(m.create_date), valor_texto=f'{dias(m.create_date)} días en borrador', partner=m.partner_id,
                 user=m.invoice_user_id or m.create_uid, payload={'fecha_base': str(m.create_date.date())}) for m in moves]


@senal('entregado_sin_facturar')
def entregado_sin_facturar(env, cfg):
    SO = env['sale.order'].sudo()
    orders = SO.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('invoice_status', '=', 'to invoice')])
    filas = []
    for partner, ords in agrupar(orders, lambda o: o.partner_id.commercial_partner_id).items():
        total = sum(o.amount_total for o in ords)
        filas.append(fila(f'entregado_sin_facturar:partner:{partner.id}',
                          [doc(o, o.name, monto=o.amount_total, fecha=str(o.date_order.date())) for o in ords[:30]],
                          valor=round(total, 2), valor_texto=f'{len(ords)} pedidos por facturar, {texto_monto(total)}', partner=partner,
                          user=next((o.user_id for o in ords if o.user_id), None)))
    return filas


@senal('banco_sin_conciliar')
def banco_sin_conciliar(env, cfg):
    Line = env['account.bank.statement.line'].sudo()
    lines = Line.search([('company_id', 'in', companias(env)), ('is_reconciled', '=', False)], order='date')
    filas = []
    for journal, ls in agrupar(lines, lambda l: l.journal_id).items():
        filas.append(fila(f'banco_sin_conciliar:journal:{journal.id}',
                          [doc(l, f'{l.date} {l.payment_ref or ""}'[:80], monto=l.amount) for l in ls[:20]],
                          valor=len(ls), valor_texto=f'{len(ls)} movimientos sin conciliar, el más viejo {dias(ls[0].date)} días',
                          payload={'grupo': journal.name, 'dias_max': dias(ls[0].date), 'monto': round(sum(l.amount for l in ls), 2)}))
    return filas


@senal('cfdi_cancelacion_pendiente')
def cfdi_cancelacion_pendiente(env, cfg):
    Move = env['account.move'].sudo()
    if 'l10n_mx_edi_cfdi_state' not in Move._fields:
        return None
    moves = Move.search([('company_id', 'in', companias(env)), ('l10n_mx_edi_cfdi_state', '=', 'cancel_requested')])
    return [fila(f'cfdi_cancelacion_pendiente:account.move:{m.id}', [doc(m, m.name, monto=m.amount_total)], valor=dias(m.write_date),
                 valor_texto=f'cancelación solicitada hace {dias(m.write_date)} días', partner=m.partner_id, user=m.invoice_user_id) for m in moves]


@senal('sat_discrepancia')
def sat_discrepancia(env, cfg):
    if not tiene_modelo(env, 'sat.compare.line'):
        return None
    issues = ('monto', 'moneda', 'cancelado_odoo', 'cancelado_sat', 'solo_sat', 'solo_odoo')
    lines = env['sat.compare.line'].sudo().search([('issue', 'in', issues)])
    filas = []
    for l in lines:
        ref = l.move_id or l.cfdi_id
        filas.append(fila(f'sat_discrepancia:{l.issue}:{l.uuid or l.id}',
                          [doc(ref, l.move_name or l.uuid or '?', uuid=l.uuid)] if ref else [],
                          valor=1, valor_texto=dict(l._fields['issue'].selection).get(l.issue, l.issue),
                          partner=getattr(l, 'partner_id', None) or (l.move_id.partner_id if l.move_id else None),
                          payload={'grupo': l.issue, 'direction': l.direction}))
    return filas


@senal('sat_complemento')
def sat_complemento(env, cfg):
    if not tiene_modelo(env, 'sat.pago.compare'):
        return None
    rows = env['sat.pago.compare'].sudo().search([('issue', 'in', ('sin_complemento', 'complemento_duplicado'))])
    return [fila(f'sat_complemento:{r.issue}:{r.move_id.id if r.move_id else r.cfdi_id.id}', [doc(r.move_id or r.cfdi_id, r.move_name or '?', monto=r.total)],
                 valor=abs(r.delta_mxn or 0), valor_texto=dict(r._fields['issue'].selection).get(r.issue, r.issue), partner=r.partner_id,
                 payload={'grupo': r.issue, 'direction': r.direction, 'saldo_sat': r.saldo_sat, 'saldo_odoo': r.saldo_odoo}) for r in rows]


@senal('sat_extraccion_detenida')
def sat_extraccion_detenida(env, cfg):
    Company = env['res.company'].sudo()
    if 'sat_data_until_issued' not in Company._fields:
        return None
    n = umbral(cfg, 'dias', 3)
    filas = []
    for c in Company.browse(companias(env)):
        for campo, sentido in (('sat_data_until_issued', 'emitidos'), ('sat_data_until_received', 'recibidos')):
            hasta = c[campo]
            if hasta and dias(hasta) > n:
                filas.append(fila(f'sat_extraccion_detenida:{c.id}:{sentido}', [doc(c, c.name)], valor=dias(hasta),
                                  valor_texto=f'{sentido}: datos del SAT hasta {hasta} ({dias(hasta)} días)', payload={'sentido': sentido}))
    return filas


@senal('nomina_borrador')
def nomina_borrador(env, cfg):
    if not tiene_modelo(env, 'hr.payslip'):
        return None
    n = umbral(cfg, 'dias', 3)
    slips = env['hr.payslip'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'draft'),
                                             ('date_to', '<', hace(n))])
    return [fila(f'nomina_borrador:hr.payslip:{s.id}', [doc(s, s.name or s.number or f'#{s.id}', periodo=f'{s.date_from}..{s.date_to}')],
                 valor=dias(s.date_to), valor_texto=f'{s.employee_id.name}: periodo al {s.date_to} sin confirmar',
                 payload={'grupo': (s.payslip_run_id.name if s.payslip_run_id else str(s.date_to)[:7]), 'fecha_base': str(s.date_to)}) for s in slips]


@senal('cash_bajo_piso')
def cash_bajo_piso(env, cfg):
    if not tiene_modelo(env, 'cash.flow.forecast.engine') or not tiene_modelo(env, 'cash.flow.config'):
        return None
    filas = []
    for config in env['cash.flow.config'].sudo().search([('company_id', 'in', companias(env))]):
        if not config.forecast_min_cash:
            continue
        res = env['cash.flow.forecast.engine'].compute(config, hoy())
        below = res.get('below_min') or []
        if not below:
            continue
        weeks = res['weeks']
        closing = res['closing']
        runway = next((i for i, v in enumerate(closing) if v <= 0), None)
        primera = weeks[below[0]][0]
        filas.append(fila(f'cash_bajo_piso:company:{config.company_id.id}', [doc(config, config.display_name)],
                          valor=round(min(closing), 2),
                          valor_texto=(f'{len(below)} semanas bajo el piso desde {primera}; mínimo {texto_monto(min(closing))}'
                                       + (f'; efectivo ≤ 0 en la semana del {weeks[runway][0]}' if runway is not None else '')),
                          vence=primera, payload={'semanas_bajo_piso': [str(weeks[i][0]) for i in below], 'piso': config.forecast_min_cash,
                                                  'runway': str(weeks[runway][0]) if runway is not None else None}))
    return filas


def _medidas_rojas(env, cfg, filtro_nombres=None):
    if not tiene_modelo(env, 'sgi.indicator.measure'):
        return None
    n = umbral(cfg, 'dias', 60)
    Measure = env['sgi.indicator.measure'].sudo()
    ms = Measure.search([('semaphore', '=', 'rojo'), ('state', '=', 'validado'), ('period_date', '>=', hace(n))], order='period_date desc')
    if filtro_nombres:
        pats = [p.lower() for p in filtro_nombres]
        ms = ms.filtered(lambda m: any(p in (m.indicator_id.name or '').lower() for p in pats))
    vistos = set()
    out = []
    for m in ms:  # solo la medida más reciente por indicador
        if m.indicator_id.id in vistos:
            continue
        vistos.add(m.indicator_id.id)
        out.append(m)
    return out


@senal('indicador_financiero_rojo')
def indicador_financiero_rojo(env, cfg):
    ms = _medidas_rojas(env, cfg, umbral(cfg, 'nombres', ['DSO', 'cartera', 'DPO']))
    if ms is None:
        return None
    return [fila(f'indicador_financiero_rojo:sgi.indicator:{m.indicator_id.id}', [doc(m, f'{m.indicator_id.name} {m.period_date}', valor=m.value, objetivo=m.target_objective)],
                 valor=m.value, valor_texto=f'{m.indicator_id.name}: {m.value} {m.uom or ""} (objetivo {m.target_objective})', vence=m.period_date,
                 user=getattr(m.indicator_id, 'responsible_id', None) or getattr(m.indicator_id, 'user_id', None),
                 payload={'grupo': m.indicator_id.name, 'periodo': str(m.period_date)}) for m in ms]
```

Nota: `getattr(m.indicator_id, 'responsible_id', None)` — antes de dejarlo, mira `grep -n "responsible\|user_id" addons/quimibond_sgi/models/sgi_indicator.py | head` y usa el campo real del responsable del indicador (si no hay, quita el `user=`).

- [ ] **Step 4: `flake8`, push, CI verde en `test_senales_finanzas`.** Commit — "Señales de finanzas: cartera, CxP, facturación, banco, CFDI, SAT, nómina, flujo, indicadores".

### Task 2.4: Señales comerciales

**Files:**
- Create: `models/senales/comercial.py`, `tests/test_senales_comercial.py` (+ import en `tests/__init__.py`)

- [ ] **Step 1: Tests**

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesComercial(SenalesCommon):

    def _salida(self, partner, hace):
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        prod = self.env['product.product'].create({'name': 'Tela', 'type': 'consu', 'is_storable': True})
        pick = self.env['stock.picking'].create({
            'partner_id': partner.id, 'picking_type_id': wh.out_type_id.id,
            'location_id': wh.lot_stock_id.id, 'location_dest_id': self.env.ref('stock.stock_location_customers').id,
            'scheduled_date': self.hace(hace),
            'move_ids': [(0, 0, {'name': 'Tela', 'product_id': prod.id, 'product_uom_qty': 5, 'product_uom': prod.uom_id.id,
                                 'location_id': wh.lot_stock_id.id, 'location_dest_id': self.env.ref('stock.stock_location_customers').id})],
        })
        pick.action_confirm()
        pick.move_ids.write({'date': self.hace(hace)})  # scheduled_date del picking se calcula de los movimientos
        return pick

    def test_entrega_vencida_por_cliente(self):
        a = self._salida(self.cliente, 3)
        self._salida(self.cliente, -2)  # futura
        f = self.fila_de(self.filas('entrega_vencida'), 'entrega_vencida:partner:%d' % self.cliente.id)
        self.assertEqual([d['id'] for d in f['documentos']], [a.id])
        self.assertEqual(f['valor'], 1)
        self.assertEqual(f['payload']['fecha_base'], str(self.hace(3)))

    def test_pedido_sin_fecha(self):
        prod = self.env['product.product'].create({'name': 'Entretela', 'type': 'consu'})
        so = self.env['sale.order'].create({'partner_id': self.cliente.id, 'order_line': [(0, 0, {'product_id': prod.id, 'product_uom_qty': 2})]})
        so.action_confirm()
        so.commitment_date = False
        f = self.fila_de(self.filas('pedido_sin_fecha'), 'pedido_sin_fecha:partner:%d' % self.cliente.id)
        self.assertEqual(f['documentos'][0]['id'], so.id)

    def test_costeo_no_instalado_devuelve_none(self):
        if 'qb.cotizacion' not in self.env:
            self.assertIsNone(self.filas('cotizacion_bajo_costo'))
```

- [ ] **Step 2: Correr → fallan (`KeyError`).**

- [ ] **Step 3: Implementar `comercial.py`**

```python
# -*- coding: utf-8 -*-
"""Señales comerciales (spec §4): entregas, pedidos, leads, márgenes, rentabilidad, cotizaciones."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar, texto_monto


def _pickings(env, code, extra=None):
    return env['stock.picking'].sudo().search([
        ('company_id', 'in', companias(env)), ('picking_type_code', '=', code),
        ('state', 'not in', ('done', 'cancel')), ('scheduled_date', '<', hoy()),
    ] + (extra or []), order='scheduled_date')


def _por_contraparte(picks, prefijo, sale=True):
    filas = []
    for partner, ps in agrupar(picks, lambda p: p.partner_id.commercial_partner_id if p.partner_id else None).items():
        clave = f'{prefijo}:partner:{partner.id}' if partner else f'{prefijo}:partner:0'
        mas_vieja = max(dias(p.scheduled_date) for p in ps)
        filas.append(fila(clave, [doc(p, p.name, origen=p.origin, fecha=str(p.scheduled_date.date())) for p in ps[:30]],
                          valor=len(ps), valor_texto=f'{len(ps)} entregas vencidas, la más vieja {mas_vieja} días',
                          vence=min(p.scheduled_date for p in ps).date(), partner=partner,
                          user=next((p.sale_id.user_id for p in ps if sale and p.sale_id and p.sale_id.user_id), None) or next((p.user_id for p in ps if p.user_id), None),
                          payload={'dias_max': mas_vieja, 'fecha_base': str(min(p.scheduled_date for p in ps).date())}))
    return filas


@senal('entrega_vencida')
def entrega_vencida(env, cfg):
    return _por_contraparte(_pickings(env, 'outgoing'), 'entrega_vencida')


@senal('pedido_sin_fecha')
def pedido_sin_fecha(env, cfg):
    SO = env['sale.order'].sudo()
    orders = SO.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('commitment_date', '=', False)])
    orders = orders.filtered(lambda o: any(l.product_uom_qty > l.qty_delivered and l.product_id.type != 'service' for l in o.order_line))
    filas = []
    for partner, ords in agrupar(orders, lambda o: o.partner_id.commercial_partner_id).items():
        filas.append(fila(f'pedido_sin_fecha:partner:{partner.id}', [doc(o, o.name, monto=o.amount_total, fecha=str(o.date_order.date())) for o in ords[:30]],
                          valor=len(ords), valor_texto=f'{len(ords)} pedidos confirmados sin fecha comprometida', partner=partner,
                          user=next((o.user_id for o in ords if o.user_id), None)))
    return filas


@senal('lead_frio')
def lead_frio(env, cfg):
    if not tiene_modelo(env, 'crm.lead'):
        return None
    n = umbral(cfg, 'dias', 14)
    leads = env['crm.lead'].sudo().search([('company_id', 'in', companias(env) + [False]), ('type', '=', 'opportunity'), ('active', '=', True),
                                           ('probability', '<', 100), ('date_last_stage_update', '<', hace(n))])
    leads = leads.filtered(lambda l: not l.activity_date_deadline or l.activity_date_deadline < hace(n))
    return [fila(f'lead_frio:crm.lead:{l.id}', [doc(l, l.name, monto=l.expected_revenue, etapa=l.stage_id.name)], valor=dias(l.date_last_stage_update),
                 valor_texto=f'{dias(l.date_last_stage_update)} días en {l.stage_id.name}', partner=l.partner_id, user=l.user_id,
                 payload={'fecha_base': str(l.date_last_stage_update.date())}) for l in leads]


@senal('venta_margen_negativo')
def venta_margen_negativo(env, cfg):
    Line = env['sale.order.line'].sudo()
    if 'margin' not in Line._fields:
        return None
    n = umbral(cfg, 'dias', 90)
    lines = Line.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('margin', '<', 0), ('create_date', '>=', hace(n))])
    filas = []
    for partner, ls in agrupar(lines, lambda l: l.order_id.partner_id.commercial_partner_id).items():
        malos = [l for l in ls if not l.purchase_price or not l.price_unit]
        filas.append(fila(f'venta_margen_negativo:partner:{partner.id}',
                          [doc(l, f'{l.order_id.name} · {l.product_id.display_name}'[:120], margen=round(l.margin, 2), precio=l.price_unit, costo=l.purchase_price) for l in ls[:30]],
                          valor=round(sum(l.margin for l in ls), 2), valor_texto=f'{len(ls)} líneas con margen negativo, {texto_monto(sum(l.margin for l in ls))}',
                          partner=partner, user=next((l.order_id.user_id for l in ls if l.order_id.user_id), None),
                          payload={'dato_malo': f'{len(malos)} líneas con costo 0 o precio 0' if len(malos) == len(ls) else None}))
    for f in filas:
        if not f['payload'].get('dato_malo'):
            f['payload'].pop('dato_malo', None)
    return filas


def _rentabilidad(env, modelo, prefijo, campo_id, grupo):
    if not tiene_modelo(env, modelo):
        return None
    recs = env[modelo].sudo().search([]).filtered(lambda r: r.semaforo == 'rojo')  # semaforo es computado no almacenado: filtrar en Python
    filas = []
    for r in recs:
        obj = r[campo_id]
        filas.append(fila(f'{prefijo}:{obj._name}:{obj.id}', [doc(obj, obj.display_name, ventas_12m=round(r.revenue_12m, 2), contribucion=round(r.contrib_12m, 2))],
                          valor=round(r.contrib_12m, 2), valor_texto=f'{r.veredicto or "pierde"}; ventas 12m {texto_monto(r.revenue_12m)}',
                          partner=obj if obj._name == 'res.partner' else None, payload={'grupo': grupo}))
    return filas


@senal('producto_pierde')
def producto_pierde(env, cfg):
    return _rentabilidad(env, 'qb.producto.rentabilidad', 'producto_pierde', 'product_id', 'productos')


@senal('cliente_pierde')
def cliente_pierde(env, cfg):
    return _rentabilidad(env, 'qb.cliente.rentabilidad', 'cliente_pierde', 'partner_id', 'clientes')


@senal('cotizacion_bajo_costo')
def cotizacion_bajo_costo(env, cfg):
    if not tiene_modelo(env, 'qb.cotizacion'):
        return None
    n = umbral(cfg, 'dias_vigencia', 15)
    cots = env['qb.cotizacion'].sudo().search([('state', 'in', ('draft', 'done')), ('semaforo', '=', 'rojo')])
    return [fila(f'cotizacion_bajo_costo:qb.cotizacion:{c.id}', [doc(c, c.display_name)], valor=1,
                 valor_texto=('por vencer el %s' % c.validez_hasta) if c.validez_hasta and c.validez_hasta <= hace(-n) else 'debajo del costo variable',
                 vence=c.validez_hasta, partner=c.partner_id, user=c.create_uid) for c in cots]
```

- [ ] **Step 4: `flake8`, push, CI verde.** Commit — "Señales comerciales".

### Task 2.5: Señales de operaciones

**Files:** `models/senales/operaciones.py`, `tests/test_senales_operaciones.py`

- [ ] **Step 1: Tests** (mrp y stock están en la imagen del CI):

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesOperaciones(SenalesCommon):

    def _op(self, hace):
        prod = self.env['product.product'].create({'name': 'Rollo', 'type': 'consu', 'is_storable': True})
        mo = self.env['mrp.production'].create({'product_id': prod.id, 'product_qty': 1, 'product_uom_id': prod.uom_id.id, 'date_start': self.hace(hace)})
        mo.action_confirm()
        mo.date_start = self.hace(hace)
        return mo

    def test_op_atrasada_y_zombie(self):
        vieja = self._op(100)
        media = self._op(10)
        self._op(-3)
        filas = self.filas('op_atrasada', {'umbrales': {'dias': 7}})
        fv = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % vieja.id)
        fm = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % media.id)
        self.assertEqual(fv['payload']['fecha_base'], str(self.hace(100)))  # senales_actualizar la hará zombie (> 90)
        self.assertEqual(fm['valor'], 10)
        self.assertEqual(len(filas), 2)

    def test_existencia_negativa_por_ubicacion(self):
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        prod = self.env['product.product'].create({'name': 'Hilo', 'type': 'consu', 'is_storable': True})
        self.env['stock.quant'].sudo().create({'product_id': prod.id, 'location_id': wh.lot_stock_id.id, 'quantity': -4})
        f = self.fila_de(self.filas('existencia_negativa'), 'existencia_negativa:stock.location:%d' % wh.lot_stock_id.id)
        self.assertEqual(f['payload']['grupo'], wh.lot_stock_id.complete_name)
        self.assertEqual(f['valor'], 1)
```

- [ ] **Step 2: Correr → fallan.**

- [ ] **Step 3: Implementar `operaciones.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de operaciones (spec §4): producción, componentes, tiempos, existencias, reorden, transferencias, familias, mantenimiento."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar


@senal('op_atrasada')
def op_atrasada(env, cfg):
    n = umbral(cfg, 'dias', 7)
    mos = env['mrp.production'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('confirmed', 'progress')), ('date_start', '<', hace(n))], order='date_start')
    return [fila(f'op_atrasada:mrp.production:{m.id}', [doc(m, m.name, producto=m.product_id.display_name, cantidad=m.product_qty, inicio=str(m.date_start.date()))],
                 valor=dias(m.date_start), valor_texto=f'{m.product_id.display_name}: debía iniciar hace {dias(m.date_start)} días ({m.state})',
                 vence=m.date_start.date(), user=m.user_id, payload={'fecha_base': str(m.date_start.date()), 'estado': m.state, 'origen': m.origin}) for m in mos]


@senal('op_sin_componentes')
def op_sin_componentes(env, cfg):
    n = umbral(cfg, 'dias', 7)
    mos = env['mrp.production'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'confirmed'),
                                                ('date_start', '>=', hoy()), ('date_start', '<', hace(-n)), ('reservation_state', '!=', 'assigned')])
    return [fila(f'op_sin_componentes:mrp.production:{m.id}', [doc(m, m.name, producto=m.product_id.display_name, inicio=str(m.date_start.date()))],
                 valor=1, valor_texto=f'{m.product_id.display_name}: inicia el {m.date_start.date()} y los componentes no están reservados ({m.reservation_state})',
                 vence=m.date_start.date(), user=m.user_id) for m in mos]


@senal('tiempos_excepcion')
def tiempos_excepcion(env, cfg):
    if not tiene_modelo(env, 'qb.workorder.excepcion'):
        return None
    n = umbral(cfg, 'dias', 7)
    xs = env['qb.workorder.excepcion'].sudo().search([('semana', '>=', hace(n))])
    return [fila(f'tiempos_excepcion:mrp.workorder:{x.workorder_id.id}', [doc(x.workorder_id, x.workorder_id.display_name, horas=x.horas, rendimiento=x.rendimiento)],
                 valor=abs(x.horas_desviadas or 0), valor_texto=f'{x.tipo}: {x.workcenter_id.name} · {x.product_id.display_name}, {x.horas:.1f} h registradas',
                 payload={'grupo': x.tipo, 'semana': str(x.semana)}) for x in xs]


@senal('existencia_negativa')
def existencia_negativa(env, cfg):
    quants = env['stock.quant'].sudo().search([('company_id', 'in', companias(env)), ('location_id.usage', '=', 'internal'), ('quantity', '<', 0)])
    filas = []
    for loc, qs in agrupar(quants, lambda q: q.location_id).items():
        filas.append(fila(f'existencia_negativa:stock.location:{loc.id}', [doc(q.product_id, q.product_id.display_name, cantidad=q.quantity) for q in qs[:30]],
                          valor=len(qs), valor_texto=f'{len(qs)} productos en negativo en {loc.complete_name}',
                          payload={'grupo': loc.complete_name, 'total_negativo': round(sum(q.quantity for q in qs), 2)}))
    return filas


@senal('reorden_pendiente')
def reorden_pendiente(env, cfg):
    ops = env['stock.warehouse.orderpoint'].sudo().search([('company_id', 'in', companias(env)), ('trigger', '=', 'auto')])
    ops = ops.filtered(lambda o: o.qty_to_order > 0)  # qty_to_order es computado: filtrar en Python
    return [fila(f'reorden_pendiente:stock.warehouse.orderpoint:{o.id}', [doc(o.product_id, o.product_id.display_name, a_pedir=o.qty_to_order, minimo=o.product_min_qty)],
                 valor=o.qty_to_order, valor_texto=f'{o.product_id.display_name}: pedir {o.qty_to_order:g} ({o.warehouse_id.name})') for o in ops]


@senal('transferencia_atorada')
def transferencia_atorada(env, cfg):
    n = umbral(cfg, 'dias', 7)
    picks = env['stock.picking'].sudo().search([('company_id', 'in', companias(env)), ('picking_type_code', 'in', ('internal', 'mrp_operation')),
                                                ('state', 'in', ('confirmed', 'waiting')), ('scheduled_date', '<', hace(n))], order='scheduled_date')
    return [fila(f'transferencia_atorada:stock.picking:{p.id}', [doc(p, p.name, origen=p.origin, fecha=str(p.scheduled_date.date()))],
                 valor=dias(p.scheduled_date), valor_texto=f'{p.picking_type_id.name}: {dias(p.scheduled_date)} días en {p.state}',
                 user=p.user_id, payload={'grupo': p.picking_type_id.name, 'fecha_base': str(p.scheduled_date.date())}) for p in picks]


@senal('familia_saturada')
def familia_saturada(env, cfg):
    if not tiene_modelo(env, 'qb.familia.carga'):
        return None
    pct = umbral(cfg, 'pct', 90)
    rows = env['qb.familia.carga'].sudo().search([]).filtered(lambda r: (r.utilization_pct or 0) >= pct)
    return [fila(f'familia_saturada:qb.costeo.familia:{r.familia_id.id}', [doc(r.familia_id, r.familia_id.name, utilizacion=round(r.utilization_pct, 1), capacidad=r.capacity_month_units, carga=r.load_month_units)],
                 valor=round(r.utilization_pct, 1), valor_texto=f'{r.familia_id.name}: {r.utilization_pct:.0f} % de utilización') for r in rows]


@senal('mantenimiento_abierto')
def mantenimiento_abierto(env, cfg):
    if not tiene_modelo(env, 'maintenance.request'):
        return None
    n = umbral(cfg, 'dias', 7)
    Req = env['maintenance.request'].sudo()
    reqs = Req.search([('company_id', 'in', companias(env) + [False]), ('stage_id.done', '=', False), ('archive', '=', False),
                       '|', ('request_date', '<', hace(n)), '&', ('maintenance_type', '=', 'preventive'), ('schedule_date', '<', hoy())])
    return [fila(f'mantenimiento_abierto:maintenance.request:{r.id}', [doc(r, r.name, equipo=r.equipment_id.name, etapa=r.stage_id.name)],
                 valor=dias(r.request_date), valor_texto=f'{r.equipment_id.name or "sin equipo"}: {r.maintenance_type} en {r.stage_id.name} desde hace {dias(r.request_date)} días',
                 vence=r.schedule_date.date() if r.schedule_date else None, user=r.user_id, payload={'fecha_base': str(r.request_date)}) for r in reqs]
```

- [ ] **Step 4: `flake8`, push, CI verde.** Commit — "Señales de operaciones".

### Task 2.6: Señales de compras

**Files:** `models/senales/compras.py`, `tests/test_senales_compras.py`

- [ ] **Step 1: Tests**

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesCompras(SenalesCommon):

    def _oc(self, hace_aprobada, ref=False):
        prod = self.env['product.product'].create({'name': 'Colorante', 'type': 'consu'})
        po = self.env['purchase.order'].create({'partner_id': self.proveedor.id, 'partner_ref': ref,
                                                'order_line': [(0, 0, {'product_id': prod.id, 'product_qty': 1, 'price_unit': 10})]})
        po.button_confirm()
        self.env.cr.execute('UPDATE purchase_order SET date_approve = %s WHERE id = %s', (self.hace(hace_aprobada), po.id))
        po.invalidate_recordset(['date_approve'])
        return po

    def test_oc_sin_confirmacion(self):
        po = self._oc(8)
        self._oc(8, ref='ACK-1')  # con acuse del proveedor
        self._oc(1)               # reciente
        f = self.fila_de(self.filas('oc_sin_confirmacion', {'umbrales': {'dias': 5}}), 'oc_sin_confirmacion:partner:%d' % self.proveedor.id)
        self.assertEqual([d['id'] for d in f['documentos']], [po.id])

    def test_actividad_vencida_oc_por_usuario(self):
        po = self._oc(1)
        act = self.env['mail.activity'].create({'res_model_id': self.env['ir.model']._get_id('purchase.order'), 'res_id': po.id,
                                                'activity_type_id': self.env.ref('mail.mail_activity_data_todo').id,
                                                'user_id': self.env.user.id, 'date_deadline': self.hace(2), 'summary': 'Llamar'})
        f = self.fila_de(self.filas('actividad_vencida_oc'), 'actividad_vencida_oc:user:%d' % self.env.user.id)
        self.assertIn(act.id, [d['id'] for d in f['documentos']])
```

- [ ] **Step 2: Correr → fallan.**

- [ ] **Step 3: Implementar `compras.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de compras (spec §4): recepciones, órdenes sin acuse, aprobaciones, precios, actividades, proveedores reprobados."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar
from .comercial import _pickings, _por_contraparte


@senal('recepcion_vencida')
def recepcion_vencida(env, cfg):
    return _por_contraparte(_pickings(env, 'incoming'), 'recepcion_vencida', sale=False)


@senal('oc_sin_confirmacion')
def oc_sin_confirmacion(env, cfg):
    n = umbral(cfg, 'dias', 5)
    pos = env['purchase.order'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'purchase'),
                                               ('date_approve', '<', hace(n)), ('partner_ref', '=', False), ('effective_date', '=', False)])
    pos = pos.filtered(lambda p: not any(pk.state in ('assigned', 'done') for pk in p.picking_ids))
    filas = []
    for partner, ps in agrupar(pos, lambda p: p.partner_id.commercial_partner_id).items():
        filas.append(fila(f'oc_sin_confirmacion:partner:{partner.id}', [doc(p, p.name, monto=p.amount_total, aprobada=str(p.date_approve.date())) for p in ps[:30]],
                          valor=len(ps), valor_texto=f'{len(ps)} órdenes sin acuse del proveedor, la más vieja {max(dias(p.date_approve) for p in ps)} días',
                          partner=partner, user=next((p.user_id for p in ps if p.user_id), None),
                          payload={'fecha_base': str(min(p.date_approve for p in ps).date())}))
    return filas


@senal('aprobacion_pendiente')
def aprobacion_pendiente(env, cfg):
    n = umbral(cfg, 'dias', 2)
    filas = []
    if tiene_modelo(env, 'approval.request'):
        reqs = env['approval.request'].sudo().search([('request_status', 'in', ('new', 'pending')), ('create_date', '<', hace(n))])
        reqs = reqs.filtered(lambda r: 'rh' not in (r.category_id.name or '').lower())  # las de RH van en aprobacion_rh
        for r in reqs:
            aprobador = next((a.user_id for a in r.approver_ids if a.status == 'pending'), None)
            filas.append(fila(f'aprobacion_pendiente:approval.request:{r.id}', [doc(r, r.name, categoria=r.category_id.name, solicita=r.request_owner_id.name)],
                              valor=dias(r.create_date), valor_texto=f'{r.category_id.name}: {r.name} lleva {dias(r.create_date)} días', user=aprobador or r.request_owner_id,
                              payload={'fecha_base': str(r.create_date.date())}))
    if tiene_modelo(env, 'purchase.requisition'):
        for r in env['purchase.requisition'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('in_progress', 'open', 'confirmed'))]):
            filas.append(fila(f'aprobacion_pendiente:purchase.requisition:{r.id}', [doc(r, r.name)], valor=dias(r.create_date),
                              valor_texto=f'acuerdo de compra {r.name} en {r.state}', user=r.user_id, payload={'fecha_base': str(r.create_date.date())}))
    if not tiene_modelo(env, 'approval.request') and not tiene_modelo(env, 'purchase.requisition'):
        return None
    return filas


@senal('precio_compra_subio')
def precio_compra_subio(env, cfg):
    pct, meses, n = umbral(cfg, 'pct', 15), umbral(cfg, 'meses', 6), umbral(cfg, 'dias', 30)
    Line = env['purchase.order.line'].sudo()
    recientes = Line.search([('company_id', 'in', companias(env)), ('state', 'in', ('purchase', 'done')), ('date_approve', '>=', hace(n)), ('price_unit', '>', 0)])
    filas = []
    for product, ls in agrupar(recientes, lambda l: l.product_id).items():
        prev = Line.read_group([('company_id', 'in', companias(env)), ('state', 'in', ('purchase', 'done')), ('product_id', '=', product.id),
                                ('date_approve', '<', hace(n)), ('date_approve', '>=', hace(n + 30 * meses)), ('price_unit', '>', 0)],
                               ['price_unit:avg'], [])
        prom = prev and prev[0].get('price_unit')
        if not prom:
            continue
        ultima = max(ls, key=lambda l: l.date_approve)
        subida = (ultima.price_unit - prom) / prom * 100
        if subida >= pct:
            filas.append(fila(f'precio_compra_subio:product:{product.id}', [doc(ultima.order_id, ultima.order_id.name, precio=ultima.price_unit, promedio=round(prom, 2))],
                              valor=round(subida, 1), valor_texto=f'{product.display_name}: {ultima.price_unit:.2f} vs promedio {prom:.2f} ({subida:+.0f} %)',
                              partner=ultima.partner_id, user=ultima.order_id.user_id))
    return filas


@senal('actividad_vencida_oc')
def actividad_vencida_oc(env, cfg):
    acts = env['mail.activity'].sudo().search([('res_model', '=', 'purchase.order'), ('date_deadline', '<', hoy())], order='date_deadline')
    filas = []
    for user, xs in agrupar(acts, lambda a: a.user_id).items():
        filas.append(fila(f'actividad_vencida_oc:user:{user.id}', [doc(a, f'{a.summary or a.activity_type_id.name} · {a.res_name}'[:120], vence=str(a.date_deadline)) for a in xs[:30]],
                          valor=len(xs), valor_texto=f'{len(xs)} actividades vencidas sobre compras, la más vieja {dias(xs[0].date_deadline)} días', user=user,
                          payload={'fecha_base': str(xs[0].date_deadline)}))
    return filas


@senal('proveedor_reprobado')
def proveedor_reprobado(env, cfg):
    if not tiene_modelo(env, 'sgi.supplier.eval'):
        return None
    minimo = umbral(cfg, 'score', 70)
    Eval = env['sgi.supplier.eval'].sudo()
    # Campo del proveedor y de la fecha: confírmalos con `grep -n "fields\." addons/quimibond_sgi/models/sgi_supplier_eval.py | head -20` (se esperan partner_id y date).
    vistos, filas = set(), []
    for ev in Eval.search([], order='date desc, id desc'):
        pid = ev.partner_id.id
        if pid in vistos:
            continue
        vistos.add(pid)
        if (ev.score or 0) < minimo:
            filas.append(fila(f'proveedor_reprobado:partner:{ev.partner_id.commercial_partner_id.id}', [doc(ev, ev.display_name, score=ev.score)],
                              valor=ev.score, valor_texto=f'{ev.partner_id.name}: calificación {ev.score:.0f} (< {minimo})', partner=ev.partner_id))
    return filas
```

- [ ] **Step 4: `flake8`, push, CI verde.** Commit — "Señales de compras".

### Task 2.7: Señales de calidad / SGI

Todas dependen de `quimibond_sgi` (Enterprise; no está en el CI). Cada función empieza con `if not tiene_modelo(...): return None`. **Antes de escribir cada consulta, confirma los campos** con `grep -n "fields\." addons/quimibond_sgi/models/<archivo>.py` (verificado 2026-09-18: `sgi.action.line(state, responsible_id, date_commit)`, `sgi.legal.requirement(compliance_state, next_eval_date)`, `sgi.risk(attention_level, state, process_id, sgi_area_id)`, `sgi.ppap(state)`, `sgi.audit.program.line(state, planned_month, lead_auditor_id, program_id)`, `sgi.alert.source(code, name, enabled, suppressed_count)`, `maintenance.equipment(sgi_calibration_state, sgi_do_not_use, sgi_next_calibration_date)`, `sgi.indicator.measure(indicator_id, period_date, value, semaphore, state)`).

**Files:** `models/senales/calidad_sgi.py`, `tests/test_senales_calidad.py`

- [ ] **Step 1: Test** (solo lo que Community puede probar):

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesCalidad(SenalesCommon):

    def test_sgi_no_instalado_devuelve_none(self):
        if 'sgi.risk' in self.env:
            self.skipTest('SGI instalado')
        for s in ('indicador_rojo', 'accion_correctiva_vencida', 'calibracion_vencida', 'legal_incumplido', 'riesgo_sin_tratar', 'ppap_rechazado', 'auditoria_pendiente', 'fuente_sgi_apagada'):
            self.assertIsNone(self.filas(s), s)

    def test_nc_abierta_sin_quality_devuelve_none(self):
        if 'quality.alert' not in self.env:
            self.assertIsNone(self.filas('nc_abierta'))
```

- [ ] **Step 2: Implementar `calidad_sgi.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de calidad / SGI (spec §4). Todas viven en quimibond_sgi (Enterprise): None si no está instalado."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila
from .finanzas import _medidas_rojas


@senal('indicador_rojo')
def indicador_rojo(env, cfg):
    ms = _medidas_rojas(env, cfg)
    if ms is None:
        return None
    return [fila(f'indicador_rojo:sgi.indicator:{m.indicator_id.id}', [doc(m, f'{m.indicator_id.name} {m.period_date}', valor=m.value, objetivo=m.target_objective)],
                 valor=m.value, valor_texto=f'{m.indicator_id.name}: {m.value} {m.uom or ""} (objetivo {m.target_objective})', vence=m.period_date,
                 payload={'grupo': m.indicator_id.name, 'periodo': str(m.period_date)}) for m in ms]


@senal('accion_correctiva_vencida')
def accion_correctiva_vencida(env, cfg):
    if not tiene_modelo(env, 'sgi.action.line'):
        return None
    xs = env['sgi.action.line'].sudo().search([('state', '=', 'vencida')], order='date_commit')
    return [fila(f'accion_correctiva_vencida:sgi.action.line:{x.id}', [doc(x, x.display_name, compromiso=str(x.date_commit))], valor=dias(x.date_commit),
                 valor_texto=f'{x.display_name}: vencida hace {dias(x.date_commit)} días', vence=x.date_commit, user=x.responsible_id,
                 payload={'fecha_base': str(x.date_commit)}) for x in xs]


@senal('nc_abierta')
def nc_abierta(env, cfg):
    if not tiene_modelo(env, 'quality.alert'):
        return None
    n = umbral(cfg, 'dias', 7)
    alerts = env['quality.alert'].sudo().search([('company_id', 'in', companias(env)), ('stage_id.done', '=', False), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'nc_abierta:quality.alert:{a.id}', [doc(a, a.display_name, etapa=a.stage_id.name)], valor=dias(a.create_date),
                 valor_texto=f'{a.display_name}: {dias(a.create_date)} días en {a.stage_id.name}', partner=a.partner_id, user=a.user_id,
                 payload={'fecha_base': str(a.create_date.date())}) for a in alerts]


@senal('calibracion_vencida')
def calibracion_vencida(env, cfg):
    Eq = env['maintenance.equipment'].sudo() if tiene_modelo(env, 'maintenance.equipment') else None
    if Eq is None or 'sgi_calibration_state' not in Eq._fields:
        return None
    eqs = Eq.search(['|', ('sgi_calibration_state', '=', 'vencido'), ('sgi_do_not_use', '=', True)])
    return [fila(f'calibracion_vencida:maintenance.equipment:{e.id}', [doc(e, e.name, proxima=str(e.sgi_next_calibration_date or ''))],
                 valor=dias(e.sgi_next_calibration_date) or 0, valor_texto=('NO USAR; ' if e.sgi_do_not_use else '') + f'calibración {e.sgi_calibration_state}',
                 vence=e.sgi_next_calibration_date, user=e.technician_user_id) for e in eqs]


@senal('legal_incumplido')
def legal_incumplido(env, cfg):
    if not tiene_modelo(env, 'sgi.legal.requirement'):
        return None
    xs = env['sgi.legal.requirement'].sudo().search(['|', ('compliance_state', 'in', ('no_cumple', 'parcial')), ('next_eval_date', '<=', hoy())])
    return [fila(f'legal_incumplido:sgi.legal.requirement:{x.id}', [doc(x, x.display_name, estado=x.compliance_state)], valor=1,
                 valor_texto=f'{x.display_name}: {x.compliance_state}' + (f', evaluación vencida el {x.next_eval_date}' if x.next_eval_date and x.next_eval_date <= hoy() else ''),
                 vence=x.next_eval_date, user=getattr(x, 'responsible_id', None)) for x in xs]


@senal('riesgo_sin_tratar')
def riesgo_sin_tratar(env, cfg):
    if not tiene_modelo(env, 'sgi.risk'):
        return None
    xs = env['sgi.risk'].sudo().search([('attention_level', 'in', ('inmediata', 'alto')), ('state', '=', 'identificado')])
    return [fila(f'riesgo_sin_tratar:sgi.risk:{x.id}', [doc(x, x.display_name, nivel=x.attention_level, score=x.score)], valor=x.score or 0,
                 valor_texto=f'{x.display_name}: atención {x.attention_level}, sin tratamiento', user=getattr(x, 'responsible_id', None),
                 payload={'grupo': x.process_id.name if x.process_id else 'sin proceso'}) for x in xs]


@senal('ppap_rechazado')
def ppap_rechazado(env, cfg):
    if not tiene_modelo(env, 'sgi.ppap'):
        return None
    xs = env['sgi.ppap'].sudo().search([('state', '=', 'rechazado')])
    return [fila(f'ppap_rechazado:sgi.ppap:{x.id}', [doc(x, x.display_name)], valor=1, valor_texto=f'{x.display_name}: rechazado',
                 partner=getattr(x, 'partner_id', None), user=getattr(x, 'responsible_id', None)) for x in xs]


@senal('auditoria_pendiente')
def auditoria_pendiente(env, cfg):
    if not tiene_modelo(env, 'sgi.audit.program.line'):
        return None
    xs = env['sgi.audit.program.line'].sudo().search([('state', '=', 'pendiente')])
    xs = xs.filtered(lambda x: x.planned_month and int(x.planned_month) < hoy().month and (not getattr(x.program_id, 'year', None) or int(x.program_id.year) <= hoy().year))
    return [fila(f'auditoria_pendiente:sgi.audit.program.line:{x.id}', [doc(x, x.display_name, mes=x.planned_month)], valor=hoy().month - int(x.planned_month),
                 valor_texto=f'{x.display_name}: planeada para el mes {x.planned_month}, sigue pendiente', user=x.lead_auditor_id) for x in xs]


@senal('fuente_sgi_apagada')
def fuente_sgi_apagada(env, cfg):
    if not tiene_modelo(env, 'sgi.alert.source'):
        return None
    xs = env['sgi.alert.source'].sudo().search([('enabled', '=', False), ('suppressed_count', '>', 0)])
    return [fila(f'fuente_sgi_apagada:sgi.alert.source:{x.id}', [doc(x, x.name, codigo=x.code, omitidas=x.suppressed_count)], valor=x.suppressed_count,
                 valor_texto=f'{x.name}: apagada, {x.suppressed_count} alertas omitidas') for x in xs]
```

- [ ] **Step 3: `flake8`, push, CI verde.** Commit — "Señales de calidad / SGI".

### Task 2.8: Señales de RH y sistemas

**Files:** `models/senales/rh.py`, `models/senales/sistemas.py`, `tests/test_senales_rh_sistemas.py`

- [ ] **Step 1: Test**

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesRhSistemas(SenalesCommon):

    def test_enterprise_no_instalado_devuelve_none(self):
        for s, modelos in (('aprobacion_rh', ('approval.request', 'hr.leave')), ('evaluacion_vencida', ('hr.appraisal', 'sgi.competence.gap')), ('ticket_abierto', ('helpdesk.ticket',))):
            if not any(m in self.env for m in modelos):
                self.assertIsNone(self.filas(s), s)
```

- [ ] **Step 2: Implementar**

`rh.py`:

```python
# -*- coding: utf-8 -*-
"""Señales de RH (spec §4): aprobaciones, evaluaciones. Los pendientes de correo de RH salen de la memoria."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, doc, fila


@senal('aprobacion_rh')
def aprobacion_rh(env, cfg):
    n = umbral(cfg, 'dias', 2)
    filas, alguno = [], False
    if tiene_modelo(env, 'approval.request'):
        alguno = True
        reqs = env['approval.request'].sudo().search([('request_status', 'in', ('new', 'pending')), ('create_date', '<', hace(n))])
        for r in reqs.filtered(lambda r: 'rh' in (r.category_id.name or '').lower()):
            aprobador = next((a.user_id for a in r.approver_ids if a.status == 'pending'), None)
            filas.append(fila(f'aprobacion_rh:approval.request:{r.id}', [doc(r, r.name, categoria=r.category_id.name)], valor=dias(r.create_date),
                              valor_texto=f'{r.category_id.name}: {r.name} lleva {dias(r.create_date)} días', user=aprobador or r.request_owner_id,
                              payload={'fecha_base': str(r.create_date.date())}))
    if tiene_modelo(env, 'hr.leave'):
        alguno = True
        for l in env['hr.leave'].sudo().search([('state', 'in', ('confirm', 'validate1')), ('create_date', '<', hace(n))]):
            aprobador = l.employee_id.leave_manager_id or l.employee_id.parent_id.user_id
            filas.append(fila(f'aprobacion_rh:hr.leave:{l.id}', [doc(l, l.display_name, empleado=l.employee_id.name, desde=str(l.date_from.date()))], valor=dias(l.create_date),
                              valor_texto=f'permiso de {l.employee_id.name} ({l.holiday_status_id.name}) por aprobar desde hace {dias(l.create_date)} días', vence=l.date_from.date(),
                              user=aprobador, payload={'fecha_base': str(l.create_date.date())}))
    return filas if alguno else None


@senal('evaluacion_vencida')
def evaluacion_vencida(env, cfg):
    filas, alguno = [], False
    if tiene_modelo(env, 'hr.appraisal'):
        alguno = True
        for a in env['hr.appraisal'].sudo().search([('state', 'in', ('new', 'pending')), ('date_close', '<', hoy())]):
            filas.append(fila(f'evaluacion_vencida:hr.appraisal:{a.id}', [doc(a, a.display_name, cierre=str(a.date_close))], valor=dias(a.date_close),
                              valor_texto=f'evaluación de {a.employee_id.name} vencida hace {dias(a.date_close)} días', vence=a.date_close,
                              user=a.manager_ids[:1].user_id if a.manager_ids else None, payload={'fecha_base': str(a.date_close)}))
    if tiene_modelo(env, 'sgi.competence.gap'):
        alguno = True
        # Brecha abierta = nivel actual por debajo del requerido. Confirma si el modelo tiene `state`; si no, usa el filtro de niveles.
        for g in env['sgi.competence.gap'].sudo().search([]):
            if getattr(g, 'state', None) in (None, 'abierta', 'open') or getattr(g, 'gap', 0) > 0:
                filas.append(fila(f'evaluacion_vencida:sgi.competence.gap:{g.id}', [doc(g, f'{g.employee_id.name} · {g.skill_id.name}')], valor=1,
                                  valor_texto=f'{g.employee_id.name}: {g.skill_id.name} en {g.current_level_id.name or "sin nivel"} (requiere {g.required_level_id.name})',
                                  user=g.employee_id.parent_id.user_id))
    return filas if alguno else None
```

`sistemas.py`:

```python
# -*- coding: utf-8 -*-
"""Señales de sistemas (spec §4): tickets. job_caido la manda el watchdog (Edge Function health), no Odoo."""
from .base import senal, hace, dias, umbral, tiene_modelo, doc, fila


@senal('ticket_abierto')
def ticket_abierto(env, cfg):
    if not tiene_modelo(env, 'helpdesk.ticket'):
        return None
    n = umbral(cfg, 'dias', 7)
    ts = env['helpdesk.ticket'].sudo().search([('stage_id.fold', '=', False), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'ticket_abierto:helpdesk.ticket:{t.id}', [doc(t, t.display_name, etapa=t.stage_id.name, equipo=t.team_id.name)], valor=dias(t.create_date),
                 valor_texto=f'{t.display_name}: {dias(t.create_date)} días en {t.stage_id.name}', partner=t.partner_id, user=t.user_id,
                 payload={'fecha_base': str(t.create_date.date())}) for t in ts]
```

- [ ] **Step 3: `flake8`, push, CI verde.** Commit — "Señales de RH y sistemas".

### Task 2.9: Señales de dirección (actividades, firmas, acuses, acuerdos, obligaciones legado)

**Files:** `models/senales/direccion.py`, `tests/test_senales_direccion.py`

- [ ] **Step 1: Tests**

```python
# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesDireccion(SenalesCommon):

    def _act(self, hace):
        return self.env['mail.activity'].create({'res_model_id': self.env['ir.model']._get_id('res.partner'), 'res_id': self.cliente.id,
                                                 'activity_type_id': self.env.ref('mail.mail_activity_data_todo').id,
                                                 'user_id': self.env.user.id, 'date_deadline': self.hace(hace), 'summary': 'x'})

    def test_carga_actividades_separa_zombis(self):
        a = self._act(5)
        z = self._act(200)
        self._act(-1)
        filas = self.filas('carga_actividades', {'reglas_calidad': {'zombie_dias': 180}})
        viva = self.fila_de(filas, 'carga_actividades:user:%d' % self.env.user.id)
        zombie = self.fila_de(filas, 'carga_actividades:user:%d:zombie' % self.env.user.id)
        self.assertEqual([d['id'] for d in viva['documentos']], [a.id])
        self.assertEqual([d['id'] for d in zombie['documentos']], [z.id])
        self.assertEqual(zombie['payload']['fecha_base'], str(self.hace(200)))
        self.assertEqual(viva['payload']['grupo'], 'vivas')

    def test_obligacion_legado_solo_abiertas(self):
        if 'qb.obligation' not in self.env:
            self.assertIsNone(self.filas('obligacion_legado'))
            return
        ob = self.env['qb.obligation'].create({'name': 'Enviar cotización', 'description': 'Enviar cotización a cliente', 'user_id': self.env.user.id,
                                               'date_deadline': self.hace(1), 'partner_id': self.cliente.id, 'obligation_type': 'other', 'state': 'confirmed'})
        f = self.fila_de(self.filas('obligacion_legado'), 'obligacion_legado:qb.obligation:%d' % ob.id)
        self.assertEqual(f['vence'], str(self.hace(1)))
        ob.write({'state': 'done'})
        self.assertEqual([x for x in self.filas('obligacion_legado') if x['clave'].endswith(':%d' % ob.id)], [])
```

(Si `obligation_type: 'other'` no es un valor válido del Selection de `qb.obligation`, mira `addons/qb_obligation/models/qb_obligation.py:59` y usa uno que exista.)

- [ ] **Step 2: Implementar `direccion.py`**

```python
# -*- coding: utf-8 -*-
"""Señales de dirección (spec §4): actividades vencidas por persona, firmas, acuses, acuerdos, obligaciones de qb_obligation (puente)."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar


@senal('carga_actividades')
def carga_actividades(env, cfg):
    zombie = int(((cfg or {}).get('reglas_calidad') or {}).get('zombie_dias') or 180)
    acts = env['mail.activity'].sudo().search([('date_deadline', '<', hoy()), ('user_id.share', '=', False)], order='date_deadline')
    filas = []
    for user, xs in agrupar(acts, lambda a: a.user_id).items():
        for sufijo, grupo, sub in (('', 'vivas', [a for a in xs if dias(a.date_deadline) <= zombie]), (':zombie', 'zombis', [a for a in xs if dias(a.date_deadline) > zombie])):
            if not sub:
                continue
            filas.append(fila(f'carga_actividades:user:{user.id}{sufijo}',
                              [doc(a, f'{a.summary or a.activity_type_id.name} · {a.res_name or a.res_model}'[:120], modelo_doc=a.res_model, vence=str(a.date_deadline)) for a in sub[:30]],
                              valor=len(sub), valor_texto=f'{user.name}: {len(sub)} actividades vencidas, la más vieja {dias(sub[0].date_deadline)} días', user=user,
                              payload={'grupo': grupo, 'fecha_base': str(sub[0].date_deadline), 'dias_max': dias(sub[0].date_deadline)}))
    return filas


@senal('firma_pendiente')
def firma_pendiente(env, cfg):
    if not tiene_modelo(env, 'sign.request'):
        return None
    n = umbral(cfg, 'dias', 7)
    reqs = env['sign.request'].sudo().search([('state', '=', 'sent'), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'firma_pendiente:sign.request:{r.id}', [doc(r, r.reference or r.display_name, firmantes=', '.join(r.request_item_ids.filtered(lambda i: i.state == 'sent').mapped('partner_id.name'))[:200])],
                 valor=dias(r.create_date), valor_texto=f'{r.reference or r.display_name}: {dias(r.create_date)} días sin firmar', user=r.create_uid,
                 payload={'fecha_base': str(r.create_date.date())}) for r in reqs]


@senal('acuse_documento')
def acuse_documento(env, cfg):
    if not tiene_modelo(env, 'sgi.document.ack'):
        return None
    acks = env['sgi.document.ack'].sudo().search([('state', '=', 'pendiente')])
    filas = []
    for user, xs in agrupar(acks.filtered(lambda a: a.user_id), lambda a: a.user_id).items():
        filas.append(fila(f'acuse_documento:user:{user.id}', [doc(a.document_id, f'{a.sgi_code or ""} {a.document_id.name}'.strip()) for a in xs[:30]],
                          valor=len(xs), valor_texto=f'{user.name}: {len(xs)} documentos sin acusar', user=user))
    return filas


@senal('acuerdo_direccion_vencido')
def acuerdo_direccion_vencido(env, cfg):
    if not tiene_modelo(env, 'sgi.management.review.agreement'):
        return None
    Ag = env['sgi.management.review.agreement'].sudo()
    dom = [('deadline', '<', hoy())]
    if 'state' in Ag._fields:
        dom.append(('state', 'not in', ('done', 'cancel', 'cerrado', 'hecho')))
    xs = Ag.search(dom)
    xs = xs.filtered(lambda x: not x.task_id or x.task_id.state not in ('1_done', '1_canceled'))
    return [fila(f'acuerdo_direccion_vencido:sgi.management.review.agreement:{x.id}', [doc(x, x.name, revision=x.review_id.display_name, limite=str(x.deadline))],
                 valor=dias(x.deadline), valor_texto=f'{x.name}: venció hace {dias(x.deadline)} días', vence=x.deadline, user=x.responsible_id,
                 payload={'fecha_base': str(x.deadline)}) for x in xs]


@senal('obligacion_legado')
def obligacion_legado(env, cfg):
    """Puente hasta retirar qb_obligation (plan B, paso 6). Antes de desinstalarlo hay que mandar un último lote vacío
    (o desactivar la señal en senales_config) para que sus situaciones se resuelvan y no queden sin_datos."""
    if not tiene_modelo(env, 'qb.obligation'):
        return None
    obs = env['qb.obligation'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('candidate', 'confirmed'))], order='date_deadline')
    return [fila(f'obligacion_legado:qb.obligation:{o.id}', [doc(o, o.name, descripcion=(o.description or '')[:300], documento=f'{o.res_model},{o.res_id}' if o.res_model else None)],
                 valor=dias(o.date_deadline) if o.date_deadline and o.date_deadline < hoy() else 0,
                 valor_texto=f'{o.name} ({o.state}); vence {o.date_deadline}', vence=o.date_deadline, partner=o.partner_id, user=o.user_id,
                 payload={'estado': o.state, 'tipo': o.obligation_type, 'area': o.area, 'origen': o.source, 'source_ref': o.source_ref}) for o in obs]
```

- [ ] **Step 3: `flake8`, push, CI verde**, y ahora `test_registro_tiene_las_senales_de_odoo` pasa. Commit — "Señales de dirección y puente de qb_obligation".

### Task 2.10: El push (`_push_senales`)

**Files:**
- Create: `addons/quimibond_intelligence/models/sync_push_senales.py`
- Modify: `addons/quimibond_intelligence/models/sync_push.py` (`PUSH_MODELS`, `PUSH_MODELS_DEFAULT`, `FULL_PUSH_METHODS`, lista `methods` en `push_to_supabase`)
- Modify: `addons/quimibond_intelligence/models/__init__.py` (+ `sync_push_senales`)
- Create: `addons/quimibond_intelligence/tests/test_push_senales.py`

Contrato (spec §4 regla 2): lee `senales_config` (un `fetch`), respeta `cada_horas` con el parámetro `quimibond_intelligence.senales_last_run` (json `{senal: iso}`), genera un `uuid` de corrida, por cada señal en turno corre su función dentro de un savepoint y manda el lote con `rpc_strict('senales_ingestar')`; un error en una señal no detiene las demás pero deja el método en error (fila propia en el Historial de Sync + excepción al final para que `_run_push` escriba `status='error'` en `pipeline_logs`); al terminar llama `senales_push_terminado(corrida)`. Un lote solo se manda si la consulta terminó bien; `None` = no se manda.

- [ ] **Step 1: Tests** (cliente falso, sin red):

```python
# -*- coding: utf-8 -*-
import json

from odoo.tests import TransactionCase, tagged

from ..models.senales import base
from ..models.supabase_client import SupabaseError


class ClienteFalso:
    def __init__(self, config, fallar_en=()):
        self.config, self.fallar_en, self.calls, self.logs = config, set(fallar_en), [], []

    def fetch(self, table, params=None):
        assert table == 'senales_config'
        return self.config

    def rpc_strict(self, fn, params, timeout=120.0):
        self.calls.append((fn, params))
        if fn == 'senales_ingestar' and params['p_senal'] in self.fallar_en:
            raise SupabaseError('HTTP 500 simulado')
        return {'ok': True, 'nuevas': len(params.get('p_filas', []))} if fn == 'senales_ingestar' else {'ok': True}

    def insert(self, table, rows, batch_size=200):
        self.logs.extend(rows)
        return len(rows)

    def close(self):
        pass


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestPushSenales(TransactionCase):

    def setUp(self):
        super().setUp()
        self.ICP = self.env['ir.config_parameter'].sudo()
        self.ICP.set_param('quimibond_intelligence.senales_last_run', '')
        self.sync = self.env['quimibond.sync']
        # Señales de prueba en el registro (se limpian al final).
        self._orig = dict(base.REGISTRO)
        base.REGISTRO.clear()
        base.REGISTRO['_a'] = lambda env, cfg: [base.fila('_a:1', [], valor=1)]
        base.REGISTRO['_b'] = lambda env, cfg: []
        base.REGISTRO['_none'] = lambda env, cfg: None
        base.REGISTRO['_boom'] = lambda env, cfg: 1 / 0
        self.addCleanup(lambda: (base.REGISTRO.clear(), base.REGISTRO.update(self._orig)))
        self.config = [{'senal': s, 'umbrales': {}, 'cada_horas': 1, 'reglas_calidad': {}} for s in ('_a', '_b', '_none', '_boom', '_sin_consulta')]

    def test_manda_un_lote_por_senal_y_termina(self):
        c = ClienteFalso([x for x in self.config if x['senal'] in ('_a', '_b', '_none')])
        n = self.sync._push_senales(c)
        ingestas = [p for f, p in c.calls if f == 'senales_ingestar']
        self.assertEqual([p['p_senal'] for p in ingestas], ['_a', '_b'])          # _none no manda lote
        self.assertEqual(ingestas[0]['p_filas'][0]['clave'], '_a:1')
        self.assertEqual({p['p_corrida'] for p in ingestas}, {c.calls[-1][1]['p_corrida']})  # misma corrida
        self.assertEqual(c.calls[-1][0], 'senales_push_terminado')
        self.assertEqual(n, 1)
        last = json.loads(self.ICP.get_param('quimibond_intelligence.senales_last_run'))
        self.assertEqual(set(last), {'_a', '_b'})

    def test_respeta_turno(self):
        c = ClienteFalso([{'senal': '_a', 'umbrales': {}, 'cada_horas': 6, 'reglas_calidad': {}}])
        self.sync._push_senales(c)
        self.sync._push_senales(c)
        self.assertEqual(len([1 for f, _ in c.calls if f == 'senales_ingestar']), 1, 'fuera de turno no se manda lote')

    def test_error_en_una_senal_no_detiene_las_demas_pero_deja_error(self):
        c = ClienteFalso([x for x in self.config if x['senal'] in ('_a', '_boom', '_b')], fallar_en=('_a',))
        with self.assertRaises(SupabaseError):
            self.sync._push_senales(c)
        self.assertEqual([p['p_senal'] for f, p in c.calls if f == 'senales_ingestar'], ['_a', '_b'])  # _boom no manda; _b sí
        self.assertEqual(c.calls[-1][0], 'senales_push_terminado')
        log = self.env['quimibond.sync.log'].search([('name', '=', 'Push señales con errores')], limit=1)
        self.assertTrue(log and '_a' in log.summary and '_boom' in log.summary)
        last = json.loads(self.ICP.get_param('quimibond_intelligence.senales_last_run'))
        self.assertNotIn('_a', last, 'la fallida se reintenta en el siguiente push')

    def test_senal_sin_consulta_se_ignora_con_aviso(self):
        c = ClienteFalso([x for x in self.config if x['senal'] == '_sin_consulta'])
        self.assertEqual(self.sync._push_senales(c), 0)
        self.assertEqual([f for f, _ in c.calls], ['senales_push_terminado'])

    def test_push_to_supabase_incluye_senales(self):
        self.assertIn('senales', self.sync.PUSH_MODELS)
        self.assertIn('senales', self.sync.FULL_PUSH_METHODS)
        self.assertIn('senales', self.sync._push_models_allowed())
```

- [ ] **Step 2: Correr → fallan (`AttributeError: _push_senales`).**

- [ ] **Step 3: Implementar `sync_push_senales.py`**

```python
# -*- coding: utf-8 -*-
"""Push de señales Odoo → Supabase (`senales`), spec §4 regla 2.

Una llamada a senales_ingestar por señal con la lista COMPLETA de claves
activas; Supabase resuelve lo que no viene. Turnos por cada_horas en el
parámetro quimibond_intelligence.senales_last_run. rpc_strict: un lote
solo cuenta si Supabase respondió bien; cualquier error deja el método en
error en el Historial de Sync (y en pipeline_logs vía _run_push).
"""
import json
import logging
import time
import uuid
from datetime import datetime, timedelta

from odoo import models

from .senales.base import REGISTRO
from .supabase_client import SupabaseError

_logger = logging.getLogger(__name__)

PARAM_LAST_RUN = 'quimibond_intelligence.senales_last_run'
HOLGURA = timedelta(minutes=5)  # el cron horario no cae exacto: 55 min cuentan como 1 h


class QuimibondSyncSenales(models.TransientModel):
    _inherit = 'quimibond.sync'

    def _push_senales(self, client, last_sync=None) -> int:
        ICP = self.env['ir.config_parameter'].sudo()
        config = client.fetch('senales_config', {'fuente': 'eq.odoo', 'activa': 'eq.true',
                                                 'select': 'senal,umbrales,reglas_calidad,cada_horas,agrupar_por', 'order': 'senal'})
        if not config:
            raise SupabaseError('senales_config vacío o inaccesible: no se manda ningún lote')
        try:
            last_run = json.loads(ICP.get_param(PARAM_LAST_RUN) or '{}')
        except ValueError:
            last_run = {}
        corrida = str(uuid.uuid4())
        ahora = datetime.utcnow()
        total, errores, detalle = 0, [], {}

        for cfg in config:
            nombre = cfg['senal']
            fn = REGISTRO.get(nombre)
            if fn is None:
                _logger.info('senal %s: sin consulta en Odoo (¿plan B o fuente=memoria?)', nombre)
                continue
            prev = last_run.get(nombre)
            if prev:
                try:
                    if ahora - datetime.fromisoformat(prev) < timedelta(hours=int(cfg.get('cada_horas') or 1)) - HOLGURA:
                        detalle[nombre] = 'fuera de turno'
                        continue
                except ValueError:
                    pass
            t0 = time.monotonic()
            try:
                with self.env.cr.savepoint():
                    filas = fn(self.env, cfg)
                if filas is None:
                    detalle[nombre] = 'no aplica'
                    continue
                res = client.rpc_strict('senales_ingestar', {'p_senal': nombre, 'p_fuente': 'odoo', 'p_corrida': corrida, 'p_filas': filas})
                if not isinstance(res, dict) or not res.get('ok'):
                    raise SupabaseError('senales_ingestar %s: %s' % (nombre, (res or {}).get('error') if isinstance(res, dict) else res))
                total += len(filas)
                last_run[nombre] = ahora.isoformat(timespec='seconds')
                detalle[nombre] = {'n': len(filas), 'nuevas': res.get('nuevas'), 'resueltas': res.get('resueltas'), 's': round(time.monotonic() - t0, 1)}
            except Exception as exc:  # noqa: BLE001 — una señal no tumba a las demás
                _logger.exception('senal %s falló', nombre)
                errores.append('%s: %s' % (nombre, str(exc)[:200]))
                detalle[nombre] = {'error': str(exc)[:200], 's': round(time.monotonic() - t0, 1)}

        ICP.set_param(PARAM_LAST_RUN, json.dumps(last_run))
        try:
            client.rpc_strict('senales_push_terminado', {'p_corrida': corrida, 'p_origen': 'odoo'})
        except Exception as exc:  # noqa: BLE001
            errores.append('push_terminado: %s' % str(exc)[:200])

        try:
            client.insert('pipeline_logs', [{'level': 'error' if errores else 'info', 'phase': 'odoo_push_senales',
                                             'message': '[senales] %d filas en %d señales, %d errores' % (total, len([d for d in detalle.values() if isinstance(d, dict) and 'n' in d]), len(errores)),
                                             'details': {'corrida': corrida, 'senales': detalle, 'errores': errores}}])
        except Exception as exc:  # noqa: BLE001
            _logger.warning('log de señales: %s', exc)

        if errores:
            self.env['quimibond.sync.log'].sudo().create({
                'name': 'Push señales con errores', 'direction': 'push', 'status': 'error',
                'summary': '\n'.join(errores)[:2000],
            })
            raise SupabaseError('%d señal(es) fallaron: %s' % (len(errores), '; '.join(errores)[:500]))
        return total
```

En `sync_push.py`:

```python
    PUSH_MODELS = ('contacts', 'users', 'senales')
    PUSH_MODELS_DEFAULT = 'contacts,users,senales'
    FULL_PUSH_METHODS = frozenset(['users', 'senales'])
```
y en `push_to_supabase`: `methods = [('contacts', self._push_contacts), ('users', self._push_users), ('senales', self._push_senales)]`. Actualiza el docstring de `_push_models_allowed` ("'all' = contacts, users y senales"). En `models/__init__.py` agrega `from . import sync_push_senales` después de `sync_push_partners`.

- [ ] **Step 4: `flake8`, push, CI verde.** Commit — "quimibond_intelligence: _push_senales (lotes por señal, turnos, errores visibles)".

### Task 2.11: `buzon_personas` en `_push_users`

**Files:** `models/sync_push_partners.py`, `tests/test_push_senales.py` (+1 test)

- [ ] **Step 1: Test** — agrega a `TestPushSenales`:

```python
    def test_push_users_manda_buzon_personas(self):
        class C(ClienteFalso):
            def upsert(self, table, rows, on_conflict, batch_size=200):
                return len(rows)

            def rpc(self, fn, params):
                self.calls.append((fn, params))
                return 0
        c = C([])
        self.sync._push_users(c)
        fn, params = [x for x in c.calls if x[0] == 'buzon_personas_reemplazar'][0]
        self.assertIsInstance(params['p_filas'], list)  # [] si qb_memoria no está; filas {buzon, odoo_user_id, area} si está
```

- [ ] **Step 2: Implementar** — al final de `_push_users`, antes del `return client.upsert(...)`:

```python
        # §4 regla 1: personas detrás de buzones compartidos, desde "Buzones (memoria)" de qb_memoria.
        # quimibond_intelligence NO depende de qb_memoria (manifest congelado): si no está, manda la lista vacía.
        buzones = []
        if 'qb.memoria.mailbox' in self.env:
            for mb in self.env['qb.memoria.mailbox'].sudo().search([('active', '=', True), ('user_id', '!=', False)]):
                buzones.append({'buzon': mb.email, 'odoo_user_id': mb.user_id.id, 'area': mb.area or None})
        client.rpc('buzon_personas_reemplazar', {'p_filas': buzones})
```

- [ ] **Step 3: `flake8`, push, CI verde.** Commit — "_push_users: buzon_personas desde qb.memoria.mailbox".

### Task 2.12: Documentación, PR, despliegue y aceptación del paso 2

**Files:** `CLAUDE.md` (qb19), `docs/RUNBOOK_DESPLIEGUE.md`, `addons/quimibond_intelligence/__manifest__.py` (solo `summary`/`description`, NO `version`)

- [ ] **Step 1: Docs.** En `CLAUDE.md`: estructura (`models/senales/`, `sync_push_senales.py`), tabla "Modelos sincronizados" con la fila `_push_senales` (Odoo → `senales`, una fila por hecho), Crons (push_models default `contacts,users,senales`; `senales_last_run`), y un párrafo "Cómo agregar una señal" (función `@senal` + fila en `senales_config`, sin tocar el bot). En el runbook: verificación post-deploy:

```sql
-- Supabase: llegaron lotes de Odoo en la última hora y cuántas señales abiertas hay por área
select senal, max(recibido_en) ultimo, bool_and(ok) ok from senales_lotes where fuente='odoo' and recibido_en > now()-interval '2 hours' group by 1 order by 1;
select area, calidad, count(*) from senales where resuelta_en is null group by 1,2 order by 1,2;
select * from situacion_salud();
```
y en Odoo: Ajustes → Técnico → Quimibond Sync: no debe haber "Push señales con errores".

- [ ] **Step 2: PR de qb19** (draft desde `claude/awesome-franklin-odolbj`: "quimibond_intelligence: push de señales a Supabase (situación, plan A paso 2)"), `subscribe_pr_activity`, CI verde (`check` + `odoo-tests`), ready, squash-merge, PR "Merge main into quimibond (#N)" con merge commit, rebuild de la rama.
- [ ] **Step 3: Pedir al CEO el deploy** (shell de Odoo.sh, rama `quimibond`): `odoo-update quimibond_intelligence && odoosh-restart http && odoosh-restart cron`. Hasta que corra, el paso 3 puede seguir con las señales de memoria.
- [ ] **Step 4: Aceptación (spec §8 paso 2)** tras el primer push (o "Forzar Push" en Ajustes → Técnico → Quimibond Sync):

```sql
select * from situacion_mapa('finanzas') where senal = 'cartera_vencida';           -- clientes agrupados, ~413 facturas en documentos
select clave, calidad, calidad_motivo from senales where senal='cartera_vencida' and calidad='dato_malo' and resuelta_en is null;  -- partes relacionadas
select count(*) from senales where senal='op_atrasada' and calidad='zombie' and resuelta_en is null;   -- ~296 OPs viejas
select details->'senales' from pipeline_logs where phase='odoo_push_senales' order by created_at desc limit 1;  -- tiempos por señal; total < 60 s
```
Si el push pasa de 60 s: sube `cada_horas` de las señales caras en `senales_config` (UPDATE) y anota cuáles.

---

## Parte 3 — El bot `situacion-consolidar` (paso 3 del spec)

Todo en `/home/user/quimibond-intelligence/supabase/functions/`. Patrón de referencia: `memory-consolidate/index.ts` (mismos helpers: `serviceClient`, `authorizeCron`, `readBody`, `pipelineLog` de `_shared/env.ts`; `anthropicClient`, `claudeJSON`, `MODEL_BULK` de `_shared/claude.ts`). Presupuesto: 110 s por invocación, ≤ 40 candidatas, ~4k tokens de entrada por candidata (spec §6).

### Task 3.1: `prompt.ts` (puro): sistema, armado del contexto con presupuesto, validación del JSON

**Files:**
- Create: `supabase/functions/situacion-consolidar/prompt.ts`
- Create: `supabase/functions/situacion-consolidar/prompt_test.ts`

- [ ] **Step 1: Tests Deno** (`deno test supabase/functions/situacion-consolidar/` si hay `deno`; si no, se corren una vez en el contenedor `denoland/deno` con Docker o se documenta que no corrieron):

```ts
import { assertEquals, assert } from "jsr:@std/assert@1";
import { armarContexto, validarSalida, SYSTEM } from "./prompt.ts";

const ctx = {
  situacion: { id: 7, clave: "cartera_vencida|company:9", senal: "cartera_vencida", area: "finanzas", tipo: "credito", titulo: "Cartera vencida · ACME", estado: "empeoro", severidad: 3, calidad: "viva", dias_abierta: 12, dias_sin_cambio: 0, ultimo_cambio: "empeoró: 5 → 7 documentos", valor: 53000, valor_texto: "7 facturas", n_senales: 7, version: 3, ia_version: 2 },
  senal_config: { titulo: "Cartera vencida", descripcion: "Facturas vencidas por cliente", severidad_base: 3, severidad_max: 5 },
  senales: Array.from({ length: 50 }, (_, i) => ({ clave: `cartera_vencida:partner:${i}`, valor: i, valor_texto: "x".repeat(300), documentos: [{ modelo: "account.move", id: i, nombre: `F/${i}` }] })),
  contraparte: { empresa: { name: "ACME" }, memoria: { hechos: [{ hecho: "paga a 60 días" }], hilos: [] } },
  conversaciones: [{ tema: "Cobro", resumen: "y".repeat(5000), estado: "abierto" }],
  hermanas: [], posibles_duplicados: [{ id: 8, titulo: "Promesa de pago · ACME", similitud: 0.4 }], reglas: [], personas: [{ odoo_user_id: 2, name: "Ana", motivo: "dueño" }], historia: [],
};

Deno.test("armarContexto respeta el presupuesto y conserva lo esencial", () => {
  const txt = armarContexto(ctx as never, 6000);
  assert(txt.length <= 6200, `largo ${txt.length}`);
  assert(txt.includes("Cartera vencida · ACME"));
  assert(txt.includes("Posibles duplicados"));
  assert(txt.includes("Ana"));
});

Deno.test("validarSalida recorta severidad a la banda y limpia duplicados", () => {
  const out = validarSalida({ titulo: "T", resumen: "R", recomendacion: "Rec", severidad: 9, responsable_sugerido_user_id: "2", responsable_motivo: "m", estado: "empeoro",
    duplicados: [{ id: 8, decision: "fusionar", motivo: "misma cartera" }, { id: 7, decision: "fusionar", motivo: "yo misma" }, { id: 99, decision: "distinta" }], evento_historia: "e" },
    { base: 3, max: 5, id: 7, candidatos: [8, 99] });
  assertEquals(out.severidad, 5);
  assertEquals(out.responsable_sugerido_user_id, 2);
  assertEquals(out.duplicados, [{ id: 8, decision: "fusionar", motivo: "misma cartera" }]);
});

Deno.test("validarSalida rechaza salida sin título o resumen", () => {
  let err = "";
  try { validarSalida({ titulo: "", resumen: "" } as never, { base: 1, max: 5, id: 1, candidatos: [] }); } catch (e) { err = String(e); }
  assert(err.includes("titulo"));
});

Deno.test("el prompt del sistema fija el contrato", () => {
  for (const s of ["Quimibond", "JSON", "severidad", "responsable", "duplicados", "nunca"]) assert(SYSTEM.includes(s), s);
});
```

- [ ] **Step 2: Correr → falla (módulo no existe).**

- [ ] **Step 3: Implementar `prompt.ts`**

```ts
/**
 * Prompt y contrato del bot de situaciones (spec §6.4). Puro: sin red, testeable.
 */
export interface Contexto {
  situacion: Record<string, unknown> & { id: number; titulo: string; senal: string; estado: string; severidad: number; version: number; ia_version: number };
  senal_config: { titulo: string; descripcion: string | null; severidad_base: number; severidad_max: number; umbrales?: unknown } | null;
  senales: Record<string, unknown>[];
  documentos?: unknown[];
  contraparte: { empresa?: Record<string, unknown>; memoria?: Record<string, unknown> | null; company_id?: number } | null;
  conversaciones: Record<string, unknown>[];
  hermanas: { id: number; titulo: string; senal: string; severidad: number; estado: string; dias_abierta: number }[];
  posibles_duplicados: { id: number; titulo: string; senal: string; similitud?: number; documentos_comunes?: number }[];
  reglas: Record<string, unknown>[];
  personas: { odoo_user_id: number; name: string; department?: string | null; motivo: string }[];
  historia: Record<string, unknown>[];
}

export interface Salida {
  titulo: string;
  resumen: string;
  recomendacion: string;
  severidad: number;
  responsable_sugerido_user_id: number | null;
  responsable_motivo: string | null;
  estado: "abierta" | "empeoro" | "mejoro";
  duplicados: { id: number; decision: "fusionar" | "distinta"; motivo: string }[];
  evento_historia: string;
}

export const SYSTEM = `Eres el analista de situación de Quimibond (textil, México). Hablas desde Quimibond, para el director general, que decide qué delega y a quién. Recibes UNA situación (señales determinísticas de Odoo o del correo, agrupadas por SQL) con su contexto: documentos, ficha de memoria de la contraparte, conversaciones resumidas, situaciones hermanas, posibles duplicados, reglas del director e historia.

Devuelve SOLO un objeto JSON (sin markdown) con este esquema exacto:
{
 "titulo": "una línea, ≤ 90 caracteres, concreta: qué y con quién (conserva el título actual si sigue siendo correcto)",
 "resumen": "un párrafo (3-6 frases): qué pasa, desde cuándo, con quién, cuánto, qué cambió. Cifras, fechas y nombres tal como vienen.",
 "recomendacion": "la acción concreta que harías: verbo + responsable + documento. Una o dos frases.",
 "severidad": entero dentro de la banda indicada, justificado por la regla de la señal y la magnitud,
 "responsable_sugerido_user_id": id de odoo_users de la lista de personas, o null,
 "responsable_motivo": "por qué esa persona (dueño del documento, buzón que atiende, etc.)",
 "estado": "abierta | empeoro | mejoro",
 "duplicados": [{"id": <id de posibles_duplicados>, "decision": "fusionar | distinta", "motivo": "una frase"}],
 "evento_historia": "una línea para la bitácora: qué cambió y por qué importa"
}

Reglas estrictas:
- Solo lo que está en el contexto. Nada inventado; si falta un dato, dilo o usa null.
- Una situación = una decisión posible del director. Si son dos decisiones, dilo en la recomendación, no inventes otra situación.
- La severidad NUNCA sale de la banda [severidad_base, severidad_max]; base = lo normal para esa señal; max = con agravantes (monto alto, reincidencia/episodio > 1, cliente estratégico, reclamación tensa, plazo legal).
- La recomendación nombra la acción, al responsable y el documento (número de factura, OP, pedido, hilo).
- "duplicados": fusionar solo si es EL MISMO asunto con la misma contraparte (p.ej. la promesa de pago del correo y la cartera vencida de ese cliente); si dudas, "distinta". Nunca te fusiones a ti misma.
- Respeta las reglas del director listadas (ignorar, severidad fija, responsable fijo).
- Español neutro, sin adjetivos de relleno, sin copiar correos completos ni firmas. No repitas el contexto: resume.`;

const J = (x: unknown, max: number) => {
  const s = JSON.stringify(x ?? null);
  return s.length > max ? s.slice(0, max - 3) + "…\"]" : s;
};

/** Arma el bloque de usuario con presupuesto de caracteres: lo esencial completo, lo largo recortado. */
export function armarContexto(ctx: Contexto, presupuesto = 16_000): string {
  const s = ctx.situacion;
  const cfg = ctx.senal_config;
  const partes: string[] = [];
  partes.push(`Hoy: ${new Date().toISOString().slice(0, 10)}`);
  partes.push(`Situación #${s.id} [${s.senal} · ${String(s.area)}/${String(s.tipo)}] estado=${s.estado} calidad=${String(s.calidad)} severidad_actual=${s.severidad} banda=[${cfg?.severidad_base ?? 1},${cfg?.severidad_max ?? 5}]`);
  partes.push(`Título actual: ${s.titulo}`);
  partes.push(`Señal: ${cfg?.titulo ?? s.senal} — ${cfg?.descripcion ?? ""}`);
  partes.push(`Días abierta: ${String(s.dias_abierta)}; sin cambio: ${String(s.dias_sin_cambio)}; último cambio: ${String(s.ultimo_cambio ?? "")}; valor: ${String(s.valor ?? "")} (${String(s.valor_texto ?? "")}); vence: ${String(s.vence ?? "")}`);
  if (s.resumen) partes.push(`Resumen anterior (v${s.ia_version}): ${String(s.resumen)}\nRecomendación anterior: ${String(s.recomendacion ?? "")}`);
  if (ctx.reglas?.length) partes.push(`Reglas del director: ${J(ctx.reglas, 800)}`);
  partes.push(`Personas candidatas (odoo_user_id · nombre · motivo): ${ctx.personas.map((p) => `${p.odoo_user_id} · ${p.name}${p.department ? " (" + p.department + ")" : ""} · ${p.motivo}`).join("; ") || "ninguna"}`);
  if (ctx.posibles_duplicados?.length) partes.push(`Posibles duplicados (id · título · señal · similitud · documentos comunes): ${ctx.posibles_duplicados.map((d) => `${d.id} · ${d.titulo} · ${d.senal} · ${d.similitud ?? "-"} · ${d.documentos_comunes ?? 0}`).join("; ")}`);
  if (ctx.hermanas?.length) partes.push(`Otras situaciones abiertas de la misma contraparte: ${ctx.hermanas.slice(0, 8).map((h) => `#${h.id} ${h.titulo} (sev ${h.severidad}, ${h.dias_abierta} d)`).join("; ")}`);
  if (ctx.historia?.length) partes.push(`Historia: ${J(ctx.historia.slice(-6), 900)}`);

  // Bloques largos, en orden de importancia, con lo que quede del presupuesto.
  const fijo = partes.join("\n").length;
  let resto = Math.max(presupuesto - fijo - 200, 1500);
  const largo: [string, unknown, number][] = [
    ["Señales (clave, valor, texto, vence, calidad, episodio, documentos)", ctx.senales.map((x) => ({ clave: x.clave, valor: x.valor, valor_texto: String(x.valor_texto ?? "").slice(0, 160), vence: x.vence, calidad: x.calidad, episodio: x.episodio, docs: Array.isArray(x.documentos) ? (x.documentos as unknown[]).slice(0, 5) : [] })).slice(0, 30), 0.45],
    ["Contraparte", ctx.contraparte ? { empresa: ctx.contraparte.empresa, memoria: ctx.contraparte.memoria ? { encargados: (ctx.contraparte.memoria as Record<string, unknown>).encargados, hechos: (ctx.contraparte.memoria as Record<string, unknown>).hechos, stats: (ctx.contraparte.memoria as Record<string, unknown>).stats } : null } : null, 0.25],
    ["Conversaciones ligadas (resúmenes)", ctx.conversaciones.map((c) => ({ ...c, resumen: String(c.resumen ?? "").slice(0, 700) })), 0.30],
  ];
  for (const [titulo, obj, frac] of largo) {
    if (obj == null || (Array.isArray(obj) && !obj.length)) continue;
    const cupo = Math.floor(resto * frac) + 200;
    const txt = J(obj, cupo);
    partes.push(`${titulo}: ${txt}`);
    resto -= txt.length;
  }
  return partes.join("\n\n");
}

/** Normaliza y valida la respuesta de Claude contra la banda y los duplicados ofrecidos. Lanza si no sirve. */
export function validarSalida(raw: Partial<Salida> & Record<string, unknown>, opts: { base: number; max: number; id: number; candidatos: number[] }): Salida {
  const titulo = String(raw.titulo ?? "").trim().slice(0, 160);
  const resumen = String(raw.resumen ?? "").trim();
  if (!titulo || !resumen) throw new Error("salida sin titulo o resumen");
  const sevRaw = Number(raw.severidad);
  const severidad = Math.min(Math.max(Number.isFinite(sevRaw) ? Math.round(sevRaw) : opts.base, opts.base), opts.max);
  const uidRaw = raw.responsable_sugerido_user_id;
  const uid = uidRaw == null || uidRaw === "" ? null : Number(uidRaw);
  const estado = (["abierta", "empeoro", "mejoro"] as const).includes(raw.estado as never) ? (raw.estado as Salida["estado"]) : "abierta";
  const duplicados = (Array.isArray(raw.duplicados) ? raw.duplicados : [])
    .map((d) => ({ id: Number((d as { id: unknown }).id), decision: (d as { decision: string }).decision === "fusionar" ? "fusionar" as const : "distinta" as const, motivo: String((d as { motivo?: unknown }).motivo ?? "").slice(0, 200) }))
    .filter((d) => d.decision === "fusionar" && d.id !== opts.id && opts.candidatos.includes(d.id));
  return {
    titulo, resumen, recomendacion: String(raw.recomendacion ?? "").trim(), severidad,
    responsable_sugerido_user_id: uid != null && Number.isFinite(uid) ? uid : null,
    responsable_motivo: raw.responsable_motivo ? String(raw.responsable_motivo).slice(0, 300) : null,
    estado, duplicados, evento_historia: String(raw.evento_historia ?? "redactada").slice(0, 300),
  };
}
```

- [ ] **Step 4: `deno test` verde** (o documentar que no hay Deno local y que se corrió en Docker: `docker run --rm -v "$PWD":/w -w /w denoland/deno:latest test supabase/functions/situacion-consolidar/`). Commit — "situacion-consolidar: prompt, contexto con presupuesto y validación (puro, con tests)".

### Task 3.2: `index.ts` — el ciclo

**Files:**
- Create: `supabase/functions/situacion-consolidar/index.ts`

- [ ] **Step 1: Implementar**

```ts
/**
 * situacion-consolidar (Edge Function) — el bot de situaciones (spec §6).
 *
 * Cada corrida: situacion_ciclo (SQL: señales de memoria → calidad → situaciones
 * determinísticas) → situacion_candidatas (nuevas/empeoradas/mejoradas sin
 * redacción vigente) → por candidata situacion_contexto → Claude (Sonnet, JSON
 * cerrado) → situacion_redactar (solo título, resumen, recomendación, severidad
 * en banda, responsable, fusiones). La IA nunca toca clave, documentos,
 * evidencia ni estado. Cierra la corrida en situacion_corridas.
 *
 * Disparo: senales_push_terminado (al terminar el push horario de Odoo) y
 * pg_cron situacion_respaldo (si no corrió en 50 min).
 * Body opcional: { corrida: uuid, origen: "odoo"|"cron"|"manual", batch: 40, id: <situacion_id> (solo esa, aunque ya esté redactada), sin_ia: true }.
 */
import { serviceClient, authorizeCron, json, pipelineLog, readBody, type Client } from "../_shared/env.ts";
import { anthropicClient, claudeJSON, MODEL_BULK } from "../_shared/claude.ts";
import { SYSTEM, armarContexto, validarSalida, type Contexto, type Salida } from "./prompt.ts";

const TIME_BUDGET_MS = 110_000;
const MAX_CANDIDATAS = 40;
const PROMPT_CHARS = 16_000;

async function redactarUna(supabase: Client, client: any, id: number, model: string, corridaId: number) {
  const { data: ctx, error } = await supabase.rpc("situacion_contexto", { p_id: id });
  if (error) throw new Error(`situacion_contexto: ${error.message}`);
  if (!ctx) throw new Error(`situación ${id} no existe`);
  const c = ctx as Contexto;
  const banda = { base: c.senal_config?.severidad_base ?? 1, max: c.senal_config?.severidad_max ?? 5, id, candidatos: (c.posibles_duplicados ?? []).map((d) => d.id) };
  const raw = await claudeJSON<Partial<Salida> & Record<string, unknown>>(client, supabase, {
    model, system: SYSTEM, user: armarContexto(c, PROMPT_CHARS), max_tokens: 1200, effort: "low",
  }, "situacion-consolidar");
  const out = validarSalida(raw, banda);
  const { data: saved, error: saveErr } = await supabase.rpc("situacion_redactar", { p_id: id, p: out, p_modelo: model, p_corrida_id: corridaId });
  if (saveErr) throw new Error(`situacion_redactar: ${saveErr.message}`);
  return { id, titulo: out.titulo, severidad: out.severidad, fusiones: Number((saved as { fusiones?: number })?.fusiones ?? 0) };
}

Deno.serve(async (req: Request) => {
  const supabase = serviceClient();
  const denied = await authorizeCron(req, supabase);
  if (denied) return denied;
  const body = await readBody(req);
  const started = Date.now();
  const origen = typeof body.origen === "string" ? body.origen : "manual";
  const batch = Math.min(Math.max(Number(body.batch ?? MAX_CANDIDATAS), 1), MAX_CANDIDATAS);
  const model = typeof body.model === "string" && body.model ? body.model : MODEL_BULK;
  const only = body.id ? Number(body.id) : null;

  // 1. Ciclo SQL (memoria → calidad → situaciones). Siempre, aunque la IA falle después.
  const { data: ciclo, error: cicloErr } = await supabase.rpc("situacion_ciclo", { p_corrida: typeof body.corrida === "string" ? body.corrida : crypto.randomUUID(), p_origen: origen });
  if (cicloErr) {
    await pipelineLog(supabase, "situacion_consolidar", "error", `situacion_ciclo: ${cicloErr.message}`, { origen });
    return json({ error: cicloErr.message }, 500);
  }
  const corridaId = Number((ciclo as { corrida_id: number }).corrida_id);

  // 2. Candidatas.
  let ids: number[] = [];
  if (only) ids = [only];
  else if (body.sin_ia !== true) {
    const { data: cand, error: candErr } = await supabase.rpc("situacion_candidatas", { p_limit: batch });
    if (candErr) {
      await pipelineLog(supabase, "situacion_consolidar", "error", `situacion_candidatas: ${candErr.message}`, { origen, corridaId });
      return json({ error: candErr.message, ciclo }, 500);
    }
    ids = ((cand ?? []) as { id: number }[]).map((c) => c.id);
  }

  // 3. Redacción.
  const results: Record<string, unknown>[] = [];
  const errores: string[] = [];
  let ok = 0, fusiones = 0;
  const client = ids.length ? await anthropicClient(supabase) : null;
  if (ids.length && !client) {
    errores.push("anthropic_api_key no configurado (env ni Vault)");
  } else {
    for (const id of ids) {
      if (Date.now() - started > TIME_BUDGET_MS) { errores.push(`presupuesto de tiempo agotado con ${ids.length - ok - errores.length} candidatas sin redactar`); break; }
      try {
        const r = await redactarUna(supabase, client, id, model, corridaId);
        ok++; fusiones += r.fusiones; results.push(r);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        errores.push(`#${id}: ${message.slice(0, 200)}`);   // se reintenta en la siguiente corrida (ia_version sigue < version)
        results.push({ id, error: message.slice(0, 200) });
      }
    }
  }

  // 4. Tokens de esta corrida (token_usage) y cierre.
  const { data: tok } = await supabase.from("token_usage").select("input_tokens, output_tokens").eq("endpoint", "situacion-consolidar").gte("created_at", new Date(started).toISOString());
  const tokens = ((tok ?? []) as { input_tokens: number; output_tokens: number }[]).reduce((a, t) => ({ i: a.i + (t.input_tokens ?? 0), o: a.o + (t.output_tokens ?? 0) }), { i: 0, o: 0 });
  await supabase.rpc("situacion_corrida_cerrar", { p_id: corridaId, p: { n_candidatas: ids.length, n_redactadas: ok, n_fusiones: fusiones, tokens_in: tokens.i, tokens_out: tokens.o, modelo: ids.length ? model : null, errores } });
  const elapsed_s = Math.round((Date.now() - started) / 1000);
  const sit = (ciclo as { situaciones?: Record<string, unknown> }).situaciones ?? {};
  await pipelineLog(supabase, "situacion_consolidar", errores.length && !ok ? "error" : errores.length ? "warning" : "info",
    `Situación (${origen}): ${sit.nuevas ?? 0} nuevas, ${sit.actualizadas ?? 0} actualizadas, ${sit.resueltas ?? 0} resueltas; ${ok}/${ids.length} redactadas, ${fusiones} fusiones, ${errores.length} errores (${elapsed_s}s)`,
    { corrida_id: corridaId, origen, ciclo: sit, candidatas: ids.length, redactadas: ok, fusiones, errores, tokens, elapsed_s, model });
  return json({ ok: true, corrida_id: corridaId, ciclo, candidatas: ids.length, redactadas: ok, fusiones, errores, tokens, elapsed_s, results });
});
```

- [ ] **Step 2: Desplegar** con el MCP `deploy_edge_function` (name `situacion-consolidar`, `verify_jwt: false`, files: `situacion-consolidar/index.ts`, `situacion-consolidar/prompt.ts`, `_shared/env.ts`, `_shared/claude.ts`). Si el deploy falla por sintaxis, el mensaje trae la línea.

- [ ] **Step 3: Probar a mano, primero sin IA y luego una sola situación:**

```sql
select invoke_edge('situacion-consolidar', '{"origen":"manual","sin_ia":true}'::jsonb);
-- espera ~10 s
select message, details from pipeline_logs where phase = 'situacion_consolidar' order by created_at desc limit 1;
select id, titulo from situacion_candidatas(3);
select invoke_edge('situacion-consolidar', '{"origen":"manual","batch":1}'::jsonb);
select id, titulo, severidad, recomendacion, responsable_sugerido_user_id, ia_version, version from situaciones where ia_version > 0 order by updated_at desc limit 3;
select * from claude_cost_summary where endpoint = 'situacion-consolidar';
```
Expected: título y recomendación en español, severidad dentro de la banda, `ia_version = version`; ~4k tokens de entrada por candidata (spec §6). Si un contexto pasa de 6k tokens, baja `PROMPT_CHARS`.

- [ ] **Step 4: Commit** — "situacion-consolidar: ciclo SQL + redacción con Sonnet + fusiones".

### Task 3.3: Disparo por evento y cron de respaldo

**Files:**
- Create: `supabase/migrations/20260920a_situacion_bot_cron.sql`

- [ ] **Step 1: Migración** (solo ahora que la función existe):

```sql
-- 2026-09-20a — Situación plan A paso 3: cron de respaldo del bot.
-- El disparo normal es por evento (senales_push_terminado al terminar el push de Odoo).
-- Respaldo: cada hora en :20, solo si no hubo corrida en los últimos 50 minutos (spec §6.1).
DO $do$
DECLARE j record;
BEGIN
  FOR j IN SELECT jobid FROM cron.job WHERE jobname = 'situacion_respaldo' LOOP PERFORM cron.unschedule(j.jobid); END LOOP;
END $do$;
SELECT cron.schedule('situacion_respaldo', '20 * * * *',
  $cmd$SELECT public.invoke_edge('situacion-consolidar', '{"origen":"cron"}'::jsonb)
       WHERE NOT EXISTS (SELECT 1 FROM public.situacion_corridas WHERE iniciada_en > now() - interval '50 minutes')$cmd$);
INSERT INTO pipeline_logs (level, phase, message, details)
VALUES ('info', 'migration', 'Situación plan A paso 3: job situacion_respaldo (hourly :20, solo si no corrió en 50 min)', jsonb_build_object('migration', '20260920a_situacion_bot_cron'));
```

- [ ] **Step 2: Aplicar** y verificar `select jobname, schedule, active from cron.job where jobname like 'situacion%'` y, tras el siguiente push de Odoo, que `pipeline_logs` tenga la fila `Push terminado (odoo) … consolidación disparada` seguida de `Situación (odoo): …`.
- [ ] **Step 3: Commit** — "Situación: cron de respaldo situacion_respaldo".

### Task 3.4: El watchdog manda `job_caido`

**Files:**
- Modify: `supabase/functions/health/index.ts`

- [ ] **Step 1: Implementar** — justo antes de `const healthy = issues.length === 0;`:

```ts
  // 5. Situación: lo que el watchdog ve entra al mapa como señal job_caido (fuente watchdog), lista completa (vacía = todo resuelto).
  const filas = issues.filter((i) => i.kind !== "pipeline_errors").map((i) => ({
    clave: `job_caido:${i.kind}:${i.detail.split(":")[0].trim().replace(/\s+/g, "_").slice(0, 60)}`,
    valor: 1, valor_texto: i.detail.slice(0, 300), payload: { kind: i.kind },
  }));
  const { data: lote, error: loteErr } = await supabase.rpc("senales_ingestar", { p_senal: "job_caido", p_fuente: "watchdog", p_corrida: crypto.randomUUID(), p_filas: filas });
  if (loteErr || !(lote as { ok?: boolean })?.ok) console.warn("[health] senales_ingestar job_caido", loteErr?.message ?? lote);
```
y agrega `odoo_stale` para el push de señales: junto al bloque 2 (`odoo contacts`), el mismo check con `.eq("method", "senales")` y umbral 3 h (`odoo senales: Nh sin push exitoso`). También en `JOB_INTERVALS`: `situacion_respaldo: 60` no (corre condicional); en cambio agrega un check: última fila de `situacion_corridas` con `terminada_en` más nueva que 3 h, si no → `issues.push({ kind: "cron_stale", detail: "situacion-consolidar: Nh sin corrida terminada" })`.

- [ ] **Step 2: Desplegar `health`** (files `health/index.ts`, `_shared/env.ts`, `_shared/mailer.ts`) y probar `select invoke_edge('health')`; luego `select * from senales where senal = 'job_caido'` (vacío si todo está sano) y `select count(*) from senales_lotes where senal = 'job_caido'` = 1.
- [ ] **Step 3: Commit** — "health: señal job_caido y vigilancia del push de señales y del bot".

### Task 3.5: Aceptación del paso 3, documentación y PR

- [ ] **Step 1: Aceptación (spec §8 paso 3)** — con el push de Odoo ya desplegado (Tarea 2.12) y dos o tres corridas del bot:
  1. `select * from situacion_mapa() where redactada order by severidad desc limit 20;` → el CEO revisa 20 por MCP (título, recomendación, responsable). Anota en el PR las que corrigió.
  2. Tres fusiones preparadas: (a) `promesa_pago_vencida` + `cartera_vencida` del mismo cliente (por construcción caen en la misma situación: verifica que el mapa muestre UNA por cliente), (b) dos situaciones hermanas con título parecido que el bot debe fusionar (`select * from situaciones where fusionada_en is not null`), (c) una pareja parecida que NO debe fusionar (distinta señal, distinto documento) y sigue separada. Registra ids y resultado.
  3. Costo: `select * from claude_cost_summary where endpoint = 'situacion-consolidar'` → ≤ 40 candidatas/hora, ~4k tokens de entrada cada una.
  4. Higiene: `select situacion_higiene();` → zombis (OPs > 90 d, transferencias, actividades > 180 d) y datos malos (partes relacionadas, márgenes con costo 0) con su limpieza recomendada. Es la primera sesión de higiene del CEO (spec §9).
- [ ] **Step 2: Docs.** `CLAUDE.md` (quimibond-intelligence): sección "Situación de la empresa" completa (qué es, tablas, RPCs de lectura con ejemplos MCP, RPCs del bot, job, disparo por evento, cómo agregar una señal, pruebas en seco, costo). Tabla de jobs: `situacion_respaldo`. Deuda: plan B (correo, delegación, app en Odoo), `cliente_callado` sin la parte de pedidos, `indicador_financiero_rojo` por nombre de indicador.
- [ ] **Step 3: PR** de quimibond-intelligence ("situacion-consolidar: el bot de situaciones, cron de respaldo y watchdog (plan A, paso 3)"), CI, standing-down de Vercel, merge, rebuild de la rama.

---

## Orden de ejecución y dependencias

1. Parte 1 completa (Supabase) — no depende de nada. Al terminar la Tarea 1.5 el mapa ya existe con las señales de memoria (aceptación del paso 1).
2. Parte 2 (qb19) — puede empezar en paralelo desde la Tarea 2.1; la Tarea 2.12 (aceptación) necesita la Parte 1 aplicada y el deploy en Odoo.sh por el CEO.
3. Parte 3 (bot) — necesita la Parte 1; la aceptación completa necesita la Parte 2 en producción. Las Tareas 3.1 y 3.2 se pueden hacer mientras el CEO despliega Odoo.

**Cosas que el CEO tiene que hacer** (no se pueden automatizar desde aquí): correr `odoo-update quimibond_intelligence && odoosh-restart http && odoosh-restart cron` tras el merge a `quimibond`; revisar las 20 situaciones redactadas; decidir sobre la primera lista de higiene.

**Fuera de este plan (plan B):** `situacion_cambios`, `situacion-digest` y retiro de `email-digest`; `situacion_decidir` y reglas persistentes desde MCP; delegación (`sync_commands.payload`, `crear_actividad`, `mail.activity.situacion_id`, hooks, `delegacion_estado`, fix de `status='error'`); `qb_situacion` en Odoo y desinstalación de `qb_obligation` (antes: último lote vacío de `obligacion_legado`).
