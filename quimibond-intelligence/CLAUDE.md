# Quimibond Intelligence System

Sistema de inteligencia empresarial para Quimibond (fabricante textil mexicano).

## Arquitectura

- **Odoo 19 addon** (`addons/quimibond_intelligence/`) — Motor principal: lee Gmail vía Service Account, enriquece con 10 modelos Odoo (res.partner, sale.order, account.move, purchase.order, mail.message, mail.activity, crm.lead, stock.picking, account.payment, calendar.event), genera briefings con Claude AI, persiste en Supabase
- **Next.js 15 frontend** (`frontend/`) — React 19 + TypeScript + Tailwind + shadcn/ui. Lee SOLO de Supabase (nunca directo a Odoo). Páginas: dashboard, contacts, alerts, briefings, actions, chat.
- **Supabase** — PostgreSQL como data warehouse intermedio entre Odoo y frontend

## Flujo de datos

```
Gmail → Odoo addon (Python ORM) → Claude AI → Supabase → Next.js frontend
```

## Configuración

- Odoo: todo en `ir.config_parameter` (Supabase URL/key, Anthropic key, Voyage key, Google SA JSON, emails)
- Frontend: `.env.local` con `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`, `ANTHROPIC_API_KEY`
- No usa XML-RPC ni REST API — es addon nativo con acceso directo al ORM

## Stack

- Backend: Python (Odoo 19), httpx, google-auth, google-api-python-client
- Frontend: Next.js 15, React 19, TypeScript, Supabase.js, Tailwind CSS
- AI: Claude API (Anthropic), Voyage AI (embeddings)
- DB: Supabase PostgreSQL + Odoo PostgreSQL

## Archivos clave

- `addons/quimibond_intelligence/models/intelligence_engine.py` — Orquestador principal
- `addons/quimibond_intelligence/models/intelligence_config.py` — Configuración UI
- `addons/quimibond_intelligence/services/supabase_service.py` — Cliente Supabase REST
- `addons/quimibond_intelligence/services/claude_service.py` — Integración Claude AI
- `addons/quimibond_intelligence/__manifest__.py` — Manifiesto del addon
- `addons/quimibond_intelligence/data/ir_cron_data.xml` — Cron jobs (daily/weekly)
- `frontend/src/app/api/chat/route.ts` — Chat API endpoint
- `frontend/src/lib/supabase.ts` — Cliente Supabase

## Idioma

El código está en inglés. Los prompts de Claude AI y contenido de negocio están en español.
