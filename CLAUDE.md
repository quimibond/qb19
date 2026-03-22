# Quimibond Odoo 19 Addons (qb19)

Addons de Odoo 19 para Quimibond (fabricante textil mexicano).
Repo vinculado a Odoo.sh — solo contiene modulos de Odoo.

## Modulos

- `mrp_caja_surtido` — Modulo de manufactura (caja surtido en picking)
- `stock_dymo_labels` — Impresion de etiquetas Dymo/Zebra
- `quimibond_intelligence` — Sistema de inteligencia (Gmail sync, Knowledge Graph, briefings, alertas, scoring, RAG)

## Stack

- Python (Odoo 19)
- PostgreSQL (Odoo)
- Supabase (embeddings, knowledge graph — schema en repo quimibond-intelligence)
- Claude API (analisis inteligente)
- Gmail API (sincronizacion de correos)

## Estructura

```
addons/
├── mrp_caja_surtido/
│   ├── models/
│   ├── views/
│   └── security/
├── stock_dymo_labels/
│   ├── models/
│   ├── views/
│   ├── reports/
│   └── static/
└── quimibond_intelligence/
    ├── models/
    ├── views/
    ├── data/
    ├── security/
    └── services/
requirements.txt
```

## Repos relacionados

- `quimibond-intelligence` — Frontend (Next.js 15) + Supabase migrations del sistema de inteligencia
  - Dashboard, Emails, Chat (Claude RAG), Briefings, Alertas, Acciones, Contactos
  - Knowledge Graph (entidades, hechos, relaciones)
  - Sistema (sync status, DB stats)
  - Supabase schema: 18 tablas, 6 RPC functions, RLS, pgvector
  - Auth: password + cookie middleware

## Odoo.sh

- Odoo.sh detecta addons automaticamente buscando `__manifest__.py`
- `requirements.txt` en raiz se instala automaticamente en cada build
- Para actualizar un modulo en Odoo.sh, incrementar `version` en `__manifest__.py`

## Idioma

El codigo esta en ingles. Contenido de negocio en espanol.
