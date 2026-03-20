# Quimibond Odoo 19 Addons (qb19)

Addons de Odoo 19 para Quimibond (fabricante textil mexicano).

## Módulos

- `mrp_caja_surtido` — Módulo de manufactura (caja surtido en picking)
- `stock_dymo_labels` — Impresión de etiquetas Dymo/Zebra
- `quimibond_intelligence` — Sistema de inteligencia (Gmail sync, Knowledge Graph, briefings, alertas, scoring, RAG)

## Frontend

- `frontend/` — Dashboard Next.js 15 + shadcn/ui para Quimibond Intelligence

## Stack

- Python (Odoo 19)
- PostgreSQL (Odoo)
- Next.js 15 / TypeScript (frontend Intelligence)
- Supabase (embeddings, knowledge graph)
- Claude API (análisis inteligente)
- Gmail API (sincronización de correos)

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
frontend/
├── src/app/
├── src/components/
└── src/lib/
```

## Idioma

El código está en inglés. Contenido de negocio en español.
