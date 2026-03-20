# Quimibond Intelligence

Sistema de inteligencia empresarial para Quimibond.

## Componentes

- `addons/quimibond_intelligence/` — Addon de Odoo 19
- `frontend/` — Dashboard Next.js 15

## Setup

### Addon de Odoo

1. Copiar `addons/quimibond_intelligence/` al directorio de addons de Odoo
2. Instalar dependencias: `pip install -r requirements.txt`
3. Actualizar lista de módulos en Odoo e instalar "Quimibond Intelligence"
4. Configurar parámetros en Ajustes > Técnico > Parámetros del sistema

### Frontend

```bash
cd frontend
cp .env.local.example .env.local
# Editar .env.local con tus credenciales
npm install
npm run dev
```
