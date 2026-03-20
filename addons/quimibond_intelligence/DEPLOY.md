# Deploy: Quimibond Intelligence System → Odoo.sh

## Paso 1 — Clonar el repo en Cursor

Abre la terminal de Cursor (`` Ctrl+` `` o `` Cmd+` `` en Mac) y ejecuta:

```bash
cd ~/Desktop
git clone git@github.com:quimibond/qb19.git
```

Luego en Cursor: **File → Open Folder → selecciona ~/Desktop/qb19**

---

## Paso 2 — Crear rama staging

En la terminal de Cursor:

```bash
git checkout -b staging/intelligence
```

---

## Paso 3 — Copiar el módulo

Copia toda la carpeta `quimibond_intelligence/` a la raíz del repo `qb19/`.

En Mac:
```bash
cp -r ~/Library/Application\ Support/Claude/local-agent-mode-sessions/*/outputs/quimibond_intelligence ./
```

Si no funciona la ruta, busca la carpeta `quimibond_intelligence/` en los archivos que Cowork te generó y cópiala manualmente a la carpeta `qb19/`.

La estructura debe quedar:
```
qb19/
├── quimibond_intelligence/     ← NUEVO
│   ├── __init__.py
│   ├── __manifest__.py
│   ├── data/
│   ├── models/
│   ├── security/
│   ├── services/
│   ├── static/
│   └── views/
├── (otros módulos que ya tengas)
└── requirements.txt
```

---

## Paso 4 — Dependencias Python

Si ya existe `requirements.txt` en la raíz, agrega estas líneas al final:

```
google-auth
google-api-python-client
httpx
```

Si NO existe, créalo con ese contenido.

---

## Paso 5 — Commit y push

```bash
git add quimibond_intelligence/ requirements.txt
git commit -m "Add Intelligence System module for staging test"
git push origin staging/intelligence
```

Si git pide configurar usuario:
```bash
git config user.email "jose.mizrahi@quimibond.com"
git config user.name "Jose Mizrahi"
```

---

## Paso 6 — En Odoo.sh

1. Ve a https://www.odoo.sh/project/quimibond-qb19 (o como se llame tu proyecto)
2. La rama `staging/intelligence` aparece en la pestaña **Staging**
3. Espera a que termine el build (2-5 min)
4. Entra a la base staging
5. Ve a **Aplicaciones → Actualizar lista de aplicaciones**
6. Busca "Quimibond Intelligence" e instala
7. Aparece el menú **🧠 Intelligence** — configura las API keys
8. Dale **🚀 Ejecutar Ahora** para probar

---

## Paso 7 — Si funciona, merge a producción

```bash
git checkout main
git merge staging/intelligence
git push origin main
```

O hazlo desde la interfaz de Odoo.sh arrastrando la rama a producción.
