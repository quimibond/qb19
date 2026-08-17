# Runbook de despliegue — qb19

Procedimiento y verificaciones para llevar código a producción sin que la base
de datos se quede atrás. Cada paso existe porque su ausencia costó un incidente.

**Ramas:** `main` = desarrollo · `quimibond` = **producción** · `qbtesting` = pruebas
· `consolti` = línea paralela

---

## Regla de oro: las ramas son superconjuntos

```
main ⊇ quimibond          qbtesting ⊇ quimibond
```

Toda BD de build (staging, dev, pruebas) es **copia de producción**. Si esa rama
no trae un módulo que producción sí tiene instalado, Odoo no puede cargar sus
modelos y el arranque revienta:

```
KeyError: 'sgi.indicator'                       ← el systray de actividades
"mrp.production"."sgi_format_banner" undefined  ← cualquier vista heredada
```

Antes de trabajar en una rama de desarrollo, mergea `quimibond` en ella.

---

## Despliegue

### 1. Poner `main` al día

PR **base `main` ← compare `quimibond`**. Merge limpio o se resuelve ahí, nunca
en el sentido contrario.

### 2. Revisar qué se va

```bash
git fetch origin main quimibond
git diff --stat origin/quimibond origin/main | tail -5
```

Y los checks estáticos contra lo que se va a desplegar:

```bash
python3 tools/check_addons.py --base-ref origin/quimibond
```

**Cero errores antes de seguir.** Las advertencias se leen, no se ignoran.

### 3. Pre-checks según lo que cambie

| Si el diff toca… | Antes de mergear |
|---|---|
| Restricciones nuevas (`models.Constraint`) | Buscar duplicados en producción (§ Verificaciones) |
| Un módulo de `tools/no_bump.txt` | Anotar el `odoo-update` que hará falta |
| Vistas heredadas de modelos con Studio encima | Confirmar que no hay vistas Studio inválidas |

### 4. Mergear

PR **base `quimibond` ← compare `main`**.

### 5. Actualizar lo que no se actualiza solo

Odoo.sh corre `-u` **sólo en los módulos cuya versión cambió**. Todo lo demás
queda en el repo sin llegar a la base:

```bash
odoo-update <cada modulo de tools/no_bump.txt que haya cambiado>
odoosh-restart http && odoosh-restart cron
```

### 6. Verificar

No des el deploy por bueno hasta correr § Verificaciones.

---

## Verificaciones

### El build cargó

```bash
grep -E "Registry loaded|Failed to load registry" ~/logs/update.log | tail -3
```

`Failed to load registry` = producción abajo. `Registry loaded in Ns` = arriba.

### No quedaron modelos huérfanos ni sin tabla

```bash
grep -E "has no table|Missing model" ~/logs/update.log | sort -u
```

- **`Model X has no table`** → el módulo no se actualizó: `odoo-update <modulo>`
- **`Missing model X`** → se borró del código y la fila sigue en `ir_model`;
  también lo arregla el `odoo-update` del módulo dueño

### Las restricciones se crearon de verdad

Odoo **no aborta** si una restricción no se puede crear: registra
`unable to add constraint` y **se la salta**. El update sale "bien" y te deja
creyendo que quedaste protegido.

```sql
SELECT conrelid::regclass AS tabla, conname
FROM pg_constraint WHERE conname LIKE '%\_uniq' ORDER BY 1;
```

Si falta alguna, tiene duplicados:

```sql
-- patrón: agrupar por las columnas de la restricción
SELECT period, product_id, company_id, count(*)
FROM qb_costo_producto GROUP BY 1,2,3 HAVING count(*) > 1;
```

### Las tablas existen

```sql
SELECT to_regclass('qb_cotizacion_tramo'), to_regclass('qb_producto_ficha');
```

Ningún `NULL`.

---

## Cuando algo truena

El **primer** `ERROR` con su traceback es lo que importa; lo que sigue suele ser
cascada:

```bash
awk '/ (ERROR|CRITICAL) /{f=1} f' ~/logs/update.log | head -100
```

Crudo de la terminal, no de la vista de logs de Odoo.sh — esa reordena por
timestamp y parte los tracebacks.

`update.log` es el despliegue; `odoo.log` es lo que pasa después, ya corriendo.

### Ruido conocido, ignorable

- `Two fields ... have the same label` sobre campos `x_studio_*`
- `RELAXNG` / `invalid custom view(s)` de vistas Studio
- `could not serialize access due to concurrent update` — Postgres, Odoo reintenta
- `documents.document()._gc_clear_bin()` — bug del autovacuum de Documents

---

## Deuda abierta

1. **4 vistas Studio inválidas** (`res.groups`, `account.move`, `mrp.bom.line`,
   `purchase.order.line`). Son la razón por la que `quimibond_intelligence` está
   en `tools/no_bump.txt`. Limpiarlas desbloquea el update automático.
2. **6 claves de config con doble declaración** dentro de `quimibond_sgi`
   (ver manual técnico §11). Hoy sobreviven por el orden de carga del manifest.
3. **`taxes_id` en `purchase.order.line`** — algún cliente externo por `/jsonrpc`
   quedó con el nombre viejo; en Odoo 19 es `tax_ids`. Y `/jsonrpc` desaparece
   en Odoo 22.
