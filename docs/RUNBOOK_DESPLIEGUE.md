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

### 5. Revisar qué actualizó Odoo.sh y actualizar solo lo que falte

**Al mergear a `quimibond`, Odoo.sh despliega y actualiza por su cuenta** los
módulos cuya versión subió. Lo hace en los primeros minutos, antes de que uno
llegue al shell. El 2026-10-05 la 57.109.1 del SGI quedó instalada a las
23:03:21; el `odoo-update` manual de las 23:06 corrió con producción ya
arriba y con usuarios. Chocó con ellos al alterar `res_company`
(`deadlock detected`, `Failed to load registry`) y se deshizo completo. No
dañó nada, pero parecía que el despliegue había fallado.

Por eso, **primero se revisa la versión instalada**. Espera a que termine el
despliegue de Odoo.sh (pestaña del build en verde) y compara la base contra
el manifest:

```sql
SELECT name, latest_version, state FROM ir_module_module
 WHERE name IN ('quimibond_sgi', 'quimibond_intelligence' /* , los que cambiaron */) ORDER BY 1;
```

Por MCP de Odoo (solo lectura) es lo mismo: `search_records` sobre
`ir.module.module` con los campos `latest_version` (versión en la base) e
`installed_version` (versión del código que corre).

- **`latest_version` ya es la del manifest:** el módulo se actualizó solo. **No
  corras `odoo-update`**: pasa a § Verificaciones y lee el `update.log` del
  despliegue de Odoo.sh.
- **Sigue en la versión anterior**, o el módulo está en
  `tools/no_bump.txt` y cambió: entonces sí, a mano. Aun así no se ha
  confirmado cuándo Odoo.sh corre `-u`: se ha visto un build de rama saltarse
  un módulo con la versión congelada, y un deploy a producción actualizar otro
  sin bump.

```bash
odoo-update <solo los modulos que no quedaron en su version>
odoosh-restart http && odoosh-restart cron
```

`odoo-update` en producción corre **con usuarios conectados**. Si falla con
`deadlock detected`, el update se deshizo entero y la base sigue como
estaba. Vuelve a revisar la versión antes de reintentar, porque a veces otro
proceso ya la actualizó. Si de verdad falta, reintenta una vez en horario de
poco uso.

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

- **`Model X has no table`** → ese módulo no se actualizó: `odoo-update <modulo>`
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

### Los crons de sync siguen vivos

Odoo 19 apaga solo un cron tras 5 fallos en más de 7 días y no lo vuelve a
encender. Así murieron el push a Supabase (3-jul-2026) y el pull (1-jun-2026)
sin que nadie lo viera. El update de `quimibond_intelligence` los reactiva, pero
compruébalo: a la hora del despliegue, en Ajustes → Técnico → Quimibond Sync
(Historial de Sync) debe haber una corrida de **OdooBot** ("Push completo" con
`contacts=…, users=…`) y otra "Pull completo". Si solo hay corridas tuyas
("Forzar Push"), el cron está apagado o fallando:

```sql
SELECT name, active, nextcall, failure_count, first_failure_date
FROM ir_cron WHERE cron_name ILIKE 'Quimibond%';
```

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

### Incidentes conocidos

- **`deadlock detected` en `ALTER TABLE "res_company" ... DROP NOT NULL`**
  durante un `odoo-update` manual: chocó con usuarios o con el despliegue
  automático de Odoo.sh (2026-10-05). El update se deshace entero. Revisa
  primero `latest_version` (§ 5 de Despliegue): ese día el módulo ya estaba
  actualizado y no hacía falta repetir.
- **`MissingError: Record does not exist or has been deleted` en una
  migración** (2026-10-05, `ir.actions.server(2882,)`): un registro apunta a
  otro que ya no existe. En ese caso era el menú 1471, «Entregas», con su
  acción de servidor borrada; Odoo no limpia esas referencias. Toda la
  actualización se deshace y la base queda en la versión anterior. Si se
  reinicia `http` antes de corregir, el código nuevo corre sobre la base
  vieja: **no reiniciar**. Se corrige en el código, como en la 57.109.1 con
  `exists()` sobre la acción del menú, y se vuelve a desplegar.

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
2. **`taxes_id` en `purchase.order.line`** — algún cliente externo por `/jsonrpc`
   quedó con el nombre viejo; en Odoo 19 es `tax_ids`. Y `/jsonrpc` desaparece
   en Odoo 22.

## SGI (`quimibond_sgi` y satélites)

Además de lo general. El SGI no entra al CI de GitHub (depende de
Enterprise): lo que se valida al cargarlo solo se ve en Odoo.sh.

### Antes del PR a `main`

- `python3 tools/check_odoo_views.py --base-ref origin/main` y
  `python3 tools/check_addons.py --base-ref origin/main` en cero.
- Pruebas en el **build de desarrollo** de la rama, solo con `--test-tags`
  (el suite completo se detiene en las pruebas de nómina):

```bash
odoo-bin -u quimibond_sgi --test-tags /quimibond_sgi,/quimibond_sgi_pesaje,/quimibond_sgi_revisado,/quimibond_sgi_knowledge,/quimibond_sgi_studio --stop-after-init --no-http
```

  Guarda el log. `main` es la copia de producción (staging) y **no corre las
  pruebas del SGI**: no sustituye este paso.

### Actualizar

Primero revisa la versión instalada (§ 5 de Despliegue). Si Odoo.sh ya
actualizó el SGI al mergear, no repitas el `odoo-update`; solo verifica.
Si hace falta a mano, `odoo-update` recibe los módulos **separados por comas, sin espacios** (igual
que `-u` de Odoo):

```bash
odoo-update quimibond_sgi,quimibond_sgi_pesaje,quimibond_sgi_plm,quimibond_sgi_revisado,quimibond_sgi_knowledge,quimibond_sgi_studio
odoosh-restart http && odoosh-restart cron
```

`quimibond_sgi_mapa` **no** se instala en producción.

### Verificar

```sql
-- versión instalada = la del manifest
SELECT name, latest_version, state FROM ir_module_module WHERE name LIKE 'quimibond_sgi%' ORDER BY 1;
```

```bash
# lo que reportaron las migraciones y los avisos del SGI
grep -E "quimibond_sgi" ~/logs/update.log | grep -E "WARNING|ERROR|migrat" | tail -40
```

- **Menú:** SGI → las ocho entradas (Inicio, Reportar, Sistema, Planeación,
  Seguridad y ambiente, Desempeño, Mejora, Administración; 57.113.0) y
  «Sistema → Del Dropbox a Odoo» visibles para Jefe MAST; el árbol esperado está en
  `addons/quimibond_sgi/tools/sgi_menu_tree.txt` (`test_menu_tree` lo compara).
- **Crons:** están en `noupdate`; un cambio de cron llega solo por migración.
  En Ajustes → Técnico → Acciones planificadas, los «SGI …» activos y sin
  `failure_count`.
- **Herencias propias:** 0. Si el log dice «no puede ser localizado en la
  vista padre» sobre una vista `quimibond_sgi.*`, falta un `pre-migrate` que
  borre la herencia vieja (ver `CLAUDE.md`).
- **Cambios en el CHANGELOG** con la marca **Migración**: leer qué reportan en
  el log y compararlo con lo esperado en la entrada.

## Las señales de situación llegan a Supabase

Tras un `odoo-update quimibond_intelligence` (o "Forzar Push" en Ajustes → Técnico → Quimibond Sync), en Supabase (MCP):

```sql
-- lotes de Odoo en las últimas 2 horas, uno por señal, todos ok
select senal, max(recibido_en) ultimo, bool_and(ok) ok from senales_lotes
 where fuente = 'odoo' and recibido_en > now() - interval '2 hours' group by 1 order by 1;
-- señales abiertas por área y calidad; salud del mapa
select area, calidad, count(*) from senales where resuelta_en is null group by 1, 2 order by 1, 2;
select * from situacion_salud();
```

En Odoo, el Historial de Sync no debe traer "Push señales con errores"; si lo trae, el resumen dice qué señal falló. El detalle por señal (filas y segundos) está en `pipeline_logs` con `phase = 'odoo_push_senales'`; si el push pasa de 60 s, sube `cada_horas` de las señales caras en `senales_config`.
