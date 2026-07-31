# qb_capacidad_costeo — Capacidad, Costeo, Ociosidad y Cotizador

Módulo **read-only y no invasivo** para Odoo 19 (Odoo.sh). Lee los modelos
nativos (`mrp.*`, `hr.*`, `account.*`, `product.*`, `stock.*`, `sale.*`,
`resource.*`) **en vivo** y, con tablas de configuración propias, calcula
capacidad por máquina, balance de línea, costo real por producto, costo de
ociosidad y cotizaciones. **Nunca escribe ni extiende modelos nativos.**

Es un módulo aparte de `quimibond_intelligence` (el sync a Supabase): no toca
su lógica ni su versión de manifest.

## Modelos

### Configuración (el usuario los llena en la UI — menú Configuración)

| Modelo | Qué captura |
|---|---|
| `qb.costeo.centro` | Catálogo de centros de proceso: naturaleza, driver (peso/largo), **workcenter_ids** (M2M a `mrp.workcenter`), departamentos RH, throughput nominal, capacidad normal (IAS 2), renta contractual, patrón de órdenes (fallback pre-workcenter) |
| `qb.costeo.cuenta.class` | Clasificación de cuentas: cuenta o patrón LIKE → bucket (mp/energia/mod/overhead_fab/depreciacion/arrend_maquinaria/operacion/ventas/no_costeo), variable/fija, centro, driver, % |
| `qb.costeo.factor.config` | Parámetros globales por key: `fab_weight_share` (0.67), `smoothing_months` (12), `production_window_months` (3), `target_margin`, `m_per_kg_default`, `energia_por_kg` (0=auto), `op_pct_override` (0=auto), `weeks_per_month`, `entretela_overhead_extra_mxn`, `denominador_kg_override`, `denominador_m_override` |
| `qb.producto.peso` | Override de peso/unidad y kg↔m por producto (manual > cvu > ref_gramaje > bom > odoo_weight) |
| `qb.producto.ruteo` | Producto/categoría/patrón → familia de costeo (tela / entretela tejida / entretela carda / importado / subproducto) + centros de su ruta |
| `qb.turno.config` | Turnos manuales por centro (fallback mientras no haya `resource.calendar` vía workcenters) |

### Vistas SQL read-only (siempre en vivo, `_table_query`)

| Modelo | Qué muestra |
|---|---|
| `qb.costeo.cuenta.map` | Cuenta → mejor clasificación (cuenta específica > patrón más largo) |
| `qb.capacidad` | Por workcenter: horas de calendario real × eficiencia, producción prom. (workorders), capacidad, utilización, horas libres, throughput real vs nominal |
| `qb.balance` | Por centro, en **metros-equivalentes**: capacidad vs producción, cuello de botella (= techo de planta) |
| `qb.rh.centro` | Dotación, horas y costo MOD/hora (desde sueldos de `hr.version` y desde GL — ambos) |
| `qb.ociosidad` | Costo fijo del centro × (1 − utilización) = capacidad hundida (IAS 2); fijo unitario a capacidad normal vs a producción real |

### Motor de costeo (stored, por período)

| Modelo | Qué guarda |
|---|---|
| `qb.costo.factores` | Los factores del mes: pools GL suavizados, denominadores kg/m, factor $/kg y $/m, energía $/kg, op %, factor entretela, **cobertura del pool** — trazabilidad completa |
| `qb.costo.producto` | Costo por capa por producto: MP (BOM recursiva a último costo), energía, fabricación híbrida, operación; márgenes de contribución y absorbido; **contribución por hora-máquina** |
| `qb.cotizacion` | Cotizaciones guardadas con supuestos (para comparar antes/después). El wizard es una **calculadora viva**: los resultados (costo por capa, pisos, contribución, capacidad) se recalculan al instante al cambiar producto/volumen/precio/margen; el botón solo guarda el escenario |
| `qb.costeo.snapshot` | Foto mensual de capacidad/ociosidad por centro (tendencia) |

## Fórmulas

```
MP/u        = explosión recursiva de BOM al ÚLTIMO costo de compra (fallback avg)
              importados (' I') = landed (avg Odoo);  sin costo → gemelo nacional
              subproductos (SALDO/DESPERDICIO) = $0
energía/u   = energia_por_kg × peso   (importados: 0)
fab/u       = híbrida:  tela en m  → kg/m × factor_kg + factor_m
                        tela en kg → factor_kg + m/kg × factor_m
              factor_kg = ws × pool_fab / kg_producidos     (ws = fab_weight_share)
              factor_m  = (1−ws) × pool_fab / m_producidos
              entretela carda → factor propio $/m (su MOD + renta ÷ sus metros)
              entretela tejida → factor_kg (tejido+tint) + factor entretela
              importados y subproductos NO cargan fabricación
op/u        = op_pct × precio    (op_pct = Σ operación ÷ Σ ventas, suavizado)

costo_variable  = MP + energía            → margen de CONTRIBUCIÓN
costo_absorbido = variable + fab + op     → P&L / piso a planta llena

piso ocioso  = costo variable
piso lleno   = (variable + fab) / (1 − op_pct)
precio sugerido = (variable + fab) / (1 − op_pct − target_margin)
```

## Ventanas de datos

- Gastos GL: promedio móvil `smoothing_months` (12m default), **excluyendo
  meses con pool ≤ 0** (reversos de cierre anual).
- Producción: promedio `production_window_months` (3m default) de meses completos.
- Renta: **contractual fija** por centro (el GL de renta se paga a saltos →
  la cuenta 504.01.0008 está clasificada `no_costeo` para no doble contar).
- MP: último costo de compra por hoja de BOM, convertido a MXN al FX de la compra.
- Capacidad: `resource.calendar` real × `time_efficiency` — nunca 24/7 asumido.

## Configuración automática desde Supabase (cero manual)

**Configuración → Importar desde Supabase** (y cron semanal que además
recalcula el período): jala toda la configuración curada en la capa silver
de Quimibond Intelligence usando las credenciales que el sync ya tiene en
Odoo.sh (`quimibond_intelligence.supabase_url` / `supabase_service_key`):

| Tabla Supabase | → Modelo del módulo |
|---|---|
| `cost_center_config` (12 centros) | `qb.costeo.centro` + departamentos RH por patrón de nómina |
| `rent_lot_assignment` (5 lotes) | renta contractual por centro (Σ monto × %) |
| `workcenter_cost_config` | throughput nominal (11 kg/h) + **auto-link de workcenters** por patrón `%CIRCULAR%` |
| `overhead_account_assignment` | clasificación cuenta→centro (energía variable / overhead directo; asignadas a admin → fuera del pool fabril) |
| `costing_variable_accounts` | luz/gas/agua como variables |
| `costing_config` | `fab_weight_share` (0.67) y demás parámetros |
| `product_kg_per_unit` (2,758) + `product_uom_conversion` (769) | `qb.producto.peso` (match directo por `odoo_product_id`; overrides locales `manual` no se pisan) |

Idempotente: se puede correr las veces que sea. Workcenters nuevos que
matcheen el patrón entran solos en la corrida semanal.

## Cómo agregar un centro nuevo (cero código)

1. Dar de alta el `mrp.workcenter` en Manufactura (con su `resource.calendar`
   real y `time_efficiency`).
2. En **Capacidad & Costeo → Configuración → Centros de costo**: crear (o
   abrir) el centro y ligarle el workcenter en `workcenter_ids`. Poner su
   `std_output_per_hour` (kg/h o m/h) y driver.
3. Listo: aparece en Capacidad, Balance, Ociosidad y RH. Si sus cuentas de
   gasto ya están clasificadas, su costo fijo entra solo.

Mientras un proceso NO tenga workcenters (hoy: tintorería, acabado):
- capturar sus horas en **Turnos / capacidad manual**, y
- (opcional) su `mo_name_pattern` (ej. `TL/OP-ACA%`) para atribuir producción.
Al darlo de alta como workcenter real, la vía nativa lo sustituye sola.

## Cómo clasificar una cuenta nueva

**Configuración → Clasificación de cuentas**: agregar fila con patrón (ej.
`504.01.0099%`) o cuenta específica → bucket/centro/driver. El matching se
refresca al guardar y cada noche (cron), así las cuentas nuevas de una
familia ya clasificada entran solas. El menú **Cuentas sin clasificar**
lista las 4xx-7xx pendientes.

## Validaciones (§ pruebas)

- `costo_absorbido = MP + energía + fab + op` exacto (test).
- Importados sin fabricación; subproductos MP $0 (tests).
- `qb.costo.factores.cobertura_fab_pct`: Σ fab absorbida en vendidos ÷ pool
  (~90% es sano; mucho menos = revisar denominadores/clasificación).
- Parser de gramaje: solo bloques de exactamente 3 dígitos (4 dígitos =
  código de resina, p.ej. 4032/9032).

## Rendimiento (límites de cron de Odoo.sh)

El recálculo mensual está diseñado para caber en el límite de tiempo de los
crons de Odoo.sh con miles de SKUs:

- **Un solo query** resuelve el último costo de compra de TODAS las hojas de
  BOM (`_last_purchase_line_map`, `DISTINCT ON`) + warm-up del cache ORM.
- Las reglas de ruteo y los pesos se resuelven **una vez por corrida**
  (`_engine_ctx`) y se comparten en todo el loop, incluida la recursión de BOM.
- Los registros nuevos se crean **en lote** (un `create(vals_list)`).
- Un producto con BOM rota (ciclo, UoM inconsistente) se **loggea y se omite**
  — no tumba el cálculo mensual completo; el log reporta el conteo de errores.
- `post_init_hook`: el matching cuenta↔clase queda poblado al instalar, así
  las vistas SQL tienen datos desde el día 1.

## Crons

| Cuándo | Qué |
|---|---|
| Diario 2:30 | Refrescar matching de cuentas (plan contable nuevo entra solo) |
| Día 1 de mes | Snapshot de capacidad/ociosidad + recálculo de costeo del mes cerrado |

## Despliegue (Odoo.sh)

- Instalar con `-i qb_capacidad_costeo` (módulo nuevo — no `-u` masivo).
- No requiere que existan tintorería/acabado como workcenters: degrada con
  gracia (capacidad desde turnos config, "sin datos" en vez de romper).
- Multi-compañía: vistas SQL llevan `company_id`.
- No cambia la versión del manifest de `quimibond_intelligence`.
