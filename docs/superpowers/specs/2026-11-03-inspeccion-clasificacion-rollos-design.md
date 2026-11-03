# Inspección y clasificación de rollos: diseño

Pedido de Dirección General (Jose), 2026-11-03: «al salir de la rama los rollos yo pueda inspeccionar los kgs y
poder consumir los rollos y clasificarlos en calidades y también poder sacar rollos de diferentes tamaños y que ahí
me salgan cuántos kgs y cuántos metros tiene cada rollo después de la inspección».

Base: módulo `quimibond_rollos_bitacora` 19.0.3.0.1 de Jose Sacramento (Consolti), rama `qbtesting`, diseño técnico
v5.4. Este documento dice **qué se le agrega**; lo que ya hace se respeta tal cual.

## 1. Decisiones (Dirección General, 2026-11-03)

| # | Decisión |
|---|---|
| D1 | La Inspección **se queda en la orden de acabado en kg** (Devanado → Tintorería → Abridora → Rama → Inspección), como la tiene José. |
| D2 | Se inspecciona **en cuanto el rollo sale de rama**, con la orden en kg abierta. |
| D3 | **El corte se hace en la revisadora** (Inspección). |
| D4 | Cada rollo que sale es un **rollo final con lote nuevo**; al final cada uno tiene sus kg y sus metros. |
| D5 | **Segundas y desperdicio se venden en kg**; primera y FE en metros. |
| D6 | **Solo se juntan rollos del mismo baño.** |
| D7 | Se puede **entregar antes de que termine el baño** sin entregar lo que no está en inventario. |

## 2. Evidencia

**Producción, entradas de producción a calidades no primera, 2026** (`stock.move.line` hechas, origen
producción/inspección/liberación):

| Ubicación | En metros (la tela) | En kg |
|---|---|---|
| 15 FE | 476,798 m (2,000 movimientos) | 17,868 kg (29) |
| 16 Segundas | 281,976 m (1,780) | 7,119 kg (26) |
| 17 Desperdicio | 391,497 m (4,737) | 28,891 kg (60) |

**Producción, salidas a clientes desde esas ubicaciones, 2026:** la FE sale como la tela en metros (81,197 m); las
segundas como `SALDO DE TELA TEJIDO` en kg (4,504 kg) y el desperdicio como `SALDO TEJIDO D` en kg (23,743 kg). Hoy
segundas y desperdicio entran en metros de la tela y se convierten a mano a un saldo en kg para venderlos: el flujo
nuevo los saca directo en kg y elimina esa conversión.

**qbtesting, prueba del 3-nov** (TL/OP-M/00008, 3,500 m de WJ038Q22JNT160): la rama pesó 2 rollos (`00008-0001`
32.1 kg, `00008-0002` 32.5 kg), se inspeccionaron, la orden en kg TL/OP-KG/00008 cerró y los rollos quedaron en 10
INSPECCIÓN apartados por la orden en metros. Falta clasificar.

**Lo que el módulo de José no cubre hoy** (código de 3.0.1):

1. Un rollo de rama = un rollo final con el mismo nombre de lote (`rollo_clasificacion_wizard.action_confirmar`):
   no hay corte ni unión.
2. Todas las calidades salen como la tela en metros al destino del terminado (14 PQ); la calidad solo cambia la
   etiqueta. El ruteo a 15/16/17 está en su lista de pendientes. El costeo mediría todo como primera.
3. La clasificación exige el rollo en inventario (`_clasif_validar_rollo_disponible`), o sea la orden en kg cerrada;
   y el cierre iguala la orden a lo pesado y consume todo el crudo (`button_mark_done`): no hay cierre parcial.
4. Se consume el kg de rama (`_clasif_asegurar_consumo`), no lo que sale.

## 3. Flujo

### Orden de acabado en kg

**1. Rama pesa cada rollo** — sin cambio. Lote por rollo `<orden>-NNNN`, etiqueta chica. **Nuevo:** casilla
«Último rollo del baño» al pesar.

**2. Inspección en la revisadora** (orden en kg abierta):

- Escanea el rollo de rama; mide metros, marca defectos y empalmes (como hoy).
- Registra los **rollos finales** que salen de la revisadora, uno o varios, cada uno con metros y calidad
  (PQ / FE / Segunda / Desperdicio) y defectos.
- Cada rollo final recibe **su número de lote nuevo** y su etiqueta preliminar (§4.2).
- **Juntar:** se pueden escanear dos o más rollos de rama del **mismo baño** (§4.4) y registrar un solo rollo final.
- No se pesa ni se mueve inventario: en la orden en kg el rollo de rama sigue con sus kg de rama.

**3. Cierre del baño**

- **Automático:** cuando el último rollo pesado del baño (el marcado en rama) y todos los anteriores están
  inspeccionados, la orden en kg se cierra sola. El bloqueo actual («no cierra con rollos sin inspeccionar») se
  queda.
- **«Liberar lo inspeccionado»** (botón en la orden en kg, grupo de supervisores): cierra la orden solo por los
  rollos de rama ya inspeccionados (§4.5). Lo pesado sin inspeccionar y lo que falta de rama pasan a la orden
  pendiente.

### Orden en metros (Clasificación final, Empaque)

**4. Empaque: un pesaje por rollo final**

- Escanea la etiqueta del rollo final, lo pesa con la tara de Clasificación final (báscula IoT o manual con
  motivo, como hoy) y confirma o corrige la calidad (si la cambia, pide motivo).
- Produce el rollo final en su calidad (§4.3) con su lote, sus metros y sus kg.
- Cuando todos los rollos finales de un rollo de rama están clasificados, consume el rollo de rama completo (§4.6).
- Imprime la etiqueta final: PQ, o PNC para FE / Segunda / Desperdicio, como hoy.

**5. Cierre de la orden en metros** — con los metros reales de primera y FE y los kg de los saldos. Sobrante a
existencia; faltante, otra orden (ya va con la primera para planear, `qb_costeo` 1.9.0).

## 4. Reglas

### 4.1 Rollo final

Modelo nuevo `mrp.rollo.final`: una fila por rollo final, con

- `name` (lote), `production_id` (orden en metros), `bitacora_ids` (rollos de rama de los que sale, Many2many);
- de Inspección: `metros_insp`, `clasificacion_inicial`, `defecto_ids`, operador, revisadora, fecha;
- de Empaque: `kg_bruto`, `tara`, `kg_neto`, `metros_final`, `clasificacion_final`, `product_id` y `lot_id`
  producidos, operador, fecha, captura manual y motivo.

Un rollo final viene de un rollo de rama (sin corte o con corte) o de varios (unión); no se permite cortar y juntar
en la misma captura.

### 4.2 Lote

- Nombre: `M<último segmento de la orden en metros>-NNNN`, consecutivo por orden en metros (`M00008-0001` de
  TL/OP-M/00008). La `M` lo distingue a simple vista del rollo de rama (`00008-0001`), que en piso conviven. El
  código de barras de la etiqueta final lleva orden y número de rollo, como el de José.
- El nombre se aparta en Inspección (va en la etiqueta preliminar); el `stock.lot` se crea en Empaque para el
  producto que corresponda a la calidad definitiva (la tela o el saldo), porque un lote es de un solo producto.
- La liga rollo final → rollos de rama → baño → crudo queda en `bitacora_ids` y en la orden; no se codifica en el
  nombre.

### 4.3 Calidad → producto y ubicación

| Calidad definitiva | Producto que sale | Unidad | Ubicación |
|---|---|---|---|
| PQ | la tela (producto de la orden) | m | 14 PRODUCTO TERMINADO PQ |
| FE | la tela | m | 15 PRODUCTO TERMINADO FE |
| Segunda | `SALDO DE TELA TEJIDO` (subproducto) | kg | 16 PRODUCTO TERMINADO SEGUNDAS |
| Desperdicio | `SALDO TEJIDO D` (subproducto) | kg | 17 PRODUCTO TERMINADO DESPERDICIO |

- PQ y FE son líneas del terminado con su lote; la FE con destino 15. Segunda y desperdicio son líneas de los
  subproductos de la lista en metros (cantidad 0 en la lista; la real es el peso). Ubicaciones y productos se leen
  de parámetros, no se escriben en código.
- Los dos saldos ya existen en producción con seguimiento por lote (ids 17929 y 19288).

### 4.4 Mismo baño

Baño = orden en kg raíz (la orden y sus pendientes de «Liberar lo inspeccionado» son el mismo baño). Al juntar, todos
los rollos de rama deben ser del mismo baño; si no, error.

### 4.5 Liberar lo inspeccionado

- Cantidad a cerrar = kg de rama de los rollos inspeccionados. Cierre parcial nativo de Odoo con pendiente
  (`backorder`): el crudo y los tiempos se reparten en proporción; al final del baño el total es el mismo.
- Los rollos pesados no inspeccionados pasan a la pendiente con sus lotes. La pendiente hereda «Último rollo del
  baño» si ya se marcó.
- El cierre actual de José (iguala la orden a lo pesado, consume todo el crudo) se queda para el cierre final del
  baño; el parcial es otro camino.

### 4.6 Consumo y merma

- Se consume el rollo de rama completo (sus kg de rama) cuando el último de sus rollos finales se clasifica; con
  unión, cuando se clasifica el rollo final que los junta.
- Merma visible por grupo (rollos de rama ↔ rollos finales): kg de rama − kg netos de los rollos finales. Alerta si
  pasa de un parámetro (%).
- Lo que se consume es lo que existe: nunca más kg de los que tiene el lote.

## 5. Cambios por módulo

**`quimibond_rollos_bitacora` (José):**

1. «Último rollo del baño» en PESAR ROLLO.
2. Inspección: rollos finales (corte y unión, mismo baño), lote apartado, etiqueta preliminar por rollo final.
3. Cierre automático del baño; botón «Liberar lo inspeccionado».
4. Clasificación por rollo final: pesaje, calidad, producto y ubicación según §4.3; consumo según §4.6.
5. Pruebas de los casos de §6.

**`quimibond_ficha_tecnica_tela` (nosotros):** las listas en metros que genera «Generar listas desde la ruta» llevan
los dos subproductos (`SALDO DE TELA TEJIDO` → 16, `SALDO TEJIDO D` → 17, cantidad 0, costo 0 %). Hoy las listas de
las 31 telas en producción no tienen subproductos.

**`qb_costeo` (nosotros):** el rendimiento cuenta como merma de la tela los saldos en kg que salen de su
clasificación, convertidos a metros con el rendimiento de la ficha (m/kg); el desperdicio de rama (orden en kg) sigue
sin contar, porque ya está en el factor del crudo.

## 6. Casos de aceptación

| # | Caso | Resultado esperado |
|---|---|---|
| 1 | Rollo de rama todo primera, sin corte | 1 rollo final, lote nuevo, m y kg, en 14 |
| 2 | Rollo cortado en 250 m primera + 120 m segunda | 2 lotes: la tela 250 m en 14; saldo de tela en kg en 16; rollo de rama consumido completo; merma = kg rama − suma |
| 3 | Rollo todo FE | la tela en m, en 15 |
| 4 | Desperdicio en la revisadora | saldo tejido D en kg, en 17 |
| 5 | Juntar 2 rollos cortos del mismo baño | 1 rollo final; los dos rollos de rama consumidos |
| 6 | Juntar rollos de baños distintos | error |
| 7 | Entregar con la rama a medio baño | «Liberar lo inspeccionado» → los rollos finales clasificados quedan en existencia y se pueden entregar; la pendiente sigue |
| 8 | Último rollo inspeccionado | la orden en kg se cierra sola |
| 9 | Empaque cambia la calidad | pide motivo; el lote se crea para el producto de la calidad definitiva |
| 10 | Rollo de rama sin inspeccionar en Empaque | error |
| 11 | Pesaje manual sin motivo | error (como hoy) |
| 12 | Costeo del mes | rendimiento = primera ÷ (primera + FE + segundas + desperdicio en m equivalentes) |

## 7. Fuera de alcance

Reprocesos (rollo que regresa a tintorería o rama), QC de laboratorio, mediciones de Rama, Almacén 31 de desarrollos
(José ya lo tiene), Quality Points.
