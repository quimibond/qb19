# Inspección y clasificación de rollos: diseño

Pedido de Dirección General (Jose), 2026-11-03: «al salir de la rama los rollos yo pueda inspeccionar los kgs y
poder consumir los rollos y clasificarlos en calidades y también poder sacar rollos de diferentes tamaños y que ahí
me salgan cuántos kgs y cuántos metros tiene cada rollo después de la inspección».

Base: módulo `quimibond_rollos_bitacora` 19.0.3.0.1 de Jose Sacramento (Consolti), rama `qbtesting`, diseño técnico
v5.4. Este documento dice **qué cambia y qué se le agrega**; Rama (pesaje, lote por rollo, etiqueta, desperdicio),
taras, bitácora, defectos y restricciones de Inspección se respetan tal cual.

## 1. Decisiones (Dirección General, 2026-11-03)

| # | Decisión |
|---|---|
| D1 | La Inspección **se queda en la orden de acabado en kg** (Devanado → Tintorería → Abridora → Rama → Inspección). |
| D2 | Se inspecciona **en cuanto el rollo sale de rama**, con la orden en kg abierta. |
| D3 | **El corte se hace en la revisadora** (Inspección). |
| D4 | Cada rollo que sale es un **rollo final con lote nuevo**; al final cada uno tiene sus kg y sus metros. |
| D5 | ~~**Segundas y desperdicio se venden en kg**; primera y FE en metros.~~ **Sustituida el 2026-11-08 (José; módulo 3.15.0):** Segunda es tela en metros (ubicación 16, como dice el diseño técnico §2, §8 y §16); solo el desperdicio es saldo en kg (17). Sin orden en metros (venta en kg) la Segunda sigue como saldo en kg. |
| D6 | **Solo se juntan rollos del mismo baño.** |
| D7 | **No se entrega nada hasta que se cierre la orden en kg completa** (el baño). No hay cierre parcial. |
| D8 | **Una sola estación: Inspección pesa, clasifica y etiqueta.** No hay operación de Clasificación final en Empaque. Inspección tiene báscula. |
| D9 | **El baño se cierra con el cierre nativo de Odoo** en la orden en kg (supervisor); nadie marca nada en rama. |

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

**Dónde se pesa hoy:** las taras de «Clasificación final» del módulo de José salen del archivo de Quimibond
«INSPECCION TARAS»; el pesaje del rollo terminado ya lo hace Inspección. El centro EMPAQUE (746) no tiene ninguna
orden de trabajo en producción en el último año.

**qbtesting, prueba del 3-nov** (TL/OP-M/00008, 3,500 m de WJ038Q22JNT160): la rama pesó 2 rollos (`00008-0001`
32.1 kg, `00008-0002` 32.5 kg), se inspeccionaron, la orden en kg TL/OP-KG/00008 cerró y los rollos quedaron en 10
INSPECCIÓN apartados por la orden en metros. Falta clasificar.

**Lo que el módulo de José no cubre hoy** (código de 3.0.1):

1. Un rollo de rama = un rollo final con el mismo nombre de lote (`rollo_clasificacion_wizard.action_confirmar`):
   no hay corte ni unión.
2. Todas las calidades salen como la tela en metros al destino del terminado (14 PQ); la calidad solo cambia la
   etiqueta. El ruteo a 15/16/17 está en su lista de pendientes. El costeo mediría todo como primera.
3. Dos toques por rollo: Inspección (sin pesar, etiqueta preliminar) y Clasificación final en Empaque (pesa,
   consume, etiqueta final). La clasificación exige el rollo en inventario, o sea la orden en kg cerrada.
4. Se consume el kg de rama (`_clasif_asegurar_consumo`), no lo que sale.

## 3. Flujo

### Orden de acabado en kg

**1. Rama pesa cada rollo** — sin cambio. Lote por rollo `<orden>-NNNN`, etiqueta chica.

**2. Inspección: la única estación** (orden en kg abierta, en cuanto el rollo sale de rama)

- Escanea el rollo de rama (o varios del **mismo baño** para juntarlos, §4.4).
- Lo pasa por la revisadora: metros, defectos, empalmes (como hoy).
- Por cada **rollo final** que sale (uno si no se corta, varios si se corta):
  - lote nuevo (§4.2);
  - **peso** en la báscula de Inspección menos la tara (IoT o manual con motivo, como en Rama);
  - metros de la revisadora;
  - calidad (PQ / FE / Segunda / Desperdicio) y defectos;
  - **etiqueta final** (PQ, o PNC para FE / Segunda / Desperdicio, el formato de José) con kg y m.
- Cuadre a la vista: kg de los rollos de rama contra kg de los rollos finales; merma con alerta (§4.6).
- No se mueve inventario todavía: los rollos finales quedan «inspeccionados, pendientes de registrar».

**3. Cierre del baño → la orden en metros se registra sola**

- **Con el cierre nativo de Odoo** («Marcar como hecho» en la orden en kg), cuando el supervisor sabe que el baño
  terminó. El bloqueo que ya tiene José se queda: no cierra si queda un rollo sin inspeccionar. Al cerrar, en la
  misma operación, se registra la orden en metros (§4.5).
- Filtro en las órdenes en kg: «Rollos inspeccionados, sin cerrar», para que no se queden baños terminados sin
  cerrar (sus rollos no están en inventario hasta entonces).
- **No hay cierre parcial** (D7): los rollos finales de un baño no existen en inventario ni se entregan hasta que
  el baño cierra.

### Orden en metros

**Sin operación ni captura.** Al registrarse (paso 3): consume los rollos de rama (kg) y produce los rollos finales
con su lote, sus metros y sus kg, cada calidad en su producto y ubicación (§4.3). Se cierra con los metros reales de
primera y FE y los kg de los saldos. Sobrante a existencia; faltante, otra orden (ya va con la primera para planear,
`qb_costeo` 1.9.0).

## 4. Reglas

### 4.1 Rollo final

Modelo nuevo `mrp.rollo.final`: una fila por rollo final, con

- `name` (lote), `bitacora_ids` (rollos de rama de los que sale, Many2many), orden en kg y orden en metros;
- de Inspección: `kg_bruto`, `tara`, `kg_neto`, captura manual y motivo, `metros`, `clasificacion`, `defecto_ids`,
  operador, revisadora, fecha;
- del registro: `product_id` y `lot_id` producidos, `estado` (inspeccionado / registrado).

Un rollo final viene de un rollo de rama (sin corte o con corte) o de varios (unión); no se permite cortar y juntar
en la misma captura.

### 4.2 Lote

- Nombre: `M<último segmento de la orden en metros>-NNNN`, consecutivo por orden en metros (`M00008-0001` de
  TL/OP-M/00008). La `M` lo distingue a simple vista del rollo de rama (`00008-0001`), que en piso conviven. El
  código de barras de la etiqueta final lleva orden y número de rollo, como el de José.
- El nombre se asigna en Inspección (va en la etiqueta final); el `stock.lot` se crea al registrar, para el producto
  que corresponde a la calidad (la tela o el saldo), porque un lote es de un solo producto.
- La liga rollo final → rollos de rama → baño → crudo queda en `bitacora_ids` y en las órdenes; no se codifica en el
  nombre.

### 4.3 Calidad → producto y ubicación

| Calidad | Producto que sale | Unidad | Ubicación |
|---|---|---|---|
| PQ | la tela (producto de la orden en metros) | m | 14 PRODUCTO TERMINADO PQ |
| FE | la tela | m | 15 PRODUCTO TERMINADO FE |
| Segunda | `SALDO DE TELA TEJIDO` (subproducto) | kg | 16 PRODUCTO TERMINADO SEGUNDAS |
| Desperdicio | `SALDO TEJIDO D` (subproducto) | kg | 17 PRODUCTO TERMINADO DESPERDICIO |

- PQ y FE son líneas del terminado con su lote; la FE con destino 15. Segunda y desperdicio son líneas de los
  subproductos de la lista en metros (cantidad 0 en la lista; la real es el peso). Ubicaciones y productos se leen
  de parámetros, no se escriben en código.
- Los dos saldos ya existen en producción con seguimiento por lote (ids 17929 y 19288).

### 4.4 Mismo baño

Baño = orden en kg. Al juntar, todos los rollos de rama deben ser de la misma orden en kg; si no, error.

### 4.5 Registro de la orden en metros

- La orden en metros es la que pidió la orden en kg (origen MTO). El registro corre al cerrar la orden en kg, en la
  misma transacción: si falla, no se cierra nada.
- El cierre de José se queda como está: iguala la orden en kg a lo pesado y consume todo el crudo. No hay cierre
  parcial ni orden pendiente.

### 4.6 Consumo y merma

- La orden en metros consume cada rollo de rama completo (sus kg de rama).
- Merma visible por grupo (rollos de rama ↔ rollos finales): kg de rama − kg netos de los rollos finales. Alerta en
  Inspección si pasa de un parámetro (%), antes de guardar.
- Lo que se consume es lo que existe: nunca más kg de los que tiene el lote.

## 5. Cambios por módulo

**`quimibond_rollos_bitacora` (José):**

1. Inspección como estación única: rollos finales (corte y unión, mismo baño), pesaje con tara de Inspección (la que
   hoy es de «Clasificación final»), calidad, lote nuevo y etiqueta final; desaparece la etiqueta preliminar.
2. Al cerrar la orden en kg con el botón nativo (bloqueado si falta inspeccionar, como hoy), registro automático de la orden en metros; filtro «Rollos inspeccionados, sin cerrar».
3. Producto y ubicación por calidad (§4.3); consumo según §4.6.
4. Sale el botón CLASIFICAR de Empaque.
5. Pruebas de los casos de §6.

**`quimibond_ficha_tecnica_tela` 2.24.0 (hecho):** la lista en metros que genera «Generar listas desde la ruta» lleva
los dos subproductos (cantidad 0, sin costo) y **no lleva operación**; la migración quita de las rutas los pasos
«Clasificación final» (42, todos en EMPAQUE).

**`qb_costeo` 1.10.0 (hecho):** el rendimiento cuenta como merma de la tela los saldos en kg que salen de su
clasificación, convertidos a metros con la ficha; la orden en metros sin operaciones no carga horas (la inspección
va una sola vez, en la orden en kg).

## 6. Casos de aceptación

| # | Caso | Resultado esperado |
|---|---|---|
| 1 | Rollo de rama todo primera, sin corte | 1 rollo final pesado en Inspección, lote nuevo, etiqueta con m y kg; al cerrar el baño, en 14 |
| 2 | Rollo cortado en 250 m primera + 120 m segunda | 2 rollos finales pesados; al cerrar: la tela 250 m en 14 y saldo de tela en kg en 16; rollo de rama consumido completo; merma = kg rama − suma |
| 3 | Rollo todo FE | la tela en m, en 15 |
| 4 | Desperdicio en la revisadora | saldo tejido D en kg, en 17 |
| 5 | Juntar 2 rollos cortos del mismo baño | 1 rollo final; los dos rollos de rama consumidos |
| 6 | Juntar rollos de baños distintos | error |
| 7 | Rollos finales de un baño que no ha cerrado | no están en existencia; no se pueden entregar |
| 8 | Supervisor cierra la orden en kg con todo inspeccionado | la orden en kg se cierra y la orden en metros queda registrada en la misma operación; con un rollo sin inspeccionar, no cierra |
| 9 | Merma arriba del límite | alerta antes de guardar en Inspección |
| 10 | Pesaje manual sin motivo | error (como hoy) |
| 11 | Falla el registro de la orden en metros | no se cierra la orden en kg; mensaje con el motivo |
| 12 | Costeo del mes | rendimiento = primera ÷ (primera + FE + segundas + desperdicio en m equivalentes); horas de inspección solo en la orden en kg |

## 7. Fuera de alcance

Reprocesos (rollo que regresa a tintorería o rama), QC de laboratorio, mediciones de Rama, Almacén 31 de desarrollos
(José ya lo tiene), Quality Points.
