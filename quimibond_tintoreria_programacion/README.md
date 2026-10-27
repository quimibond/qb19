# quimibond_tintoreria_programacion

Programación de Tintorería (Odoo SH 19) — diseño técnico v5.3, §17.

## Qué hace
1. Las MOs del Producto A (kg; **código de la tela acabada con «I» al frente**, DAT P-D02-01 rev. 05) llegan por MPS/MTO por el total; **sin** calcular tamaño de baño.
2. **Manufactura → Programar Tintorería** (o el botón en la MO / acción de lista): Tintorería asigna el HTJ de cada orden.
3. Al confirmar:
   - **Rendimiento:** el de **tejido** del producto, `product._ft_rendimiento()` (módulo de fichas). Con él se obtiene la banda A/B/C usando `GRUPOS` de las fichas (A 3–<7, B 7–<11, C 11–16; el límite inferior pertenece a la banda).
   - **Capacidad por baño** (fuente única: configuración vigente del jet en las fichas técnicas):
     1. si el artículo tiene «kg por carga» en ese jet, ese valor manda;
     2. jet con bandas capturadas: capacidad de la banda (no mayor que la capacidad general); se **bloquea** si la banda es N/A o el rendimiento cae fuera de las bandas;
     3. jet sin bandas capturadas (p. ej. HTJ5): capacidad general, con aviso en el chatter.
   - `n = ceil(kg / capacidad)`; reparto **simétrico** `kg / n` (el redondeo cae en el último baño), todos en el mismo HTJ.
   - Si `n > 1` se usa la división nativa `mrp.production._split_productions` (conserva los enlaces MTO hacia la MO en metros).
   - **Duración de Tintorería (por baño; cada orden es un baño):** se fija en la workorder de Tintorería de cada MO con el primer dato disponible: (1) *Tiempo de ciclo* del artículo en ese jet (proceso del artículo en la ficha técnica; la ficha lo guarda en **horas**, se convierte a minutos); (2) minutos de la fórmula de la BoM en ese HTJ (`tintoreria.formula.tiempo`, menú Fichas técnicas → Tintorería → Tiempos por fórmula y jet); (3) la operación de Tintorería de la BoM, con aviso en el chatter: su tiempo es el de Odoo, por unidad de la BoM (en las listas en kg, minutos por kg), y se multiplica por los kg del baño (1.5.0). Si no existe ninguno, la programación se bloquea. El asistente muestra los minutos y su origen antes de confirmar.
   - Se asigna el HTJ a la workorder de Tintorería de cada MO → `quimibond_tintoreria_dosificacion` recalcula g/L y % con la RB del HTJ.
   - Se crea un registro `tintoreria.programacion` con el detalle.
4. **Validación:** sin programar, `mrp.workorder.button_start`, `mrp.production.action_start` y `button_mark_done` bloquean la orden.

## Campos nuevos en `mrp.production`
- `requiere_programacion_tintoreria` (calculado y guardado): el producto es el «Acabado en proceso» (`product_proceso_id`) de alguna **ficha técnica de acabado** y la BoM tiene operación de Tintorería (`bom._operacion_tintoreria()`).
- `tintoreria_pendiente`, `tintoreria_programada`, `tintoreria_programacion_id`.

## Requisitos de datos
- El producto en kg debe estar ligado a una **ficha de acabado** (`product_proceso_id`) y esa ficha a una **ficha de tejido** con peso y ancho (de ahí sale el rendimiento). Sin ficha de acabado la orden **no** requiere programación y el asistente responde «Selecciona órdenes pendientes de programar Tintorería».
- El campo se calcula al **crear** la orden: si la ficha se liga después, las órdenes ya creadas no se recalculan solas.
- El jet debe tener configuración vigente en las fichas de máquina (capacidad general y/o por banda).
- El asistente ofrece los centros de trabajo cuyo nombre empieza con «Tintoreria» (HTJ1 a HTJ5).

## Cambios 19.0.1.5.0

- El tiempo de respaldo de la operación de Tintorería de la BoM (cuando no hay tiempo de ciclo del artículo en el jet
  ni tiempo de la fórmula en ese HTJ) se tomaba como minutos por baño. En Odoo ese tiempo es por unidad de la BoM, y
  las listas en kg lo traen en minutos por kg (unos 0.5): un baño de 500 kg salía de medio minuto. Ahora se calcula
  como Odoo (`_minutos_operacion_bom`): preparación + limpieza + ciclos × tiempo, entre la eficiencia del HTJ, con
  los kg del baño (0.5 min/kg × 500 kg = 250 min). El aviso en la orden dice el tiempo, la unidad y los kg usados.
- El tiempo de la ficha y el de la fórmula siguen siendo por baño, sin cambio.
- Prueba: `test_tiempo_de_la_operacion_de_la_bom_va_por_kg`.

## Cambios 19.0.1.4.0

- La fórmula de tintorería (Fichas técnicas → Tintorería → Fórmulas) trae la pestaña «Tiempos por jet»: los minutos
  por baño de esa fórmula en cada jet se ven y se capturan ahí (`tintoreria.formula.tiempo_ids`). La lista «Tiempos
  por fórmula y jet» suma la columna de horas por baño.
- Lista de programaciones: totales de kg y de baños, y las canceladas en gris.

## Cambios 19.0.1.3.0
- El menú de tiempos pasa a **Fichas técnicas → Tintorería → Tiempos por fórmula y jet** (antes Fabricación → Configuración → Tiempos de Tintorería), junto a las fórmulas. Lo ven los usuarios de Fabricación en solo lectura y lo editan los jefes. «Programar Tintorería» y «Programaciones de Tintorería» no se mueven.

## Cambios 19.0.1.2.0
- Nuevo modelo `tintoreria.formula.tiempo` (fórmula × HTJ → minutos por baño), con lista editable y menú de configuración. Carga inicial de ejemplo en `data/tintoreria.formula.tiempo.csv` (NATURAL001/004/005 y BLANCO001, de `tiempos tintoreria.xlsx`; importar con Importar registros).
- La programación fija la duración esperada de la workorder de Tintorería (un ciclo por orden) y guarda `minutos_bano` y `origen_tiempo` en `tintoreria.programacion`.
- El asistente filtra los HTJ por **tipo de máquina «Tintorería»** (antes por nombre).

## Cambios 19.0.1.1.1
- `_check_programable`: los estados de la workorder en Odoo 19 son `blocked / ready / progress / done / cancel`; se sustituyen los `pending / waiting` de versiones anteriores (antes toda orden con operaciones bloqueadas se consideraba «iniciada»).
- Wizard: `_rendimiento_acabado()` (inexistente) → `_rendimiento_tejido()`.
- Manifest y README alineados con el diseño v5.3 (prefijo «I», fuente de capacidades en las fichas, bandas continuas).

## Pendiente (siguientes fases)
- **Reprogramación con merge** (antes de iniciar).
- Antes de programar, la duración de la workorder de Tintorería sigue siendo la que calcula la BoM (se corrige al programar).

## Pruebas
```
odoo-bin -d <base_de_pruebas> -i quimibond_tintoreria_programacion --test-tags /quimibond_tintoreria_programacion --stop-after-init
```
