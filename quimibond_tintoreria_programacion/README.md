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
   - `n = ceil(kg / capacidad)`; el reparto es **por rollos completos** (ver 1.9.0); sin componente de crudo de tejido se conserva el reparto simétrico `kg / n`, todos en el mismo HTJ.
   - Si `n > 1` se usa la división nativa `mrp.production._split_productions` (conserva los enlaces MTO hacia la MO en metros).
   - **Duración de Tintorería (por baño; cada orden es un baño):** se fija en la workorder de Tintorería de cada MO con el primer dato disponible: (1) *Tiempo de ciclo* del artículo en ese jet (proceso del artículo en la ficha técnica; la ficha lo guarda en **horas**, se convierte a minutos); (2) minutos de la fórmula de la BoM en ese HTJ (`tintoreria.formula.tiempo`, menú Fichas técnicas → Tintorería → Tiempos por fórmula y jet); (3) la operación de Tintorería de la BoM, con aviso en el chatter de cada orden: su tiempo es el de Odoo (manual o calculado por historial, 1.6.0), por unidad de la BoM (en las listas en kg, minutos por kg), y se multiplica por los kg del baño (1.5.0). Si no existe ninguno, la programación se bloquea. El asistente muestra los minutos y su origen antes de confirmar.
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
- El asistente ofrece los jets (tipo de máquina «Tintorería») que están **operando**; uno en montaje, en pruebas o fuera de servicio no se ofrece y, si ya estaba escogido, no se confirma (1.7.1).

## Cambios 19.0.1.9.0 (Dirección, 2026-11-11): programar con rollos completos

- **Flujo:** primero se surte el crudo (requisición Alm. 5 → Producción 2, con los lotes/rollos, p. ej. `H13850-0001…0012`) y
  después se programa. Si la orden tiene componente de tela cruda (categoría TEJIDO) y no está surtido, la programación se
  **bloquea** con aviso que nombra la requisición pendiente (o «sin rollos surtidos»). Órdenes sin componente de tejido
  siguen el cálculo aritmético de antes.
- **Reparto:** se parten los rollos **consecutivos** sin cortar ninguno: n inicia en `ceil(kg / capacidad)` y sube si no
  cabe; se minimiza el baño más cargado y luego la suma de cuadrados (kg lo más parejos). Un rollo mayor que la capacidad
  bloquea. Cada baño queda con los lotes exactos de sus rollos.
- **Cantidad:** la orden en kg de cada baño = suma de kg de sus rollos (la dosificación calcula sobre esa cantidad real).
  Una sola máquina = baño completo (p. ej. 394.70 kg, rollos 1-12). La orden en metros se divide proporcional a los kg de
  cada baño (total sin cambio).
- **Asistente:** muestra lote de crudo, kg totales, rollos totales y, al elegir jet, el detalle «Baño N: kg · rango
  (n rollos)». El registro de programación guarda `lote_crudo`, `n_rollos`, `kg_pedido` y `bano_ids`
  (`tintoreria.programacion.bano`).
- `kg_bano` y `kg_crudo_bano` ahora son promedios. Crear programaciones requiere permiso de gerente (igual que el modelo).

## Cambios 19.0.1.8.2 (revisión, 2026-11-08)

- Si la orden en metros no se puede dividir entre los baños (la abastecen varias órdenes, ya empezó, ya no está
  confirmada o también la abastece otra orden en kg), se queda como estaba, pero ahora deja nota en los baños y en la
  orden en metros. Antes no avisaba. Cada baño registra sus metros en ella y deja pendiente para los que siguen
  (rollos 3.3.0). Prueba 74 de rollos.
- Volver a programar no es posible: una orden ya programada se cancela y se vuelve a crear (sin cambio).
- Terminar una operación (`button_finish`) exige la misma Programación de Tintorería confirmada que iniciarla. Antes se
  podía terminar Devanado, Tintorería o Abridora sin iniciar (botón de terminar, Shop Floor o RPC): quedaban «hechas»
  con duración 0, la orden pasaba a «iniciada» y ya no se podía programar (hallazgo de la prueba de punta a punta en
  qbtesting, 2026-11-08).

## Cambios 19.0.1.8.1 (ronda 13 en main, 2026-11-06)

Al dividir la orden en metros se vuelven a ligar **todos** los terminados de cada baño, no solo el del acabado: el
desperdicio de Rama (subproducto en cero de la lista en kg) quedaba ligado a las dos partes de la orden en metros. Por eso
el cierre del primer baño dejaba un pendiente (TL/OP-M/00035-003) y cancelar un baño no cancelaba su orden en metros.
La revisión de cancelación cuenta solo los terminados del acabado.

## Cambios 19.0.1.8.0 (Dirección, 2026-11-06): un baño = una orden en metros

Al dividir la orden en kg en baños, la programación divide también, en la misma proporción, la orden en metros que la
consume (MTO), con la división nativa de Odoo, y liga cada parte a su baño. Cada baño cierra su propia orden en metros
con sus metros reales: ya no queda una orden en metros a medias con un pendiente calculado con el factor de la lista,
ni se reparte la demanda de kg entre baños. Motivo: en la ronda 12 en main la orden de 9,040 m cerró en 10,763 m y el
reparto hizo nacer una orden en kg que nadie pidió (corregido también en rollos 3.6.2).

- La orden en metros original queda como la del primer baño; las demás nacen con el sufijo nativo (`…-002`).
- La entrega del pedido queda ligada a todas las partes (la división nativa conserva el destino del terminado).
- No divide si la orden en metros ya empezó o la abastece otra orden en kg además de estos baños; en ese caso sigue el
  flujo de varios baños para una orden en metros (rollos 3.3.0).
- Un baño sin primera ni FE cancela solo su orden en metros (rollos 3.2.0); la de los otros baños no cambia.
- Cancelar un baño (antes de Rama) cancela también su orden en metros; la de los otros baños no cambia.
- Solo divide con `quimibond_rollos_bitacora` instalado: sin su cierre, cada orden en metros habría que cerrarla a mano.
- Si la división no da una parte por baño, no se programa nada (error).

## Cambios 19.0.1.7.1 (ronda 10 en main, 2026-11-06)

- **No se programa en un jet que no está operando.** El asistente dejaba escoger TINTORERIA 5, en montaje, sin aviso;
  como el jet programado queda fijo, la orden se quedaba en una máquina que no trabaja. Ahora el asistente solo ofrece
  jets operando y, si el renglón ya traía uno que no opera, muestra la advertencia y no confirma.

## Cambios 19.0.1.7.0 (auditoría del flujo, 2026-11-06)

- **El número de baños sale de los kg de crudo que entran al jet**, no de los kg de tela terminada de la orden. El
  crudo es el componente en peso de una categoría de tejido con más kg planeados (la misma regla con la que
  `quimibond_rollos_bitacora` valida el crudo al cerrar). En IWJ038Q22JNT160 entran 1.1505 kg de crudo por kg de
  tela: en TINTORERIA 1 (banda B, 600 kg) una orden de 550 kg de tela cabía en 1 baño, que cargaba 633 kg de crudo;
  ahora son 2 baños de 316 kg (ronda 8 en main, 2026-11-06). La orden se sigue repartiendo en kg de tela, en baños
  iguales.
- El cálculo del crudo (`_kg_carga_jet`) vive en `quimibond_tintoreria_dosificacion` 1.4.0, que con los mismos kg
  dosifica los químicos (g/L × relación de baño) y los colorantes (% sobre el peso cargado).
- El asistente, la programación y la nota de cada orden muestran los kg de crudo por baño. Sin componente de tejido
  se usan los kg de la orden, como antes, y queda un aviso en el chatter.
- La capacidad del jet se toma como kg de crudo cargado: en el flujo anterior la orden de Tintorería consume el crudo
  1 a 1 (TL/OP-TIN/06736: 404.44 kg de WJ035Q22HNT200 para 404.44 kg), y con ese flujo se fijaron las capacidades.

## Cambios 19.0.1.6.2 (ronda 5 de pruebas en main, 2026-11-05)

- Cancelar una orden de una programación lo anota en la programación; si ya no le queda ninguna orden viva, la
  programación pasa a «Cancelada» (antes se quedaba «Programada»).

## Cambios 19.0.1.6.1 (revisión de Consolti, 2026-10-30)

- Redacción del aviso del chatter cuando se usa la operación de Tintorería de la BoM: decía «150.0 min por 1.0 kg,
  500.0 kg por baño», que se leía como 150 min por kg aunque el cálculo da 150 min por baño. Ahora dice los minutos
  por baño y aparte la cantidad base de la lista: «… se usó la operación de Tintorería de la BoM, 150.0 min por baño
  (cantidad base de la lista: 1.0 kg; 500.0 kg por baño)». Sin cambio en el cálculo.

## Cambios 19.0.1.6.0 (revisión de Consolti, 2026-10-28)

- **El jet programado no se mueve.** Desde las fichas 2.20.0 el planificador solo usa alternativas que operan y tienen
  proceso del artículo; si el HTJ asignado aquí estaba en montaje o el artículo no tenía proceso ahí (HTJ5, con aviso),
  la operación se iba a otro jet al planear. Ahora la orden de trabajo de Tintorería de una orden programada
  (`tintoreria_programada`) es «máquina fijada a mano» (`mrp.workorder._ft_maquina_fija`, fichas 2.21.0): el
  planificador la deja en su HTJ y queda solo el aviso. Prueba: `test_jet_programado_no_lo_mueve_el_planificador`.
- **Respaldo de la operación de la BoM sin bloquear.** El tiempo se toma como lo da Odoo (`time_cycle`): manual o
  calculado por historial (antes solo manual, y el modo calculado bloqueaba la programación). Sigue por unidad de la
  BoM y con el aviso en el chatter de cada orden. Solo se bloquea si no hay ningún tiempo (ni ficha, ni fórmula, ni
  operación). Prueba: `test_respaldo_de_la_bom_en_modo_calculado_avisa_en_el_chatter`.

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
