# 2026-10-07 — Rollos pesados sin línea de movimiento (H13915, H13637, H13669)

**Síntoma (Mariano IT, 7-oct 9:02):** en TL/OP-TEJ/H13915 el Historial de
Pesaje trae 12 rollos (432.64 kg) pero Movimientos de inventario solo muestra
los rollos 0011 y 0012. «Se registran algunos sí y unos no».

## Causa

No es un desarrollo de qb19 (`quimibond_sgi_pesaje` está desinstalado en
producción y ningún módulo propio escribe en `stock.move.line`). Es una
interacción entre `pesaje_rollos_tejido` (Consolti) y el core de Odoo 19:

1. El módulo mantiene en **0** la demanda (`product_uom_qty`) del movimiento
   del terminado para no generar rollo de ajuste, y la vuelve a escribir en 0
   en cada pesaje si la encuentra distinta.
2. Al editar la **Cantidad a producir** de una OF en progreso, el core vuelve
   a poner esa demanda = `product_qty` (lo hace con `do_not_unreserve=True`).
3. En el siguiente pesaje el módulo escribe `product_uom_qty = 0` **sin** ese
   contexto. `stock.move.write()` del core ve que la demanda nueva es menor
   que la cantidad ya registrada y llama a `_do_unreserve()`, que **borra
   todas las líneas no marcadas `picked`**: los rollos pesados hasta ahí.
4. Las líneas eran borrables porque el 19-ago-2026 Consolti quitó el
   `picked: True` de las líneas de rollo (`_do_unreserve` respeta las
   `picked`).

Evidencia en H13915: Carlos Vargas cambió la cantidad 450 → 398.62 el 6-oct
10:51; el pesaje del rollo 0011 (2:05 p.m.) borró 0001–0010. Mismo patrón en
H13637 (edición 10:34, quedaron 5 de 10) y H13669 (edición 5-oct 8:25,
quedaron 7 de 13). `qty_producing` y `qty_produced` de la orden de trabajo
conservan el peso de todos los rollos; solo faltan las `stock.move.line`.

## Corrección

`pesaje_rollos_tejido` 19.0.1.4.2: las dos escrituras de la demanda del
terminado (`action_register_roll_with_weight` y `button_mark_done`) van con
`with_context(do_not_unreserve=True)`, igual que el core.

## Recuperación

`tools/pesaje_recuperar_lineas_rollos.py` en `odoo-shell` de producción:
`detectar(env)` lista las OFs con rollos sin línea; `recuperar(env, [...])`
recrea las líneas desde el Historial de Pesaje (en seco por defecto,
`aplicar=True` confirma); `desarmar(env)` deja la demanda en 0 con el contexto
correcto en las OFs en progreso (H13891 y TL/CONV-ART/00970 estaban a un
pesaje de perder sus rollos). Solo se recrea un rollo cuyo lote existe y no
tiene línea en ningún movimiento; no se toca `qty_producing`.

## Operación mientras no esté desplegado

No editar la Cantidad a producir de una OF que ya tenga rollos pesados; si hay
que ajustarla, hacerlo al final, antes de Producir (el módulo ya iguala la
cantidad al total pesado en ese paso).
