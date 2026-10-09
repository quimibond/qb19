# qb_cotizador — Cotizaciones de costo

Cotizador de la segunda generación del costeo (spec
`docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`, §6 y §6.1). Vive
en **Ventas → Cotizaciones de costo**. No toca `qb_capacidad_costeo`: importa
sus cotizaciones y convive con él hasta que se retire.

## Cómo se cotiza

1. Cliente, producto existente **o** especificación nueva (gramaje, ancho,
   galga), volumen mensual, moneda y precio al cliente. En divisa el **tipo de
   cambio** sale de Odoo al elegir la moneda («TC de hoy» lo refresca) y se
   puede capturar a mano en borrador; la ficha dice de dónde salió y avisa si
   una cotización en divisa trae TC 1.
2. **Calcular costo**: toma la fila de `qb.costo.unitario` del producto (o del
   hermano) en el último período cerrado (si no hay ninguno cerrado, el último
   con costos, marcado provisional) y la guarda como foto, con calidad y
   fuente. Un desarrollo sin receta se captura a mano (fuente «manual»). Sin
   costo calculado no hay semáforo ni márgenes.
3. Pisos, márgenes y semáforo salen del precio sobre esa foto (editar el
   precio los actualiza; recalcular el costo es explícito y deja rastro).
4. **Enviar a aprobación** → el puesto configurado (Director de Finanzas y
   Administración) aprueba o regresa con motivo. En rojo solo el grupo
   «Autoriza precio bajo piso».
5. **Presentada**: validez, seguimiento a Ventas a los N días hábiles, y al
   vencer «Vencida» con actividad para decidir: renovar (revisión nueva),
   ganada o perdida (motivo de lista).
6. **Ganada** (a mano o al confirmar un pedido del mismo cliente y producto)
   + aprobación del cliente con evidencia ⇒ precio en su tarifa.

## Parámetros (Ajustes → Ventas → Cotizador de costo)

Los que el brief deja sin definir están **vacíos** y no tienen efecto hasta
que alguien les ponga valor: suplente para aprobar, margen neto mínimo,
descuento de la escalera de volumen.

## Pruebas

`tests/` corre en CI (`--test-tags /qb_cotizador`): flujo con aprobación por
puesto, impresión bloqueada, vencimiento y seguimiento por cron, tarifa al
ganar, importación del cotizador anterior.
