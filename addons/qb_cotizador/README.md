# qb_cotizador — Cotizaciones de costo

Cotizador de la segunda generación del costeo (spec
`docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`, §6 y §6.1). Vive
en **Ventas → Cotizaciones de costo**. Las 75 cotizaciones del cotizador
anterior (`qb_capacidad_costeo`, desinstalado y retirado del repo el
2026-10-22) están aquí con su `legacy_id` y fuente de costo «Importado del
cotizador anterior».

## Cómo se cotiza

1. Cliente, producto existente **o** especificación nueva (gramaje, ancho,
   galga), volumen mensual, moneda y precio al cliente. En divisa el **tipo de
   cambio** sale de Odoo al elegir la moneda («TC de hoy» lo refresca) y se
   puede capturar a mano en borrador; la ficha dice de dónde salió y avisa si
   una cotización en divisa trae TC 1.
2. **Calcular costo**: toma la fila de `qb.costo.unitario` del producto (o del
   hermano) en el último período cerrado (si no hay ninguno cerrado, el último
   con costos, marcado provisional) y la guarda como foto, con calidad y
   fuente. Un desarrollo sin receta se captura a mano (fuente «manual», que
   deja la foto en blanco al elegirla). Sin costo calculado no hay semáforo ni
   márgenes.
   **Receta del desarrollo** (1.7.0): el artículo nuevo que todavía no tiene
   costo se costea con su lista de materiales, explotada hasta lo comprado
   (acabado → teñido → crudo; una fórmula de tintorería → sus químicos). Un
   componente con costo en el período entra con ese costo; uno comprado, al
   precio de compra del período o a su costo estándar (dicho en el desglose).
   La conversión sale de las horas de la ruta × la tarifa del centro; un
   centro sin horas hereda la conversión por kg de tela del hermano. El
   rendimiento es el del hermano y la calidad, media. Si el consumo de tela por
   unidad se aleja más de 10 % del hermano ajustado por ancho y gramaje, la
   cotización avisa.
   **La foto va en la unidad del volumen** (m o kg): si el costo del producto
   está en la otra unidad, se convierte con los kg por metro del costo o, si no
   los trae, con el gramaje × ancho de la cotización; sin ninguno no calcula.
   Si la unidad del volumen cambia después, la cotización lo avisa en rojo y no
   se aprueba hasta recalcular.
   Se convierten costos por m, kg y m² (m² con el ancho); otra unidad (pieza,
   servicio) no se cotiza. Si el peso del costo no cuadra con el código del
   artículo (más de 10 %), aviso y no se aprueba hasta corregir el peso en
   Costos o marcar «Peso revisado».
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

## Unidades del precio en el PDF

En la pestaña Condiciones se marca en qué unidades sale el precio al cliente
(m, yd, kg; varias a la vez). Solo cambia la impresión: el precio aprobado
sigue en la unidad de la cotización. 1 yd = 0.9144 m; m ↔ kg con los kg por
metro del costo o el gramaje × ancho. El default sale del cliente (pestaña
Ventas y compras del contacto); sin nada marcado, la unidad de la cotización.

## Parámetros (Ajustes → Ventas → Cotizador de costo)

Los que el brief deja sin definir están **vacíos** y no tienen efecto hasta
que alguien les ponga valor: suplente para aprobar, margen neto mínimo,
descuento de la escalera de volumen.

## Pruebas

`tests/` corre en CI (`--test-tags /qb_cotizador`): flujo con aprobación por
puesto, impresión bloqueada, vencimiento y seguimiento por cron, tarifa al
ganar, importación del cotizador anterior.
