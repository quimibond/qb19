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

## Negociaciones de precio

Ventas → Cotizaciones de costo → **Negociaciones de precio** (desde 1.8.0). Para
clientes que ya compran: la renovación anual de precios y las peticiones de
rebaja (p. ej. el programa Blockbuster de ContiTech). No sustituye a la
cotización: la cotización es un producto nuevo; la negociación, el precio de lo
que ya se vende.

- **Una negociación por cliente** (la empresa, no el contacto), moneda y
  vigencia (p. ej. 2027-01-01 a 2027-12-31), con las notas de lo que pidió el
  cliente. Mismo ciclo y misma aprobación por puesto que la cotización (el
  ciclo vive en `qb.cotizador.aprobacion` y lo comparten las dos): un precio o
  tramo debajo del costo variable solo lo aprueba «Autoriza precio bajo piso».
- **Una línea por producto**, de tres tipos: precio fijo nuevo; rebaja % sobre
  el precio actual; o **escalera incremental**: precio base + tramos en
  unidades **por mes**, donde el % de cada tramo aplica solo a las unidades que
  caen dentro del tramo, medidos por mes, trimestre (umbrales × 3) o año (× 12),
  con la opción «solo facturas pagadas en plazo».
- **Prellenado** al elegir el producto: el precio vigente de la tarifa del
  cliente (la regla que Odoo aplicaría hoy), el volumen de 12 meses y el
  promedio mensual (facturas publicadas menos notas de crédito; la nota de una
  refacturación cuenta en la fecha de la factura que corrige; las notas de los
  acuerdos no cuentan) y los pisos del último período de costeo (costo
  variable = piso ocioso, costo completo = piso lleno) en la moneda de la
  negociación. El pronóstico anual es editable (el forecast del cliente).
- **Simulador** en la ficha («Simular»): cada producto y el total a 12 meses,
  pronóstico, pronóstico ± 15 % (configurable) y volúmenes libres: descuento al
  año contra el precio actual, % efectivo, % de la compra total del cliente,
  venta neta, contribución sobre costo variable y completo y semáforo del
  último tramo alcanzado. Con la contrapropuesta del cliente (rebaja pareja %
  y sus tramos) pone su descuento al lado.
- **PDF**: «Comparación interna» (propuesta contra contrapropuesta) y «Carta
  para el cliente» con la tabla de tramos, en español o inglés según el idioma
  del contacto de la carta; la carta solo sale aprobada.
- **Al ganar**, con «El cliente aceptó» + medio y fecha (como la cotización):
  en la tarifa propia del cliente (`TARIFA <cliente>`, se crea si no tiene) el
  precio vigente del producto se cierra con `date_end` = inicio − 1 día y se
  crea el nuevo con la vigencia de la negociación. No se borra nada: el
  historial queda en la tarifa. Las fechas van al día completo en UTC (inicio
  00:00:00, fin 23:59:59), como el resto del cotizador.
- La escalera además crea un **acuerdo de volumen** (Ventas → Cotizaciones de
  costo → Acuerdos de volumen). «Calcular periodo» (Gerente de ventas o
  Facturación) y el cron diario (días de espera después del cierre, 5 por
  omisión) suman las unidades del periodo, calculan el descuento por tramo,
  excluyen o solo marcan las facturas no pagadas en plazo (pagada = la última
  conciliación a más tardar en el vencimiento) y crean **una nota de crédito
  `out_refund` EN BORRADOR** relacionada con las facturas (uso G02 y CFDI
  origen `01|UUID…` si está la localización). Nunca se publica ni se timbra
  aquí: Contabilidad la revisa. Correrlo otra vez actualiza la misma línea de
  historial y la misma nota mientras siga en borrador; con la nota publicada el
  periodo ya no se toca.

No confundir con `qb.cotizador.tramo`: esa es la escalera de precio único
(½×, 1×, 2×, 4×) que imprime el PDF de la cotización.

## Parámetros (Ajustes → Ventas → Cotizador de costo)

Los que el brief deja sin definir están **vacíos** y no tienen efecto hasta
que alguien les ponga valor: suplente para aprobar, margen neto mínimo,
descuento de la escalera de volumen.

## Pruebas

`tests/` corre en CI (`--test-tags /qb_cotizador`): flujo con aprobación por
puesto, impresión bloqueada, vencimiento y seguimiento por cron, tarifa al
ganar, importación del cotizador anterior. `test_negociacion.py` (1.8.0)
toma como aceptación el caso ContiTech: Polycotton a US$1.880 con tramos
0–50k, 50–75k 10 %, 75–95k 12 %, >95k 15 % medidos por trimestre da US$91,368 al
año con 1,055,000 m (4.6 % efectivo) y US$22,842 en un trimestre de 263,750 m;
Crep 90 de 1.080 con rebaja de 1 % da 1.0692 con el cambio de tarifa al
2027-01-01; la liquidación crea una sola nota en borrador.
