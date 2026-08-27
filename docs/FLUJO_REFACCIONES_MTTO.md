# Flujo de consumo de refacciones de mantenimiento (acuerdo 26/08/26)

Configuración en Odoo para el control de refacciones y el costo de las órdenes de
mantenimiento, conforme al flujo acordado (`flujo_consumo_de_refacciones_260826.pdf`):
las refacciones se dan de salida al gasto conforme llegan, y el control posterior
(entrega a mantenimiento → consumo por solicitud) se lleva en ubicaciones
**virtuales**, sin efecto contable adicional.

## Mecánica de Odoo 19 (verificada contra el código fuente de `stock_account`)

El esquema de valuación de Odoo 19 cambió respecto a versiones anteriores. Reglas
que gobiernan este flujo (`stock_account/models/stock_move.py`):

1. **Un movimiento de stock solo genera asiento** si se cumplen TODAS:
   - el producto es almacenable y su categoría tiene valuación **Perpetua**
     (`real_time`); con valuación **Periódica** nunca hay asiento por movimientos;
   - el movimiento cruza la frontera interna↔externa (interna/tránsito de la
     compañía vs. virtual/proveedor/cliente);
   - la ubicación virtual involucrada tiene **Cuenta de valoración de existencias**
     (`valuation_account_id`) configurada.
2. El asiento de salida es: **cargo** a la cuenta de la ubicación virtual destino,
   **abono** a la cuenta de valuación de la categoría del producto. La contrapartida
   sale de la **ubicación** (una sola cuenta fija), no de la categoría del producto.
   Config estándar NO puede cargar "a la cuenta de gasto de la categoría" con una
   sola ubicación destino.
3. **Virtual → virtual nunca genera asiento** (ninguna de las dos es valuada).
4. Con valuación **Periódica**, la factura de proveedor carga directo la **cuenta de
   gasto de la categoría** (a costo exacto de factura). Con **Perpetua**, la factura
   carga la cuenta de valuación de la categoría (115.01.03 Inventario refacciones).
5. Las reglas de almacenamiento (putaway) solo enrutan hacia hijos **internos**
   (`child_internal_location_ids`): no sirven para auto-enrutar hacia
   sub-ubicaciones virtuales por categoría.
6. Cambiar una categoría de Periódica a Perpetua **no** genera asientos
   retroactivos; solo cambia el comportamiento futuro.

## Configuración aplicada (27/08/26, vía MCP)

| Objeto | ID | Detalle |
|---|---|---|
| Ubicación `Virtual/Refacciones Mantenimiento` | 358 | usage=Pérdida de inventario, cía. 1, cuenta 504.01.0005 (inerte mientras las categorías sean periódicas) |
| Ubicación `Virtual/Consumo Refacciones Mantenimiento` | 359 | usage=Pérdida de inventario, cía. 1 |
| Tipo de operación `Salida Refacciones a Gasto (MTTO)` | 264 | `REF-GTO`, TVAR/Existencias → 358. Candidato a archivarse si se decide reutilizar `ENT-REF` retargeteado a 358 |
| Tipo de operación `Consumo Refacciones MTTO` | 265 | `REF-CON`, 358 → 359. La **Solicitud de mantenimiento va en "Documento origen"** para costear por orden |
| Traslado `TVAR/REF-GTO/00001` (**borrador**) | 97709 | Migración inicial: 145 productos, ~$701,158 a costo promedio, TVAR/Existencias → 358. Sin líneas de Empaque (única familia perpetua) → validarlo no genera ningún asiento |

Estado del stock al 27/08/26: $701,158.52 de refacciones en TVAR/Existencias
(ya dadas de baja contablemente vía factura, por la valuación periódica); el
histórico de salidas `ENT-REF` está acumulado en Virtual/Scrap.

## Flujo operativo

```
Recepción OC (IN) → TVAR/Existencias                       [factura → gasto por categoría]
Salida conforme llegan → Virtual/Refacciones Mantenimiento  [control: en poder de MTTO]
Consumo por solicitud (REF-CON) → Virtual/Consumo           [control: consumida; Origen = solicitud]
Devolución de pieza no usada: botón Regresar sobre la salida
```

El costo por orden de mantenimiento = suma del valor (`value`, a costo promedio) de
los movimientos `REF-CON` cuyo Origen es la solicitud.

## Esquema contable definitivo (decidido 27/08/26)

**Solo Bolsas (326) y Tubos (328) llevan inventario** en valuación Perpetua: su
factura carga 115.01.03 y su consumo lo descarga. **Todo lo demás del árbol
"Refacciones y Consumibles" es Periódico**: el gasto se registra directo al
facturar la compra, por categoría y a costo exacto, y los traslados nunca generan
asientos — el flujo de mantenimiento es control puro de cantidades.

Ajustes aplicados ese día vía MCP:

- Se regresaron a Periódica: Agujados (336), Consumibles de cómputo (337) y
  Despensa (338) — alguien las había puesto en Perpetua — además de Cintas (327)
  y el padre Empaque (317), por instrucción de José ("la cinta no lleva
  inventario, solo el tubo y la bolsa"). No hizo falta crear categorías nuevas.
- Con esto el traslado de migración 97709 puede validarse **en cualquier
  momento**: cero asientos garantizado.
- **Pendiente para contabilidad:** quedaron **$125,955.90** cargados en 115.01.03
  por facturas de Agujados registradas mientras esa categoría estuvo en Perpetua
  (detalle producto por producto en el chatter del picking 97709). Requiere
  reclasificación manual Dr 504.01.0007 AGUJADOS / Cr 115.01.03. Cintas se
  verificó sin saldo: no requiere nada.

## Reglas fijas

- **Bolsas y Tubos** (únicas familias con valuación perpetua) jamás pasan por los
  tipos de operación de mantenimiento: un traslado suyo a la ubicación 358 sí
  generaría asiento (cargo a 504.01.0005, que no es su cuenta). Se consumen por
  sus flujos de producción.
- Las existencias virtuales se consultan filtrando ubicación = Virtual/Refacciones
  Mantenimiento (es inventario de control, ya sin valor en libros).
