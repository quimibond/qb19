# Quimibond - Cierre de inventario

Casilla **No valuar en el cierre** en la categoría de producto (Inventario →
Configuración → Categorías de producto, sección Valuación de inventario).

## Por qué

El cierre de inventario de Odoo 19 (`res.company.action_close_stock_valuation`)
y el reporte de valuación toman **todo producto almacenable**. No hay forma
nativa de excluir una categoría. En Quimibond eso:

- capitaliza las refacciones que Dirección decidió llevar a gasto al comprarse
  (115.01.03 debe quedar en cero);
- mete al inventario el activo fijo, cuya factura ya va a 153 (115.01.04);
- reclasifica por ubicación todos los consumos de esas categorías desde el
  último cierre (si nunca hubo uno, toda la historia: 16.6 M y 4.9 M en el
  reporte del 31-oct-2026).

## Qué hace

Para una categoría marcada y todas sus subcategorías:

- `_get_valuation_product_domain`: sus productos no entran al cierre ni al reporte;
- `_get_location_valuation_vals`: sus movimientos no entran a la reclasificación
  por ubicación (filtro por contexto en `stock.move._read_group`);
- `stock_accounting_value`: la cuenta de inventario que **solo** usan categorías
  marcadas no se compara contra el almacén. Una cuenta compartida con una
  categoría que sí se valúa (por ejemplo 115.02.01) se sigue comparando.

Existencias y costo no cambian.

## Lo que no arregla

El valor a una **fecha pasada** (`total_value` con `to_date`) rehace el costo
promedio con todo el historial de movimientos y de cambios de costo. Con los
datos de Quimibond da costos absurdos (WD040Q46JNT159: 90.20 por metro contra
7.81). Los cierres se corren **sin fecha**, el último día del mes, al terminar
la operación: así Odoo usa existencia × costo.
