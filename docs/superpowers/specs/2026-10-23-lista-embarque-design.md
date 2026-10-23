# Lista de embarque en Odoo — diseño

Fecha: 2026-10-23. Estado: **propuesta, pendiente de validar con Almacén, Logística y CxC** (preguntas al final).

Objetivo: que la lista de embarque (F-IT-P-A07-01-03 nacional, F-IT-P-A07-01-04 exportación) y el EDI 856 de
Shawmut salgan de Odoo con los rollos que de verdad se cargaron, en lugar de capturarse a mano en Excel.

## 1. Evidencia

### 1.1 Procedimientos (control de documentos del SGI en Odoo)

| Fuente | Qué dice |
|---|---|
| **P-A07 Rev. 05 (jul-2026) §4.1.1.4** | El producto terminado llega de Inspección con el F-P-I01-01 «Reporte de inspección y empaque»; el auxiliar de registros lo verifica y lo ingresa «capturando rollo por rollo». |
| **P-A07 §4.1.2.5** | Al embarcar, Almacén elabora la **F-IT-P-A07-01-03 «Lista de embarque»** o la **F-IT-P-A07-01-04 «Lista de embarque de exportación»**. La factura firmada por el transportista y la lista original se escanean a la carpeta de Drive «Facturas» y el asistente las sube a Odoo en el pedido. |
| **P-A16 Rev. 01 (jul-2026) §4.2, §4.3, §4.7** | Logística pide los datos de la unidad (placas, operador, línea) en F-P-A-16-02; despachada la unidad, manda al cliente evidencia de carga, factura y **packing list**; el expediente de exportación lleva el packing list. |
| **DAT-P-A16-01 Plan de embarques** | Datos obligatorios antes de cargar: empresa transportista, operador, placas del tracto **y de la caja**, licencia vigente. |
| **DAT P-I01-09 (FXI)** | Cada rollo se etiqueta con código, lote y rollo, longitud (m / yd), peso (kg / lb) y ancho; el ancho en cm son los tres últimos dígitos del código (WD3846NT**168**). Metros por rollo por planta: Juárez y Cuautitlán 1000 ± 100 m, Santa Teresa 1200 ± 200 yd. |
| **F-IT-P-A07-01-03 / -04 (Rev. 01 ago-2025)** | Columnas nacional: P., folio, N. rollo, artículo, m, kg, subtotales. Exportación: además m², yardas y libras. Pie: transporte, sello, placas, licencia, hoja X de Y y cinco firmas: elaboró (almacén), revisó (encargado de almacén), autorizó (jefe de inventarios y almacenes), vigilancia, transportista. Catálogo de nombres en la hoja «Nombres». |
| **F-IT-P-A07-01-06** «Control de rollos por embarque» | N.º de rollos y firma de oficina, almacén, vigilancia y chofer por embarque. |
| **F-IT-P-A07-01-07 / -08** | Control de sellos de embarque nacional y de exportación. |
| **F-P-I01-01** (Inspección) | Por rollo: lote, jumbo, metros, **peso del rollo** y clasificación (PQ / FE / 2DA / DESP), contra la especificación de peso mínimo / media / máximo. |

**Hallazgo documental:** las listas que circulan traen la clave **F-IT-P-A05-01-07**, que ya no es la vigente
(la familia P-A05 no tiene procedimiento; ver `docs/sgi/transicion/formatos-bloque-3.md` punto 5). Y el mapeo
de formatos del SGI liga las entregas a **F-P-A16-01**, que es «Citas» (`sgi_format_map_data.xml`, nota
«pendiente de que MAST confirme»). Con este diseño se corrige a F-IT-P-A07-01-03 / -04.

### 1.2 Correo (memoria, últimos 12 meses)

- ~2,000 archivos «LE-…» de Almacén Toluca, Daniel González, Gustavo Delgado, Cynthia Santana, Melanny Granados
  y Dario: LE-LEAR, LE-957 SHAWMUT SILAO, LE-914 FXI JUÁREZ, LE-817 FXI STA TERESA, LE-878 SALTILLO LAMINATION,
  LE-929 ORIGINALES SHYLA, LE-949 COSMO MODA… Folios: **LE-957 (16-jun-2026) → LE-1811 (23-oct-2026)**, unos
  850 en cuatro meses.
- **EDI 856 Shawmut Silao**: 105 archivos de Sandra Dávila a Shawmut (L. Reyes), uno por embarque. Plantilla
  Jomar: plan 12, proveedor 3254, PO 12033, unidades YD, BOL; **una fila por rollo**: item del cliente, Roll ID
  (`20005J2`), lote (`20005J`), cantidad neta y bruta en yardas. «WARNING: EDI MACRO WILL NOT WORK IF FORMAT IS
  CHANGED».

### 1.3 Odoo (producción, consultas del 2026-10-23)

- **Las ventas en metros ya salen rollo por rollo.** Del 1-jul al 23-oct-2026: 757 entregas en metros,
  11,785 líneas, **11,412 paquetes distintos** (paquete = rollo, lote = partida). En existencias: producto
  terminado PQ 3,207 registros con 2,887 paquetes; FE 2,364 con 1,551; segundas 763 con 756.
- **Los rollos entran a producto terminado por «Toluca: Liberar»** (7,489 líneas desde el 1-sep), desde la
  ubicación Liberación: ahí captura el auxiliar «rollo por rollo».
- **Las ventas en kg pierden los rollos.** 139 entregas en kg (631 líneas) sin paquetes. Ejemplo LE-957: la
  «Conversión de unidades» TL/CVU/02230 consume los rollos 20005 002 (505 m) y 20005 003 (500 m) del
  WJ053Q22JNT160 y produce 84.1 kg del IWJ053Q22JNT160 en **un lote llamado «LE-957»**; la entrega
  TL/OUT/13279 sale con ese lote. Así siguen en octubre (LE-1705, LE-1798, LE-1811).
- **El peso por rollo no está en Odoo.** El paquete tiene `shipping_weight = 0` y `x_studio_metros_reales = 0`;
  el lote no tiene campo de peso. Ventas lo escribe como nota del pedido: PV16118 «1X41.60,1X42.50»,
  PV16121 «1X19.40» — los mismos kg de la LE-957.
- **Item y PO del cliente** (ITEM 242803, PO 12033) no están en Odoo: el pedido trae `client_order_ref` «1573».
- m² sale del ancho: 505 m × 1.60 m = 808 m² (LE-957). Yardas = m / 0.9144; libras = kg × 2.20462.

### 1.4 Modelo de producción (Dirección, 2026-10-19/21)

`quimibond_ficha_tecnica_tela/README.md`, «Modelo de producción vigente»: todo en kg hasta la Clasificación
final; la orden de acabado en kg **se cierra con los rollos pesados e inspeccionados**; la venta en kg sale del
«I…» sin conversión; la venta en metros pasa por la Clasificación final con el mismo lote. **La conversión de
unidades desaparece.** El diseño tiene que servir hoy y después de ese cambio.

## 2. Decisiones

| # | Decisión | Evidencia |
|---|---|---|
| D1 | **La lista de embarque es un documento propio** (`qb.embarque`) que agrupa las entregas de un camión, no un reporte de una sola entrega. | Una LE junta varias entregas (LE-957 = TL/OUT/13259 + 13279 + 13280, todas con lote «LE-957») y varias facturas (EDI «INV2026060121;0123;0124»). |
| D2 | **Sus renglones son los rollos (paquetes)** que salen, con lote, rollo, artículo, m, m², yd, kg y lb. | Formatos -03/-04, EDI 856, 11,412 paquetes ya en las entregas en metros. |
| D3 | **El peso va en el rollo** (campo `qb_peso_kg` en `stock.package`) y se captura **donde ya se captura el rollo: en «Liberar»**, con el F-P-I01-01 en la mano. Aviso si sale de la especificación de peso. | P-A07 §4.1.1.4; F-P-I01-01 trae «peso rollo»; hoy se pierde (paquete en 0, nota en el pedido). |
| D4 | **Ventas en kg (mientras exista la conversión):** Almacén arma la LE escaneando los rollos en metros; el botón «Convertir» crea la conversión de unidades con esos rollos, con kg = suma de sus pesos y lote = folio LE (como hoy, pero sin teclear). La LE conserva los rollos. | TL/CVU/02230; lotes «LE-xxx» en 139 entregas. |
| D5 | **Con kg primero**, el peso llega solo desde Inspección y los rollos llegan vivos a la entrega: la captura en «Liberar» y el botón «Convertir» se apagan por ajuste, sin cambiar el documento. | Modelo de producción vigente. |
| D6 | **Folio LE-#### con secuencia propia** que sigue la numeración actual. | LE-957 → LE-1811. |
| D7 | **Nacional o exportación** según el destino (país del cliente o factura con comercio exterior), con cambio manual. | -03 vs -04. |
| D8 | **Datos del transporte en la LE**: transportista, operador, licencia, placas del tracto y de la caja, sello(s), ruta. El sello no se repite entre LE (reemplaza el control de sellos -07/-08). | DAT-P-A16-01, F-IT-P-A07-01-07/-08. |
| D9 | **Firmas como usuarios** (elaboró, revisó, autorizó) más vigilancia y transportista por nombre; la LE escaneada firmada se adjunta y **se copia a las entregas y facturas**. | P-A07 §4.1.2.5 (hoy: Drive «Facturas» → Odoo a mano). |
| D10 | **Artículo del cliente** (item, PO abierta y unidad del EDI) en una tabla por cliente y producto. | ITEM 242803 / PO 12033 no existen en Odoo. |
| D11 | **EDI 856 Shawmut** como exportación .xlsx con la plantilla Jomar exacta, configurable por cliente (plan, proveedor, unidades). | 105 EDI de Sandra; la macro no acepta cambios de formato. |
| D12 | **Liga con pedimentos:** en exportación, el PDF de la LE cumple el documento «Packing list» del expediente ligado a sus entregas (puente opcional si `qb_pedimento` está instalado). | P-A16 §4.7; checklist de exportación de `qb_pedimento`. |

## 3. Diseño

**Módulo nuevo `qb_embarque`** (depende de `stock`, `sale_stock`; puente `auto_install` con `qb_pedimento`).

```
qb.embarque                  (mail.thread)  folio LE-####, fecha, destino, tipo nacional/exportación,
                             picking_ids (M2M), transportista, operador, licencia, placas tracto/caja,
                             sello, ruta, elaboró/revisó/autorizó (res.users), vigilancia, estado
                             borrador → cargado → salió, adjunto firmado
qb.embarque.rollo            embarque, package_id, lot_id, product_id, artículo del cliente, PO, item,
                             m, m², yd, kg, lb (guardados al cargar: la LE es evidencia histórica)
qb.embarque.articulo.cliente partner, producto → item, PO abierta, unidad EDI
stock.package                + qb_peso_kg (Peso del rollo, kg)
stock.move.line              + qb_peso_kg en «Liberar» (escribe en el paquete)
```

- **Armar la LE:** desde las entregas listas (botón en la entrega o en Embarques) o escaneando rollos. Los
  renglones salen de las líneas con paquete; m² con el ancho de la ficha de acabado o, si falta, el de los tres
  últimos dígitos del código.
- **Validaciones al pasar a «Cargado»:** todo rollo con peso, sello y placas capturados, ningún rollo en otra LE.
- **PDF** F-IT-P-A07-01-03 / -04 con subtotal por artículo, hoja X de Y y el pie de firmas.
- **«Salió»:** adjunta el PDF a entregas y facturas; si hay pedimento ligado, cumple el packing list.
- **EDI 856:** botón en la LE (solo clientes configurados), Roll ID por regla del cliente.

## 4. Fases

1. **Peso por rollo en «Liberar»** y LE para ventas en metros (PDF -03 / -04, transporte, firmas, adjunto).
2. **Ventas en kg:** LE con «Convertir» automático.
3. **EDI 856 Shawmut** y artículo del cliente.
4. **Kg primero:** el peso viene de Inspección; se apagan la captura en «Liberar» y la conversión.

## 5. Preguntas antes de construir

1. **¿Inspección pesa todos los rollos** (PQ y FE) y el peso llega a Almacén en el F-P-I01-01? ¿Lo capturamos en
   «Liberar» (auxiliar de registros) o en Inspección?
2. **El kg de la LE, ¿es neto o bruto** (con tubo y bolsa)? El EDI pide neto y bruto.
3. **Roll ID del EDI:** `20005J2` = partida + «J» + número de rollo. ¿La «J» es fija o depende del artículo?
   (CxC, Sandra).
4. **Item y PO de cada cliente:** ¿quién tiene la lista (Shawmut, FXI, Lear…)? ¿Otros clientes piden EDI?
5. **Firmas:** ¿siguen G. Delgado (revisó), C. Santana (autorizó), vigilancia J. Martínez / J. Camacho?
6. **MAST:** confirmar que la clave vigente es F-IT-P-A07-01-03 / -04 y corregir el mapeo de la entrega
   (hoy F-P-A16-01 «Citas»).
7. Los ~320 registros de PT PQ sin paquete y el PT FE con lote = rollo: ¿rollos sin etiquetar o tela que no se
   maneja por rollo?
