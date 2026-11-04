# quimibond_rollos_bitacora

Taras por producto y operación, bitácora de rollos, pesaje en Rama, Inspección y desperdicio de la MO en kg.
Diseño técnico v5.4 (§3.2, §8, §9, §10, §12, §13, §16). Entregas: **Rama** (1.x) e **Inspección + desperdicio** (2.0).

## Qué hay
- `mrp.tara.producto`: Manufactura → Configuración → Taras por producto. Rama y Clasificación final.
  Botón **Cargar taras iniciales** (66 de Rama, 36 de Clasificación final; archivos en `data/`).
  Empata por referencia interna: primero el «I…» (kg), luego el código tal cual.
- `mrp.rollo.bitacora` (+ `mrp.rollo.medicion.rama`): Manufactura → Reportes → Bitácora de rollos.
- Botones en la lista de operaciones (como PESAR en tejido), solo en operaciones listas o en proceso:
  - **PESAR ROLLO** (Rama): bruto − tara = neto, lote `<último segmento de la MO>-<NNNN>`, etiqueta ZPL
    solo con el código de barras. La ventana se cierra al imprimir la etiqueta.
  - **PESAR DESPERDICIO** (Rama): un solo pesaje por orden del subproducto Desperdicio Tejido (peso real,
    báscula IoT o captura manual). Requiere el subproducto en la lista de materiales del «I…». Si el
    subproducto lleva seguimiento por lote, usa el lote mensual de la planta («MM-AAAA») y lo crea si no existe.
  - **INSPECCIONAR** (Inspección): se escanea la etiqueta de Rama; clasificación inicial (PQ/FE/2das/DESP),
    no. de empalmes, metros, defectos, máquina (Revisadora) y operador (número de empleado). Imprime la
    etiqueta preliminar F-P-I01-02 (ZPL; Kgs = neto de Rama). Inspección no pesa y no tiene QC.
- La operación de Inspección y la MO **no cierran** si queda algún rollo sin inspeccionar.
- `calidad.defecto.tela`: Configuración → Defectos de tela. 64 siglas de Tejido/Acabado cargadas desde
  `CATALOGO DE DEFECTOS.xlsx` (hoja ACABADO; incluye MERMA). Las siglas de un rollo se separan con espacio.
- `mrp.inspeccion.restriccion`: Configuración → Restricciones de Inspección (botón **Cargar restricciones
  iniciales**: el centro 9 no revisa «WD» y revisa solo WN075Q66JBL205 y WJ070Q66JBL205). Se evalúa sobre el
  código sin el prefijo «I» al registrar la inspección.
- `es_desarrollo` en la MO (pestaña Rollos); la MO en kg lo hereda de la de metros por MTO.
- En las órdenes con Rama se oculta el lote único de la orden (los lotes son los de cada rollo).
- Ajustes de Manufactura: alertas de metros y tamaño de las etiquetas de Rama y preliminar.

## Clasificación final (3.0)
- Botón **CLASIFICAR** en la lista de operaciones de la MO en metros (operación en centro EMPAQUE).
- Se escanea el lote-serie del rollo de A (debe estar inspeccionado, tener existencia en el origen del consumo
  —Ubic. 10— y no haber sido clasificado). Segundo pesaje con la tara de «Clasificación final» (kg finales =
  referencia), metros finales (los de Inspección; si se cambian pide motivo), clasificación definitiva y defectos.
- Al registrar: lote de B con el mismo nombre del de A y cantidad = metros finales; consumo del **kg real** del lote
  de A (la demanda de la lista de materiales es teórica); alertas 1 y 2 en la bitácora (se avisan antes de guardar).
- Etiqueta final ZPL: **PQ** (neto/bruto del segundo pesaje, 1 código de barras `Metros;Peso Neto;OF;No. Rollo`) o
  **PNC** FE/Segunda/Desp (kilos y libras, siglas de defectos, sufijo `..` / `.` / `*` en el código, 2.º código
  `No. Rollo + siglas`). Tamaño provisional 812x1218; parámetros `quimibond_rollos_bitacora.etiqueta_final_ancho/alto`.
- Color: decodificado de las posiciones 10-11 del código (DAT P-D02-01); ancho: ficha de acabado (`ancho_acabado`) o,
  si no hay, posiciones 12-14 del código. La clave corta del producto ya no se usa.
- Campo **Siglas** en el empleado: Manufactura → Configuración → Siglas de operadores (botón «Cargar siglas
  iniciales», archivo MAQUINA Y OPERADOR INSPECCION). Se imprimen las del operador que inspeccionó el rollo.
- Cierre de la MO en metros: cantidad = suma de rollos, consumo de A = kg reales por lote (se iguala la demanda).

## Pendiente (siguientes entregas)
Ruteo a 14/15/16/17/31 al cerrar la MO de B, Quality Points, QC de laboratorio, Saldo Tejido D, captura de las
mediciones de Rama.

## Puntos a verificar al probar
- El producto en kg debe tener seguimiento por lote.
- Cierre de la MO en kg: se porta el patrón de `pesaje_rollos_tejido` (demanda en 0 al pesar, igualar la
  cantidad antes del cierre) pero **sin escalar los componentes**: la tela cruda se consume completa.
- Desperdicio: el movimiento del subproducto se iguala al peso y se marca `picked`; verificar al cerrar la MO que
  el subproducto llega a su destino (Almacén 17) y no se cancela.
- Etiqueta preliminar: tamaño (812x609 puntos) y posición de los campos son provisionales; «Operador» imprime
  número y nombre del empleado (no las siglas del archivo de operadores); «Calidad» repite la clasificación.
