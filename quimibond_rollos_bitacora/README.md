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

## Inspección como estación única (3.1.0, Dirección General 2026-11-03)

Diseño: `docs/superpowers/specs/2026-11-03-inspeccion-clasificacion-rollos-design.md`. Se agrega a lo de arriba; Rama,
pesaje, taras, bitácora, defectos y el bloqueo de cierre se quedan igual.

- **INSPECCIONAR** (operación de Inspección de la orden en kg, en cuanto el rollo sale de rama) abre la estación
  `rollo.inspeccion.estacion.wizard`: se escanea el rollo de Rama (o varios **del mismo baño** para juntarlos), se pesa
  el rollo final con la tara de Inspección (la de «Clasificación final» en la tabla de taras; báscula IoT o manual con
  motivo), se capturan metros, empalmes, calidad y defectos y se imprime la **etiqueta final** (PQ o PNC).
- **Corte:** un registro por pedazo con «Último pedazo» apagado; el último lo cierra y el rollo de Rama queda
  inspeccionado. No se corta y junta en la misma captura.
- **Rollo final** (`mrp.rollo.final`, Manufactura → Reportes → Rollos finales): lote `M<orden en metros>-NNNN`, kg,
  metros, calidad, rollos de Rama de los que sale. Merma del grupo (kg de Rama − kg de los rollos finales) con alerta
  por porcentaje antes de guardar (parámetro `quimibond_rollos_bitacora.alerta_merma_inspeccion_pct`, 3 %).
- **Sin inventario hasta cerrar el baño:** el supervisor cierra la orden en kg con «Marcar como hecho» (no cierra con
  rollos sin inspeccionar) y en la misma operación se registra la orden en metros que la orden en kg abastece:
  consume cada rollo de Rama completo y produce cada rollo final con su lote: **PQ** la tela en metros a 14, **FE** la
  tela a 15, **Segunda** `SALDO DE TELA TEJIDO` en kg a 16 y **Desperdicio** `SALDO TEJIDO D` en kg a 17 (ubicaciones
  en los parámetros `quimibond_rollos_bitacora.ubicacion_pq/_fe/_segunda/_desperdicio`; saldos en los de fichas). Si
  algo falla no se cierra nada. Filtro y menú «Baños inspeccionados sin cerrar».
- Sale el botón **CLASIFICAR** de Empaque (la lista en metros ya no lleva operación, fichas 2.25.0). El asistente de
  Clasificación final y la etiqueta preliminar quedan en el código sin uso.
- **Baño para venta en kg (3.2.0):** si la orden en kg no abastece una orden en metros (el cliente compra el «I…» en
  kg, o es inventario en kg), Inspección funciona igual y los lotes son `K<orden en kg>-NNNN`. Al cerrar, la orden en
  kg produce los rollos finales con su peso real (no los de Rama): PQ a 14 y FE a 15 como el acabado en kg, y la
  Segunda y el Desperdicio como saldos en kg a 16 y 17. Cada rollo guarda también sus metros.
- **Baño sin primera ni FE (3.2.0):** la orden en metros se cancela, la orden en kg cierra como siempre y una orden de
  saldos (tipo Fabricación Tela en m) consume los rollos de Rama completos y saca cada rollo final como saldo en kg.
  Odoo no cierra una orden con cero de su producto, por eso va aparte.
- Si la lista no trae el subproducto del saldo, se agrega a la orden al cerrar.
- Pruebas: `tests/test_inspeccion_estacion.py` (casos 1 a 11 del diseño, baño en kg y baño solo de saldos).

## Revisión del flujo (3.3.0, 2026-11-05)

Revisión de punta a punta del flujo (código, configuración de producción y pruebas de qbtesting). Arreglos:

- **Varios baños para una orden en metros.** La programación de tintorería divide la orden en kg en baños (v5.4 §17.3)
  y todos abastecen la misma orden en metros. Antes el primer baño la cerraba y el segundo ya no podía cerrar. Ahora
  cada baño registra sus metros y la orden en metros deja **pendiente** nativo (`TL/OP-M/…-002`) con lo planeado de los
  baños que siguen (kg de esos baños ÷ kg por metro de la lista); el último baño la cierra con sus metros reales. Los
  lotes siguen un solo consecutivo por orden (`M<orden>-NNNN`, sin el sufijo del pendiente).
- **Cierre por estado:** el registro de la orden en metros se decide por el estado de la orden en kg después del cierre
  nativo. Antes, si el cierre venía del piso de producción (devuelve una acción), la orden en kg quedaba hecha y la de
  metros sin registrar.
- **Baño sin primera ni FE:** ya no cancela la orden en metros mientras otro baño la abastezca; se cancela solo si es
  el último.
- **Lote reutilizable:** si Odoo pide confirmar algo al cerrar y el cierre se repite, los lotes ya creados (sin
  movimientos) se reutilizan en vez de dar «Ya existe el lote».
- **Anular captura** (botón en el rollo final, responsable de Manufactura): borra un rollo final sin registrar y deja
  pendientes sus rollos de Rama para volver a inspeccionarlos. Un rollo registrado no se anula ni se borra.
- **Alerta 2 de José (v5.4 §9.1):** en la estación, metros contra kg netos × rendimiento de tela acabada de la ficha;
  avisa si se aparta más del parámetro de Ajustes «Alerta 2» (3 %). Se guarda en el rollo final (`metros_teoricos`,
  `alerta_metros`). Un 585 capturado como 58.5 ya no pasa sin aviso.
- **Motivo de la captura manual** obligatorio también en PESAR ROLLO (Rama) y PESAR DESPERDICIO (v5.4 §8.8 y §13).
- **Dos revisadoras a la vez:** la estación bloquea los rollos de Rama y la orden mientras registra (no se duplica un
  rollo ni el consecutivo); el rollo escaneado se busca primero en este baño.
- **Aviso al cerrar el baño:** en el chatter, kg pesados en Rama contra kg planeados. Con el parámetro
  `quimibond_rollos_bitacora.cierre_minimo_pct` capturado (vacío por defecto) no cierra un baño abajo de ese
  porcentaje: en la prueba de qbtesting se cerró un baño con 2 de ~7 rollos y el 73 % del crudo quedó como merma.
- La prueba 11 (falla el registro, no cierra nada) usaba `assertRaisesRegex`, que no revierte; ya usa `assertRaises`
  con savepoint, como la transacción de la pantalla. El cierre sí es atómico.
- Pruebas 15 a 20 en `tests/test_inspeccion_estacion.py`. Corridas en local con Odoo 19 Community y un `iot` mínimo
  de prueba: 42 de 42 pasan.

## Prueba de punta a punta en main (3.3.1, 2026-11-05)

Corrida con las cuatro telas piloto en el ambiente de main (un baño, crudo en el 5, dos baños por capacidad del jet,
corte y unión, anular, motivo manual, saldos, venta en kg, alertas, costeo, programación y dosificación). Dos errores
de código, arreglados con su prueba:

- **El último baño no cerraba si uno anterior tuvo Segunda o Desperdicio** y él no: la división nativa copiaba al
  pendiente la línea sin lote del saldo y el cierre pedía lote. Ahora los saldos pendientes van en cero con sus líneas
  (`_insp_limpiar_saldos`), en el pendiente y en el baño que no trae esa calidad (prueba 23).
- **La orden de saldos creaba una orden en kg que nadie pidió**: su consumo tomaba la regla bajo pedido de la ruta del
  producto en kg. Ahora se confirma con el contexto `qb_insp_sin_abastecer` y su consumo sale de existencias
  (`stock.move._adjust_procure_method`, prueba 24).

Decisiones de Dirección del mismo día:

- **Los saldos entran a costo cero** (se venden a 1–2 $/kg según las facturas del último año). En la orden en metros y
  en la venta en kg ya era así (subproductos con `cost_share` 0). En la orden de saldos de un baño sin primera ni FE,
  Segunda y Desperdicio entran a cero (`_cal_price` con el contexto `qb_insp_saldos_sin_costo`) y el costo del baño
  se queda en la cuenta de la ubicación de producción como merma del periodo, no como inventario de saldo (en la
  prueba entraba a 131.79 $/kg). El módulo depende ahora de `mrp_account` (prueba 25).
- **Segunda y FE piden al menos un defecto** en la estación de Inspección: sin defecto no queda la causa de la baja de
  calidad (prueba 26).

Pruebas de integración con `quimibond_tintoreria_programacion` (21 y 22, se saltan si no está instalado): la
programación divide la orden en kg en baños con la división nativa y cada baño cierra su parte de la orden en metros.

## Consumo de crudo al cerrar (3.3.2, 2026-11-05)

La revalidación en main mostró que, con el consumo flexible, un baño cerraba aunque no hubiera crudo en existencia (19 y
0 kg consumidos) y el kg de la tela salía casi gratis. Ahora la orden en kg con rollos de Rama **no cierra si el crudo
consumido es menor que los kg que pesó Rama** (la tela teñida no puede pesar más que el crudo que entró). El crudo es el
componente en peso con más kg planeados. Con el parámetro `quimibond_rollos_bitacora.crudo_minimo_pct` capturado,
además pide ese porcentaje del crudo planeado; vacío, no aplica (prueba 27).

## Faltante de componentes al cerrar (3.3.3 → 3.3.4, 2026-11-05)

Decisión de Dirección para el piloto: los químicos se quedan en 2 PRODUCCIÓN, se surten por requisición de tambo cuando
se acaban y la orden en kg consume la cantidad teórica de la fórmula (sin orden de Cocina Tintorería para las telas del
flujo nuevo). Su existencia en Odoo es una estimación: el agua no se mide (Almacén le suma 2,000,000 L por ajuste cada
2 o 3 semanas) y el engomado tiene reservas sin existencia. Por eso un faltante **no detiene el cierre**:

- La 3.3.3 bloqueaba el cierre si a un químico, colorante, engomado o agua le faltaba existencia. La 3.3.4 lo cambia por
  un aviso: la orden en kg con rollos de Rama consume lo que sí está apartado (también lo parcial, que Odoo cancelaba) y
  cierra; deja en la orden una nota con cada faltante y una actividad «Faltante de componentes» para el usuario del
  parámetro `quimibond_rollos_bitacora.usuario_faltantes` (login; vacío = el responsable de la orden). El conteo
  periódico corrige la diferencia (pruebas 29 y 30).
- **Solo el crudo bloquea** (3.3.2): son rollos reales con lote y el costo grande.
- Recomendación de configuración: el agua (costo 0, sin medidor) como producto sin inventario (consumible), para que
  siga en la fórmula sin descontarse.

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
