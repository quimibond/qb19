# Go-live de Tintorería, Rama e Inspección en producción

Lista de lo que hay que hacer el día que el flujo nuevo pasa de `main` (piloto) a producción. Complementa
`docs/RUNBOOK_DESPLIEGUE.md`: el runbook dice cómo se despliega cualquier código; este archivo dice qué
configuración y qué datos del piloto hay que llevar, y cómo verificarlo. Plan general y estado del piloto: documento
«Plan de implementación: tintorería, acabado e inspección en Odoo» (sección 2.1 Despliegue).

Inventario verificado el 2026-11-06 comparando `main` contra producción por MCP; puesto al día el 2026-11-08 con lo que
José Sacramento metió a `main` el 2026-11-07 (rollos 3.7.0 a 3.13.0, fichas 2.28.0 con la Clasificación final en
Empaque, dosificación 1.4.1). **Antes del go-live se vuelve a comparar**: el piloto sigue cambiando datos en `main` y
producción también cambia.

## Regla

`main` no se copia encima de producción. Cada cosa pasa por el camino que le toca (tabla de abajo) y los registros
se emparejan por clave natural (código de producto, nombre del centro, artículo de la ficha), nunca por id: los ids
de lo que se crea al instalar `quimibond_rollos_bitacora` en producción no serán los de `main`.

**Mientras dure el piloto:** lo que ya existe en producción (fichas, procesos, fórmulas, tiempos, capacidades) se
captura en producción; lo que solo existe en `main` (taras, rendimiento forzado, restricciones de Inspección, siglas)
se captura en `main` hasta el go-live.

## Qué pasa y por dónde

| # | Qué | Camino |
|---|---|---|
| 1 | Código: rollos 3.13.0, fichas 2.28.0, programación 1.8.1, dosificación 1.4.1 (costeo 1.10.1 ya está). En producción hoy: fichas 2.25.0, programación 1.6.1, dosificación 1.3.0, rollos sin instalar | «push a quimibond» + `odoo-update` + instalar `quimibond_rollos_bitacora` (runbook) |
| 2 | Lo que crea el código al instalar: modelos, vistas, catálogo de 64 defectos, reportes de etiqueta. **Numeración J** (rollos 3.7.0): las secuencias de «Fabricación Tela en kg» (410) y «en m» (411) pasan de `TL/OP-KG/` y `TL/OP-M/` a `TL/OP-KG/J` y `TL/OP-M/J` desde 00001, y el número de baño es el nombre sin el prefijo (J00001; la parte de una orden partida, J00001-001). En producción las dos secuencias están en 1 y no hay órdenes de esos tipos (2026-11-08), así que no hay choque. También crea la ubicación «Toluca/Stock/Químicos-Colorantes Piso» (rollos 3.8.0, ver «Lo que debe estar resuelto») | Viaja con el paso 1 |
| 2b | **Fichas 2.25.0 → 2.28.0 cambia datos sola en producción** (migración de la Clasificación final en Empaque): agrega el paso «Clasificación final» (EMPAQUE, 60 min por ciclo) a las rutas de familia que no lo tienen, la operación CLASIFICACION a las listas en metros **archivadas** del flujo, la capacidad de 400 m por ciclo en EMPAQUE a cada tela en metros con ficha (provisional) y rehace la revisión de listas. Las listas activas no se tocan. En `main` ya está (42 de 42 rutas) | Viaja con el paso 1; revisar el log del update |
| 3 | Listas «Flujo en kg (borrador)»: 45 en `main`, 0 en producción | Botón «Generar listas» de fichas en producción, tela por tela. No se copian: salen de los datos de la ficha |
| 4 | Ruta de entrega desde la 14 y quitar «Convertir Unidades» | Código (propuesta: al activar la tela) o a mano (abajo) |
| 5 | Datos capturados en `main` (taras, rendimientos forzados, restricción de Inspección, procesos quitados, siglas, parámetros) | Copia por MCP: ensayo → aplicar → verificar |
| 6 | Impresora Zebra y básculas en los reportes y estaciones | Sistemas, a mano, con los dispositivos del IoT de producción |

Ya iguales en los dos lados (no se tocan): los 21 centros de Tintorería, Devanado, Abridora, Rama e Inspección
(estado, calendario y alternativas), los calendarios 48 «Tintorería y Acabado (2 turnos)» y 49 «Inspección
(2 turnos)», el tipo de operación 410 «Fabricación Tela en kg» y el parámetro `quimibond_rollos_bitacora.usuario_faltantes`
(almacen.toluca@quimibond.com, Cynthia Santana).

## Datos a llevar (paso 5)

Estado al 2026-11-06. En el ensayo se vuelve a leer de `main`.

| Dato | En `main` | Clave para emparejar | Nota |
|---|---|---|---|
| Taras (`mrp.tara.producto`) | 99 | producto + operación | 5 son provisionales (deducidas de telas hermanas); faltan las de WT140Q21JNT155 y WN070B66JNG155 |
| Rendimiento de la ficha (`ficha.tecnica.acabado`, `rendimiento_tela_acabada` y `rendimiento_manual`) | 17 fichas forzadas | artículo | Copiar el rendimiento y la marca de las fichas que Ingeniería escribió o aprobó en `main`. Desde fichas 2.28.0 el recálculo nocturno **propone** (`rendimiento_medido`) y no escribe: en producción las fichas no cambian solas al desplegar; Ingeniería aprueba cada propuesta con «Usar el medido». Lo que se aleja más de 25 % del teórico (1000 ÷ g/m² × ancho, parámetro `tope_teorico_pct`) no se propone. Los campos existen en producción desde fichas 2.28 (paso 1) |
| Restricción de Inspección (`mrp.inspeccion.restriccion`) | 1 | centro | INSPECCION 9: `prefijos_excluidos` = `WD`, `codigos_exclusivos` = `WN075Q66JBL205,WJ070Q66JBL205` |
| Procesos de máquina quitados (`ficha.tecnica.proceso`) | 16 | artículo + centro | Archivar en producción (mismos ids, verificados): las 6 IWD038/IWD040 en INSPECCION 9 (1034, 887, 888, 1040, 886, 1046) y IWJ070Q66JBL205 e IWN075Q66JBL205 en INSPECCION 2, 5, 6, 7 y 8 (757, 788, 819, 850, 881, 1143 a 1147) |
| Procesos nuevos de Ingeniería (2026-11-06) | 8 | artículo + centro | Devanado de IWJ042Q22JNN160 a 35 m/min (Devanadoras 1 y 2) e Inspección de IWT140Q21JNT155 a 12 m/min (720 m/h, Inspección 2, 5 a 9). En producción después se usa «tiempo de la ficha» en esas operaciones |
| Correcciones de listas (2026-11-06) | 6 listas | producto «I» + componente | WN055 Devanado y Abridora con el tiempo de la ficha; LAVADO001 de las dos WJ032 a 1.85 kg por kg en Abridora; componentes de WD038Q46JNG163, WJ038, WJ053 y WJ070 en Tintorería y el engomado en Rama. Las listas se generan en producción con «Generar listas»: revisar que salgan igual o corregirlas igual |
| Otros procesos | producción 1,086 vs `main` 1,029 | artículo + centro | Además de los 16, hay 41 de diferencia: revisar en el ensayo de qué lado cambiaron antes de tocar nada |
| Secuencia de los centros (`mrp.workcenter`, `sequence`) | 21 centros | centro | Orden del proceso en la planeación por centro de trabajo: Devanadoras 10, Tintorerías 20, Abridoras 30, Ramas 40, Inspecciones 50 (en producción todos en 1; mismos ids 701 a 722 sin la 714). Puesto en `main` el 2026-11-06 |
| Filtro «Piloto Tintorería (orden del proceso)» (`ir.filters` 489, compartido) | 1 | nombre | Órdenes de trabajo de los 21 centros sin las canceladas, agrupadas por centro. Se crea igual en producción (ids de los centros iguales) |
| Reabastecimiento de químicos en 2 PRODUCCIÓN (`stock.warehouse.orderpoint`) | 21 reglas | producto + ubicación | Acuerdo del piloto (2026-11-06): los químicos se surten por tambo, no por orden, igual que el engomado. Cada regla, en 2 PRODUCCIÓN con la ruta 47 «Requisición MP» (de 1 Materia Prima), mínimo 0, máximo 0, automática: cuando el pronóstico de la 2 baja de cero (baños confirmados sin existencia), Odoo crea la requisición y Almacén surte el tambo completo. Son los químicos comprados de las listas «Flujo en kg (borrador)»; fórmulas de Cocina (ENGOMADO001/002, LAVADO 001, APRESTO 008) no llevan regla. Falta el tamaño del tambo de cada químico (Almacén) para ponerlo como múltiplo. **Choca con el diseño del Piso de rollos 3.8.0** si alguien crea sus rutas (ver «Lo que debe estar resuelto») |
| Kg por carga del artículo (`ficha.tecnica.proceso.valor`, parámetro 39 `kg_carga`) | 27 procesos | artículo + centro | Deducido el 2026-11-06 de producción: mediana de kg de crudo por orden de Tintorería (TL/OP-TIN, 1-may a 6-nov) del crudo de la tela en ese jet, **cerrada a la decena** (Dirección). Solo se cargó si el crudo tiene 5 baños o más y p75 ≤ 1.25 × p25. Valores: Tintorería 1: WJ035 y WJ035 HNN 420 kg (IWJ038, IWJ042 JNT160, JNN160 y JNT165; en IWJ038 el 420 es el dato de Tintorería), WJ044 450 (IWJ042JNT175, IWJ045, las 4 IWJ053), XJ130 1,220; Tintorería 3: XJ130 1,220; Tintorería 2: WJ047 510 (las 4 IWJ060 JNT157 a 170), WD040 600 (las 4 IWD038), WC090 410 (IWC090 JNT168 y JNG165), WN048 610, WN063 620 (las 2 IWN075), WJ064 680, WT140 750. **Tintorería 1 y 3 ya están en producción** (2026-11-06, autorizado por Dirección), con el proceso nuevo de IWJ042Q22JNT165 en Tintorería 1 (id 1216 en producción, 1169 en `main`) y las notas fechadas iguales en las dos bases. **Tintorería 2 queda pendiente** de que Tintorería confirme si el baño entra entero o a la mitad: los valores viejos de producción (IWJ060Q21JNT170 255, IWN055Q66JNT162 309) son la mitad del baño, y las órdenes de producción muestran una sola orden de Tintorería por baño entero. Los procesos de Tintorería 2 de IWD038Q46JNT152 e IWC090Q11JNG165 no existen en producción: se crean (en `main` son 1170 y 1171; emparejar por artículo + centro). La relación de baño no se cambia: 9 y 6 son la base de las recetas (NAT 005 a 9 L/kg; BLANCO 001 y NATURAL 006 a 6 L/kg). Sin cargar (dos tamaños de baño o pocos baños): NN053 en la 2, WB046 y WD040 en la 4, WJ045R, WJ047HNN, WN052B y las muestras |
| Capacidad por producto en el centro (`mrp.workcenter.capacity`) | 27 filas de Tintorería 1, 2 y 3 | producto + centro | Igual a los kg por carga de la ficha. El botón «Usar la capacidad de la ficha» (`action_ft_usar_capacidad_ficha`) se salta las diferencias chicas (451 contra 450): revisar después y escribir el número exacto donde no coincida. Sin esto el tiempo de Tintorería cuenta varios ciclos por baño (p. ej. IWD038 tenía 300 kg y su baño es de 600). Tintorería 1 y 3 ya iguales en producción (12 filas). Probado en `main` el 2026-11-06: IWJ038 de 721 kg salió en 2 baños de 414.8 kg de crudo con 210 min por baño |
| Siglas de operadores (`hr.employee.siglas`) | 13 | empleado | Las usa la etiqueta final. Faltan Misael Bibiano (S-4), Juanito Silva (S-50) y Lucio Mejía (S-25) |
| Catálogo de defectos (`calidad.defecto.tela`) | 64 del módulo | código | Solo si Inspección lo modifica en `main` durante el piloto |
| `quimibond_ficha_tecnica_tela.rendimiento_rollos_desde` | 2026-11-06 | — | En producción, la fecha del go-live |
| `quimibond_ficha_tecnica_tela.tope_teorico_pct` | vacío (25 del código) | — | No se crea salvo que Ingeniería pida otro tope |
| `quimibond_rollos_bitacora.alerta_diferencia_pct` | 8.0 (piloto) | — | En producción no se crea: queda el 3 % del código |
| `crudo_minimo_pct`, `cierre_minimo_pct` | vacíos | — | Se fijan con las dos primeras semanas del piloto (pendiente de Dirección) |
| `quimibond_rollos_bitacora.clasificacion_empaque` | vacío | — | Vacío (código): Inspección clasifica y el cierre del baño registra y cierra la orden en metros, como hasta 3.10. En `1` (rollos 3.11 a 3.13): el baño pasa sus rollos a la orden en metros sin registrarlos; Empaque confirma o cambia la calidad de cada rollo, pesa el desperdicio de la orden en metros y el cierre de la orden en metros registra todo con la calidad de Empaque. Decisión de Dirección para el go-live (ver abajo) |
| `quimibond_rollos_bitacora.ubicacion_piso` | id 492 | — | Lo pone solo la instalación de rollos (3.8.0) con el id de la ubicación del Piso en producción; no se copia |

Cómo se copia:

1. **Ensayo** (un día antes): leer cada dato en `main` y en producción, y listar las diferencias campo por campo y de
   qué lado cambió. Dirección revisa la lista. Nada se escribe.
2. **Aplicar** (el día, después del paso 1): escribir en producción solo lo aprobado.
3. **Verificar**: repetir la comparación; debe dar cero diferencias en lo aprobado.

## Ruta de entrega desde la 14 (paso 4)

En `main` (2026-11-06, configurado a mano):

- Ruta 184 «Toluca: Entregar tela acabada desde 14 (PQ)», seleccionable en producto, con una regla: 401 «TL: 14 PQ →
  Clientes», acción *pull*, de 14 PRODUCTO TERMINADO PQ TOLUCA (ubicación 40) a Socios/Clientes, tipo «Toluca: Pick»,
  *make_to_stock*, almacén Toluca, secuencia 20.
- Asignada a 45 productos del flujo nuevo.
- Ruta 120 «Convertir Unidades» quitada de los 14 productos «I» del piloto que la tenían. En producción la tienen 69
  productos «I»; una venta de un «I» con esa ruta la dispararía.

Dos maneras de llevarlo:

- **Propuesta (código, pendiente de aprobar):** que fichas, al activar la tela (cuando hoy le pone la ruta MTO al
  acabado en kg, 2.25.0), le ponga también la ruta de la 14 y le quite «Convertir Unidades». Así sale solo en
  producción y en cada tela nueva.
  Está hecha en el PR [#953](https://github.com/quimibond/qb19/pull/953) (fichas 2.28.1, sin fusionar): apagada, con
  dos parámetros (`al_activar_quitar_convertir` = `1` y `al_activar_ruta_entrega` = nombre exacto de la ruta, que
  Sistemas crea antes).
- **A mano:** crear la ruta y la regla en producción con los datos de arriba, asignarla a las telas que se activan y
  quitarles la ruta 120.

## Impresoras y básculas (paso 6)

Reportes que se ligan a la Zebra del IoT (campo de dispositivos IoT del reporte). En `main` están ligados al
dispositivo 3, ZDesigner ZD230; en producción el dispositivo será otro:

- `quimibond_rollos_bitacora.report_etiqueta_rama` — Etiqueta de Rama (lote-serie)
- `quimibond_rollos_bitacora.report_etiqueta_preliminar` — Etiqueta preliminar de Inspección
- `quimibond_rollos_bitacora.report_etiqueta_rollo_preliminar` — Etiqueta preliminar del rollo (Inspección)
- `quimibond_rollos_bitacora.report_etiqueta_final` — Etiqueta final (PQ / PNC)
- `quimibond_rollos_bitacora.report_etiqueta_rollo_final` — Etiqueta final del rollo (Inspección)

Sistemas imprime una etiqueta en cada estación. Sin báscula registrada las estaciones abren en captura manual con
el motivo «Sin báscula IoT».

## Orden del día

1. **Un día antes:** ensayo de la copia (paso 5) y comparación de la ruta (paso 4); Dirección aprueba la lista.
2. **«push a quimibond»**, `odoo-update` de los módulos e instalación de rollos (runbook, con sus verificaciones).
3. **Aplicar** la copia de datos aprobada.
4. **«Generar listas»** en las fichas de las telas de la ola.
5. **Sistemas** liga los reportes a la Zebra y da de alta las básculas; prueba una etiqueta por estación.
6. **Verificar:** comparación en cero, versiones instaladas, secuencias `TL/OP-KG/J` y `TL/OP-M/J` en 00001, una orden de prueba en kg
   programada (sin confirmarla) para ver baños, crudo y jet. Con programación 1.8.1 una orden de dos baños deja dos
   órdenes en metros, una por baño (un baño = una orden en kg = una orden en metros); cancelar la prueba cancela las dos.
7. **Activar las telas** de la ola con el visto bueno de Ingeniería (ruta MTO y, según el paso 4, ruta de la 14 y
   sin «Convertir Unidades»).

## Lo que debe estar resuelto antes

- Visto bueno escrito de Ingeniería por ola (el del piloto lo dio Jessica Francisco el 2026-11-06).
- Catálogo de defectos validado por Inspección.
- Respuesta de Tintorería (correo del 2026-11-06): TINTORERIA 5, banda A de la 3 y 150 kg de la 4. (Lo de kg de
  crudo ya lo confirmó Tintorería a José Sacramento, ver abajo.) (Las 4 telas sin rendimiento de tejido son muestras, Ingeniería 2026-11-06.)
- LUBRICANTECHT en 2 PRODUCCIÓN y ENGOMADO002 fabricado por Cocina el día del baño.
- Respuesta de Manufactura sobre el Jet 2 (una carga repartida en sus dos partes o dos cargas de 300 kg): con ella se
  cargan los kg por carga de Tintorería 2 en producción.
- ~~Confirmación de José Sacramento sobre kg de crudo y «un baño = una orden en metros»~~: resuelto en su trabajo del
  2026-11-07. Fichas 2.28.0 pasa los avisos y las cargas del jet a kg de crudo («Tintorería confirmó el 2026-11-06 que
  la capacidad del jet y la relación de baño son por kg de crudo») y la Clasificación en Empaque exige un baño por orden
  en metros (cita programación 1.8.0). Las 259 pruebas de los cuatro módulos pasan con su código y el nuestro juntos
  (2026-11-08, Odoo 19 local).
- **Piso contra 2 PRODUCCIÓN (decisión de Dirección).** Rollos 3.8.0 y 3.9.0 hacen que la orden en kg consuma
  químicos, colorantes y engomado de «Químicos-Colorantes Piso» y devuelva lo que sobra, pero solo si existen las rutas
  «Surtido Engomado» y «Surtido Químicos y Colorantes» (no existen ni en `main` ni en producción, 2026-11-08). Sin
  ellas no cambia nada: se consume de 2 PRODUCCIÓN con las 21 reglas de reabastecimiento del piloto. Hay que escoger una
  de las dos formas antes del go-live; si conviven, el químico se pide dos veces o a la ubicación equivocada.
- **Clasificación final en Empaque: prendida o apagada el día del go-live** (parámetro `clasificacion_empaque`).
  Prendida, Empaque necesita su estación, su báscula y su impresora como Rama e Inspección (paso 6).
- Pendiente de José, sin PR: rama `claude/ficha-clasificacion-tiempos` (las operaciones CLASIFICACION viejas con
  0.05 min por metro pasan a 60 min por 400 m). Y el PR [#953](https://github.com/quimibond/qb19/pull/953) (fichas 2.28.1: al activar una tela quita «Convertir
  Unidades» y pone la ruta de la 14 solo con dos parámetros, apagado; es la propuesta del paso 4).
- Ingeniería revisa los rendimientos «por aprobar» de fichas 2.28.0 en `main` (4 al 2026-11-06: NN040Q66JNT163,
  WJ060Q21JNN165, WJ042Q22JNT175 y WJ032Q22JNT155; los dos primeros salen iguales al teórico porque sus órdenes viejas
  calcularon los kg con el factor de la lista, no son medición).
