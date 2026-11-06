# Go-live de Tintorería, Rama e Inspección en producción

Lista de lo que hay que hacer el día que el flujo nuevo pasa de `main` (piloto) a producción. Complementa
`docs/RUNBOOK_DESPLIEGUE.md`: el runbook dice cómo se despliega cualquier código; este archivo dice qué
configuración y qué datos del piloto hay que llevar, y cómo verificarlo. Plan general y estado del piloto: documento
«Plan de implementación: tintorería, acabado e inspección en Odoo» (sección 2.1 Despliegue).

Inventario verificado el 2026-11-06 comparando `main` contra producción por MCP. **Antes del go-live se vuelve a
comparar**: el piloto sigue cambiando datos en `main` y producción también cambia.

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
| 1 | Código: rollos 3.5.3, fichas 2.27.1, programación 1.7.1, dosificación 1.4.0 (costeo 1.10.1 ya está) | «push a quimibond» + `odoo-update` + instalar `quimibond_rollos_bitacora` (runbook) |
| 2 | Lo que crea el código al instalar: modelos, vistas, secuencia del número de baño (arranca en 00001), catálogo de 64 defectos, reportes de etiqueta | Viaja con el paso 1 |
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
| Rendimiento forzado (`ficha.tecnica.acabado`, `rendimiento_manual`) | 17 fichas | artículo | Copiar el rendimiento y la marca. El campo existe en producción hasta fichas 2.27 (paso 1) |
| Restricción de Inspección (`mrp.inspeccion.restriccion`) | 1 | centro | INSPECCION 9: `prefijos_excluidos` = `WD`, `codigos_exclusivos` = `WN075Q66JBL205,WJ070Q66JBL205` |
| Procesos de máquina quitados (`ficha.tecnica.proceso`) | 16 | artículo + centro | Archivar en producción (mismos ids, verificados): las 6 IWD038/IWD040 en INSPECCION 9 (1034, 887, 888, 1040, 886, 1046) y IWJ070Q66JBL205 e IWN075Q66JBL205 en INSPECCION 2, 5, 6, 7 y 8 (757, 788, 819, 850, 881, 1143 a 1147) |
| Otros procesos | producción 1,086 vs `main` 1,029 | artículo + centro | Además de los 16, hay 41 de diferencia: revisar en el ensayo de qué lado cambiaron antes de tocar nada |
| Siglas de operadores (`hr.employee.siglas`) | 13 | empleado | Las usa la etiqueta final. Faltan Misael Bibiano (S-4), Juanito Silva (S-50) y Lucio Mejía (S-25) |
| Catálogo de defectos (`calidad.defecto.tela`) | 64 del módulo | código | Solo si Inspección lo modifica en `main` durante el piloto |
| `quimibond_ficha_tecnica_tela.rendimiento_rollos_desde` | 2026-11-06 | — | En producción, la fecha del go-live |
| `quimibond_rollos_bitacora.alerta_diferencia_pct` | 8.0 (piloto) | — | En producción no se crea: queda el 3 % del código |
| `crudo_minimo_pct`, `cierre_minimo_pct` | vacíos | — | Se fijan con las dos primeras semanas del piloto (pendiente de Dirección) |

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
6. **Verificar:** comparación en cero, versiones instaladas, número de baño en 00001, una orden de prueba en kg
   programada (sin confirmarla) para ver baños, crudo y jet.
7. **Activar las telas** de la ola con el visto bueno de Ingeniería (ruta MTO y, según el paso 4, ruta de la 14 y
   sin «Convertir Unidades»).

## Lo que debe estar resuelto antes

- Visto bueno escrito de Ingeniería por ola.
- Catálogo de defectos validado por Inspección.
- Respuestas de Tintorería e Ingeniería (correo del 2026-11-06): capacidad y relación de baño por kg de crudo,
  TINTORERIA 5, banda A de la 3, 150 kg de la 4, rendimiento de tejido de IWR150Q67JNG160DI, IWC120Q21JNT165,
  IZJ042R22JNT160 e IWK140R70JNT165.
- LUBRICANTECHT en 2 PRODUCCIÓN y ENGOMADO002 fabricado por Cocina el día del baño.
