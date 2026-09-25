# SGI — Lo que falta programar (CEO, 2026-09-25)

Lista de programación para que el SGI quede completo, ordenada por los cinco
niveles de la estructura (empresa, proceso, actividad, persona, registro), más
mejora y documentos. Cada punto dice qué hace, cómo se construye y cuándo se da
por terminado. 29 puntos, 16 con prioridad 1.

**Prioridad:** 1 = antes de la auditoría interna (octubre). 2 = antes de la
revisión por la dirección (diciembre). 3 = 2027.

**Ya hecho, no se repite:** Mi procedimiento y Mi equipo, menú de cinco
entradas, ficha de proceso y de actividad, publicación de puestos con acuses,
indicadores (detalle, prueba u oficial, NC por persistencia, plan en rojo,
metas escalonadas, fórmula configurable), ligas entre actividades con match y
due_field, y limpieza de menús.

**Fuera de esta lista:** los controles de operación de cada proceso (motivo al
cancelar una orden, tejido solo desde la planeación y los demás P-13 a P-44).
Siguen en Pendientes para el programador del SGI y se programan con el piloto
de su proceso.

**Estado:** la columna «Estado» la actualiza el programador al cerrar cada
punto (versión del módulo y PR).

## Mejora: auditorías

El modelo existe completo (programa, auditoría con estados, equipo, procesos,
normas, minutas, checklist, hallazgos que generan NC). Falta lo que lo vuelve
usable.

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| AU-1 | Checklist generado del proceso | Al pasar la auditoría a «planificada», crear una línea de checklist por actividad de cada proceso auditado: pregunta «¿se cumple C2.17 en plazo y con evidencia?», con botón a los registros recientes del entregable de la actividad. Respuesta: conforme, observación, NC menor o NC mayor, con evidencia. No usar Encuestas para esto | Se audita C2 sin escribir una sola pregunta y cada respuesta no conforme crea su hallazgo | 1 | **Hecho en 50.0.0** |
| AU-2 | Independencia del auditor | Restricción: ningún miembro del equipo auditor puede ejecutar ni aprobar actividades del proceso auditado (por su puesto en los roles). Mensaje claro al guardar | No deja asignar a Paris César como auditor de C3 | 1 | **Hecho en 50.0.0** |
| AU-3 | Informe de auditoría en PDF (F-P-G03-07) | Reporte con alcance, equipo, fechas, procesos, minutas, hallazgos por tipo con cláusula y evidencia, conclusión y firmas. Se archiva como documento al cerrar la auditoría | Al pasar a «cerrada» queda el PDF en la auditoría | 1 | **Hecho en 50.0.0** |
| AU-4 | Tipos «cliente» y «proveedor» | Agregar al tipo de auditoría; en cliente, campo cliente y número de reporte externo; en proveedor, campo proveedor, y sus hallazgos van a la NC a proveedor (NC-6) | Se registra una auditoría de un cliente automotriz con sus hallazgos | 2 | **Hecho en 53.0.0** |
| AU-5 | Programa anual sugerido | Botón en el programa: una línea por proceso vigente o en piloto, trimestre sugerido; procesos con NC abiertas o indicadores en rojo, dos veces al año | El programa 2027 se arma con un clic y se ajusta a mano | 2 | **Hecho en 53.0.0** |

## Mejora: no conformidades y acciones

La NC ya tiene origen, cláusula, proceso, 5 porqués, Ishikawa, acciones,
eficacia y reincidencia. Hay 20 NC, ninguna cerrada y cero acciones: le falta
que el tiempo corra.

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| NC-1 | Plazos por etapa con aviso y escalamiento | Al abrir la NC se calculan fechas: contención 1 día hábil, causa raíz 10, plan 15. Cada una crea una actividad al responsable; vencida, escala al dueño del proceso y luego a MAST. Parámetros ajustables | Una NC abierta hoy muestra sus tres fechas y avisa el día que vence cada una | 1 | **Hecho en 49.0.0** |
| NC-2 | Acción de contención | Agregar «contención» al tipo de acción (hoy: corrección, correctiva, preventiva). Una NC de reclamación no pasa de Abierta sin al menos una contención | No se puede avanzar una reclamación sin contención | 1 | **Hecho en 49.0.0** |
| NC-3 | Eficacia programada | Al terminar la última acción correctiva, crear la verificación de eficacia a 90 días para MAST. La NC solo pasa a Cerrada con eficacia registrada | No se puede cerrar una NC sin fecha y nota de eficacia | 1 | **Hecho en 49.0.0** |
| NC-4 | Cancelar solo con motivo y aprobación | Pasar a Cancelada pide motivo y lo aprueba Jefe MAST. Motivo en el historial | Nadie fuera de MAST puede cancelar; el motivo queda escrito | 1 | **Hecho en 49.0.0** |
| NC-5 | Solo las etapas del SGI | Migración: las NC en Nuevo, Confirmado, Acción propuesta y Resuelto pasan a su equivalente (Abierta, Seguimiento, Cerrada) y esas cuatro se archivan | La NC muestra 4 etapas: Abierta, Seguimiento, Cerrada, Cancelada | 1 | **Hecho en 49.0.0** (las cuatro etapas nativas no se archivan: son de los equipos de piso y ninguna NC del SGI estaba en ellas; se desligan de los equipos del SGI) |
| NC-6 | NC a proveedor | Desde una NC de materia prima: «Enviar al proveedor» manda por portal la NC con lote y evidencia; el proveedor contesta causa y acción en el portal. Plazo y aviso como NC-1. Alimenta la evaluación de proveedores (S1.08) | Un proveedor contesta su causa sin correo de por medio | 2 | **Hecho en 53.0.0** |
| NC-7 | Reporte 8D | PDF desde la NC de reclamación: D1 equipo, D2 problema, D3 contención, D4 causa (5 porqués e Ishikawa), D5-D6 acciones, D7 prevención (AMEF y documentos tocados), D8 cierre. Con el número de NCR del cliente | Se manda a un cliente un 8D generado de Odoo | 2 | **Ya existía** (`report_8d.xml`, D1–D8 con NCR del cliente) |

## Documentos

514 documentos vigentes: 488 ya ligados a los procesos nuevos y 26 sin
proceso. Falta que un cambio llegue solo a quien lo usa y que el auditor tenga
su lista.

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| DOC-1 | Publicación en un paso | Al aprobarse el cambio documental: publicar la revisión nueva, marcar la anterior obsoleta, avisar a los puestos del proceso y dejar pendiente el acuse de quienes deben leerlo | Aprobar un cambio deja la nueva revisión vigente y los acuses pendientes, sin tocar nada más | 1 | **Hecho en 51.0.0** |
| DOC-2 | Obsoletos al pasar un proceso a vigente | Al cambiar el estado del proceso a «vigente», sus documentos sustituidos pasan a obsoletos con fecha y motivo. Verificar si ya entró con la limpieza; si no, programarlo | Pasar C2 a vigente vuelve obsoletos P-A28, P-A16 y P-C07 | 1 | **Hecho en 45.0.0** (`sgi.process.write` → `_sgi_obsolete_replaced_documents`, prueba `TestCleanup45.test_04`); falta verificar fecha y motivo en el chatter del documento |
| DOC-3 | Lista maestra en PDF | Reporte por proceso: clave, título, tipo, revisión, fecha de vigencia, próxima revisión, dueño | Se imprime la lista maestra de C2 desde la ficha del proceso | 1 | **Hecho en 51.0.0** |
| DOC-4 | Aviso de próxima revisión | Actividad al dueño 60 días antes de la fecha de próxima revisión de cada documento | Un documento que vence en 60 días aparece en Mis pendientes del dueño | 2 | **Hecho en 53.0.0** (el aviso al dueño ya existía; ahora también en Mis pendientes) |
| DOC-5 | Instructivos en Knowledge | El instructivo de una actividad puede ser un artículo de Knowledge. Al publicarlo se congela como revisión del documento controlado, con su clave IT | Se escribe un IT en Knowledge, con fotos, y aparece en la tarjeta de la actividad | 2 | **Hecho en 53.0.0** |

## Nivel 1: empresa y dirección

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| DIR-1 | Requisitos legales con evaluación y vencimiento | Responsable obligatorio por requisito; cada evaluación registra resultado (cumple, no cumple, no aplica), evidencia y fecha de la siguiente; aviso 60 días antes; «no cumple» abre NC. PDF de la matriz legal | Los requisitos de STPS que vencen el 30 de noviembre aparecen en Mis pendientes de su responsable | 1 | **Hecho en 51.0.0** |
| DIR-2 | Riesgos con evaluación periódica | Probabilidad × impacto con semáforo; reevaluación en enero y julio con actividad al dueño del proceso; riesgo alto sin acción abierta queda marcado. PDF de la matriz por proceso | La ficha de cada proceso muestra sus riesgos con color y fecha de evaluación | 2 | **Hecho en 52.0.0** |
| DIR-3 | Informe de revisión por la dirección | PDF con todas las entradas de ISO 9.3 tomadas solas de Odoo: acuerdos anteriores, indicadores y objetivos, auditorías, NC, riesgos, requisitos legales, satisfacción, proveedores, quejas. Los acuerdos son acciones con responsable y fecha (miden E1-02) | La revisión de diciembre sale de Odoo sin armar nada a mano | 2 | **Hecho en 52.0.0** |
| DIR-4 | Tablero de dirección (I-9) | Nivel del indicador (dirección, proceso, actividad); tablero con objetivos integrales y sus indicadores oficiales, últimos 6 periodos, rojos sin plan, acuerdos vencidos y procesos con más atrasos | Dirección ve 10 a 12 indicadores y no 90 | 2 | **Hecho en 52.0.0** (marcar en cada indicador el nivel «Dirección») |

## Niveles 2 a 5: proceso, actividad, persona y registro

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| PER-1 | Ver el procedimiento de otra persona | En Mi procedimiento, selector «Ver como» (puesto o empleado) para administrador, MAST, Dirección y jefes con su equipo; botón «Ver su procedimiento» en empleado, puesto y Mi equipo | Jose abre el procedimiento de Paris César en dos clics | 1 | **Hecho en 46.1.0** |
| PER-2 | EPP del puesto con responsiva | Bloque de EPP en Mi procedimiento y en el PDF; responsiva de entrega firmada por el empleado y ligada a él | En curso | 1 | **Hecho en 48.0.0** (`sgi.epp.delivery`, pestaña Mi EPP, bloque en el PDF, prueba `TestEpp`) |
| PER-3 | Matriz de competencias | Reporte puesto × persona: procedimiento firmado, instructivos leídos, capacitación y habilidades | Se imprime la matriz de un área para el auditor | 2 | **Hecho en 52.0.0** |
| REG-1 | Firmas de Sign ligadas a su registro | Firmar desde la orden de compra, la entrega, el lote, el producto o el traslado, con la solicitud ligada al registro (campo de referencia). El entregable de la actividad puede exigir «firmado». Hoy son más de 300 firmas al mes sueltas | Un certificado de calidad firmado aparece en su entrega y cuenta para C2.24 | 2 | **Hecho en 53.0.0** |
| REG-2 | Encuesta respondida como entregable | Una respuesta de Encuestas puede ser el entregable de una actividad (satisfacción del cliente, DNC) | E2.12 se mide sola | 2 | **Hecho en 53.0.0** |
| PR-1 | Sustituir un proceso sin dejar nada colgado | Al archivar un proceso con replaces, sus indicadores, riesgos y documentos pasan al nuevo y se listan en el reporte de carga | Una carga futura no deja indicadores en procesos archivados | 3 | **Hecho en 53.1.0** |

## Permisos

| ID | Qué | Cómo | Listo cuando | Prio | Estado |
|---|---|---|---|---|---|
| PERM-1 | Auditor de solo lectura | El grupo Auditor SGI lee todo el SGI y los registros de los procesos auditados, y solo puede escribir hallazgos | Un auditor abre cualquier evidencia sin poder cambiarla | 1 | **Hecho en 48.0.0** (prueba `TestPermAuditor`) |
| PERM-2 | Grupos correctos | Aplicar lo acordado: Jefe MAST solo Blanca Areli; Dirección de Operaciones a Jorge Ortiz; los demás a Usuario SGI | Los grupos coinciden con la lista aprobada | 1 | **Hecho en producción 2026-09-25** por MCP |

## Cómo agruparlo en PRs

| PR | Contiene | Prio |
|---|---|---|
| 1 | PERM-1, PERM-2, PER-1, PER-2 (permisos y Mi procedimiento completo) | 1 |
| 2 | NC-1 a NC-5 (no conformidades que sí se cierran) | 1 |
| 3 | AU-1 a AU-3 (auditoría lista para octubre) | 1 |
| 4 | DOC-1 a DOC-3 y DIR-1 (documentos y matriz legal) | 1 |
| 5 | DIR-2, DIR-3, DIR-4, PER-3 (revisión por la dirección de diciembre) | 2 |
| 6 | NC-6, NC-7, AU-4, AU-5, DOC-4, DOC-5, REG-1, REG-2 (proveedores, clientes y firmas) | 2 |
| 7 | PR-1 | 3 |

Los PR 1 a 4 van antes de la auditoría interna, en ese orden.
