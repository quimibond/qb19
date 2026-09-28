# Auditoría funcional de `quimibond_sgi` por rol — 28-sep-2026

**Alcance.** El código es el de `main` (19.0.56.6.2) más la rama del PR #436
(19.0.56.7.0). Los datos son los reales de producción, leídos **solo en lectura**
con el conector de Odoo. Producción tiene instalada la **19.0.56.1.0**.

**Cómo se hizo y qué falta.**
- Este entorno no tiene red hacia Odoo.sh ni hacia `www.quimibond.com`: el proxy
  responde 403. Por eso todavía no hay staging ni capturas; la columna
  «Captura» dice *pendiente staging*.
- Lo que ve cada persona se reconstruyó a partir de:
  - sus grupos reales;
  - los grupos de los menús, las vistas y los botones;
  - los permisos (`ir.model.access.csv`, igual en código y en producción:
    316 = 60 renglones, 317 = 82, 318 = 72);
  - las reglas de registro;
  - los datos que calcula cada pantalla.
- Además, `tests/test_role_audit.py` entra **como cada rol** (`with_user`) y
  comprueba lo que puede y no puede hacer. Esa prueba corre en el build de
  Odoo.sh.
- No se cambió ningún dato de producción. El reporte no contiene contraseñas.

**Severidad.**
- **bloquea**: no se puede operar o se rompe el control ISO.
- **alta**: se opera mal o se abre un hueco de seguridad.
- **media**: se opera con fricción.
- **baja**: detalle.

---

## Hallazgos que aplican a todos (leer primero)

| # | Hallazgo | Severidad |
|---|---|---|
| G1 | Producción tiene 56.1.0: allá **no existen** «Mis pendientes», «Proponer cambio», «Documentos del puesto», el aviso «sin empleado», Mis indicadores, ni la corrección del puesto en Mi equipo. Lo que se prueba en `main` no es lo que ve la gente. | bloquea |
| G2 | **Nadie veía sus indicadores en Mi procedimiento**: la pantalla contaba solo los «oficiales» y los 93 están en «prueba». | alta |
| G3 | Usuario SGI **no tiene menú** para ver o capturar indicadores ni para ver documentos controlados: Indicadores, Mediciones y Documentos estaban solo en Administración SGI, que es de MAST. Además, «Ir a hacerlo» de C6.26, C2.36 y S2.23 manda a un menú que el usuario no ve. | alta |
| G4 | Usuario SGI implica «Aprobaciones: **aprobar todas** las solicitudes» (grupo 164). Los 30 usuarios ven y gestionan todas las solicitudes, incluidas compras y «Cambio en Odoo (S6)». | alta |
| G5 | **486 documentos controlados vigentes** los puede editar o reemplazar **cualquier** usuario interno (`access_internal = edit`). | bloquea |
| G6 | Sin reglas de «solo lo mío»: cualquier Usuario SGI captura mediciones de **cualquier** indicador, termina acciones ajenas, evalúa cualquier riesgo y marca cualquier requisito legal. | alta |
| G7 | Las escalaciones «al Jefe MAST» le llegan al **CEO** (usuario 7). El sistema toma al primero, por id, de todos los que heredan el grupo; Administrador y Dirección lo heredan. | bloquea |
| G8 | Jefe MAST y SGI (318) lo tienen **7 personas**: 128 Areli, 150 Darío y 130 Sergio de forma directa; 35 Jorge por Dirección; 7, 9 y 152 por Administrador. Con el grupo llegan gerente de Calidad, Documentos, Soporte, Proyecto y Aprobaciones, además de «ver a todos» en Mi procedimiento y Publicar. | alta |
| G9 | **59 actividades del SGI vencidas** que nadie cierra: 27 «Eslabón atorado», 16 «Capturar indicador», 10 «Revisar parte interesada», 5 «Riesgo alto sin acción» y 1 de pronóstico. Resolver la causa no las cerraba. | alta |
| G10 | **Ningún indicador tiene `nc_on_red`** (0 de 93) y ninguno es oficial: un rojo no hace nada. Hoy están en rojo TR-01, TR-03, C4-02, CO-01, AL-01, AL-02, C2-06 y EX-07, todos sin NC. | alta (decisión) |
| G11 | «Documentos que aplican al puesto» sale vacío **también por datos**: las actividades de C2, C6, S1, S2, S3 y S4 no tienen instructivo, formatos ni procedimiento ligados. Hay 48 IT, 50 procedimientos y 192 formatos vigentes sin ligar. | alta (datos) |
| G12 | Mi procedimiento está publicado solo para **2 puestos** (MP-059 y MP-221). Los puestos auditados están sin publicar, así que **nadie puede firmar**. Hay 7 acuses en toda la base, todos pendientes. | alta (datos) |
| G13 | EPP: ningún puesto tiene EPP requerido. Hay 0 responsivas y 0 solicitudes de firma con la plantilla 4905. Sign necesita el correo del empleado, y el Almacenista K 530 y Jorge Matías 603 no lo tienen. | alta (datos) |

---

## 1. Elena Delgado — Auxiliar de Compras (usuario 16, empleado 248, Usuario SGI)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | Inicio / apps | CRM, Ventas, Compras (admin), Inventario (admin), Manufactura, Calidad, Facturación, Documentos, Proyecto, Soporte, Firmar, Aprobaciones y SGI (Inicio, Procesos, Mejora) | Navegar | Leer todo el SGI | Sin menú de Indicadores, Mediciones ni Documentos SGI (G3). Ve todas las solicitudes de Aprobaciones (G4) | alta | pendiente staging |
| b | Mi procedimiento | Proceso S1. **Ejecuta 7**: S1.05, .07, .09, .12, .13, .17, .28. Aprueba 0. Recibe el escalamiento de S1.18. Participa en 1 (S1.06). Se entera de 13. Los textos tienen «cómo» y «terminada cuando» completos | Leer, imprimir, proponer cambio (56.2+) | Ver y firmar su procedimiento | **Sin revisión publicada: no hay qué firmar** (G12). S1.14 y S1.26 sin menú de Odoo | alta | pendiente staging |
| b | Documentos del puesto | 0 | — | Formatos e IT de S1 | Vacío por datos (G11) | alta | pendiente staging |
| c | Pendientes | Mediciones CO-01 (3 capturadas, rojo), CO-02 (3) y CO-03 (3 pendientes desde jun-26) | Capturar | Una lista con lo atrasado marcado | **CO-02 y CO-03 están archivados** y siguen saliendo como pendientes (corregido en 56.7.0). S1-01 y S1-02 no tienen ninguna medición | media | pendiente staging |
| d | Indicadores | CO-01 48.15 % contra meta 85 (rojo). S1-01 y S1-02 sin dato. Ninguno oficial | Capturar | Rojo abre NC | Rojo sin NC (G10). En 56.1.0 no los ve en Mi procedimiento (G2) | alta | pendiente staging |
| f | Proponer cambio | Categoría 11: aprobación del jefe obligatoria, Areli obligatoria, mínimo 2 | Desde Aprobaciones (en 56.1.0 no hay botón) | Llega a su jefe y a Areli | Llega a Jorge (su jefe y dueño de S1) y a Areli: correcto. La única solicitud existente (C-SGI00002, 18-ago) sigue en «new» | media | pendiente staging |
| g | Permisos | Solo lectura en proceso, actividad, rol, indicador, auditoría, hallazgo y revisión. **RWC** en medición, acción, riesgo y calibración. **RW** en legal. NC nativa como usuario de Calidad | — | Solo escribe lo suyo | Escribía lo de todos (G6). Corregido con reglas en 56.7.0 | alta | pendiente staging |
| h | De más / de menos | Sin RH ni salarios (correcto). Todas las solicitudes de Aprobaciones | — | — | G4 | alta | pendiente staging |
| i | Celular | Mi procedimiento: ficha con pestañas. Mis actividades: tarjetas en celular y lista en escritorio (56.7.0) | — | Legible a ancho de teléfono | Pendiente de validar en staging | media | pendiente staging |

## 2. Cynthia Santana — Jefe de Inventarios (usuario 15, empleado 11; dueña de C6)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | Inicio / apps | Lo mismo que Elena, más Punto de venta | — | — | Mismo hueco de menús (G3) | alta | pendiente staging |
| b | Mi procedimiento | Ejecuta 4: C6.21, .22, .24, .26. Aprueba 1: C6.17. Recibe 14 escalamientos. Participa en 2 y se entera de 5 | — | — | C6.26 manda a un menú que no ve. Sin publicar | media | pendiente staging |
| c | Pendientes | AL-01 (rojo, 6.2 contra 5), AL-02 (rojo), C2-06 (0 %, rojo), C6-03; C6-01 y C6-04 pendientes desde el 21-sep; 6 actividades del SGI vencidas | Capturar y validar | Una lista con semáforo | En 56.1.0 eran botones separados y sin semáforo. **13 mediciones sin validar** | alta | pendiente staging |
| e | Mi equipo | 9 reportes directos: Ana Silvia Colín (245), José Gómez (260) y **7 Almacenista K sin usuario**. Además, por ser dueña de C6, entraban ~15 puestos con cualquier rol en C6, incluidos **su jefe Jorge** y sus pares | Abrir procedimiento y pendientes | Su gente y quien opera su proceso | Alcance de más: se contaban «informa» y «escala». Corregido en 56.7.0: solo ejecuta y aprueba de actividades activas | media | pendiente staging |
| e | Conteos | Ana: 6 ejecuta (1 atrasada). José: 5 (1 atrasada). Almacenista K: 10 (1 atrasada) | — | — | Correctos | — | pendiente staging |
| e | Abrir desde Mi equipo | **En 56.1.0: sin puesto y 0 actividades** (lee `job_id` del empleado público, vacío para quien no es de RH) | — | Puesto y actividades | Sigue en producción. Corregido en 56.1.1, que no está desplegado (G1) | bloquea | pendiente staging |
| f | Proponer cambio | Las de Ana y José llegan a Cynthia (jefa y dueña de C6) y a Areli | — | — | Correcto | — | pendiente staging |
| h | De más / de menos | Sin RH. El Almacenista K no tiene usuario y 7 de sus 10 actividades dicen «validar o escanear en Odoo» | — | — | No pueden hacerlas con usuario propio | alta (datos) | pendiente staging |

## 3. Irma Luna — Contador General (usuario 68, empleado 8; dueña de S2 y S3)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | Inicio / apps | Lo anterior, más Empleados (administrador), Nómina, Flotilla, Hojas de horas, eLearning y Punto de venta | — | — | Ve salarios y contratos de todos. Es coherente con S4.13 (autoriza la nómina), pero hay que decidirlo | baja (decisión) | pendiente staging |
| b | Mi procedimiento | 30 actividades: ejecuta 11, aprueba 19. **24 de 30 sin medición automática**. Recibe ~40 escalamientos. Procesos S1, S2, S3, S4 y C6 | — | — | 12 actividades sin menú (banca, SAT, IMSS) | media | pendiente staging |
| c | Pendientes | Solo EX-07 de ago-26 capturado (rojo, 50.9 contra 42) | Validar | Todo lo suyo | S2-03 y S3-01 a S3-05 **sin ninguna medición** (4 son manuales mensuales) | alta | pendiente staging |
| d | Indicadores (7) | EX-07, S2-03, S3-01 a S3-05; todos en prueba | — | Rojo abre NC | 6 sin dato; rojo sin NC (G10). No los ve en Mi procedimiento (G2) | alta | pendiente staging |
| f | Aprobar cambios de su proceso | — | — | El dueño aprueba los cambios de su proceso | La categoría 11 manda al **jefe directo y a Areli, no al dueño**: un cambio de Jessica a S2 no le llega a Irma. **Corregido en 56.7.0**: el dueño del proceso se agrega como aprobador | alta | pendiente staging |
| e | Mi equipo | Departamento Logística, más los puestos de S2 y S3 (entraban los directores) | — | — | Acotado en 56.7.0 | media | pendiente staging |

## 4. Areli Ballesteros — Jefe MAST y SGI (usuario 128, empleado 40; dueña de E2)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | App SGI | Todo el menú, con Administración (Documentos, Indicadores, Diagnóstico, Firmas y Configuración). No ve «Cargar catálogo» ni «Ajustes» | Todo lo de MAST | Igual | Correcto. Está también en los grupos heredados 295 y 296 | baja | pendiente staging |
| b | Mi procedimiento | Puesto 189 con 37 roles: 26 ejecuta, 2 aprueba, 3 escala, 3 informa, 3 participa. Puede elegir a cualquiera, publicar y firmar | — | Igual | Correcto | — | pendiente staging |
| c | Pendientes | 3 vencidas desde el 05-sep: «Capturar TR-02/03/04 (08/2026)». **TR-03 ya estaba capturado** y la actividad seguía abierta | — | Se cierra al capturar | G9, corregido en 56.7.0 | alta | pendiente staging |
| d | Indicadores (9) | CA-02, E2-01, E2-02, E2-03, SST-01, TR-01 a TR-04. Con dato: TR-01 (rojo, 0) y TR-03 (rojo, 1,498.14) | Capturar y validar | Rojo abre NC | Sin NC (G10); 7 de 9 sin valor | alta | pendiente staging |
| e | Mi equipo | Toda la planta (alcance de administradora) | — | Su gente y E2 | Aceptable para quien administra el SGI | baja | pendiente staging |
| f | Propuestas | Aprobadora obligatoria de la categoría 11 | Aprobar | — | La solicitud C-SGI00002 sigue en «new» | media | pendiente staging |
| h | De más | Todas las carpetas de Documentos, todas las aprobaciones y todos los tickets | — | — | Por su rol está bien. Las escalaciones sin dueño le llegaban al CEO (G7) | alta | pendiente staging |

## 5. Jorge Ortiz — Dirección de Operaciones (usuario 35, empleado 6; dueño de E1 y S1)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | App SGI | Lo de MAST, más Revisión por la Dirección y el Tablero | **Todo lo de Jefe MAST**, porque 319 implica 318 | Dirección: leer, aprobar y revisión por la dirección | Edita procesos, documentos y configuración, y publica. **Hay que decidir** si 319 deja de implicar 318 | alta (decisión) | pendiente staging |
| b | Mi procedimiento | 103 roles (56 escala, 8 aprueba) | Ver a cualquiera, publicar | Ver la planta | Correcto en alcance | — | pendiente staging |
| c | Pendientes | 2 «Eslabón atorado» de E1 vencidas (24-sep) | — | Se cierran al fluir | G9, corregido | media | pendiente staging |
| d | Indicadores (8) | C4-01, C4-02 (rojo, 38.67), E1-01, E1-02, MA-02 (sin dato: capacidad 0 con más de 1,000 OP), MA-04 (verde), MA-05 (amarillo), S1-05 | — | Rojo abre NC | C4-02 en rojo sin NC | alta | pendiente staging |
| e | Mi equipo | Toda la planta (165) | — | Toda la planta | Correcto | — | pendiente staging |
| h | De más | Configuración del SGI; gerente de Documentos, Calidad y Aprobaciones | — | Configuración solo de lectura | Tiene de más (ver fila a) | alta | pendiente staging |

## 6. Mariano Domínguez — Administrador SGI (usuario 152, empleado 474; dueño de S6)

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | App SGI | Todo, con «Cargar catálogo», Tipos de documento y términos de fórmula | Todo | Todo, incluida la configuración | Correcto. También está en 296 (heredado, no da nada) | baja | pendiente staging |
| c | Pendientes | «Capturar TI-01 (08/2026)» vencida desde el 05-sep | — | — | TI-01 sigue pendiente | media | pendiente staging |
| d | Indicadores (5) | S6-01, S6-03 y S6-04 (creados el 24-sep, sin medición); S6-02 y TI-01 manuales | — | — | Ninguno con valor. La primera medición sale el 1-oct | media | pendiente staging |

## 7. Auditor SGI (grupo 317 solo; hoy nadie lo tiene directo)

La auditoría no le asignó el grupo a ningún usuario de producción. Este rol lo
prueba `tests/test_perm_auditor.py`, que crea un usuario de prueba solo con 317.

| Paso | Pantalla | Qué ve | Qué puede hacer | Esperado | Hallazgo | Severidad | Captura |
|---|---|---|---|---|---|---|---|
| a | Menú | SGI → Inicio, Procesos, Mejora (sin «Acciones»), Dirección (sin Revisión por la Dirección) | — | Leer todo el SGI y la evidencia | **Sin menú de Documentos, Lista maestra, Indicadores, Mediciones ni Acuses**: con los permisos que tiene no llega a la evidencia | alta | pendiente staging |
| b | Mi procedimiento | Menú visible, pero el modelo de la pantalla no tiene permiso para 317: da error de acceso | — | Abrir sin error | Error de acceso | alta | pendiente staging |
| g | Permisos | Lectura de todo el SGI y de los modelos auditados. Escribe `sgi.audit`, el programa y sus líneas (sin borrar), hallazgos y checklist (con borrar) | — | Solo escribe hallazgos | **Aprueba, cierra y reabre el programa de auditoría** (debería ser de MAST). «Registrar hallazgo» crea una NC (`quality.alert`) que el auditor no puede crear | media | pendiente staging |
| h | Real | auditorcalidad@ (184) tiene 316, no 317: es auditor de nombre con permisos de usuario | — | — | Asignar 317 si va a auditar | media (datos) | pendiente staging |

## 8. Cuentas genéricas sin empleado — supervisor@ (92), manufactura@ (80), auditorcalidad@ (184)

| Paso | Pantalla | Producción 56.1.0 | 56.7.0 | Esperado | Severidad | Captura |
|---|---|---|---|---|---|---|
| b | Mi procedimiento | `UserError` «Tu usuario no tiene empleado ligado…» | Abre con el aviso «Tu usuario no está ligado a un empleado» | Aviso claro | media | pendiente staging |
| c | Pendientes | — | Solo lo que vive en el usuario | — | baja | pendiente staging |
| h | Pendiente huérfano | «Capturar MA-04 (08/2026)» asignado a manufactura@, con la medición ya capturada | Se cierra solo (56.7.0) | — | media | pendiente staging |

## 9. Operador de planta sin usuario (ejemplo: Germana Paulino, empleada 432, Operador de tejido circular G)

| Paso | Qué recibe fuera de Odoo | Hallazgo | Severidad |
|---|---|---|---|
| PDF | MP-059, con **una sola actividad** (C4.05, familia OP-TEJ). El documento está `stale`: cambió después de publicarse | Pobre en contenido y desactualizado | alta (datos) |
| Firma | 6 acuses pendientes del 25-sep. Solo MAST firma por ellos | **Jorge Matías (603), dado de alta el 25-sep, no tiene acuse**: al alta en un puesto ya publicado no se genera acuse | media |
| EPP / Sign | Ningún puesto tiene EPP; 0 responsivas. Sign pide correo y varios operadores no lo tienen | La responsiva no se puede mandar | alta (datos) |
| Almacenista K (7 personas) | 10 actividades, 7 de ellas «en Odoo», sin usuario. C6.15 maneja químicos sin EPP definido | — | alta (datos) |

---

## Confirmación de la lista que pediste revisar

| Punto | Estado en producción (56.1.0) | Estado en `main` + 56.7.0 |
|---|---|---|
| Mi procedimiento desde Mi equipo o «Ver como» sale sin puesto y con 0 actividades | **Sigue** | Corregido desde 56.1.1 |
| «Documentos que aplican al puesto» vacío para todos | **Sigue**, por código y por datos (G11) | El código está corregido desde 56.2.0; **faltan los datos** (ligar IT, formatos y procedimientos) |
| Botones separados de atrasadas y al día | **Sigue** | Una sola lista con semáforo desde 56.3.0 |
| Pantalla vacía para usuarios sin empleado | **Sigue** (sale error) | Aviso desde 56.2.0 |
| 23 indicadores automáticos sin medición | **Sigue, con matiz**. 17 no tienen ninguna: se crearon entre el 23 y el 25-sep, después de la corrida del 1-sep, y su primera medición sale el 1-oct. 6 solo tienen «sin dato»: CA-02 (no hay encuestas), EX-01 (póliza de cierre; el arreglo no está desplegado), MA-02 (capacidad 0), MT-02 (0 preventivos), RH-02 (sin competencias), VE-02 (sin presupuesto) | Igual; los 6 se arreglan con datos o con la corrida de octubre |
| 59 actividades automáticas vencidas | **Sigue** (exactamente 59) | Cierre automático en 56.7.0: la próxima corrida diaria cierra las resueltas |
| Jefe MAST con 3 usuarios (Areli, Darío, Sergio) | **Sigue**, y en total 7 con los heredados | Las escalaciones van ahora al Jefe MAST directo. **Quitar a 150 y 130 lo haces tú** (dato de producción) |
| `sgi.employer.obligation` obsoleto | 0 registros. Lo usan el menú Dirección → Obligaciones patronales y un paso del cron | Retirarlo es chico (ver C12) |

---

## 1. Hallazgos consolidados por severidad

**Bloquea**
1. G1 — Producción está en 56.1.0: nada de lo corregido está instalado.
2. G5 — Cualquier interno edita documentos controlados.
3. G7 — Las escalaciones del Jefe MAST le llegan al CEO.
4. Mi equipo → procedimiento sin puesto y con 0 actividades (en producción).

**Alta**
5. G2 — Nadie ve sus indicadores en Mi procedimiento.
6. G3 — Usuario y Auditor sin menú de Indicadores, Mediciones ni Documentos; «Ir a hacerlo» manda a menús invisibles.
7. G4 — Usuario SGI aprueba todas las solicitudes.
8. G6 — Usuario SGI escribe mediciones, acciones, riesgos y legales ajenos.
9. G8 — Jefe MAST lo tienen 7 personas; Dirección hereda todo MAST.
10. G9 — 59 actividades vencidas que no se cierran.
11. G10 — Ningún indicador oficial ni con NC por rojo.
12. G11 / G12 / G13 — Documentos sin ligar, procedimientos sin publicar, EPP sin cargar (datos).
13. Los cambios a un proceso no los aprueba su dueño.
14. Auditor: Mi procedimiento da error de acceso y no tiene menú de evidencia.
15. En la lista de indicadores, un clic prende o apaga `nc_on_red`.

**Media**
16. El Mi equipo del dueño de proceso incluía a su jefe y a sus pares.
17. Mediciones de indicadores archivados como pendientes fantasma.
18. «Mis pendientes»: toda solicitud salía atrasada el mismo día.
19. El auditor aprueba el programa de auditoría; «Registrar hallazgo» crea una NC que no puede crear.
20. Botones de auditoría, «Generar acuses», «Sincronizar/Adoptar regla» y resultados legales sin `groups`: el usuario los ve y le sale error.
21. No se crea el acuse al dar de alta a alguien en un puesto ya publicado.
22. Un Usuario SGI puede pasar una NC a «Cerrada».
23. Calibración, AMEF, PPAP y plan de control editables por todo Usuario SGI (vía Calidad).
24. Diseño: ficha de proceso con 12 botones inteligentes; campos duplicados en la ficha de actividad; segundo notebook en NC; emojis; textos en «usted»; jerga interna en pantalla.

**Baja**
25. Grupos heredados 295 y 296. 295 solo da acceso al modelo de Studio `x_emp_activity`, que tiene 0 registros.
26. Usuario SGI no lee la Revisión por la Dirección.
27. `sgi.dev.characteristic` lo borra cualquier interno.
28. `sgi.employer.obligation` obsoleto.

## 2. Corrección propuesta y tiempo

| # | Corrección | Dónde | Tiempo | Estado |
|---|---|---|---|---|
| C1 | Desplegar 56.7.0: fusionar #436, aprobar la solicitud 685 y correr `odoo-update quimibond_sgi` | Odoo.sh | 30 min | **pendiente de tu OK** |
| C2 | Documentos controlados vigentes en solo lectura para internos | `models/sgi_document.py` `_sgi_share_controlled` + `migrations/19.0.56.7.0/post-migrate.py` | 20 min | **hecho** |
| C3 | Escalaciones al Jefe MAST directo o al parámetro `quimibond_sgi.mast_user_id` | `models/sgi_cron.py` `_sgi_first_user_id` / `_sgi_manager_user_id`; `models/sgi_doc_change.py` | 20 min | **hecho** |
| C4 | Mis indicadores (pestaña, botón y menú Inicio), con todos los activos a cargo | `models/sgi_my_procedure_screen.py`, `models/sgi_indicator.py` `action_sgi_measures`, `views/sgi_my_procedure_views.xml` | 40 min | **hecho** |
| C5 | Quitar «aprobar todas» de Usuario SGI | `security/sgi_security.xml` (`(3, ref(...))`) | 5 min | **hecho** |
| C6 | Reglas de escritura «solo lo mío» (medición, acción, riesgo, legal y evaluación legal) | `security/sgi_security.xml` | 40 min | **hecho** |
| C7 | Cierre automático de actividades resueltas | `models/sgi_cron.py` `_sgi_close_resolved_activities` (paso del cron diario) | 40 min | **hecho** |
| C8 | Dueño del proceso como aprobador de las propuestas | `models/sgi_mp_change.py` `_sgi_add_process_owner_approver` | 15 min | **hecho** |
| C9 | Mi equipo del dueño acotado; pendientes sin indicadores archivados; solicitudes que vencen a los 3 días | `models/sgi_my_procedure_screen.py`, `models/sgi_my_pending.py` | 15 min | **hecho** |
| C10 | Menús y botones por rol: auditor con Documentos e Indicadores de lectura, permisos de la pantalla para 317, `groups` en botones de auditoría, acuses, aprobaciones y legales; «Mis pendientes» en Inicio | `views/sgi_menus.xml`, `security/ir.model.access.csv`, vistas | 1.5 h | **en curso en esta rama** |
| C11 | Pulido de diseño (lista primero, textos neutros, sin emojis ni jerga, campos duplicados, `nc_on_red` de solo lectura en la lista) | vistas | 2 h | **en curso en esta rama** |
| C12 | Retirar `sgi.employer.obligation` y pasar a `qb_obligation` (0 registros) | `models/sgi_kpi_fields.py`, vistas, CSV, pre-migrate | 1.5 h | propuesto |
| C13 | Solo MAST cierra una NC; candado de borrado para NC con folio | `models/sgi_nonconformity.py` `write`/`unlink` | 45 min | propuesto |
| C14 | Auditor: aprobar el programa, solo MAST. «Registrar hallazgo» crea `sgi.audit.finding` | `models/sgi_audit.py`, `views/sgi_process_views.xml` | 1 h | propuesto |
| C15 | Acuse automático al dar de alta a alguien en un puesto publicado | `models/sgi_my_procedure.py` (write de `hr.version`/`hr.employee`) | 45 min | propuesto |
| C16 | Dirección (319) sin heredar Jefe MAST | `security/sgi_security.xml` | 30 min | **necesita tu decisión** |
| C17 | Datos: quitar a Darío (150) y Sergio (130) de 318; archivar 295, 296 y `x_emp_activity`; dar 317 a auditorcalidad@; ligar IT, formatos y procedimientos a las actividades; publicar Mi procedimiento de todos los puestos; cargar EPP y correos; `nc_on_red` en los indicadores críticos | producción (con tu OK) | 1–2 días de MAST | **tuyo o de MAST** |
| C18 | Lista de actividades a pantalla completa con búsqueda propia y filtro «Atrasadas» (`mp_status` buscable) | `models/sgi_my_procedure_screen.py`, vista search | 1 h | propuesto |
| C19 | Herencias de vistas propias (30) fusionadas al padre con pre-migrate | vistas, `migrations/` | 1 día | propuesto |

## 3. Qué quedó corregido en la rama y cómo se probó

Todo está en la rama `claude/amazing-wozniak-q5lxkx`, en el PR #436, versión
19.0.56.7.0.

- **Pantalla Mi procedimiento**:
  - Ficha con pestañas.
  - Lista como vista principal y tarjetas en el celular.
  - Mis indicadores: pestaña, botón y menú Inicio.
  - Publicar se mueve a Administración SGI.
- **Seguridad**: C2, C5 y C6, más lo corregido en la primera auditoría del
  día:
  - Una propuesta enviada ya no se edita.
  - Un acuse firmado ya no se puede mover.
  - Los adjuntos del COA se validan.
- **Operación**:
  - C3 (escalaciones), C7 (cierre de actividades), C8 (dueño aprueba) y C9.
  - Aprobar una propuesta de actividad nueva o de cambio de ejecutor ya no
    truena.
  - Obligaciones vencidas.
  - Reincidencia de NC.
  - Medición con savepoint.
- **Cómo se probó**:
  - Checadores locales, sin errores: `tools/check_odoo_views.py --base-ref
    origin/main`, `tools/check_addons.py` y `flake8`.
  - Pruebas nuevas, que corren en el build de Odoo.sh:
    - `tests/test_role_audit.py`: indicadores en prueba visibles; el usuario
      captura solo lo suyo y MAST cualquiera; el usuario no edita procesos y
      no aprueba todo; la escalación va al Jefe MAST directo; la actividad de
      captura se cierra sola.
    - `test_mp_change.test_06`: cambio de ejecutor aplicado sin romper la
      regla; candado de edición.
    - `test_my_procedure_ui`: pestañas, lista primero y menú Publicar.
  - Pruebas ajustadas: `test_ola0` (el usuario termina sus propias acciones).
  - CI de GitHub (`check` y `odoo-tests`) en verde en el commit anterior.
    GitHub no instala el SGI (depende de Enterprise), así que las pruebas del
    SGI solo corren en Odoo.sh.
- **Falta**: el recorrido en staging con capturas por persona. Requiere:
  1. marcar una rama como Staging en Odoo.sh;
  2. abrir la red de este entorno al dominio del staging;
  3. poner un acceso de administrador del staging como secreto del entorno.
