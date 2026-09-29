# Changelog — quimibond_sgi

Una sección por versión del manifest, la más nueva arriba. Reconstruido el
2026-09-29 desde el historial completo de git (`git fetch --unshallow`,
auditoría K-018): la fecha y el resumen salen del commit que subió la versión;
las líneas **Migración** resumen el docstring del script de
`migrations/<versión>/`. Las versiones de las entregas que entraron a `main`
en un squash (lotes 2 a 4, #466 y #469) citan el commit de su rama.

**Regla:** el PR que sube `version` en `__manifest__.py` agrega aquí su
entrada, con el mismo número. `tools/check_addons.py --base-ref` lo exige.

Secciones posibles dentro de una entrada: Agregado, Cambiado, Corregido,
Retirado, Seguridad, Migración, Datos de producción.

## 19.0.57.4.0 — 2026-09-29

**Retirado:** el mapa viejo de procesos sale del módulo (A-002 + B-021,
decisión 6: el SGI se instala sin procesos). `data/sgi_process_data.xml`
(21 procesos MP-*/P-* y 19 flujos) y `data/sgi_process_flows_extra.xml`
(18 flujos) dejan el manifest y pasan a `docs/historico/quimibond_sgi_data/`.

**Retirado:** la siembra del piloto P-VEN (`seed_procedure_ventas`, sus
constantes `_SGI_VENTAS_*` y `_sgi_vigente_docs_by_codes`) y
`seed_process_purposes` con `_SGI_PROCESS_PURPOSES` (A-003, B-002).
`harden_noupdate` ya no lista `sgi.process` ni `sgi.process.flow`. Los scripts
de un solo uso `carga_documental.py`, `post_carga_documental.py` y
`reporte_telas_rollout.py` salen de `tools/` del módulo a
`docs/historico/quimibond_sgi_tools/` (B-018).

**Cambiado:** las pruebas crean sus propios procesos (`XPM-A`, `XPM-B`) en
lugar de usar `proc_*`/`flow_*` o «el primer proceso» de la base
(`test_process_map`, `test_process_map_46`, `test_flows_48`, `test_ola1`,
`test_ola2`, `test_format_map_documento`; J-019). Se retira `TestProcedureVentasSeed` con su siembra, y
`test_flows_48.test_01` (verificaba los flujos del XML). Prueba nueva
`test_procesos_viejos`.

**Migración (pre, `migrations/19.0.57.4.0/pre-migrate.py`):** los 58 XML IDs
(21 `sgi.process`, 37 `sgi.process.flow`) pasan a `__export__` con prefijo
`quimibond_sgi_legado_`; si alguno estuviera activo se archiva. No borra nada.

**Migración (post, `migrations/19.0.57.4.0/post-migrate.py`):** respaldo CSV
(adjunto en el proceso) de las 7 filas de `sgi.process.responsibility` del
proceso archivado P-VEN. Las filas se quedan: el modelo no tiene `active`.

**Datos de producción (2026-09-29, lectura):** 58 XML IDs `quimibond_sgi`
sobre procesos (21) y flujos (37), todos archivados; `sgi.process` 14 activos
y 25 archivados; `sgi.process.flow` 50 activos y 37 archivados.

## 19.0.57.3.0 — 2026-09-29

**Retirado:** las 39 carpetas de migración de 19.0.13.7.0 a 19.0.56.24.0
(`19.0.13.7.0` … `19.0.56.24.0`; A-026, B-003, A-027). Ninguna vuelve a correr:
producción está en 19.0.57.0.0 (o 19.0.56.38.2) y todas las bases son copia
de producción. Su resumen queda abajo, en las líneas **Migración** de cada
versión, y el código sigue en git (etiqueta `sgi-antes-de-limpieza-56.x` sobre
`main`, que se pone antes de integrar esta versión).
Con ellas se van las dos convenciones de nombre (`post-migration.py` contra
`post-migrate.py`, A-027). Quedan las migraciones ≥ 19.0.56.25.0.

**Retirado:** `models/sgi_groups_cleanup.py` (limpieza de grupos de 56.8.0, ya
aplicada; solo la llamaba la migración 56.8.0 y llevaba un login fijo) y su
prueba `tests/test_perm_groups.py` (B-005).

## 19.0.57.2.0 — 2026-09-29

**Agregado:** este CHANGELOG, reconstruido desde git para las 243 versiones
anteriores (K-018, D-30), y el chequeo en `tools/check_addons.py`: si un PR
cambia la versión del manifest de `quimibond_sgi`, también tiene que cambiar
este archivo y traer la sección de la versión nueva.

## 19.0.57.0.0 — 2026-09-29

Del Dropbox a Odoo, rutina por rutina, buscador y avance.

**Migración (post, `migrations/19.0.57.0.0/post-migrate.py`):** primera carga del grupo «Dueño de proceso (SGI)» (``quimibond_sgi.group_sgi_process_owner``), que ve «Rutina por rutina», «Procedimientos anteriores» y «Avance de la transición».

Commit `09a18eb` en la rama de la entrega; entró a `main` en el squash `d5e0b61` (PR #469).

## 19.0.56.39.0 — 2026-09-29

Del Dropbox a Odoo, formatos y documentos anteriores.

**Migración (post, `migrations/19.0.56.39.0/post-migrate.py`):** los instructivos, DAT, anexos, protocolos y reglamentos controlados y activos que NO tienen clase de migración pasan a clase D («Sigue como documento») y «No aplica (se queda)».

Commit `55f5d66` en la rama de la entrega; entró a `main` en el squash `d5e0b61` (PR #469).

## 19.0.56.38.2 — 2026-09-29

La clave heredada del Dropbox vale en sgi.format.map.

Commit `e0e2f2e`.

## 19.0.56.38.1 — 2026-09-29

Decisiones de Jose sobre la entrega 8a.

**Migración (post, `migrations/19.0.56.38.1/post-migrate.py`):** se liberan TODOS los equipos de medición que están en «No usar». 1. Respaldo en la tabla ``sgi_equipment_do_not_use_bak_563801`` (id, name, sgi_do_not_use, sgi_calibration_state, sgi_next_calibration_date y la hora del respaldo) de cada equipo que se va a…

Commit `1078998` en la rama de la entrega; entró a `main` en el squash `e7bed84` (PR #466).

## 19.0.56.38.0 — 2026-09-29

Calibración avisa sin bloquear, con resumen diario.

**Migración (post, `migrations/19.0.56.38.0/post-migrate.py`):** la calibración solo avisa, no bloquea, y los avisos van al Coordinador de Laboratorio y al Jefe de Calidad en un resumen diario.

Commit `56a97ef` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.37.0 — 2026-09-29

Avisos de los crons con clave y cierre por episodio.

**Migración (post, `migrations/19.0.56.37.0/post-migrate.py`):** los avisos de MAST que quedaron en otras bandejas. 1. ``quimibond_sgi.mast_user_id``: si no está puesto (o vale 0), se fija al usuario activo con login ```` (en producción, el 128, Blanca Areli Ballesteros).

Commit `a6fdc78` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.36.0 — 2026-09-29

Mis pendientes con todo adentro (entrega 8a, bandeja).

Commit `7f9f4bf` en la rama de la entrega; entró a `main` en el squash `20dbec7` (PR #465).

## 19.0.56.35.0 — 2026-09-29

export_payload, dominios portables, categoría «Proponer cambio» al núcleo y frontera del mapa.

**Migración (pre, `migrations/19.0.56.35.0/pre-migrate.py`):** Solo SQL, sin importar código del módulo, idempotente y sin borrar nada. 1. A-004 / D-01: los 10 términos de fórmula de ``sgi_indicator_formula_data.xml`` (ubicaciones, categorías, tipo de operación y proveedor con IDs de producción) salen del núcleo: el…

Commit `10a4b9f` en la rama de la entrega; entró a `main` en el squash `65a5284` (PR #464).

## 19.0.56.34.0 — 2026-09-29

El SGI es de una sola empresa (entrega 3, e3-multiempresa).

Commit `3742218` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.33.0 — 2026-09-29

Integridad del modelo (entrega 3, e3-integridad).

Commit `09e7786` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.32.0 — 2026-09-29

Clave del Dropbox como clave anterior y clave nueva D-02 (C-004, C-005, C-007, C-009).

**Migración (post, `migrations/19.0.56.32.0/post-migrate.py`):** Corre después de 56.30.0 (C-006: los pies de formato ya están ligados al documento, decisión 4 de Jose).

Commit `d6f2a4f` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.31.0 — 2026-09-29

El documento manda en procedimiento ↔ proceso (C-001, L-003, L-005).

**Migración (post, `migrations/19.0.56.31.0/post-migrate.py`):** estado de migración de los procedimientos según la decisión de Jose (decisiones.md, «Estado de migración de los procedimientos (C-003)»): - los 23 sustituidos (con ``sgi_replaced_by_process_id``) → «En curso» (pasan a «Baja tramitada» cuando su proceso…

**Migración (pre, `migrations/19.0.56.31.0/pre-migrate.py`):** ``sgi.process.replaced_document_ids`` deja de ser un Many2many (tabla ``sgi_process_replaced_doc_rel``) y pasa a ser el One2many inverso de ``documents.document.sgi_replaced_by_process_id``.

Commit `bfc5181` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.30.0 — 2026-09-29

El pie de formato controlado sale del documento (C-006).

**Migración (post, `migrations/19.0.56.30.0/post-migrate.py`):** liga cada ``sgi.format.map`` a su documento controlado por la clave con la que se sembró, ANTES de que la clave del Dropbox pase a clave anterior (decisión 4 de Jose: primero C-006, después la migración de clave).

Commit `e40db91` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.29.0 — 2026-09-29

Árbol de menús de la entrega 4 en un solo archivo.

**Migración (post, `migrations/19.0.56.29.0/post-migrate.py`):** A. Salarios de eficiencias: los importes pasan del grupo de Nómina (hr_payroll.group_hr_payroll_user, que en producción incluye a usuarios que no deben verlos) al grupo propio «Salarios de eficiencias (SGI)».

Commit `9a67d48` en la rama de la entrega; entró a `main` en el squash `4ff9643` (PR #463).

## 19.0.56.28.0 — 2026-09-29

Entrega 1c de la auditoría (seguridad).

**Migración (post, `migrations/19.0.56.28.0/post-migrate.py`):** Areli (Jefe MAST y SGI) queda como responsable SGI de los documentos controlados que no tienen (489 en producción el 2026-09-29: 487 vigentes y 2 obsoletos), antes de que la regla «controlado ⇒ con responsable» empiece a exigirlo.

Commit `c40791e`.

## 19.0.56.27.0 — 2026-09-29

Archivar una regla de aprobación cierra sus avisos.

Commit `fa8f78c`.

## 19.0.56.26.0 — 2026-09-29

Entrega 1b, Mis pendientes solo de la empresa del SGI.

Commit `9aa8c83`.

## 19.0.56.25.0 — 2026-09-29

Entrega 1 de la auditoría (instalación limpia, siembras, Sign y salud).

Commit `7ec8736`.

## 19.0.56.24.1 — 2026-09-29

Sgi.my.pending se extiende como TransientModel.

Commit `4415976`.

## 19.0.56.24.0 — 2026-09-29

Menú SGI reordenado con los nombres de producción y pendientes sin archivados.

**Migración (post, `migrations/19.0.56.24.0/post-migrate.py`):** borra, con respaldo, los roles de actividades archivadas (223 en producción, todos «ejecuta», de los 15 procesos viejos P-xxx).

Commit `d386922`.

## 19.0.56.23.0 — 2026-09-28

Actividades ligadas a los requisitos de la norma.

Commit `4a3027f`.

## 19.0.56.22.0 — 2026-09-28

Eficiencias por área, checklists firmados con PIN y respuesta al cliente en la NC.

**Migración (post, `migrations/19.0.56.22.0/post-migrate.py`):** - Eficiencias de personal: los jefes de departamento (usuario del jefe en hr.department) entran al grupo «Captura de eficiencias».

Commit `0d39511`.

## 19.0.56.21.0 — 2026-09-28

Documento externo, estudios/exámenes, recorrido CSH y checklists.

Commit `275a5f1`.

## 19.0.56.20.0 — 2026-09-28

Menús con claves, vencimientos largos, orígenes de NC y acta.

**Migración (post, `migrations/19.0.56.20.0/post-migrate.py`):** las NC que nacieron de un incidente SST quedan con origen «Incidente/accidente SST» (antes se guardaban como «Proceso»).

Commit `d8d292b`.

## 19.0.56.19.0 — 2026-09-28

«Mi procedimiento» firmado en Sign antes de entrar en vigor.

**Migración (post, `migrations/19.0.56.19.0/post-migrate.py`):** «Mi procedimiento» se firma en Sign antes de entrar en vigor (decisión del CEO: el SGI usa Sign).

Commit `0f54d72`.

## 19.0.56.18.0 — 2026-09-28

Acuses y responsivas de EPP firmados sin plantilla manual.

Commit `8626a9b`.

## 19.0.56.17.0 — 2026-09-28

Cambio documental firmado en Sign.

**Migración (post, `migrations/19.0.56.17.0/post-migrate.py`):** cambio documental firmado en Sign y categorías de Aprobaciones que sí se pueden aprobar. - «Modificación de documento SGI»: se aprueba firmando en Sign (elaboró → revisó → aprobó).

Commit `249c64e`.

## 19.0.56.16.1 — 2026-09-28

Etiqueta única para sgi_mp_ack_ids.

Commit `617ba61`.

## 19.0.56.16.0 — 2026-09-28

Líneas de negocio («Aplica a»).

**Migración (post, `migrations/19.0.56.16.0/post-migrate.py`):** líneas de negocio con lo que Odoo ya tiene. 1. «C2 Pedido a entrega» mezcla pedido industrial, de confección y exportación.

Commit `20c2631`.

## 19.0.56.15.0 — 2026-09-28

Bloque 10: retiro de sgi.employer.obligation y roles de actividades archivadas fuera de los cálculos.

**Migración (pre, `migrations/19.0.56.15.0/pre-migrate.py`):** se retira ``sgi.employer.obligation`` (S4-04), sin registros en producción; las obligaciones viven en ``qb_obligation``.

Commit `30d6ca3`.

## 19.0.56.14.0 — 2026-09-28

Bloques 8 y 9: tablero de dirección sin oficiales y proveedores sin datos.

**Migración (post, `migrations/19.0.56.14.0/post-migrate.py`):** «Sin datos» no es «Baja». Antes, un proveedor sin recepciones con fecha compromiso tenía OTD 0 y calificación 30 → «Baja» (44 de 87 evaluaciones con OTD 0; 84 de 87 proveedores en «Baja»).

Commit `d7789ab`.

## 19.0.56.13.0 — 2026-09-28

Bloque 7: clasificación de formatos en lote (7.4).

Commit `49c2465`.

## 19.0.56.12.0 — 2026-09-28

Bloque 6: motivo cuando un indicador no calcula, cierre de avisos y crons escalonados.

**Migración (post, `migrations/19.0.56.12.0/post-migrate.py`):** horarios escalonados de los crons del SGI. Antes, 10 crons diarios arrancaban a las 20:44 UTC y «Mediciones semanales» a la misma hora que «Mediciones de indicadores».

Commit `a5078a6`.

## 19.0.56.11.0 — 2026-09-28

Bloque 5: obsolescencia con fecha, motivo y proceso que sustituye (DOC-2).

**Migración (post, `migrations/19.0.56.11.0/post-migrate.py`):** campos nuevos «Obsoleto desde», «Motivo de obsolescencia» y «Lo sustituye el proceso». Los documentos que ya estaban obsoletos toman como fecha su última modificación y como motivo «Obsoleto antes de 56.11.0» (no se sabe el motivo real).

Commit `2147d32`.

## 19.0.56.10.0 — 2026-09-28

Bloque 4: programa de auditorías sin auditor no se aprueba; flujo completo con proceso real.

Commit `e27adb9`.

## 19.0.56.9.0 — 2026-09-28

Bloque 3: acción terminada exige 100 % y fecha no futura (3.6).

**Migración (post, `migrations/19.0.56.9.0/post-migrate.py`):** una acción terminada tiene 100 % de avance y fecha de término de hoy o antes. Datos existentes (no se borra nada): - «Terminada» con fecha FUTURA (en producción, la acción 63 «Abrir mercado mexicano», 0 % y fecha 2026-11-30): se reabre (sin fecha de…

Commit `e4ece13`.

## 19.0.56.8.0 — 2026-09-28

Bloque 2: Jefe MAST solo con Areli y retiro de grupos heredados.

**Migración (post, `migrations/19.0.56.8.0/post-migrate.py`):** - 2.2 PERM-2: Jefe MAST y SGI con un solo miembro directo (Areli); los demás miembros directos pasan a Usuario SGI.

Commit `86d6b9e`.

## 19.0.56.7.0 — 2026-09-28

«Mi procedimiento» como ficha con pestañas y correcciones de la auditoría.

**Migración (post, `migrations/19.0.56.7.0/post-migrate.py`):** los documentos controlados en piloto o vigente quedan de solo lectura para los usuarios internos (access_internal = 'view').

Commit `056c0ab`.

## 19.0.56.6.2 — 2026-09-28

«Nuevo» de la lista de Mis actividades abre la propuesta de actividad nueva.

Commit `d068042`.

## 19.0.56.6.1 — 2026-09-28

Botón nativo «Nuevo» en Mis actividades abre la propuesta de actividad nueva.

Commit `1b6ff0a`.

## 19.0.56.6.0 — 2026-09-28

Cómo se aprueba (botón, solicitud o firma) y reglas duplicadas en el botón.

Commit `94d6aa6`.

## 19.0.56.5.0 — 2026-09-28

El rol «Aprueba» ligado a la aprobación nativa de Odoo.

Commit `f54afee`.

## 19.0.56.4.0 — 2026-09-28

Proponer cambio edita la actividad de verdad y se aplica al aprobarse.

Commit `84178b8`.

## 19.0.56.3.2 — 2026-09-28

Proponer cambio abre sin error (propuesta y motivo obligatorios en la vista, no en el modelo).

Commit `ebcfce9`.

## 19.0.56.3.1 — 2026-09-28

Etiqueta propia para pending_late (chocaba con late_count en el build).

Commit `204f388`.

## 19.0.56.3.0 — 2026-09-28

Mis pendientes en una sola lista con semáforo (pantalla y Mi equipo).

Commit `9ecd728`.

## 19.0.56.2.0 — 2026-09-28

Documentos del puesto desde sus actividades, proponer cambios a Mi procedimiento y aviso sin empleado.

**Migración (post, `migrations/19.0.56.2.0/post-migrate.py`):** la categoría de Aprobaciones «Proponer cambio a mi procedimiento (SGI)» queda marcada para el botón «Proponer cambio».

Commit `dbc1148`.

## 19.0.56.1.1 — 2026-09-28

Mi procedimiento abierto desde Mi equipo muestra el puesto y las actividades del empleado.

**Migración (post, `migrations/19.0.56.1.1/post-migrate.py`):** pantallas «Mi procedimiento» guardadas con empleado y sin puesto (abiertas desde Mi equipo) reciben el puesto del empleado.

Commit `c6dae03`.

## 19.0.56.1.0 — 2026-09-25

Build sin amarillo (etiquetas duplicadas, compute mixto, botón kanban, acceso al wizard de acuses, firmas en la variante) y README de 56.0.0.

Commit `e7dfb55`.

## 19.0.56.0.0 — 2026-09-25

Migración de formatos Excel, bloque del programador.

Commit `236ab1a`.

## 19.0.55.0.0 — 2026-09-25

Fórmulas para 28 indicadores más (fechas relativas, comparar fechas, solo conteo) y campos nuevos.

**Migración (pre, `migrations/19.0.55.0.0/pre-migrate.py`):** varios términos con el mismo papel se suman; la restricción SQL «un numerador y un denominador por indicador» ya no existe en el código y Odoo no siempre la retira al actualizar.

Commit `7a94d4d`.

## 19.0.54.4.0 — 2026-09-25

Mi equipo abre en organigrama con el estado SGI de cada persona.

Commit `966e992`.

## 19.0.54.3.0 — 2026-09-25

Cada diagrama con el trazado que le corresponde.

Commit `445ca21`.

## 19.0.54.2.0 — 2026-09-25

«Mi procedimiento» sin pestañas, cada lista en su botón.

**Migración (pre, `migrations/19.0.54.2.0/pre-migrate.py`):** «Mi procedimiento» sin pestañas; la herencia propia sgi_my_procedure_view_form_pr6 (lista de documentos por revisar) guardada en la base apuntaba a un campo que ya no está en la vista.

Commit `74a7ebd`.

## 19.0.54.1.0 — 2026-09-25

Pantalla «Mi procedimiento» como ficha de persona con botones y tarjetas compactas.

**Migración (pre, `migrations/19.0.54.1.0/pre-migrate.py`):** la ficha del proceso vuelve a cambiar de forma (sin pestaña Conexiones) y las herencias del propio módulo guardadas en la base (procedimiento, estructura) se revalidan contra el padre nuevo antes de recargarse: el build reventó igual que en 54.0.0.

Commit `066d963`.

## 19.0.54.0.0 — 2026-09-25

El diagrama como vista de cada acción y ficha del proceso con botones.

**Migración (pre, `migrations/19.0.54.0.0/pre-migrate.py`):** la ficha del proceso cambió de forma (pestañas → botones). Odoo revalida las vistas heredadas que YA están en la base en cuanto carga la vista padre nueva, antes de llegar al archivo que las corrige; la de estructura tenía xpaths sobre…

Commit `a5ec01b`.

## 19.0.53.6.0 — 2026-09-25

Catálogo ISO de diagramas, impresión aparte y ajuste de carriles.

Commit `14be8f3`.

## 19.0.53.5.0 — 2026-09-25

Campos de liga entrada ↔ salida y cuatro reglas de datos.

**Migración (post, `migrations/19.0.53.5.0/post-migrate.py`):** - Los documentos de Odoo que no son del SGI quedan sin «Estado SGI» (había 2,524 marcados como borrador solo por el default del campo).

Commit `636fb4d`.

## 19.0.53.4.0 — 2026-09-25

Motor de diagramas en HTML y seis diagramas.

Commit `992b91a`.

## 19.0.53.3.1 — 2026-09-25

Mapa de procesos con acabado moderno.

Commit `027c2e4`.

## 19.0.53.3.0 — 2026-09-25

Mapa de procesos con conexiones (componente sgi_process_map).

Commit `719cbbf`.

## 19.0.53.2.1 — 2026-09-25

Tarjetas del diagrama solo con numeral, etapa y título.

Commit `0c6d540`.

## 19.0.53.2.0 — 2026-09-25

Diagramas con la vista nativa hierarchy (mapa de procesos y flujo de actividades).

Commit `5d03a31`.

## 19.0.53.1.5 — 2026-09-25

Tarjetas de Mi procedimiento sin el «cuándo» apilado.

Commit `562e1dd`.

## 19.0.53.1.4 — 2026-09-25

Orden de carga de vistas heredadas (el build de producción reventó).

Commit `e15b818`.

## 19.0.53.1.3 — 2026-09-25

OrderedSet en los métodos de búsqueda y acuses al publicar instructivo.

Commit `9981cc1`.

## 19.0.53.1.2 — 2026-09-25

Correcciones de la primera corrida real de las pruebas de los PR 1 a 7.

Commit `c4d131f`.

## 19.0.53.1.1 — 2026-09-25

Mi procedimiento con usuarios sin RH, y registrar 8 archivos de pruebas.

Commit `72cbafc`.

## 19.0.53.1.0 — 2026-09-25

PR 7 del plan, sustituir un proceso sin dejar nada colgado (PR-1).

Commit `01f1fe3`.

## 19.0.53.0.0 — 2026-09-25

PR 6 del plan, proveedores, clientes y firmas (NC-6, AU-4, AU-5, DOC-4, DOC-5, REG-1, REG-2).

Commit `49ac1ac`.

## 19.0.52.0.0 — 2026-09-25

PR 5 del plan, revisión por la dirección de diciembre (DIR-2, DIR-3, DIR-4, PER-3).

**Migración (post, `migrations/19.0.52.0.0/post-migrate.py`):** - DIR-2: los riesgos ya evaluados (probabilidad e impacto capturados) reciben `last_eval_date` = su última modificación y, si no tenían próxima revisión, el siguiente enero o julio; así el cron los pone en el ciclo semestral.

Commit `96b956e`.

## 19.0.51.0.1 — 2026-09-25

Import local de sgi_safe_domain en sgi_audit (el import a nivel de módulo cargaba sgi_activity_spec antes que sgi_catalog y rompía el registro en el build de main).

Commit `e35e665`.

## 19.0.51.0.0 — 2026-09-25

PR 4 del plan, documentos y matriz legal (DOC-1, DOC-3, DIR-1).

**Migración (post, `migrations/19.0.51.0.0/post-migrate.py`):** DIR-1 hace obligatorio el responsable de cada requisito legal: los que estaban sin responsable reciben al Jefe MAST (primer usuario del grupo) y quedan en el log para reasignarlos.

Commit `0cc69b1`.

## 19.0.50.0.0 — 2026-09-25

PR 3 del plan, auditoría lista para octubre (AU-1 a AU-3).

Commit `e7b0a10`.

## 19.0.49.0.1 — 2026-09-25

<group> sin atributos en las vistas de búsqueda (el RNG de Odoo 19 tampoco acepta string=; rompía el build de main).

Commit `68be390`.

## 19.0.49.0.0 — 2026-09-25

PR 2 del plan, no conformidades que sí se cierran (NC-1 a NC-5).

**Migración (post, `migrations/19.0.49.0.0/post-migrate.py`):** - NC-5: las NC del SGI (con folio) que estén en una etapa ajena a los equipos del SGI (Nuevo / Confirmado / Acción propuesta / Resuelto, las nativas de Calidad) pasan a su equivalente (Abierta / Abierta / Seguimiento / Cerrada) y se quita a los equipos del…

Commit `9aeac24`.

## 19.0.48.1.1 — 2026-09-25

Quitar expand= de los <group> de búsqueda (RNG de Odoo 19 lo rechaza; rompía el build de main).

Commit `e1b8db6`.

## 19.0.48.1.0 — 2026-09-25

«Mi procedimiento» como pestaña nativa en la ficha del empleado y del puesto.

Commit `da47ecc`.

## 19.0.48.0.0 — 2026-09-25

PR 1 del plan de auditoría (auditor de solo lectura y EPP con responsiva).

Commit `ed7b0c3`.

## 19.0.47.1.0 — 2026-09-25

Diagnóstico del SGI como lista nativa de hallazgos.

**Migración (post, `migrations/19.0.47.1.0/post-migrate.py`):** el Diagnóstico del SGI ya no es un wizard HTML. Se borran la acción de ventana y el formulario viejos (Odoo no los quita solo al desaparecer del XML) y se vacía la acción del menú si quedó apuntando a algo borrado.

Commit `34c6f2f`.

## 19.0.47.0.0 — 2026-09-25

Mi procedimiento y Revisión previa solo con vistas nativas.

Commit `6cedb34`.

## 19.0.46.1.0 — 2026-09-25

Mi equipo es una lista nativa y «Ver su procedimiento» desde empleado, puesto y equipo.

Commit `83509fd`.

## 19.0.46.0.1 — 2026-09-25

Inicio no apunta a la acción borrada del Panel de procesos.

**Migración (post, `migrations/19.0.46.0.1/post-migrate.py`):** menús del SGI con acción borrada. Odoo no vacía `action` cuando el menuitem deja de traerla; Inicio quedó apuntando al Panel de procesos retirado en 45.0.0 y abría con «El registro no existe».

Commit `bc41ac5`.

## 19.0.46.0.0 — 2026-09-24

El PDF de «Mi procedimiento» trae lo mismo que la pantalla.

Commit `0de7c80`.

## 19.0.45.0.5 — 2026-09-24

Cuatro pruebas dejan de depender de datos de la copia de producción.

Commit `8f92c32`.

## 19.0.45.0.4 — 2026-09-24

La prueba del religado crea la revisión vieja antes que la vigente.

Commit `72ad0f7`.

## 19.0.45.0.3 — 2026-09-24

Las pruebas de limpieza usan claves con nomenclatura heredada válida.

Commit `9d8b415`.

## 19.0.45.0.2 — 2026-09-24

El religado respeta la familia de clave y no tira el build por datos viejos.

**Migración (post, `migrations/19.0.45.0.2/post-migrate.py`):** Limpieza antes de producción (CEO, 2026-09-24). Deja la base igual que `main`: lo que ya no está en el código se borra aquí de forma explícita (menús, acciones y vistas por xmlid, y los dos menús de Studio con su acción), lo que tiene datos se religa…

Commit `550b250`.

## 19.0.45.0.1 — 2026-09-24

La migración de limpieza leía el xmlid después de borrar el registro.

Commit `f0f1380`.

## 19.0.45.0.0 — 2026-09-24

Limpieza antes de producción (menús, acciones, Studio, religado y retiro por ola).

Commit `9c3c5bd`.

## 19.0.44.2.3 — 2026-09-24

Pruebas de la ficha de proceso respetan el candado de medición y usan un IT binario.

Commit `508c9b2`.

## 19.0.44.2.2 — 2026-09-24

La ficha de proceso hereda sin selectores @string (el build de main fallaba).

Commit `83f72e4`.

## 19.0.44.2.1 — 2026-09-24

El aviso semanal de Mi procedimiento cuelga del puesto desactualizado.

Commit `6996fac`.

## 19.0.44.2.0 — 2026-09-24

Paso 6 de la estructura — «Registrar hallazgo» para el auditor en proceso y actividad.

Commit `7169a34`.

## 19.0.44.1.0 — 2026-09-24

Estructura del SGI pasos 3 y 4 — ficha de proceso con pestañas y ficha de actividad con «Ir a hacerlo».

Commit `fff122b`.

## 19.0.44.0.0 — 2026-09-24

Menú del SGI en 5 entradas por perfil (paso 2 de la estructura).

Commit `1c140ca`.

## 19.0.43.3.0 — 2026-09-24

Mi procedimiento en el orden de la estructura del SGI y pantalla «Mi equipo».

Commit `7c510f4`.

## 19.0.43.2.0 — 2026-09-24

Mi procedimiento — correcciones del CEO, documentos del puesto y Mis pendientes.

Commit `a1b1c63`.

## 19.0.43.1.1 — 2026-09-24

La pantalla Mi procedimiento usa hr.employee.public y sudo (usuarios sin lectura de hr.employee).

Commit `1d56893`.

## 19.0.43.1.0 — 2026-09-24

Pantalla «Mi procedimiento» en Inicio (tarjetas por cadencia, estado, firma, equipo).

Commit `809b343`.

## 19.0.43.0.0 — 2026-09-24

«Mi procedimiento» por puesto (PDF archivado, acuses, vista en Inicio).

Commit `414e976`.

## 19.0.42.1.1 — 2026-09-24

Merge main en la rama del COA (versión 19.0.42.1.1).

Commit `4c09108`.

## 19.0.42.1.0 — 2026-09-24

Etiquetas de la frase del procedimiento en negritas en el PDF.

Commit `423a9ff`.

## 19.0.42.0.6 — 2026-09-24

El campo COA ya no revisa todos los adjuntos al abrir un traslado.

Commit `28e16a4`.

## 19.0.42.0.5 — 2026-09-24

Las pólizas de cierre anual no entran a EX-01, EX-02 ni a las fórmulas.

Commit `e32917b`.

## 19.0.42.0.4 — 2026-09-24

Corrige 5 pruebas de I-7, fórmula y P-40 en el build de main.

Commit `0f8a8e1`.

## 19.0.42.0.3 — 2026-09-24

La extensión de sgi.cron (I-6) es AbstractModel.

Commit `a611492`.

## 19.0.42.0.2 — 2026-09-24

Etiqueta única para los términos de la fórmula.

Commit `b6f4055`.

## 19.0.42.0.1 — 2026-09-24

Herencias del formulario de medición sin selector string.

Commit `dc84fa0`.

## 19.0.42.0.0 — 2026-09-24

Meta con trayectoria y sentido «dentro de un rango» (I-7).

Commit `633813d`.

## 19.0.41.0.0 — 2026-09-24

Plan de acción en rojo (I-4), calendario de cálculo (I-6), ventana visible (I-8) y validación masiva (P-40).

**Migración (post, `migrations/19.0.41.0.0/post-migrate.py`):** I-6: los crons de medición pasan a diarios; el método decide si hoy toca medir (tercer día hábil del mes / lunes).

Commit `04b6f3c`.

## 19.0.40.0.1 — 2026-09-24

AL-01 sin stock.valuation.layer (no existe en Odoo 19).

Commit `41f932b`.

## 19.0.40.0.0 — 2026-09-24

Modo «fórmula configurable» con corrida en paralelo.

Commit `3af6092`.

## 19.0.39.0.1 — 2026-09-24

desperdicio_kg sin «not child_of» y dos pruebas ajustadas a la copia de producción.

Commit `ac61a37`.

## 19.0.39.0.0 — 2026-09-24

Reproceso, diferencia de inventario y energía por tonelada (P-21).

Commit `d029849`.

## 19.0.38.0.0 — 2026-09-24

Fórmulas corregidas de MA-05, EX-01 y EX-02 (I-3).

**Migración (post, `migrations/19.0.38.0.0/post-migrate.py`):** I-3: MA-05, EX-01 y EX-02 pasan a las fórmulas aprobadas, solo si siguen en el modo viejo (una decisión de MAST posterior se respeta).

Commit `7b3549e`.

## 19.0.37.0.0 — 2026-09-24

NC por persistencia (I-5).

Commit `3f5ae92`.

## 19.0.36.0.0 — 2026-09-24

Detalle por medición (I-1) e indicadores confiables (I-2).

Commit `a60c0bf`.

## 19.0.35.0.0 — 2026-09-24

No surtir lotes sin liberar (P-7) y entrega del traslado Embarcar.

**Migración (post, `migrations/19.0.35.0.0/post-migrate.py`):** Liga los traslados internos de 2026 con la entrega de su pedido (sgi_delivery_picking_id), para que C2.21 y C2.26 tengan historia.

Commit `82b68fd`.

## 19.0.34.0.0 — 2026-09-24

Relaciones para ligar entradas (P-3) y vencimiento por campo (P-4).

**Migración (post, `migrations/19.0.34.0.0/post-migrate.py`):** P-3: llena las ligas nuevas con lo que ya existe. - ``documents.document.sgi_doc_change_id``: la última solicitud de cambio documental aprobada que apunta al documento.

Commit `040418d`.

## 19.0.33.0.0 — 2026-09-24

Modos genéricos del indicador (P-1).

Commit `7cdff0d`.

## 19.0.32.0.2 — 2026-09-23

El estado del COA se recalcula al marcar al cliente.

**Migración (post, `migrations/19.0.32.0.2/post-migrate.py`):** Recalcula el estado del COA de las salidas abiertas que ya tienen «Requiere COA»: en 19.0.32.0.1 se quedaron en «no aplica» porque solo se recalculó el requisito y no el estado que depende de él.

Commit `d9b3665`.

## 19.0.32.0.1 — 2026-09-23

La vista del COA en el cliente no lleva group_ids.

Commit `e1d329d`.

## 19.0.32.0.0 — 2026-09-23

COA ligado a la entrega y al pedido (fase 1).

**Migración (post, `migrations/19.0.32.0.0/post-migrate.py`):** COA fase 1 (19.0.32.0.0). - Marca «Requiere COA en cada embarque» en los 12 clientes que lo piden (compañía principal).

Commit `c394075`.

## 19.0.31.0.0 — 2026-09-23

El SGI muestra solo la estructura vigente.

Commit `030deca`.

## 19.0.30.1.2 — 2026-09-23

El reporte del procedimiento no rendía.

Commit `b56d4cd`.

## 19.0.30.1.1 — 2026-09-23

Pruebas aisladas de los datos reales (secciones C y D).

Commit `2eec9bc`.

## 19.0.30.1.0 — 2026-09-23

Calendario hábil, semana ISO, candados probados con usuario real, NC sin etapa.

**Migración (post, `migrations/19.0.30.1.0/post-migrate.py`):** Días hábiles del SGI (19.0.30.1.0): el calendario de la compañía en producción (id 9) es de lunes a jueves y sábado.

Commit `83dcc83`.

## 19.0.30.0.0 — 2026-09-23

Actividades específicas (dónde, cómo, terminado, plazo, si falla, escalamiento).

**Migración (post, `migrations/19.0.30.0.0/post-migrate.py`):** Actividades específicas (19.0.30.0.0): calcula los faltantes de especificación de las actividades que ya existían.

Commit `f72d17d`.

## 19.0.29.5.0 — 2026-09-22

Replaces archiva también las actividades del proceso sustituido.

Commit `ed99744` (PR #333).

## 19.0.29.4.0 — 2026-09-22

Llaves desconocidas son error, replaces, fórmula del indicador, fecha compromiso registrada.

Commit `385f47c` (PR #332).

## 19.0.29.3.0 — 2026-09-22

Condición solo en aprueba/informa; inicio y fin calculados.

Commit `d10f86e` (PR #331).

## 19.0.29.2.0 — 2026-09-22

Estructura en vez de texto — numeral, etapas, entregables que miden y conectan.

**Migración (post, `migrations/19.0.29.2.0/post-migrate.py`):** Estructura en vez de texto (19.0.29.2.0). - Paso entero por actividad, en el orden en que ya estaban (secuencia, id); el numeral se recalcula como clave del proceso + paso.

**Migración (pre, `migrations/19.0.29.2.0/pre-migrate.py`):** Numeral y sección dejan de ser texto: se guardan antes del update. El numeral pasa a calcularse (clave del proceso + paso).

Commit `797494d` (PR #330).

## 19.0.29.1.0 — 2026-09-22

Menú de seis entradas y fichas de proceso y actividad simples.

Commit `3f4fc59`.

## 19.0.29.0.0 — 2026-09-22

Catálogo único, fase 1 (roles, tipos de documento, revisión entera, carga por API).

**Migración (post, `migrations/19.0.29.0.0/post-migrate.py`):** Fase 1 del catálogo — después de cargar modelos y datos. - company_id = 1 (Quimibond) en todo lo del catálogo que aún no la tenga.

**Migración (pre, `migrations/19.0.29.0.0/pre-migrate.py`):** Fase 1 del catálogo — antes de cargar los modelos nuevos. 1. La revisión del documento pasa de texto a entero.

Commit `eeb0257`.

## 19.0.28.1.0 — 2026-09-21

quimibond_sgi: declarar hr_timesheet en depends.

Commit `dab9ef2` (PR #324).

## 19.0.28.0.0 — 2026-08-29

quimibond_sgi: fix multi-compañía del motor de indicadores y saneo de mediciones.

Commit `0218746` (PR #199).

## 19.0.27.0.0 — 2026-08-29

quimibond_sgi: KPIs automáticos del plan de expansión comercial EX-01..EX-16.

Commit `5f85c6c` (PR #194).

## 19.0.26.0.0 — 2026-08-24

Integraciones Sign/eLearning y digest semanal.

Commit `26f13c9`.

## 19.0.25.0.1 — 2026-08-24

**Corregido:** Quitar expand del group en search views (hotfix del build).

Commit `4c9eef8`.

## 19.0.25.0.0 — 2026-08-24

Bump a 19.0.25.0.0 + README de la ola certificable.

Commit `ac63ec1`.

## 19.0.24.0.0 — 2026-08-24

Bump a 19.0.24.0.0 (correcciones de la auditoría 2026-08).

Commit `b75c5c4`.

## 19.0.23.2.0 — 2026-08-24

**Corregido:** Migración pre-update — la vista heredada vieja bloqueaba el rename.

**Migración (pre, `migrations/19.0.23.2.0/pre-migrate.py`):** borra la vista heredada vieja de `activity_ids` antes del update (el rename chocaba con ella).

Commit `9d58eae`.

## 19.0.23.1.0 — 2026-08-24

**Corregido:** activity_ids del proceso chocaba con mail.activity.mixin — el build no cargaba.

Commit `7f7646d`.

## 19.0.23.0.0 — 2026-08-24

**Corregido:** El aviso de eslabón atorado tumbaba el cron — sgi.process sin activity_schedule.

Commit `f1813ac`.

## 19.0.22.2.0 — 2026-08-24

**Corregido:** La resolución de menús tronaba — complete_name no es almacenado.

Commit `5a3d3e1`.

## 19.0.22.1.0 — 2026-08-23

Resolver el menú del formulario de Odoo desde su destino de migración.

Commit `5497628`.

## 19.0.22.0.0 — 2026-08-23

Formularios consistentes y documento tipo «Formulario de Odoo».

Commit `352c9c8`.

## 19.0.21.1.0 — 2026-08-23

Ficha de proceso más clara y con el procedimiento al frente.

Commit `9a824a8`.

## 19.0.21.0.0 — 2026-08-23

Flujo de la cadena, NC automatica y tablero de cumplimiento (fase 11).

Commit `409ba34`.

## 19.0.20.5.0 — 2026-08-23

Referencias entrantes entre procedimientos (fase 10.5).

Commit `ef4a11b`.

## 19.0.20.4.0 — 2026-08-23

La cadena visible en todo el sistema (fase 10.4).

Commit `484d924`.

## 19.0.20.3.0 — 2026-08-23

Encadenamiento entre actividades del procedimiento (fase 10.3).

Commit `3b4b5bb`.

## 19.0.20.2.0 — 2026-08-23

El paso del procedimiento siempre navegable (fase 10.2).

Commit `0bf8c4e`.

## 19.0.20.1.0 — 2026-08-23

Mapeo de medicion por nombre tecnico y cron blindado (fase 10.1).

Commit `e403001`.

## 19.0.20.0.0 — 2026-08-23

Actividades de procedimiento medibles con acciones reales de Odoo (fase 10).

Commit `72cfe15`.

## 19.0.19.6.0 — 2026-08-23

El menu Procesos abre primero el mapa de procesos.

Commit `d1ce7f5`.

## 19.0.19.5.0 — 2026-08-23

Un solo juego de etapas NC compartido por ambos equipos.

Commit `7e1bd68`.

## 19.0.19.4.0 — 2026-08-23

Los menus de Calidad preventiva/Tableros se acomodan antes de Configuracion.

Commit `a127299`.

## 19.0.19.3.0 — 2026-08-23

Del formato a su worksheet en un clic (liga real de migracion).

Commit `93a4445`.

## 19.0.19.2.0 — 2026-08-23

Las encuestas de fase 9 viven en produccion (creadas por MCP), no como semilla.

Commit `a0d8a54`.

## 19.0.19.1.0 — 2026-08-23

Especificaciones del producto (C04-06/C14-02) y EPP por puesto (S03-01).

Commit `1f9b2b8`.

## 19.0.19.0.0 — 2026-08-23

Formularios que sustituyen formatos — ronda sin piso (fase 9).

Commit `72bf990`.

## 19.0.18.4.0 — 2026-08-23

La OC estampa su clave real (F-P-A02-03, no la requisicion).

Commit `d6b45ca`.

## 19.0.18.3.0 — 2026-08-23

El menu raiz de Calidad se DESCUBRE en runtime (sin depender de xmlid).

Commit `317325e`.

## 19.0.18.2.0 — 2026-08-23

Build — el menu de Calidad se recuelga en runtime, sin ref dura.

Commit `535fedb`.

## 19.0.18.1.0 — 2026-08-23

Los Tableros de calidad (Paretos) se mudan del Panel a la app Calidad.

Commit `27b9a40`.

## 19.0.18.0.0 — 2026-08-23

La operacion vive en su app — el SGI solo gestiona.

Commit `6865386`.

## 19.0.17.0.0 — 2026-08-23

Candados AIAG en AMEF (NPR post a la baja) y PPAP por nivel (S/R).

Commit `542c807`.

## 19.0.16.8.0 — 2026-08-21

UX de 'Panel' y 'Revisión por la Dirección' — cierre del tour de menús.

Commit `8bd244f`.

## 19.0.16.7.0 — 2026-08-21

UX de 'Calidad preventiva' — equipos de medición, AMEF, planes y calibraciones.

Commit `afcac97`.

## 19.0.16.6.0 — 2026-08-21

UX de 'Riesgos y auditorías' — el auditor no sale de la auditoría.

Commit `ed16f64`.

## 19.0.16.5.0 — 2026-08-21

UX de 'Medición' — el KPI enseña su configuración y su tendencia.

Commit `9e4bf8e`.

## 19.0.16.4.0 — 2026-08-21

UX de 'Documental' — registrar, difundir y cambiar sin fricción.

Commit `ef875cc`.

## 19.0.16.3.0 — 2026-08-21

UX de 'Mejora continua' — el tablero de NC ya no pierde registros.

Commit `f1939e0`.

## 19.0.16.2.0 — 2026-08-21

UX de 'Mi trabajo' — el empleado puede ver, entender y TERMINAR lo suyo.

Commit `8851442`.

## 19.0.16.1.0 — 2026-08-21

Diagnóstico conectado al piso + ligado rápido de puntos de control.

Commit `370b660`.

## 19.0.16.0.0 — 2026-08-21

Del OTD de proveedores + Diagnóstico del SGI.

Commit `533a896`.

## 19.0.15.2.0 — 2026-08-21

UX de procesos — el sistema se explica solo.

Commit `7d5add7`.

## 19.0.15.1.0 — 2026-08-21

Fase 8 — las señales que Odoo ya registra alimentan la mejora continua.

Commit `2834a82`.

## 19.0.15.0.0 — 2026-08-21

Fase 7 — cierre de bucles ISO (pasos 3-8 de la evaluación).

Commit `88bfbfb`.

## 19.0.14.1.0 — 2026-08-21

Auditoría de flujos — lote 1 de mejoras (quick wins).

**Migración (pre, `migrations/19.0.14.1.0/pre-migration.py`):** Los 6 ir.config_parameter que se declaraban como `<record>` en data (además de sembrarse en sgi.config.seed_parameters) dejan de estar en los XML.

Commit `82f6d8e`.

## 19.0.14.0.0 — 2026-08-17

Registro de fuentes de NC automaticas — interruptor por motivo.

Commit `88c68b1`.

## 19.0.13.12.0 — 2026-07-24

**Cambiado:** Simplificar el modelo del presupuesto sin cambiar comportamiento.

Commit `6cb068b`.

## 19.0.13.11.0 — 2026-07-24

**Corregido:** Multicompañía en el presupuesto — filtrar por compañía y blindar el prefetch.

Commit `38feafb`.

## 19.0.13.10.0 — 2026-07-24

Ajustes post-revisión — precios honestos, gobernanza del revisado y conciliación.

Commit `844d50a`.

## 19.0.13.9.0 — 2026-07-22

Cobertura del pronóstico — pedidos capturados vs pronosticados.

Commit `a85ff37`.

## 19.0.13.8.0 — 2026-07-22

**Corregido:** Import de presupuesto — resolución de cliente exacta antes que parcial.

Commit `36aba44`.

## 19.0.13.7.0 — 2026-07-21

Presupuesto y pronóstico alineados al P-A28 Rev.15 — ciclos, roles e interconexión.

**Migración (post, `migrations/19.0.13.7.0/post-migration.py`):** Mini-fase 5.5 (P-A28 Rev.15): el pronóstico es un documento vivo y ya no se aprueba, su estado terminal es 'revisado'.

Commit `f6df476`.

## 19.0.13.6.0 — 2026-07-21

Control de precios — lista vs facturado y cobertura de listas.

Commit `1d3fe29`.

## 19.0.13.5.0 — 2026-07-21

Análisis del presupuesto — submenú con 4 vistas preconfiguradas.

Commit `24fb45f`.

## 19.0.13.4.0 — 2026-07-21

**Corregido:** Borrar un presupuesto ya no truena (flush sobre líneas eliminadas).

Commit `bc44030`.

## 19.0.13.3.0 — 2026-07-21

**Corregido:** Plantilla de presupuesto — filas por cliente × producto del historial.

Commit `2648ae6`.

## 19.0.13.2.0 — 2026-07-21

Presupuesto — paso 3, panel comercial, curva y cierre accionable.

Commit `bba9d83`.

## 19.0.13.1.0 — 2026-07-21

Presupuesto — paso 2, menú padre, plantilla descargable, ficha guiada.

Commit `40dca6c`.

## 19.0.13.0.0 — 2026-07-21

Presupuesto "solo cantidades" — paso 1, manda la lista, doble moneda.

Commit `afd60a6`.

## 19.0.12.2.0 — 2026-07-21

Pronóstico — consumo (demanda neta) y envío al MPS.

Commit `6f2334d`.

## 19.0.12.1.0 — 2026-07-21

Pronóstico semanal — paso 2, importación, impresión y menús.

Commit `e6ad047`.

## 19.0.12.0.0 — 2026-07-21

Pronóstico semanal por cliente — paso 1, el modelo aprende semanas.

Commit `1b56cfa`.

## 19.0.11.6.0 — 2026-07-21

**Corregido:** Presupuesto de ventas — el pivot puede agregar facturado/pedido.

Commit `aacbecf`.

## 19.0.11.5.0 — 2026-07-21

Presupuesto de ventas — importación desde el Excel real (F-P-A28-18).

Commit `780b740`.

## 19.0.11.4.0 — 2026-07-21

Presupuesto de ventas — precio sugerido desde la lista del cliente.

Commit `b8aecfd`.

## 19.0.11.3.0 — 2026-07-21

Presupuesto de ventas — dimensión cliente opcional (sin doble conteo).

Commit `26c3009`.

## 19.0.11.2.0 — 2026-07-21

Presupuesto de ventas — paso 3, conexión al SGI (VE-02 + cierre de mes).

Commit `cf2d3b5`.

## 19.0.11.1.0 — 2026-07-21

Presupuesto de ventas — paso 2, matriz, menús, import y reporte.

Commit `a1d825d`.

## 19.0.11.0.0 — 2026-07-21

Presupuesto maestro de ventas — paso 1, modelo + real automático.

Commit `707e0c8`.

## 19.0.10.1.0 — 2026-07-21

KPIs automáticos 2.0 — paso 2, parámetros + proxy + RH-02.

Commit `1d23c04`.

## 19.0.10.0.0 — 2026-07-21

KPIs automáticos 2.0 — paso 1, los 4 directos.

Commit `8007db2`.

## 19.0.9.2.0 — 2026-07-21

Abrir el archivo desde las listas de documentos.

Commit `7ff62e5`.

## 19.0.9.1.0 — 2026-07-20

**Corregido:** Cierre olas A+B — semilla no dispara G14; escalar incidente fuerza NC.

Commit `7e5032e`.

## 19.0.9.0.0 — 2026-07-20

**Cambiado:** Paquete OLA A + OLA B.

Commit `ec36895`.

## 19.0.8.1.0 — 2026-07-20

Actividad del procedimiento ligada a un menú real de Odoo.

Commit `be3225a`.

## 19.0.8.0.0 — 2026-07-20

Procedimiento vivo paso 1 — secciones y actividades como datos.

Commit `a6ae15c`.

## 19.0.7.0.0 — 2026-07-20

OLA2 paso 1 — política integral, cabeza de la cascada (H13).

Commit `1b23137`.

## 19.0.6.0.0 — 2026-07-20

OLA1 paso 1 — causa raíz antes que acción (H8) + cierre real de NC mayor.

Commit `8cdc607`.

## 19.0.5.0.0 — 2026-07-20

**Cambiado:** OLA0 paso 1 — sgi.base.mixin, mata duplicacion de folio.

Commit `56b2a4e`.

## 19.0.4.4.17 — 2026-07-20

**Corregido:** Candados criticos de la auditoria ISO (H1, H5, H6).

Commit `99692c7`.

## 19.0.4.4.16 — 2026-07-20

**Corregido:** Quitar target=inline de la accion de Ajustes (Odoo 19 lo elimino).

Commit `2421d59`.

## 19.0.4.4.15 — 2026-07-20

Clic en el valor -> Ver evidencia; el KPI explica su comportamiento.

Commit `56636eb`.

## 19.0.4.4.14 — 2026-07-20

Cada indicador declara su fuente — automatico vs manual.

Commit `17012a0`.

## 19.0.4.4.13 — 2026-07-20

Panel de Ajustes amigable (adios a Parametros del sistema).

Commit `b3c6ab3`.

## 19.0.4.4.12 — 2026-07-20

**Cambiado:** Revision vista por vista de listas y formularios.

Commit `c3ad5d1`.

## 19.0.4.4.11 — 2026-07-20

Siembra de objetivos de proceso desde los SIPOC reales.

Commit `89a2f15`.

## 19.0.4.4.10 — 2026-07-20

Navegacion reagrupada — de 21 entradas de primer nivel a 9.

Commit `1bc2f97`.

## 19.0.4.4.9 — 2026-07-20

**Cambiado:** Limpieza de auditoria integral.

Commit `fe18f1b`.

## 19.0.4.4.8 — 2026-07-20

**Corregido:** Quitar expand del grupo de busqueda (rompia el build de main).

Commit `6813a22`.

## 19.0.4.4.7 — 2026-07-20

**Cambiado:** Mejores practicas de UI/UX en todas las vistas.

Commit `407ed89`.

## 19.0.4.4.6 — 2026-07-20

Menu Mis acciones — la lista personal de pendientes del SGI.

Commit `b8010d4`.

## 19.0.4.4.5 — 2026-07-20

Familia documental por nomenclatura + referencias cruzadas.

Commit `9bdbde7`.

## 19.0.4.4.4 — 2026-07-20

Ficha completa de proceso (caracterizacion navegable).

Commit `5382ca7`.

## 19.0.4.4.3 — 2026-07-20

**Corregido:** Claves reales de venta segun P-A28 Rev.15.

Commit `a483288`.

## 19.0.4.4.2 — 2026-07-20

**Corregido:** Endurecer noupdate en crons y mapeo de claves ya existentes.

Commit `567b233`.

## 19.0.4.4.1 — 2026-07-20

**Corregido:** Siembra de parametros idempotente (el build fallaba por clave duplicada).

Commit `54eae11`.

## 19.0.4.4.0 — 2026-07-20

Mapa completo de entradas/salidas entre procesos (mini-fase 4.8).

Commit `eafc6b8`.

## 19.0.4.3.1 — 2026-07-20

**Corregido:** Pie de clave SGI dentro del area imprimible (div.page).

Commit `6ac758f`.

## 19.0.4.3.0 — 2026-07-20

Claves de formato SGI en pantalla y PDF (mini-fase 4.7).

Commit `6cd925d`.

## 19.0.4.2.1 — 2026-07-20

**Corregido:** action_view_sgi_ppap también en product.product.

Commit `69e45e4`.

## 19.0.4.2.0 — 2026-07-20

**Corregido:** Diferencia Panel (tablero de salud) de Procesos (administración).

Commit `170211a`.

## 19.0.4.1.0 — 2026-07-17

Menú de seguimiento de migración de formatos a Odoo.

Commit `105d089`.

## 19.0.4.0.0 — 2026-07-17

Liga la calidad operativa del piso al SGI (Fase 4.1).

Commit `57aec54`.

## 19.0.3.0.0 — 2026-07-17

Fase 3 — herramientas automotrices (core tools).

Commit `ab08126`.

## 19.0.2.0.0 — 2026-07-17

Objetivos, indicadores y medición F-P-A10-03 (Fase 2).

Commit `35844c7`.

## 19.0.1.0.0 — 2026-07-17

Estructura, seguridad, catálogos y mapa de procesos.

Commit `f992ba6`.
