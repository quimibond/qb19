# 11 — Documentación existente y estructura final (agente K)

**Fecha:** 2026-09-29 · **Módulo:** `quimibond_sgi` 19.0.56.24.1 (igual en producción) · **Modo:** solo lectura (repo, `git log` en lectura, `tools/check_addons.py`, MCP de producción con `search_records`/`aggregate_records`).
**Apoyo (`docs/audit/11-documentacion/`):**

| CSV | Qué trae |
|---|---|
| `inventario_documentos.csv` | Los 20 documentos y fuentes de documentación: versión que describen, qué cubren, estado, destino, qué se rescata y a quién contradicen |
| `afirmaciones_verificadas.csv` | 53 afirmaciones concretas de los documentos, cotejadas con grep, el checker o producción |
| `estructura_propuesta.csv` | Cada archivo de la estructura final: audiencia, formato, fuente, si se genera y cómo, quién lo mantiene |

## 1. Resumen

- **Hoy no hay un solo documento vigente del SGI.** Los manuales describen versiones que ya pasaron: el técnico la 19.0.4.1.0, el de usuario más o menos la 19.0.13, el diagrama la 19.0.18 y la deuda técnica la 19.0.14. El README del módulo es un changelog de 2,578 líneas desordenado que termina en 56.24.0. De 53 afirmaciones cotejadas, **solo 11 son ciertas** (2 de ellas van contra una decisión de Jose). 20 están desactualizadas, 11 son falsas, 10 contradicen una decisión, otro documento o al propio documento, y 1 es parcial.
- **El manual de usuario manda a la gente a menús que no existen.** Tiene 11 rutas falsas («Mi trabajo», «Medición», «Auditorías y riesgos», «Documental», «Automotriz»), enseña a buscar por clave del Dropbox, en contra de la decisión 1, y dice que el checklist de auditoría va en Encuestas, lo que se quitó en 50.0.0 (K-001).
- **Ningún documento recoge las decisiones de Jose**: «Del Dropbox a Odoo», instalación vacía, `quimibond_sgi_mapa`, clave `PR-{proceso}` con la validación prendida, módulos que salen, grupo Salud ocupacional, alcance del Auditor y Dirección sin Jefe MAST. El README contradice dos de ellas: instala 5 macroprocesos y 16 procesos, y la clave anterior se busca solo durante 12 meses (K-004).
- Un documento de `docs/` no es del SGI y además está muerto: `audit_invariants.md` describe un modelo y unas tablas de Supabase que se borraron el 2026-09-18 (K-012).
- **Faltan los seis bloques de la fase 4**: técnica, manual de administración, manuales por rol, `help` y ayuda contextual, guía de transición y CHANGELOG. Lo técnico se puede **generar del código** (diccionario, matriz de seguridad, crons, parámetros, menús) con un script y un chequeo en CI (K-021).
- **El CHANGELOG se puede reconstruir.** `git log` trae 117 versiones con fecha y mensaje, de 28.0.0 (2026-09-04) a 56.24.1, porque el clon es superficial. Lo anterior sale del README (84 versiones citadas), del manual técnico §12 y de las 41 carpetas de migración. **Hay que hacerlo antes de retirar las migraciones** (A-026) (K-018).
- Propongo `docs/sgi/` con cuatro partes (usuarios, administración, transición y técnica), `docs/historico/sgi/` para 9 documentos, un README del módulo de 150 líneas como máximo y `CHANGELOG.md` junto al módulo (§6).
- Conteo: **Alta 7 · Media 12 · Baja 5** (24 hallazgos).

## 2. Tabla de hallazgos

| ID | Elemento | Hallazgo | Evidencia | Severidad | Acción | Propuesta concreta | Esfuerzo (h) | Depende de |
|---|---|---|---|---|---|---|---|---|
| K-001 | `docs/SGI_MANUAL_USUARIO.md` (304 líneas) | Es el único manual para el personal y **está desactualizado en casi cada ruta**. 11 caminos de menú no existen: `:41` «Mi trabajo → Mis procedimientos», `:64,88` «Mejora continua → No Conformidades → Tablero/Concentrado», `:141` «Medición → Indicadores», `:209,217,261` «Auditorías y riesgos», `:241` «Automotriz», `:268` «Documental → Migración de formatos». Enseña la clave del Dropbox como forma de buscar (`:36-37`), contra la decisión 1 y la respuesta 11 de la tanda 2. Dice que el checklist de auditoría va en Encuestas (`:209-210`), contra AU-1 (hecho en 50.0.0). En `:153` y `:181` trae dos versiones incompatibles del presupuesto. El rol Auditor (`:19`) no coincide ni con el código ni con la decisión. | `afirmaciones_verificadas.csv` n.º 24–36; `inventario/menus.csv`; `security/sgi_security.xml:43` | Alta | Mover | Archivar en `docs/historico/sgi/` con una nota de «no usar» al principio. Lo sustituyen los manuales por rol (§6.3). Rescatar la FAQ (§15), la regla de convivencia (§16), el porqué de los candados de NC (§3.3) y las fuentes automáticas (§3.6). | 1 | — |
| K-002 | `docs/SGI_MANUAL_TECNICO.md` (525 líneas) | Declara la v19.0.4.1.0 y le faltan o le sobran piezas: 16 dependencias en vez de 31, 4 grupos en vez de 6, «el Auditor implica Usuario» (falso: lo **quita**), 10 crons en vez de 26, «~64 tests» en vez de 87, 7 fuentes de NC en vez de 10. Dice que hay «6 advertencias vivas» del checker y hoy hay 0. Pone `main` = staging y una rama `SGI`, contra el RUNBOOK. Dice «jamás subir versión de módulos de piso», contra CLAUDE.md. Dice que el mapa es «estructural, `noupdate=0`», cuando `sgi_process_data.xml` es `noupdate="1"` y la decisión 6 lo saca del módulo. Se contradice solo con el escalamiento de las NC externas (`:457` parámetro contra `:498` «fijo en código»). | `afirmaciones_verificadas.csv` n.º 1–16; `python3 tools/check_addons.py` → «0 error(es), 0 advertencia(s)» | Alta | Mover | Archivar. Rescatar el contrato de `sgi_auto_create` y cómo agregar una fuente (`:156-206`) para `tecnica/extender.md`, la tabla de `tools/` (`:331-340`) y el racional de «un parámetro en un solo lugar» (`:406-421`). Todo lo demás se **genera** (§6.5). | 1 | — |
| K-003 | `addons/quimibond_sgi/README.md` (2,578 líneas) | No es un README: es un changelog **sin orden**, que va de «Fases 1–8» a 56.x y vuelve a 53.x, 54.x y 55.x. Por ejemplo, 56.24.0 está en `:2472` y 56.23.0 en `:2499`. De las 117 versiones que registra `git log`, **41 no tienen entrada**, entre ellas 56.24.1 (la instalada). La introducción (`:9-58`) describe la instalación de la Fase 1, con 4 grupos, mapa de procesos, carpetas 00–23 y los crons de entonces. En `:881` dice «64 tests». En `:835-840` omite `odoosh-restart cron` y prohíbe subir la versión de otros módulos. Contenido valioso enterrado: `load_payload` (`:596-723`), la lógica de indicadores (`:892-1018`), la fórmula configurable (`:1552-1594`) y el alcance multiempresa (`:802-813`). | `grep -n "^#" README.md`; `comm` de versiones de git contra el README (§7) | Alta | Corregir | Partirlo: (1) README nuevo de ≤150 líneas con qué es, instalación vacía, dependencias, satélites, liga a `docs/sgi/` y a `CHANGELOG.md`; (2) las secciones por versión, reordenadas, pasan a `CHANGELOG.md`; (3) el contenido técnico pasa a `docs/sgi/tecnica/`. Amplía B-023. | 6 | K-018 |
| K-004 | Todos los documentos contra `decisiones.md` | **Ningún documento recoge las decisiones de Jose.** `grep -ci` en README y `docs/SGI_*`: «Del Dropbox» aparece 0 veces con el sentido decidido (solo menciones sueltas en el README), «quimibond_sgi_mapa» 0, «instalación vacía» 0, «Salud ocupacional» 1 (una pestaña, no el grupo). El README contradice dos decisiones: instala el mapa (`:17-18`, contra la decisión 6) y dice que la clave anterior «se encuentra 12 meses» (`:510-511`). El código hace eso (`models/sgi_document.py:523-526`, `relativedelta(months=12)`), pero la decisión 7 pide un buscador permanente (ver C-004). | `afirmaciones_verificadas.csv` n.º 37, 41 | Alta | Agregar | La documentación nueva nace con las decisiones como punto de partida (§6). Mientras tanto, agregar al README un aviso de 10 líneas con liga a `docs/audit/decisiones.md`. | 0.5 | — |
| K-005 | `docs/SGI_BLUEPRINT.md` (229 líneas, jul-2026) | Es el plan original, anterior a la 19.0.1. El principio 5 (`:17`) pide conservar las claves P-Xnn/IT/F/DAT en el sistema, contra la decisión 1. El principio 2 («Cero Studio», `:16`) choca con `depends: web_studio` (A-010). El mapa bloque → app nativa (§3) sirve como historia del diseño. | `afirmaciones_verificadas.csv` n.º 52–53 | Media | Mover | Archivar. Los principios 1, 3, 4 y 6 se reescriben en `tecnica/arquitectura.md §Principios` con la regla de Studio actualizada según lo que decida A-010. | 0.5 | A-010 |
| K-006 | `docs/SGI_DIAGRAMA_FLUJOS.html` (914 líneas, v19.0.18.0.0) | Es el único diagrama de conjunto y está viejo. Dice «17 crons» (hay 26) y «grupos en cadena lineal, cada uno implica al anterior», lo cual es falso: el Auditor quita Usuario, Captura implica Usuario, y Administrador y Dirección implican Jefe MAST. Además los menús son los de antes de la decisión 2. | `SGI_DIAGRAMA_FLUJOS.html:88,147,785`; `security/sgi_security.xml:43,66,73,256` | Media | Mover | Archivar. Los diagramas nuevos se generan en Mermaid dentro de los `.md` (módulos, modelos, grupos) y el de flujo NC/actividad se dibuja a mano una sola vez en `arquitectura.md`. | 0.5 | — |
| K-007 | `docs/SGI_DEUDA_TECNICA.md` (v19.0.14) | Mezcla puntos cerrados y abiertos, y tiene afirmaciones falsas: «sin `ir.rule` en todo el addon» (hoy hay 32) y «README hasta Fase 4.6». Declara pendientes B.16, B.18, B.19, C.22, C.24, C.25 y D.29 sin fecha ni dueño. | `SGI_DEUDA_TECNICA.md:19-20,127`; `grep -c ir.rule` → 32 | Media | Mover | Antes de archivarlo, cotejar esos 7 pendientes con 03-modelo y 07-logica. Los que sigan abiertos entran al backlog de esta auditoría con ID propio; después se archiva. | 1.5 | — |
| K-008 | `docs/SGI_EVALUACION_ESTRUCTURA.md` (v19.0.14.1) | El «menú objetivo» (Panel / Mi trabajo / Medición / Automotriz) quedó superado por la decisión 2. «Solo `sgi.sales.budget` tiene `company_id`» es falso (hoy son 24 de 74, C-012). «`_sgi_manager_user_id` toma al primero» ya cambió: ahora lee `quimibond_sgi.mast_user_id` (`sgi_cron.py:58-62`). | `afirmaciones_verificadas.csv` n.º 17–19 | Baja | Mover | Archivar. La tabla de «bucles que cierran» (§2) se usa como insumo de `arquitectura.md §Flujos`. | 0.3 | — |
| K-009 | `docs/SGI_AUDITORIA_OLAS_AB.md` y `docs/AUDITORIA_FUNCIONAL_SGI_2026-09-28.md` | **Chocan los identificadores.** OLAS_AB usa G11–G21, la auditoría funcional usa G1–G13 con otro significado, y esta auditoría usa G-001… (agente G). Si un commit o un comentario dice «G12», no hay forma de saber a cuál se refiere. OLAS_AB además depende de un informe que no está en el repo (`:419-429`). | `SGI_AUDITORIA_OLAS_AB.md:1-20`; `AUDITORIA_FUNCIONAL…:31-47`; `07-logica.md` | Media | Documentar | Archivar los dos con una nota que diga a qué corresponde cada «G». En el índice de lo histórico, renombrar la referencia a `OLAS-G11…` y `FUNC-G1…`. Regla nueva: cada auditoría con prefijo propio y fecha. | 0.5 | — |
| K-010 | `docs/SGI_PENDIENTES_PROGRAMACION.md` | El título dice «Lo que falta programar (29 puntos)», pero **los 29 dicen «Hecho»**. Quien lo abra creerá que falta todo. El dato «Jefe MAST solo Blanca Areli» (PERM-2) coincide con producción (318 con 1 miembro directo, `06-seguridad.md`). | columna «Estado» `:462-523` | Baja | Mover | Archivar. Los «Listo cuando» sirven como casos de prueba de los manuales por rol, porque son la aceptación funcional. | 0.3 | — |
| K-011 | `docs/AUDITORIA_FUNCIONAL_SGI_2026-09-28.md` | Es el único recorrido **por persona real**, y por eso es la mejor plantilla para los manuales por rol. Pero ya quedó atrás: dice que producción tiene 56.1.0 (hoy 56.24.1) y que 318 lo tienen 7 personas (hoy 1 directo y 5 efectivos). Las correcciones C10–C11 dicen «en curso» y C13–C19 «propuesto», sin estado posterior. C12 ya está hecha: `sgi.employer.obligation` no existe. | `afirmaciones_verificadas.csv` n.º 46–48 | Media | Mover | Pasarlo a `docs/historico/sgi/`, con la liga desde `docs/audit/` como insumo. Antes, cotejar C13–C19 con los informes 02–08: lo que no esté cubierto va al backlog. | 1 | — |
| K-012 | `docs/audit_invariants.md` | **No es del SGI y está muerto.** Describe `env['quimibond.sync.audit'].run_all(...)` y tablas `odoo_*` de Supabase. Según CLAUDE.md, el modelo se eliminó y las tablas se borraron el 2026-09-18. Aparece en `docs/` junto a los del SGI y confunde. | `audit_invariants.md:384`; CLAUDE.md «Tabla huérfana… `quimibond_sgi_audit`» | Baja | Mover | Archivarlo en `docs/historico/intelligence/`, fuera del paquete SGI. | 0.2 | — |
| K-013 | `docs/RUNBOOK_DESPLIEGUE.md` | Vigente. Tiene **una afirmación falsa del SGI** («6 claves de config con doble declaración», `:167-168`; hoy el checker da 0 advertencias y no hay `<record model="ir.config_parameter">` en el SGI) y **ninguna verificación propia del SGI**: menú contra el árbol decidido (B-004), crons forzados a `noupdate` (A-007), carga del mapa en modo de prueba, etiqueta de git antes de retirar migraciones, «Del Dropbox a Odoo» visible. | `python3 tools/check_addons.py`; `grep -rn ir.config_parameter addons/quimibond_sgi*` | Media | Corregir | Quitar el punto 2 de «Deuda abierta». Agregar una sección «SGI» de media página que ligue a `docs/sgi/tecnica/upgrade-y-migraciones.md`, con las consultas de verificación. | 1 | A-007, B-004 |
| K-014 | `__manifest__.py:5-19` (`description`) y `static/description/` | La descripción que ve quien abre Apps dice «migración de formatos rastreable» y «cero Studio», y enumera PPAP, AMEF, CoA, calibraciones y presupuesto, que salen del SGI. No menciona procesos, actividades, roles, Mis pendientes, Mi procedimiento ni la instalación vacía. No hay `static/description/index.html` (solo `icon.png`). | `__manifest__.py:5-19,54`; `ls static/description` | Media | Corregir | Reescribirla al terminar la salida de módulos (6–8 líneas: qué es, qué **no** trae —el mapa va en `quimibond_sgi_mapa`—, liga a `docs/sgi/`). El `index.html` no hace falta: el módulo no se publica. | 0.3 | A-016…A-020 |
| K-015 | Satélites `quimibond_sgi_pesaje`, `_plm`, `_revisado` | No tienen README. Las descripciones del manifest están bien, salvo: `_plm` describe «ECO → PPAP», y PPAP sale al satélite automotriz (decisión tanda 1, preg. 5), y `_pesaje` fija «±3 kg» en el texto cuando es el parámetro `pesaje_tolerance_kg`. | manifests de los satélites | Baja | Agregar | README de media página por satélite (qué gancho agrega, qué parámetro usa, de qué depende). Reescribir `_plm` cuando se mude PPAP. | 1 | salida de PPAP |
| K-016 | `CLAUDE.md` (qb19), sección «Otros módulos del repo» | Documenta 8 módulos, pero **no el SGI** ni sus satélites, que es el módulo más grande del repo. Las reglas de build que nacieron del SGI sí están. Un agente nuevo no sabe dónde está la documentación del SGI ni que existen `decisiones.md` y la regla «estructura contra datos». | `grep -n quimibond_sgi CLAUDE.md` → solo reglas (`:101-108`) | Media | Agregar | Un párrafo: qué es, versión, satélites, «se instala vacío (mapa en `quimibond_sgi_mapa`)», liga a `docs/sgi/README.md` y `docs/audit/decisiones.md`. | 0.3 | — |
| K-017 | Docstrings y comentarios del código | Los 102 modelos propios tienen `_description`, pero solo **21 tienen docstring de clase**. De 254 `_compute`, 12 tienen docstring; de 303 `action_*`, 106; de 33 métodos de cron, 19. En cambio, la historia vive en el código: **65 comentarios XML** y **69 comentarios `#`** en `models/` citan versión, fase, PR u ola (ya señalado en B-023). Sin docstrings, el diccionario generado sale sin la columna «para qué». | script AST sobre `models/*.py` (§8, comando 1) | Media | Agregar | Docstring de 1–3 líneas en los 81 modelos que no lo tienen (qué representa, quién lo crea, qué lo cierra) y en los métodos `cron_*`, porque de ahí sale `crons.md`. La historia de los comentarios pasa al CHANGELOG cuando se toque cada archivo, no en un PR masivo. | 8 | K-021 |
| K-018 | `CHANGELOG.md` (no existe) | No hay changelog. La historia está repartida en el README (84 versiones citadas y desordenadas), el manual técnico §12 (1.0–4.1), 41 carpetas de migración con su docstring y los commits. El clon es **superficial** (`git rev-parse --is-shallow-repository` → `true`): el log llega a 28.0.0 (2026-09-04), con 117 versiones y mensajes con cuerpo explicativo (p. ej. `4415976` para 56.24.1). A-026 propone retirar las migraciones: **si se retiran antes, se pierde la fuente de 41 entradas.** | §7 | Alta | Agregar | Reconstruir con el procedimiento de §7 en un PR propio **antes** de A-026. Desde ahí, regla: cada bump agrega su entrada en el mismo PR. Se puede revisar en CI: si cambia la versión del manifest, debe cambiar `CHANGELOG.md`. | 6 | A-026 (va antes) |
| K-019 | `help` de campos y ayuda contextual en vistas | 1,284 campos sin `help` (608 de prioridad alta), ya en C-018. 8 acciones sin `help` y un título con ID de plan («Lista maestra de documentos (DOC-3)»), ya en D-015. En las 65 vistas XML hay solo 47 `help=` sobre campos, en 12 archivos, y 43 `alert` de ayuda. **Aquí solo agrego la regla de redacción y el orden** (§5), para no duplicar C-018 ni D-015. | `03-modelo/campos_sin_help.csv`; `04-vistas/acciones_titulo_ayuda.csv`; `grep -c 'help="' views/*.xml` | Media | Agregar | Aplicar §5: primero los campos que ve el Usuario SGI en Mis pendientes, Mi procedimiento y la NC, y después lo de MAST. El texto se escribe como ayuda del manual por rol, así el manual y la pantalla dicen lo mismo. | ver C-018 | C-018, D-015 |
| K-020 | Consumidores del SGI fuera del módulo | **No documentados.** `quimibond_intelligence/models/senales/*.py` lee 11 modelos `sgi.*`, entre ellos `sgi.ppap`, `sgi.supplier.eval`, `sgi.risk`, `sgi.indicator.measure`, `sgi.document.ack` y `sgi.competence.gap`. `qb_obligation` tiene el tipo `sgi.record` (`qb_obligation.py:74`). Al sacar PPAP o renombrar modelos, nadie sabría que hay que revisarlos. Las señales se protegen con `tiene_modelo()` (`calidad_sgi.py:71`), así que no truenan, pero **callan**. | `grep -ohE "'sgi\.[a-z_.]+'" addons/quimibond_intelligence/models/senales/*.py` | Media | Documentar | Sección «Quién más lee el SGI» en `tecnica/integraciones.md`, generada con ese grep. Agregar al checklist de toda salida de módulo: «revisar señales». | 1 | — |
| K-021 | Documentación técnica generable (no existe) | No hay diccionario de datos, matriz de seguridad, lista de crons, lista de parámetros ni árbol de menús **vigentes**. Los que hubo se escribieron a mano y se quedaron en 4, 10, 17 o 64. El inventario de esta auditoría (`docs/audit/scripts/inventario_codigo.py`) ya extrae todo eso con AST y la librería estándar. | `docs/audit/inventario/*.csv` | Alta | Agregar | `tools/sgi_docs.py` (derivado del script de inventario, sin importar Odoo) genera `docs/sgi/tecnica/{modelo-de-datos,diccionario/*,vistas-y-acciones,seguridad,crons,parametros}.md`. Con `--check` falla si lo generado difiere de lo comprometido, y corre en CI junto a `check_odoo_views.py`. Así lo técnico no vuelve a quedar viejo. | 12 | — |
| K-022 | Manuales por rol y de administración (no existen) | No hay manual de Jefe MAST y SGI ni manuales por rol (Usuario SGI, jefe de área, Dirección, Auditor, capturista de planta). Tampoco la sección «Dónde quedó lo que usabas en el Dropbox». Sin staging, las capturas no se pueden tomar (**pendiente de verificación visual**). | ausencia en `docs/` y en Knowledge (sin verificar en Knowledge) | Alta | Agregar | Índices en §6.2 y §6.3. Se escriben cuando esté estable el árbol de menús (E) y la nomenclatura (D), porque si no, nacen viejos como el anterior. | 30 | E-001, E-007, D-002, agente L |
| K-023 | Guía de transición (no existe como documento) | Los datos de la transición existen: 427 documentos, el xlsx «rutina por rutina» con 815 rutinas y la clasificación de 52 procedimientos en `decisiones.md`. Pero no hay un documento para el personal ni para el auditor externo que explique la sección «Del Dropbox a Odoo», la regla de convivencia y qué pasa con P-I01. | `decisiones.md` «Transición (para el agente L)» | Media | Agregar | Índice en §6.4. El contenido de datos lo arma el agente L; aquí solo va la estructura. | 6 | agente L, E-001 |
| K-024 | Carpetas de Documentos que citan los manuales | El manual dice «carpeta SGI → secciones 00-23». En producción hay **tres carpetas raíz** «SGI»: 1119 (con 00…**24**, «POR CLASIFICAR» con 8 documentos y «0. ANEXOS (OBSOLETO - duplicado)» con 15), 1805 «SGI» y 3078 «SGI 2026», estas dos sin documentos. El manual nuevo no puede decir «abre la carpeta SGI» mientras haya tres. | MCP `documents.document` `type=folder`, `folder_id=false` y `folder_id in [1119,1805,3078]` | Baja | Decidir | Jose decide cuál es la carpeta del SGI. Las otras dos se archivan (no se borran). Los manuales citan la carpeta por su nombre final. | 0.5 | — |

## 3. Inventario de lo que existe (resumen; detalle en `inventario_documentos.csv`)

| Documento | Versión que describe | Estado | Destino |
|---|---|---|---|
| `addons/quimibond_sgi/README.md` | 19.0.1 → 56.24.0 (desordenado) | Desactualizado | Partir en README corto + CHANGELOG + `docs/sgi/tecnica/` |
| `docs/SGI_BLUEPRINT.md` | antes de 19.0.1 (jul-2026) | Histórico | `docs/historico/sgi/` |
| `docs/SGI_MANUAL_TECNICO.md` | 19.0.4.1.0 (+ trozos 10–13) | Desactualizado | Histórico; se reescribe generado |
| `docs/SGI_MANUAL_USUARIO.md` | ≈19.0.13 | Desactualizado | Histórico; lo reemplazan los manuales por rol |
| `docs/SGI_DEUDA_TECNICA.md` | 19.0.14.0.0 | Histórico con pendientes | Pendientes al backlog; luego histórico |
| `docs/SGI_EVALUACION_ESTRUCTURA.md` | 19.0.14.1.0 | Histórico | Histórico |
| `docs/SGI_AUDITORIA_OLAS_AB.md` | 19.0.9.1.0 | Histórico | Histórico (con nota de IDs) |
| `docs/SGI_PENDIENTES_PROGRAMACION.md` | hasta 53.1.0 | Cerrado (29/29 hechos) | Histórico |
| `docs/SGI_DIAGRAMA_FLUJOS.html` | 19.0.18.0.0 | Histórico | Histórico |
| `docs/AUDITORIA_FUNCIONAL_SGI_2026-09-28.md` | 56.6.2/56.7.0 (prod. 56.1.0) | Insumo | Histórico, ligado desde `docs/audit/` |
| `docs/audit_invariants.md` | abr-2026, intelligence | Obsoleto y ajeno | `docs/historico/intelligence/` |
| `docs/RUNBOOK_DESPLIEGUE.md` | vigente | Vigente con 1 error | Se queda; se corrige (K-013) |
| `description` del manifest | ≈19.0.15 | Desactualizado | Reescribir (K-014) |
| Descripciones de los satélites | vigentes | Vigentes (`_plm` cambia con PPAP) | Se quedan; README corto (K-015) |
| `CLAUDE.md` | vigente | Sin el SGI | Agregar párrafo (K-016) |
| Docstrings / comentarios | — | Parcial / con historia | K-017 |
| `help` de campos y acciones | — | Hueco | C-018, D-015, K-019 |

**Qué se fusiona:** el contenido técnico que sigue vigente, hoy repartido entre el README, el manual técnico §5–§11 y el apéndice, se junta en `docs/sgi/tecnica/`. Los recorridos por persona de la auditoría funcional, el manual de usuario (FAQ, convivencia) y los «Listo cuando» de PENDIENTES se juntan como insumo de los manuales por rol. La historia del README, el manual técnico §12, las migraciones y `git log` se juntan en `CHANGELOG.md`.

**Contradicciones entre documentos (para no heredarlas):**

1. Ramas: el manual técnico dice `main` = staging y rama `SGI`; el RUNBOOK y CLAUDE.md dicen `main` = desarrollo, `quimibond` = producción y `qbtesting` = pruebas.
2. Versionado: el README y el manual técnico dicen «no subir la versión de otros módulos»; CLAUDE.md dice subir por default, con la única excepción de `no_bump.txt` (`quimibond_intelligence`).
3. Claves: el Blueprint y el manual de usuario conservan las claves del Dropbox en pantalla; la decisión 1 y la respuesta 11 lo prohíben.
4. Auditoría: el manual de usuario pone el checklist en Encuestas; PENDIENTES AU-1 lo quitó.
5. Grupos: el diagrama dice cadena lineal; el manual técnico dice que el Auditor implica Usuario; el código dice otra cosa (K-006, K-002).
6. Parámetros: el manual técnico `:457` contra `:498`; el RUNBOOK repite una deuda que ya se resolvió.
7. Instalación: el README instala el mapa; la decisión 6 dice que se instala vacío.

## 4. Verificación de afirmaciones (muestra de 53)

`11-documentacion/afirmaciones_verificadas.csv` (documento:línea, afirmación, cómo se verificó, resultado, evidencia).

| Resultado | Cantidad | Ejemplos |
|---|---:|---|
| Cierta | 11 (2 van contra una decisión) | `sgi_auto_create` (`sgi_nonconformity.py:454`), satélites `auto_install`, `sgi.competence.gap` con `_auto=False`, patrón `PR-{process}`, reglas por empresa, Administrador implica Jefe MAST; el Director implica Jefe MAST y la búsqueda de 12 meses están bien descritas pero van contra una decisión |
| Desactualizada | 20 | versiones, conteo de dependencias, grupos, crons, tests, fuentes de NC, versión de producción, miembros de 318 |
| Falsa | 11 | «Auditor implica Usuario», «sin `ir.rule`», «6 advertencias del checker», «mapa `noupdate=0`», 4 rutas de menú del manual de usuario, `quimibond.sync.audit` |
| Contradice decisión, otro documento o a sí misma | 10 | claves del Dropbox, instalación del mapa, ramas, bump, Encuestas, presupuesto, escalamiento externo |
| Parcial | 1 | carpeta «SGI 00-23» (hay tres carpetas y la sección 24) |

## 5. `help` y ayuda contextual: regla y orden (complementa C-018 y D-015)

**Regla de redacción** (la misma para `help`, ayuda de acción y manual):

- Para quién: la persona que llena el campo, no el programador. Una o dos frases: qué es, de dónde sale si se calcula, y qué pasa si se deja vacío.
- Segunda persona («tú»), como las pantallas de 56.7.0. Sin emojis ni jerga interna (C11 de la auditoría funcional). **Sin claves del Dropbox ni IDs de plan** (DOC-3, G14, PR 5) (decisión 1).
- Los campos calculados dicen **cada cuánto** se recalculan (p. ej. «foto; se actualiza con el botón o el cron mensual»).
- Los campos que solo ve MAST lo dicen («Solo Jefe MAST y SGI»).

**Orden**, de lo que ve más gente a lo que ve menos:

1. Mis pendientes, Mi procedimiento y Mis indicadores (`sgi.my.procedure` tiene 51 campos sin help).
2. NC y acciones (`quality.alert`, 43 de prioridad alta).
3. Actividad, rol, entregable y entrada.
4. Documento controlado.
5. Indicador y medición.
6. Seguridad y ambiente.
7. Dirección.
8. Configuración (MAST).

**Ayuda contextual en vistas:**

- Cada `act_window` del árbol decidido con `help` de estado vacío (qué es la lista, quién crea registros y cómo), ya en D-015.
- En las fichas de Mis pendientes, Mi procedimiento, NC, actividad y documento, un bloque `alert-info` plegable «¿Qué hago aquí?», con 3 líneas y la liga al manual de su rol.

**Chequeo:** `tools/sgi_docs.py --check` avisa (advertencia, no error) cuando un campo **nuevo** visible no trae `help`. Así el hueco no vuelve a crecer mientras se paga el existente.

## 6. Estructura final propuesta de `docs/`

```
docs/
├── RUNBOOK_DESPLIEGUE.md              (se queda; + sección SGI, K-013)
├── audit/                             (esta auditoría; se queda)
├── historico/
│   ├── sgi/
│   │   ├── README.md                  (índice: qué es cada uno, por qué se archivó, a qué versión corresponde, IDs G·)
│   │   ├── SGI_BLUEPRINT.md
│   │   ├── SGI_MANUAL_TECNICO.md
│   │   ├── SGI_MANUAL_USUARIO.md
│   │   ├── SGI_DEUDA_TECNICA.md
│   │   ├── SGI_EVALUACION_ESTRUCTURA.md
│   │   ├── SGI_AUDITORIA_OLAS_AB.md
│   │   ├── SGI_PENDIENTES_PROGRAMACION.md
│   │   ├── SGI_DIAGRAMA_FLUJOS.html
│   │   └── AUDITORIA_FUNCIONAL_SGI_2026-09-28.md
│   └── intelligence/
│       └── audit_invariants.md
└── sgi/
    ├── README.md                      (índice: «¿quién eres?» → qué leer)
    ├── glosario.md
    ├── usuarios/
    │   ├── usuario-sgi.md
    │   ├── jefe-de-area.md
    │   ├── direccion.md
    │   ├── auditor.md
    │   ├── capturista-de-planta.md
    │   ├── salud-ocupacional.md       (si Jose lo aprueba, pregunta 3)
    │   └── img/                       (capturas: pendiente de verificación visual, requieren staging)
    ├── administracion/
    │   └── manual-jefe-mast.md
    ├── transicion/
    │   ├── del-dropbox-a-odoo.md
    │   └── equivalencias.csv          (generado de producción)
    └── tecnica/
        ├── arquitectura.md
        ├── modelo-de-datos.md         (generado)
        ├── diccionario/<modelo>.md    (generado, uno por modelo)
        ├── vistas-y-acciones.md       (generado)
        ├── seguridad.md               (generado)
        ├── crons.md                   (generado)
        ├── parametros.md              (generado)
        ├── medicion.md
        ├── mi-procedimiento.md
        ├── carga-del-mapa.md
        ├── extender.md
        ├── upgrade-y-migraciones.md
        ├── integraciones.md
        └── pruebas.md
addons/quimibond_sgi/README.md         (≤150 líneas)
addons/quimibond_sgi/CHANGELOG.md
addons/quimibond_sgi_{pesaje,plm,revisado}/README.md
```

Quién mantiene cada archivo, su formato y cómo se genera: `estructura_propuesta.csv`. En corto: **lo que se genera lo mantiene el script** y el programador solo lo regenera (el CI avisa si no lo hizo). **Los manuales para personas los mantiene el Jefe MAST y SGI**, con el programador como redactor inicial. El CHANGELOG lo mantiene el programador en cada PR.

### 6.1 `docs/sgi/README.md` y `glosario.md`

- **README:** qué es el SGI en Odoo (5 líneas) · tabla «¿quién eres?» → manual · dónde está cada cosa (usuarios / administración / transición / técnica) · versión documentada y fecha · cómo reportar un error en la documentación.
- **Glosario:** proceso, etapa, actividad, numeral · roles (ejecuta, aprueba, participa, informa, escala) por puesto, familia o rol relativo · entregable, entrada, liga, «eslabón atorado» · vencimiento (mensual en día hábil, semanal, por mes y día, por evento) y días inhábiles · medición automática contra manual, semáforo · Mis pendientes, Mi procedimiento, acuse, publicación · documento controlado, revisión, **clave nueva contra clave anterior** · NC, acción, eficacia, fuente de NC automática · «Cumple con» (cláusulas). Cada término con su nombre en pantalla y, en una columna aparte, su nombre técnico.

### 6.2 Manual de administración — `administracion/manual-jefe-mast.md`

1. Tu papel y el del Administrador SGI (qué hace cada grupo; Dirección consulta y aprueba sin permisos de MAST).
2. Arranque de una base vacía: instalar `quimibond_sgi`, cargar el mapa con `quimibond_sgi_mapa` **en modo de prueba primero**, revisar el reporte y cargar de verdad.
3. Personas y grupos: quién decide usuarios (Jose), quién los crea (Sistemas), a qué grupo va cada perfil, Salud ocupacional, Auditor; «en planta, el supervisor ejecuta».
4. Procesos y actividades: alta, cambio de ejecutor, archivar (nunca borrar), numerales congelados, vencimientos, entradas con plazo, aprobador distinto del solicitante.
5. Propuestas de cambio: cómo llegan (dueño del proceso más MAST), cómo se aprueban y qué cambia al aprobar.
6. Documentos: tipos y patrón `PR-{proceso}`, validación de clave, revisión, publicación, obsoletos, documentos externos, lista maestra, carpeta del SGI.
7. Publicar Mi procedimiento: cuándo, firma en Sign, acuses, alta de una persona en un puesto ya publicado.
8. Indicadores y mediciones: validación de las automáticas por el dueño en 3 días hábiles; paso a «Manual» con justificación; `nc_on_red`; oficial contra prueba.
9. NC y acciones: candados, cierre forzado, cancelación con motivo, fuentes automáticas (apagar sin perder control).
10. Calendario: días inhábiles (LFT art. 74 y contrato colectivo), qué se adelanta.
11. Crons: qué corre, a qué hora (México), cómo ver si falló (tabla incluida de `tecnica/crons.md`).
12. Parámetros (tabla incluida de `tecnica/parametros.md`).
13. «Del Dropbox a Odoo»: cómo se edita (solo MAST), estados de migración, cuándo un procedimiento pasa a «Baja tramitada».
14. Diagnóstico: cómo leer Cobertura de medición, Cumplimiento semanal y Faltantes de especificación.
15. Qué **no** hacer: Studio, borrar registros, editar lo que trae el módulo.
16. A quién pedir qué (desarrollo contra configuración).

### 6.3 Manuales por rol (1 a 3 páginas cada uno, con capturas)

Estructura común, para que se lean igual:

1. **Qué ves al entrar** (captura del menú de tu rol).
2. **Tu día**: 3 a 5 tareas con pasos numerados y una captura cada una.
3. **Lo que te llega solo** (avisos, escalamientos, vencimientos).
4. **Lo que no puedes hacer y a quién pedirlo.**
5. **Dónde quedó lo que usabas en el Dropbox**: tabla «Antes (clave y nombre del Dropbox) → Ahora (menú en Odoo)», **solo para los procedimientos y formatos de ese rol**, generada de `transicion/equivalencias.csv`. Es el único lugar del manual donde aparece la clave vieja, según la decisión 1.
6. **Preguntas frecuentes** (3 a 5).

Tareas por rol:

- **Usuario SGI:** revisar Mis pendientes; hacer una actividad y marcarla; leer y firmar Mi procedimiento; capturar una medición; levantar una NC o una queja; proponer un cambio a tu actividad.
- **Jefe de área / dueño de proceso** (incluye Captura de eficiencias): Mi equipo y ver el procedimiento de otro; atender escalamientos; validar mediciones de tus indicadores (3 días hábiles); aprobar propuestas de tu proceso; capturar eficiencias del área; riesgos de tu proceso.
- **Dirección:** Tablero; revisión por la dirección; aprobar lo que te toca; Política, Objetivos, Riesgos y Requisitos legales; qué **no** edita (sin Jefe MAST).
- **Auditor:** programa y auditorías; checklist generado del proceso; registrar hallazgos; dónde está la evidencia (documentos, lista maestra, indicadores, acuses); qué no ve (salud y salarios); independencia.
- **Capturista de planta** (sin grupo SGI o supervisor que ejecuta): hoja de checklist de hoy (lista a las 05:30), captura con PIN; qué hacer si algo sale fuera; a quién avisar. Versión de una hoja para imprimir junto a la máquina.

**Pendiente de verificación visual:** todas las capturas. Hacen falta staging, un usuario de prueba por rol y el guion de capturas. Con los recorridos de `AUDITORIA_FUNCIONAL` como plantilla, son unas 6 capturas por rol.

### 6.4 Guía de transición — `transicion/del-dropbox-a-odoo.md`

1. Qué cambió y por qué (media página): de procedimientos PDF a actividades con responsable, vencimiento y medición.
2. La regla de convivencia: cuándo se deja de llenar el formato viejo (un mes de doble captura como máximo, según el manual de usuario §16) y «un dato, un lugar».
3. Cómo usar «Del Dropbox a Odoo»: buscador por clave anterior, procedimientos anteriores, formatos con destino, rutina por rutina, tablero de avance.
4. Estado de los 52 procedimientos: 23 sustituidos, 21 con pendientes, 5 que se quedan como control operacional y P-I01 aparte. Qué significa cada estado de migración.
5. Qué pasa con los registros viejos (respaldo del Dropbox de solo lectura).
6. Para el auditor externo: cómo demostrar la trazabilidad antes → ahora (equivalencias y acuses).
7. Anexo: `equivalencias.csv` (generado).

El contenido de datos lo arma el agente L.

### 6.5 Técnica — secciones de cada documento

- **`arquitectura.md`**:
  1. Principios (de K-005, actualizados).
  2. Módulos: núcleo, `quimibond_sgi_mapa` y satélites (pesaje, plm, revisado, automotriz, ventas, contabilidad), con diagrama de `depends` generado.
  3. Estructura contra datos (decisión 4): qué trae el módulo y qué se captura en producción.
  4. Dominios: catálogo de procesos, ejecución y medición, documentos, mejora, SST, dirección.
  5. El ciclo de una actividad (vencimiento → pendiente → medición → escalamiento → cierre).
  6. El ciclo de una NC.
  7. Seguridad en una página (liga a `seguridad.md`).
  8. Multiempresa: decisión «SGI = empresa 1» y su alcance (C-012).
  9. Decisiones de arquitectura con fecha (liga a `decisiones.md`).
- **`modelo-de-datos.md`** (generado): diagrama Mermaid por dominio con las relaciones entre `sgi.*` y los modelos estándar extendidos, y una tabla de modelos (nombre, descripción, docstring, n.º de campos, archivo).
- **`diccionario/<modelo>.md`** (generado): encabezado con `_name`, `_description`, docstring, herencias y archivo. Tabla de campos: nombre, tipo, etiqueta, `help`, requerido, relación, `ondelete`, compute/store, `groups`, archivo:línea. Después restricciones, `_order` y métodos públicos con la primera línea de su docstring.
- **`vistas-y-acciones.md`** (generado): árbol de menús con grupos, sacado del XML y resuelto por padre. Una tabla de acciones: título, modelo, vistas, contexto, `help` sí/no. Vistas por modelo, marcando cuáles heredan de otro módulo.
- **`seguridad.md`** (generado): grupos con `implied_ids` resueltos; matriz modelo × grupo con CRUD **efectivo** (el mismo cálculo que `06-seguridad/permisos_por_rol.csv`); `ir.rule` con dominio y grupos; campos con `groups`; métodos con `sudo()` y su justificación (de `06-seguridad/sudo_clasificacion.csv`, hasta que haya comentario en el código).
- **`crons.md`** (generado): nombre, modelo.método, intervalo, hora de siguiente corrida (hora de México), qué hace (primera línea del docstring), idempotencia y actividades que agenda. Advertencia fija: los crons están en `noupdate` en la base (A-007); cambiarlos requiere migración.
- **`parametros.md`** (generado de `sgi.config._SGI_DEFAULT_PARAMS`): clave, default, comentario, quién lo cambia.
- **`medicion.md`**: medición de actividades (entregable con modelo y dominio, `complete_domain`, semáforo); indicadores (`calc_mode`, tabla generada de `_SOURCE_INFO`, fórmula configurable, términos); validación por el dueño; «Manual con justificación»; NC por rojo y por persistencia; periodos y fechas (fin inclusivo, zona horaria de OdooBot).
- **`mi-procedimiento.md`**: de dónde sale el contenido (roles por puesto, familia o rol relativo); filtros de lo archivado (decisión 3); PDF; publicación y firma en Sign; acuses; alta de persona en un puesto publicado; «Ver como».
- **`carga-del-mapa.md`**: `quimibond_sgi_mapa`; esquema JSON de `sgi.process.load_payload` (tomado del README `:596-723`); `dry_run`; idempotencia; qué hace con lo que ya no viene (archiva); exportador inverso (H-022); errores típicos.
- **`extender.md`**: agregar un proceso, una actividad o un entregable (es **dato**: por la pantalla o por carga, nunca por XML del núcleo, según la decisión 4). Agregar un modo de cálculo, una fuente de NC (de manual técnico `:182-206`), un reporte con pie de formato, un cron (con `_sgi_step`, savepoint y zona horaria), una señal para `quimibond_intelligence`. Reglas de build de CLAUDE.md que aplican al SGI (vistas propias, orden del manifest, tests registrados).
- **`upgrade-y-migraciones.md`**: qué sobrevive a un `odoo-update` (`noupdate`, `<function>` que corren siempre, A-006); cuándo hace falta migración; convención de nombres; etiqueta de git antes de retirar migraciones (decisión 4 de la tanda 1); verificaciones posteriores (árbol de menú B-004, crons, conteo de procesos y actividades, «Del Dropbox a Odoo» visible).
- **`integraciones.md`**: satélites y sus ganchos; módulos que salen del SGI y qué modelos se llevan; consumidores externos (K-020, lista generada); Sign, Knowledge, Aprobaciones y Helpdesk (qué equipos son del SGI).
- **`pruebas.md`**: cómo correr las pruebas en Odoo.sh (el CI no instala el SGI); checkers locales; lista generada de `tests/test_*.py` con la primera línea de su docstring.

### 6.6 Cómo se genera lo automatizable

`tools/sgi_docs.py`, en un PR propio, se construye sobre `docs/audit/scripts/inventario_codigo.py`: usa AST y la librería estándar, sin importar Odoo, así que corre en CI igual que `check_odoo_views.py`.

```bash
python3 tools/sgi_docs.py                  # regenera docs/sgi/tecnica/{modelo-de-datos,diccionario/*,vistas-y-acciones,seguridad,crons,parametros}.md
python3 tools/sgi_docs.py --check          # CI: falla si lo generado difiere de lo comprometido; advierte campos nuevos sin help
python3 tools/sgi_docs.py --prod crons.csv # opcional: agrega a crons.md la columna de producción (activo, siguiente corrida) desde un CSV exportado
```

| Salida | Fuente en el código |
|---|---|
| diccionario, modelo de datos | `models/*.py` (clases, `fields.*`, docstrings) |
| seguridad | `security/sgi_security.xml` (grupos, `implied_ids`, `ir.rule`) + `security/ir.model.access.csv` |
| crons | `data/*.xml` `<record model="ir.cron">` + docstring del método |
| parámetros | `sgi.config._SGI_DEFAULT_PARAMS` (`models/sgi_format_map.py`) |
| vistas, acciones, menús | `views/*.xml`, `report/*.xml` |
| integraciones | grep de `'sgi.` en `addons/*` fuera del SGI |
| equivalencias (transición) | producción: export de `documents.document` (clave anterior, título, estado y destino de migración) + el xlsx rutina por rutina |

## 7. CHANGELOG: sí se puede reconstruir (cómo)

**Fuentes, de la más confiable a la menos:**

1. `git log` del manifest: cada bump tiene fecha, asunto «quimibond_sgi X.Y.Z: …» y cuerpo. El clon es superficial: `git fetch --unshallow` (en lectura, en otra copia) trae lo anterior a 2026-09-04, si el remoto lo tiene.
2. Secciones del README por versión: 84 versiones citadas.
3. Docstrings de las 41 carpetas `migrations/<versión>/`.
4. Manual técnico §12 para 1.0.0–4.1.0.

**Comandos** (probados en lectura sobre esta rama):

```bash
# 1. Versiones y fecha desde git (117 versiones, de 28.0.0 a 56.24.1)
git log --reverse -p --format='@@ %h %ad' --date=short -- addons/quimibond_sgi/__manifest__.py \
 | awk '/^@@ /{c=$2" "$3; next} /^\+    .version.:/{gsub(/[^0-9.]/,"",$0); print $0, c}'

# 2. Cuerpo del commit de cada versión (fuente del texto de la entrada)
git log -1 --format='%B' <hash>

# 3. Versiones que tiene git y no tiene el README (hoy 41; entre ellas 56.24.1)
git log -p --format='%h' -- addons/quimibond_sgi/__manifest__.py | grep -E "^\+    'version'" \
 | sed "s/.*'19\.0\.//;s/',//" | sort -u > /tmp/gitv
grep -oE "19\.0\.[0-9]+\.[0-9]+\.[0-9]+|\b[0-9]{2}\.[0-9]+\.[0-9]+\b" addons/quimibond_sgi/README.md \
 | sed 's/^19\.0\.//' | sort -u > /tmp/readv
comm -23 /tmp/gitv /tmp/readv | sort -V

# 4. Qué versiones traen migración (marca «requiere odoo-update / migración»)
ls addons/quimibond_sgi/migrations | sort -V
```

**Formato de cada entrada** (una por versión, de la más nueva a la más vieja):

```
## 19.0.56.24.1 — 2026-09-29
**Corregido:** la extensión de sgi.my.pending se declara TransientModel (la instalación de 56.24.0 no cargaba el registro).
**Migración:** la de 56.24.0 (respaldo y borrado de roles de actividades archivadas) corre al instalar 56.24.1.
Commit 4415976.
```

Secciones posibles: Agregado / Cambiado / Corregido / Retirado / Seguridad / Migración / Datos de producción. Para 1.0–27.x, que no están en el log superficial, basta una entrada por fase o versión mayor, sacada del README y del manual técnico §12.

**Regla desde ahí:** el PR que sube la versión agrega su entrada. Un chequeo en `tools/check_addons.py` («si cambia `version` en el manifest del SGI, cambia `CHANGELOG.md`») lo vuelve automático. **Orden:** K-018 va antes que A-026 (retirar migraciones con una etiqueta de git).

## 8. Cómo se midió (reproducible)

1. Docstrings: script AST sobre `addons/quimibond_sgi/models/*.py`, que cuenta clases con `_name` distinto de `_inherit`, `_description` y docstring de clase, y métodos por prefijo (`_compute`, `action_`, `cron`) con docstring. Resultado: 102 modelos propios con `_description`, 21 con docstring; 254 `_compute` con 12; 303 `action_` con 106; 33 `cron` con 19; 63 de 88 archivos con docstring de módulo.
2. Comentarios con historia: regex `\b\d{2}\.\d+\.\d+\b|19\.0\.\d+|fase\s*\d|PR\s*\d|\bola\b` sobre los comentarios de `views/`, `data/`, `report/` y `security/` (398 comentarios, 65 con historia) y sobre las líneas `#` de `models/` (1,371, con 69 con historia).
3. Versiones: comandos de §7.
4. Checker: `python3 tools/check_addons.py` → «0 error(es), 0 advertencia(s)».
5. Producción (MCP, lectura): carpetas raíz de Documentos (`documents.document`, `type=folder`, `folder_id=false`: 36), hijas de 1119, 1805 y 3078, y conteo de documentos en 1805, 3078, 1120 y 3512.
6. Menciones de las decisiones: `grep -ci` de «Del Dropbox», «sgi_previous_code», «PR-{», «Salud ocupacional», «instal.*vac», «quimibond_sgi_mapa», «Administrador SGI», «Captura de eficiencias» y «Auditor SGI» en el README y en `docs/`.

## 9. Preguntas que requieren decisión de negocio (para Jose)

1. **¿Dónde viven los manuales para el personal?** Opciones: solo en el repo (Markdown), en Odoo como artículos de Knowledge, o como **documentos controlados del SGI** (son información documentada, ISO 7.5, y el tipo `MA-{proceso}-{seq}` ya existe). *Recomendación:* la fuente en el repo, y lo que lee el personal publicado como documento controlado en Documentos, con revisión y acuse, y ligado desde Inicio. Así el auditor externo lo ve como parte del SGI y nadie mantiene dos copias.
2. **¿Quién mantiene los manuales por rol después de escribirlos?** *Recomendación:* el Jefe MAST y SGI es el dueño (cambian cuando cambian los menús o el proceso) y el programador los redacta la primera vez y cuando cambie una pantalla.
3. **¿Manual aparte para el grupo nuevo «Salud ocupacional»?** *Recomendación:* sí, de una página, porque ve datos que nadie más ve (exámenes, estudios de higiene) y su responsabilidad es distinta.
4. **¿Cuál es la carpeta del SGI en Documentos?** Hay tres raíz: 1119 «SGI» (la real, con 00–24), 1805 «SGI» y 3078 «SGI 2026», estas dos vacías. *Recomendación:* 1119. Archivar 1805, 3078 y «0. ANEXOS (OBSOLETO - duplicado)» (1120) sin borrar, y resolver los 8 de «POR CLASIFICAR».
5. **¿El SGI ya está certificado en 45001?** El README y el manual técnico dicen «45001 en preparación»; el brief dice «SGI integrado 9001/14001/45001». *Recomendación:* que los documentos nuevos digan el estado real y la fecha de certificación de cada norma.
6. **¿Tú o usted en pantallas y manuales?** Las pantallas nuevas (56.7.0) y el manual viejo usan «tú»; la auditoría funcional señaló textos en «usted» como defecto. *Recomendación:* «tú» en todo.
7. **¿Se hace el `git fetch --unshallow` para que el CHANGELOG cubra de 1.0 a 27.x con commits reales?** *Recomendación:* sí, en una copia de lectura; si el remoto no tiene esa historia, basta una entrada por fase.

## 10. Cobertura

| Tipo | Revisados | Total | Nota |
|---|---:|---:|---|
| Documentos del encargo (README, 8 `docs/SGI_*`, auditoría funcional, invariantes, runbook, CLAUDE.md) | 13 | 13 | Leídos completos, salvo el README (estructura completa por encabezados y lectura de las secciones de introducción, instalación, tests, multiempresa, tipos de documento y 56.20–56.24) y el HTML (encabezados, versión, grupos y crons) |
| `description` de manifests (núcleo + 3 satélites) | 4 | 4 | |
| READMEs de satélites | 0 | 0 | No existen (K-015) |
| Docstrings de modelos propios | 102 | 102 | Por script (§8) |
| Métodos (docstring por prefijo) | 1,451 | 1,451 | Por script. Incluye `wizard/` y `report/` si existen; el conteo del inventario (1,413) es solo de `quimibond_sgi` sin duplicados de herencia |
| Campos sin `help` | 1,284 | 1,284 | Tomado de C-018 (`campos_sin_help.csv`); aquí solo se definieron la regla y el orden |
| Acciones (`help`) | 86 | 86 | Tomado de `04-vistas/acciones_titulo_ayuda.csv` (D-015) |
| Comentarios XML / `#` en `models/` | 398 / 1,371 | 398 / 1,371 | Por regex; no se leyeron uno por uno |
| Afirmaciones verificadas | 53 | ≥30 pedidas | `afirmaciones_verificadas.csv` |
| Otros `docs/*.md` del repo (costeo, nómina, hallazgos, superpowers) | 11 | 11 | Revisados solo por mención del SGI; no son del SGI. `superpowers/…situacion…` cita señales del SGI y queda cubierto por K-020 |

**No verificado:** si existen artículos de Knowledge o documentos en Odoo que ya sirvan de manual (no los busqué por MCP). Las capturas de pantalla quedan **pendientes de verificación visual** porque no hay staging.
