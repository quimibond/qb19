# 99 — Consolidado de la auditoría del SGI (fase 2)

**Fecha:** 2026-09-29 · **Módulo:** `quimibond_sgi` 19.0.56.24.1 en producción; entrega 1 en código en quimibond/qb19#452 (`claude/sgi-entrega-1`, 19.0.56.25.0, sin desplegar) · **Modo:** solo lectura (repo + MCP de producción con `search_records`).
**Manda:** `decisiones.md`, con todas sus secciones del 2026-09-29, incluidas «P-I01 y respuestas al agente L» y «Salud ocupacional y flujo de ramas».
**Archivos:** `99-consolidado/hallazgos.csv` trae los 277 hallazgos tal como salieron. `99-consolidado/hallazgos_consolidados.csv` trae 228 renglones únicos. `99-consolidado/universo_clasificado.csv` trae los 5,113 elementos del inventario. `99-consolidado/universo_solo_por_defecto_sensibles.csv` trae los 78 modelos que quedaron «Se queda» solo por defecto.
**Cómo se reproduce:** `python3 docs/audit/scripts/consolidar.py`, luego `python3 docs/audit/scripts/consolidar.py consolidar` y luego `python3 docs/audit/scripts/clasificar_universo.py`. Las agrupaciones, los estados y el PR de cada hallazgo están en tablas dentro de `consolidar.py` (`PRS`, `GRUPOS`, `ESTADO`), así que se pueden revisar ahí.

## 1. Resumen

- **277 hallazgos de 12 agentes y 3 pedidos nuevos de Jose (N-001 a N-003): 280 IDs en 228 renglones únicos.** 52 IDs se fundieron en 41 grupos de duplicados (§4). El script comprueba que cada ID aparezca exactamente una vez: 280 de 280.
- **Severidad de los 228:** Crítica 2 · Alta 45 · Media 109 · Baja 72. Las dos críticas ya no están abiertas:
  - L-001 (P-I01): Jose cerró el acceso. Lo verifiqué: `documents.document` 3560 tiene `access_internal = none` y `access_via_link = none`.
  - F-001 (MCP): lo aplica Jose en Ajustes.
- **Estado de los 228:** Abierto 191 · Decisión pendiente 18 · Acción de Jose 11 · En PR 452 7 · Resuelto en producción 1.
- **Hecho:**
  - PR 452 cierra A-001, B-004, B-001, F-003, F-004 (con D-022), I-013 y J-002.
  - H-003 está resuelto en 13 de 15: lo verifiqué por MCP. Faltan E2.21 y S4.30.
  - L-001 tiene el acceso cerrado. Falta que Sistemas rote las credenciales.
- **Esfuerzo pendiente:** unas **630 h** en código y datos, más 4 h de `qb_mcp_politica`. Las entregas más grandes son la 3 (modelo, 106 h), la 2 (limpieza, 104 h) y la 10 (documentación, 100 h). La entrega 7 no tiene trabajo: ningún hallazgo lo pide.
- **Decisiones pendientes:** 32, en §7. Las que más entregas frenan son la D-03 (¿una sola empresa?, 4 entregas) y las D-01, D-02, D-05 y D-08 (3 entregas cada una).
- **Inventario:** los 5,113 elementos quedaron clasificados, con 0 vacíos. Se quedan 2,694, se corrigen 1,732, se eliminan 213 y 474 salen a otro módulo (§8).

## 2. Conteos

### 2.1 Hallazgos por agente y severidad (los 277 originales)

| Agente | Crítica | Alta | Media | Baja | Total | Horas declaradas |
|---|---:|---:|---:|---:|---:|---:|
| A Arquitectura | 0 | 4 | 13 | 13 | 30 | 98.0 |
| B Obsoleto | 0 | 2 | 8 | 13 | 23 | 36.2 |
| C Modelo | 0 | 6 | 9 | 8 | 23 | 84.0 |
| D Vistas | 0 | 3 | 10 | 9 | 22 | 36.6 |
| E Menús | 0 | 4 | 6 | 7 | 17 | 23.4 |
| F Seguridad | 1 | 3 | 9 | 8 | 21 | 29.4 |
| G Lógica | 0 | 5 | 11 | 10 | 26 | 67.0 |
| H Datos | 0 | 5 | 12 | 5 | 22 | 85.0 |
| I Roles | 0 | 5 | 12 | 5 | 22 | 31.4 |
| J Calidad | 0 | 3 | 13 | 10 | 26 | 83.0 |
| K Documentación | 0 | 7 | 12 | 5 | 24 | 97.4 |
| L Transición | 1 | 7 | 9 | 4 | 21 | 56.1 |
| **Total** | **2** | **54** | **124** | **97** | **277** | **727.5** |

Las 727.5 h son la suma de lo que declaró cada agente. Tienen doble conteo en los duplicados. Las cifras consolidadas de abajo toman, en cada grupo, el esfuerzo mayor.

### 2.2 Renglones consolidados por entrega

| Entrega | Crítica | Alta | Media | Baja | Renglones | Esfuerzo (h) | Pendiente (h) |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 Seguridad crítica y datos que rompen algo | 0 | 7 | 11 | 1 | 19 | 40.0 | 23.0 (17 ya en PR 452) |
| 1b Mis pendientes | 0 | 0 | 3 | 0 | 3 | 5.0 | 5.0 |
| 2 Limpieza y módulos que salen | 0 | 3 | 16 | 17 | 36 | 104.2 | 104.2 |
| 3 Modelo de datos y `quimibond_sgi_mapa` | 0 | 6 | 14 | 3 | 23 | 106.5 | 106.5 |
| 4 Menús y acciones | 0 | 1 | 7 | 5 | 13 | 17.6 | 17.6 |
| 5 Vistas y formularios | 0 | 2 | 6 | 6 | 14 | 38.3 | 38.3 |
| 6 Del Dropbox a Odoo | 0 | 5 | 7 | 2 | 14 | 55.0 | 55.0 |
| 7 Matriz de cumplimiento | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 Lógica y rendimiento | 0 | 9 | 14 | 8 | 31 | 84.5 | 84.5 |
| 9 Pruebas | 0 | 2 | 10 | 10 | 22 | 71.5 | 71.5 |
| 10 Documentación y manuales | 0 | 6 | 12 | 15 | 33 | 99.9 | 99.9 |
| Prod (acciones en producción) | 2 | 4 | 9 | 5 | 20 | 45.4 | 24.8 |
| **Total** | **2** | **45** | **109** | **72** | **228** | **667.9** | **630.3** |

Sobre la entrega 7: «Cumple con» ya está construido y cargado (decisión 6 del brief), y ningún hallazgo pide ni pruebas ni un reporte de la matriz. J-015 (una prueba de humo que renderiza todos los reportes) cubre también `report_compliance_matrix.xml` y va en la entrega 9.

## 3. Contradicciones resueltas

| # | Qué dicen | Gana | Por qué (evidencia) |
|---|---|---|---|
| 1 | C-001 propone copiar la M2M del proceso a la M2O del documento. L-003 dice que no. | **L-003** | Decisión 3 (tanda 1): «No se rellena nada desde el lado del proceso». Jose aprobó L-003 (decisiones «Respuestas a L»). La migración solo respalda y registra conflictos. |
| 2 | C-004 habla de 426 documentos. L-004 habla de 490 (suma los 64 «Formulario de Odoo»). | **L-004** | Jose aprobó la migración sobre 490 (decisiones «Respuestas a L»). Los 64 son formatos del Dropbox ya migrados (E-004). |
| 3 | A-017: «`sgi_snapshot` no se llama». G-018: sí se llama. | **G-018** | `sgi_cron.py:380-382` lo llama desde el cron mensual dentro de `_sgi_step`. Aun así hay 0 fotos, así que el valor de inventario se queda como candidato a retirarse (decisión 5: «si nada los alimenta, proponer quitarlos»). Hay que revisar el log el 5-oct-2026. |
| 4 | E-003: las 2 aprobaciones de Sandra salen en Mis pendientes. I-004: no salen. | **I-004** | `sgi_archived_filters.py:98` (`if not rule.active … return False`) ya las filtra desde 56.24.0. Las que se ven son las de la regla 35 para Guadalupe (13). Jose las resuelve por fuera (aclaración de la tanda 3). En la entrega 1b queda el resto de E-003: filtrar procesos archivados en acción, NC, documento y legal (G-014). |
| 5 | G-013: Mi procedimiento muestra lo archivado. I-022: esas listas no se ven en ninguna vista. | **I-022** | Es código muerto. Se quita en la entrega 2 y la severidad baja a Baja. |
| 6 | G-001: la medición «capturada» la valida MAST. I-006: la valida el dueño del indicador. | **I-006** | Decisión 3 de la tanda 2: «las valida el dueño del indicador, en 3 días hábiles». |
| 7 | A-009 cuenta 15 acciones redefinidas (12 en `sgi_diagram_views.xml`). B-015 y el inventario cuentan 17. | **B-015** | Un conteo de `<record model="ir.actions.act_window">` repetidos da 17: 13 en `views/sgi_diagram_views.xml`, 2 en `sgi_hierarchy_views.xml`/`sgi_process_procedure_views.xml` y 2 en `sgi_sales_budget_views.xml`. |
| 8 | `00-inventario.md` y K-018 hablan de «41 carpetas de migración». A-026 y B-003 dicen 39 carpetas y 41 scripts. | **A-026 / B-003** | `ls addons/quimibond_sgi/migrations \| wc -l` da 39. `find migrations -name '*.py' \| wc -l` da 41. |
| 9 | B-012 (y `metodos_103.csv`) borra `sgi_next_code`. C-004 §2.2 lo usa para generar la clave nueva. | **C-004** | Está definido en `models/sgi_catalog.py:685`. La decisión 7 de la tanda 1 exige la clave nueva `PR-{proceso}`. En `universo_clasificado.csv` queda como «Se queda». |
| 10 | C-003 propone «sin sustitución → pendiente». La decisión C-003 dice que los 21 con pendientes van a «En curso». | **Decisión** | L-005 lo implementa: 44 a «En curso» y 5 a «No aplica». C-003 se funde en L-005. |
| 11 | F-004 (b) propone un grupo «Nómina / eficiencias». La decisión 2 de la tanda 1 dice depurar a los 20 «Encargados de empleados». | **Decisión** | La depuración es una acción de producción (§6). No hace falta un grupo nuevo. |
| 12 | G-006 manda los avisos de calibración a MAST. La decisión 2 de la tanda 2 los manda al Coordinador de Laboratorio y al Jefe de Calidad. | **Decisión** | Va así en el PR `e8-calibracion`. |
| 13 | La decisión 14 de la tanda 2 dice «OdooBot con zona America/Mexico_City». La aclaración de la tanda 3 dice que no se le cambia. | **Aclaración de la tanda 3** | Es posterior. G-007 se resuelve en código con `sgi_today()`, que usa la zona del calendario del SGI. |
| 14 | A-006 deja `_sgi_attach_quality_menus` como siembra legítima. E-008 propone quitarla y poner `parent` fijo. | **E-008** | Así no queda ninguna `<function>` que reacomode menús en cada update (el mismo principio de B-001). La secuencia 23/24 pasa al XML. |
| 15 | D-007 y la pregunta 2 de D proponen renombrar «Acciones correctivas» a «Acciones». | **Decisión 2 del brief** | El árbol decidido dice «Acciones correctivas». Solo queda abierto qué acciones muestra (D-17 en §7). |
| 16 | H-001 y H-002 proponen «dar usuario» o «crear el usuario de 564». | **Decisión 1 de la tanda 2** | Jose decide los usuarios y Sistemas los crea; el programador no crea ninguno. Pasan a «Acción de Jose». |
| 17 | La decisión «Salud ocupacional» dice que resuelve **I-014**, pero describe los avisos de exámenes a 88. | **Se aplica a I-013 (b)** | En `09-roles.md`, I-013 (b) es «avisos a `rh_user_id` = 88 sin acceso» e I-014 es «botón Firmar visible sin permiso de Sign». Doy I-013 por cerrado. I-014 sigue abierto en `e1-candados-rpc`. **Jose confirma** (D-32). |
| 18 | E-003 y la decisión 12 de la tanda 2 dicen «se cancelan las 2 de Sandra». La aclaración de la tanda 3 dice que las resuelve Jose por fuera y que son de la empresa 4. | **Aclaración de la tanda 3** | El programador no las toca. Por eso nace N-002, el filtro por empresa. |

## 4. Duplicados fundidos (52 IDs en 41 grupos)

| Principal | Absorbe | Motivo |
|---|---|---|
| F-004 | D-022 | La pestaña y el menú de salud cambian de grupo en el mismo PR (hecho en PR 452) |
| E-011 | F-016 | Reportes y acciones de servidor sin `group_ids`: mismo cambio |
| I-004 | E-003, G-014 | Filtros de archivado en Mis pendientes (I-004 corrige a E-003) |
| F-001 | F-002 | Misma configuración del MCP |
| A-026 | B-003, A-027 | Retirar migraciones |
| A-002 | B-021 | Procesos MP-*/P-* en el XML |
| A-003 | B-018 | Scripts `tools/*.py` de un solo uso |
| I-022 | G-013 | Listas muertas de Mi procedimiento (I-022 corrige a G-013) |
| A-023 | B-019 | Nombres por fase o PR |
| B-009 | D-011, C-017 | Encuesta y campos legado |
| B-022 | H-020 | Categorías de aprobación y familia sin uso |
| A-016 | A-013, E-014 | `account_budget` y los menús se van con el presupuesto |
| A-017 | G-018 | Valor de inventario (G-018 corrige a A-017) |
| C-001 | C-014, D-017, L-003 | Fuente única documento ↔ proceso: `restrict`, vista y migración |
| L-005 | C-003 | Estados de migración de los procedimientos |
| C-004 | L-004 | Migración de la clave anterior (490) |
| C-013 | C-022 | `ondelete`/`required` en `process_id` |
| C-012 | H-021 | Multiempresa (H-021: nada que corregir en datos) |
| E-007 | D-005 | Nombres de menú y migas de pan |
| B-015 | A-009 | Acciones redefinidas (conteo correcto en B-015) |
| E-009 | F-011 | Grupos finales de Dirección y Usuario |
| D-003 | D-020 | Textos con claves o códigos internos en pantallas |
| D-004 | E-006 | Nombres de reportes con clave del Dropbox |
| D-006 | D-010 | Equipos de reclamación del SGI (decisión 9) |
| D-013 | E-016 | Llaves de contexto que no hacen nada |
| D-015 | E-015 | Acciones sin `help` |
| E-001 | L-007, I-016, B-020 | Sección «Del Dropbox a Odoo»: menús, acciones y piezas que se reubican |
| L-010 | E-005 | Solo MAST edita los campos de transición (vista y servidor) |
| L-002 | L-015, L-021 | Modelo `sgi.legacy.routine`: campos de decisión y versión |
| L-018 | C-016 | `legacy_number` y rutinas |
| L-009 | L-019 | Tablero de avance |
| L-008 | L-016, L-020 | Importador de rutinas |
| G-001 | I-006 | «Validar medición» por el dueño (I-006 corrige a G-001) |
| G-004 | I-008, G-021 | Cierre automático de actividades de los crons |
| G-005 | G-025 | Deduplicación y actualización de actividades |
| G-002 | H-005 | Excepciones sin savepoint que dejan actividades sin semáforo |
| H-001 | I-010 | Actividades sin nadie con usuario (C4.25) |
| J-003 | J-001 | CI del SGI |
| K-003 | B-023 | README del módulo |
| A-028 | K-014 | Descripción del manifest |
| C-018 | K-019 | `help` de campos y ayuda contextual |

Son 41 grupos y 52 IDs absorbidos. Cuenta: 280 IDs (277 de los agentes + N-001 a N-003) − 52 absorbidos = 228 renglones. `consolidar.py consolidar` lo comprueba: cada ID aparece una sola vez.

**Regla de severidad del grupo:** gana la más alta, salvo I-022/G-013, donde G-013 (Alta) partía de una premisa falsa (contradicción 5). **Regla de esfuerzo:** gana el mayor del grupo, no la suma, porque el trabajo es el mismo.

## 5. Plan por entregas

### 5.1 Flujo de ramas (decisiones, «Salud ocupacional y flujo de ramas», corregido por Jose el 2026-09-29)

1. Cada PR sale de su propia rama de desarrollo y va hacia **`main`**.
2. La instalación limpia y las pruebas del SGI corren en el **build de desarrollo de esa rama** en Odoo.sh. El resultado va en el PR.
3. El paso a producción es el PR de **`main` a `quimibond`**, siguiendo `docs/RUNBOOK_DESPLIEGUE.md` (`odoo-update`, reinicios y consultas de verificación). El upgrade se prueba sobre la copia de producción que usan los builds.
4. **Nada se integra ni pasa a `quimibond` sin el visto bueno de Jose.** quimibond/qb19#452 se queda con destino `main`.
5. Cada PR sube la versión del manifest (regla de `CLAUDE.md`) y agrega su entrada al `CHANGELOG.md` desde que exista (K-018). Los modelos nuevos sin cambio de versión son error del CI.

Las dos columnas de evidencia de las tablas siguientes dicen qué hay que adjuntar en cada paso. **«Rama (build de desarrollo)»:** la instalación limpia termina y las pruebas indicadas pasan, con el `update.log` o el log de pruebas adjunto. **«main/quimibond (upgrade sobre copia de producción)»:** el update pasa sin `ERROR` de `quimibond_sgi` y las consultas MCP de verificación dan lo esperado.

### 5.2 Entrega 1 — Seguridad crítica y datos que rompen algo

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| **#452** `claude/sgi-entrega-1` · 56.25.0 (**hecho, falta build y visto bueno**) | A-001, B-004, B-001, F-003, F-004 (+D-022), I-013, J-002 | `__manifest__.py`, `sgi_menus.xml`, `sgi_parameters.xml`, `sgi_indicators_data.xml`, `sgi_sign_record.py`, `sgi_security.xml`, `ir.model.access.csv`, `sgi_hse_views.xml`, `tools/check_odoo_views.py` | no | `test_security_entrega1`, `test_update_respects_mast`, `test_cleanup_45` (6 entradas) | Bajo: grupo nuevo, ACL cambiada y `-hr.group_hr_user` en el menú 2600 | 17 | — | **Después:** 88 al grupo Salud; depurar los 20 de `hr.group_hr_user`; verificar el menú 2600 sin el grupo 68 (paso 37 de `09-roles/verificacion_visual.csv`) | Instalación limpia sin «External ID not found»; las 3 pruebas pasan | Update sin error; `res.groups` «Salud ocupacional (SGI)» existe y está vacío; ACL de `sgi.health.record` sin el grupo 68; un indicador en manual sigue en manual |
| `sgi/e1-candados-rpc` · 56.26.0 | F-005, F-006, F-017, F-007, F-008, F-015, I-014 | `sgi_mp_change.py`, `sgi_supplier_nc.py`, `controllers/portal_nc.py`, `sgi_my_procedure_screen.py`, `sgi_cron.py`/`sgi_format_map.py` (candado de `sgi.config`), `sgi_kpi_fields.py`, botones «Firmar» | no | `with_user` + `assertRaises(AccessError)` para cada F (J-005); `HttpCase` del portal (J-014) | Bajo: solo renombres de métodos a privados y candados | 8.5 | PR 452 | — | Pruebas negativas pasan | Portal de NC: una respuesta de proveedor de prueba funciona (pendiente de verificación visual) |
| `sgi/e1-fichas-estandar` · 56.27.0 | D-001, E-011 (+F-016) | `sgi_res_partner_views.xml` y herencias de fichas estándar; `group_ids` en 7 reportes y 16 acciones de servidor | no | Usuario sin SGI abre un contacto (prueba de vista con `with_user`) | Medio: herencias de vistas de **otros** módulos; revisar `update.log` | 4.5 | F-004 (grupos) | — | Instalación limpia; prueba de contacto | El contador (132) abre un contacto (paso visual) |
| `sgi/e1-reglas-captura` · 56.28.0 | F-009, F-010 | `sgi_incident.py`, `ir.model.access.csv`, `sgi_security.xml` (reglas de dueño del patrón de 56.7.0) | no | Usuario SGI no edita el incidente de otro; el reportante sí | Medio: reglas nuevas en 14 modelos | 7 | **D-06, D-07** | — | Pruebas de reglas | Conteo de `ir.rule` = 32 + las nuevas |
| `sgi/e1b-mis-pendientes` · 56.29.0 (**entrega 1b**) | I-004 (+E-003, G-014), N-002, N-003 | `sgi_my_pending.py`, `sgi_archived_filters.py`, `studio.approval.rule.write` | no | Regla archivada → no sale; proceso archivado → no sale acción/NC/documento/legal; segunda empresa → no sale (J-007) | Bajo | 5 | D-03 (para N-002) | Jose resuelve PV15254/PV15323 (empresa 4) por fuera | Pruebas pasan | Mis pendientes de Guadalupe (13) y Sandra (87) sin renglones de la empresa 4 |
| `sgi/e1c-documentos-sin-propietario` · 56.30.0 | N-001 | `sgi_document.py` (`create`/`write`), Diagnóstico | no | Controlado sin propietario no puede quedar `view` para internos salvo MAST | Medio: toca Documentos, que usa toda la empresa | 3 | H-015 (llenar propietarios antes) | **Antes:** llenar `sgi_owner_id` (H-015) | Prueba `with_user` | 0 controlados vigentes sin propietario y abiertos (consulta MCP) |
| `sgi/qb-mcp-politica` (módulo nuevo `qb_mcp_politica` 19.0.1.0.0) | cierra en código F-001 (+F-002) | `addons/qb_mcp_politica/` (hereda `mcp.enabled.model`) | no | `tests/test_politica.py` (registrado en `tests/__init__.py`) | Bajo: módulo nuevo que no toca `mcp_server` | 4 | F-001 aplicado a mano; D-22 | **Antes:** Jose aplica F-001 en Ajustes (§6) | Instalación limpia; las pruebas pasan | `list_models` muestra `ir.cron` como `[read]` y los 9 desactivados no aparecen |

### 5.3 Entrega 2 — Limpieza de lo obsoleto y módulos que salen

**Orden obligatorio** (decisiones de la tanda 1 #4 y respuestas a K #1): (1) `e2-changelog`; (2) **etiqueta de git** `sgi-antes-de-limpieza-56.x` sobre `main`; (3) `e2-sin-migraciones`; (4) el resto. Cada PR que saca algo del SGI mueve o borra sus pruebas en el mismo cambio (J-018) y **no borra ni recrea menús** a los que apuntan actividades o documentos, solo los reubica conservando `res_id` (E-002).

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e2-changelog` | K-018 | `addons/quimibond_sgi/CHANGELOG.md`, chequeo en `tools/check_addons.py` | no | chequeo «cambia `version` ⇒ cambia CHANGELOG» | Nulo (solo documentación) | 6 | D-30 (unshallow) | — | CI verde | n/a |
| `sgi/e2-sin-migraciones` | A-026 (+B-003, A-027), B-005 | borrar `migrations/19.0.13.7.0…56.24.0`, `sgi_groups_cleanup.py`, `test_perm_groups.py` | no (se borran) | — | Bajo: ninguna corre (producción está en ≥ 56.25.0) | 1.5 | K-018 y la etiqueta de git | — | Instalación limpia | Update sin error; `ir.module.module` en la versión nueva |
| `sgi/e2-procesos-viejos` | A-002 (+B-021), A-003 (+B-018), B-002 | `__manifest__.py`, `data/sgi_process_data.xml`, `sgi_process_flows_extra.xml`, `sgi_format_map.py` (P-VEN), 4 pruebas, `tools/*.py` | **sí:** `pre-migrate` del anexo A de `01-arquitectura.md` (58 XML IDs a `__export__`); `post-migrate` que borra `sgi.process.responsibility` después de un CSV de respaldo de las 7 filas | pruebas con procesos propios (`XPM-A`/`XPM-B`) | Medio: si queda un `env.ref('quimibond_sgi.proc_*')` olvidado, falla (a propósito) | 17 | — | — | Instalación limpia con `sgi.process` vacío; `test_process_map*` y `test_flows_48` pasan | `ir.model.data` con `quimibond_sgi` + `sgi.process` = 0; `__export__ quimibond_sgi_legado_%` = 58; `sgi.process` activos 14 y archivados 25 |
| `sgi/e2-siembras-y-funciones` | A-006, E-008, A-007, A-005 | `data/sgi_parameters.xml` (se queda solo `seed_parameters`), `sgi_menus.xml` (`parent` fijo de Calidad), `sgi_my_procedure_data.xml` (`noupdate`) | no | `recompute_pending_measures` desde el cron diario | Bajo | 7 | **D-12** | — | Instalación limpia | Update sin «recompute» en el log; menús de Calidad en la secuencia 23/24 |
| `sgi/e2-codigo-muerto` | B-011, I-022 (+G-013), B-012 (menos `sgi_next_code`), B-013, B-014, B-016, D-012, B-006, A-023 (+B-019) | `sgi_my_procedure_screen.py`, 27 `action_*`, 8 helpers, 10 campos no guardados, `sgi_hierarchy_views.xml`, renombres de `data/*fase*` | `post-migrate` que borra las 15 filas de `ir_model_data` de `sgi.employer.obligation` (B-006) | ajustar las pruebas que solo ejercitaban lo borrado | Medio: si una herencia propia ya en producción ancla un campo borrado → `pre-migrate` que borre esa vista (regla de `CLAUDE.md`) | 10.7 | — | — | `check_odoo_views.py --base-ref` en verde | Update sin «no puede ser localizado en la vista padre» |
| `sgi/e2-legado` | B-009 (+D-011, C-017), B-010 | `sgi_audit_views.xml`, `sgi_audit.py`, `sgi_document.py:88-91`, `sgi_indicator.py` (`CALC_MODES`) | `post-migrate`: archivar la encuesta 151 (no borrar); `pre-migrate` defensivo de `calc_mode` | modos retirados: ningún indicador activo los usa | Medio | 7 | **D-13, D-16** | — | Instalación limpia | `sgi.indicator` agrupado por `calc_mode` sin los modos retirados |
| `sgi/e2-dependencias` | A-011, A-012, A-015, A-014, A-010 | `__manifest__.py`, `data/sgi_dyd_data.xml:14`; A-010 → satélite `quimibond_sgi_studio` si se decide | no | — | Bajo: en producción no se desinstala nada | 7.5 | **D-10**; A-014: confirmar que no hay artículos de Knowledge ligados a documentos o a Mi procedimiento (condición de la decisión 5) | — | Instalación limpia con menos dependencias | Update sin error |
| (según decisión) | B-007, B-017, B-022 (+H-020) | modelos `x_*` de Studio, `sgi_cron_weekly_digest`, categorías de aprobación | no | — | Bajo | 2.5 | **D-11, D-14, D-15** | B-007: `sgi_drop_empty_studio_models(dry_run=True)` en el shell | — | — |
| `sgi/e2-sale-presupuesto-ventas` → módulo `quimibond_ventas_presupuesto` | A-016 (+A-013, E-014) | `models/sgi_sales_budget*.py`, `views/sgi_sales_budget_views.xml`, `report_sales_budget.xml`, 2 crons, 7 parámetros, `test_sales_budget.py` (117 pruebas) | **sí:** cambiar `ir_model_data.module` de modelos, campos, vistas, accesos, menús y crons, **sin borrar datos** (decisión 5) | las 117 pruebas se mueven | **Alto:** mudanza entre módulos sin base de pruebas aparte; E-002: los menús 2434/2435 conservan `res_id` | 16 | E-002, J-018 | — | Instalación limpia de los dos módulos; las 117 pruebas pasan en el módulo nuevo | `sgi.sales.budget` = 4 registros igual que antes; menús 2434/2435 con el mismo id |
| `sgi/e2-sale-contabilidad` | A-017 (+G-018) | `sgi_kpi_fields.py:182-270` (bitácora + `res.company.write`) → módulo contable; valor de inventario → retirar si el log del 5-oct confirma 0 fotos | mudanza de `ir_model_data` (bitácora) | `test_kpi_fields` se mueve | Medio | 4 | Revisar el log del cron el 5-oct-2026 | — | Instalación limpia | Menús 2587/2588 en Contabilidad → Reportes con el mismo id |
| `sgi/e2-sale-automotriz` → `quimibond_sgi_automotriz` | A-018 | `sgi.ppap*`, `sgi.fmea*`, `sgi.msa.study`, `sgi_ppap_elements.xml`; `quimibond_sgi_plm` pasa a depender de él | mudanza de `ir_model_data` | `test_ppap`, `test_fmea`, `TestMsa` se mueven | **Alto** | 12 | **Antes:** listar entregables y actividades que apuntan a `sgi.ppap`/`sgi.fmea`/`sgi.msa.study` (PPAP se usa para CLI-01, decisión 5; H-004 cita `sgi.ppap` ×2 y `sgi.fmea`) y las 2 actividades con menú PPAP/AMEF (E-002) | — | Instalación limpia de núcleo + satélite + plm | Entregables C1-PPAP (380) y los que citan PPAP siguen con `odoo_model_id` válido |
| `sgi/e2-ganchos-satelites` | A-019, A-020 | `_calc_calidad_pq` → `quimibond_sgi_revisado`; `pesaje_tolerance_kg` → `quimibond_sgi_pesaje` | no | modo `calidad_pq` registrado desde el satélite | Medio | 6 | — | — | Instalación limpia de núcleo + satélites | El indicador de calidad PQ mide igual |

### 5.4 Entrega 3 — Modelo de datos

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e3-format-map-documento` (**va primero**, decisión 4 de la tanda 1) | C-006 | `sgi_format_map.py` (`document_id`), pies y reportes que buscan por clave, 8 claves escritas en código | `post-migrate`: ligar los 9 `sgi.format.map` a su documento por la clave actual | el pie de `sale.order` imprime la clave y la revisión viva | Medio: pie de formato en pedidos, OC y remisiones | 8 | — | — | Pruebas de pie de formato | Imprimir un pedido de prueba: el pie sale igual que antes (visual) |
| `sgi/e3-procedimiento-proceso` | C-001 (+C-014, D-017, L-003), C-002, C-015, L-005 (+C-003) | `sgi_process.py:52` (One2many), `sgi_document.py:111` (`restrict`, `tracking`), `sgi_cleanup.py:153-171`, `sgi_load.py:449-469`, vistas de proceso y documento | **sí:** `pre-migrate` = respaldo de `sgi_process_replaced_doc_rel` + conflictos al log (**sin** copiar a la M2O); `post-migrate` de L-005 (44 `en_curso`, 5 `na`, P-I01 fuera) | P-T1…P-T7 de `03-modelo.md` y P-L8/P-L9 de `12-transicion.md` | **Alto:** cambia el tipo de un campo con datos | 18 | C-006 | **Después:** Jose carga la sustitución de los 13 procesos vacíos conforme se cierren sus rutinas | Pruebas pasan | `process.replaced_document_ids` = 23 en 11 procesos; 3560 fuera de C4; 49 procedimientos: 44/5 |
| `sgi/e3-clave-anterior` | C-004 (+L-004), C-005, C-007, C-009 | `sgi_document.py` (`write`, `_sgi_find_by_code`, `sgi_title`), `sgi_catalog.py` (tipos, `PR-{proceso}`, validación de clave encendida), asistente «Asignar clave nueva» | **sí:** `post-migrate` `sgi_code` → `sgi_previous_code` en **490** (fuera 5556) | migración idempotente (490, luego 0), doble renombre, búsqueda 13 meses después, clave `PR-C1` válida y `P-C01` rechazada (J-006) | Medio: 490 filas por SQL, sin chatter | 16 | C-006; **D-02** (patrones F-IT, DAT, …) | **Antes:** C-008 (limpieza de 5556, 3359, etc.) | Pruebas pasan | `documents.document` con `sgi_previous_code` = 490 |
| `sgi/e3-integridad` | C-013 (+C-022), C-010, C-019, C-021, B-008, A-022 | M2O a `sgi.process`/`sgi.area`… con `restrict`; `sgi_doc_type` → `sgi_doc_type_id.code` (59 usos); regla de `sgi_activity_spec.py:280` sin `block`; separar `sgi_format_map.py` y `sgi_kpi_fields.py` | no (`ondelete` no requiere migración; `required` sí: 0 vacíos verificados en C-022) | borrar un proceso con indicador → error | Medio | 18 | — | — | Instalación limpia | Update sin «no se puede agregar la restricción» en el log |
| `sgi/e3-multiempresa` | C-012 (+H-021), F-014 | README + restricción «procesos solo en la empresa del SGI»; reglas en 10 modelos | no | `TestMultiCompany` (J-013) | Bajo | 3.5 | **D-03** | — | Pruebas pasan | Conteo de `ir.rule` |
| `sgi/e3-quimibond-sgi-mapa` → módulo `quimibond_sgi_mapa` | H-022, H-007, H-014, H-010, A-004, J-019 | `sgi.process.export_payload()` (inverso de `load_payload`), `data/mapa.json`, categoría 11 al núcleo con XML ID `noupdate`, campo `boundary` en `sgi.deliverable`, `sgi_indicator_formula_data.xml` fuera del núcleo | `pre-migrate`: XML IDs de los términos a `__export__` (patrón del anexo A) | exportar → cargar en base limpia con `dry_run` → 0 diferencias | Medio | 43 | **D-01**; C-004 (documentos por clave) | Carga **manual** con modo de prueba primero (decisión 13 de la tanda 2) | Base limpia + `quimibond_sgi_mapa`: 14 procesos, 60 etapas, 310 actividades | Exportar desde producción y comparar conteos |

### 5.5 Entrega 4 — Menús y acciones

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e4-arbol-de-menus` | E-007 (+D-005), E-013, A-025, E-012, E-010, I-009, B-015 (+A-009), E-017 | todos los `<menuitem>` a `sgi_menus.xml`; nombres y secuencias de `05-menus/menus_final.csv`; `view_mode` en la acción original; rutas del Diagnóstico desde una constante | no (el xml_id no cambia) | árbol contra `tools/sgi_menu_tree.txt` (J-023) | Bajo: renombrar y reparentar conserva `res_id` (E-002) | 11.1 | D-17, D-18, D-19, D-20 | E-017: archivar el tablero 65 si D-19 lo decide | Instalación limpia; el árbol coincide con `arbol_final.md` | `ir.ui.menu child_of 2377` = árbol final; las 57 actividades y 23 documentos con `odoo_menu_id` siguen resolviendo |
| `sgi/e4-grupos-direccion-auditor` | E-009 (+F-011), F-013, F-012, D-009, I-015 | `sgi_security.xml` (319 deja de implicar 318 e implica 316 + 317; lectura del Auditor sin salud ni salarios), `groups` de menús con `-` donde haya que quitar un grupo (lección de I-013), botones de cerrar/reabrir | no | Dirección no edita procesos; el Auditor no lee `sgi.health.record` (J-005) | Medio: cambia lo que ve el único miembro de 319 (35) | 6.5 | F-004 (hecho); **D-06, D-21** | **Después:** revisar con Jorge Manuel Ortiz (35) su menú | Pruebas pasan | `res.groups` 319 `implied_ids`; menús de Dirección para 316 |

### 5.6 Entrega 5 — Vistas y formularios

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e5-nomenclatura-en-pantallas` | D-002, D-003 (+D-020), D-004 (+E-006) | `sgi_document_views.xml` (`<h1>` = título limpio), 12 vistas con textos, 17 plantillas QWeb, 11 nombres de reporte | no | render de reportes (J-015) | Bajo | 8 | C-004, C-009, **D-02** | — | Render de reportes | Ficha de un documento: título sin clave (visual) |
| `sgi/e5-herencias-propias` | A-008, D-019, D-018 | 30 herencias propias integradas a su padre, una herencia por ficha estándar | **sí:** `pre-migrate` que borra cada herencia propia vieja (`ir_ui_view` + `ir_model_data`), como en 54.2.0 | `check_odoo_views.py --base-ref` | **Alto:** es lo que tumbó los builds de 54.x; un archivo por PR si hace falta | 15.5 | — | — | `--base-ref` en verde | Update sin «no puede ser localizado»; 33 → 3 herencias sobre vistas del SGI |
| `sgi/e5-reclamaciones` | D-006 (+D-010) | `helpdesk.team.sgi_is_complaint`, `sgi_complaint_views.xml` | `post-migrate`: marcar «Reclamaciones entretelas» y «ATENCION A CLIENTES» (decisión 9) | «Generar NC» solo en equipos del SGI (J-012) | Bajo | 1 | — | — | Pruebas pasan | Los 228 tickets de Sistemas sin el bloque SGI |
| `sgi/e5-fichas-y-busquedas` | D-007, D-008, D-013 (+E-016), D-014, D-015 (+E-015), D-016, D-021 | patrón común de encabezado y estados en Mejora/SST, `confirm` en 25 botones, búsquedas faltantes, ayudas de acciones, campos técnicos solo para MAST | no | — | Bajo | 13.8 | D-17; D-29 (tú) | — | Instalación limpia | Verificación visual de las fichas de NC, incidente y auditoría |

### 5.7 Entrega 6 — «Del Dropbox a Odoo»

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e6-seccion-del-dropbox` | E-001 (+L-007, I-016, B-020), E-004, L-010 (+E-005), C-011, L-011, L-012, L-013, L-014 | carpeta `menu_sgi_dropbox` (Procesos, sec. 60); menú 2419 y acción 3870 se mueven (mismo id); guarda en `documents.document.write/create`; `sgi_destination_label`; `sgi_legacy_family`; cron de menú ampliado | `post-migrate`: IT, DAT, anexos, protocolos y reglamentos sin clase → clase D y «No aplica» (respuesta 3 a L) | P-L7 (el usuario no escribe campos de transición) | Medio | 17 | C-004, C-001 (entrega 3) | **Después:** MAST corrige las 13 contradicciones de clase y estado (L-013; respuesta 8 a L) | Pruebas pasan | Pasos 6 y 14 de `verificacion_visual.csv`: un usuario encuentra F-P-A16-10 |
| `sgi/e6-rutinas` · **57.0.0** (modelos nuevos) | L-002 (+L-015, L-021), L-017, L-018 (+C-016), L-006, L-009 (+L-019), L-008 (+L-016, L-020) | `sgi.legacy.routine`, `sgi.dropbox.key` y `sgi.dropbox.progress` (vistas SQL), asistente de importación, ACL y regla `any` | no (modelos nuevos; sin datos con XML ID, decisión 4) | P-L1…P-L6 y P-L9 de `12-transicion.md` §3.11, en `tests/__init__.py` | Medio: vistas SQL con nombres de columna por confirmar en el build | 38 | e6-seccion | **Después:** validador → «Probar» → «Cargar» (Jose o MAST). Aceptación: 815 rutinas (709/68/38), segunda corrida con 0 cambios, P-I01 con 0 | Instalación limpia; P-L* pasan | Importación en modo de prueba sobre la copia: 0 errores |

### 5.8 Entrega 7 — Matriz de cumplimiento

Sin PR. «Cumple con» está cargado en 225 actividades, las 85 sin cláusula son a propósito y los 24 requisitos sin actividad los atiende Jose (decisión 6 del brief y «Para la tanda 2»). Ningún hallazgo pide algo más. El render de `report_compliance_matrix.xml` entra en la prueba de humo de J-015 (entrega 9).

### 5.9 Entrega 8 — Lógica y rendimiento

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e8-mediciones-y-bandeja` | G-001 (+I-006), I-001, I-007, I-012, G-017, I-021 | `sgi_my_pending.py` (tipos «Capturar», «Validar» a 3 días hábiles, `actividad`, `firma`), `sgi_my_procedure_screen.py`, semáforo único | no | una prueba por tipo de pendiente (J-007) | Bajo | 19 | **D-04** | — | Pruebas pasan | Dario (150): 12 «Medir» pasan a «Validar» con vencimiento; supervisor 24 deja de ver «Estás al día» |
| `sgi/e8-avisos-de-crons` | G-004 (+I-008, G-021), G-005 (+G-025), G-011, G-010, G-012, G-024, I-003, I-002 | `sgi_cron.py` (`_sgi_schedule` con `date_deadline`, cierre por episodio, parámetro `quimibond_sgi.mast_user_id = 128`) | `post-migrate` con respaldo: reasignar a 128 las actividades de MAST que están con 67 y 7, y cerrar las 42 «Procedimiento vivo cambió» triplicadas | los crons corren dos veces sin duplicar (J-008) | Medio: toca `mail.activity` en producción | 22 | **D-04** (I-002) | — | Prueba de humo de crons | `mail.activity` de 67 baja de 46; 128 recibe las de MAST |
| `sgi/e8-zona-horaria-y-dias-habiles` | G-007, G-008, G-009, G-022, G-020 | helper `sgi_today()` (zona del calendario, **sin** tocar OdooBot), `sgi_add_business_days` en escalamientos, adelantar al día hábil anterior, `no_timing` con mes y día obligatorios | `post-migrate`: festivos de la LFT art. 74 y del contrato colectivo en el calendario 21 (decisión 4 de la tanda 2); horarios de crons (los crons son `noupdate`, A-007) | vencimiento en sábado y en festivo (J-011) | Medio | 13 | — | Confirmar con RH los festivos del contrato colectivo | Pruebas pasan | `ir.cron` de checklists: `nextcall` 11:30 UTC (05:30 México) |
| `sgi/e8-medicion-robusta` | G-002 (+H-005), G-003, H-006, G-019, H-016, G-026, H-018 | `sgi_process_procedure.py:995-1085,1264-1295` (savepoint, dominio inválido → aviso), filtro de empresa en la medición, aviso de procedimiento de otro proceso, validación aprobador ≠ solicitante | no | segunda empresa no cuenta (J-009); aprobador = solicitante → error (J-010) | Bajo | 12.5 | D-03 (G-019) | **Antes:** Jose carga el aprobador correcto de E2.01, S4.03 y S6.07 (decisión 7 de la tanda 2) | Pruebas pasan | Las 8 actividades con registros de H-005 tienen semáforo |
| `sgi/e8-calibracion` | G-006 | `sgi_cron.py:888-926` (el bloqueo pasa a parámetro apagado; avisos al Coordinador de Laboratorio y al Jefe de Calidad; un resumen diario) | `post-migrate`: cerrar las 143 «Calibración VENCIDA» duplicadas (con respaldo) | el cron avisa sin bloquear | Medio | 3 | — | — | Pruebas pasan | 143 equipos dejan de estar en «No usar» por el cron |
| `sgi/e8-rendimiento` | G-015, G-016, G-023 | `hr.job` con hash y estado guardados; fechas hábiles por corrida; `flush_all()` antes de la fusión de puestos | no | — | Bajo | 13 | — | — | Tiempos de Mi equipo (pendiente de medir) | Mi equipo con filtro abre en menos de 3 s (medir) |
| `sgi/e8-checklist-pin` | I-005 | `sgi_checklist.py:211-217` (parámetro de PIN obligatorio) | no | sin PIN no se firma | Bajo | 2 | **D-08** | RH captura los PIN antes de arrancar | Pruebas pasan | — |

### 5.10 Entrega 9 — Pruebas

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e9-ci-y-checkers` | J-003 (+J-001), J-023, J-022, J-021, J-024 | `.github/workflows/ci.yml` (job con Enterprise, informativo al principio), checks nuevos (sin `sgi.process` en `data/`, árbol de menús, `t-esc`, `ref` a módulo en `depends`), `.pre-commit-config.yaml`, `quimibond_sgi/lib/` con pruebas `pytest` | no | todo el suite en CI | Nulo | 23 | **D-23, D-25** | — | CI verde | n/a |
| `sgi/e9-pruebas` | J-004 …J-017, J-020, J-025, J-026 (17 renglones) | pruebas con datos propios, etiqueta `sgi_datos_reales` (D-24), `freezegun` o fechas fijas, `t-esc` → `t-out` | no | las listadas | Nulo | 48.5 | Van **junto con** el PR que corrige cada cosa; lo que quede suelto va aquí | — | El suite completo pasa | `update.log` sin `WARNING` de `quimibond_sgi` (J-026) |

### 5.11 Entrega 10 — Documentación y manuales (los manuales van **después** de aplicar la entrega 4)

| PR (rama · versión) | Cierra | Archivos principales | Migración | Pruebas nuevas | Riesgo de upgrade | h | Depende de | Manual en producción | Evidencia en rama (build de desarrollo) | Evidencia en main/quimibond (upgrade sobre copia de producción) |
|---|---|---|---|---|---|---:|---|---|---|---|
| `sgi/e10-docs-historico` | K-001, K-002, K-005…K-012 | `docs/historico/sgi/`, `docs/historico/intelligence/` | no | — | Nulo | 6.8 | — | — | CI verde | n/a |
| `sgi/e10-readme-y-reglas` | K-003 (+B-023), K-004, K-016, K-013, A-028 (+K-014), K-015, A-021, A-024, A-029, A-030, C-020, C-023, H-017, F-018, F-019, F-021 | README ≤150 líneas, `CLAUDE.md` (párrafo del SGI), RUNBOOK (sección SGI), `description` y licencia del manifest, README de los satélites | no | — | Nulo | 15.6 | K-018; **D-28, D-31** | — | CI verde | n/a |
| `sgi/e10-docs-generadas` | K-021, K-020, K-017 | `tools/sgi_docs.py` (+ `--check` en CI), `docs/sgi/tecnica/*`, docstrings | no | `--check` | Nulo | 21 | — | — | CI verde | n/a |
| `sgi/e10-help` | C-018 (+K-019) | `help` en los 608 campos de prioridad alta (3 tandas) y bloque «¿Qué hago aquí?» | no | advertencia de `sgi_docs.py --check` para campos nuevos sin `help` | Bajo | 20 | **D-29** | — | Instalación limpia | Verificación visual |
| `sgi/e10-manuales` | K-022, K-023, K-024 | `docs/sgi/usuarios/*` (operador o supervisor, jefe de área, MAST, Dirección, auditor), `administracion/manual-jefe-mast.md`, `transicion/del-dropbox-a-odoo.md` | no | — | Nulo | 36.5 | entrega 4 aplicada en producción; **D-26, D-27, D-05** | Capturas con usuarios reales (visual) | — | — |

## 6. Acciones en producción (Jose, MAST, RH o Sistemas; no son código)

| # | Quién | Qué | Cuándo | IDs |
|---|---|---|---|---|
| 1 | Jose | MCP en Ajustes: respaldo CSV, 30 modelos en solo lectura, 9 desactivados (incluido `ir.config_parameter`) y límite de peticiones encendido (`06-seguridad/mcp_propuesta.csv`, `06-seguridad.md` §5) | Ya, antes que todo | F-001 (+F-002) |
| 2 | Sistemas | Cambiar las contraseñas de las cuentas que aparecen en P-I01. El acceso ya está cerrado (verificado: 3560 `none`/`none`) | Ya | L-001 |
| 3 | MAST | Emitir un P-I01 sin credenciales, o darlo de baja si C4 y C5 lo cubren. Mientras, sigue «En curso» (respuesta 1 a L) | Antes de la carga de rutinas | L-001 |
| 4 | Jose o Sistemas | Meter **solo** al usuario 88 (Miguel Medina) al grupo «Salud ocupacional (SGI)» y verificar que el menú 2600 ya no tenga el grupo 68 | Después de desplegar la entrega 1 | F-004, I-013 |
| 5 | Jose o Sistemas | Depurar a los 20 miembros de «Empleados / Encargado» (`hr.group_hr_user`). Deja de exponer los salarios de eficiencias (F-004 b) | Después de la entrega 1 | F-004 |
| 6 | Areli (MAST) y RH | Vencimiento de E2.21 (fecha del registro de Protección Civil) y S4.30 (periodo de SIRCE). Las otras 13 ya están (verificado por MCP) | Cuando tengan las fechas | H-003 |
| 7 | Jose | Archivar (nunca borrar) 5556 (antes compararlo con 3518 y, si es más nuevo, subirlo como versión de 3518), 3359, 4995, 4022, 3760, 3644 y 5119 | Antes de `e3-clave-anterior` | C-008, L-016 |
| 8 | Jose → Sistemas | Nombre del dueño de C4 (hoy hr.employee 564 sin usuario); Sistemas crea el usuario | Cuanto antes | H-002 |
| 9 | Jose → Sistemas; MAST | Usuarios de oficina (Asistente de MAST, Almacenista K…). En planta, el supervisor ejecuta: MAST reasigna C4.25 y las 50 actividades sin nadie con usuario | Antes de la entrega 8 | H-001 (+I-010) |
| 10 | Jose | Membresía del grupo 327 (captura de eficiencias) según D-09 | Tras D-09 | I-020, F-020 |
| 11 | Jose o Sistemas | Cuentas 92 «Supervisor», 80 «manufactura@» y 184 según D-08 | Tras D-08 | I-017 |
| 12 | Jose o Sistemas | Quitar Calidad / Usuario (66) al contador externo (132) | Cuando se decida | I-018 |
| 13 | MAST | Cargar el plan de emergencia vigente y un recorrido CSH antes de anunciar «Seguridad y ambiente» | Antes de la entrega 10 | I-019 |
| 14 | MAST (programador por carga, con modo de prueba) | Mediciones contra módulos sin uso → «Manual» con justificación (decisión 6 de la tanda 2); correcciones de entregables H-008, H-009, H-011, H-012 y H-019 | Entrega 3 o 6 | H-004, H-008, H-009, H-011, H-012, H-019 |
| 15 | MAST | Asignar equipos a las 3 plantillas de checklist | Antes de `e8-zona-horaria-y-dias-habiles` | H-013 |
| 16 | MAST / dueños de proceso | Llenar el propietario (`sgi_owner_id`) de los 487 documentos vigentes sin él | **Antes** de `e1c-documentos-sin-propietario` | H-015, N-001 |
| 17 | Jose | Aprobador correcto de E2.01, S4.03 y S6.07 (el programador lo propone) | Antes de `e8-medicion-robusta` | H-018 |
| 18 | Jose | Resolver por fuera PV15254 y PV15323 (empresa 4) | Cuando quiera | I-004 |
| 19 | Jose | Nombrar al auditor interno (grupo 317 solo) | Tras D-05 | I-011 |
| 20 | **Areli** con cada dueño de proceso | Decidir las **38 rutinas pendientes a más tardar el 16-oct-2026**; preguntar a Areli por las familias sin procedimiento (P-A05, A13, A23, A30, C10, P07), que mientras tanto quedan como «Por definir»; corregir las 13 contradicciones de clase y estado antes de la carga real | 16-oct-2026 | L-014, L-013, L-015 |
| 21 | Jose | Ligar en el **documento** la sustitución de los 13 procesos que hoy están vacíos, conforme se cierren sus rutinas | Continuo | C-001 (decisión 3) |
| 22 | Programador (repo) | Etiqueta de git sobre `main` antes de `e2-sin-migraciones` | Entrega 2 | A-026 |
| 23 | Programador | Revisar el log del cron mensual (lunes 5-oct-2026) en busca de «foto del valor del inventario» | 5-oct-2026 | A-017 (+G-018) |
| 24 | Sistemas | `sgi_drop_empty_studio_models(dry_run=True)` y después en real, en el shell | Tras D-11 | B-007 |
| 25 | Jose | Archivar las carpetas de Documentos 1805, 3078 y 1120 (sin borrar) y resolver los 8 de «POR CLASIFICAR» | Tras D-27 | K-024 |
| 26 | Jose | Archivar el tablero «Salud del SGI» (65) | Tras D-19 | E-017 |
| 27 | Quien despliega | Después de cada PR de `main` a `quimibond`: `odoo-update`, reinicios y consultas del RUNBOOK, más las de la columna «Evidencia en main/quimibond» | Cada despliegue | todos |

## 7. Decisiones pendientes para Jose (32; ordenadas por cuántas entregas frenan)

Ya salieron de esta lista las preguntas contestadas en `decisiones.md`: todas las de L; las de la tanda 1 y la tanda 2 (A 1, 4, 5, 6 y 8; B 1; C P-1, P-4 y P-6; D 1 y 3; E 1, 4 y 6; F 1, 2, 5 y 6; G 1 a 5; H 1 a 6; I 3 y 7); la de Knowledge (A 3), que se resolvió con la condición de la decisión 5; y la de los miembros de Salud ocupacional (I 5).

| # | Pregunta (origen) | Recomendación | Desbloquea | Entregas |
|---|---|---|---|---|
| D-03 | ¿El SGI es de una sola empresa (id 1) para siempre? (C P-3) | Sí: declararlo en el README, poner una restricción al crear procesos y que Mis pendientes filtre por la empresa del SGI | C-012, F-014, G-019, H-006, J-013, N-002 | 1b, 3, 8, 9 |
| D-01 | Indicadores (43), objetivos (10), planes de control (2) y términos (10): ¿catálogo base del SGI o módulo `quimibond_sgi_mapa`? (A 7, J 2) | Los términos con IDs de producción, al mapa (A-004). Los indicadores automáticos sin IDs fijos se quedan como catálogo. Objetivos y planes de control, al mapa | A-004, J-019, H-014, H-022, J-004, A-030 | 3, 9, 10 |
| D-02 | Patrón de clave nueva de F-IT, DAT, PROT, DF, R, Anexo y MIID. ¿Se renombra el nombre del archivo? (C P-2 resto, P-5) | F-IT → `F-{proceso}-{nn}`, DAT → `DA-{proceso}-{nn}`; Anexo, PROT y R según MAST. El archivo **no** se renombra: el título sale de `sgi_title` | C-004 (asignación), C-007, C-009, D-002, K-022 | 3, 5, 10 |
| D-05 | ¿Quién es el auditor interno si MAST administra el SGI? (I 4) | Un interno de otra área formado en 19011, o un externo, con el grupo 317 solo. E2.05 lo tiene en «Ejecuta» | I-011, F-012 (usuario de prueba), K-022 (manual del auditor) | 4, 10, Prod |
| D-08 | ¿Cuenta de tableta para checklists? ¿Se desactiva 92 «Supervisor»? ¿80 «manufactura@» es el Jefe de Manufactura? ¿Qué es 184? ¿PIN obligatorio? (I 2, F-018) | Tableta con `base.group_user` + Mantenimiento; desactivar 92; ligar 80 a su empleado; PIN obligatorio (el del quiosco) | I-005, I-017, F-018 | 8, 10, Prod |
| D-04 | ¿Cuál es la bandeja oficial: Mis pendientes o la campana? (I 1) | Mis pendientes, con las actividades atrasadas, las firmas y los avisos del SGI adentro | I-001, I-002, I-012, alcance de G-004/G-005 | 8, 10 |
| D-06 | Incidentes: ¿todos reportan y solo SST/MAST/Salud investigan, cierran y reabren? ¿Quién lee los datos de lesión? (F 3, D 4) | Sí. El reportante edita mientras siga en «reportado» | F-009, D-009 | 1, 4 |
| D-07 | Planes de emergencia, AMEF, PPAP y evaluación de proveedores: ¿captura abierta o «cada quien lo suyo»? (F 4) | El patrón de 56.7.0: responsable o creador, más MAST | F-010 | 1, 2 |
| D-11 | ¿Se borran los modelos de Studio viejos `x_emp_activity`, `x_no_conformidades`, `x_actividades_obligato` y `x_calendario_de_obliga`? (B 2) | Sí: primero en modo de prueba y, si dan 0 registros, en real; después se retira la herramienta | B-007 | 2, Prod |
| D-15 | Categorías sin uso: «Solicitud de compra SGI», MOC (14), la 13, y la familia OP-PTAR sin roles (B 6, H-020) | Archivar las categorías (nunca borrar) si Compras sigue con su flujo; quitar OP-PTAR o asignarle E2.24 | B-022 (+H-020) | 2, Prod |
| D-17 | «Acciones correctivas» (el nombre lo fija la decisión 2): ¿muestra solo NC e incidentes, o todas con filtro por origen? (D 2) | Todas, con filtro por origen y evidencia obligatoria al terminar | D-007, E-007 | 4, 5 |
| D-22 | MCP: ¿se apaga `allow_unlink` en `sgi.*` para que archive en vez de borrar? (F 8) | Sí, congruente con «no borrar datos» | F-001, `qb_mcp_politica` | 1, Prod |
| D-27 | ¿Cuál es la carpeta del SGI en Documentos? (K 4) | 1119; archivar 1805, 3078 y 1120 | K-024 | 10, Prod |
| D-29 | ¿«Tú» o «usted» en pantallas y manuales? (K 6) | «Tú» en todo | C-018, D-015, K-022 | 5, 10 |
| D-09 | Captura de eficiencias: ¿Jose (7) y Sistemas (152) en el grupo 327? ¿Un supervisor de un departamento padre captura todos los hijos? ¿Entran Gerardo (24) y Sergio (130)? (F 7, F-020, I-020) | Sacar a 7 y 152; `child_of` sí; 24 y 130 entran (en planta el supervisor ejecuta) | F-020, I-020 | Prod |
| D-10 | ¿Se va a usar la aprobación nativa de Studio para el rol «Aprueba»? (A 2) | Pasarla a un satélite `auto_install` y quitar `web_studio` del núcleo | A-010 | 2 |
| D-12 | `recompute_pending_measures`: ¿al cron diario o botón? (B 3) | Al cron diario de indicadores | A-006 | 2 |
| D-13 | ¿E1-02 «Acuerdos de la RxD cumplidos a tiempo» se liga al modo `acuerdos_rxd` o se borra el modo? (B 4) | Ligarlo si E1-02 no existe como fórmula (lo pide 9.3) | B-010 | 2 |
| D-14 | ¿Vuelve el resumen semanal por correo (cron apagado desde el 24-ago)? (B 5) | No: retirarlo | B-017 | 2 |
| D-16 | ¿Se retira la encuesta de auditoría legado (151) y su pestaña? (D 5) | Sí, archivando la encuesta | B-009 (+D-011) | 2 |
| D-30 | ¿`git fetch --unshallow` para que el CHANGELOG cubra 1.0–27.x? (K 7) | Sí, en una copia de lectura | K-018 | 2 |
| D-18 | Diagnóstico: ¿entrada única (decisión 2) o carpeta con 5? (E 2) | Carpeta; el Auditor lee | E-007, E-013 | 4 |
| D-19 | Tablero «Salud del SGI» en la app Tableros: ¿se usa? (E 3) | Archivarlo si está vacío | E-017 | 4 |
| D-20 | ¿Quitar «(SGI)» de «Calidad → Tableros (SGI)», «Competencias (SGI)» y «Eficiencias de personal (SGI)»? (E 5) | Sí; «Paretos de calidad» | E-007 | 4 |
| D-21 | ¿El Usuario SGI tiene una entrada «Documentos vigentes» (título y revisión) dentro del SGI? (I 6) | Sí (congruente con la decisión 11 de la tanda 2) | I-015 | 4 |
| D-23 | ¿Se paga la llave de `odoo/enterprise` para correr el suite del SGI en el CI? (J 1) | Sí; mientras tanto, corrida en el build de desarrollo de cada rama | J-003 (+J-001) | 9 |
| D-24 | ¿Las pruebas con datos reales (C2) se conservan con la etiqueta `sgi_datos_reales`? (J 3) | Sí; se excluyen del CI de base limpia | J-004 | 9 |
| D-25 | ¿Se apaga `translation-required` (395 avisos de pylint-odoo)? (J 4) | Sí | J-021 | 9 |
| D-26 | Manuales: ¿dónde viven, quién los mantiene y hay manual de Salud ocupacional? (K 1, 2 y 3) | Fuente en el repo, publicados como documento controlado (`MA-{proceso}`), dueño Jefe MAST; manual de salud de una página | K-022, K-023 | 10 |
| D-28 | ¿Qué normas están certificadas y desde cuándo (45001)? (K 5) | Poner el estado real y las fechas en los documentos nuevos | K-003, K-022 | 10 |
| D-31 | Licencia del manifest (hoy LGPL-3) (A-028) | `OPL-1` / «Other proprietary» | A-028 | 10 |
| D-32 | La decisión «Salud ocupacional» cita I-014, pero describe I-013 (b). ¿Se confirma que I-014 (botón «Firmar» visible sin permiso de Sign) sigue abierto? | Sí: se corrige en `e1-candados-rpc` (1 h) | I-014 | 1 |

## 8. Clasificación del inventario

`scripts/clasificar_universo.py` aplica las reglas en este orden:

1. **Decisiones:** módulos que salen, procesos viejos, migraciones y demo.
2. **CSV por elemento de los agentes:** `04-vistas/revision_por_vista.csv`, `02-obsoleto/metodos_103.csv`, `03-modelo/campos_sin_help.csv` (→ «Se corrige», C-018), `05-menus/menus_final.csv`, `05-menus/acciones_revision.csv`, `06-seguridad/reglas_evaluacion.csv` y accesos con hallazgo, `07-logica/crons.csv` y `10-calidad/pruebas.csv`.
3. **Nombre del elemento citado en un hallazgo consolidado:**
   - Si se cita en la columna `elemento`, toma la acción del hallazgo: Eliminar → Se elimina; Mover a otro módulo → Sale; Corregir, Agregar o Mover dentro → Se corrige.
   - Si se cita solo en la `propuesta`, o si es un contenedor (modelo, archivo, vista), llega como máximo a «Se corrige».
4. **Por defecto:** «Se queda».

Los IDs se escriben como `id_final`.

| Tipo | Se queda | Se corrige | Se elimina | Sale a otro módulo | Total |
|---|---:|---:|---:|---:|---:|
| Campo | 313 | 1,190 | 9 | 149 | 1,661 |
| Método | 1,164 | 45 | 51 | 153 | 1,413 |
| Dato con XML ID | 314 | 2 | 91 | 29 | 436 |
| Archivo | 221 | 82 | 49 | 19 | 371 |
| Vista | 188 | 94 | 4 | 36 | 322 |
| Acceso | 227 | 10 | 3 | 40 | 280 |
| Modelo heredado | 45 | 135 | 1 | 7 | 188 |
| Acción | 54 | 52 | 1 | 13 | 120 |
| Menú | 62 | 28 | 1 | 13 | 104 |
| Modelo propio | 44 | 46 | 1 | 11 | 102 |
| QWeb | 24 | 18 | 0 | 4 | 46 |
| Regla | 26 | 5 | 1 | 0 | 32 |
| Cron | 4 | 21 | 1 | 0 | 26 |
| Grupo | 4 | 2 | 0 | 0 | 6 |
| Asset JS | 4 | 2 | 0 | 0 | 6 |
| **Total** | **2,694** | **1,732** | **213** | **474** | **5,113** |

**Cobertura: 100 %.** Los 5,113 renglones tienen clasificación y ningún renglón que no sea «Se queda» se quedó sin ID. Así se clasificaron:

| Fuente | Renglones |
|---|---:|
| Por defecto | 2,102 |
| Campos sin `help` | 1,172 |
| Decisiones | 613 |
| Nombre citado en un hallazgo | 362 |
| Vistas | 329 |
| Acciones | 107 |
| Menús | 104 |
| Métodos | 99 |
| Pruebas | 83 |
| Datos por modelo o XML ID | 62 |
| Reglas | 31 |
| Crons | 26 |
| Grupos y accesos | 16 |
| Siembras de B-001/A-006 | 7 |

**Para que Jose revise:**

- **Tipos sensibles que quedaron «Se queda» solo por defecto: 78 modelos (36 propios y 42 heredados). Ningún grupo, regla ni cron quedó solo por defecto.** Están en `99-consolidado/universo_solo_por_defecto_sensibles.csv`. Entre los propios están `sgi.audit`, `sgi.audit.finding`, `sgi.audit.checklist.line`, `sgi.audit.program.line`, `sgi.activity.week.stat`, `sgi.activity.exec.stat`, `sgi.document.ack`, `sgi.competence.gap`, `sgi.checklist.line`, `sgi.csh.finding`, `sgi.dev.characteristic`, `sgi.machine.sheet.yarn`/`.param`, `sgi.indicator.measure.split`, `sgi.indicator.step`, `sgi.direction.board`, `sgi.diagnostic(.line)`, `sgi.instruction.publish` (depende de Knowledge, A-014) y `sgi.catalog.load.wizard`. Ningún agente pidió cambiarlos, pero tampoco los revisó uno por uno contra un CSV por elemento.
- **Clasificaciones que salen de decisiones abiertas:**
  - Los datos `sgi.indicator`, `sgi.objective` y `sgi.control.plan` quedan «Se queda» con J-019 mientras se resuelve D-01.
  - El cron `sgi_cron_weekly_digest` y las categorías de aprobación sin uso quedan «Se elimina» según la recomendación, pendientes de D-14 y D-15.
  - El tablero 65 queda «Se elimina» pendiente de D-19.
- **`sgi_next_code` queda «Se queda»** aunque `metodos_103.csv` diga «Se elimina» (contradicción 9).
- **Los 149 campos, 153 métodos y 40 accesos que «Salen»** son de `sgi.sales.budget*`, `sgi.lock.date.log`, `sgi.inventory.value`, `sgi.ppap*`, `sgi.fmea*` y `sgi.msa.study`. `sgi.coa.inbox` se queda en el núcleo, por A-018.

## 9. Límites

- No hay base de pruebas aparte: todo lo visual y todos los tiempos quedan **pendientes de verificación** en el build de desarrollo de cada rama. La lista está en `09-roles/verificacion_visual.csv`.
- El plan de versiones (56.26.0 en adelante) es una sugerencia de orden. Cada PR toma la siguiente versión libre cuando se integre.
- Por MCP solo verifiqué dos cosas: el acceso de 3560 y los vencimientos de H-003. Lo demás sale de los informes y del repo: `git diff origin/main origin/claude/sgi-entrega-1`, el conteo de acciones redefinidas y de migraciones, y `sgi_archived_filters.py:98`.
