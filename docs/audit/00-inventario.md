# 00 — Inventario de línea base del SGI (fase 0)

**Fecha:** 2026-09-29 · **Rama:** `claude/quimibond-sgi-audit-rebuild-ver105` (la rama designada para esta sesión; hace el papel de `audit/sgi-2026-10`) · **Modo:** solo lectura.

## Cómo se generó (reproducible)

| Fuente | Cómo | Qué cubre |
|---|---|---|
| Código | `python3 docs/audit/scripts/inventario_codigo.py quimibond_sgi quimibond_sgi_pesaje quimibond_sgi_plm quimibond_sgi_revisado` (AST de Python + XML con la librería estándar; no importa Odoo) | modelos, campos, métodos, vistas, QWeb, acciones, menús, grupos, accesos, reglas, crons, datos con XML ID y `noupdate`, archivos, assets JS |
| Base de datos | MCP de Odoo **en producción**, solo `search`/`read`/`read_group`, empresa 1 | versión instalada, `ir.model.data` del módulo, árbol real del menú, grupos y miembros, conteos de procesos/actividades/roles/documentos y datos de la transición |

**Limitaciones (dichas de frente):**
- **No hay staging a mi alcance.** No tengo shell de Odoo.sh ni una base de copia; el MCP apunta a producción. Todo lo "de BD" de este inventario es lectura de producción; ningún create/write/unlink.
- El "dónde se usa" de cada campo/método es un **conteo de menciones del nombre** en `.py`/`.xml`/`.js`/tests del módulo (sin contar migraciones). Sirve para priorizar; no prueba uso ni desuso (un nombre común como `name` siempre sale usado). El agente B debe confirmar cada caso con grep dirigido y BD.
- El MCP no expone los 102 modelos propios: expone 83 `sgi.*`. Los conteos por modelo de todos los datos operativos quedan para el agente H.

## Hechos que cambian el punto de partida

1. **Producción ya está en `quimibond_sgi 19.0.56.24.1`** (no 56.21): `ir.module.module` id 1845, `installed_version = latest_version = 19.0.56.24.1`, igual que el manifest de `main`. El "release de menús en curso" ya se instaló (56.24.0). Satélites: `quimibond_sgi_pesaje 19.0.5.1.0`, `quimibond_sgi_plm 19.0.3.0.0`, `quimibond_sgi_revisado 19.0.4.1.1`.
2. **«Cumple con» está construido y cargado**: `sgi.process.activity.norm_clause_ids` (Many2many a `sgi.norm.clause`, `models/sgi_norm_compliance.py:24`, 56.23.0), con la inversa en la cláusula (`:45`), `report/report_compliance_matrix.xml` y el checklist de auditoría (`:165`, `:208`). En producción, **225 de las 310 actividades tienen cláusulas** (`read_group` del 2026-09-29, segunda lectura; en la primera salió vacío). Las 85 sin cláusula son de finanzas, fiscal y nómina, y **así se quedan (decisión de Jose)**. Los 24 requisitos sin actividad son huecos reales que atiende Jose, **no son hallazgos de la auditoría**: 9001 4.3, 4.4, 5.2, 5.3 y 10.1; 14001 y 45001 4.1, 4.3, 4.4, 5.1, 5.2, 5.3, 10.1 y 10.3; 14001 7.4; CLI-08 y CLI-09.
3. **Los roles de actividades archivadas ya se borraron** (56.24.0, con respaldo): `sgi.activity.role` agrupado por `activity_id.active` → 1,072 roles, todos de actividades activas. Lo mismo `sgi.activity.input` (246, todos activos).
4. **Los dos menús de contabilidad ya se movieron**: `Contabilidad/Reportes/Bitácora de bloqueo contable` (menú 2587) y `Contabilidad/Reportes/Valor del inventario por mes` (2588).
5. **Hay un 6.º grupo** que no está en la lista de 0.3: `group_sgi_efficiency_capture` (id 327, «Captura de eficiencias (jefe o supervisor de área)», implica Usuario SGI, 11 usuarios).
6. **El usuario del MCP** (tú, Jose) tiene en solo lectura únicamente `ir.model.access`, `ir.rule`, `ir.ui.view`, `ir.model.data` (y `account.online.link`, ajeno al SGI); `ir.cron`, `ir.ui.menu`, `res.groups`, `ir.config_parameter`, `ir.model`, `ir.model.fields`, `ir.module.module`, `ir.filters`, `ir.default` están con CRUD completo (`inventario/mcp_permisos.csv`). Va al agente F.

## Conteos por tipo — código (`quimibond_sgi`)

| Tipo | Cantidad | Notas | CSV |
|---|---:|---|---|
| Modelos propios | **102** | 74 `Model`, 18 `TransientModel`, 10 `AbstractModel` | `modelos.csv` |
| Modelos estándar extendidos | **47** | `account.move`, `approval.*`, `documents.document`, `hr.*`, `mail.activity`, `mrp.*`, `quality.*`, `res.*`, `sale.order`, `stock.*`, `studio.approval.rule`… | `modelos.csv` |
| Clases Python que declaran modelo | 290 | 188 son `_inherit` (muchas extienden modelos **propios** repartidos en varios archivos) | `modelos.csv` |
| Campos | **1,661** | M2O 354 · Char 239 · Integer 218 · Selection 202 · M2M 124 · Text 119 · Boolean 107 · Date 95 · O2M 78 · Float 73 · otros 52 | `campos.csv` |
| — sin `help` | 1,284 (77 %) | | |
| — `compute` sin `store` | 274 | candidatos a revisar si se buscan/agrupan (agente C) | |
| — M2O almacenados sin `ondelete` explícito | 216 | | |
| — sin ninguna mención fuera de su definición | 11 | candidatos a obsoleto (agente B) | |
| Métodos | **1,413** | 269 usan `sudo()` (420 llamadas); 44 tienen `except` | `metodos.csv` |
| — sin mención en código ni XML (excluye compute/onchange/check/action_/cron) | 103 | candidatos a obsoleto (agente B) | |
| Vistas `ir.ui.view` | **322** | list 73 · form 57 · search 45 · pivot 18 · `sgi_diagram` 17 · graph 8 · activity 7 · kanban 6 · hierarchy 3 · grid 1 · **herencias 87** | `vistas.csv` |
| — herencias de vistas de **otros** módulos | 57 | `stock.picking` 5, `hr.employee` 5, `sale.order` 4, `purchase.order` 4, … | |
| — herencias de vistas **propias** | 30 | contra la regla de `CLAUDE.md` («un módulo no hereda sus propias vistas»); p. ej. `sgi_indicator_view_form_*` ×4, `sgi_process_activity_view_form_structure`, `sgi_audit_view_form_pr6` | |
| — xpath posicional (`[n]`) | 1 | | |
| Plantillas QWeb (`<template>`) | **46** | reportes, portal, pie de formato | `qweb.csv` |
| Acciones | **120** distintas | 73 `act_window` · 30 `report` · 16 `server` · 1 `client`. **17 `act_window` se definen dos veces** en archivos distintos (la segunda pisa a la primera) | `acciones.csv` |
| Menús | **104** distintos | 20 sin acción (carpetas); 2 redefinidos (`menu_sgi_panel`, `menu_sgi_processes`) | `menus.csv` |
| Grupos | 6 | ver arriba | `grupos.csv` |
| Reglas `ir.model.access` | **280** | auditor 87 · manager 72 · user 64 · `base.group_user` 16 · otros grupos estándar 30 · admin 4 · director 2 · captura de eficiencias 2 | `accesos.csv` |
| Reglas `ir.rule` | **32** | 14 globales (sin grupo) | `reglas.csv` |
| Crons | **26** | en producción 25 activos, 1 apagado | `crons.csv` |
| Registros con XML ID en `data/` + `demo/` | **961** | 426 `noupdate="1"`, 535 se reescriben en cada upgrade | `registros_xml.csv` |
| Datos de negocio con XML ID (no vistas/acciones/menús/seguridad) | 436 | cláusulas 73, indicadores 43, encuesta 66, flujos de proceso 37, **procesos 21 (los MP-*/P-* archivados)**, PPAP 18, tipos de documento 16, secuencias 16, áreas 10, objetivos 10, términos 10, categorías de riesgo 10, fuentes de NC 9, format map 9, etapas proyecto/helpdesk/calidad 21, plantillas de correo 5, categorías de aprobación 3… | `datos.csv` |
| `<function>`/`<delete>` en XML | 8 | | `funciones_y_deletes_xml.csv` |
| Reportes QWeb (`ir.actions.report`) | 30 | | `acciones.csv` |
| Wizards (`TransientModel` propios) | 18 | | `modelos.csv` |
| Assets JS/OWL | 5 archivos, 6 registros | vista `sgi_diagram` (registro de vista + template OWL + scss) y `new_activity_list.js` (Mi procedimiento) | `assets_js.csv` |
| Archivos | **371** | manifest 134 · Python importado 88 · tests registrados 87 · helpers de test 4 · migraciones 41 carpetas · assets 5 · scripts sueltos 3 (`tools/carga_documental.py`, `tools/post_carga_documental.py`, `tools/reporte_telas_rollout.py`) · `README.md` (2,578 líneas) y `.gitignore` fuera del manifest | `archivos.csv` |

**Tests:** los 87 `tests/test_*.py` están todos registrados en `tests/__init__.py`; ninguno huérfano.

**Satélites** (inventariados, fuera del alcance principal salvo que decidas otra cosa): `quimibond_sgi_pesaje` (extiende 1 modelo, 1 dato), `quimibond_sgi_plm` (extiende 1 modelo, 6 campos, 1 vista), `quimibond_sgi_revisado` (3 vistas, 1 acción, 1 menú). Ningún modelo propio.

## Conteos — producción (`ir.model.data`, módulo `quimibond_sgi`)

Coinciden con el código: 368 vistas (322 + 46 QWeb), 73 `act_window`, 30 reportes, 104 menús, 280 accesos, 32 reglas, 6 grupos, 26 crons, 149 modelos, 3,054 campos (incluye los mágicos), 777 opciones de selección, 36 constraints SQL. Las 42 `ir.actions.server` = 16 del código + 26 de los crons.

## Árbol real del menú SGI en producción (74 entradas bajo «SGI», id 2377)

```
SGI [Usuario SGI, Auditor SGI]
├─ Inicio: Mis pendientes · Mi procedimiento · Mis indicadores · Mi equipo · Eficiencias de mi área [captura de eficiencias]
├─ Procesos: Mapa de procesos · Actividades · Matriz de responsabilidades · Puestos y procesos · Fichas de proceso por máquina
├─ Mejora: No conformidades · Reclamaciones de clientes · Acciones correctivas [Jefe MAST] · Mejora continua · Lecciones aprendidas · Quejas y sugerencias del personal
│   └─ Auditorías: Programa de auditorías · Auditorías realizadas
├─ Seguridad y ambiente: Incidentes y accidentes · Planes de emergencia · Simulacros · Recorridos de la Comisión de Seguridad e Higiene · Estudios de higiene y exámenes médicos [grupo 68, Jefe MAST] · Responsivas de EPP · Hojas de checklist (planta y unidades)
├─ Dirección [Auditor, Jefe MAST, Dirección]: Tablero de dirección · Revisión por la dirección [Jefe MAST] · Política integral · Objetivos integrales · Riesgos y oportunidades · Requisitos legales · Partes interesadas · Satisfacción del cliente
└─ Administración SGI [Auditor, Jefe MAST]
    ├─ Documentos: Documentos · Lista maestra de documentos · Documentos externos · Solicitudes de cambio a documentos [Jefe MAST] · Migración de formatos [Jefe MAST] · Tipos de documento [Jefe MAST]
    ├─ Indicadores: Indicadores · Mediciones · Mediciones por equipo o mercado
    ├─ Aprobaciones del SGI [Jefe MAST]
    ├─ Diagnóstico [Jefe MAST]: Diagnóstico del SGI · Cobertura de medición · Cumplimiento de procedimientos · Faltantes de especificación · Cumplimiento semanal
    ├─ Firmas de lectura: Publicar Mi procedimiento [Jefe MAST] · Acuses de lectura
    └─ Configuración [Jefe MAST]: Ajustes [Administración/Ajustes] · Cargar catálogo [Admin SGI] · Familias de puesto · Áreas · Normas · Cláusulas · Categorías de riesgo · Elementos PPAP · Checklists de planta y unidades · Formatos en documentos de Odoo · Fuentes de NC automáticas
```

Contra el árbol decidido (0.4-2), a primera vista: **falta «Del Dropbox a Odoo»** en Procesos; «Migración de formatos» sigue en Documentos; «Tablero de dirección» vs «Tablero»; «Solicitudes de cambio a documentos» vs «Solicitudes de cambio»; «Lista maestra de documentos» vs «Lista maestra»; «Hojas de checklist (planta y unidades)» vs «Hojas de checklist»; en Diagnóstico hay cinco submenús. El análisis fino es del agente E.

## Datos de producción (empresa 1)

| Qué | Conteo | Evidencia |
|---|---:|---|
| Procesos activos | 14 (C1–C6, S1–S6, E1, E2), **sin XML ID** | `sgi.process` activos |
| Procesos archivados | 25 (MP-ADM, MP-CAL, MP-DIS, MP-MFG, MP-MTO + 20 P-*); **21 tienen XML ID** en `data/sgi_process_data.xml` (`noupdate="1"`), o sea que una instalación nueva los vuelve a crear | `ir.model.data` `sgi.process` = 21 |
| Etapas | 60 de procesos activos + **70 de procesos archivados** | `sgi.process.stage` por `process_id.active` |
| Actividades | **310 activas**, 144 archivadas | `sgi.process.activity` por `active` |
| Roles de actividad | 1,072 (0 de archivadas) | |
| Entradas de actividad | 246 | |
| Entregables | 319 activos, 1 archivado | |
| Normas / cláusulas | 4 normas: 9001 (28), 14001 (22), 45001 (23), Requisitos de clientes (10) = 83 cláusulas; solo 73 con XML ID (las 10 CLI son de producción) | |
| Categorías de aprobación | 8 activas, 9 archivadas | |
| Crons del SGI | 25 activos, 1 apagado | `ir.cron` por modelo `sgi.%` |

## Transición Dropbox → Odoo (línea base)

| Qué | Total | Con datos de destino completos | Evidencia / observación |
|---|---:|---:|---|
| Documentos controlados (`sgi_is_controlled`) | 588 | — | 97 son «Mi procedimiento (MP)» y 64 «Formulario de Odoo (vista)», que son del sistema nuevo |
| **Documentos del Dropbox** (sin MP ni formularios de Odoo) | **427** | ver abajo | los "~490" del encargo incluían documentos externos, procedimientos y Mi procedimiento; los 427 son solo los del Dropbox (aclarado por Jose, no es discrepancia) |
| — Procedimientos (P) | 52 (50 vigentes, 1 borrador, 1 obsoleto) | **0 con `sgi_replaced_by_process_id`** | El lado del proceso sí tiene `replaced_document_ids`: 28 documentos en 12 de 14 procesos (S2 y S6 sin ninguno). Los dos lados no están sincronizados y los procedimientos sustituidos siguen **vigentes** |
| — Formatos (F 193, F-IT 65) | 258 | 258 con `sgi_migration_target` o `sgi_odoo_menu_id` | estado: migrado 177, en curso 12, no aplica 14, pendiente 55 |
| — Instructivos (IT) | 48 | 0 | los 48 en `pendiente` |
| — DAT | 42 | 0 | 41 pendientes, 1 migrado |
| — Anexos 15, Protocolos 5, Reglamentos 5, Manual 1, Diagrama 1 | 27 | 0 con destino | la mayoría marcados `migrado` sin destino |
| `sgi_previous_code` (clave anterior) | 427 | **0 con dato** | la clave del Dropbox vive hoy en `sgi_code`, que es justo lo que la decisión 1 quiere evitar en títulos |
| `sgi_migration_class` | 427 | 237 con clase (a/b/c/d/x) | 190 sin clase y `pendiente` |
| Rutinas de procedimientos anteriores | ~99 omitidas + ~30 reemplazadas + cubiertas | **no hay modelo** | el análisis está fuera de Odoo |
| `sgi.process.activity.legacy_number` | 310 activas | **0 con dato** | |

## Universo a clasificar

`inventario/universo.csv`: **5,113 renglones** (102 modelos propios, 188 clases que extienden modelos, 1,661 campos, 1,413 métodos, 322 vistas, 46 QWeb, 120 acciones, 104 menús, 6 grupos, 280 accesos, 32 reglas, 26 crons, 436 datos, 6 registros JS, 371 archivos) con las columnas `clasificacion` (Se queda / Se corrige / Se elimina) y `hallazgo` vacías, para llenarlas en la fase 2.

## Archivos

| CSV | Renglones |
|---|---:|
| `modelos.csv` | 292 |
| `campos.csv` | 1,667 |
| `metodos.csv` | 1,422 |
| `vistas.csv` | 326 |
| `qweb.csv` | 46 |
| `acciones.csv` | 138 (incluye las 17 redefiniciones) |
| `menus.csv` | 107 |
| `grupos.csv` / `accesos.csv` / `reglas.csv` | 6 / 280 / 32 |
| `crons.csv` | 26 |
| `datos.csv` / `registros_xml.csv` | 437 / 967 |
| `archivos.csv` | 389 |
| `assets_js.csv` | 6 |
| `mcp_permisos.csv` | 1,088 modelos que expone el MCP con sus permisos |
| `universo.csv` | 5,113 |
