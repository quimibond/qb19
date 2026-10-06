# Entrega 57.97.0 — Cláusulas y revisión por la dirección (plan de implementación)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Renumeración:** es la ficha **«57.96.0 — Cláusulas y revisión por la dirección (→ 57.97.0 o siguiente libre)»** del plan general (`docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`, hallazgos N-05 y N-09, puerta Q13). El número 57.96.0 lo tomó «SST y ambiente», así que esta ficha sale como **`19.0.57.97.0`**. Con eso, la ficha «57.97.0 — Interfaz» y las siguientes toman el siguiente número libre (57.98.0 en adelante) cuando se inicien; la Task 7.10 lo anota en el mapa del plan general.

> **Estado (2026-10-02):** implementado en la rama como `19.0.57.97.0` (casillas marcadas); faltan el build de Odoo.sh de la rama (Task 7.1 Step 4, Task 7.10 Step 6) y la verificación en producción (Task 7.10 Step 8). Desviación: «No eficaz» solo regresa a Seguimiento una NC cerrada (ver «Implementación» al final). Base del plan: rama `claude/confident-mendel-8yg7xu` igual a `origin/main` en `d45525a1` (merge del PR #516, manifest `19.0.57.96.0`).

**Goal:** que las cláusulas digan dónde está el hueco y que la revisión por la dirección tenga las entradas y salidas que piden 9001, 14001 y 45001, sin cambiar datos de negocio: (1) **13 cláusulas de tercer nivel** (6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4, 9.1.2 en la norma que las tiene) como **datos nuevos con xmlid**, `noupdate`, sin tocar las CLI (sin xmlid, A-029); un pre-migrate que **solo liga el xmlid** si alguien ya capturó a mano una cláusula con el mismo numeral en la misma norma; (2) una NC con folio **no sale de «Abierta» hacia «Seguimiento» (ni directo a «Cerrada») sin clasificación y cláusula**: solo en ese cambio de etapa, después de escribir, sin atrapar lo que ya está en Seguimiento o cerrado; (3) la revisión por la dirección **carga cuatro entradas nuevas** (incidentes y desempeño de SST, cambios en el contexto y partes interesadas, aspectos ambientales significativos, oportunidades de mejora), el **scrap solo de la empresa del SGI** (D-03), sus acuerdos nacen como **tipo «acuerdo»** (ya no inflan las acciones correctivas ni piden la evidencia de una correctiva), **«Marcar realizada» pide las conclusiones 9.3.3** y los **acuerdos abiertos pasan a la siguiente revisión** (cerrar no se bloquea: deja nota y la siguiente los carga). **Ningún dato de negocio cambia en el despliegue.**

**Architecture:** una versión de `quimibond_sgi` (19.0.57.97.0) sobre 57.96.0. Un XML de datos nuevo `noupdate` (`data/sgi_norms_tercer_nivel.xml`) con 13 `sgi.norm.clause`; `migrations/19.0.57.97.0/pre-migrate.py` (solo `ir_model_data`, con la tabla de cláusulas que también lee la prueba); Python en `sgi_nonconformity.py` (NC: `write`, dos métodos; acción: valor `acuerdo` en `action_type`) y `sgi_management_review.py` (campos, cuatro cargadores, `_sgi_scrap_domain`, conclusiones, acuerdos que pasan, constraint del tipo `acuerdo`); vistas propias editadas en su `<record>` (`sgi_management_review_view_form`, acción), la herencia **de otro módulo** `sgi_quality_alert_view_form` (hereda `quality_control.quality_alert_view_form`) editada en su `<record>`, el acta (`report/report_mgmt_review.xml`) y el diagrama 9.3 (`sgi_diagram_iso.py`). **Sin modelos nuevos, sin menús nuevos, sin ACL nueva.** Pruebas nuevas en un archivo (`test_clausulas_revision.py`, 12 casos) y cuatro pruebas existentes ajustadas.

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh), Python 3, XML de datos y vistas, `odoo.tests.TransactionCase`. Checadores: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`, `flake8`.

**Base de código verificada:** HEAD `d45525a1`. Las líneas citadas son de este HEAD; si cambian, buscar por el nombre del método. **Producción está en 19.0.57.90.1** (MCP, 2026-10-02): 57.91.0 a 57.96.0 todavía no se despliegan; esta entrega se despliega con ellas o después. Lo que no se pudo comprobar sin Odoo va marcado «VERIFICAR:» (sección 1.9).

---

## 0. Reglas (resumen de la sección 0 del plan general y de `CLAUDE.md`)

Leer antes de empezar: `CLAUDE.md` (raíz), `addons/quimibond_sgi/README.md` (glosario, «usted» y A-029 en «Estructura contra datos»), las entradas 57.93.0, 57.95.0 y 57.96.0 de `addons/quimibond_sgi/CHANGELOG.md`, y `docs/audit/decisiones.md` (D-03 una sola empresa, D-009 quién cierra, D-13 E1-02).

1. **Versión y CHANGELOG:** `__manifest__.py` a `'19.0.57.97.0'` y `## 19.0.57.97.0 — AAAA-MM-DD` arriba de la 57.96.0. Esta entrega **no agrega modelos** (sí una tabla `Many2many` y columnas): sin bump, `check_addons.py` solo advierte; se sube igual (regla del repo) y el CHANGELOG es obligatorio con el bump.
2. **Pruebas registradas** en `tests/__init__.py` (el checker lo exige), clases con `@tagged('post_install', '-at_install')`.
3. **Las pruebas solo corren en Odoo.sh**, build de desarrollo de la rama, con `--test-tags /quimibond_sgi` (no el suite completo). El CI no instala el SGI. Ciclo: prueba → push → ver el fallo en el build → código → checadores → push → leer el build.
4. **Checadores locales** (0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   flake8 addons/quimibond_sgi
   ```
5. **Sin herencias propias** de vistas del SGI: la ficha de la revisión se edita en su `<record>`. La única herencia que se toca es de otro módulo (`sgi_quality_alert_view_form` → `quality_control.quality_alert_view_form`), en su `<record>`, sin xpath nuevos.
6. **`_name` en extensiones con varias clases:** ninguna clase nueva. `SgiActionLineReview` (`_inherit = 'sgi.action.line'`) hereda un solo modelo; se le agrega un constraint, no herencias.
7. **Tipo de modelo:** nada nuevo.
8. **Imports entre módulos de `models/`:** `sgi_management_review.py` ya importa de `sgi_risk` y `sgi_menu_paths`; no se agregan imports de otros modelos (los cargadores usan `self.env[...]` en tiempo de ejecución). `markupsafe.Markup` sí se importa.
9. **Accesibilidad:** el aviso nuevo de la pestaña «Conclusiones» lleva `role="status"`; no hay `<a class="btn">` nuevos.
10. **Menús:** ninguno nuevo; `tools/sgi_menu_tree.txt` no cambia.
11. **«Usted» y glosario:** `tests/test_usted.py` revisa toda cadena nueva (también los nombres de las cláusulas del XML y los `placeholder`). Nada de «tú», «tu», «puedes»; «no conformidad», «NC», «Jefe MAST».
12. **Lecciones de 57.93–57.96 que aplican aquí:**
    - **La base del build es copia de producción:** ninguna prueba cuenta registros globales; se comprueba con `assertIn`/`assertNotIn` o filtrando por los registros de la prueba (los textos de las entradas se revisan por nombres de prueba con prefijo `ZRD`).
    - **Un `ERROR` en el log tumba las pruebas:** el pre-migrate registra INFO y, si hay duplicados, WARNING.
    - **El checador de referencias** (`check_addons.py`, check 4) da error por un `env.ref('quimibond_sgi.x')` literal a un xmlid que todavía no está en un XML. La prueba de las cláusulas nuevas **no** usa `env.ref` literal: lee la tabla `NEW_CLAUSES` del pre-migrate (una sola fuente) y arma el xmlid con `'quimibond_sgi.' + name` (lección de 57.95.0, pruebas 11 y 17).
    - **Candados que se revisan después de escribir** (como el cierre de la NC desde 57.93.0): el formulario manda clasificación, cláusula y etapa en un solo `write`. Las pruebas usan el helper `_locked` (el `assertRaises` de Odoo, con savepoint), no `assertRaisesRegex`.
    - **`sudo` para contar, nunca para nombrar personas:** los cargadores de la revisión leen incidentes, aspectos, tareas y scrap con `sudo` y solo devuelven conteos o nombres de registros que no son personas (partes interesadas, FODA, mejoras, aspectos). Dirección (`group_sgi_director`) los corre sin `AccessError`.
13. **Despliegue:** PR rama → `main`, revisar el build; PR `main` → `quimibond`; `odoo-update quimibond_sgi` según `docs/RUNBOOK_DESPLIEGUE.md`.
14. **Producción es de solo lectura.** El despliegue no cambia datos de negocio. El pre-migrate solo escribe `ir_model_data` (metadato) y en producción no tiene nada que ligar (0 cláusulas con esos numerales). Las 13 cláusulas son registros nuevos de la estructura del módulo (como las 73 de fábrica), no datos capturados.

---

## 1. Decisiones de diseño (con evidencia del código)

### 1.1 Lo que se midió en producción (2026-10-02, MCP, solo conteos, compañía 1)

| Qué | Cifra |
|---|---|
| Versión instalada de `quimibond_sgi` | 19.0.57.90.1 |
| `sgi.norm` | 5: ISO 9001:2015 (id 1), ISO 14001:2015 (2), ISO 45001:2018 (3) con xmlid; **ISO 31000:2018 (4) y CLIENTES «Requisitos específicos de clientes» (5) sin xmlid** |
| `sgi.norm.clause` | **83**: 9001 = 28, 14001 = 22, 45001 = 23, CLIENTES = 10 (CLI-01…CLI-10), ISO 31000 = 0 |
| `ir.model.data` de `sgi.norm` y `sgi.norm.clause` | **76**, todos `quimibond_sgi` y `noupdate` (3 normas + 73 cláusulas de `data/sgi_norms.xml`); las 10 CLI no tienen xmlid |
| Cláusulas con numeral 6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4, 9.1.2 o 9.3.3 (cualquier norma) | **0**: no hay choque de numeral; el pre-migrate no tendrá nada que ligar |
| xmlid con la forma nueva `c_<norma>_<n>_<n>_<n>` | 0 (nombres libres) |
| NC con folio (`quality.alert`, `sgi_folio`) | **16**: **2 «Abierta»**, 14 «Cancelada»; 0 en Seguimiento, 0 cerradas. Las 16 **sin clasificación y sin cláusula** (la auditoría contaba 17 con 1 sin etapa; hoy las 16 tienen etapa) |
| `sgi.management.review` | **2, ambas «Borrador»** (RD-2026-01, periodo 1–20 jul; RD-2026-12, septiembre); **0 acuerdos**; 0 acciones con `review_id` |
| `sgi.action.line` | 4, todas «Corrección» (ninguna de revisión; nada que reclasificar a «acuerdo») |
| `stock.scrap` (visibles por MCP) | 10,794 hechos + 100 borrador, **todos de la compañía 1** (el filtro nuevo no cambia hoy ninguna cifra) |
| `sgi.incident` / `sgi.env.aspect` / auditorías con hallazgos | **0 / 0 / 0** |
| `sgi.interested.party` | 10 (8 externas, 2 internas), **las 10 con revisión vencida** (próxima: agosto 2026) |
| `sgi.risk` | 25: FODA 4, oportunidades (`kind`) 4; riesgos ambientales sin aspecto 5 (57.96.0) |
| Tareas de «Mejora Continua SGI» (`project.task`) | 0 (el xmlid `quimibond_sgi.sgi_project_improvement` existe, proyecto 453) |
| `sgi.legal.requirement` por tipo | 25: NOM 19, ley 2, permiso 2, **cliente 2** (Q13: los requisitos de cliente viven en la norma CLI **y** aquí) |

Con esto: **los candados nuevos no atrapan nada existente.** Las 2 NC abiertas siguen editándose; pedirán clasificación y cláusula solo cuando alguien las mueva a Seguimiento o las cierre sin pasar por él. Las 2 revisiones en borrador pedirán conclusiones al marcarse realizadas y tendrán las entradas nuevas en cuanto alguien pulse «Cargar entradas».

### 1.2 Cláusulas de tercer nivel (N-05)

Hoy: `data/sgi_norms.xml` (`noupdate="1"`, 76 registros) llega al segundo nivel; 6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4 y 9.1.2 «quedan absorbidas» y la Matriz de cumplimiento dice 100 %. `sgi.norm.clause` (`models/sgi_norm.py:22-37`) **no tiene restricción única** de norma + numeral: un XML que cree una cláusula que alguien ya capturó a mano no falla, la **duplica**.

Decisión:
- **Archivo nuevo** `data/sgi_norms_tercer_nivel.xml`, `noupdate="1"`, después de `data/sgi_norms.xml` en el manifest. Con `noupdate`, en un `-u` Odoo crea el registro cuyo xmlid no existe y no toca los que ya existen (el comentario de `sgi_norms.xml:1-5` lo dice; VERIFICAR en el `update.log`). Archivo aparte (no se edita `sgi_norms.xml`) para que se vea qué trae esta entrega; nada de lo de fábrica cambia.
- **13 cláusulas, solo donde la norma las tiene** (texto oficial en español):

  | xmlid | Norma | Numeral | Requisito |
  |---|---|---|---|
  | `c_9001_7_1_5` | 9001 | 7.1.5 | Recursos de seguimiento y medición |
  | `c_9001_9_1_2` | 9001 | 9.1.2 | Satisfacción del cliente |
  | `c_14001_6_1_2` | 14001 | 6.1.2 | Aspectos ambientales |
  | `c_14001_6_1_3` | 14001 | 6.1.3 | Requisitos legales y otros requisitos |
  | `c_14001_6_1_4` | 14001 | 6.1.4 | Planificación de acciones |
  | `c_14001_9_1_2` | 14001 | 9.1.2 | Evaluación del cumplimiento |
  | `c_45001_6_1_2` | 45001 | 6.1.2 | Identificación de peligros y evaluación de los riesgos y oportunidades |
  | `c_45001_6_1_3` | 45001 | 6.1.3 | Determinación de los requisitos legales y otros requisitos |
  | `c_45001_6_1_4` | 45001 | 6.1.4 | Planificación de acciones |
  | `c_45001_8_1_2` | 45001 | 8.1.2 | Eliminar peligros y reducir riesgos para la SST (jerarquía de controles) |
  | `c_45001_8_1_3` | 45001 | 8.1.3 | Gestión del cambio |
  | `c_45001_8_1_4` | 45001 | 8.1.4 | Compras (contratistas y contratación externa) |
  | `c_45001_9_1_2` | 45001 | 9.1.2 | Evaluación del cumplimiento |

  xmlid con guiones bajos entre cada parte del numeral (`c_45001_8_1_2`), para que no se lea como «8.12» ni choque con la forma de fábrica (`c_45001_81` = 8.1). 9001 no tiene 6.1.3, 6.1.4, 8.1.x; 14001 no tiene 7.1.5 ni 8.1.x; 45001 no tiene 7.1.5. No se agrega 9001 6.1.2 ni subniveles más finos (Q2).
- **No se toca nada de la norma CLIENTES ni de ISO 31000** (A-029): el XML solo hace `ref` a `sgi_norm_9001`, `sgi_norm_14001` y `sgi_norm_45001`.
- **Choque de numeral:** `migrations/19.0.57.97.0/pre-migrate.py`, antes de cargar los datos: para cada cláusula nueva cuyo xmlid **no** existe, busca en su norma una cláusula con el mismo numeral (`trim(code)`); si la hay, **inserta solo la fila de `ir_model_data`** (`noupdate = true`) apuntando a la de id menor. Es metadato, no dato de negocio: la cláusula conserva nombre, actividades, NC y hallazgos ligados; el XML la encuentra y no crea otra. Con varias del mismo numeral, liga la de id menor y avisa con WARNING de las demás (las une el Jefe MAST). Sin ninguna, no hace nada (el XML la crea). Idempotente. **En producción no tendrá nada que ligar (0 numerales iguales, 1.1)**; es la red para el caso en que el Jefe MAST capture una antes del despliegue. La tabla `NEW_CLAUSES` vive en el pre-migrate y la prueba la carga con `importlib` (patrón de `test_sst_links.test_03`).
- **Efecto a saber:** las 13 cláusulas nacen **sin actividades**: la Matriz de cumplimiento pasa de 83/83 «cubiertas» a mostrar 13 en rojo, «Puntos sin actividad» las lista y «Generar checklist» de una auditoría con esas normas agrega una pregunta por cada una (`sgi_norm_compliance.py:212-232`). Es lo que pedía N-05 («la matriz dice 100 % y esconde los huecos»). Ligarlas a actividades es dato (pista de datos), no código. `_sgi_find('45001 8.1.2')` (carga de actividades, `sgi_load.py:788`) empieza a encontrarlas.

### 1.3 Clasificación y cláusula al pasar a Seguimiento (N-05)

Hoy (`models/sgi_nonconformity.py`): `sgi_classification` (l.114-120) y `sgi_norm_clause_id` (l.121-123) son opcionales; producción: 0 NC con cláusula y 0 con clasificación. Los candados de etapa: `_sgi_check_stage_move` (l.583-619, **antes** de escribir: quién mueve y a qué etapa) y `_sgi_check_can_close` (l.778-841, **después** de escribir, l.894-896, desde 57.93.0). Etapas por xmlid: `sgi_nc_int_stage_open` (Abierta), `sgi_nc_int_stage_followup` (Seguimiento), cierre por `sgi_is_closing_stage` (`data/sgi_stages.xml:17-33`).

Decisión:
- **Candado nuevo `_sgi_check_classified`**: una NC con folio que **sale de «Abierta» (o de ninguna etapa) hacia «Seguimiento» o hacia una etapa de cierre** necesita `sgi_classification` y `sgi_norm_clause_id`. Se arma la lista **antes** de escribir (`_sgi_leaving_open`, con la etapa de antes) y se revisa **después** (lo que el formulario manda junto con la etapa cuenta), como el cierre. Un `UserError` deshace el `write` completo.
- **Solo en esa transición; no atrapa lo existente:** no aplica a NC que ya están en Seguimiento (pueden cerrarse sin cláusula), ni al reabrir una cerrada (Cerrada → Seguimiento, D-009), ni a `_sgi_on_ineffective` (l.505-546, que solo mueve desde una etapa distinta de Abierta), ni a cancelar, ni a editar una NC abierta. «Antes de Seguimiento» = sin etapa, «Abierta» o cualquier etapa que no sea Seguimiento, de cierre ni de cancelación (en una base nueva la etapa genérica de Calidad, sin equipos, puede ser la de una NC creada sin etapa; revisión 2026-10-02).
- **Nacer en Seguimiento también cuenta** (revisión 2026-10-02): el alta rápida en la columna «Seguimiento» del kanban (`default_stage_id`, el mismo camino que FUNC-C13 cerró para «Cerrada») o un `create` con `stage_id` = Seguimiento se brincaba el candado del `write`. `create` lo revisa **antes** de crear (no gasta folio): sin clasificación y cláusula en los valores, `UserError`. Exento solo el sistema (`user._is_superuser()`, como el candado de alta de FUNC-C13); el Jefe MAST no, como en el `write`.
- **«Directo a Cerrada» también cuenta:** sin esto, saltarse Seguimiento brincaría el candado. El **cierre forzado** del Jefe MAST (asistente, contexto `sgi_force_close` con `sgi_bypass_allowed`) queda exento, como de los demás candados de cierre.
- **Exento solo el superusuario** (`env.su`: sistema, crons, migraciones, la mayoría de las pruebas); el Jefe MAST **no** está exento (la clasificación es un requisito de calidad, no un permiso).
- **Cualquier cláusula vale**, también CLI-xx (Q13 por omisión: los requisitos de cliente siguen en la norma CLI).
- **Producción:** las 2 NC abiertas lo pedirán al moverse. Mensaje: «La NC NCI-… no pasa a «Seguimiento» sin: • la clasificación (mayor, menor u observación) • el requisito (cláusula de la norma) que se incumplió. Captúrelos en «Datos de la NC»…».
- Vista: el aviso «NC del SGI» de la ficha (herencia de `quality_control`, `views/sgi_nonconformity_views.xml:32-38`) agrega la frase; el `help` de los dos campos lo dice. **No** se pone `required` en la vista (atraparía a quien edita una NC abierta).
- Ajustes de pruebas existentes: `test_candados_evidencia._closable_nc` (el dueño y el Jefe MAST cierran desde Abierta) y `test_nc_deadlines.test_03` (el Jefe MAST mueve a Seguimiento) agregan clasificación y cláusula (`quimibond_sgi.c_9001_102`, ya existe). Revisado con `grep -rn "stage_closed\|stage_follow\|'stage_id'" addons/quimibond_sgi/tests/*.py`: `test_nc_auditoria_evidencia` crea sus NC en Seguimiento (exentas) y reabre desde Cerrada (exenta); `test_audit_hardening.test_a3` choca antes con «dueño del proceso»; `test_nc_flow`, `test_ola1`, `test_ola_b`, `test_flows_48`, `test_nc_deadlines.test_04` y `test_nc_auditoria_evidencia` (cierres con `nc.write`) corren como superusuario.

### 1.4 Revisión por la dirección: entradas nuevas y scrap de la empresa (N-09)

Hoy (`models/sgi_management_review.py`): `action_load_inputs` (l.118-138) llena 14 entradas; faltan 45001 9.3 d (incidentes y desempeño de SST), 9.3.2 b (cambios en el contexto y partes interesadas), 14001 9.3 (aspectos significativos) y 9.3.2 f (oportunidades de mejora). `_sgi_load_env` (l.341-357) busca `stock.scrap` **sin empresa** (l.344) y como el usuario (Dirección puede no tener permisos de Inventario).

Decisión:
- Cuatro campos `Text` de solo lectura, numerados después de los 14 de hoy: **«15. Incidentes y desempeño de SST»**, **«16. Cambios en el contexto y las partes interesadas»**, **«17. Aspectos ambientales significativos»**, **«18. Oportunidades de mejora»**, con sus cargadores `_sgi_load_incidents`, `_sgi_load_context`, `_sgi_load_env_aspects`, `_sgi_load_improvements` (nombres de la auditoría, H-B9.1):
  - **15:** incidentes del periodo por tipo y por severidad, días perdidos, abiertos hoy, verificados «Eficaz» en el periodo, IPER de riesgo alto sin acción (`high_without_action`), permisos de trabajo vencidos (`expired`, guardado desde 57.96.0). **Solo conteos, nunca nombres ni títulos** (el incidente lo leen solo quien lo reportó, el Jefe MAST, Salud ocupacional y el Auditor; se cuenta con `sudo`).
  - **16:** partes interesadas (total, revisadas en el periodo, con revisión vencida hoy, nuevas en el periodo por nombre) y FODA (total y las nuevas o evaluadas en el periodo por nombre).
  - **17:** aspectos de la matriz **de la empresa del SGI** que no están «Ya no aplica»: total, significativos por nivel, significativos sin control operacional, en evaluación, y hasta 15 significativos con folio, nombre y nivel; más «Riesgos ambientales que aún no pasan a la matriz: N» (los 5 de 57.96.0 hasta el traspaso). Sin aspectos: «Sin aspectos ambientales en la matriz (ruta del menú)».
  - **18:** tareas del proyecto **«Mejora Continua SGI»** (`quimibond_sgi.sgi_project_improvement`; no el de Diseño y Desarrollo, que también tiene `sgi_is_improvement`): nuevas, terminadas (`sgi_is_done_stage`) en el periodo y abiertas; oportunidades abiertas de la matriz de riesgos (`kind = 'oportunidad'`); hallazgos «Oportunidad de mejora» de las auditorías del periodo.
  - Nombres con un helper `_sgi_names` (los 10 más nuevos «y N más»); conteos por selección con `_sgi_count_by` (en el orden de la lista).
- **Scrap:** `_sgi_scrap_domain()` agrega `('company_id', '=', sgi.config._sgi_company().id)` (D-03: la empresa del SGI, PNTQ id 1 salvo el parámetro `quimibond_sgi.sgi_company_id`) y busca con `sudo` (el filtro de empresa explícito sustituye a las reglas). El texto dice de qué empresa es («Scrap de PRODUCTORA… por motivo…»). En producción todo el scrap es de la compañía 1: ninguna cifra cambia.
- `action_load_inputs` llena las cuatro y los acuerdos que pasan (1.5). La ficha muestra 15–18 en «Entradas (9.3.2)», el acta y el diagrama 9.3 también.
- **No retroactivo:** las 2 revisiones en borrador quedan vacías en 15–18 hasta que alguien pulse «Cargar entradas».

### 1.5 Acuerdos: tipo «acuerdo» y acuerdos abiertos a la siguiente revisión (N-09)

Hoy: `action_mark_done` (l.377-406) crea cada acuerdo como `sgi.action.line` con `action_type='correctiva'` (l.400): infla la estadística de correctivas y, desde 57.93.0, le pide la **evidencia de una correctiva** al terminarse (`_sgi_needs_evidence`, `sgi_nonconformity.py:1157-1160`). `action_close` (l.414-416) cierra con acuerdos abiertos y la siguiente revisión solo ve, como texto, los de la **última** (`_sgi_load_prev_agreements`, l.232-249).

Decisión:
- **`action_type` agrega `('acuerdo', "Acuerdo de la revisión por la dirección")`** en `sgi_nonconformity.py:1036-1043` (un solo lugar; sin `selection_add`). `action_mark_done` crea los acuerdos con `'acuerdo'`. Constraint en `SgiActionLineReview` (`sgi_management_review.py`): el tipo «acuerdo» solo con `review_id` (una NC, riesgo o incidente no lo usa). Al revés no: una acción de revisión puede seguir siendo «Corrección» (la prueba `test_nc_auditoria_evidencia.test_14` las crea así).
- **El acuerdo no pide evidencia** al terminarse (como la corrección; Q4). E1-02 (`acuerdos_rxd`) no cambia: mide el **acuerdo** (`sgi.management.review.agreement`), no el tipo de la acción.
- **0 acciones de revisión en producción:** nada que reclasificar, sin migración.
- **Acuerdos abiertos pasan a la siguiente revisión, sin bloquear el cierre** (la auditoría proponía «avisar o pasar»): campo `carried_agreement_ids` (Many2many a `sgi.management.review.agreement`, solo lectura) «Acuerdos abiertos de revisiones anteriores». `action_load_inputs` lo llena con `_sgi_open_previous_agreements()`: acuerdos de **cualquier** revisión anterior (`review_id.date <= date`, otra revisión) en «Realizada» o «Cerrada» que no están cumplidos (`not is_done and not done_date`, el mismo criterio de E1-02 para los capturados a mano). El acuerdo **sigue siendo de su revisión** (su indicador y su acción no cambian); la revisión nueva solo lo muestra y el acta lo imprime. `action_close` deja en el chatter «Acuerdos abiertos al cerrar: pasan a la siguiente revisión por la dirección…» con la lista (texto escapado con `Markup %`, K-07).
- Pestaña «Acuerdos (salidas)»: lista nueva de solo lectura debajo de los acuerdos propios.

### 1.6 Conclusiones 9.3.3 obligatorias (N-09)

Hoy: no hay campos para las conclusiones (9001 9.3.3; 14001 y 45001 9.3 «conclusiones sobre la conveniencia, adecuación y eficacia continuas»).

Decisión:
- Cuatro `Text` en la pestaña nueva **«Conclusiones (9.3.3)»**: `conclusion_suitability` («Conveniencia»), `conclusion_adequacy` («Adecuación»), `conclusion_effectiveness` («Eficacia»), `output_needs` («Mejora, cambios y recursos»: 9.3.3 a, b y c). Sin `tracking` (Text).
- **«Marcar realizada» los pide los cuatro** (no vacíos ni solo espacios), después de la regla que ya existe de «al menos un acuerdo con responsable y fecha»; «Sin cambios» es una respuesta válida y la ayuda lo dice (Q5). **Solo en el cambio de estado** (`action_mark_done`, el único camino de la interfaz: la barra de estado no es clicable): no se revisa en `write`, así que las pruebas que crean revisiones ya «Realizada» o «Cerrada» con `create`/`write` no cambian. Sin excepción para el superusuario (ningún proceso del sistema marca revisiones realizadas).
- Ajustes: `test_mgmt_review.test_04` y `test_pr5_direction.test_02` capturan las conclusiones antes de `action_mark_done` (`test_mgmt_review.test_03` sigue fallando por falta de acuerdos).
- Fuera: sugerencia de conclusiones con IA (ficha 57.100.0, Q16).

### 1.7 Requisitos de cliente (puerta Q13)

Hoy viven en dos lugares: la norma «CLIENTES» con CLI-01…CLI-10 (solo producción, sin xmlid) y `sgi.legal.requirement` con `kind = 'cliente'` (2 registros). **Por omisión (lo mínimo): esta entrega no cambia nada de eso.** La NC puede citar una CLI-xx como cláusula (el candado nuevo acepta cualquier cláusula); la evaluación del cumplimiento de un requisito de cliente sigue en lo legal. Unificar es una decisión de Dirección y, si se decide, una entrega aparte (crear xmlid a las CLI con migración, A-029, o pasar los 2 legales a CLI). Pregunta Q1.

### 1.8 Fuera de esta entrega (anotado)

- Ligar la cláusula a documentos, riesgos, requisitos legales, aspectos e indicadores (resto de N-05).
- Cargar entradas también al marcar realizada (C-13), lo legal por registro en la revisión (H-B8.2).
- Las 85 actividades sin cláusula y ligar las 13 nuevas a actividades: pista de datos.
- Sugerencia de cláusula y clasificación con IA (57.100.0).

### 1.9 Lo que no se pudo verificar fuera de Odoo.sh

- VERIFICAR: que en Odoo 19 un XML `noupdate="1"` **nuevo** en un módulo ya instalado crea sus registros en el `-u` (convert.py: `noupdate` y modo distinto de `init` → si el xmlid no existe, crea). La prueba 01 lo confirma en el build (la base es copia de producción, que no los tiene).
- VERIFICAR: que el pre-migrate corre antes de cargar los datos (siempre es así en `pre-migrate`), y el `INSERT` en `ir_model_data` con estas columnas. **Verificado por MCP:** `ir.model.data` tiene `module`, `name`, `model`, `res_id`, `noupdate`, `create_uid/date`, `write_uid/date` y `studio` (opcional, de Studio).
- VERIFICAR: `self.env.registry.clear_cache()` (Odoo 17+) limpia la caché de `_xmlid_lookup` dentro de la prueba 03; si el nombre cambió en 19, usar `self.env['ir.model.data'].clear_caches()` o `self.registry.clear_cache('default')`.
- **Verificado por MCP:** `project.task.date_last_stage_update` (datetime) y `sgi_is_improvement`; `stock.scrap.company_id` (requerido), `date_done`, `state` (`draft`/`done`), `scrap_reason_tag_ids`; xmlid `quimibond_sgi.sgi_project_improvement` (proyecto 453), `sgi_nc_int_stage_open` (5), `sgi_nc_int_stage_followup` (6), `c_9001_102` (27) en producción.
- Verificado en el repo: `sgi.env.aspect.significant`/`level`/`score` guardados (`sgi_env_aspect.py:110-116`), `company_id` requerido (l.72); `sgi.risk.high_without_action` guardado (l.186); `sgi.work.permit.expired` guardado (l.111); `SgiActionLineReview` ya define `review_id` y `_check_parent_xor` (`sgi_management_review.py:455-492`); el estado de la revisión no es clicable en la ficha (`views/sgi_management_review_views.xml:26`).

---

## 2. Archivos

- Create: `addons/quimibond_sgi/data/sgi_norms_tercer_nivel.xml`
- Create: `addons/quimibond_sgi/migrations/19.0.57.97.0/pre-migrate.py`
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`QualityAlert`: help de `sgi_classification` y `sgi_norm_clause_id`, `write`, `_sgi_leaving_open`, `_sgi_check_classified`; `SgiActionLine.action_type`)
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (import `Markup`; campos; `action_load_inputs`; cuatro cargadores y dos helpers; `_sgi_scrap_domain`, `_sgi_load_env`; `_sgi_open_previous_agreements`; `_sgi_check_conclusions`; `action_mark_done`; `action_close`; constraint en `SgiActionLineReview`)
- Modify: `addons/quimibond_sgi/models/sgi_diagram_iso.py` (`_data_management_review`, lista `inputs`)
- Modify: `addons/quimibond_sgi/views/sgi_management_review_views.xml` (ficha y ayuda de la acción), `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` (aviso de `sgi_quality_alert_view_form`), `addons/quimibond_sgi/report/report_mgmt_review.xml`
- Modify: `addons/quimibond_sgi/__manifest__.py` (versión; `data/sgi_norms_tercer_nivel.xml` después de `data/sgi_norms.xml`)
- Create: `addons/quimibond_sgi/tests/test_clausulas_revision.py`; Modify: `tests/__init__.py`, `tests/test_mgmt_review.py` (`test_04`), `tests/test_pr5_direction.py` (`test_02`), `tests/test_candados_evidencia.py` (`_closable_nc`), `tests/test_nc_deadlines.py` (`test_03`)
- Modify: `docs/sgi/usuarios/direccion.md`, `docs/sgi/usuarios/mast.md`, `docs/sgi/usuarios/jefe-de-area.md`, `docs/sgi/usuarios/auditor.md`, `docs/sgi/administracion/manual-jefe-mast.md`, `addons/quimibond_sgi/README.md` (A-029)
- Modify: `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), `docs/audit/decisiones.md` (cuando Jose conteste Q1–Q8)

---

## Task 7.0: Rama

- [x] **Step 1:** seguir en `claude/confident-mendel-8yg7xu` (igual a `origin/main`). Si se trabaja fuera de esa sesión: `git fetch origin main && git checkout -B claude/sgi-57-97-clausulas-revision origin/main`. Confirmar `grep -n "'version'" addons/quimibond_sgi/__manifest__.py` → `19.0.57.96.0`.

## Task 7.1: Pruebas nuevas y ajuste de las existentes (fallan antes del código)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_clausulas_revision.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py` (al final)
- Modify: `tests/test_mgmt_review.py` (`test_04`), `tests/test_pr5_direction.py` (`test_02`), `tests/test_candados_evidencia.py` (`_closable_nc`), `tests/test_nc_deadlines.py` (`test_03`)

- [x] **Step 1: Escribir `tests/test_clausulas_revision.py`**

```python
# -*- coding: utf-8 -*-
"""57.97.0 (auditoría 2026-10: N-05 y N-09): cláusulas y revisión por la
dirección.

- Cláusulas de tercer nivel (6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4,
  9.1.2) como datos con xmlid; las de normas sin xmlid (CLI-xx) no se tocan;
  el pre-migrate solo liga el xmlid a una cláusula que ya exista con ese
  numeral en la misma norma.
- Una NC con folio no sale de Abierta hacia Seguimiento (ni directo a Cerrada)
  sin clasificación y cláusula; solo en ese cambio de etapa y después de
  escribir; no atrapa lo que ya está en Seguimiento o cerrado.
- Revisión por la dirección: entradas de incidentes, contexto, aspectos y
  mejoras (solo conteos donde hay personas); scrap de la empresa del SGI;
  acuerdos del tipo «acuerdo»; conclusiones 9.3.3 para marcarla realizada;
  los acuerdos abiertos pasan a la siguiente revisión."""
import importlib.util
import os
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PRE_MIGRATE = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.97.0', 'pre-migrate.py')

CONCLUSIONS = {
    'conclusion_suitability': 'Sigue siendo conveniente para el contexto actual.',
    'conclusion_adequacy': 'Cubre los procesos y requisitos vigentes.',
    'conclusion_effectiveness': 'Logra los resultados previstos, salvo los rojos con plan.',
    'output_needs': 'Sin cambios al SGI; un medidor de energía como recurso.',
}


def _pre_migrate():
    """El pre-migrate de 57.97.0 (su tabla NEW_CLAUSES es la única fuente de
    las cláusulas nuevas). Se carga dentro de la prueba: un archivo que falta
    no debe romper la carga de todo el paquete de pruebas."""
    spec = importlib.util.spec_from_file_location('sgi_mig_57_97_0', _PRE_MIGRATE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Case(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.today = fields.Date.context_today(env.user)
        cls.mast = sgi_set_mast(env, login='zcl_mast')
        cls.sgi_user = new_test_user(
            env, login='zcl_usuario', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.director = new_test_user(
            env, login='zcl_direccion', groups='base.group_user,quimibond_sgi.group_sgi_director')

    def _locked(self, text, method, *args, **kwargs):
        """Candado (UserError, no permiso) cuyo mensaje dice ``text``. El
        ``assertRaises`` de Odoo abre un savepoint: el candado que se revisa
        después de escribir no deja la etapa escrita."""
        with self.assertRaises(UserError) as caught:
            method(*args, **kwargs)
        self.assertNotIsInstance(caught.exception, AccessError, str(caught.exception))
        self.assertIn(text, str(caught.exception))


@tagged('post_install', '-at_install')
class TestClausulasTercerNivel(_Case):

    def _new(self):
        return _pre_migrate().NEW_CLAUSES

    def test_01_clausulas_nuevas_con_xmlid(self):
        rows = self._new()
        self.assertEqual(len(rows), 13)
        Clause = self.env['sgi.norm.clause']
        for name, norm_xmlid, code, label in rows:
            clause = self.env.ref('quimibond_sgi.' + name)
            self.assertEqual(clause.norm_id, self.env.ref('quimibond_sgi.' + norm_xmlid), name)
            self.assertEqual(clause.code, code, name)
            self.assertEqual(Clause.search_count(
                [('norm_id', '=', clause.norm_id.id), ('code', '=', code)]), 1,
                "%s: una sola cláusula con ese numeral en su norma." % name)
            self.assertEqual(Clause._sgi_find(clause.short_label), clause, name)
        jerarquia = self.env.ref('quimibond_sgi.' + 'c_45001_8_1_2')
        self.assertEqual(jerarquia.short_label, '45001 8.1.2')

    def test_02_las_normas_sin_xmlid_no_se_tocan(self):
        norms = {self.env.ref('quimibond_sgi.' + n) for n in
                 ('sgi_norm_9001', 'sgi_norm_14001', 'sgi_norm_45001')}
        for _name, norm_xmlid, code, _label in self._new():
            self.assertIn(self.env.ref('quimibond_sgi.' + norm_xmlid), norms)
            self.assertFalse(code.upper().startswith('CLI'))
        # Ningún xmlid del módulo apunta a una cláusula de otra norma (CLI-xx, A-029).
        bound = self.env['ir.model.data'].search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'sgi.norm.clause')])
        clauses = self.env['sgi.norm.clause'].browse(bound.mapped('res_id')).exists()
        self.assertFalse(clauses.filtered(lambda c: c.norm_id not in norms))

    def test_03_pre_migrate_solo_liga_el_xmlid(self):
        mig = _pre_migrate()
        IMD = self.env['ir.model.data']
        Clause = self.env['sgi.norm.clause']
        # Una cláusula capturada a mano con el mismo numeral (sin xmlid):
        # el pre-migrate le liga el xmlid y no crea otra.
        manual = self.env.ref('quimibond_sgi.' + 'c_45001_8_1_3')
        IMD.search([('module', '=', 'quimibond_sgi'), ('name', '=', 'c_45001_8_1_3')]).unlink()
        # Una cláusula nueva que nadie capturó: no hay qué ligar (el XML la crea).
        missing = self.env.ref('quimibond_sgi.' + 'c_9001_7_1_5')
        IMD.search([('module', '=', 'quimibond_sgi'), ('name', '=', 'c_9001_7_1_5')]).unlink()
        missing.code = 'Z7.1.5'
        self.env.flush_all()
        self.env.registry.clear_cache()  # VERIFICAR (1.9)
        before = Clause.search_count([])
        name_before = manual.name
        # Sin assertLogs: el logger del archivo cargado con importlib se llama
        # «sgi_mig_57_97_0»; el script solo registra INFO.
        mig.migrate(self.env.cr, '19.0.57.96.0')
        self.env.invalidate_all()
        self.env.registry.clear_cache()
        self.assertEqual(self.env.ref('quimibond_sgi.' + 'c_45001_8_1_3'), manual)
        self.assertEqual(manual.name, name_before, "Solo metadato: el registro no cambia.")
        self.assertFalse(self.env.ref('quimibond_sgi.' + 'c_9001_7_1_5', raise_if_not_found=False))
        self.assertEqual(Clause.search_count([]), before, "No crea cláusulas.")
        # Idempotente: una segunda corrida no agrega filas.
        mig.migrate(self.env.cr, '19.0.57.96.0')
        self.assertEqual(IMD.search_count(
            [('module', '=', 'quimibond_sgi'), ('name', '=', 'c_45001_8_1_3')]), 1)
        # Instalación nueva (sin versión): no hace nada.
        mig.migrate(self.env.cr, None)

    def test_04_la_matriz_muestra_el_tercer_nivel(self):
        norm = self.env.ref('quimibond_sgi.sgi_norm_45001')
        rows = [row['clause'] for row in norm._sgi_compliance_matrix()['rows']]
        codes = [c.code for c in rows]
        self.assertIn('8.1.2', codes)
        self.assertLess(codes.index('8.1'), codes.index('8.1.2'))
        self.assertLess(codes.index('8.1.4'), codes.index('8.2'))


@tagged('post_install', '-at_install')
class TestNcClasificacion(_Case):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.owner_user = new_test_user(
            env, login='zcl_dueno', groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner = env['hr.employee'].create({'name': 'ZCL Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create(
            {'code': 'ZCL', 'name': 'Proceso 57.97', 'owner_id': owner.id})
        cls.team = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.stage_follow = env.ref('quimibond_sgi.sgi_nc_int_stage_followup')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.clause = env.ref('quimibond_sgi.c_9001_102')

    def _nc(self, **vals):
        return self.env['quality.alert'].create(dict({
            'title': 'ZCL NC', 'team_id': self.team.id, 'stage_id': self.stage_open.id,
            'sgi_process_id': self.process.id}, **vals))

    def test_05_no_pasa_a_seguimiento_sin_clasificacion_ni_clausula(self):
        nc = self._nc()
        as_owner = nc.with_user(self.owner_user)
        self._locked("clasificación", as_owner.write, {'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_open)
        as_owner.write({'sgi_classification': 'menor'})
        self._locked("cláusula", as_owner.write, {'stage_id': self.stage_follow.id})
        # El formulario manda la cláusula y la etapa en un solo write: cuenta.
        as_owner.write({'sgi_norm_clause_id': self.clause.id, 'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)
        # Nacer en Seguimiento (alta rápida en esa columna del kanban) también
        # es pasar a Seguimiento: se revisa antes de crear.
        Alert = self.env['quality.alert'].with_user(self.owner_user)
        self._locked("Seguimiento", Alert.with_context(default_stage_id=self.stage_follow.id).create,
                     {'title': 'ZCL NC rápida', 'team_id': self.team.id,
                      'sgi_process_id': self.process.id})
        self.assertFalse(self.env['quality.alert'].search([('title', '=', 'ZCL NC rápida')]))

    def test_06_tampoco_directo_a_cerrada_y_el_cierre_forzado_si(self):
        nc = self._nc()
        self._locked("clasificación", nc.with_user(self.mast).write,
                     {'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_open)
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Registro duplicado de otra NC'}).action_confirm()
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_07_no_atrapa_lo_existente(self):
        # Editar una NC abierta sin clasificación sigue permitido.
        nc = self._nc()
        nc.with_user(self.owner_user).write({'sgi_deviation': 'Desviación ZCL'})
        # Reabrir una cerrada (D-009) no pide clasificación.
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Cierre de prueba ZCL'}).action_confirm()
        nc.with_user(self.owner_user).write({'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)
        # El sistema (superusuario) no pasa por el candado.
        other = self._nc()
        other.write({'stage_id': self.stage_follow.id})
        self.assertEqual(other.stage_id, self.stage_follow)


@tagged('post_install', '-at_install')
class TestRevisionPorLaDireccion(_Case):

    def _review(self, **vals):
        return self.env['sgi.management.review'].create(dict({
            'date': date(2051, 7, 1), 'period_from': date(2051, 1, 1),
            'period_to': date(2051, 6, 30)}, **vals))

    def _agreement(self, review, name, responsible=None, deadline=date(2051, 9, 30)):
        return self.env['sgi.management.review.agreement'].create({
            'review_id': review.id, 'name': name,
            'responsible_id': (responsible or self.sgi_user).id, 'deadline': deadline})

    def test_08_entradas_nuevas(self):
        review = self._review(date=self.today, period_from=self.today - timedelta(days=10),
                              period_to=self.today)
        self.env['sgi.incident'].create({
            'name': 'ZRD Golpe en la cortadora', 'incident_type': 'lesion', 'severity': 'moderado',
            'days_lost': 4, 'date': fields.Datetime.now() - timedelta(days=1)})
        self.env['sgi.interested.party'].create({
            'name': 'ZRD Vecinos del parque', 'party_type': 'externa', 'category': 'comunidad',
            'needs': 'Sin ruido de noche'})
        self.env['sgi.risk'].create({'name': 'ZRD Fortaleza nueva', 'instrument': 'foda',
                                     'foda_type': 'fortaleza'})
        self.env['sgi.risk'].create({'name': 'ZRD Oportunidad de reúso de agua',
                                     'instrument': 'ryo', 'kind': 'oportunidad'})
        process = self.env['sgi.process'].create({'code': 'ZRD', 'name': 'Proceso revisión'})
        aspect = self.env['sgi.env.aspect'].create({
            'company_id': self.env['sgi.config']._sgi_company().id,
            'process_id': process.id, 'activity': 'Lavado ZRD', 'name': 'Descarga ZRD',
            'impact': 'Contaminación del agua', 'aspect_type': 'descarga', 'severity': '5',
            'frequency': '5', 'life_cycle_stage': 'proceso'})
        self.assertTrue(aspect.significant)
        project = self.env.ref('quimibond_sgi.sgi_project_improvement')
        self.env['project.task'].create({'name': 'ZRD Mejora del secado', 'project_id': project.id})
        review.action_load_inputs()
        self.assertIn('Lesión / accidente', review.incidents_summary)
        self.assertIn('Días perdidos', review.incidents_summary)
        self.assertNotIn('ZRD Golpe', review.incidents_summary, "Solo conteos: sin títulos.")
        self.assertIn('ZRD Vecinos del parque', review.context_summary)
        self.assertIn('ZRD Fortaleza nueva', review.context_summary)
        self.assertIn(aspect.folio, review.env_aspects_summary)
        self.assertIn('ZRD Mejora del secado', review.improvement_summary)
        self.assertIn('ZRD Oportunidad de reúso de agua', review.improvement_summary)
        # Dirección los corre sin permisos de SST, Inventario ni Proyecto.
        as_director = review.with_user(self.director)
        for method in ('_sgi_load_incidents', '_sgi_load_context', '_sgi_load_env_aspects',
                       '_sgi_load_improvements', '_sgi_load_env'):
            self.assertTrue(getattr(as_director, method)(), method)

    def test_09_scrap_de_la_empresa_del_sgi(self):
        review = self._review()
        company = self.env['sgi.config']._sgi_company()
        self.assertIn(('company_id', '=', company.id), review._sgi_scrap_domain())
        self.assertIn(company.name, review._sgi_load_env())

    def test_10_acuerdo_es_su_propio_tipo(self):
        review = self._review()
        review.write(CONCLUSIONS)
        agreement = self._agreement(review, 'ZRD Comprar el medidor de energía')
        review.action_mark_done()
        line = agreement.action_line_id
        self.assertEqual(line.action_type, 'acuerdo')
        # Se termina sin la evidencia que pide una correctiva.
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)
        # Fuera de una revisión, «acuerdo» no se usa.
        risk = self.env['sgi.risk'].create({'name': 'ZRD Riesgo', 'instrument': 'ryo'})
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'risk_id': risk.id, 'action_type': 'acuerdo', 'name': 'ZRD mal tipo',
                'responsible_id': self.sgi_user.id, 'date_commit': self.today})

    def test_11_conclusiones_para_marcar_realizada(self):
        review = self._review()
        self._agreement(review, 'ZRD Acuerdo')
        self._locked("conclusiones", review.with_user(self.mast).action_mark_done)
        review.write(dict(CONCLUSIONS, conclusion_adequacy='   '))
        self._locked("Adecuación", review.with_user(self.mast).action_mark_done)
        self.assertEqual(review.state, 'borrador')
        review.write(CONCLUSIONS)
        review.with_user(self.mast).action_mark_done()
        self.assertEqual(review.state, 'realizada')

    def test_12_acuerdos_abiertos_pasan_a_la_siguiente(self):
        first = self._review()
        first.write(CONCLUSIONS)
        open_agr = self._agreement(first, 'ZRD Acuerdo abierto')
        done_agr = self._agreement(first, 'ZRD Acuerdo cumplido')
        first.action_mark_done()
        # Fecha de hoy: una acción no se termina en el futuro (_sgi_check_done).
        done_agr.action_line_id.write({'date_done': self.today})
        # Una revisión en borrador con acuerdo: no cuenta (no se realizó).
        draft = self._review(date=date(2051, 8, 1))
        draft_agr = self._agreement(draft, 'ZRD Acuerdo de borrador')
        first.with_user(self.mast).action_close()
        self.assertEqual(first.state, 'cerrada', "Cerrar no se bloquea.")
        self.assertTrue(first.message_ids.filtered(
            lambda m: 'ZRD Acuerdo abierto' in (m.body or '')
            and 'siguiente revisión' in (m.body or '')))
        second = self._review(date=date(2052, 1, 10), period_from=date(2051, 7, 1),
                              period_to=date(2051, 12, 31))
        second.action_load_inputs()
        self.assertIn(open_agr, second.carried_agreement_ids)
        self.assertNotIn(done_agr, second.carried_agreement_ids)
        self.assertNotIn(draft_agr, second.carried_agreement_ids)
        self.assertEqual(open_agr.review_id, first, "El acuerdo sigue siendo de su revisión.")
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_mgmt_review_document', second.ids)[0].decode()
        for text in ('Acuerdos abiertos de revisiones anteriores', 'ZRD Acuerdo abierto',
                     '15. Incidentes y desempeño de SST', 'Conclusiones (9.3.3)'):
            self.assertIn(text, html)
```

(Notas: el aspecto de `test_08` va con severidad y frecuencia 5 (puntaje 25, el máximo): el cargador lista solo 15 significativos por `significant desc, score desc, folio desc`, y con más aspectos en la copia de producción uno de puntaje menor quedaría fuera de la lista; con 25 y el folio más nuevo sale primero. `test_08` usa el periodo de los últimos 10 días porque partes interesadas y FODA se cuentan por `create_date`; las asserciones son por los nombres `ZRD`, nunca por totales. `2051` deja las demás pruebas fuera de los datos reales. La prueba 03 borra y restaura metadato dentro de la transacción de la prueba; se deshace al final.)

- [x] **Step 2: Ajustar pruebas existentes**

  a. `tests/test_mgmt_review.py`, `test_04_agreements_create_tasks` (l.~55-70): antes de `review.action_mark_done()` agregar

  ```python
        # 57.97.0 (N-09): las conclusiones 9.3.3 son obligatorias para marcarla realizada.
        review.write({'conclusion_suitability': 'Conveniente', 'conclusion_adequacy': 'Adecuado',
                      'conclusion_effectiveness': 'Eficaz', 'output_needs': 'Sin cambios'})
  ```

  b. `tests/test_pr5_direction.py`, `test_02_dir3_acuerdos_como_acciones_e_informe` (l.~69-80): el mismo bloque antes de `review.action_mark_done()`.

  c. `tests/test_candados_evidencia.py`, `_closable_nc` (l.64-81): agregar a los valores del `create`

  ```python
            # 57.97.0 (N-05): de Abierta a Cerrada pide clasificación y cláusula.
            'sgi_classification': 'menor',
            'sgi_norm_clause_id': self.env.ref('quimibond_sgi.c_9001_102').id,
  ```

  (La reincidencia sigue: con la misma cláusula, `sgi_recurrence_count` cuenta 2 por NC previa en vez de 1, y `sgi_is_recurrent` sigue en `True`; ninguna prueba del archivo mira el número.)

  d. `tests/test_nc_deadlines.py`, `test_03_reclamacion_no_avanza_sin_contencion` (l.103-114): `nc = self._nc(sgi_origin_type='reclamacion')` → `nc = self._nc(sgi_origin_type='reclamacion', **self._classified())` y `other = self._nc()` → `other = self._nc(**self._classified())`, con el helper en la clase:

  ```python
    def _classified(self):
        """57.97.0 (N-05): pasar de Abierta a Seguimiento pide clasificación y cláusula."""
        return {'sgi_classification': 'menor',
                'sgi_norm_clause_id': self.env.ref('quimibond_sgi.c_9001_102').id}
  ```

  (No se cambia el `_nc` común: otras pruebas del archivo miran plazos y reincidencia.)

- [x] **Step 3: Registrar** al final de `tests/__init__.py`:

```python
from . import test_clausulas_revision
```

- [ ] **Step 4: Push solo de las pruebas y verlas fallar**

```bash
python3 tools/check_addons.py --base-ref origin/main
flake8 addons/quimibond_sgi/tests
git add addons/quimibond_sgi/tests
git commit -m "quimibond_sgi: pruebas de cláusulas y revisión por la dirección (fallan antes del código)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

`check_addons` avisará «archivos cambiados sin bump» (advertencia; la versión sube en la Task 7.10) y **no** debe dar error de referencias (la prueba no tiene `env.ref` literal a los xmlid nuevos). Esperado en el log: `TestClausulasTercerNivel` con **error** (no existe el pre-migrate: `FileNotFoundError`); `test_05` y `test_06` fallan (la NC pasa sin candado); `test_07` pasa; `test_08`–`test_12` con error (campos y métodos que no existen); `test_mgmt_review.test_04` y `test_pr5_direction.test_02` con error (campos de conclusiones). `test_candados_evidencia` y `test_nc_deadlines` siguen pasando.

## Task 7.2: Cláusulas de tercer nivel y pre-migrate (N-05)

**Files:**
- Create: `addons/quimibond_sgi/migrations/19.0.57.97.0/pre-migrate.py`
- Create: `addons/quimibond_sgi/data/sgi_norms_tercer_nivel.xml`
- Modify: `addons/quimibond_sgi/__manifest__.py` (después de `'data/sgi_norms.xml',`, l.60)

- [x] **Step 1: `migrations/19.0.57.97.0/pre-migrate.py`**

```python
# -*- coding: utf-8 -*-
"""57.97.0 (N-05): cláusulas de tercer nivel con xmlid.

``data/sgi_norms_tercer_nivel.xml`` (noupdate) crea 13 cláusulas nuevas en
ISO 9001, 14001 y 45001. ``sgi.norm.clause`` no tiene restricción única de
norma + numeral: si el Jefe MAST ya capturó a mano una cláusula con el mismo
numeral en la misma norma, el XML crearía otra igual. Antes de cargar los
datos, este script solo LIGA el xmlid a la que ya existe (una fila nueva en
``ir_model_data``, ``noupdate``): es metadato, no dato de negocio; la
cláusula conserva su nombre, sus actividades, NC y hallazgos.

- Sin cláusula con ese numeral en esa norma: no hace nada (el XML la crea).
- Con varias: liga la de id menor y avisa (WARNING) de las demás.
- xmlid ya existente: no hace nada (idempotente).
- Las normas sin xmlid (requisitos de clientes CLI-xx, ISO 31000) no se tocan
  (A-029). En producción, el 2026-10-02, ninguna de las 13 existía.

``NEW_CLAUSES`` es la única tabla de las cláusulas nuevas: la lee también
``tests/test_clausulas_revision.py``. Debe coincidir con el XML."""
import logging

_logger = logging.getLogger(__name__)

MODULE = 'quimibond_sgi'

# (xmlid, xmlid de la norma, numeral, requisito)
NEW_CLAUSES = [
    ('c_9001_7_1_5', 'sgi_norm_9001', '7.1.5', "Recursos de seguimiento y medición"),
    ('c_9001_9_1_2', 'sgi_norm_9001', '9.1.2', "Satisfacción del cliente"),
    ('c_14001_6_1_2', 'sgi_norm_14001', '6.1.2', "Aspectos ambientales"),
    ('c_14001_6_1_3', 'sgi_norm_14001', '6.1.3', "Requisitos legales y otros requisitos"),
    ('c_14001_6_1_4', 'sgi_norm_14001', '6.1.4', "Planificación de acciones"),
    ('c_14001_9_1_2', 'sgi_norm_14001', '9.1.2', "Evaluación del cumplimiento"),
    ('c_45001_6_1_2', 'sgi_norm_45001', '6.1.2',
     "Identificación de peligros y evaluación de los riesgos y oportunidades"),
    ('c_45001_6_1_3', 'sgi_norm_45001', '6.1.3',
     "Determinación de los requisitos legales y otros requisitos"),
    ('c_45001_6_1_4', 'sgi_norm_45001', '6.1.4', "Planificación de acciones"),
    ('c_45001_8_1_2', 'sgi_norm_45001', '8.1.2',
     "Eliminar peligros y reducir riesgos para la SST (jerarquía de controles)"),
    ('c_45001_8_1_3', 'sgi_norm_45001', '8.1.3', "Gestión del cambio"),
    ('c_45001_8_1_4', 'sgi_norm_45001', '8.1.4', "Compras (contratistas y contratación externa)"),
    ('c_45001_9_1_2', 'sgi_norm_45001', '9.1.2', "Evaluación del cumplimiento"),
]


def _res_id(cr, name, model):
    cr.execute("SELECT res_id FROM ir_model_data WHERE module = %s AND name = %s AND model = %s",
               (MODULE, name, model))
    row = cr.fetchone()
    return row[0] if row else None


def migrate(cr, version):
    if not version:
        return
    bound = []
    for name, norm_xmlid, code, _label in NEW_CLAUSES:
        if _res_id(cr, name, 'sgi.norm.clause'):
            continue
        norm_id = _res_id(cr, norm_xmlid, 'sgi.norm')
        if not norm_id:
            continue
        cr.execute("SELECT id FROM sgi_norm_clause WHERE norm_id = %s AND trim(code) = %s ORDER BY id",
                   (norm_id, code))
        ids = [row[0] for row in cr.fetchall()]
        if not ids:
            continue
        cr.execute("""
            INSERT INTO ir_model_data (module, name, model, res_id, noupdate,
                                       create_uid, write_uid, create_date, write_date)
            VALUES (%s, %s, 'sgi.norm.clause', %s, true, 1, 1,
                    now() at time zone 'UTC', now() at time zone 'UTC')
            ON CONFLICT DO NOTHING
        """, (MODULE, name, ids[0]))
        bound.append("%s → %s" % (name, ids[0]))
        if len(ids) > 1:
            _logger.warning(
                "SGI 57.97.0: la norma %s tiene %d cláusulas con el numeral %s (ids %s); se ligó "
                "la %s a %s.%s. Revise y una las demás a mano.",
                norm_xmlid, len(ids), code, ids, ids[0], MODULE, name)
    _logger.info("SGI 57.97.0: %d cláusula(s) existente(s) ligadas a su xmlid nuevo: %s",
                 len(bound), ", ".join(bound) or "ninguna")
```

- [x] **Step 2: `data/sgi_norms_tercer_nivel.xml`** (mismo formato de una línea por cláusula que `sgi_norms.xml`; el nombre de cada `record` y su numeral **iguales** a `NEW_CLAUSES`)

```xml
<?xml version="1.0" encoding="utf-8"?>
<!-- 57.97.0 (N-05): cláusulas de tercer nivel. noupdate="1" como sgi_norms.xml:
     en un -u se crean las que no existen y no se tocan las que el Jefe MAST
     edite después. Si ya había una capturada a mano con el mismo numeral, el
     pre-migrate de 57.97.0 le liga el xmlid y aquí no se duplica. Solo normas
     con xmlid: la de requisitos de clientes (CLI-xx) no se toca (A-029). -->
<odoo noupdate="1">
    <record id="c_9001_7_1_5" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_9001"/><field name="code">7.1.5</field><field name="name">Recursos de seguimiento y medición</field></record>
    <record id="c_9001_9_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_9001"/><field name="code">9.1.2</field><field name="name">Satisfacción del cliente</field></record>

    <record id="c_14001_6_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_14001"/><field name="code">6.1.2</field><field name="name">Aspectos ambientales</field></record>
    <record id="c_14001_6_1_3" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_14001"/><field name="code">6.1.3</field><field name="name">Requisitos legales y otros requisitos</field></record>
    <record id="c_14001_6_1_4" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_14001"/><field name="code">6.1.4</field><field name="name">Planificación de acciones</field></record>
    <record id="c_14001_9_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_14001"/><field name="code">9.1.2</field><field name="name">Evaluación del cumplimiento</field></record>

    <record id="c_45001_6_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">6.1.2</field><field name="name">Identificación de peligros y evaluación de los riesgos y oportunidades</field></record>
    <record id="c_45001_6_1_3" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">6.1.3</field><field name="name">Determinación de los requisitos legales y otros requisitos</field></record>
    <record id="c_45001_6_1_4" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">6.1.4</field><field name="name">Planificación de acciones</field></record>
    <record id="c_45001_8_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">8.1.2</field><field name="name">Eliminar peligros y reducir riesgos para la SST (jerarquía de controles)</field></record>
    <record id="c_45001_8_1_3" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">8.1.3</field><field name="name">Gestión del cambio</field></record>
    <record id="c_45001_8_1_4" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">8.1.4</field><field name="name">Compras (contratistas y contratación externa)</field></record>
    <record id="c_45001_9_1_2" model="sgi.norm.clause"><field name="norm_id" ref="sgi_norm_45001"/><field name="code">9.1.2</field><field name="name">Evaluación del cumplimiento</field></record>
</odoo>
```

- [x] **Step 3: Manifest**, después de `'data/sgi_norms.xml',`:

```python
        'data/sgi_norms_tercer_nivel.xml',  # 57.97.0 (N-05): 13 cláusulas con xmlid
```

- [x] **Step 4: Comprobar que la tabla y el XML coinciden** (local, sin Odoo):

```bash
python3 - <<'EOF'
import importlib.util, xml.etree.ElementTree as ET
spec = importlib.util.spec_from_file_location('m', 'addons/quimibond_sgi/migrations/19.0.57.97.0/pre-migrate.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
xml = {(r.get('id'), r.find("field[@name='norm_id']").get('ref'), r.find("field[@name='code']").text, r.find("field[@name='name']").text)
       for r in ET.parse('addons/quimibond_sgi/data/sgi_norms_tercer_nivel.xml').getroot().iter('record')}
assert xml == set(m.NEW_CLAUSES), xml ^ set(m.NEW_CLAUSES)
print("OK", len(xml))
EOF
```

Esperado: `OK 13`.

- [x] **Step 5: Checadores y commit**

```bash
python3 tools/check_addons.py --base-ref origin/main
git add addons/quimibond_sgi/migrations/19.0.57.97.0 addons/quimibond_sgi/data/sgi_norms_tercer_nivel.xml addons/quimibond_sgi/__manifest__.py
git commit -m "quimibond_sgi: cláusulas de tercer nivel con xmlid; el pre-migrate solo liga un numeral existente (N-05)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

(El pre-migrate corre en el build de la rama solo si el número de versión sube; hasta la Task 7.10 el build carga el XML con el manifest viejo y la prueba 03 llama el script directamente.)

## Task 7.3: NC con clasificación y cláusula al salir de Abierta (N-05)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`sgi_classification` l.114-120, `sgi_norm_clause_id` l.121-123, `create` l.290-341, `write` l.843-917, métodos nuevos después de `_sgi_user_can_close` l.619-626)
- Modify: `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` (aviso «NC del SGI» de `sgi_quality_alert_view_form`, l.32-38)

- [x] **Step 1: `help` de los dos campos** (texto final):

```python
        help="Mayor, menor u observación. Obligatoria para pasar la NC de Abierta a Seguimiento. Una NC "
             "mayor manda un correo crítico al abrirse y exige aplicar la lección aprendida antes de cerrar.")
```

```python
                                         help="Cláusula de la norma que se incumplió (también las de "
                                              "requisitos de clientes). Obligatoria para pasar la NC de "
                                              "Abierta a Seguimiento.")
```

- [x] **Step 2: Métodos nuevos**, después de `_sgi_user_can_close`:

```python
    def _sgi_leaving_open(self, new_stage):
        """57.97.0 (N-05): las NC con folio que salen de Abierta (o de ninguna
        etapa, o de una etapa genérica) hacia Seguimiento o hacia una etapa de
        cierre. Se arma con la
        etapa de ANTES del write. Reabrir una cerrada, el «No eficaz» y lo que
        ya está en Seguimiento no entran: el candado no es retroactivo."""
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        if not ((followup and new_stage == followup) or new_stage.sgi_is_closing_stage):
            return self.browse()
        # «Antes de Seguimiento»: sin etapa, Abierta o una etapa genérica de
        # Calidad (base nueva); no Seguimiento, cierre ni cancelación.
        return self.filtered(lambda a: a.sgi_folio and a.stage_id != new_stage and not (
            (followup and a.stage_id == followup) or a.stage_id.sgi_is_closing_stage
            or a.stage_id.sgi_is_cancel_stage))

    def _sgi_check_classified(self):
        """57.97.0 (N-05, ISO 10.2): una NC no se trabaja sin decir qué
        requisito se incumplió y qué tan grave es. Se revisa DESPUÉS del
        write (cuenta lo que el formulario manda junto con la etapa)."""
        for alert in self:
            missing = []
            if not alert.sgi_classification:
                missing.append("• la clasificación (mayor, menor u observación)")
            if not alert.sgi_norm_clause_id:
                missing.append("• el requisito (cláusula de la norma) que se incumplió")
            if missing:
                raise UserError(
                    "La NC %s no pasa a «%s» sin:\n%s\nCaptúrelos en «Datos de la NC» y vuelva a "
                    "moverla. (ISO 10.2: qué requisito se incumplió y qué tan grave es.)"
                    % (alert.sgi_folio or alert.name, alert.stage_id.name or '', "\n".join(missing)))
```

- [x] **Step 3: `write`**: junto a `to_check = self.env['quality.alert']` (l.~866) agregar `to_classify = self.env['quality.alert']`; dentro de `if 'stage_id' in vals:`, después de calcular `force` (l.874-875):

```python
            # 57.97.0 (N-05): clasificación y cláusula al salir de Abierta (a
            # Seguimiento o directo a Cerrada). El cierre forzado del Jefe MAST
            # y el sistema quedan exentos; se revisa después de escribir.
            if not force and not self.env.su:
                to_classify = self._sgi_leaving_open(new_stage)
```

y después de `res = super().write(vals)` (l.894), **antes** de `to_check._sgi_check_can_close()`:

```python
        to_classify._sgi_check_classified()
```

- [x] **Step 3b: `create`** (revisión 2026-10-02): después del bloque de FUNC-C13 («Una NC nace abierta…») y **antes** de `super().create`:

```python
        # 57.97.0 (N-05): nacer en «Seguimiento» (alta rápida en esa columna
        # del kanban, RPC) es pasar a Seguimiento sin pasar por el write: pide
        # clasificación y cláusula. Antes de crear (no gasta folio). Solo el
        # sistema queda exento; el Jefe MAST no (como en el write).
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        if followup and not user._is_superuser():
            ctx_stage = self.env.context.get('default_stage_id')
            for vals in vals_list:
                if (vals.get('stage_id') or ctx_stage) == followup.id and not (
                        vals.get('sgi_classification') and vals.get('sgi_norm_clause_id')):
                    raise UserError(
                        "Una NC no nace en «Seguimiento» sin la clasificación (mayor, menor u "
                        "observación) y el requisito (cláusula) que se incumplió. Créela en "
                        "«Abierta» o capture los dos en el alta.")
```

(`user` ya está definido arriba en `create`. Ninguna prueba existente crea NC en Seguimiento con un usuario real: `test_nc_auditoria_evidencia._nc` crea en Seguimiento como superusuario.)

- [x] **Step 4: Vista.** En el aviso «NC del SGI» (`views/sgi_nonconformity_views.xml:32-38`), antes de «Para cerrarla:», agregar: «Para pasarla a Seguimiento: clasificación y requisito (cláusula).» El `role="status"` ya está.

- [x] **Step 5: Checadores y commit**

```bash
python3 tools/check_odoo_views.py --base-ref origin/main
flake8 addons/quimibond_sgi/models/sgi_nonconformity.py
git add addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/views/sgi_nonconformity_views.xml
git commit -m "quimibond_sgi: la NC no sale de Abierta sin clasificación y cláusula (N-05)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.4: Tipo «acuerdo» y acuerdos abiertos a la siguiente revisión (N-09)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`SgiActionLine.action_type`, l.1036-1043)
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (imports l.1-8; campo después de `agreement_ids` l.88-89; `_sgi_open_previous_agreements` después de `_sgi_load_prev_agreements` l.232-249; `action_mark_done` l.400; `action_close` l.414-416; `SgiActionLineReview`)

- [x] **Step 1: `action_type`**:

```python
    action_type = fields.Selection([
        ('contencion', "Contención"),
        ('correccion', "Corrección"),
        ('correctiva', "Acción correctiva"),
        ('preventiva', "Acción preventiva"),
        # 57.97.0 (N-09): los acuerdos de la revisión por la dirección ya no
        # cuentan como acciones correctivas (ni piden su evidencia).
        ('acuerdo', "Acuerdo de la revisión por la dirección"),
    ], string="Tipo", default='correccion', required=True,
        help="Contención y corrección atienden el efecto; la acción correctiva ataca la causa; la "
             "preventiva, una causa potencial; el acuerdo es una salida de la revisión por la dirección.")
```

- [x] **Step 2: Imports** de `sgi_management_review.py`: agregar `from markupsafe import Markup` (después de `from dateutil.relativedelta import relativedelta`).

- [x] **Step 3: Campo**, después de `agreement_ids`:

```python
    # 57.97.0 (N-09): los acuerdos abiertos de revisiones anteriores pasan a
    # esta. Siguen siendo de su revisión (E1-02 los mide en su fecha límite).
    carried_agreement_ids = fields.Many2many(
        'sgi.management.review.agreement', 'sgi_review_carried_agreement_rel',
        'review_id', 'agreement_id', string="Acuerdos abiertos de revisiones anteriores",
        readonly=True,
        help="Acuerdos de revisiones ya realizadas o cerradas que siguen sin cumplirse. Se cargan con "
             "«Cargar entradas»; cada uno sigue siendo de su revisión.")
```

- [x] **Step 4: Método**, después de `_sgi_load_prev_agreements`:

```python
    def _sgi_open_previous_agreements(self):
        """57.97.0 (N-09): acuerdos sin cumplir de cualquier revisión anterior
        ya realizada o cerrada. «Cumplido» como en E1-02: su acción terminada
        o la fecha de cumplimiento capturada a mano."""
        self.ensure_one()
        agreements = self.env['sgi.management.review.agreement'].search([
            ('review_id', '!=', self.id),
            ('review_id.state', 'in', ('realizada', 'cerrada')),
            ('review_id.date', '<=', self.date)])
        return agreements.filtered(lambda a: not a.is_done and not a.done_date)
```

- [x] **Step 5: `action_mark_done`** (l.400): `'action_type': 'correctiva',` → `'action_type': 'acuerdo',` y el comentario de arriba (l.393-395) dice «cada acuerdo es una acción del SGI del tipo «acuerdo» (57.97.0; antes «correctiva»)».

- [x] **Step 6: `action_close`**:

```python
    def action_close(self):
        self._sgi_check_mast()
        self.write({'state': 'cerrada'})
        # 57.97.0 (N-09): cerrar no se bloquea; los acuerdos abiertos quedan en
        # el historial y la siguiente revisión los carga («Cargar entradas»).
        for review in self:
            pending = review.agreement_ids.filtered(lambda a: not a.is_done and not a.done_date)
            if not pending:
                continue
            items = Markup("<br/>").join(
                Markup("• %s (responsable: %s, límite: %s)") % (
                    agr.name, agr.responsible_id.name or '-', agr.deadline or '-')
                for agr in pending)
            review.message_post(body=Markup(
                "<b>Acuerdos abiertos al cerrar:</b> pasan a la siguiente revisión por la "
                "dirección (se cargan con «Cargar entradas»).<br/>%s") % items)
```

- [x] **Step 7: Constraint** en `SgiActionLineReview` (al final de la clase):

```python
    @api.constrains('action_type', 'review_id')
    def _check_acuerdo_only_in_review(self):
        """57.97.0 (N-09): «Acuerdo» es solo para los acuerdos de una revisión
        por la dirección."""
        wrong = self.filtered(lambda l: l.action_type == 'acuerdo' and not l.review_id)
        if wrong:
            raise ValidationError(
                "El tipo «Acuerdo de la revisión por la dirección» es solo para los acuerdos de una "
                "revisión por la dirección. Elija otro tipo para: %s" % ", ".join(wrong.mapped('name')))
```

- [x] **Step 8: Checadores y commit**

```bash
flake8 addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/models/sgi_management_review.py
git add addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/models/sgi_management_review.py
git commit -m "quimibond_sgi: acuerdos de la revisión como tipo «acuerdo»; los abiertos pasan a la siguiente (N-09)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.5: Entradas nuevas de la revisión y scrap de la empresa del SGI (N-09)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (campos después de `satisfaction_summary` l.83-86; `action_load_inputs` l.118-138; métodos nuevos después de `_sgi_load_satisfaction`; `_sgi_load_env` l.341-357)

- [x] **Step 1: Campos**, después de `satisfaction_summary`:

```python
    # 57.97.0 (N-09): las entradas que faltaban (45001 9.3 d, 9.3.2 b,
    # 14001 9.3, 9.3.2 f). Snapshot de «Cargar entradas».
    incidents_summary = fields.Text(
        string="15. Incidentes y desempeño de SST", readonly=True,
        help="45001 9.3: incidentes del periodo por tipo y severidad, días perdidos, abiertos, IPER de "
             "riesgo alto sin acción y permisos de trabajo vencidos. Solo conteos, sin nombres.")
    context_summary = fields.Text(
        string="16. Cambios en el contexto y las partes interesadas", readonly=True,
        help="9.3.2 b: partes interesadas nuevas y revisadas, revisiones vencidas y cuestiones FODA "
             "nuevas o evaluadas en el periodo.")
    env_aspects_summary = fields.Text(
        string="17. Aspectos ambientales significativos", readonly=True,
        help="14001 9.3: aspectos significativos de la matriz, sin control operacional o en evaluación.")
    improvement_summary = fields.Text(
        string="18. Oportunidades de mejora", readonly=True,
        help="9.3.2 f: propuestas de Mejora Continua, oportunidades de la matriz de riesgos y de las "
             "auditorías del periodo.")
```

- [x] **Step 2: `action_load_inputs`**: agregar al `write` (después de `'satisfaction_summary': …`):

```python
                'incidents_summary': review._sgi_load_incidents(),
                'context_summary': review._sgi_load_context(),
                'env_aspects_summary': review._sgi_load_env_aspects(),
                'improvement_summary': review._sgi_load_improvements(),
                'carried_agreement_ids': [(6, 0, review._sgi_open_previous_agreements().ids)],
```

- [x] **Step 3: Helpers y cargadores**, después de `_sgi_load_satisfaction`:

```python
    @staticmethod
    def _sgi_count_by(records, field_name):
        """«Etiqueta: n, …» en el orden de la lista de selección."""
        counts = {}
        for value in records.mapped(field_name):
            counts[value] = counts.get(value, 0) + 1
        return ", ".join("%s: %d" % (label, counts[key])
                         for key, label in records._fields[field_name].selection if counts.get(key))

    @staticmethod
    def _sgi_names(records, limit=10):
        """Los nombres de los más nuevos primero, «… y N más». Solo para
        registros que no son personas."""
        names = records.sorted('id', reverse=True).mapped('display_name')
        if len(names) > limit:
            return "%s y %d más" % (", ".join(names[:limit]), len(names) - limit)
        return ", ".join(names)

    def _sgi_load_incidents(self):
        """Entrada 15 (45001 9.3 d): incidentes y desempeño de SST. Solo
        conteos: el incidente lo leen quien lo reportó, el Jefe MAST, Salud
        ocupacional y el Auditor; se cuenta con sudo y no se nombra nada."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        Incident = self.env['sgi.incident'].sudo()
        incidents = Incident.search([('date', '>=', dt_from), ('date', '<', dt_to)])
        parts = []
        if incidents:
            parts.append("Incidentes en el periodo: %d (%s)." % (
                len(incidents), self._sgi_count_by(incidents, 'incident_type')))
            parts.append("Por severidad: %s." % self._sgi_count_by(incidents, 'severity'))
            parts.append("Días perdidos: %d." % sum(incidents.mapped('days_lost')))
        else:
            parts.append("Sin incidentes registrados en el periodo.")
        parts.append("Incidentes abiertos hoy: %d." % Incident.search_count([('state', '!=', 'cerrado')]))
        parts.append("Verificados como eficaces en el periodo: %d." % Incident.search_count([
            ('sgi_effective', '=', 'eficaz'), ('sgi_effectiveness_date', '>=', self.period_from),
            ('sgi_effectiveness_date', '<=', self.period_to)]))
        parts.append("IPER de riesgo alto sin acción abierta: %d." % self.env['sgi.risk'].sudo().search_count([
            ('instrument', '=', 'iper'), ('high_without_action', '=', True)]))
        # D-03: el permiso sí tiene empresa (regla multiempresa); con sudo, explícita.
        parts.append("Permisos de trabajo vencidos sin cerrar: %d." % self.env['sgi.work.permit'].sudo()
                     .search_count([('expired', '=', True),
                                    ('company_id', '=', self.env['sgi.config']._sgi_company().id)]))
        return "\n".join(parts)

    def _sgi_load_context(self):
        """Entrada 16 (9.3.2 b): cambios en las cuestiones externas e internas
        (FODA) y en las partes interesadas."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        today = fields.Date.context_today(self)
        Party = self.env['sgi.interested.party'].sudo()
        parts = []
        total = Party.search_count([])
        if not total:
            parts.append("Sin partes interesadas registradas (%s)." % sgi_menu_path('partes_interesadas'))
        else:
            reviewed = Party.search_count([('last_review_date', '>=', self.period_from),
                                           ('last_review_date', '<=', self.period_to)])
            overdue = Party.search_count([('next_review_date', '!=', False),
                                          ('next_review_date', '<', today)])
            parts.append("Partes interesadas: %d; revisadas en el periodo: %d; con revisión vencida "
                         "hoy: %d." % (total, reviewed, overdue))
            new = Party.search([('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            if new:
                parts.append("Nuevas en el periodo: %s." % self._sgi_names(new))
        Risk = self.env['sgi.risk'].sudo()
        foda = Risk.search([
            ('instrument', '=', 'foda'), '|',
            '&', ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
            '&', ('last_eval_date', '>=', self.period_from), ('last_eval_date', '<=', self.period_to)])
        parts.append("FODA: %d cuestión(es); nuevas o evaluadas en el periodo: %s." % (
            Risk.search_count([('instrument', '=', 'foda')]),
            self._sgi_names(foda) if foda else "ninguna"))
        return "\n".join(parts)

    def _sgi_load_env_aspects(self):
        """Entrada 17 (14001 9.3 y 6.1.2): aspectos significativos de la
        matriz de la empresa del SGI (D-03)."""
        self.ensure_one()
        company = self.env['sgi.config']._sgi_company()
        Aspect = self.env['sgi.env.aspect'].sudo()
        domain = [('company_id', '=', company.id), ('state', '!=', 'obsoleto')]
        total = Aspect.search_count(domain)
        if not total:
            parts = ["Sin aspectos ambientales en la matriz (%s)." % sgi_menu_path('aspectos_ambientales')]
        else:
            significant = Aspect.search(domain + [('significant', '=', True)])
            levels = dict(Aspect._fields['level'].selection)
            no_control = significant.filtered(
                lambda a: not a.operational_control_id and not (a.control_description or '').strip())
            parts = [
                "Aspectos en la matriz: %d; significativos: %d (%s)." % (
                    total, len(significant), self._sgi_count_by(significant, 'level') or "-"),
                "Significativos sin control operacional: %d. En evaluación: %d." % (
                    len(no_control), Aspect.search_count(domain + [('state', '=', 'borrador')])),
            ]
            for aspect in significant[:15]:
                parts.append("• %s %s (%s)" % (aspect.folio or '', aspect.name,
                                               levels.get(aspect.level, '-')))
            if len(significant) > 15:
                parts.append("… y %d más." % (len(significant) - 15))
        # Activos o archivados, como los cuenta el «Traspaso de riesgos ambientales» (57.96.0).
        legacy = self.env['sgi.risk'].sudo().with_context(active_test=False).search_count([
            ('instrument', '=', 'ambiental'), ('sgi_env_aspect_ids', '=', False)])
        if legacy:
            parts.append("Riesgos ambientales que aún no pasan a la matriz: %d (Traspaso de riesgos "
                         "ambientales)." % legacy)
        return "\n".join(parts)

    def _sgi_load_improvements(self):
        """Entrada 18 (9.3.2 f, 10.3): oportunidades de mejora. Solo el
        proyecto «Mejora Continua SGI» (el de Diseño y Desarrollo también
        trae ``sgi_is_improvement``)."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        parts = []
        project = self.env.ref('quimibond_sgi.sgi_project_improvement', raise_if_not_found=False)
        if project:
            Task = self.env['project.task'].sudo()
            base = [('project_id', '=', project.id)]
            new = Task.search(base + [('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            done = Task.search_count(base + [('stage_id.sgi_is_done_stage', '=', True),
                                             ('date_last_stage_update', '>=', dt_from),
                                             ('date_last_stage_update', '<', dt_to)])
            open_now = Task.search_count(base + [('stage_id.sgi_is_done_stage', '=', False)])
            parts.append("Mejora Continua SGI: %d propuesta(s) nuevas, %d terminadas en el periodo, "
                         "%d abiertas hoy." % (len(new), done, open_now))
            if new:
                parts.append("Nuevas: %s." % self._sgi_names(new))
        opportunities = self.env['sgi.risk'].sudo().search([
            ('kind', '=', 'oportunidad'), ('state', '!=', 'cerrado')])
        parts.append("Oportunidades abiertas en la matriz de riesgos: %d%s" % (
            len(opportunities), (": %s." % self._sgi_names(opportunities)) if opportunities else "."))
        findings = self._sgi_period_audits().sudo().finding_ids.filtered(
            lambda f: f.finding_type == 'oportunidad')
        parts.append("Oportunidades de mejora de las auditorías del periodo: %d." % len(findings))
        return "\n".join(parts)
```

- [x] **Step 4: Scrap** (reemplaza `_sgi_load_env`, l.341-357):

```python
    def _sgi_scrap_domain(self):
        """57.97.0 (N-09, D-03): el scrap del periodo de la empresa del SGI.
        Antes se buscaba sin empresa y podía sumar el de otras razones
        sociales activas en la sesión."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        company = self.env['sgi.config']._sgi_company()
        return [('company_id', '=', company.id), ('state', '=', 'done'),
                ('date_done', '>=', dt_from), ('date_done', '<', dt_to)]

    def _sgi_load_env(self):
        self.ensure_one()
        company = self.env['sgi.config']._sgi_company()
        # sudo: Dirección no siempre tiene Inventario; la empresa va explícita.
        scraps = self.env['stock.scrap'].sudo().search(self._sgi_scrap_domain())
        if not scraps:
            return "Sin registros de scrap de %s en el periodo." % company.name
        by_reason = {}
        for scrap in scraps:
            reason = ", ".join(scrap.scrap_reason_tag_ids.mapped('name')) or "Sin motivo"
            by_reason[reason] = by_reason.get(reason, 0.0) + scrap.scrap_qty
        lines = ["Scrap de %s por motivo (%d movimientos):" % (company.name, len(scraps))]
        for reason, qty in by_reason.items():
            lines.append("• %s: %s" % (reason, round(qty, 2)))
        return "\n".join(lines)
```

- [x] **Step 5: Checadores y commit**

```bash
flake8 addons/quimibond_sgi/models/sgi_management_review.py
git add addons/quimibond_sgi/models/sgi_management_review.py
git commit -m "quimibond_sgi: revisión por la dirección con incidentes, contexto, aspectos y mejoras; scrap de la empresa del SGI (N-09)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.6: Conclusiones 9.3.3 para marcar realizada (N-09)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_management_review.py` (constante de módulo después de los imports; campos después de `carried_agreement_ids`; `_sgi_check_conclusions`; `action_mark_done`)

- [x] **Step 1: Constante** (después de los imports):

```python
# 57.97.0 (N-09, ISO 9001 9.3.3; 14001 y 45001 9.3): sin estas salidas la
# revisión no se marca realizada.
SGI_REVIEW_CONCLUSIONS = ('conclusion_suitability', 'conclusion_adequacy',
                          'conclusion_effectiveness', 'output_needs')
```

- [x] **Step 2: Campos**:

```python
    # 57.97.0 (N-09): conclusiones y salidas 9.3.3.
    conclusion_suitability = fields.Text(
        string="Conveniencia",
        help="¿El SGI sigue siendo conveniente para la empresa y su contexto? Obligatoria para marcar la "
             "revisión como Realizada; si no hay cambios, escríbalo.")
    conclusion_adequacy = fields.Text(
        string="Adecuación",
        help="¿El SGI cubre lo que la empresa necesita (procesos, requisitos, partes interesadas)? "
             "Obligatoria para marcar la revisión como Realizada.")
    conclusion_effectiveness = fields.Text(
        string="Eficacia",
        help="¿El SGI logra los resultados previstos (objetivos, indicadores, NC, incidentes)? "
             "Obligatoria para marcar la revisión como Realizada.")
    output_needs = fields.Text(
        string="Mejora, cambios y recursos",
        help="Decisiones sobre oportunidades de mejora, cambios al SGI y recursos que se necesitan "
             "(9.3.3 a, b y c). Obligatoria para marcar la revisión como Realizada.")
```

- [x] **Step 3: Método**, antes de `action_mark_done`:

```python
    def _sgi_check_conclusions(self):
        """57.97.0 (N-09): las conclusiones 9.3.3 no vacías (ni solo espacios).
        Solo al marcar realizada (el cambio de estado), no en write."""
        for review in self:
            missing = [review._fields[name].string for name in SGI_REVIEW_CONCLUSIONS
                       if not (review[name] or '').strip()]
            if missing:
                raise UserError(
                    "No se puede marcar como Realizada la revisión %s sin las conclusiones (ISO 9.3.3): "
                    "%s. Captúrelas en la pestaña «Conclusiones (9.3.3)»; si no hay cambios, "
                    "escríbalo («Sin cambios»)." % (review.folio or review.name, ", ".join(missing)))
```

- [x] **Step 4: `action_mark_done`**: después del bloque `incomplete` (l.~386-392) y antes de `Line = self.env['sgi.action.line']`:

```python
            review._sgi_check_conclusions()
```

- [x] **Step 5: Commit**

```bash
flake8 addons/quimibond_sgi/models/sgi_management_review.py
git add addons/quimibond_sgi/models/sgi_management_review.py
git commit -m "quimibond_sgi: conclusiones 9.3.3 obligatorias para marcar realizada la revisión (N-09)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.7: Ficha, acta y diagrama de la revisión

**Files:**
- Modify: `addons/quimibond_sgi/views/sgi_management_review_views.xml` (`sgi_management_review_view_form`, `sgi_management_review_action`)
- Modify: `addons/quimibond_sgi/report/report_mgmt_review.xml`
- Modify: `addons/quimibond_sgi/models/sgi_diagram_iso.py` (`_data_management_review`, l.485-492)

- [x] **Step 1: Ficha** (`sgi_management_review_view_form`):
  - Texto de flujo (l.43-50): «… → **Cargar entradas** llena las 18 entradas de la norma (NC, reclamaciones, auditorías, indicadores en rojo, riesgos altos, incidentes, contexto, aspectos, mejoras…) y trae los acuerdos abiertos de revisiones anteriores → capture las **Conclusiones** y los **Acuerdos** → **Marcar realizada** y **Cerrar**.»
  - Pestaña «Entradas (9.3.2)», después de `participation_summary`:
    ```xml
                                <field name="incidents_summary" readonly="sgi_is_locked"/>
                                <field name="context_summary" readonly="sgi_is_locked"/>
                                <field name="env_aspects_summary" readonly="sgi_is_locked"/>
                                <field name="improvement_summary" readonly="sgi_is_locked"/>
    ```
  - Pestaña nueva antes de «Acuerdos (salidas)»:
    ```xml
                        <page string="Conclusiones (9.3.3)" name="conclusions">
                            <div class="alert alert-info mb-2" role="status">
                                Para marcar la revisión como Realizada, escriba las conclusiones sobre la
                                conveniencia, la adecuación y la eficacia del SGI y las decisiones sobre mejora,
                                cambios y recursos (ISO 9001 9.3.3; ISO 14001 y 45001 9.3). Si no hay cambios,
                                escríbalo.
                            </div>
                            <group>
                                <field name="conclusion_suitability" readonly="sgi_is_locked"
                                       placeholder="¿El SGI sigue siendo conveniente para la empresa y su contexto?"/>
                                <field name="conclusion_adequacy" readonly="sgi_is_locked"
                                       placeholder="¿Cubre los procesos, los requisitos y las partes interesadas?"/>
                                <field name="conclusion_effectiveness" readonly="sgi_is_locked"
                                       placeholder="¿Logra los resultados previstos?"/>
                                <field name="output_needs" readonly="sgi_is_locked"
                                       placeholder="Oportunidades de mejora, cambios al SGI y recursos"/>
                            </group>
                        </page>
    ```
  - Pestaña «Acuerdos (salidas)», después del `field agreement_ids`:
    ```xml
                            <separator string="Acuerdos abiertos de revisiones anteriores"/>
                            <div class="text-muted mb-2">
                                Se cargan con «Cargar entradas». Siguen siendo de su revisión; aquí se revisa
                                cómo van.
                            </div>
                            <field name="carried_agreement_ids" readonly="1">
                                <list decoration-danger="action_state == 'vencida'">
                                    <field name="review_id"/>
                                    <field name="name"/>
                                    <field name="responsible_id"/>
                                    <field name="deadline"/>
                                    <field name="action_state" string="Acción" widget="badge"/>
                                </list>
                            </field>
    ```
  - Ayuda de `sgi_management_review_action` (l.180): «El botón Cargar entradas llena las 18 entradas de la norma con los datos reales del periodo y trae los acuerdos abiertos de revisiones anteriores. Los acuerdos se vuelven acciones con seguimiento.»

- [x] **Step 2: Acta** (`report/report_mgmt_review.xml`), después de la entrada 14:

```xml
                        <p><strong>15. Incidentes y desempeño de SST (45001 9.3):</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.incidents_summary"/></p>
                        <p><strong>16. Cambios en el contexto y las partes interesadas (9.3.2 b):</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.context_summary"/></p>
                        <p><strong>17. Aspectos ambientales significativos (14001 9.3):</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.env_aspects_summary"/></p>
                        <p><strong>18. Oportunidades de mejora (9.3.2 f):</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.improvement_summary"/></p>

                        <h4>Conclusiones (9.3.3)</h4>
                        <p><strong>Conveniencia:</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.conclusion_suitability"/></p>
                        <p><strong>Adecuación:</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.conclusion_adequacy"/></p>
                        <p><strong>Eficacia:</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.conclusion_effectiveness"/></p>
                        <p><strong>Mejora, cambios y recursos:</strong><br/>
                            <span style="white-space: pre-line;" t-field="doc.output_needs"/></p>
```

y después de la tabla de acuerdos:

```xml
                        <h4>Acuerdos abiertos de revisiones anteriores</h4>
                        <p t-if="not doc.carried_agreement_ids">Ninguno.</p>
                        <table t-else="" class="table table-sm table-bordered">
                            <thead>
                                <tr><th>Revisión</th><th>Acuerdo</th><th>Responsable</th><th>Fecha límite</th><th>Estado</th></tr>
                            </thead>
                            <tbody>
                                <tr t-foreach="doc.carried_agreement_ids" t-as="agr">
                                    <td><span t-field="agr.review_id.folio"/></td>
                                    <td><span t-field="agr.name"/></td>
                                    <td><span t-field="agr.responsible_id"/></td>
                                    <td><span t-field="agr.deadline"/></td>
                                    <td><span t-out="agr.status_label"/></td>
                                </tr>
                            </tbody>
                        </table>
```

(El título «Acuerdos abiertos de revisiones anteriores» sale siempre: la prueba 12 lo busca con un acuerdo cargado; con `t-if`/`t-else` solo cambia la tabla.)

- [x] **Step 3: Diagrama 9.3** (`sgi_diagram_iso.py`, lista `inputs` de `_data_management_review`): agregar después de `('resources_note', "Recursos")`:

```python
            ('incidents_summary', "Incidentes y SST"), ('context_summary', "Contexto y partes interesadas"),
            ('env_aspects_summary', "Aspectos significativos"), ('improvement_summary', "Oportunidades de mejora"),
```

- [x] **Step 4: Checadores y commit**

```bash
python3 tools/check_odoo_views.py --base-ref origin/main
git add addons/quimibond_sgi/views/sgi_management_review_views.xml addons/quimibond_sgi/report/report_mgmt_review.xml addons/quimibond_sgi/models/sgi_diagram_iso.py
git commit -m "quimibond_sgi: entradas 15-18, conclusiones y acuerdos que pasan en la ficha, el acta y el diagrama 9.3" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.8: Manuales

**Files:**
- Modify: `docs/sgi/usuarios/direccion.md` (2.2), `docs/sgi/usuarios/mast.md` (2.2), `docs/sgi/usuarios/jefe-de-area.md`, `docs/sgi/usuarios/auditor.md`, `docs/sgi/administracion/manual-jefe-mast.md` (sección 9 y una nota en «Qué no hacer»), `addons/quimibond_sgi/README.md` (A-029, l.~29-32)

- [x] **Step 1: Textos** («usted»):
  - **Dirección, 2.2:** paso 2 «**Cargar entradas** llena las 18 entradas de la norma (además de las de hoy: incidentes y desempeño de SST, cambios en el contexto y las partes interesadas, aspectos ambientales significativos y oportunidades de mejora) y trae los **acuerdos abiertos de revisiones anteriores**.» Paso nuevo antes de «Marcar realizada»: «Escriba las **Conclusiones (9.3.3)**: conveniencia, adecuación, eficacia y mejora, cambios y recursos. Sin ellas no se marca realizada; si no hay cambios, escríbalo.» Al final: «Cerrar con acuerdos abiertos se permite: quedan anotados y la siguiente revisión los carga.»
  - **MAST, 2.2:** «Para pasar una NC de Abierta a Seguimiento (o cerrarla sin pasar por Seguimiento) capture la **clasificación** y el **requisito (cláusula)**. Desde 57.97.0 hay cláusulas de tercer nivel (por ejemplo, 45001 8.1.2 jerarquía de controles, 8.1.4 contratistas, 14001 6.1.2 aspectos ambientales); use la más precisa. El cierre forzado no lo pide.»
  - **Jefe de área (dueño de proceso):** «Antes de mover una NC de su proceso a Seguimiento, capture su clasificación y la cláusula que se incumplió.»
  - **Auditor:** «La Matriz de cumplimiento muestra ahora las cláusulas de tercer nivel; las que salen en rojo no tienen ninguna actividad ligada.»
  - **Manual del Jefe MAST, sección 9:** lo de la NC; y «Las 13 cláusulas de tercer nivel salen en rojo en la Matriz hasta que se liguen a actividades (pestaña «Cumple con» de cada actividad). Si capturó a mano una cláusula con el mismo numeral antes de 57.97.0, la actualización le liga el xmlid y no la duplica.» En «Qué no hacer»: «No capture cláusulas de tercer nivel a mano en ISO 9001, 14001 o 45001: ya vienen con el módulo.»
  - **README, A-029:** agregar «Las cláusulas de tercer nivel de ISO 9001, 14001 y 45001 (57.97.0) sí traen xmlid (`data/sgi_norms_tercer_nivel.xml`); las CLI siguen sin él.»

- [x] **Step 2: Commit**

```bash
git add docs/sgi/usuarios docs/sgi/administracion addons/quimibond_sgi/README.md
git commit -m "docs: cláusulas de tercer nivel y revisión por la dirección en los manuales (57.97.0)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 7.9: Revisión de lo que se tocó (antes de la versión)

- [x] **Step 1:** `grep -rn "action_mark_done()\|'correctiva'" addons/quimibond_sgi/models addons/quimibond_sgi/tests | grep -i "review\|revis"` y confirmar que ninguna otra prueba ni código marca revisiones realizadas sin conclusiones ni espera `'correctiva'` en un acuerdo (en `d45525a1`: solo `test_mgmt_review.test_04` y `test_pr5_direction.test_02`).
- [x] **Step 2:** `grep -rn "stage_follow\|stage_closed\|'stage_id'" addons/quimibond_sgi/tests/*.py` y confirmar que toda prueba que mueve una NC **desde Abierta** con un usuario real trae clasificación y cláusula (1.3).
- [x] **Step 3:** `python3 tools/check_odoo_views.py --base-ref origin/main`: `role` de los `alert-*`, sin herencias propias nuevas.
- [x] **Step 4:** `test_usted.py` no corre local; revisar a ojo las cadenas nuevas (campos, ayudas, `placeholder`, mensajes, nombres de las cláusulas) contra la regla 11.

## Migración

- **`migrations/19.0.57.97.0/pre-migrate.py`:** solo inserta filas en `ir_model_data` (metadato) para ligar el xmlid de una cláusula nueva a una cláusula que ya exista con el mismo numeral en la misma norma; idempotente; log INFO con lo ligado y WARNING si hay duplicados. **En producción no liga nada** (0 numerales iguales el 2026-10-02). No toca datos de negocio ni borra nada.
- **Registros nuevos:** 13 `sgi.norm.clause` (XML `noupdate` nuevo). Columnas nuevas vacías en `sgi_management_review` (`incidents_summary`, `context_summary`, `env_aspects_summary`, `improvement_summary`, `conclusion_suitability`, `conclusion_adequacy`, `conclusion_effectiveness`, `output_needs`) y la tabla `sgi_review_carried_agreement_rel`. Valor nuevo `acuerdo` en `sgi_action_line.action_type` (sin filas que convertir: 0 acciones de revisión).
- **No hay migración de datos de negocio.** Las 2 NC abiertas y las 2 revisiones en borrador quedan como están.

## Task 7.10: Versión, CHANGELOG, documentación y build

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), este plan (casillas)

- [x] **Step 1: Versión** `'19.0.57.97.0'` en `__manifest__.py` (l.21).

- [x] **Step 2: CHANGELOG**, arriba de `## 19.0.57.96.0`

```markdown
## 19.0.57.97.0 — 2026-10-XX

**Cláusulas y revisión por la dirección** (auditoría 2026-10: N-05 y N-09; es
la ficha «57.96.0» del plan general, renumerada porque 57.96.0 fue «SST y
ambiente»). Plan:
`docs/superpowers/plans/2026-10-0X-sgi-57-97-0-clausulas-revision.md`.
**Ningún dato de negocio cambia en el despliegue.**

### Agregado

- **Cláusulas de tercer nivel** (`data/sgi_norms_tercer_nivel.xml`,
  `noupdate`, con xmlid `c_<norma>_<n>_<n>_<n>`): ISO 9001 7.1.5 y 9.1.2;
  ISO 14001 6.1.2, 6.1.3, 6.1.4 y 9.1.2; ISO 45001 6.1.2, 6.1.3, 6.1.4,
  8.1.2, 8.1.3, 8.1.4 y 9.1.2. Las de requisitos de clientes (CLI-xx, sin
  xmlid) no se tocan (A-029).
- **Revisión por la dirección:** entradas «15. Incidentes y desempeño de SST»
  (solo conteos), «16. Cambios en el contexto y las partes interesadas», «17.
  Aspectos ambientales significativos» y «18. Oportunidades de mejora»;
  pestaña «Conclusiones (9.3.3)» (conveniencia, adecuación, eficacia y
  mejora, cambios y recursos); «Acuerdos abiertos de revisiones anteriores»
  (`carried_agreement_ids`). En la ficha, el acta y el diagrama 9.3.
- Tipo de acción **«Acuerdo de la revisión por la dirección»** (`acuerdo`)
  en `sgi.action.line`; solo para acciones con revisión.

### Cambiado

- **NC:** con folio, no sale de «Abierta» hacia «Seguimiento» ni directo a
  «Cerrada» sin clasificación y requisito (cláusula), ni nace en
  «Seguimiento» sin ellos (alta rápida en esa columna del kanban). El cambio
  de etapa se revisa después de escribir, solo en esa transición: no alcanza
  a lo que ya está en Seguimiento o cerrado, ni al reabrir, al «No eficaz» o
  al cancelar. Exentos el sistema y el cierre forzado del Jefe MAST.
- **«Marcar realizada»** pide las cuatro conclusiones 9.3.3 (no vacías).
- **Acuerdos de la revisión:** nacen como «Acuerdo» (antes «Acción
  correctiva»): ya no inflan las correctivas ni piden su evidencia. E1-02 no
  cambia.
- **Cerrar la revisión** con acuerdos abiertos deja la lista en el historial;
  la siguiente revisión los carga con «Cargar entradas».
- **«8. Desempeño ambiental»:** scrap solo de la empresa del SGI (D-03),
  leído con `sudo` (antes, sin empresa y como el usuario).

### Migración

`migrations/19.0.57.97.0/pre-migrate.py`: si alguien capturó a mano una
cláusula con el mismo numeral en la misma norma, solo le liga el xmlid
(`ir_model_data`, `noupdate`) para que el XML no la duplique; con varias,
liga la de id menor y avisa (WARNING). Idempotente. En producción, el
2026-10-02, no había ninguna. Lo demás son columnas nuevas vacías, una tabla
`Many2many` y 13 registros nuevos.

### Datos de producción

- La Matriz de cumplimiento muestra 13 cláusulas más, en rojo hasta que se
  liguen a actividades (pista de datos); «Generar checklist» de una auditoría
  con esas normas pregunta por ellas.
- Las 2 NC abiertas pedirán clasificación y cláusula al pasar a Seguimiento.
- Las 2 revisiones en borrador (RD-2026-01, RD-2026-12) tendrán las entradas
  15-18 al pulsar «Cargar entradas» y pedirán conclusiones al marcarse
  realizadas.

### Decisiones por omisión (preguntas del plan)

- (llenar con las respuestas de Jose a Q1–Q8, o «por omisión» en cada una)

**Pruebas:** `test_clausulas_revision` (12 casos: cláusulas nuevas con xmlid
y únicas por norma, las CLI sin tocar, el pre-migrate que solo liga e
idempotente, la matriz con el tercer nivel; NC que no pasa a Seguimiento ni
directo a Cerrada sin clasificación y cláusula, ni nace en Seguimiento, cierre forzado, reabrir y
sistema exentos; entradas 15-18 (también como Dirección), scrap de la
empresa del SGI, acuerdo como tipo propio y sin evidencia, conclusiones
obligatorias, acuerdos abiertos que pasan y el acta). Ajustadas:
`test_mgmt_review.test_04` y `test_pr5_direction.test_02` (conclusiones),
`test_candados_evidencia` y `test_nc_deadlines.test_03` (clasificación y
cláusula).

**Verificación pendiente en Odoo.sh:** (1) que el XML `noupdate` nuevo crea
las 13 cláusulas en el `-u` de una base que no las tiene; (2) que
`registry.clear_cache()` limpia la caché de xmlid en la prueba 03.
```

- [x] **Step 3: Documentación técnica generada**

```bash
python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
```

(Deben aparecer los campos nuevos de `sgi.management.review` y el valor `acuerdo` de `sgi.action.line`.)

- [x] **Step 4: Mapa del plan general.** En `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`: renglón «57.96.0 → 57.97.0 o siguiente libre» y título de la ficha: «57.96.0 → entregada como **57.97.0** (2026-10-XX)»; renglón y ficha «57.97.0 — Interfaz»: «→ 57.98.0 o siguiente libre»; párrafo «Renumeración (2026-10-02)»: una oración con lo mismo. Guardar este plan como `docs/superpowers/plans/2026-10-0X-sgi-57-97-0-clausulas-revision.md`.

- [x] **Step 5: Checadores** (sección 0, punto 4). Esperado: 0 errores (sin la advertencia de bump).

- [ ] **Step 6: Commit, push y build**

```bash
git add -A addons/quimibond_sgi docs/sgi docs/superpowers/plans
git commit -m "quimibond_sgi 19.0.57.97.0: cláusulas y revisión por la dirección (N-05, N-09)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

Esperado en el build de la rama (`--test-tags /quimibond_sgi`): `test_clausulas_revision` 12/12; `test_mgmt_review`, `test_pr5_direction`, `test_candados_evidencia`, `test_nc_deadlines`, `test_nc_auditoria_evidencia`, `test_nc_flow`, `test_norm_compliance`, `test_legado`, `test_entrega4`, `test_nomenclatura_pantallas`, `test_diagram_view`, `test_usted` OK, y ningún fallo nuevo respecto del build de `main`. Si falla una prueba existente que mueve una NC desde Abierta con un usuario real, agregarle clasificación y cláusula (no relajar el candado). En el `update.log`: «SGI 57.97.0: 0 cláusula(s) existente(s) ligadas…», las 13 cláusulas creadas, sin errores de vistas.

- [x] **Step 7: Marcar las casillas** de este plan y commit (`docs: plan 57.97.0 al día`).

- [ ] **Step 8: Verificación en producción** (solo lectura, por MCP)
  - `ir.module.module [('name','=','quimibond_sgi')]` → `latest_version = 19.0.57.97.0`.
  - `aggregate_records('sgi.norm.clause', ['norm_id'])` → 9001 = 30, 14001 = 26, 45001 = 30, CLIENTES = 10 (96 en total).
  - `search_records('sgi.norm.clause', [('code','in',['6.1.2','6.1.3','6.1.4','7.1.5','8.1.2','8.1.3','8.1.4','9.1.2'])])` → 13, ninguna repetida por norma.
  - `aggregate_records('ir.model.data', ['module'], [('model','=','sgi.norm.clause')])` → `quimibond_sgi` 86 (73 + 13).
  - `get_fields('sgi.management.review', ['incidents_summary','conclusion_suitability','carried_agreement_ids'])`; `get_fields('sgi.action.line', ['action_type'])` con `acuerdo` en la lista.
  - `aggregate_records('quality.alert', ['stage_id'], [('sgi_folio','!=',False)])` → igual que antes (2 Abierta, 14 Cancelada).
  - `aggregate_records('sgi.management.review', ['state'])` → 2 borrador (nada cambió).

---

## Preguntas para Jose (con la opción por omisión)

- **Q1 (= Q13 de la auditoría). Requisitos de cliente: ¿en la norma CLI o como requisito legal de tipo «cliente»?** **Por omisión:** esta entrega no cambia nada: las CLI-01…CLI-10 siguen en la norma «CLIENTES» (sin xmlid, no se tocan) y una NC puede citarlas como cláusula; los 2 requisitos legales de tipo «cliente» siguen en lo legal para evaluar su cumplimiento. Si decide una sola fuente, va en otra entrega (crear xmlid a las CLI con migración, A-029, o pasar los 2 legales a CLI).
- **Q2. ¿Qué cláusulas de tercer nivel?** **Por omisión:** las 13 de la tabla de 1.2 (solo donde la norma las tiene). No se agregan 9001 6.1.2, las de 9.3.x ni el cuarto nivel (45001 8.1.4.1–8.1.4.3, 6.1.2.1–6.1.2.3). Ligarlas a actividades es pista de datos del Jefe MAST.
- **Q3. ¿Cuándo pide la NC clasificación y cláusula?** **Por omisión:** al salir de Abierta hacia Seguimiento **o directo a Cerrada**, y al crearla directo en Seguimiento; el cierre forzado del Jefe MAST y el sistema quedan exentos; reabrir, «No eficaz» y cancelar no la piden; el Jefe MAST sí la captura. Alternativa: solo hacia Seguimiento (cerrar desde Abierta sin cláusula seguiría permitido).
- **Q4. ¿El acuerdo pide evidencia al terminarse?** **Por omisión:** no (como una corrección; hasta hoy, por ser «correctiva», la pedía desde 57.93.0, pero no hay ningún acuerdo en producción). Alternativa: evidencia igual que la correctiva.
- **Q5. Conclusiones 9.3.3: ¿cuáles son obligatorias?** **Por omisión:** las cuatro (conveniencia, adecuación, eficacia y mejora, cambios y recursos), con «Sin cambios» como respuesta válida. Alternativa: solo las tres conclusiones.
- **Q6. Acuerdos abiertos al cerrar la revisión.** **Por omisión:** cerrar no se bloquea; queda la lista en el historial y la siguiente revisión los carga (de cualquier revisión anterior realizada o cerrada); cada acuerdo sigue siendo de su revisión y E1-02 lo mide en su fecha límite. Alternativa: no cerrar con acuerdos abiertos.
- **Q7. ¿De qué empresa es el scrap de la revisión?** **Por omisión:** de la empresa del SGI (D-03, PNTQ). Alternativa: la empresa activa en la sesión de quien carga las entradas.
- **Q8. Numeración.** **Por omisión:** esta ficha sale como 57.97.0 y «Interfaz» (57.97.0 en el mapa) toma el siguiente número libre.

---

## Revisión del plan (2026-10-02, contra `d45525a1`)

Revisado leyendo `sgi_norm.py`, `sgi_norm_compliance.py`, `data/sgi_norms.xml`, `data/sgi_stages.xml`, `sgi_nonconformity.py` (create, write, candados, asistentes de cierre forzado y cancelación, `sgi.action.line`), `sgi_management_review.py`, `sgi_kpi_review.py`, `sgi_incident.py`, `sgi_context.py`, `sgi_risk.py`, `sgi_env_aspect.py`, `sgi_work_permit.py`, `sgi_improvement.py`, `sgi_diagram_iso.py`, las vistas y el acta de la revisión, `tools/check_addons.py` (check 4), `tests/test_usted.py`, `common_users.py` y todas las pruebas que mueven etapas de NC o marcan revisiones realizadas. Original guardado como `plan_5797.orig.md`.

**Confirmado:**
- Líneas y anclas citadas coinciden con el HEAD (desvíos de 1-2 líneas en la ficha y la ayuda de la acción: corregidos).
- `sgi.norm.clause` no tiene `parent`/`sequence` ni restricción única; el orden de la matriz es `_sgi_sort_key` (8.1 < 8.1.2 < 8.1.4 < 8.2). `_sgi_find('45001 8.1.2')` y `short_label` funcionan con el numeral de tres niveles.
- XML `noupdate` nuevo en un `-u`: `convert.py` crea el registro cuyo xmlid no existe (`forcecreate` por omisión); lo dice el comentario de `sgi_norms.xml:1-5` y lo usó 57.96.0 (cron nuevo en XML nuevo). El pre-migrate escribe columnas reales de `ir_model_data`; `ON CONFLICT DO NOTHING` sin destino cubre el índice único `(module, name)`; filas `noupdate = true` no las borra el `_process_end`.
- La prueba no tiene `env.ref` literal a xmlid nuevos (`REF_RE` solo ve literales); cargar el pre-migrate con `importlib` es el patrón de `test_sst_links.test_03`, `test_legacy_routine` y `test_replaced_by_process` (el archivo viaja con el addon).
- `registry.clear_cache()` existe desde Odoo 17 y `_xmlid_lookup` está en la caché «default»; además `ir.model.data.unlink` la limpia solo.
- Pruebas que mueven una NC desde Abierta con usuario real: solo `test_candados_evidencia.test_08` (vía `_closable_nc`) y `test_nc_deadlines.test_03`; `test_07` de candados y `test_audit_hardening.test_a3` chocan antes con «dueño del proceso» (antes de escribir); `test_nc_auditoria_evidencia` crea en Seguimiento como superusuario; `test_flows_48`, `test_nc_flow`, `test_ola1`, `test_ola_b`, `test_pegamento`, `test_nc_deadlines.test_04` son superusuario. Ningún satélite (`_pesaje`, `_plm`…) ni `quimibond_intelligence` mueve etapas de NC ni usa `action_type`.
- `action_mark_done` de la revisión solo lo llaman `test_mgmt_review.test_04` y `test_pr5_direction.test_02`; `test_entrega4.test_09/test_11` cierran revisiones sin acuerdos (sin nota nueva). `action_type == 'correctiva'` solo se usa en la NC (eficacia, reincidencia, 8D); nadie cuenta acciones de revisión por tipo.
- `done_date` del acuerdo existe (`sgi_kpi_review.py`, guardado); `next_review_date` de la parte interesada y `expired` del permiso están guardados; `company_id` de `stock.scrap`, del aspecto y del permiso existen; ACL de Dirección: revisión 1,1,1,1, acuerdos 1,1,1,0.
- Cadenas nuevas sin tuteo ni fuera del glosario (`test_usted` salta `migrations/`).

**Corregido en este plan:**
1. *Importante* — **Alta directa en Seguimiento** (alta rápida en esa columna del kanban, el mismo camino que FUNC-C13 cerró para «Cerrada») se brincaba el candado. Nuevo Step 3b en `create` (antes de crear, no gasta folio; exento solo el sistema), una aserción más en `test_05`, 1.3, CHANGELOG y Q3.
2. *Importante* — **`test_08` frágil con la copia de producción:** el aspecto 4×3 podía quedar fuera de los 15 significativos que lista el cargador cuando el Jefe MAST capture o traspase aspectos. Ahora 5×5 (puntaje máximo, folio más nuevo).
3. *Menor* — `_sgi_leaving_open`: «antes de Seguimiento» incluye una etapa genérica de Calidad (base nueva), no solo Abierta/sin etapa.
4. *Menor* — Permisos vencidos en la entrada 15 filtrados por la empresa del SGI (D-03; el permiso es multiempresa y se lee con `sudo`).
5. *Menor* — «Riesgos ambientales que aún no pasan a la matriz» cuenta también los archivados, como el asistente de traspaso de 57.96.0.
6. *Menor* — líneas de la ficha (43-50) y de la ayuda de la acción (180).

**Anotado, sin cambio:**
- «Acuerdo de la revisión por la dirección» aparece en el selector de tipo de las acciones de NC, riesgo, incidente, AMEF y simulacro; el constraint lo rechaza con un mensaje claro. Ocultarlo pediría un dominio por vista (fuera de alcance).
- `_locked` duplica en parte `common_users.assert_locked` (este además revisa el texto): aceptable.
- El acuerdo de una revisión cerrada sigue el K-03 existente (`_sgi_origin_closed`): terminado = evidencia; pendiente se sigue trabajando.
- El Step 3b lee `default_stage_id` del contexto como FUNC-C13; los caminos que crean NC desde otro modelo («Generar NC» del ticket, fuentes automáticas) son botones o `sudo` sin esa clave, así que no hay choque de ids entre modelos. Si algún día una NC nace desde un alta rápida de otro kanban, Odoo ya le pondría esa etapa por `default_get`: el candado haría bien en detenerla.
- La entrada 1 («Acuerdos previos», solo la última revisión) convive con `carried_agreement_ids` (todas las anteriores): redundancia menor, intencional.

**Veredicto:** listo para implementar con las correcciones de arriba. Lo único que solo confirma el build es la creación de las 13 cláusulas en el `-u` (prueba 01) y la limpieza de caché de la prueba 03.

---

## Implementación (2026-10-02)

- Tasks 7.0 a 7.10 en la rama, un commit por tarea. El código sigue el plan; las cifras y anclas se revisaron contra el repo.
- **Desviación:** `_sgi_on_ineffective` regresa a Seguimiento solo una NC en etapa de cierre (antes: cualquier etapa distinta de Abierta). Una NC sin etapa o en una etapa genérica de Calidad habría chocado con el candado de clasificación al marcar «No eficaz». La ayuda de «Resultado de la eficacia» y los manuales se ajustaron.
- **Sin bump intermedio:** la carpeta `migrations/19.0.57.97.0` entró antes de subir la versión sin error de `check_addons.py` (solo la advertencia de bump); la versión subió en la Task 7.10.
- Manual del Jefe MAST: las cláusulas se ligan en el campo «Cumple con» de la actividad (no es una pestaña).
- Jose aceptó Q1–Q8 por omisión el 2026-10-02 (`docs/audit/decisiones.md`).
