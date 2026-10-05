# Entrega 57.96.0 — SST y ambiente (plan de implementación)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Renumeración:** es la ficha **«57.95.0 — SST y ambiente»** del plan general (`docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`, hallazgos N-06, N-07 e incidente desde `hr.leave`). El número 57.95.0 lo tomó «Rendimiento y robustez» (párrafo «Renumeración (2026-10-02)» del plan general), así que esta ficha sale como **`19.0.57.96.0`**. La ficha «57.96.0 — Cláusulas y revisión por la dirección» y las siguientes toman el siguiente número libre (57.97.0 en adelante) cuando se inicien; la Task 6.12 lo anota en el mapa del plan general.

> **Estado (2026-10-02):** implementada en la rama `claude/confident-mendel-8yg7xu`
> como 19.0.57.96.0. Jose aceptó las opciones por omisión de Q1 a Q11. Las
> desviaciones respecto de este plan y los ajustes de las revisiones (eficacia
> del incidente también al crear, permiso con LOTO que tampoco se cancela,
> motivo por renglón en el traspaso, aviso de NC en lo legal, campos del
> contratista de solo lectura fuera del Jefe MAST) están en la entrada
> 19.0.57.96.0 de `addons/quimibond_sgi/CHANGELOG.md`. Sin marcar: lo que solo
> se comprueba en el build de Odoo.sh y en producción.

**Goal:** cerrar los huecos de 45001 y 14001 que la auditoría marcó como «sin soporte» sin meter pasos obligatorios nuevos donde Dirección no ha decidido: (1) **jerarquía de controles** (45001 8.1.2) en riesgos y acciones; un IPER de riesgo alto con solo EPP, o sin jerarquía declarada, no se controla ni se cierra; (2) **incidente con equipo de investigación** (45001 5.4: al menos un trabajador o un integrante de la Comisión de Seguridad e Higiene), **verificación de eficacia** como en las NC y, si es moderado/grave/fatal con IPER ligado, **IPER reevaluado después del incidente**; (3) **permiso de trabajo vencido** guardado y avisado **cada hora** al jefe del área y al Jefe MAST; el permiso **no se cierra con un LOTO aplicado**; (4) **competencia requerida por tipo de permiso** (configurable; sin configuración no cambia nada) y **evaluación SST del contratista** en su contacto (por omisión solo avisa; bloquea si el Jefe MAST enciende un parámetro); (5) **aspecto ambiental en un solo lugar** (`sgi.env.aspect`, con etapa del ciclo de vida); «Aspecto ambiental» ya no se elige al crear un riesgo a mano; un **asistente manual** del Jefe MAST pasa los 5 riesgos ambientales a la matriz (cuenta primero, aplica después; los riesgos originales se conservan ligados); (6) **lo legal con evidencia**: los botones rápidos «Cumple / Parcial / No cumple / No aplica» abren el asistente con evidencia (o motivo) obligatoria; (7) **incidente desde la incapacidad**: una ausencia aprobada del tipo «Riesgo de trabajo (IMSS)» crea el incidente en «Reportado» con la persona y los días perdidos, y avisa al Jefe MAST. **Ningún dato de negocio cambia en el despliegue.**

**Architecture:** una versión de `quimibond_sgi` (19.0.57.96.0) sobre 57.95.0. Python en modelos existentes (`sgi_risk.py`, `sgi_nonconformity.py` (acción), `sgi_incident.py`, `sgi_work_permit.py`, `sgi_env_aspect.py`, `sgi_legal.py`, `sgi_cron.py`), un módulo de constantes sin modelos (`sgi_control_hierarchy.py`), dos archivos de modelos nuevos (`sgi_env_aspect_transfer.py`: asistente transitorio con renglones; `sgi_incident_leave.py`: extensión de `hr.leave`), un modelo de configuración nuevo (`sgi.work.permit.skill`), una dependencia nueva en el manifest (`hr_holidays`, ya instalado en producción), una acción planificada nueva cada hora (`sgi_cron_work_permits`, XML `noupdate` nuevo), vistas propias editadas en su `<record>`, dos vistas nuevas con su acción y menú, y una herencia de vista de **otro** módulo ya existente (`sgi_res_partner_supplier_view_form`, hereda `base.view_partner_form`) editada en su `<record>`. **Sin migración**: todo son columnas nuevas vacías o calculadas sobre tablas con 0 filas (permisos), y registros nuevos. La ficha pedía un post-migrate que pasara los 5 riesgos ambientales a aspectos; es un cambio de datos de negocio y va como asistente manual con el OK de Jose (Q1). Pruebas nuevas en un archivo (`test_sst_ambiente.py`, 20 casos) y cinco pruebas existentes ajustadas.

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh), Python 3, XML de vistas, `odoo.tests.TransactionCase`. Checadores del repo: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`, `flake8`.

**Base de código verificada:** rama `claude/confident-mendel-8yg7xu` igual a `origin/main`, HEAD `05f476e9` (merge del PR #515, 19.0.57.95.0; manifest `19.0.57.95.0`). Las líneas citadas abajo son de este HEAD; si cambian, buscar por el nombre del método. **Producción está en 19.0.57.90.1** (MCP, 2026-10-02): 57.91.0 a 57.95.0 todavía no se despliegan; esta entrega se despliega con ellas o después. Lo que no se pudo comprobar sin Odoo va marcado «VERIFICAR:» (sección 1.10).

---

## 0. Reglas (resumen de la sección 0 del plan general y de `CLAUDE.md`)

Leer antes de empezar: `CLAUDE.md` (raíz), `addons/quimibond_sgi/README.md` (glosario y «usted»), las entradas 57.93.0, 57.94.0–57.94.2 y 57.95.0 de `addons/quimibond_sgi/CHANGELOG.md`, y `docs/audit/decisiones.md` (D-06 incidentes, D-009 quién cierra, D-15 MOC archivada).

1. **Versión y CHANGELOG:** `__manifest__.py` a `'19.0.57.96.0'` y `## 19.0.57.96.0 — AAAA-MM-DD` arriba de la 57.95.0. Esta entrega **agrega modelos** (`sgi.work.permit.skill`, `sgi.env.aspect.transfer`, `sgi.env.aspect.transfer.line`): sin bump, `tools/check_addons.py` da **error**.
2. **Pruebas registradas** en `tests/__init__.py` (el checker lo exige), clases con `@tagged('post_install', '-at_install')`.
3. **Las pruebas solo corren en Odoo.sh**, build de desarrollo de la rama, con `--test-tags /quimibond_sgi` (no el suite completo: las pruebas de nómina lo detienen). El CI no instala el SGI. Ciclo: prueba → push → ver el fallo en el build → código → checadores → push → leer el build.
4. **Checadores locales** (0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   flake8 addons/quimibond_sgi
   ```
5. **Sin herencias propias** de vistas del SGI: las fichas de riesgo, acción, incidente, permiso, aspecto y requisito legal se editan en su `<record>`. La única herencia que se toca es de otro módulo (`sgi_res_partner_supplier_view_form` → `base.view_partner_form`), editada en su `<record>`. Las vistas nuevas son primarias.
6. **`_name` en extensiones con varias clases:** ninguna clase nueva de esta entrega hereda una lista. `HrLeaveSgiIncident` (`_inherit = 'hr.leave'`) y `ResPartnerSstContractor` (`_inherit = 'res.partner'`) heredan un solo modelo; los modelos nuevos llevan `_name`. Si al implementar se agrega un mixin, poner `_name` igual al modelo heredado (57.94.1).
7. **Tipo de modelo en extensiones:** `hr.leave` y `res.partner` son `models.Model`; las extensiones también.
8. **Imports entre módulos de `models/`:** la lista de la jerarquía de controles vive en `models/sgi_control_hierarchy.py`, que **no define modelos** (patrón de `sgi_guard.py`); `sgi_risk.py` y `sgi_nonconformity.py` la importan sin mover el orden del registro. Las listas de aspecto (`ASPECT_TYPES`, `ASPECT_CONDITIONS`, `LIFE_CYCLE_STAGES`) se importan de `sgi_env_aspect.py` **solo** desde `sgi_env_aspect_transfer.py`, que va después en `models/__init__.py`.
9. **Accesibilidad en vistas:** un `<a class="btn …">` lleva `role="button"`; un elemento con clase `alert-*` lleva `role="alert"`, `"alertdialog"` o `"status"` (57.94.2). En un `<search>`, el `<group>` va sin atributos.
10. **Menús** solo en `views/sgi_menus.xml` y su árbol en `addons/quimibond_sgi/tools/sgi_menu_tree.txt` (`tests/test_menu_tree.py` lo compara). Esta entrega agrega **dos** menús (Tasks 6.4 y 6.8).
11. **«Usted» y glosario:** `tests/test_usted.py` revisa toda cadena nueva (nada de «tú», «tu», «puedes»; «Jefe MAST», «no conformidad», «las NC», «Comisión de Seguridad e Higiene»).
12. **Lecciones de 57.93–57.95 que aplican aquí:**
    - **La base del build es copia de producción:** ninguna prueba cuenta registros globales («hay 1 aspecto»); se comprueba con `assertIn` o filtrando por los registros de la prueba. Lo real que estorbe se neutraliza dentro de la prueba (se deshace al final): la configuración de competencias por tipo de permiso y el parámetro del contratista se apagan en `setUpClass`.
    - **Un `ERROR` en el log tumba las pruebas de Odoo.sh:** lo que se atrapa a propósito (un aviso que no se pudo agendar, una incapacidad cuyo incidente no se pudo crear) se registra con `_logger.warning(..., exc_info=True)`, nunca con `_logger.exception`.
    - **Anclas que quien recibe puede leer:** los avisos de permiso vencido van sobre el permiso (el jefe del área es Usuario SGI y lo lee); el aviso del incidente desde la incapacidad va al Jefe MAST, que lo lee. No se agenda nada sobre `hr.employee` ni `hr.leave`.
    - **`sudo` donde hace falta:** `hr.employee` (equipo de investigación, competencias) y `hr.leave` solo los lee RH; las revisiones del sistema leen con `sudo()` y escriben a nombre de quien actúa.
13. **Despliegue:** PR rama → `main`, revisar el build; PR `main` → `quimibond`; `odoo-update quimibond_sgi` según `docs/RUNBOOK_DESPLIEGUE.md`.
14. **Producción es de solo lectura.** El despliegue no cambia datos de negocio. El traspaso de los 5 riesgos ambientales a aspectos **no** es migración: es un asistente que el Jefe MAST corre a mano **después** del visto bueno escrito de Jose en el PR (Q1), con conteo previo, nota en cada registro y log antes y después. Por omisión los riesgos originales **se conservan** ligados al aspecto; se archivan solo si Jose lo pide. Nada se borra.

---

## 1. Decisiones de diseño (con evidencia del código)

### 1.1 Lo que se midió en producción (2026-10-02, MCP, solo conteos, compañía 1)

| Qué | Cifra |
|---|---|
| Versión instalada de `quimibond_sgi` | 19.0.57.90.1 |
| `sgi.risk` (25, todos activos) | IPER 8 (**5 alto**, 3 medio, todos «Identificado»), ambiental 5, R y O 7 (1 en tratamiento), FODA 4, patrimonial 1 |
| IPER de riesgo alto | 5, todos «Identificado», **sin acciones** (`high_without_action`), sin puesto, residual vacío. Ninguno está controlado ni cerrado: el candado nuevo no alcanza a nada existente |
| Riesgos ambientales (`instrument = 'ambiental'`) | **5** (ids 14–18, folios RSG-2026-09 a 13): 4 «Riesgo» y 1 «Oportunidad» (merma de fibra), todos «Identificado»; nivel 4 media (puntaje 12) y 1 intermedia (8); procesos C4 (3), C6 (1), S5 (1); **0 acciones, 0 NC, 0 control operacional**; controles existentes en texto; próxima revisión 2027-01-01 |
| Requisitos legales ligados a esos riesgos | 4: tres ambientales (ligados a 14, 15 y 18) y uno de SST (ligado a 16 y a otro riesgo) |
| Planes de emergencia ligados a esos riesgos | 0 |
| `sgi.env.aspect` | **0** (la matriz llena es el Excel 4868 en Documentos, CHANGELOG 57.82.0; no se abrió) |
| `sgi.legal.requirement` | 25, **todos «Sin evaluar»** (SST 17, ambiental 5, calidad 2, transversal 1) |
| `sgi.incident` / `sgi.work.permit` / `sgi.loto` | **0 / 0 / 0** |
| Grupos «Salud ocupacional (SGI)» (328) y «Comisión de Seguridad e Higiene (SGI)» (330) | **0 miembros** cada uno; «Jefe MAST y SGI» (318): 2 |
| Tipos de ausencia (`hr.leave.type`) | 10; **ya existe «Riesgo de trabajo (IMSS)»** (id 7, xmlid `hr_holidays.l10n_mx_leave_type_work_risk_imss`, sin asignación) y «Incapacidad por enfermedad (IMSS)» (id 9). La auditoría decía que no existía: no hace falta crearlo |
| Ausencias (`hr.leave`) | 3 en toda la base (1 cancelada, 1 por aprobar, 1 rechazada); **0** de los tipos 7 y 9 |
| Módulos | `hr_holidays` y `hr_skills` instalados; `frontdesk` **no** instalado |
| Competencias (`hr.skill`) | 61 en 4 tipos (Idiomas 21, Habilidades blandas 15, Auditor interno SGI 4, Capacitación SGI y STPS 21) |
| Contactos marcados como contratistas | 0 (no hay etiqueta «contratista»); el permiso solo tiene `contractor_id` |

Con esto: **los candados nuevos no tocan nada que exista** (0 incidentes, 0 permisos, 0 LOTO, 0 IPER controlados o cerrados, 0 aspectos). Lo único que cambia para registros existentes es que «Aspecto ambiental» ya no se puede **elegir** al crear o reclasificar un riesgo a mano, y que los botones rápidos de lo legal abren el asistente (los 25 requisitos siguen «Sin evaluar»).

### 1.2 Jerarquía de controles (N-06, 45001 8.1.2)

Hoy (`models/sgi_risk.py:56-165`): `sgi.risk` no tiene tipo de control (solo `existing_controls`, texto); `sgi.action.line` (`models/sgi_nonconformity.py:1009-1083`) tampoco. El candado H11 (`_sgi_check_can_close`, `sgi_risk.py:314-336`) pide para un riesgo de atención máxima una acción terminada y el residual a la baja; lo llama `write` al pasar a `controlado`/`cerrado` (l.338-344) y lo revalidan las acciones al reabrirse o borrarse (`sgi_nonconformity.py:1385-1388, 1398-1406`).

Decisión:
- Lista única en `models/sgi_control_hierarchy.py` (sin modelos): `eliminacion`, `sustitucion`, `ingenieria`, `administrativo`, `epp`, en ese orden y con el número en la etiqueta («1. Eliminación» … «5. Equipo de protección personal (EPP)»).
- `sgi.risk.control_hierarchy` («Control existente de mayor nivel») y `sgi.action.line.control_hierarchy` («Jerarquía del control»), opcionales y con `tracking`.
- **Candado nuevo, separado de H11** (`_sgi_check_control_hierarchy`): solo para `instrument = 'iper'` con `attention_level = 'alto'`, y **solo en la transición** a `controlado` o `cerrado` (registros cuyo estado de antes era otro). Los niveles que cuentan son el del riesgo y los de sus acciones **terminadas**. Sin ninguno: «indique la jerarquía»; si todos son EPP: «el EPP es el último recurso; agregue un control de mayor nivel». No se llama desde las acciones (reabrir o borrar una acción de un IPER ya controlado no lo dispara), así que **no es retroactivo**: un IPER controlado antes de 57.96.0 sigue igual hasta que alguien lo reabra y lo vuelva a controlar. Hoy hay 0 IPER controlados o cerrados.
- Sin excepción para el superusuario (igual que H11): ningún proceso del sistema controla riesgos.
- `create` no pasa por el candado (como H11): un riesgo creado directamente en «Controlado» (importación) no se revisa. Hoy nadie lo hace; se anota.
- Pregunta Q5: ¿se permite «solo EPP» con justificación escrita? Por omisión, no.

### 1.3 Aspecto ambiental en un solo lugar (N-07, 14001 6.1.2)

Hoy: `sgi.env.aspect` (`models/sgi_env_aspect.py`, 0 registros) ya se declara como «la evaluación vive aquí» y crea un riesgo `instrument='ambiental'` solo para el tratamiento (`action_create_risk`, l.215-239); pero `sgi.risk` deja elegir «Aspecto ambiental» a mano (`sgi_risk.py:56-64`) y por esa vía nacieron los 5 riesgos ambientales. No hay ciclo de vida.

Decisión:
- **«Aspecto ambiental» fuera del selector para registros nuevos:** el valor se conserva (los 5 existentes y los que crea el aspecto lo usan), pero `sgi.risk.create` y `write` rechazan `instrument='ambiental'` salvo con el contexto `sgi_from_env_aspect` (lo ponen `action_create_risk` y el asistente de traspaso). Un riesgo que **ya** es ambiental se sigue editando (nombre, acciones, estado). Mensaje: «Los aspectos ambientales se registran en Seguridad y ambiente → Aspectos ambientales…». Una `Selection` de Odoo no se puede filtrar por registro en la vista; el candado en Python es la garantía y la ficha del riesgo explica de dónde vienen los ambientales (aviso `role="status"`).
- Efecto a saber: **Duplicar** uno de los 5 riesgos ambientales también queda bloqueado (`copy` pasa por `create` con `instrument='ambiental'`); es lo buscado (el aspecto nuevo va en la matriz), pero el mensaje debe decirlo y el CHANGELOG lo anota.
- El contexto lo puede poner un cliente RPC: no es un control de seguridad, es una guía de captura (igual que otros contextos de creación del módulo); lo importante es que la interfaz ya no lleva a capturar el aspecto en el lugar equivocado.
- `sgi.risk.sgi_env_aspect_ids` (One2many inverso de `sgi.env.aspect.risk_id`) para ver desde el riesgo de qué aspecto es el tratamiento.
- **`life_cycle_stage`** en `sgi.env.aspect` (materia prima e insumos, proceso en planta, transporte y distribución, uso por el cliente, fin de vida y disposición). **Obligatorio para «Registrar evaluación»** (`_sgi_check_can_evaluate`), no en la base: 0 aspectos hoy, nada que rellenar. En ficha, lista y búsqueda (agrupar por etapa).
- Las listas de tipo, condición y etapa pasan a constantes del módulo (`ASPECT_TYPES`, `ASPECT_CONDITIONS`, `LIFE_CYCLE_STAGES`) para que el asistente use las mismas.
- **Fuera de esta entrega:** la sección 5 del procedimiento impreso (`sgi_process_procedure.py:393-396`, `report/report_procedure.xml:80`) sigue leyendo los riesgos ambientales del proceso; como por omisión los 5 riesgos se conservan ligados, el impreso no cambia. Pasarla a aspectos va con la matriz oficial cargada (Q2).

### 1.4 Traspaso de los 5 riesgos ambientales (N-07; la ficha pedía post-migrate)

Es un **cambio de datos de negocio** (crea 5 aspectos y escribe en los riesgos): no va en migración. Asistente **manual** `sgi.env.aspect.transfer` («Traspaso de riesgos ambientales a la matriz», Administración SGI → Configuración, solo Jefe MAST), con el patrón de `sgi.company.fix` (57.95.0):
- **Al abrir solo cuenta y propone:** un renglón por riesgo ambiental (activo o archivado) que **no** tenga ya un aspecto con `risk_id` a él. Cada renglón trae el riesgo (folio, proceso, tipo riesgo u oportunidad, puntaje) y lo que el Jefe MAST debe decidir porque el riesgo no lo dice: **actividad** (propuesta: el nombre del riesgo), **tipo de aspecto** (propuesta: «Otro»), **condición** (propuesta: «Normal») y **etapa del ciclo de vida** (vacía). Puede quitar renglones.
- **«Traspasar a la matriz»** (con confirmación): por renglón, en su savepoint, crea el aspecto **en «En evaluación»** (no evaluado: el Jefe MAST revisa y pulsa «Registrar evaluación»), con `name` = nombre del riesgo, `impact` = consecuencia (o el nombre), `severity` = impacto del riesgo, `frequency` = probabilidad (el mapeo inverso exacto de `action_create_risk`), proceso, área, `control_description` = controles existentes, documento de control, próxima revisión, los requisitos legales que ligan al riesgo y `risk_id` = el riesgo (el riesgo queda como su tratamiento). Nota en el aspecto («Traspasado desde el riesgo RSG-… el AAAA-MM-DD por <usuario>; 57.96.0») y en el riesgo («La evaluación de este aspecto vive ahora en la matriz: ASP-…»).
- **Los riesgos se conservan activos** por omisión (siguen en el impreso del procedimiento y en las listas de riesgos; desde la ficha se ve el aspecto). Casilla «Archivar los riesgos originales» (apagada): solo si Jose lo pide (Q1). Nada se borra.
- **Log** antes (ids de los riesgos) y después (cuántos aspectos, cuántos fallaron y sus ids, con `WARNING` para lo que falló). **Idempotente:** un riesgo con aspecto ya no aparece; volver a abrir el asistente cuenta 0.
- **Efecto a saber:** los umbrales de la matriz (moderado ≥ 5, severo ≥ 10, crítico ≥ 16) no son los del riesgo (intermedia ≥ 4, media ≥ 9, inmediata ≥ 16): los 4 de puntaje 12 quedan «Severo» y el de 8 «Moderado»; **los 5 serán significativos** y piden control operacional para evaluarse (ya tienen el texto de controles existentes). Y la «Oportunidad» (merma de fibra) entra como aspecto (residuo); si Jose prefiere dejarla solo como oportunidad, se quita su renglón (Q1).

### 1.5 Lo legal con evidencia (N-07, 14001/45001 9.1.2)

Hoy (`models/sgi_legal.py:163-195`): `action_mark_cumple`, `_parcial`, `_no_cumple` y `_no_aplica` llaman `_sgi_mark(state)` sin evidencia; el asistente `sgi.legal.evaluate` (l.277-313) sí la exige (`evidence`, `required=True`). Los cuatro botones están en la ficha del requisito, solo para el Jefe MAST (`views/sgi_legal_views.xml:73-81`).

Decisión: los cuatro métodos **abren el asistente** con `default_result` (mismo nombre de método: la vista no cambia de botones, solo pierden el `confirm`, porque la confirmación es el asistente). «No aplica» muestra la etiqueta «Motivo por el que no aplica» sobre el mismo campo obligatorio. `_sgi_mark` no cambia; la NC por «Parcial» o «No cumple» la sigue levantando `action_confirm`. Ajuste: `tests/test_ola_certificable.py` (`test_legal_no_cumple_creates_nc`, `test_legal_source_off_no_nc`) registra con el asistente.

### 1.6 Incidente: equipo de investigación, eficacia e IPER reevaluado (N-06, 45001 5.4 y 10.2)

Hoy (`models/sgi_incident.py`): sin participantes de la investigación; `_sgi_check_can_close` (l.259-282) pide SCAT, acciones terminadas y, si es grave o fatal, IPER ligado; se revisa **antes** de escribir (l.161-162), salvo superusuario.

Decisión:
- `investigation_team_ids` (Many2many a `hr.employee`; el modelo ya hereda `hr.mixin`, así que quien no es de RH puede capturarlo). Para cerrar: al menos un integrante que sea **trabajador** (empleado sin personal a su cargo, `child_ids` vacío, leído con `sudo`) **o integrante de la Comisión de Seguridad e Higiene** (usuario en `quimibond_sgi.group_sgi_csh`). Con la Comisión sin miembros (hoy), basta un trabajador. Pregunta Q8.
- **Eficacia como en la NC** (57.93.0): `sgi_effective` (Eficaz / No eficaz), `sgi_effectiveness_date`, `sgi_effectiveness_note`, `sgi_effectiveness_by` (lo pone el sistema) y `sgi_ineffective_count`. Cerrar exige «Eficaz», nota y fecha (no futura, no antes de la última acción terminada). «No eficaz» deja nota en el historial, suma el contador, limpia la verificación, **regresa el incidente a «Acciones»** y agenda «Registrar acción nueva del incidente …» a quien la registró; el cierre pide entonces una acción **terminada de la ronda nueva** (`sgi.action.line.effectiveness_round`, el mismo campo de la NC, se llena al crear la acción del incidente). Solo el Jefe MAST y Salud ocupacional registran la eficacia (como investigar y cerrar, D-06/D-009). No se exige esperar N días (la NC espera 90): pregunta Q9.
- **IPER reevaluado:** si el incidente es moderado, grave o fatal y tiene riesgo ligado, la última evaluación del riesgo (`last_eval_date`) debe ser del día del incidente o posterior («Registrar evaluación» en el riesgo).
- El candado de cierre pasa a revisarse **después** de escribir (como la NC desde 57.93.0): lo que el formulario manda junto con el estado cuenta. Sigue exento el superusuario (pruebas y sistema), como hoy.
- **No retroactivo:** 0 incidentes en producción. Las pruebas existentes que cierran incidentes con un usuario real (`test_entrega4.test_07`, `test_audit_hardening.test_a6`) agregan equipo y eficacia (Task 6.1, paso 2).

### 1.7 Permiso de trabajo: vencido guardado, aviso cada hora y LOTO al cerrar (N-06)

Hoy (`models/sgi_work_permit.py`): `expired` es calculado sin guardar (l.108, 144-149): no se puede buscar ni avisar; ningún cron lo revisa. `action_close` (l.256-265) no mira el LOTO ligado (`sgi.loto.work_permit_id`, `sgi_loto.py:46`).

Decisión:
- `expired` **guardado e indexado** (`@api.depends('date_end', 'state')`; al guardar compara con la hora de ese momento) y método `_sgi_expired_on(now)`. El paso del tiempo lo pone la acción planificada nueva **«SGI: Permisos de trabajo vencidos (cada hora)»** (`cron_work_permits`, `data/sgi_sst_cron.xml`, `noupdate`, registro nuevo en archivo nuevo): escribe `expired` solo donde cambió (`_sgi_write_changed`, el patrón de `sgi.action.line.state`, que también es calculado guardado) y agenda sobre el permiso «Permiso de trabajo vencido: PTAR-…» al **jefe del área** (`area_manager_id`; si no hay, quien lo solicitó) con clave `permiso_vencido`, y al **Jefe MAST** con clave `permiso_vencido_mast` (si no es la misma persona). Cierre por episodio (`_sgi_sweep`) cuando el permiso se cierra, se cancela o se renueva. Al crear la columna, el ORM calcula `expired` para los permisos existentes (0 en producción).
- **LOTO:** `loto_ids` (One2many inverso) en el permiso, con una pestaña de solo lectura. Cerrar el permiso (botón o `write` de `state`) con un bloqueo ligado en «Bloqueado» → `UserError` que nombra el bloqueo. Sin excepción para el Jefe MAST: cerrar con energía bloqueada es justo lo que no debe pasar. **Cancelar** el permiso con el bloqueo aplicado no se bloquea en esta entrega (el bloqueo sigue vivo y se retira por su cuenta); si Jose lo quiere igual que el cierre, basta con `vals.get('state') in ('cerrado', 'cancelado')` en el `write`.

### 1.8 Competencia por tipo de permiso y evaluación SST del contratista (N-06, 45001 7.2 y 8.1.4)

- **Modelo de configuración** `sgi.work.permit.skill` («Competencias por tipo de permiso», Administración SGI → Configuración): tipo de trabajo (la misma lista `WORK_TYPES`), competencia (`hr.skill`), por qué se exige (NOM, DC-3) y archivado. Única por tipo y competencia.
- **Candado** al solicitar (`action_submit`) y al autorizar (`_sgi_check_can_authorize`): cada persona del personal que ejecuta debe tener cada competencia exigida para el tipo **vigente hasta el fin del permiso** (`hr.employee.skill` con `valid_to` vacío o ≥ la fecha de fin; leído con `sudo`). **Sin configuración no cambia nada** (hoy: 0 filas): es opt-in hasta que el Jefe MAST capture qué exige cada tipo.
- **Contratista:** en el contacto (pestaña SGI, grupo nuevo «Contratista (SST, 45001 8.1.4)»): `sgi_sst_eval_valid_until` («Evaluación SST vigente hasta») y `sgi_sst_eval_note` («Qué se revisó»: REPSE, SUA, DC-3, inducción). Solo el Jefe MAST los escribe (`write` del contacto). Se lee de la **empresa** del contacto (`commercial_partner_id`).
- En el permiso, `sgi_contractor_eval_ok` (sin guardar) y un aviso `role="alert"` cuando el contratista no tiene evaluación vigente hasta el fin. **Por omisión solo avisa.** Con el parámetro `quimibond_sgi.permit_contractor_eval_required = 1` bloquea la solicitud y la autorización. Q11 sigue abierta (quién evalúa, Frontdesk): pregunta Q4.

### 1.9 Incidente desde la incapacidad por riesgo de trabajo (N-06, D7 de la auditoría)

Hoy: el SGI no depende de `hr_holidays` y no sabe de ausencias. Producción ya tiene el tipo «Riesgo de trabajo (IMSS)» (1.1); 0 ausencias de ese tipo.

Decisión:
- **Dependencia nueva** `hr_holidays` en el manifest (instalado en producción; trae el tipo). Pregunta Q7.
- Tipos que cuentan: el parámetro `quimibond_sgi.work_risk_leave_type_ids` (ids separados por coma) si está puesto; si no, `hr_holidays.l10n_mx_leave_type_work_risk_imss`. Sin tipo, no hace nada. **No se crea un tipo nuevo** («Incapacidad por riesgo de trabajo» de la ficha ya existe con otro nombre).
- `hr.leave.sgi_incident_id` (solo lectura, `groups` RH de ausencias, Jefe MAST y Salud ocupacional). En `create` y `write`, cuando una ausencia de esos tipos **pasa a «Aprobado»** (`state = 'validate'`): si ya tiene incidente, solo refresca los días; si la misma persona tiene una incapacidad aprobada ligada a un incidente **no cerrado** que terminó hasta 3 días antes (subsecuente del IMSS; parámetro `quimibond_sgi.work_risk_followup_days`), se liga a ese incidente; si no, crea el incidente con `sudo` en **«Reportado»**: tipo «Lesión / accidente», severidad **moderado** (lesión con días perdidos; quien investiga la reclasifica), fecha = inicio de la incapacidad, persona afectada, días perdidos = suma redondeada de sus incapacidades aprobadas, `sgi_from_leave = True`, reportado por quien aprobó (usuario y su empleado). **El título no lleva diagnóstico ni la descripción de la ausencia**: «Riesgo de trabajo (incapacidad IMSS) del AAAA-MM-DD». Agenda al Jefe MAST «Investigar riesgo de trabajo INC-…» (clave `incidente_incapacidad`).
- **Idempotente ante dos pasadas:** según el camino de `hr_holidays`, una ausencia que nace aprobada puede pasar por el `write` (dentro del `create` de Odoo) y luego por el `create` de aquí. La segunda pasada encuentra la ausencia ya ligada y solo refresca los días (sin nota ni segundo aviso). Un incidente ya cerrado no cambia sus días.
- **Rechazar o cancelar** una incapacidad ya ligada: nota en el incidente y días recalculados si el incidente no está cerrado. Nada se borra.
- **La aprobación nunca falla por el SGI:** cada ausencia en su savepoint; si algo truena, `WARNING` con la traza y la aprobación sigue.
- Datos sensibles: el incidente solo lo leen quien lo reportó, el Jefe MAST, Salud ocupacional y el Auditor (reglas actuales). En el incidente se ven solo el número de incapacidades y la suma de días (`sgi_leave_count`, `sgi_leave_days`, calculados con `sudo`), nunca el tipo ni la descripción de la ausencia.

### 1.10 Lo que no se pudo verificar fuera de Odoo.sh

No hay fuentes de Odoo 19 en este entorno. Confirmar en el shell de Odoo.sh antes de empujar el código (Task 6.1, paso 4):
- VERIFICAR: que `hr.leave` en Odoo 19 pasa a «Aprobado» por `write({'state': 'validate'})` en todos los caminos (`action_approve`, `action_validate`, `_action_validate`, tipo sin validación al crear): `grep -n "def action_approve\|def action_validate\|def _action_validate\|'state': 'validate'\|state='validate'" /home/odoo/src/odoo/addons/hr_holidays/models/hr_leave.py`. Si algún camino escribe la columna por SQL o en `create` con `state='validate'` sin `write`, engancharse además en ese método (el `create` del plan ya cubre lo que nace aprobado). Y los nombres reales de los métodos de aprobar y rechazar que usa la prueba 19 (`action_approve` / `action_validate` / `action_refuse`).
- ~~VERIFICAR~~ **Verificado por MCP en producción (revisión 2026-10-02):** `hr.leave.type.leave_validation_type` tiene `no_validation`, `hr`, `manager`, `both`; `requires_allocation` es booleano; `hr.leave.state` es un campo guardado **sin cálculo** (`depends` vacío; valores `confirm`, `refuse`, `validate1`, `validate`, `cancel`) y `hr.leave` **no tiene `active`** (lo cancelado queda en `cancel`); existen `private_name`, `number_of_days` (guardado), `request_date_from/to`, `date_from`. El xmlid `hr_holidays.l10n_mx_leave_type_work_risk_imss` existe (módulo `hr_holidays`, `hr.leave.type` id 7). `res.groups.all_user_ids` existe.
- ~~VERIFICAR~~ **Verificado por MCP:** `hr.employee.skill` tiene `valid_from`, `valid_to` (guardados), `skill_type_id`, `skill_level_id` (requeridos) e `is_certification`; `hr.skill.type` tiene `skill_ids`, `skill_level_ids`, `is_certification`; `hr.skill.level` tiene `level_progress` y `default_level`. Los 4 tipos de producción tienen `is_certification = False` (también «Capacitación SGI y STPS (F-P-A01-04)»). Queda por ver en Odoo.sh si Odoo 19 guarda `valid_to` en tipos que no son certificación; la prueba 16 usa `is_certification=True` para no depender de ello (ver Q10).
- VERIFICAR: que el Jefe MAST de prueba con `base.group_partner_manager` puede escribir en `res.partner` (prueba 17), y que un Usuario SGI recibe el `UserError` del candado antes que el `AccessError` del ACL (el `write` del SGI revisa primero).
- VERIFICAR: que escribir un campo calculado guardado sin `inverse` (`expired`) desde el cron se conserva hasta que cambie una dependencia (mismo caso que `sgi.action.line.state`, que el cron ya escribe con `_sgi_write_changed`): la prueba 14 lo confirma.
- VERIFICAR: que agregar `hr_holidays` a `depends` de un módulo ya instalado no exige reinstalar (en 17/18 `-u` lee el manifest y agrega la dependencia): revisar el `update.log` del build de la rama.
- VERIFICAR: que el `Many2one` a `hr.leave` con `groups` no rompe la lectura del incidente para el Auditor (el campo está en `hr.leave`, no en el incidente; en el incidente solo hay conteos calculados con `sudo`).
- Verificado en el repo: `sgi.incident` hereda `hr.mixin` (l.22) y `sgi.work.permit` también (l.75); `sgi.action.line.effectiveness_round` se llena en `create` desde la NC (`sgi_nonconformity.py:1331-1333`) y solo el sistema lo cambia (l.1352-1353); `_sgi_write_changed` escribe calculados guardados (`sgi_cron.py:1229-1238`); `sgi_require_system` (`models/sgi_guard.py`); `sgi.loto.lock` deja retirar candados al superusuario (`sgi_loto.py:~204`).

---

## 2. Archivos

- Create: `addons/quimibond_sgi/models/sgi_control_hierarchy.py` (constantes, sin modelos)
- Modify: `addons/quimibond_sgi/models/sgi_risk.py` (import; campos `control_hierarchy`, `sgi_env_aspect_ids`; `create`; `write`; `_sgi_check_control_hierarchy`)
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`SgiActionLine.control_hierarchy`; `create`: ronda del incidente y cierre del aviso «Registrar acción nueva del incidente»)
- Modify: `addons/quimibond_sgi/models/sgi_env_aspect.py` (constantes; `life_cycle_stage`; `_sgi_check_can_evaluate`; `action_create_risk` con contexto)
- Create: `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py`
- Modify: `addons/quimibond_sgi/models/sgi_legal.py` (`_sgi_open_evaluate` y los cuatro botones)
- Modify: `addons/quimibond_sgi/models/sgi_incident.py` (campos de investigación, eficacia e incapacidad; `write`; `_sgi_check_can_close`; `_sgi_on_ineffective`)
- Modify: `addons/quimibond_sgi/models/sgi_work_permit.py` (`expired` guardado; `loto_ids`; `write`; competencias y contratista; modelo `sgi.work.permit.skill`; extensión de `res.partner`)
- Create: `addons/quimibond_sgi/models/sgi_incident_leave.py`
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (`cron_work_permits`)
- Modify: `addons/quimibond_sgi/models/__init__.py` (al final: `sgi_env_aspect_transfer`, `sgi_incident_leave`)
- Create: `addons/quimibond_sgi/data/sgi_sst_cron.xml`
- Create: `addons/quimibond_sgi/views/sgi_env_aspect_transfer_views.xml`, `addons/quimibond_sgi/views/sgi_work_permit_skill_views.xml`
- Modify: `addons/quimibond_sgi/views/sgi_risk_views.xml`, `sgi_action_line_views.xml`, `sgi_incident_views.xml`, `sgi_work_permit_views.xml`, `sgi_env_aspect_views.xml`, `sgi_legal_views.xml`, `sgi_res_partner_views.xml`, `sgi_menus.xml`; `addons/quimibond_sgi/tools/sgi_menu_tree.txt`
- Modify: `addons/quimibond_sgi/security/ir.model.access.csv`
- Modify: `addons/quimibond_sgi/__manifest__.py` (versión, `hr_holidays` en `depends`, archivos nuevos)
- Create: `addons/quimibond_sgi/tests/test_sst_ambiente.py`; Modify: `tests/__init__.py`, `tests/test_entrega4.py`, `tests/test_audit_hardening.py`, `tests/test_ola_certificable.py`, `tests/test_env_aspect.py`, `tests/test_work_permit.py`
- Modify: `docs/sgi/usuarios/mast.md`, `jefe-de-area.md`, `operador-o-supervisor.md`, `rh.md`, `docs/sgi/administracion/manual-jefe-mast.md`
- Modify: `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), `docs/audit/decisiones.md` (cuando Jose conteste Q1–Q11)

---

## Task 6.0: Rama

- [x] **Step 1:** seguir en `claude/confident-mendel-8yg7xu` (igual a `origin/main`). Si se trabaja fuera de esa sesión: `git fetch origin main && git checkout -B claude/sgi-57-96-sst-ambiente origin/main`. Confirmar `grep -n "'version'" addons/quimibond_sgi/__manifest__.py` → `19.0.57.95.0`.

## Task 6.1: Pruebas nuevas y ajuste de las existentes (fallan antes del código)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_sst_ambiente.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py` (al final)
- Modify: `tests/test_entrega4.py` (`test_07`), `tests/test_audit_hardening.py` (`test_a6`), `tests/test_ola_certificable.py` (dos pruebas legales), `tests/test_env_aspect.py` (`_aspect`), `tests/test_work_permit.py` (`setUpClass`)

- [x] **Step 1: Escribir `tests/test_sst_ambiente.py`**

```python
# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10: N-06, N-07 e incidente desde la incapacidad):
SST y ambiente.

- Jerarquía de controles (45001 8.1.2): un IPER de riesgo alto con solo EPP,
  o sin jerarquía, no se controla ni se cierra; no es retroactivo.
- El aspecto ambiental vive en la matriz: «Aspecto ambiental» no se elige a
  mano en un riesgo; etapa del ciclo de vida para evaluar; asistente manual
  que traspasa los riesgos ambientales a aspectos (cuenta, aplica, idempotente,
  conserva los riesgos salvo que se pida archivarlos).
- Lo legal con evidencia: los botones rápidos abren el asistente.
- Incidente: equipo de investigación con un trabajador o un integrante de la
  Comisión de Seguridad e Higiene, verificación de eficacia y, si es moderado
  o más, IPER reevaluado después del incidente.
- Permiso de trabajo: vencido guardado y avisado cada hora; no se cierra con
  un bloqueo (LOTO) aplicado; competencia exigida por tipo; evaluación SST del
  contratista (avisa o bloquea según el parámetro).
- Una incapacidad por riesgo de trabajo aprobada crea el incidente."""
from datetime import date, datetime, timedelta
from unittest.mock import patch

from odoo import fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import assert_locked, sgi_set_mast, sgi_test_user


class _SstCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.today = fields.Date.context_today(env.user)
        cls.mast = sgi_set_mast(env, login='zst_mast')
        cls.sgi_user = sgi_test_user(env, 'zst_usuario', 'quimibond_sgi.group_sgi_user')
        cls.process = env['sgi.process'].create({'code': 'ZST', 'name': 'Proceso 57.96'})
        cls.Risk = env['sgi.risk']
        cls.Activity = env['mail.activity'].with_context(active_test=False)

    def _done_action(self, record, level=False, **vals):
        field = 'risk_id' if record._name == 'sgi.risk' else 'incident_id'
        return self.env['sgi.action.line'].create(dict({
            field: record.id, 'name': 'Control ZST %s' % (level or 'sin nivel'),
            'responsible_id': self.mast.id, 'date_commit': self.today,
            'date_done': self.today, 'control_hierarchy': level}, **vals))

    def _notices(self, record, kind):
        return self.Activity.search([('res_model', '=', record._name), ('res_id', '=', record.id),
                                     ('sgi_cron_kind', '=', kind)])

    def _locked(self, text, method, *args, **kwargs):
        """Candado (UserError, no permiso) cuyo mensaje dice ``text``.

        No usar ``assertRaisesRegex``: el ``assertRaises`` de Odoo abre un
        savepoint y limpia la caché al fallar; ``assertRaisesRegex`` (unittest)
        no. Con los candados que se revisan DESPUÉS de escribir (cierre del
        incidente, H11 y jerarquía del riesgo) el estado quedaría escrito y el
        resto de la prueba correría sobre un registro ya «cerrado»."""
        with self.assertRaises(UserError) as caught:
            method(*args, **kwargs)
        self.assertNotIsInstance(caught.exception, AccessError, str(caught.exception))
        self.assertIn(text, str(caught.exception))


@tagged('post_install', '-at_install')
class TestJerarquiaControles(_SstCase):

    def _iper_alto(self, **vals):
        return self.Risk.create(dict({
            'name': 'Caída de altura ZST', 'instrument': 'iper', 'process_id': self.process.id,
            'eval_probability': '3', 'eval_impact': '3',
            'residual_probability': '1', 'residual_impact': '2'}, **vals))

    def test_01_iper_alto_con_solo_epp_no_se_controla(self):
        risk = self._iper_alto(control_hierarchy='epp')
        self.assertEqual(risk.attention_level, 'alto')
        self._done_action(risk, 'epp')
        risk.with_user(self.mast).action_set_en_tratamiento()
        assert_locked(self, risk.with_user(self.mast).action_set_controlado)
        self.assertEqual(risk.state, 'en_tratamiento')
        self._done_action(risk, 'ingenieria')
        risk.with_user(self.mast).action_set_controlado()
        self.assertEqual(risk.state, 'controlado')
        self.assertIn('control_hierarchy', self.env['sgi.action.line']._fields)

    def test_02_sin_jerarquia_no_se_controla_y_otros_no_cambian(self):
        risk = self._iper_alto()
        self._done_action(risk)
        self._locked("jerarquía", risk.with_user(self.mast).write, {'state': 'controlado'})
        # R y O de atención inmediata: sigue solo con H11 (acción y residual).
        ryo = self.Risk.create({
            'name': 'Riesgo inmediato ZST', 'instrument': 'ryo', 'eval_probability': '5',
            'eval_impact': '5', 'residual_probability': '1', 'residual_impact': '1'})
        self._done_action(ryo)
        ryo.with_user(self.mast).write({'state': 'controlado'})
        # IPER medio: sin candado nuevo.
        medio = self.Risk.create({'name': 'IPER medio ZST', 'instrument': 'iper',
                                  'eval_probability': '2', 'eval_impact': '2'})
        medio.with_user(self.mast).write({'state': 'controlado'})
        self.assertEqual((ryo.state, medio.state), ('controlado', 'controlado'))

    def test_03_no_es_retroactivo(self):
        risk = self._iper_alto()
        line = self._done_action(risk)
        # Un IPER controlado antes de 57.96.0 (sin jerarquía).
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_risk SET state = 'controlado' WHERE id = %s", (risk.id,))
        risk.invalidate_recordset(['state'])
        line.write({'name': 'Control renombrado ZST'})
        risk.with_user(self.mast).action_evaluate()
        self.assertEqual(risk.state, 'controlado', "Editar y evaluar no dispara el candado.")
        risk.with_user(self.mast).action_set_en_tratamiento()
        assert_locked(self, risk.with_user(self.mast).action_set_controlado)


@tagged('post_install', '-at_install')
class TestAspectoUnicaFuente(_SstCase):

    def _aspect(self, **vals):
        return self.env['sgi.env.aspect'].create(dict({
            'process_id': self.process.id, 'activity': 'Lavado de tambos ZST',
            'name': 'Descarga con residuos ZST', 'impact': 'Contaminación del agua',
            'aspect_type': 'descarga', 'severity': '4', 'frequency': '3',
            'control_description': 'Trampa de grasas'}, **vals))

    def test_04_ambiental_solo_desde_la_matriz(self):
        self._locked("Aspectos ambientales", self.Risk.create,
                     {'name': 'Ambiental a mano ZST', 'instrument': 'ambiental'})
        ryo = self.Risk.create({'name': 'R y O ZST', 'instrument': 'ryo'})
        with self.assertRaises(UserError):
            ryo.write({'instrument': 'ambiental'})
        aspect = self._aspect(life_cycle_stage='proceso')
        aspect.action_create_risk()
        self.assertEqual(aspect.risk_id.instrument, 'ambiental')
        self.assertIn(aspect, aspect.risk_id.sgi_env_aspect_ids)
        aspect.risk_id.write({'name': 'Tratamiento renombrado ZST'})

    def test_05_ciclo_de_vida_para_evaluar(self):
        aspect = self._aspect()
        self._locked("ciclo de vida", aspect.action_evaluate)
        aspect.life_cycle_stage = 'fin_vida'
        aspect.action_evaluate()
        self.assertEqual(aspect.state, 'evaluado')


@tagged('post_install', '-at_install')
class TestTraspasoAmbiental(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Risk = cls.Risk.with_context(sgi_from_env_aspect=True)
        cls.risk_a = Risk.create({
            'name': 'Descarga de agua de tintorería ZST', 'instrument': 'ambiental',
            'kind': 'riesgo', 'process_id': cls.process.id, 'eval_probability': '3',
            'eval_impact': '4', 'consequence': 'Exceder límites de descarga',
            'existing_controls': 'Análisis periódicos'})
        cls.risk_b = Risk.create({
            'name': 'Merma de fibra ZST', 'instrument': 'ambiental', 'kind': 'oportunidad',
            'process_id': cls.process.id, 'eval_probability': '4', 'eval_impact': '3'})
        cls.requirement = cls.env['sgi.legal.requirement'].create({
            'name': 'NOM-001 ZST', 'system': 'ambiental', 'risk_ids': [(6, 0, cls.risk_a.ids)]})
        cls.mine = cls.risk_a | cls.risk_b
        cls.Wizard = cls.env['sgi.env.aspect.transfer']
        cls.Aspect = cls.env['sgi.env.aspect'].with_context(active_test=False)

    def _wizard(self, **vals):
        wizard = self.Wizard.with_user(self.mast).create(vals)
        # La copia de producción trae sus 5 riesgos ambientales: fuera de la prueba.
        (wizard.line_ids - wizard.line_ids.filtered(lambda l: l.risk_id in self.mine)).unlink()
        return wizard

    def test_06_al_abrir_solo_cuenta(self):
        wizard = self._wizard()
        self.assertEqual(wizard.line_ids.risk_id, self.mine)
        line_a = wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a)
        self.assertEqual((line_a.activity, line_a.aspect_type, line_a.condition),
                         (self.risk_a.name, 'otro', 'normal'))
        self.assertFalse(self.Aspect.search([('risk_id', 'in', self.mine.ids)]), "Abrir no escribe.")
        with self.assertRaises(AccessError):
            self.Wizard.with_user(self.sgi_user).create({})

    def test_07_traspasa_ligado_e_idempotente(self):
        wizard = self._wizard()
        wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a).write(
            {'aspect_type': 'descarga', 'life_cycle_stage': 'proceso'})
        wizard.action_apply()
        aspect = self.Aspect.search([('risk_id', '=', self.risk_a.id)])
        self.assertEqual(len(aspect), 1)
        self.assertEqual((aspect.name, aspect.impact, aspect.severity, aspect.frequency),
                         (self.risk_a.name, 'Exceder límites de descarga', '4', '3'))
        self.assertEqual((aspect.aspect_type, aspect.life_cycle_stage, aspect.state),
                         ('descarga', 'proceso', 'borrador'))
        self.assertEqual(aspect.process_id, self.process)
        self.assertEqual(aspect.control_description, 'Análisis periódicos')
        self.assertEqual(aspect.legal_requirement_ids, self.requirement)
        self.assertTrue(self.risk_a.active, "Por omisión el riesgo se conserva.")
        self.assertIn(aspect.folio, self.risk_a.message_ids[:1].body)
        self.assertEqual(wizard.done_count, 2)
        again = self._wizard()
        self.assertFalse(again.line_ids, "Con su aspecto ya no se proponen.")

    def test_08_archivar_solo_si_se_pide(self):
        wizard = self._wizard(archive_risks=True)
        wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a).unlink()
        wizard.action_apply()
        self.assertFalse(self.risk_b.active)
        self.assertEqual(self.Aspect.search([('risk_id', '=', self.risk_b.id)]).risk_id, self.risk_b)
        self.assertTrue(self.risk_a.active, "El renglón quitado no se toca.")


@tagged('post_install', '-at_install')
class TestLegalConEvidencia(_SstCase):

    def test_09_botones_rapidos_abren_el_asistente(self):
        req = self.env['sgi.legal.requirement'].create({'name': 'Permiso de descarga ZST',
                                                        'system': 'ambiental'})
        for method, result in (('action_mark_cumple', 'cumple'), ('action_mark_parcial', 'parcial'),
                               ('action_mark_no_cumple', 'no_cumple'),
                               ('action_mark_no_aplica', 'no_aplica')):
            action = getattr(req.with_user(self.mast), method)()
            self.assertEqual(action['res_model'], 'sgi.legal.evaluate')
            self.assertEqual(action['context']['default_result'], result)
        self.assertEqual(req.compliance_state, 'pendiente', "Ningún botón registra sin evidencia.")
        self.assertFalse(req.evaluation_ids)
        wizard = self.env['sgi.legal.evaluate'].with_user(self.mast).with_context(
            action['context']).create({'evidence': 'La planta no descarga a cuerpo federal'})
        wizard.action_confirm()
        self.assertEqual(req.compliance_state, 'no_aplica')
        self.assertEqual(req.evaluation_ids[:1].evidence, 'La planta no descarga a cuerpo federal')


@tagged('post_install', '-at_install')
class TestIncidenteInvestigacion(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Employee = cls.env['hr.employee']
        cls.boss = Employee.create({'name': 'Jefe de turno ZST'})
        cls.worker = Employee.create({'name': 'Operador ZST', 'parent_id': cls.boss.id})
        cls.csh_user = new_test_user(cls.env, login='zst_csh',
                                     groups='base.group_user,quimibond_sgi.group_sgi_csh')
        cls.csh_boss = Employee.create({'name': 'Supervisor CSH ZST', 'user_id': cls.csh_user.id})
        Employee.create({'name': 'Ayudante ZST', 'parent_id': cls.csh_boss.id})

    def _incident(self, **vals):
        incident = self.env['sgi.incident'].create(dict({
            'name': 'Golpe en cortadora ZST', 'severity': 'leve', 'incident_type': 'lesion',
            'date': fields.Datetime.now() - timedelta(days=2),
            'immediate_causes': 'a', 'basic_causes': 'b', 'lack_of_control': 'c'}, **vals))
        self._done_action(incident, 'ingenieria', date_done=self.today - timedelta(days=1))
        incident.with_user(self.mast).action_set_investigacion()
        return incident

    def _effective(self, incident, **vals):
        incident.with_user(self.mast).write(dict({
            'sgi_effective': 'eficaz', 'sgi_effectiveness_date': self.today,
            'sgi_effectiveness_note': 'Sin repetición en dos semanas'}, **vals))

    def test_10_equipo_con_trabajador_o_comision(self):
        incident = self._incident()
        self._effective(incident)
        assert_locked(self, incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).investigation_team_ids = [(6, 0, self.boss.ids)]
        self._locked("trabajador", incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).investigation_team_ids = [(4, self.worker.id)]
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')
        other = self._incident()
        self._effective(other, investigation_team_ids=[(6, 0, self.csh_boss.ids)])
        other.with_user(self.mast).action_set_cerrado()
        self.assertEqual(other.state, 'cerrado', "Un integrante de la Comisión basta.")

    def test_11_eficacia_y_no_eficaz(self):
        incident = self._incident(investigation_team_ids=[(6, 0, self.worker.ids)])
        self._locked("eficacia", incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).write({'sgi_effective': 'no_eficaz',
                                             'sgi_effectiveness_note': 'Se repitió el golpe'})
        self.assertEqual(incident.state, 'acciones')
        self.assertEqual(incident.sgi_ineffective_count, 1)
        self.assertFalse(incident.sgi_effective)
        self.assertTrue(incident.activity_ids.filtered(
            lambda a: a.summary.startswith("Registrar acción nueva del incidente")))
        self._effective(incident)
        self._locked("No eficaz", incident.with_user(self.mast).action_set_cerrado)
        self._done_action(incident, 'administrativo')
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')
        late = self._incident(investigation_team_ids=[(6, 0, self.worker.ids)])
        self._effective(late, sgi_effectiveness_date=self.today - timedelta(days=2))
        assert_locked(self, late.with_user(self.mast).action_set_cerrado)

    def test_12_iper_reevaluado_despues_del_incidente(self):
        risk = self.Risk.create({'name': 'Atrapamiento ZST', 'instrument': 'iper',
                                 'eval_probability': '2', 'eval_impact': '2',
                                 'last_eval_date': self.today - timedelta(days=30)})
        incident = self._incident(severity='moderado', risk_id=risk.id,
                                  investigation_team_ids=[(6, 0, self.worker.ids)])
        self._effective(incident)
        self._locked("IPER", incident.with_user(self.mast).action_set_cerrado)
        risk.action_evaluate()
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')

    def test_13_solo_quien_investiga_registra_la_eficacia(self):
        incident = self.env['sgi.incident'].create({
            'name': 'Resbalón ZST', 'incident_type': 'casi_accidente',
            'reporter_id': self.sgi_user.id, 'reporter_employee_id': False})
        assert_locked(self, incident.with_user(self.sgi_user).write, {'sgi_effective': 'eficaz'})


@tagged('post_install', '-at_install')
class TestPermisoDeTrabajo(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # La copia de producción puede traer configuración: fuera de la prueba.
        env['sgi.work.permit.skill'].search([]).write({'active': False})
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.permit_contractor_eval_required', '0')
        cls.requester = sgi_test_user(env, 'zst_solicita', 'quimibond_sgi.group_sgi_user')
        cls.boss = sgi_test_user(env, 'zst_jefe_area', 'quimibond_sgi.group_sgi_user')
        cls.worker = env['hr.employee'].create({'name': 'Electricista ZST'})
        cls.equipment = env['maintenance.equipment'].create({'name': 'Carda ZST'})
        cls.Cron = env['sgi.cron']

    def _permit(self, **vals):
        start = datetime(2046, 5, 4, 8, 0)
        permit = self.env['sgi.work.permit'].with_user(self.requester).create(dict({
            'name': 'Cambio de interruptor ZST', 'work_type': 'electrico', 'location': 'Subestación',
            'date_start': start, 'date_end': start + timedelta(hours=8),
            'area_manager_id': self.boss.id, 'hazards': 'Arco eléctrico',
            'executor_ids': [(6, 0, self.worker.ids)]}, **vals))
        permit.check_ids.write({'answer': 'si'})
        return permit

    def _authorized(self, **vals):
        permit = self._permit(**vals)
        permit.action_submit()
        permit.with_user(self.boss).action_approve_area()
        permit.with_user(self.mast).action_approve_sst()
        self.assertEqual(permit.state, 'autorizado')
        return permit

    def test_14_vencido_guardado_y_aviso_cada_hora(self):
        self.assertTrue(self.env['sgi.work.permit']._fields['expired'].store)
        permit = self._authorized()
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_work_permit SET date_start = %s, date_end = %s WHERE id = %s",
                            (datetime(2020, 1, 1, 8), datetime(2020, 1, 1, 18), permit.id))
        permit.invalidate_recordset()
        self.assertFalse(permit.expired, "Sin la corrida, la columna no sabe que pasó la hora.")
        self.Cron.cron_work_permits()
        self.assertTrue(permit.expired)
        boss_notice = self._notices(permit, 'permiso_vencido').filtered('active')
        mast_notice = self._notices(permit, 'permiso_vencido_mast').filtered('active')
        self.assertEqual((boss_notice.user_id, mast_notice.user_id), (self.boss, self.mast))
        self.Cron.cron_work_permits()
        self.assertEqual(len(self._notices(permit, 'permiso_vencido')), 1, "Cada hora, un solo aviso.")
        permit.close_note = 'Tableros cerrados'
        permit.action_close()
        self.assertFalse(permit.expired)
        self.Cron.cron_work_permits()
        self.assertFalse(boss_notice.active)
        self.assertTrue(boss_notice.sgi_episode_closed)
        with self.assertRaises(AccessError):
            self.Cron.with_user(self.mast).cron_work_permits()

    def test_15_no_se_cierra_con_loto_aplicado(self):
        permit = self._authorized()
        locker = self.env['hr.employee'].create({'name': 'Mecánico ZST'})
        loto = self.env['sgi.loto'].create({
            'equipment_id': self.equipment.id, 'name': 'Bloqueo ZST', 'work_permit_id': permit.id,
            'affected_notified': True, 'zero_energy_verified': True,
            'zero_energy_method': 'Intento de arranque',
            'energy_ids': [(0, 0, {'energy_type': 'electrica', 'isolation_point': 'Interruptor ZST'})],
            'lock_ids': [(0, 0, {'employee_id': locker.id, 'lock_number': 'C-ZST',
                                 'tag_number': 'T-ZST'})]})
        loto.action_apply()
        self.assertIn(loto, permit.loto_ids)
        permit.close_note = 'Área limpia'
        self._locked("bloqueo", permit.action_close)
        with self.assertRaises(UserError):
            permit.with_user(self.mast).write({'state': 'cerrado'})
        loto.lock_ids.action_remove_lock()
        loto.write({'area_notified_end': True, 'removal_note': 'Guardas colocadas'})
        loto.action_remove()
        permit.action_close()
        self.assertEqual(permit.state, 'cerrado')

    def test_16_competencia_por_tipo_de_permiso(self):
        # is_certification: la vigencia (valid_to) es de las certificaciones en
        # Odoo 19; así la prueba no depende de si se guarda en las demás.
        skill_type = self.env['hr.skill.type'].create({
            'name': 'Permisos ZST', 'is_certification': True,
            'skill_ids': [(0, 0, {'name': 'Trabajo eléctrico NOM-029 ZST'})],
            'skill_level_ids': [(0, 0, {'name': 'Aprobado ZST', 'level_progress': 100})]})
        skill = skill_type.skill_ids
        self._permit().action_submit()  # sin configuración, nada cambia
        self.env['sgi.work.permit.skill'].create({'work_type': 'electrico', 'skill_id': skill.id,
                                                  'note': 'NOM-029-STPS'})
        permit = self._permit()
        self._locked("Electricista ZST", permit.action_submit)
        employee_skill = self.env['hr.employee.skill'].create({
            'employee_id': self.worker.id, 'skill_id': skill.id, 'skill_type_id': skill_type.id,
            'skill_level_id': skill_type.skill_level_ids.id, 'valid_to': date(2046, 5, 1)})
        assert_locked(self, permit.action_submit)  # vence antes del fin del permiso
        employee_skill.valid_to = date(2047, 1, 1)
        permit.action_submit()
        self.assertEqual(permit.state, 'solicitado')

    def test_17_evaluacion_sst_del_contratista(self):
        contractor = self.env['res.partner'].create({'name': 'Instalaciones ZST', 'is_company': True})
        # El Jefe MAST de la prueba solo trae sus grupos: que pueda editar contactos.
        self.mast.group_ids = [(4, self.env.ref('base.group_partner_manager').id)]
        permit = self._permit(executor_ids=[(5, 0, 0)], contractor_id=contractor.id)
        self.assertFalse(permit.sgi_contractor_eval_ok)
        permit.action_submit()  # por omisión solo avisa
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.permit_contractor_eval_required', '1')
        blocked = self._permit(executor_ids=[(5, 0, 0)], contractor_id=contractor.id)
        self._locked("evaluación SST", blocked.action_submit)
        with self.assertRaises(UserError):
            contractor.with_user(self.sgi_user).write({'sgi_sst_eval_valid_until': date(2047, 1, 1)})
        contractor.with_user(self.mast).write({'sgi_sst_eval_valid_until': date(2047, 1, 1),
                                               'sgi_sst_eval_note': 'REPSE, SUA y DC-3 revisados'})
        blocked.invalidate_recordset(['sgi_contractor_eval_ok'])
        self.assertTrue(blocked.sgi_contractor_eval_ok)
        blocked.action_submit()
        self.assertEqual(blocked.state, 'solicitado')


@tagged('post_install', '-at_install')
class TestIncidenteDesdeIncapacidad(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # «hr»: nace «Por aprobar» y se aprueba con action_approve (camino de
        # write de RH). El tipo sin validación (otro_tipo, y test_19) cubre el
        # camino de create.
        cls.work_risk = env['hr.leave.type'].create({
            'name': 'Riesgo de trabajo ZST', 'requires_allocation': False,
            'leave_validation_type': 'hr'})
        cls.other_type = env['hr.leave.type'].create({
            'name': 'Permiso ZST', 'requires_allocation': False,
            'leave_validation_type': 'no_validation'})
        env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.work_risk_leave_type_ids', str(cls.work_risk.id))
        cls.employee = env['hr.employee'].create({'name': 'Tejedor ZST'})

    def _leave(self, leave_type, day_from, day_to):
        leave = self.env['hr.leave'].create({
            'employee_id': self.employee.id, 'holiday_status_id': leave_type.id,
            'request_date_from': day_from, 'request_date_to': day_to,
            'private_name': 'Diagnóstico confidencial ZST'})
        if leave.state != 'validate':
            self.assertFalse(leave.sgi_incident_id, "Por aprobar no crea incidente.")
            leave.action_approve()  # Odoo 18/19: aprueba y, si no es doble, valida
        if leave.state != 'validate':
            leave.action_validate()
        self.assertEqual(leave.state, 'validate')
        return leave

    def test_18_incapacidad_aprobada_crea_el_incidente(self):
        first = self._leave(self.work_risk, date(2046, 3, 5), date(2046, 3, 7))
        incident = first.sgi_incident_id
        self.assertTrue(incident)
        self.assertEqual((incident.state, incident.incident_type, incident.severity),
                         ('reportado', 'lesion', 'moderado'))
        self.assertEqual(incident.employee_ids, self.employee)
        self.assertEqual(incident.days_lost, round(first.number_of_days))
        self.assertTrue(incident.sgi_from_leave)
        self.assertNotIn('Diagnóstico', incident.name + (incident.description or ''))
        notice = self._notices(incident, 'incidente_incapacidad').filtered('active')
        self.assertEqual(notice.user_id, self.mast)
        first.write({'private_name': 'Otra nota ZST'})
        self.assertEqual(self.env['sgi.incident'].search_count(
            [('id', '=', incident.id)]), 1)
        self.assertEqual(len(self._notices(incident, 'incidente_incapacidad')), 1)
        # La subsecuente se liga al mismo incidente y suma días.
        second = self._leave(self.work_risk, date(2046, 3, 8), date(2046, 3, 9))
        self.assertEqual(second.sgi_incident_id, incident)
        self.assertEqual(incident.days_lost, round(first.number_of_days + second.number_of_days))
        self.assertEqual(incident.sgi_leave_count, 2)

    def test_19_otro_tipo_no_y_rechazo_deja_nota(self):
        other = self._leave(self.other_type, date(2046, 4, 2), date(2046, 4, 3))
        self.assertFalse(other.sgi_incident_id)
        first = self._leave(self.work_risk, date(2046, 3, 5), date(2046, 3, 7))
        incident = first.sgi_incident_id
        second = self._leave(self.work_risk, date(2046, 3, 8), date(2046, 3, 9))
        second.action_refuse()  # VERIFICAR (1.10)
        self.assertTrue(incident.exists(), "Nada se borra.")
        self.assertEqual(incident.days_lost, round(first.number_of_days))
        self.assertTrue(incident.message_ids.filtered(
            lambda m: 'ya no está aprobada' in (m.body or '')))

    def test_20_la_aprobacion_no_falla_por_el_sgi(self):
        Leave = type(self.env['hr.leave'])
        with patch.object(Leave, '_sgi_link_incident',
                          side_effect=ValueError("Falla simulada del SGI")), \
                self.assertLogs('odoo.addons.quimibond_sgi.models.sgi_incident_leave',
                                level='WARNING'):
            leave = self._leave(self.work_risk, date(2046, 6, 1), date(2046, 6, 1))
        self.assertEqual(leave.state, 'validate')
        self.assertFalse(leave.sgi_incident_id)
```

(`test_20` usa `assertLogs` con WARNING: la falla simulada no deja `ERROR` en el log del build. `2046-03-05` es lunes; `2046-06-01` es viernes.)

- [x] **Step 2: Ajustar pruebas existentes**

  a. `tests/test_entrega4.py`, `test_07_incident_reporter_edits_while_reported_then_only_reads` (l.~99-113): antes de `inc.with_user(self.mast).action_set_cerrado()` agregar

  ```python
        # 57.96.0 (N-06): equipo con un trabajador y eficacia antes de cerrar.
        worker = self.env['hr.employee'].create({'name': 'E4 Trabajador'})
        inc.with_user(self.mast).write({
            'investigation_team_ids': [(6, 0, worker.ids)], 'sgi_effective': 'eficaz',
            'sgi_effectiveness_date': date.today(), 'sgi_effectiveness_note': 'Sin repetición'})
  ```

  b. `tests/test_audit_hardening.py`, `test_a6_incident_close_via_write` (l.~229-245): antes de la última `incident.with_user(self.sgi_manager).write({'state': 'cerrado'})`, el mismo bloque con `self.sgi_manager` (y `'name': 'A6 Trabajador'`). El primer `assertRaises` (sin SCAT) sigue igual.

  c. `tests/test_ola_certificable.py`: agregar el helper a la clase y usarlo

  ```python
    def _evaluate(self, req, result):
        """57.96.0 (N-07): los botones rápidos abren el asistente con evidencia."""
        action = getattr(req, {'cumple': 'action_mark_cumple', 'parcial': 'action_mark_parcial',
                               'no_cumple': 'action_mark_no_cumple'}[result])()
        self.env['sgi.legal.evaluate'].with_context(action['context']).create({
            'evidence': 'Evidencia de prueba'}).action_confirm()
  ```

  En `test_legal_no_cumple_creates_nc`: `req.action_mark_no_cumple()` → `self._evaluate(req, 'no_cumple')`; `req.action_mark_parcial()` → `self._evaluate(req, 'parcial')`; `req.action_mark_cumple()` → `self._evaluate(req, 'cumple')`. En `test_legal_source_off_no_nc`: dentro del `assertRaises(UserError)`, `self._evaluate(req, 'no_cumple')`.

  d. `tests/test_env_aspect.py`, `_aspect` (l.~18-23): agregar `'life_cycle_stage': 'proceso'` a `base` (test_03 evalúa aspectos; la etapa es obligatoria para evaluar desde 57.96.0).

  e. `tests/test_work_permit.py`, `setUpClass` (l.~16-22), al final:

  ```python
        # 57.96.0 (N-06): la configuración real de competencias por tipo y el
        # parámetro del contratista no deben cambiar estas pruebas.
        cls.env['sgi.work.permit.skill'].search([]).write({'active': False})
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.permit_contractor_eval_required', '0')
  ```

  (Revisado contra 05f476e9 con `grep -n "action_set_cerrado\|'state': 'cerrado'" addons/quimibond_sgi/tests/*.py`: `test_incident`, `test_ola0`, `test_ola_b` y `test_nc_auditoria_evidencia` cierran incidentes con el superusuario, exento del candado como hoy; las de `sgi_user` esperan el candado. `test_risk`, `test_ola0` y `test_vistas_pulido` controlan o cierran riesgos R y O, fuera del candado nuevo. `test_env_aspect.test_04` crea el riesgo ambiental por `action_create_risk`, que pone el contexto. `test_work_permit.test_05` escribe `date_end` en el pasado y espera `expired`: con el campo guardado el ORM lo recalcula en ese `write`.)

- [x] **Step 3: Registrar** al final de `tests/__init__.py`:

```python
from . import test_sst_ambiente
```

- [ ] **Step 4: Confirmar en el shell de Odoo.sh lo de la sección 1.10** (`hr.leave` y sus métodos de aprobar y rechazar, `leave_validation_type`, `valid_to` en competencias). Si algo difiere, ajustar la prueba antes de empujar.

- [ ] **Step 5: Push solo de las pruebas y verlas fallar**

```bash
python3 tools/check_addons.py --base-ref origin/main
flake8 addons/quimibond_sgi/tests
git add addons/quimibond_sgi/tests
git commit -m "quimibond_sgi: pruebas de SST y ambiente (fallan antes del código)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

`check_addons` avisará «archivos cambiados sin bump» (advertencia): la versión sube en la Task 6.12. Esperado en el log (`--test-tags /quimibond_sgi`): **error** en todas las clases nuevas (campos y modelos que no existen: `control_hierarchy`, `sgi.env.aspect.transfer`, `investigation_team_ids`, `sgi.work.permit.skill`, `cron_work_permits`, `hr.leave.sgi_incident_id`); `test_09` falla (los botones registran); `test_entrega4.test_07`, `test_audit_hardening.test_a6`, `test_env_aspect` y `test_work_permit` dan **error** por los campos y modelos que aún no existen; `test_ola_certificable` da error (los botones aún devuelven `True` y `action['context']` no existe) y pasa después de la Task 6.5.

## Task 6.2: Jerarquía de controles en riesgos y acciones (N-06)

**Files:**
- Create: `addons/quimibond_sgi/models/sgi_control_hierarchy.py`
- Modify: `addons/quimibond_sgi/models/sgi_risk.py` (imports l.2-3; campos después de `existing_controls` l.88; `write` l.338-344; método nuevo después de `_sgi_check_can_close`)
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`SgiActionLine`, después de `action_type` l.~1035-1042)
- Modify: `addons/quimibond_sgi/views/sgi_risk_views.xml` (`sgi_risk_view_form`), `views/sgi_action_line_views.xml` (`sgi_action_line_view_form`), `views/sgi_incident_views.xml` (lista de acciones de `sgi_incident_view_form`)

- [x] **Step 1: `models/sgi_control_hierarchy.py`**

```python
# -*- coding: utf-8 -*-
"""57.96.0 (N-06, ISO 45001 8.1.2): jerarquía de controles.

Módulo sin modelos (patrón de ``sgi_guard.py``): lo importan ``sgi_risk.py`` y
``sgi_nonconformity.py`` sin mover el orden del registro. El orden de la
lista es el de la norma: primero eliminar el peligro; el equipo de
protección personal es el último recurso."""

CONTROL_HIERARCHY = [
    ('eliminacion', "1. Eliminación"),
    ('sustitucion', "2. Sustitución"),
    ('ingenieria', "3. Controles de ingeniería"),
    ('administrativo', "4. Controles administrativos (procedimientos, señalización, capacitación)"),
    ('epp', "5. Equipo de protección personal (EPP)"),
]

CONTROL_HIERARCHY_HELP = (
    "Jerarquía de controles de ISO 45001 8.1.2: eliminación, sustitución, controles de "
    "ingeniería, controles administrativos y, como último recurso, equipo de protección "
    "personal. Un IPER de riesgo alto no se controla ni se cierra si su único control es EPP.")
```

- [x] **Step 2: Campos y candado en `sgi_risk.py`**

Imports (l.2-3):

```python
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from .sgi_control_hierarchy import CONTROL_HIERARCHY, CONTROL_HIERARCHY_HELP
```

Constante de módulo, después de `SGI_HIGH_ATTENTION` (l.23):

```python
# 57.96.0 (N-07): «Aspecto ambiental» ya no se elige a mano en un riesgo. La
# evaluación del aspecto vive en la matriz (sgi.env.aspect); el riesgo
# ambiental solo nace desde ahí, como tratamiento («Tratar como riesgo») o con
# el asistente de traspaso, que ponen este contexto.
SGI_ENV_ASPECT_CONTEXT = 'sgi_from_env_aspect'
SGI_ENV_ASPECT_MSG = (
    "Los aspectos ambientales se registran en Seguridad y ambiente → Aspectos ambientales "
    "(una sola matriz, ISO 14001 6.1.2). Si el aspecto necesita acciones, use «Tratar como "
    "riesgo» desde el aspecto.")
```

Campos, después de `existing_controls` (l.88):

```python
    # 57.96.0 (N-06): jerarquía de controles (45001 8.1.2).
    control_hierarchy = fields.Selection(
        CONTROL_HIERARCHY, string="Control existente de mayor nivel", tracking=True,
        help=CONTROL_HIERARCHY_HELP)
    # 57.96.0 (N-07): el aspecto de la matriz cuyo tratamiento es este riesgo.
    sgi_env_aspect_ids = fields.One2many(
        'sgi.env.aspect', 'risk_id', string="Aspecto ambiental de la matriz",
        help="Aspecto ambiental cuya evaluación vive en la matriz; este riesgo guarda sus "
             "acciones de tratamiento.")
```

`create` nuevo (antes de `write`) y `write` (reemplaza l.338-344):

```python
    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.context.get(SGI_ENV_ASPECT_CONTEXT) \
                and any(vals.get('instrument') == 'ambiental' for vals in vals_list):
            raise UserError(SGI_ENV_ASPECT_MSG)
        return super().create(vals_list)

    def write(self, vals):
        if vals.get('instrument') == 'ambiental' and not self.env.context.get(SGI_ENV_ASPECT_CONTEXT) \
                and self.filtered(lambda r: r.instrument != 'ambiental'):
            raise UserError(SGI_ENV_ASPECT_MSG)
        # 57.96.0 (N-06): la jerarquía se revisa solo en la transición a
        # controlado o cerrado (no es retroactiva ni la disparan las acciones).
        moving = self.filtered(lambda r: r.state != vals['state']) \
            if vals.get('state') in self._SGI_CLOSING_STATES else self.browse()
        res = super().write(vals)
        if vals.get('state') in self._SGI_CLOSING_STATES:
            self.filtered(
                lambda r: r.state in self._SGI_CLOSING_STATES
            )._sgi_check_can_close()
            moving._sgi_check_control_hierarchy()
        return res
```

Método nuevo, después de `_sgi_check_can_close`:

```python
    def _sgi_check_control_hierarchy(self):
        """57.96.0 (N-06, 45001 8.1.2): un IPER de riesgo alto no se controla
        ni se cierra sin jerarquía de controles declarada, ni con EPP como
        único control. Cuentan el control existente del riesgo y el de sus
        acciones terminadas. Lo llama ``write`` solo en la transición."""
        labels = dict(CONTROL_HIERARCHY)
        for risk in self:
            if risk.instrument != 'iper' or risk.attention_level not in self._SGI_HIGH_ATTENTION:
                continue
            levels = {risk.control_hierarchy} | set(
                risk.action_line_ids.filtered('date_done').mapped('control_hierarchy'))
            levels.discard(False)
            if not levels:
                raise UserError(
                    "No se puede controlar ni cerrar el IPER de riesgo alto %s: indique la "
                    "jerarquía del control (eliminación, sustitución, ingeniería, administrativo o "
                    "EPP) en el riesgo o en sus acciones terminadas (ISO 45001 8.1.2)."
                    % (risk.folio or risk.name))
            if levels == {'epp'}:
                raise UserError(
                    "No se puede controlar ni cerrar el IPER de riesgo alto %s con solo %s: el "
                    "EPP es el último recurso. Registre y termine un control de mayor nivel "
                    "(eliminación, sustitución, ingeniería o administrativo)."
                    % (risk.folio or risk.name, labels['epp']))
```

- [x] **Step 3: Acción** (`sgi_nonconformity.py`): import junto a los demás de arriba del archivo (`from .sgi_control_hierarchy import CONTROL_HIERARCHY, CONTROL_HIERARCHY_HELP`) y campo después de `action_type`:

```python
    # 57.96.0 (N-06): jerarquía del control que aplica la acción (riesgos e
    # incidentes). Cuenta para el candado de los IPER de riesgo alto.
    control_hierarchy = fields.Selection(
        CONTROL_HIERARCHY, string="Jerarquía del control", tracking=True,
        help=CONTROL_HIERARCHY_HELP)
```

- [x] **Step 4: Vistas.**
  - `sgi_risk_view_form`, grupo «Descripción», después de `existing_controls`:
    ```xml
                        <field name="control_hierarchy" readonly="sgi_is_locked"
                               invisible="instrument == 'foda'"/>
    ```
    y en el `div` de IPER (l.~116-120) agregar al final: «Un IPER Alto tampoco se controla con EPP como único control: declare la jerarquía (eliminación, sustitución, ingeniería, administrativo) en el riesgo o en sus acciones.»
  - En la lista de acciones de `sgi_risk_view_form` y de `sgi_incident_view_form`, después de `name`: `<field name="control_hierarchy" optional="show"/>`.
  - `sgi_action_line_view_form`, grupo «Compromiso», después de `action_type`:
    ```xml
                            <field name="control_hierarchy" invisible="not risk_id and not incident_id"/>
    ```

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_control_hierarchy.py addons/quimibond_sgi/models/sgi_risk.py addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/views/sgi_risk_views.xml addons/quimibond_sgi/views/sgi_action_line_views.xml addons/quimibond_sgi/views/sgi_incident_views.xml
git commit -m "quimibond_sgi: jerarquía de controles; IPER alto con solo EPP no se controla (N-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

(El `create`/`write` de `sgi.risk` con el candado de «ambiental» entra en este mismo commit porque toca el mismo `write`; sus pruebas, `test_04`, pasan después de la Task 6.3, que pone el contexto en `action_create_risk`.)

## Task 6.3: El aspecto ambiental en la matriz (N-07)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_env_aspect.py` (constantes; `aspect_type` l.53-65; `condition` l.71-77; campo nuevo; `_sgi_check_can_evaluate` l.172-184; `action_create_risk` l.215-239)
- Modify: `addons/quimibond_sgi/views/sgi_env_aspect_views.xml` (form, list, search), `views/sgi_risk_views.xml` (`sgi_risk_view_form`)

- [x] **Step 1: Constantes** después de `SIGNIFICANT_LEVELS` (l.31); `aspect_type` y `condition` pasan a usarlas sin cambiar valores ni etiquetas:

```python
ASPECT_TYPES = [
    ('emision', "Emisión a la atmósfera"),
    ('descarga', "Descarga de agua residual"),
    ('residuo_peligroso', "Residuo peligroso"),
    ('residuo_manejo_especial', "Residuo de manejo especial"),
    ('residuo_urbano', "Residuo sólido urbano"),
    ('energia', "Consumo de energía"),
    ('agua', "Consumo de agua"),
    ('materiales', "Consumo de materiales o químicos"),
    ('ruido', "Ruido"),
    ('suelo', "Afectación al suelo"),
    ('otro', "Otro"),
]
ASPECT_CONDITIONS = [
    ('normal', "Normal"),
    ('anormal', "Anormal"),
    ('emergencia', "Emergencia"),
]
# 57.96.0 (N-07, ISO 14001 6.1.2): perspectiva de ciclo de vida.
LIFE_CYCLE_STAGES = [
    ('materia_prima', "Obtención de materia prima e insumos"),
    ('proceso', "Proceso en planta"),
    ('transporte', "Transporte y distribución"),
    ('uso', "Uso por el cliente"),
    ('fin_vida', "Fin de vida y disposición"),
]
```

- [x] **Step 2: Campo**, después de `condition`:

```python
    life_cycle_stage = fields.Selection(
        LIFE_CYCLE_STAGES, string="Etapa del ciclo de vida", tracking=True,
        help="En qué etapa del ciclo de vida del producto ocurre el aspecto (ISO 14001 6.1.2). "
             "Se pide para registrar la evaluación.")
```

- [x] **Step 3: Candado de evaluación.** En `_sgi_check_can_evaluate`, después del problema de severidad y frecuencia:

```python
            if not aspect.life_cycle_stage:
                problems.append("• Indique la etapa del ciclo de vida (ISO 14001 6.1.2).")
```

- [x] **Step 4: `action_create_risk`**: `self.env['sgi.risk'].create(` → `self.env['sgi.risk'].with_context(sgi_from_env_aspect=True).create(`, con el comentario «57.96.0 (N-07): el riesgo ambiental solo nace desde la matriz».

- [x] **Step 5: Vistas.**
  - Ficha del aspecto: `<field name="life_cycle_stage"/>` después de `condition`. Lista: `<field name="life_cycle_stage" optional="show"/>` después de `condition`. Búsqueda: `<filter name="group_life_cycle" string="Etapa del ciclo de vida" context="{'group_by': 'life_cycle_stage'}"/>` dentro del `<group>` existente (sin atributos).
  - `sgi_risk_view_form`, grupo «Ubicación», después de `operational_control_id`:
    ```xml
                            <field name="sgi_env_aspect_ids" widget="many2many_tags" readonly="1"
                                   invisible="instrument != 'ambiental'"/>
    ```
    y debajo del `div class="oe_title"`:
    ```xml
                    <div class="alert alert-info mb-2" role="status" invisible="instrument != 'ambiental'">
                        La evaluación de este aspecto vive en Seguridad y ambiente → Aspectos ambientales.
                        Este riesgo guarda sus acciones de tratamiento. Los aspectos nuevos se registran
                        en la matriz, no aquí.
                    </div>
    ```
  - El `help` de `instrument` en `sgi_risk.py` (l.63-64) agrega: «"Aspecto ambiental" solo lo pone la matriz de aspectos (Tratar como riesgo).»

- [x] **Step 6: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_env_aspect.py addons/quimibond_sgi/models/sgi_risk.py addons/quimibond_sgi/views/sgi_env_aspect_views.xml addons/quimibond_sgi/views/sgi_risk_views.xml
git commit -m "quimibond_sgi: aspecto ambiental solo en la matriz y etapa del ciclo de vida (N-07)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.4: Asistente de traspaso de riesgos ambientales a la matriz (N-07, Q1)

**Files:**
- Create: `addons/quimibond_sgi/models/sgi_env_aspect_transfer.py`; Modify: `models/__init__.py` (al final)
- Create: `addons/quimibond_sgi/views/sgi_env_aspect_transfer_views.xml`; Modify: `__manifest__.py` (antes de `views/sgi_menus.xml`)
- Modify: `security/ir.model.access.csv`, `views/sgi_menus.xml`, `tools/sgi_menu_tree.txt`

- [x] **Step 1: `models/sgi_env_aspect_transfer.py`**

```python
# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10, N-07): traspaso de los riesgos ambientales a la
matriz de aspectos (sgi.env.aspect, ISO 14001 6.1.2).

Antes de 57.96.0 un riesgo se podía capturar con el instrumento «Aspecto
ambiental» y así nacieron 5 (2026-10-02). La evaluación del aspecto vive en la
matriz; el riesgo queda como su tratamiento (``risk_id``). Es un cambio de
datos de negocio: no va en migración; el Jefe MAST lo corre a mano con el
visto bueno de Jose. Al abrir solo cuenta y propone; «Traspasar» crea un
aspecto «En evaluación» por renglón (cada uno en su savepoint), deja nota en
el aspecto y en el riesgo, y registra antes y después en el log. Los riesgos
se conservan salvo que se pida archivarlos. Idempotente; nada se borra."""
import logging

from markupsafe import Markup

from odoo import Command, api, fields, models
from odoo.exceptions import AccessError

from .sgi_env_aspect import ASPECT_CONDITIONS, ASPECT_TYPES, LIFE_CYCLE_STAGES

_logger = logging.getLogger(__name__)


class SgiEnvAspectTransfer(models.TransientModel):
    """Traspaso de riesgos ambientales a la matriz de aspectos (N-07)."""
    _name = 'sgi.env.aspect.transfer'
    _description = "Traspaso de riesgos ambientales a la matriz de aspectos"

    line_ids = fields.One2many('sgi.env.aspect.transfer.line', 'wizard_id',
                               string="Riesgos ambientales sin aspecto")
    risk_count = fields.Integer(string="Riesgos por traspasar", compute='_compute_risk_count')
    archive_risks = fields.Boolean(
        string="Archivar los riesgos originales",
        help="Apagado: cada riesgo se conserva, ligado al aspecto como su tratamiento. "
             "Encendido: además se archiva (no se borra). Úselo solo si Dirección lo pidió.")
    done_count = fields.Integer(string="Aspectos creados", readonly=True)
    failed_count = fields.Integer(
        string="Riesgos que no se pudieron traspasar", readonly=True,
        help="El motivo de cada uno está en el log del servidor.")

    @api.model
    def _sgi_check_manager(self):
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST traspasa los riesgos ambientales a la matriz.")

    @api.model
    def _sgi_candidates(self):
        """Riesgos ambientales (activos o archivados) sin aspecto que los use."""
        risks = self.env['sgi.risk'].sudo().with_context(active_test=False).search(
            [('instrument', '=', 'ambiental')], order='id')
        linked = self.env['sgi.env.aspect'].sudo().with_context(active_test=False).search(
            [('risk_id', 'in', risks.ids)]).risk_id
        return risks - linked

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if 'line_ids' in fields_list:
            # _sgi_candidates lee con sudo: nadie más que el Jefe MAST ve la propuesta.
            self._sgi_check_manager()
            res['line_ids'] = [Command.create({
                'risk_id': risk.id, 'activity': risk.name, 'aspect_type': 'otro',
                'condition': 'normal'}) for risk in self._sgi_candidates()]
        return res

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_manager()
        return super().create(vals_list)

    @api.depends('line_ids')
    def _compute_risk_count(self):
        for wizard in self:
            wizard.risk_count = len(wizard.line_ids)

    def _sgi_aspect_vals(self, line):
        risk = line.risk_id.sudo()
        legal = self.env['sgi.legal.requirement'].sudo().with_context(active_test=False).search(
            [('risk_ids', 'in', risk.ids)])
        return {
            'name': risk.name,
            'activity': line.activity or risk.name,
            'aspect_type': line.aspect_type,
            'condition': line.condition,
            'life_cycle_stage': line.life_cycle_stage or False,
            'impact': risk.consequence or risk.name,
            # Mapeo inverso exacto de sgi.env.aspect.action_create_risk.
            'severity': risk.eval_impact or False,
            'frequency': risk.eval_probability or False,
            'process_id': risk.process_id.id,
            'sgi_area_id': risk.sgi_area_id.id,
            'control_description': risk.existing_controls or False,
            'operational_control_id': risk.operational_control_id.id,
            'next_review_date': risk.next_review_date,
            'legal_requirement_ids': [Command.set(legal.ids)],
            'risk_id': risk.id,
            'company_id': self.env['sgi.config']._sgi_company().id,
        }

    def action_apply(self):
        self.ensure_one()
        self._sgi_check_manager()
        candidates = self._sgi_candidates()
        lines = self.line_ids.filtered(lambda l: l.risk_id in candidates)
        who = self.env.user.display_name
        today = fields.Date.context_today(self)
        _logger.info("SGI N-07: antes, %d riesgos ambientales sin aspecto %s; se traspasan %s.",
                     len(candidates), candidates.ids, lines.risk_id.ids)
        Aspect = self.env['sgi.env.aspect']
        done = Aspect.browse()
        failed = self.env['sgi.risk'].browse()
        for line in lines:
            risk = line.risk_id
            try:
                with self.env.cr.savepoint():
                    aspect = Aspect.create(self._sgi_aspect_vals(line))
                    aspect.message_post(body=Markup(
                        "Traspasado desde el riesgo <b>%s</b> el %s por %s (57.96.0, N-07). "
                        "Revise la etapa del ciclo de vida, el tipo y la condición, y registre la "
                        "evaluación.") % (risk.folio or risk.name, today, who))
                    risk.message_post(body=Markup(
                        "La evaluación de este aspecto vive ahora en la matriz: <b>%s</b>. Este "
                        "riesgo queda como su tratamiento.") % aspect.folio)
                    if self.archive_risks:
                        risk.write({'active': False})
                        risk.message_post(body="Archivado al traspasarlo a la matriz (no se borró).")
                done |= aspect
            except Exception:
                failed |= risk
                _logger.warning("SGI N-07: el riesgo %s (%s) no se pudo traspasar.",
                                risk.id, risk.folio, exc_info=True)
        _logger.info("SGI N-07: después, %d aspectos creados %s, %d riesgos con error %s; "
                     "%d riesgos ambientales siguen sin aspecto.", len(done), done.ids,
                     len(failed), failed.ids, len(self._sgi_candidates()))
        # La ficha que regresa muestra solo lo que falló; los contadores suman
        # si el Jefe MAST vuelve a pulsar «Traspasar» sobre lo que quedó.
        self.line_ids.filtered(lambda l: l.risk_id in done.risk_id).unlink()
        self.write({'done_count': self.done_count + len(done), 'failed_count': len(failed)})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

    def action_view_aspects(self):
        self._sgi_check_manager()
        return {
            'type': 'ir.actions.act_window', 'name': "Aspectos traspasados",
            'res_model': 'sgi.env.aspect', 'view_mode': 'list,form',
            'domain': [('risk_id.instrument', '=', 'ambiental')],
            'context': {'active_test': False},
        }


class SgiEnvAspectTransferLine(models.TransientModel):
    """Un riesgo ambiental por traspasar y lo que el Jefe MAST decide del aspecto."""
    _name = 'sgi.env.aspect.transfer.line'
    _description = "Riesgo ambiental por traspasar a la matriz"

    wizard_id = fields.Many2one('sgi.env.aspect.transfer', required=True, ondelete='cascade')
    risk_id = fields.Many2one('sgi.risk', string="Riesgo", required=True, readonly=True)
    risk_kind = fields.Selection(related='risk_id.kind', string="Riesgo u oportunidad")
    process_id = fields.Many2one(related='risk_id.process_id', string="Proceso")
    score = fields.Integer(related='risk_id.score', string="Puntaje del riesgo")
    activity = fields.Char(string="Actividad u operación", required=True,
                           help="Qué se hace. Propuesta: el nombre del riesgo; corríjala.")
    aspect_type = fields.Selection(ASPECT_TYPES, string="Tipo de aspecto", required=True)
    condition = fields.Selection(ASPECT_CONDITIONS, string="Condición", required=True)
    life_cycle_stage = fields.Selection(LIFE_CYCLE_STAGES, string="Etapa del ciclo de vida")
```

`models/__init__.py`, al final: `from . import sgi_env_aspect_transfer` (después de `sgi_env_aspect`, que ya está antes; el import de constantes no carga modelos nuevos).

- [x] **Step 2: `views/sgi_env_aspect_transfer_views.xml`**

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo>
    <!-- 57.96.0 (N-07): el Jefe MAST traspasa a la matriz los riesgos
         ambientales capturados como riesgo. Al abrir solo cuenta. -->
    <record id="sgi_env_aspect_transfer_view_form" model="ir.ui.view">
        <field name="name">sgi.env.aspect.transfer.form</field>
        <field name="model">sgi.env.aspect.transfer</field>
        <field name="arch" type="xml">
            <form string="Traspaso de riesgos ambientales a la matriz">
                <sheet>
                    <div class="alert alert-info" role="status">
                        Cada renglón es un riesgo con el instrumento «Aspecto ambiental» que todavía no
                        tiene aspecto en la matriz. Corrija la actividad, el tipo, la condición y la
                        etapa del ciclo de vida; quite los renglones que no deban pasar. «Traspasar»
                        crea cada aspecto «En evaluación», lo liga al riesgo como su tratamiento, deja
                        una nota en los dos y no borra nada. Úselo solo con el visto bueno de Dirección.
                    </div>
                    <group>
                        <field name="risk_count"/>
                        <field name="archive_risks"/>
                        <field name="done_count" invisible="not done_count"/>
                        <field name="failed_count" invisible="not failed_count"/>
                    </group>
                    <field name="line_ids">
                        <list editable="bottom" create="0">
                            <!-- force_save: risk_id es de solo lectura y el cliente web
                                 no manda los campos de solo lectura al guardar; sin él el
                                 renglón llega sin riesgo (required) al pulsar «Traspasar». -->
                            <field name="risk_id" force_save="1"/>
                            <field name="risk_kind"/>
                            <field name="process_id"/>
                            <field name="score"/>
                            <field name="activity"/>
                            <field name="aspect_type"/>
                            <field name="condition"/>
                            <field name="life_cycle_stage"/>
                        </list>
                    </field>
                </sheet>
                <footer>
                    <button name="action_apply" type="object" string="Traspasar a la matriz"
                            class="btn-primary" invisible="not risk_count"
                            confirm="Se crearán los aspectos de los renglones de la lista y se ligarán a sus riesgos. ¿Continuar?"/>
                    <button name="action_view_aspects" type="object" string="Ver los aspectos traspasados"
                            invisible="not done_count"/>
                    <button string="Cerrar" special="cancel"/>
                </footer>
            </form>
        </field>
    </record>

    <record id="sgi_env_aspect_transfer_action" model="ir.actions.act_window">
        <field name="name">Traspaso de riesgos ambientales</field>
        <field name="res_model">sgi.env.aspect.transfer</field>
        <field name="view_mode">form</field>
        <field name="target">new</field>
    </record>
</odoo>
```

Manifest, antes del comentario de menús:

```python
        # 57.96.0 (N-07): traspaso de riesgos ambientales a la matriz.
        'views/sgi_env_aspect_transfer_views.xml',
```

- [x] **Step 3: ACL** (al final de `security/ir.model.access.csv`):

```
access_sgi_env_aspect_transfer_manager,sgi.env.aspect.transfer.manager,model_sgi_env_aspect_transfer,group_sgi_manager,1,1,1,1
access_sgi_env_aspect_transfer_line_manager,sgi.env.aspect.transfer.line.manager,model_sgi_env_aspect_transfer_line,group_sgi_manager,1,1,1,1
```

- [x] **Step 4: Menú** (`views/sgi_menus.xml`, después de `menu_sgi_config_company_fix`):

```xml
    <!-- 57.96.0 (N-07): asistente manual del Jefe MAST. -->
    <menuitem id="menu_sgi_config_env_aspect_transfer" name="Traspaso de riesgos ambientales"
              parent="menu_sgi_config" action="sgi_env_aspect_transfer_action" sequence="95"/>
```

y en `addons/quimibond_sgi/tools/sgi_menu_tree.txt`, después de la línea de «Empresa en documentos controlados»:

```
SGI/Administración SGI/Configuración/Traspaso de riesgos ambientales | menu_sgi_config_env_aspect_transfer | -
```

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_env_aspect_transfer.py addons/quimibond_sgi/models/__init__.py addons/quimibond_sgi/views/sgi_env_aspect_transfer_views.xml addons/quimibond_sgi/views/sgi_menus.xml addons/quimibond_sgi/tools/sgi_menu_tree.txt addons/quimibond_sgi/security/ir.model.access.csv addons/quimibond_sgi/__manifest__.py
git commit -m "quimibond_sgi: asistente manual de traspaso de riesgos ambientales a la matriz (N-07)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.5: Lo legal con evidencia (N-07)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_legal.py` (l.163-195)
- Modify: `addons/quimibond_sgi/views/sgi_legal_views.xml` (`sgi_legal_requirement_view_form` l.73-81; `sgi_legal_evaluate_view_form` l.31-48)

- [x] **Step 1: Botones rápidos.** Reemplazar `action_mark_no_aplica`, `action_mark_cumple`, `action_mark_parcial` y `action_mark_no_cumple` por:

```python
    # 57.96.0 (N-07, 9.1.2): los botones rápidos ya no registran sin
    # evidencia; abren el asistente con el resultado elegido. La NC por
    # «Parcial» o «No cumple» la levanta el asistente al confirmar.
    def _sgi_open_evaluate(self, result):
        self.ensure_one()
        action = self.action_evaluate()
        action['context'] = dict(action['context'], default_result=result)
        return action

    def action_mark_cumple(self):
        return self._sgi_open_evaluate('cumple')

    def action_mark_parcial(self):
        return self._sgi_open_evaluate('parcial')

    def action_mark_no_cumple(self):
        return self._sgi_open_evaluate('no_cumple')

    def action_mark_no_aplica(self):
        return self._sgi_open_evaluate('no_aplica')
```

- [x] **Step 2: Vistas.** En la ficha del requisito, los botones «Cumple parcialmente» y «No cumple» pierden su `confirm` (el asistente es la confirmación; su `help` lo dice: «Abre el registro de la evaluación con la evidencia»). En el asistente, el campo `evidence` toma dos etiquetas:

```xml
                <group>
                    <field name="result"/>
                    <label for="evidence" string="Motivo por el que no aplica" invisible="result != 'no_aplica'"/>
                    <label for="evidence" string="Evidencia revisada" invisible="result == 'no_aplica'"/>
                    <field name="evidence" nolabel="1"
                           placeholder="Qué se revisó (bitácora, dictamen, constancia…) y qué se encontró, o por qué no aplica"/>
                    <field name="next_date"/>
                </group>
```

- [x] **Step 3: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_legal.py addons/quimibond_sgi/views/sgi_legal_views.xml
git commit -m "quimibond_sgi: los botones rápidos de lo legal abren el asistente con evidencia (N-07)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.6: Incidente con equipo de investigación, eficacia e IPER reevaluado (N-06)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_incident.py` (imports; campos después de `sgi_alert_id` l.78-80; `write` l.139-167; `_sgi_check_can_close` l.259-282; métodos nuevos)
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`SgiActionLine.create`, l.~1322-1346)
- Modify: `addons/quimibond_sgi/views/sgi_incident_views.xml` (`sgi_incident_view_form`)

- [x] **Step 1: Campos** (imports: `from markupsafe import Markup`):

```python
    # 57.96.0 (N-06, 45001 5.4 y 10.2): quién investigó y si funcionó.
    investigation_team_ids = fields.Many2many(
        'hr.employee', 'sgi_incident_investigation_rel', 'incident_id', 'employee_id',
        string="Equipo de investigación",
        help="Quiénes investigaron el evento. Para cerrar: al menos un trabajador sin personal "
             "a su cargo o un integrante de la Comisión de Seguridad e Higiene (ISO 45001 5.4).")
    sgi_effective = fields.Selection([
        ('eficaz', "Eficaz"),
        ('no_eficaz', "No eficaz"),
    ], string="Resultado de la eficacia", tracking=True, copy=False,
        help="¿Las acciones evitaron que se repita? El incidente solo cierra con «Eficaz». "
             "«No eficaz» lo regresa a Acciones y pide una acción nueva.")
    sgi_effectiveness_date = fields.Date(string="Fecha de la verificación", tracking=True, copy=False)
    sgi_effectiveness_note = fields.Text(string="Qué se verificó", copy=False)
    sgi_effectiveness_by = fields.Many2one('res.users', string="Verificó", readonly=True, copy=False)
    sgi_ineffective_count = fields.Integer(
        string="Verificaciones no eficaces", readonly=True, copy=False,
        help="Veces que la verificación salió «No eficaz».")
    # 57.96.0: nació de una incapacidad por riesgo de trabajo (sgi_incident_leave.py).
    sgi_from_leave = fields.Boolean(string="Desde una incapacidad", readonly=True, copy=False)
    sgi_leave_count = fields.Integer(string="Incapacidades", compute='_compute_sgi_leave',
                                     help="Incapacidades por riesgo de trabajo aprobadas ligadas.")
    sgi_leave_days = fields.Float(string="Días de incapacidad", compute='_compute_sgi_leave')

    def _compute_sgi_leave(self):
        # sudo: hr.leave solo lo lee RH; aquí solo se muestran conteos.
        Leave = self.env['hr.leave'].sudo()
        for incident in self:
            leaves = Leave.search([('sgi_incident_id', '=', incident.id), ('state', '=', 'validate')]) \
                if incident.id else Leave
            incident.sgi_leave_count = len(leaves)
            incident.sgi_leave_days = sum(leaves.mapped('number_of_days'))
```

(`sgi_incident_id` lo agrega la Task 6.9 en `hr.leave`; mientras tanto el cálculo no se usa en vistas. Si se implementa en otro orden, la Task 6.9 va antes de mostrar estos campos.)

- [x] **Step 2: `write`** (reemplaza l.139-167):

```python
    _SGI_SYSTEM_FIELDS = ('sgi_ineffective_count', 'sgi_effectiveness_by', 'sgi_from_leave')

    def write(self, vals):
        """(Docstring de antes.) 57.96.0 (N-06): la eficacia solo la registran
        el Jefe MAST y Salud ocupacional; «No eficaz» regresa a Acciones; el
        candado de cierre se revisa después de escribir (cuenta lo que el
        formulario manda junto con el estado), como la NC desde 57.93.0."""
        self._sgi_check_reporter_employee([vals])
        if not self.env.su and any(f in vals for f in self._SGI_SYSTEM_FIELDS):
            vals = {k: v for k, v in vals.items() if k not in self._SGI_SYSTEM_FIELDS}
        if 'state' in vals and not self.env.su and not self._sgi_can_investigate():
            if self.filtered(lambda i: i.state != vals['state']):
                raise UserError(
                    "Solo el Jefe MAST y Salud ocupacional investigan, cierran o reabren "
                    "un incidente. Usted puede reportarlo y consultar cómo se cerró.")
        if 'sgi_effective' in vals and not self.env.su and not self._sgi_can_investigate():
            raise UserError("Solo el Jefe MAST y Salud ocupacional registran la verificación de "
                            "eficacia de un incidente.")
        if vals.get('sgi_effective'):
            vals = dict(vals, sgi_effectiveness_by=self.env.uid)
        escalating = self.browse()
        if vals.get('severity') in ('grave', 'fatal'):
            escalating = self.filtered(lambda i: i.severity not in ('grave', 'fatal'))
        closing = self.filtered(lambda i: i.state != 'cerrado') \
            if vals.get('state') == 'cerrado' and not self.env.su else self.browse()
        newly_ineffective = self.filtered(lambda i: i.sgi_effective != 'no_eficaz') \
            if vals.get('sgi_effective') == 'no_eficaz' else self.browse()
        res = super().write(vals)
        # Un UserError aquí deshace el write completo (misma transacción).
        closing._sgi_check_can_close()
        newly_ineffective._sgi_on_ineffective()
        for incident in escalating:
            incident._sgi_notify_if_serious()
            incident._sgi_create_alert()
        return res
```

(Los `sudo().write` del sistema —`_sgi_on_ineffective`, Task 6.9— pasan los campos del sistema porque `env.su`.)

- [x] **Step 3: Candado de cierre.** En `_sgi_check_can_close`, después del bloque de grave/fatal sin IPER, agregar:

```python
            # 57.96.0 (N-06): equipo de investigación (45001 5.4).
            problems += incident._sgi_team_problems()
            # 57.96.0 (N-06): IPER reevaluado después del incidente.
            risk = incident.sudo().risk_id
            if incident.severity in ('moderado', 'grave', 'fatal') and risk and incident.date:
                happened = fields.Date.context_today(incident, incident.date)
                if not risk.last_eval_date or risk.last_eval_date < happened:
                    problems.append(
                        "• Reevalúe el IPER %s después del incidente («Registrar evaluación» en el "
                        "riesgo; última evaluación: %s)." % (risk.folio or risk.name,
                                                             risk.last_eval_date or "ninguna"))
            # 57.96.0 (N-06): eficacia como en la NC.
            problems += incident._sgi_effectiveness_problems()
```

y los métodos nuevos (después de `_sgi_check_can_close`):

```python
    def _sgi_team_problems(self):
        """Al menos un trabajador (sin personal a su cargo) o un integrante de
        la Comisión de Seguridad e Higiene. sudo: hr.employee solo lo lee RH."""
        self.ensure_one()
        team = self.sudo().investigation_team_ids
        if not team:
            return ["• Registre el equipo de investigación (pestaña Investigación y eficacia)."]
        csh = self.env.ref('quimibond_sgi.group_sgi_csh', raise_if_not_found=False)
        csh_users = csh.sudo().all_user_ids if csh else self.env['res.users']
        if any(not member.child_ids or (member.user_id and member.user_id in csh_users)
               for member in team):
            return []
        return ["• El equipo de investigación necesita al menos un trabajador sin personal a su "
                "cargo o un integrante de la Comisión de Seguridad e Higiene (ISO 45001 5.4)."]

    def _sgi_effectiveness_problems(self):
        self.ensure_one()
        problems = []
        if self.sgi_effective != 'eficaz':
            problems.append("• Falta verificar la eficacia de las acciones: «Eficaz», con fecha y "
                            "qué se verificó (pestaña Investigación y eficacia).")
        elif not (self.sgi_effectiveness_date and (self.sgi_effectiveness_note or '').strip()):
            problems.append("• Falta la fecha o la nota de la verificación de eficacia.")
        eff_date = self.sgi_effectiveness_date
        if eff_date:
            if eff_date > fields.Date.context_today(self):
                problems.append("• La fecha de la verificación (%s) no puede ser futura." % eff_date)
            done = [d for d in self.action_line_ids.mapped('date_done') if d]
            if done and eff_date < max(done):
                problems.append("• La eficacia (%s) se registró antes de que terminara la última "
                                "acción (%s)." % (eff_date, max(done)))
        if self.sgi_ineffective_count and not self.action_line_ids.filtered(
                lambda l: l.date_done and l.effectiveness_round >= self.sgi_ineffective_count):
            problems.append("• La verificación anterior salió «No eficaz»: registre y termine una "
                            "acción nueva.")
        return problems

    def _sgi_on_ineffective(self):
        """57.96.0 (N-06): «No eficaz» queda en el historial, suma el
        contador, limpia la verificación, regresa el incidente a Acciones y
        pide la acción nueva a quien la registró (o al Jefe MAST)."""
        Cron = self.env['sgi.cron']
        user_id = self.env.uid if self.env.user.active and not self.env.user._is_superuser() \
            else Cron._sgi_manager_user_id()
        for incident in self:
            folio = incident.folio or incident.name
            incident.message_post(body=Markup(
                "<b>Verificación de eficacia: no eficaz</b> (%s).<br/>%s") % (
                    incident.sgi_effectiveness_date or fields.Date.context_today(incident),
                    incident.sgi_effectiveness_note or ''))
            incident.sudo().write({
                'sgi_ineffective_count': incident.sgi_ineffective_count + 1,
                'sgi_effective': False, 'sgi_effectiveness_date': False,
                'sgi_effectiveness_note': False, 'state': 'acciones'})
            Cron._sgi_schedule(
                incident, "Registrar acción nueva del incidente %s (no eficaz)" % folio,
                "La verificación de eficacia salió «No eficaz». Revise las causas y registre una "
                "acción nueva; el incidente no cierra sin ella.", user_id)
```

- [x] **Step 4: Ronda de la acción del incidente** (`SgiActionLine.create`, dentro del `for vals in vals_list`, después del `if vals.get('alert_id'):`):

```python
            elif vals.get('incident_id'):
                # 57.96.0 (N-06): la ronda de eficacia también para incidentes.
                vals = dict(vals, effectiveness_round=self.env['sgi.incident'].browse(
                    vals['incident_id']).sudo().sgi_ineffective_count)
```

y al final de `create`, antes de `return lines`:

```python
        for line in lines.filtered(lambda l: l.incident_id and l.effectiveness_round
                                   and l.effectiveness_round >= l.incident_id.sudo().sgi_ineffective_count):
            line.incident_id.sudo().activity_ids.filtered(
                lambda a: (a.summary or '').startswith("Registrar acción nueva del incidente")
            ).action_feedback(feedback="Se registró la acción «%s»." % line.name)
```

- [x] **Step 5: Vista** (`sgi_incident_view_form`): página nueva después de «Acciones» (el campo `sgi_user_can_investigate` ya está en la ficha, l.~43; no se repite):

```xml
                        <page string="Investigación y eficacia" name="investigation">
                            <group>
                                <field name="investigation_team_ids" readonly="sgi_is_locked"
                                       widget="many2many_tags" options="{'no_create': True}"/>
                            </group>
                            <div class="text-muted mb-2">
                                Al menos un trabajador sin personal a su cargo o un integrante de la
                                Comisión de Seguridad e Higiene participa en la investigación
                                (ISO 45001 5.4).
                            </div>
                            <group string="Verificación de eficacia">
                                <field name="sgi_effective" widget="radio"
                                       readonly="sgi_is_locked or not sgi_user_can_investigate"/>
                                <field name="sgi_effectiveness_date"
                                       readonly="sgi_is_locked or not sgi_user_can_investigate"/>
                                <field name="sgi_effectiveness_note"
                                       readonly="sgi_is_locked or not sgi_user_can_investigate"
                                       placeholder="Qué se revisó para saber que no se repite"/>
                                <field name="sgi_effectiveness_by" invisible="not sgi_effectiveness_by"/>
                                <field name="sgi_ineffective_count" invisible="not sgi_ineffective_count"/>
                            </group>
                            <group string="Incapacidad por riesgo de trabajo" invisible="not sgi_from_leave">
                                <field name="sgi_from_leave" invisible="1"/>
                                <field name="sgi_leave_count"/>
                                <field name="sgi_leave_days"/>
                            </group>
                        </page>
```

- [x] **Step 6: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_incident.py addons/quimibond_sgi/models/sgi_nonconformity.py addons/quimibond_sgi/views/sgi_incident_views.xml
git commit -m "quimibond_sgi: incidente con equipo de investigación, eficacia e IPER reevaluado (N-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.7: Permiso de trabajo: vencido guardado, aviso cada hora y LOTO al cerrar (N-06)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_work_permit.py` (`expired` l.108 y 144-149; `loto_ids`; `write` nuevo)
- Modify: `addons/quimibond_sgi/models/sgi_cron.py` (después de `cron_nightly_backup`, l.~957-970)
- Create: `addons/quimibond_sgi/data/sgi_sst_cron.xml`; Modify: `__manifest__.py` (después de `data/sgi_nightly_cron.xml`)
- Modify: `addons/quimibond_sgi/views/sgi_work_permit_views.xml` (`sgi_work_permit_view_form`, lista y búsqueda)

- [x] **Step 1: `expired` guardado y LOTO** (reemplaza l.108 y 144-149):

```python
    # 57.96.0 (N-06): guardado e indexado para buscar y avisar. Al guardar se
    # compara con la hora de ese momento; el paso del tiempo lo pone la acción
    # planificada «SGI: Permisos de trabajo vencidos (cada hora)».
    expired = fields.Boolean(string="Vencido", compute='_compute_expired', store=True, index=True,
                             help="Autorizado y pasada su hora de fin. Se revisa cada hora.")
    loto_ids = fields.One2many('sgi.loto', 'work_permit_id', string="Bloqueos (LOTO)",
                               help="Bloqueos de energía ligados al permiso. El permiso no se "
                                    "cierra mientras uno siga aplicado.")

    def _sgi_expired_on(self, now):
        self.ensure_one()
        return bool(self.state == 'autorizado' and self.date_end and self.date_end < now)

    @api.depends('date_end', 'state')
    def _compute_expired(self):
        now = fields.Datetime.now()
        for permit in self:
            permit.expired = permit._sgi_expired_on(now)
```

`write` nuevo (después de `create`):

```python
    def write(self, vals):
        # 57.96.0 (N-06): con energía bloqueada el permiso no se cierra (por
        # el botón o por escritura directa; tampoco el Jefe MAST).
        if vals.get('state') == 'cerrado':
            self.filtered(lambda p: p.state != 'cerrado')._sgi_check_loto_released()
        return super().write(vals)

    def _sgi_check_loto_released(self):
        Loto = self.env['sgi.loto'].sudo()
        for permit in self:
            applied = Loto.search([('work_permit_id', '=', permit.id), ('state', '=', 'bloqueado')])
            if applied:
                raise UserError(
                    "No se puede cerrar el permiso %s: el bloqueo %s sigue aplicado. Cada "
                    "trabajador retira su candado y se retira el bloqueo antes de cerrar el permiso."
                    % (permit.folio or permit.name, ", ".join(applied.mapped('display_name'))))
```

- [x] **Step 2: Cron** en `sgi_cron.py`, después de `cron_nightly_backup` (imports ya presentes: `fields`, `sgi_require_system`; agregar `import pytz` arriba si no está):

```python
    # ------------------------------------------------------------------
    # 57.96.0 (N-06) — Permisos de trabajo vencidos, cada hora
    # ------------------------------------------------------------------
    @api.model
    def cron_work_permits(self):
        """Cron cada hora: marca vencidos los permisos de trabajo autorizados
        que pasaron su hora de fin y avisa sobre el permiso al jefe del área (o
        a quien lo solicitó) y al Jefe MAST. Los avisos se cierran solos cuando
        el permiso se cierra, se cancela o se renueva."""
        sgi_require_system(self.env)
        self = self._sgi_new_run()
        now = fields.Datetime.now()
        authorized = self.env['sgi.work.permit'].search([('state', '=', 'autorizado')])
        self._sgi_write_changed(authorized, 'expired', lambda permit: permit._sgi_expired_on(now))
        manager_id = self._sgi_manager_user_id()
        tz = pytz.timezone('America/Mexico_City')

        def _notify(permit):
            ended = pytz.utc.localize(permit.date_end).astimezone(tz)
            summary = "Permiso de trabajo vencido: %s" % (permit.folio or permit.name)
            note = ("El permiso %s (%s) venció el %s y sigue autorizado. Suspenda el trabajo, "
                    "ciérrelo con las condiciones del área o solicite uno nuevo."
                    % (permit.folio or '', permit.name, ended.strftime('%d/%m/%Y %H:%M')))
            boss = permit.area_manager_id if permit.area_manager_id.active else permit.requester_id
            # Odoo no asigna una actividad a quien no puede leer el permiso
            # (_check_access_assignation): sin «Usuario SGI», el aviso va solo
            # al Jefe MAST; si no, el renglón fallaría cada hora (ERROR en el log).
            if boss and not boss.has_group('quimibond_sgi.group_sgi_user'):
                boss = self.env['res.users']
            deadline = ended.date()
            if boss and boss.active:
                self._sgi_schedule(permit, summary, note, boss.id, date_deadline=deadline,
                                   key='permiso_vencido')
            if manager_id and manager_id != boss.id:
                self._sgi_schedule(permit, summary, note, manager_id, date_deadline=deadline,
                                   key='permiso_vencido_mast')

        failures = self._sgi_for_each(authorized.filtered('expired'), _notify, "permisos vencidos")
        self._sgi_sweep(['permiso_vencido', 'permiso_vencido_mast'],
                        "el permiso ya se cerró, se canceló o se renovó", failures)
        return True
```

- [x] **Step 3: `data/sgi_sst_cron.xml`**

```xml
<?xml version="1.0" encoding="utf-8"?>
<odoo noupdate="1">
    <!-- 57.96.0 (N-06): permisos de trabajo vencidos y todavía autorizados.
         Registro nuevo en archivo nuevo (no edita un cron noupdate). -->
    <record id="sgi_cron_work_permits" model="ir.cron">
        <field name="name">SGI: Permisos de trabajo vencidos (cada hora)</field>
        <field name="model_id" ref="model_sgi_cron"/>
        <field name="state">code</field>
        <field name="code">model.cron_work_permits()</field>
        <field name="interval_number">1</field>
        <field name="interval_type">hours</field>
        <field name="nextcall" eval="(DateTime.now() + relativedelta(hours=1, minute=7, second=0)).strftime('%Y-%m-%d %H:%M:%S')"/>
        <field name="active" eval="True"/>
    </record>
</odoo>
```

Manifest, después de `'data/sgi_nightly_cron.xml',`: `'data/sgi_sst_cron.xml',  # 57.96.0 (N-06): permisos vencidos`.

- [x] **Step 4: Vistas.** En la ficha del permiso, página nueva «Bloqueos (LOTO)» con `<field name="loto_ids" readonly="1"><list><field name="folio"/><field name="equipment_id"/><field name="state" widget="badge"/></list></field>`. En la lista: `<field name="expired" optional="show" widget="boolean"/>`. En la búsqueda, el filtro que ya existe `expired_open` («Vencidos sin cerrar», l.161-162, comparaba `date_end` con el día) cambia solo su dominio a `[('expired', '=', True)]` (ahora se puede, el campo está guardado); no se agrega un segundo filtro «Vencidos». El aviso `alert-danger` existente (l.35-37) ya usa `expired`.

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_work_permit.py addons/quimibond_sgi/models/sgi_cron.py addons/quimibond_sgi/data/sgi_sst_cron.xml addons/quimibond_sgi/__manifest__.py addons/quimibond_sgi/views/sgi_work_permit_views.xml
git commit -m "quimibond_sgi: permiso vencido guardado y avisado cada hora; no cierra con LOTO aplicado (N-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.8: Competencia por tipo de permiso y evaluación SST del contratista (N-06, Q4)

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_work_permit.py` (`action_submit` l.185-209; `_sgi_check_can_authorize` l.211-220; campo `sgi_contractor_eval_ok`; clases nuevas al final)
- Create: `addons/quimibond_sgi/views/sgi_work_permit_skill_views.xml`; Modify: `__manifest__.py`
- Modify: `views/sgi_work_permit_views.xml`, `views/sgi_res_partner_views.xml` (`sgi_res_partner_supplier_view_form`), `security/ir.model.access.csv`, `views/sgi_menus.xml`, `tools/sgi_menu_tree.txt`

- [x] **Step 1: Permiso.** Campo y métodos:

```python
    sgi_contractor_eval_ok = fields.Boolean(
        string="Contratista con evaluación SST vigente", compute='_compute_sgi_contractor_eval_ok',
        help="La evaluación SST del contratista cubre hasta el fin del permiso.")

    @api.depends('contractor_id.commercial_partner_id.sgi_sst_eval_valid_until', 'date_end')
    def _compute_sgi_contractor_eval_ok(self):
        for permit in self:
            partner = permit.contractor_id.commercial_partner_id.sudo()
            until = permit.date_end.date() if permit.date_end else fields.Date.context_today(permit)
            permit.sgi_contractor_eval_ok = bool(
                not partner or (partner.sgi_sst_eval_valid_until
                                and partner.sgi_sst_eval_valid_until >= until))

    def _sgi_people_problems(self):
        """57.96.0 (N-06, 45001 7.2 y 8.1.4): competencias exigidas por tipo
        de trabajo (sin configuración no exige nada) y evaluación SST del
        contratista (bloquea solo con el parámetro encendido)."""
        self.ensure_one()
        problems = []
        until = (self.date_end or fields.Datetime.now()).date()
        rules = self.env['sgi.work.permit.skill'].sudo().search([('work_type', '=', self.work_type)])
        if rules:
            Skill = self.env['hr.employee.skill'].sudo()
            for employee in self.sudo().executor_ids:
                missing = rules.filtered(lambda r: not Skill.search_count([
                    ('employee_id', '=', employee.id), ('skill_id', '=', r.skill_id.id),
                    '|', ('valid_to', '=', False), ('valid_to', '>=', until)]))
                if missing:
                    problems.append("• %s no tiene vigente hasta el %s: %s." % (
                        employee.name, until, ", ".join(missing.mapped('skill_id.name'))))
        required = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.permit_contractor_eval_required', '0') in ('1', 'True', 'true')
        if required and self.contractor_id and not self.sgi_contractor_eval_ok:
            problems.append(
                "• El contratista %s no tiene evaluación SST vigente hasta el fin del permiso (%s). "
                "La registra el Jefe MAST en el contacto, pestaña SGI."
                % (self.contractor_id.commercial_partner_id.display_name, until))
        return problems
```

En `action_submit`, antes de `if problems:`: `problems += permit._sgi_people_problems()`. En `_sgi_check_can_authorize`, después del `failing` existente:

```python
            people = permit._sgi_people_problems()
            if people:
                raise UserError("No se puede autorizar el permiso %s:\n%s" % (
                    permit.folio, "\n".join(people)))
```

Clases nuevas al final del archivo:

```python
class SgiWorkPermitSkill(models.Model):
    """57.96.0 (N-06, 45001 7.2): competencia exigida por tipo de permiso."""
    _name = 'sgi.work.permit.skill'
    _description = "Competencia requerida por tipo de permiso de trabajo"
    _order = 'work_type, id'

    _work_type_skill_uniq = models.Constraint(
        'unique(work_type, skill_id)', "Esa competencia ya se exige para ese tipo de trabajo.")

    work_type = fields.Selection(WORK_TYPES, string="Tipo de trabajo", required=True)
    skill_id = fields.Many2one('hr.skill', string="Competencia", required=True, ondelete='restrict',
                               help="Competencia que cada persona que ejecuta debe tener vigente "
                                    "hasta el fin del permiso.")
    skill_type_id = fields.Many2one(related='skill_id.skill_type_id', string="Tipo de competencia")
    note = fields.Char(string="Por qué se exige", help="NOM o procedimiento: NOM-009-STPS, DC-3…")
    active = fields.Boolean(default=True)


class ResPartnerSstContractor(models.Model):
    """57.96.0 (N-06, 45001 8.1.4): evaluación SST del contratista."""
    _inherit = 'res.partner'

    sgi_sst_eval_valid_until = fields.Date(
        string="Evaluación SST vigente hasta", tracking=True,
        help="Hasta cuándo vale la evaluación de seguridad y salud del contratista. El permiso de "
             "trabajo la revisa. La registra el Jefe MAST.")
    sgi_sst_eval_note = fields.Text(
        string="Qué se revisó (SST)",
        help="REPSE, SUA, constancias DC-3, inducción de seguridad, seguro…")

    _SGI_SST_EVAL_FIELDS = ('sgi_sst_eval_valid_until', 'sgi_sst_eval_note')

    def _sgi_check_sst_eval(self, vals_list):
        if any(f in vals for vals in vals_list for f in self._SGI_SST_EVAL_FIELDS) and not (
                self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("La evaluación SST del contratista la registra el Jefe MAST.")

    @api.model_create_multi
    def create(self, vals_list):
        # También al crear (importación o RPC), no solo al editar.
        self._sgi_check_sst_eval(vals_list)
        return super().create(vals_list)

    def write(self, vals):
        self._sgi_check_sst_eval([vals])
        return super().write(vals)
```

- [x] **Step 2: Vistas.**
  - Ficha del permiso, junto al aviso de vencido (`role="alert"`), con `contractor_id` y el campo nuevo en la ficha:
    ```xml
                <div class="alert alert-warning mb-0" role="alert"
                     invisible="not contractor_id or sgi_contractor_eval_ok">
                    El contratista no tiene evaluación SST vigente hasta el fin del permiso. El Jefe MAST
                    la registra en el contacto, pestaña SGI.
                </div>
    ```
    y `<field name="sgi_contractor_eval_ok" invisible="1"/>` dentro de la ficha.
  - `sgi_res_partner_supplier_view_form` (herencia de `base.view_partner_form`, de otro módulo; se edita en su `<record>`), dentro de la página «SGI», después del primer `<group>`:
    ```xml
                    <group string="Contratista (SST, ISO 45001 8.1.4)">
                        <field name="sgi_sst_eval_valid_until"/>
                        <field name="sgi_sst_eval_note"
                               placeholder="REPSE, SUA, constancias DC-3, inducción de seguridad…"/>
                    </group>
    ```
  - `views/sgi_work_permit_skill_views.xml` (nuevo):
    ```xml
    <?xml version="1.0" encoding="utf-8"?>
    <odoo>
        <!-- 57.96.0 (N-06): competencias que exige cada tipo de permiso. -->
        <record id="sgi_work_permit_skill_view_list" model="ir.ui.view">
            <field name="name">sgi.work.permit.skill.list</field>
            <field name="model">sgi.work.permit.skill</field>
            <field name="arch" type="xml">
                <list string="Competencias por tipo de permiso" editable="bottom">
                    <field name="work_type"/>
                    <field name="skill_id"/>
                    <field name="skill_type_id"/>
                    <field name="note"/>
                </list>
            </field>
        </record>

        <record id="sgi_work_permit_skill_view_search" model="ir.ui.view">
            <field name="name">sgi.work.permit.skill.search</field>
            <field name="model">sgi.work.permit.skill</field>
            <field name="arch" type="xml">
                <search string="Competencias por tipo de permiso">
                    <field name="skill_id"/>
                    <filter name="inactive" string="Archivados" domain="[('active', '=', False)]"/>
                    <group>
                        <filter name="group_work_type" string="Tipo de trabajo" context="{'group_by': 'work_type'}"/>
                    </group>
                </search>
            </field>
        </record>

        <record id="sgi_work_permit_skill_action" model="ir.actions.act_window">
            <field name="name">Competencias por tipo de permiso</field>
            <field name="res_model">sgi.work.permit.skill</field>
            <field name="view_mode">list</field>
            <field name="search_view_id" ref="sgi_work_permit_skill_view_search"/>
            <field name="help" type="html">
                <p class="o_view_nocontent_smiling_face">Sin competencias exigidas</p>
                <p>Mientras esta lista esté vacía, el permiso de trabajo no revisa competencias. Agregue
                   las que exige cada tipo de trabajo (por ejemplo, NOM-009 para alturas): cada persona
                   que ejecuta debe tenerlas vigentes hasta el fin del permiso.</p>
            </field>
        </record>
    </odoo>
    ```
    Manifest, después de `'views/sgi_loto_views.xml',`: `'views/sgi_work_permit_skill_views.xml',  # 57.96.0 (N-06)`.

- [x] **Step 3: ACL** (al final de `ir.model.access.csv`):

```
access_sgi_work_permit_skill_user,sgi.work.permit.skill.user,model_sgi_work_permit_skill,group_sgi_user,1,0,0,0
access_sgi_work_permit_skill_auditor,sgi.work.permit.skill.auditor,model_sgi_work_permit_skill,group_sgi_auditor,1,0,0,0
access_sgi_work_permit_skill_manager,sgi.work.permit.skill.manager,model_sgi_work_permit_skill,group_sgi_manager,1,1,1,1
```

- [x] **Step 4: Menú** (después de `menu_sgi_config_env_aspect_transfer`):

```xml
    <!-- 57.96.0 (N-06): competencias que exige cada tipo de permiso. -->
    <menuitem id="menu_sgi_config_work_permit_skills" name="Competencias por tipo de permiso"
              parent="menu_sgi_config" action="sgi_work_permit_skill_action" sequence="96"/>
```

y en `sgi_menu_tree.txt`, después de la línea del traspaso:

```
SGI/Administración SGI/Configuración/Competencias por tipo de permiso | menu_sgi_config_work_permit_skills | -
```

- [x] **Step 5: Checadores y commit**

```bash
git add addons/quimibond_sgi/models/sgi_work_permit.py addons/quimibond_sgi/views/sgi_work_permit_views.xml addons/quimibond_sgi/views/sgi_work_permit_skill_views.xml addons/quimibond_sgi/views/sgi_res_partner_views.xml addons/quimibond_sgi/views/sgi_menus.xml addons/quimibond_sgi/tools/sgi_menu_tree.txt addons/quimibond_sgi/security/ir.model.access.csv addons/quimibond_sgi/__manifest__.py
git commit -m "quimibond_sgi: competencia por tipo de permiso y evaluación SST del contratista (N-06)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.9: Incidente desde la incapacidad por riesgo de trabajo (N-06, D7)

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py` (`depends`)
- Create: `addons/quimibond_sgi/models/sgi_incident_leave.py`; Modify: `models/__init__.py` (al final)

- [x] **Step 1: Dependencia** en `__manifest__.py`, después de `'hr_skills',`:

```python
        'hr_holidays',  # 57.96.0: incapacidad por riesgo de trabajo → incidente (instalado en producción)
```

- [x] **Step 2: `models/sgi_incident_leave.py`**

```python
# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10, N-06 y D7): incidente desde la incapacidad.

Una ausencia aprobada de un tipo de riesgo de trabajo crea el incidente en
«Reportado» con la persona y los días perdidos, y avisa al Jefe MAST para que
lo investigue. Tipos: el parámetro ``quimibond_sgi.work_risk_leave_type_ids``
(ids separados por coma) o, sin él, «Riesgo de trabajo (IMSS)»
(``hr_holidays.l10n_mx_leave_type_work_risk_imss``, que ya existe en
producción).

- La subsecuente (misma persona, hasta N días después de una incapacidad
  ligada a un incidente no cerrado; parámetro
  ``quimibond_sgi.work_risk_followup_days``, 3) se liga al mismo incidente.
- Rechazar o cancelar una incapacidad ligada deja nota y recalcula los días;
  nada se borra.
- El incidente no lleva diagnóstico ni la descripción de la ausencia.
- La aprobación nunca falla por el SGI: cada ausencia en su savepoint; una
  falla queda en el log como WARNING con la traza."""
import logging
from datetime import timedelta

from markupsafe import Markup

from odoo import api, fields, models

_logger = logging.getLogger(__name__)
WORK_RISK_XMLID = 'hr_holidays.l10n_mx_leave_type_work_risk_imss'


class HrLeaveSgiIncident(models.Model):
    _inherit = 'hr.leave'

    sgi_incident_id = fields.Many2one(
        'sgi.incident', string="Incidente SST", readonly=True, copy=False, index='btree_not_null',
        ondelete='set null',
        groups='hr_holidays.group_hr_holidays_user,quimibond_sgi.group_sgi_manager,'
               'quimibond_sgi.group_sgi_health',
        help="Incidente del SGI que se abrió por esta incapacidad por riesgo de trabajo.")

    @api.model
    def _sgi_work_risk_types(self):
        param = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.work_risk_leave_type_ids', '') or ''
        ids = [int(part) for part in param.split(',') if part.strip().isdigit()]
        if ids:
            return self.env['hr.leave.type'].sudo().browse(ids).exists()
        return self.env.ref(WORK_RISK_XMLID, raise_if_not_found=False) or self.env['hr.leave.type']

    @api.model_create_multi
    def create(self, vals_list):
        leaves = super().create(vals_list)
        leaves.filtered(lambda l: l.state == 'validate')._sgi_sync_work_risk()
        return leaves

    def write(self, vals):
        before = {leave.id: leave.state for leave in self} if 'state' in vals else {}
        res = super().write(vals)
        if before:
            self.filtered(lambda l: l.state == 'validate'
                          and before.get(l.id) != 'validate')._sgi_sync_work_risk()
            self.filtered(lambda l: before.get(l.id) == 'validate'
                          and l.state != 'validate')._sgi_sync_work_risk(dropped=True)
        return res

    def _sgi_sync_work_risk(self, dropped=False):
        types = self._sgi_work_risk_types()
        if not types:
            return
        for leave in self.filtered(lambda l: l.holiday_status_id in types):
            try:
                with self.env.cr.savepoint():
                    if dropped:
                        leave._sgi_unlink_note()
                    else:
                        leave._sgi_link_incident()
            except Exception:
                _logger.warning("SGI: no se pudo registrar el incidente de la incapacidad %s; la "
                                "ausencia sigue su curso.", leave.id, exc_info=True)

    def _sgi_followup_incident(self):
        """Incidente no cerrado de una incapacidad aprobada de la misma persona
        que terminó hasta N días antes de que empiece esta."""
        self.ensure_one()
        Param = self.env['ir.config_parameter'].sudo()
        try:
            days = int(Param.get_param('quimibond_sgi.work_risk_followup_days', 3) or 3)
        except (TypeError, ValueError):
            days = 3
        start = self.request_date_from
        previous = self.sudo().search([
            ('id', '!=', self.id), ('employee_id', '=', self.employee_id.id),
            ('state', '=', 'validate'), ('sgi_incident_id', '!=', False),
            ('sgi_incident_id.state', '!=', 'cerrado'),
            ('request_date_to', '<', start), ('request_date_to', '>=', start - timedelta(days=days)),
        ], order='request_date_to desc', limit=1)
        return previous.sgi_incident_id

    def _sgi_link_incident(self):
        self.ensure_one()
        leave = self.sudo()
        # Ya ligada (p. ej. hr_holidays valida dentro de su create y el create
        # de aquí vuelve a pasar): solo se refrescan los días, sin nota.
        already = leave.sgi_incident_id
        incident = already or leave._sgi_followup_incident()
        created = False
        if not incident:
            user = self.env.user
            incident = self.env['sgi.incident'].sudo().create({
                # Sin diagnóstico ni la descripción de la ausencia (dato de salud).
                'name': "Riesgo de trabajo (incapacidad IMSS) del %s" % leave.request_date_from,
                'incident_type': 'lesion',
                'severity': 'moderado',
                'date': leave.date_from,
                'employee_ids': [(6, 0, leave.employee_id.ids)],
                'description': "Registrado desde una incapacidad por riesgo de trabajo aprobada en "
                               "Ausencias. Complete la fecha y el lugar del evento, el proceso, la "
                               "descripción y la investigación.",
                'reporter_id': user.id,
                'reporter_employee_id': user.employee_id.id or False,
                'sgi_from_leave': True,
            })
            created = True
        if leave.sgi_incident_id != incident:
            leave.sgi_incident_id = incident
        if incident.state != 'cerrado':  # un incidente cerrado es evidencia
            incident._sgi_refresh_leave_days()
        if created:
            Cron = self.env['sgi.cron']
            Cron._sgi_schedule(
                incident, "Investigar riesgo de trabajo %s" % (incident.folio or incident.name),
                "RH aprobó una incapacidad por riesgo de trabajo. Complete el incidente e inicie la "
                "investigación SCAT.", Cron._sgi_manager_user_id(), key='incidente_incapacidad')
        elif not already:
            incident.message_post(body="Se ligó otra incapacidad por riesgo de trabajo aprobada.")

    def _sgi_unlink_note(self):
        self.ensure_one()
        incident = self.sudo().sgi_incident_id
        if not incident:
            return
        incident.message_post(body=Markup(
            "Una incapacidad ligada a este incidente ya no está aprobada (rechazada o cancelada en "
            "Ausencias). No se borró nada."))
        if incident.state != 'cerrado':
            incident._sgi_refresh_leave_days()
```

En `sgi_incident.py`, método nuevo:

```python
    def _sgi_refresh_leave_days(self):
        """57.96.0: días perdidos = suma redondeada de las incapacidades por
        riesgo de trabajo aprobadas ligadas. Solo escribe si cambió."""
        Leave = self.env['hr.leave'].sudo()
        for incident in self.sudo():
            days = round(sum(Leave.search([('sgi_incident_id', '=', incident.id),
                                           ('state', '=', 'validate')]).mapped('number_of_days')))
            if incident.days_lost != days:
                incident.write({'days_lost': days})
```

`models/__init__.py`, al final: `from . import sgi_incident_leave`.

- [x] **Step 3: Checadores y commit**

```bash
git add addons/quimibond_sgi/__manifest__.py addons/quimibond_sgi/models/sgi_incident_leave.py addons/quimibond_sgi/models/sgi_incident.py addons/quimibond_sgi/models/__init__.py
git commit -m "quimibond_sgi: incapacidad por riesgo de trabajo aprobada crea el incidente (N-06, D7)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.10: Manuales

**Files:**
- Modify: `docs/sgi/usuarios/mast.md`, `docs/sgi/usuarios/jefe-de-area.md`, `docs/sgi/usuarios/operador-o-supervisor.md`, `docs/sgi/usuarios/rh.md`, `docs/sgi/administracion/manual-jefe-mast.md`

- [x] **Step 1: Textos** («usted»):
  - **MAST (usuario):** «Al controlar o cerrar un IPER de riesgo alto, declare la jerarquía del control en el riesgo o en sus acciones terminadas; con solo EPP no se puede. Para cerrar un incidente: equipo de investigación con al menos un trabajador o un integrante de la Comisión de Seguridad e Higiene, verificación de eficacia («Eficaz») y, si es moderado o más, el IPER reevaluado después del evento. Cada hora le llega un aviso por cada permiso de trabajo vencido que sigue autorizado. Los botones Cumple / Parcial / No cumple / No aplica de un requisito legal abren el registro con la evidencia (o el motivo).»
  - **Jefe de área:** «Si un permiso de trabajo que usted autoriza vence y sigue autorizado, le llega un aviso cada hora hasta que se cierre o se renueve. El permiso no se cierra mientras un bloqueo (LOTO) ligado siga aplicado.»
  - **Operador o supervisor:** «Si a usted lo invitan a investigar un incidente, queda en el equipo de investigación (ISO 45001 pide que participen trabajadores).»
  - **RH:** «Al aprobar una incapacidad del tipo «Riesgo de trabajo (IMSS)», el SGI abre solo el incidente con la persona y los días; el Jefe MAST lo investiga. No escriba el diagnóstico en la descripción de la ausencia.»
  - **Manual del Jefe MAST:** sección 11 (acciones planificadas): «desde 57.96.0, «SGI: Permisos de trabajo vencidos (cada hora)»». Secciones nuevas: «Traspaso de riesgos ambientales» (dónde está, que primero cuenta, que solo se aplica con el visto bueno de Dirección, qué decide en cada renglón, que no archiva los riesgos salvo la casilla, que se puede volver a abrir para comprobar que cuenta 0); «Competencias por tipo de permiso» (sin filas no exige nada; cada persona las debe tener vigentes hasta el fin del permiso); «Contratistas» (fecha de evaluación SST en el contacto; el parámetro `quimibond_sgi.permit_contractor_eval_required` = 1 la vuelve obligatoria); «Incidentes desde incapacidades» (tipo de ausencia, parámetros `work_risk_leave_type_ids` y `work_risk_followup_days`).

- [x] **Step 2: Commit**

```bash
git add docs/sgi/usuarios docs/sgi/administracion
git commit -m "docs: SST y ambiente en los manuales (57.96.0)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
```

## Task 6.11: Revisión de lo que se tocó (antes de la versión)

- [x] **Step 1:** `grep -rn "action_mark_cumple\|action_mark_no_aplica\|instrument.*ambiental\|_compute_expired\|expired" addons/quimibond_sgi --include=*.py --include=*.xml` y confirmar que ningún otro lugar crea riesgos ambientales sin el contexto ni espera que los botones rápidos registren (en 05f476e9: solo `sgi_env_aspect.action_create_risk` y las pruebas ajustadas).
- [x] **Step 2:** `python3 tools/check_odoo_views.py --base-ref origin/main`: roles de `alert-*` y `btn`, `group` sin atributos en el `<search>` nuevo, orden padre-hijo en el manifest (la herencia del contacto no cambia de archivo).
- [x] **Step 3:** `python3 -m pytest addons/quimibond_sgi/tests/test_usted.py` no corre local (Odoo); revisar a ojo las cadenas nuevas contra la regla 11.

## Migración

- **No hay script de migración.** Columnas nuevas que crea el ORM, vacías: `sgi_risk.control_hierarchy`, `sgi_action_line.control_hierarchy`, `sgi_env_aspect.life_cycle_stage`, `sgi_incident.{sgi_effective, sgi_effectiveness_date, sgi_effectiveness_note, sgi_effectiveness_by, sgi_ineffective_count, sgi_from_leave}`, la tabla `sgi_incident_investigation_rel`, `res_partner.{sgi_sst_eval_valid_until, sgi_sst_eval_note}`, `hr_leave.sgi_incident_id`. `sgi_work_permit.expired` pasa de calculado a **guardado**: el ORM crea la columna y la calcula para los permisos existentes (0 en producción). Tablas nuevas: `sgi_work_permit_skill` (vacía) y las transitorias del asistente.
- **Registros nuevos:** la acción planificada `sgi_cron_work_permits` (XML nuevo `noupdate`), dos acciones, tres vistas primarias, dos menús, cinco líneas de ACL. La dependencia `hr_holidays` (ya instalada).
- **No hay migración de datos de negocio.** La ficha pedía un post-migrate que pasara los 5 riesgos ambientales a aspectos; lo hace el Jefe MAST con el asistente después del OK escrito de Jose (Q1). Los 25 requisitos legales, los 5 IPER altos y todo lo demás quedan como están.

## Task 6.12: Versión, CHANGELOG, documentación y build

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado), `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md` (mapa), este plan (casillas)

- [x] **Step 1: Versión** `'19.0.57.96.0'` en `__manifest__.py` (l.21).

- [x] **Step 2: CHANGELOG**, arriba de `## 19.0.57.95.0`

```markdown
## 19.0.57.96.0 — 2026-10-XX

**SST y ambiente** (auditoría 2026-10: N-06, N-07 e incidente desde la
incapacidad; es la ficha «57.95.0» del plan general, renumerada porque 57.95.0
fue «Rendimiento y robustez»). Plan:
`docs/superpowers/plans/2026-10-0X-sgi-57-96-0-sst-ambiente.md`.

### Agregado

- **Jerarquía de controles (ISO 45001 8.1.2):** `control_hierarchy` en
  `sgi.risk` («Control existente de mayor nivel») y en `sgi.action.line`
  («Jerarquía del control»): eliminación, sustitución, ingeniería,
  administrativo, EPP.
- **Incidente:** equipo de investigación (`investigation_team_ids`),
  verificación de eficacia (`sgi_effective` Eficaz / No eficaz, fecha, nota,
  quién verificó, cuántas veces salió no eficaz).
- **Permiso de trabajo:** pestaña «Bloqueos (LOTO)»; filtro «Vencidos».
- **Competencias por tipo de permiso** (Administración SGI → Configuración,
  `sgi.work.permit.skill`): qué competencia exige cada tipo de trabajo. Vacía
  no exige nada.
- **Contratista:** «Evaluación SST vigente hasta» y «Qué se revisó (SST)» en
  el contacto (pestaña SGI); solo el Jefe MAST los escribe.
- **Etapa del ciclo de vida** en el aspecto ambiental (`life_cycle_stage`).
- **«Traspaso de riesgos ambientales»** (Administración SGI → Configuración,
  solo Jefe MAST, `sgi.env.aspect.transfer`): propone un renglón por riesgo
  ambiental sin aspecto y, a mano, crea el aspecto «En evaluación» ligado al
  riesgo como su tratamiento, con nota en los dos y el antes y el después en
  el log. Conserva los riesgos salvo que se marque «Archivar». Idempotente;
  nada se borra.
- **Incidente desde la incapacidad:** una ausencia aprobada del tipo
  «Riesgo de trabajo (IMSS)» (o los del parámetro
  `quimibond_sgi.work_risk_leave_type_ids`) crea el incidente en «Reportado»
  con la persona y los días perdidos y avisa al Jefe MAST; la subsecuente
  (hasta `quimibond_sgi.work_risk_followup_days`, 3) suma días al mismo; sin
  diagnóstico. Dependencia nueva: `hr_holidays`.
- **Acción planificada «SGI: Permisos de trabajo vencidos (cada hora)»**
  (`sgi_cron_work_permits`, `noupdate`, solo el sistema): marca vencidos los
  permisos autorizados que pasaron su hora de fin y avisa sobre el permiso al
  jefe del área y al Jefe MAST; los avisos se cierran solos al cerrar, cancelar
  o renovar el permiso. 28 acciones planificadas del SGI.

### Cambiado

- **IPER de riesgo alto:** no se controla ni se cierra sin jerarquía declarada
  ni con EPP como único control. Solo en la transición a controlado o
  cerrado: no alcanza a lo ya controlado ni lo disparan las acciones.
- **Cerrar un incidente** pide además: equipo con al menos un trabajador sin
  personal a su cargo o un integrante de la Comisión de Seguridad e Higiene;
  eficacia «Eficaz» con fecha y nota (no futura, no antes de la última
  acción); tras un «No eficaz», una acción nueva terminada; si es moderado,
  grave o fatal con IPER ligado, el IPER reevaluado después del incidente.
  «No eficaz» regresa a «Acciones» y pide la acción nueva. Solo el Jefe MAST
  y Salud ocupacional registran la eficacia. El candado se revisa después de
  escribir (como la NC).
- **Permiso de trabajo:** `expired` guardado e indexado; no se cierra con un
  bloqueo (LOTO) ligado todavía aplicado; al solicitar y autorizar, cada
  persona que ejecuta debe tener vigentes las competencias del tipo; contratista
  sin evaluación SST vigente: aviso en la ficha (bloquea solo con
  `quimibond_sgi.permit_contractor_eval_required` = 1).
- **Aspecto ambiental en la matriz:** «Aspecto ambiental» ya no se elige al
  crear o reclasificar un riesgo a mano (solo lo pone la matriz con «Tratar
  como riesgo» o el traspaso; tampoco se duplica uno existente); los 5
  riesgos ambientales existentes se siguen editando. Registrar la evaluación de un aspecto pide la etapa del ciclo de
  vida.
- **Lo legal:** «Cumple», «Cumple parcialmente», «No cumple» y «No aplica»
  abren el asistente con la evidencia (o el motivo) obligatoria; ya no
  registran sin ella.

### Migración

Ninguna. Columnas nuevas vacías; `sgi_work_permit.expired` se calcula al
crearse la columna (0 permisos en producción). Registros nuevos: acción
planificada `noupdate` en un XML nuevo, vistas, acciones, menús y ACL.

### Datos de producción

- **El traspaso de los 5 riesgos ambientales no se aplica en el despliegue.**
  Solo con el **visto bueno escrito de Jose** (en el PR), el Jefe MAST abre
  Administración SGI → Configuración → «Traspaso de riesgos ambientales»,
  revisa que proponga 5 renglones (RSG-2026-09 a 13), corrige actividad, tipo,
  condición y etapa, y traspasa. Los 5 aspectos quedarán significativos
  (puntaje 12 = «Severo», 8 = «Moderado» en la matriz) y piden control
  operacional para evaluarse.
- Los 5 IPER de riesgo alto (todos «Identificado», sin acciones) pedirán la
  jerarquía cuando se controlen.
- La configuración de competencias por tipo de permiso queda vacía y el
  parámetro del contratista apagado hasta que Dirección decida (Q4).

### Decisiones por omisión (preguntas del plan)

- (llenar con las respuestas de Jose a Q1–Q11, o «por omisión» en cada una)

**Pruebas:** `test_sst_ambiente` (20 casos: IPER alto con solo EPP, sin
jerarquía, no retroactivo; «ambiental» solo desde la matriz; ciclo de vida;
traspaso que solo cuenta al abrir, que liga e idempotente, archivo solo a
pedido; botones legales que abren el asistente; equipo con trabajador o
Comisión, eficacia y «No eficaz», IPER reevaluado, solo quien investiga
registra la eficacia; permiso vencido guardado y aviso cada hora, cierre con
LOTO aplicado, competencias por tipo, contratista con aviso o bloqueo;
incapacidad aprobada que crea el incidente y suma la subsecuente, otro tipo no
y rechazo con nota, aprobación que no falla por el SGI). Ajustadas:
`test_entrega4.test_07` y `test_audit_hardening.test_a6` (equipo y eficacia
antes de cerrar), `test_ola_certificable` (evaluación con el asistente),
`test_env_aspect` (etapa del ciclo de vida) y `test_work_permit` (sin
configuración real de competencias ni parámetro del contratista).

**Verificación pendiente en Odoo.sh:** (1) los caminos de aprobación de
`hr.leave` en Odoo 19 pasan por `write` de `state`; (2) `valid_to` de
`hr.employee.skill` en tipos que no son certificación; (3) que el cron
conserve `expired` escrito sobre un calculado guardado; (4) que agregar
`hr_holidays` a `depends` no pida reinstalar. (Los nombres de campos de
`hr.leave`, `hr.leave.type` y `hr_skills` ya se verificaron por MCP.)
```

- [x] **Step 3: Documentación técnica generada**

```bash
python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
```

(Debe aparecer el cron nuevo en `docs/sgi/tecnica/crons.md`, los modelos `sgi.work.permit.skill`, `sgi.env.aspect.transfer` y `sgi.env.aspect.transfer.line`, y los campos nuevos.)

- [x] **Step 4: Mapa del plan general.** En `docs/superpowers/plans/2026-10-01-sgi-auditoria-implementacion.md`: renglón «57.95.0 → 57.96.0 o siguiente libre» del mapa y título de la ficha: «57.95.0 → entregada como **57.96.0** (2026-10-XX)»; renglón y ficha «57.96.0 — Cláusulas y revisión por la dirección»: «→ 57.97.0 o siguiente libre». Guardar este plan como `docs/superpowers/plans/2026-10-0X-sgi-57-96-0-sst-ambiente.md`.

- [x] **Step 5: Checadores** (sección 0, punto 4). Esperado: 0 errores (sin la advertencia de bump).

- [ ] **Step 6: Commit, push y build**

```bash
git add -A addons/quimibond_sgi docs/sgi docs/superpowers/plans
git commit -m "quimibond_sgi 19.0.57.96.0: SST y ambiente (N-06, N-07, incidente desde la incapacidad)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BRxbxfgk8QnBprCruiNKx7"
git push
```

Esperado en el build de la rama (`--test-tags /quimibond_sgi`): `test_sst_ambiente` 20/20, `test_menu_tree`, `test_env_aspect`, `test_work_permit`, `test_ola_certificable`, `test_entrega4`, `test_audit_hardening`, `test_incident`, `test_ola_b`, `test_risk` OK, y ningún fallo nuevo respecto del build de `main`. Si falla una prueba existente que cerraba un incidente con un usuario real, agregarle equipo y eficacia (no relajar el candado). Si falla una que creaba un riesgo ambiental a mano, crearlo con `with_context(sgi_from_env_aspect=True)` o desde un aspecto. En el `update.log`: sin errores de vistas ni «no puede ser localizado»; la acción planificada nueva creada.

- [x] **Step 7: Marcar las casillas** de este plan y commit (`docs: plan 57.96.0 al día`).

- [ ] **Step 8: Verificación en Odoo.sh y en producción** (solo lectura, por MCP)
  - `ir.module.module [('name','=','quimibond_sgi')]` → `latest_version = 19.0.57.96.0`.
  - `get_fields('sgi.risk', ['control_hierarchy','sgi_env_aspect_ids'])`, `get_fields('sgi.incident', ['investigation_team_ids','sgi_effective'])`, `get_fields('sgi.work.permit', ['expired'], attributes=['store'])` → `store: true`, `get_fields('hr.leave', ['sgi_incident_id'])`.
  - `ir.cron [('name','ilike','Permisos de trabajo vencidos')]` → 1 activo con `interval_type = hours`.
  - `sgi.risk [('instrument','=','ambiental')]` → 5 (nada cambió); `sgi.env.aspect` → 0 (hasta que el Jefe MAST aplique el traspaso); `sgi.work.permit.skill` → 0.
  - Después del traspaso, **solo** con el OK de Jose: `sgi.env.aspect [('risk_id.instrument','=','ambiental')]` → 5, todos `state = borrador`; `sgi.risk [('instrument','=','ambiental'),('active','=',True)]` → 5 (por omisión).
  - Después de la primera incapacidad por riesgo de trabajo aprobada: `aggregate_records('sgi.incident', ['state'], [('sgi_from_leave','=',True)])` (solo conteos).

---

## Preguntas para Jose (con la opción por omisión)

- **Q1. Traspaso de los 5 riesgos ambientales a la matriz (cambio de datos de negocio).** **Por omisión:** no se hace en el despliegue; con su OK escrito en el PR, el Jefe MAST corre «Traspaso de riesgos ambientales» (propone los 5, corrige actividad, tipo, condición y etapa, crea los aspectos «En evaluación», nota en cada registro, log antes y después). Los riesgos **se conservan** activos y ligados como tratamiento; se archivan solo si usted lo pide (casilla del asistente). La «Oportunidad» RSG-2026-12 (merma de fibra) entra como aspecto salvo que prefiera dejarla solo como oportunidad (se quita su renglón). Se anota en `docs/audit/decisiones.md`.
- **Q2 (= Q10 de la auditoría). ¿La matriz oficial es el Excel 4868 y se carga a `sgi.env.aspect`?** **Por omisión:** esta entrega no lo carga (es dato, no código); deja la matriz lista (etapa del ciclo de vida, un solo lugar). Si confirma, el Jefe MAST la importa con la importación estándar de Odoo después del traspaso, y una entrega posterior pasa la sección 5 del procedimiento impreso a leer aspectos.
- **Q3 (= Q9). Gestión del cambio (9001 6.3, 45001 8.1.3) con la MOC archivada (D-15).** **Por omisión:** sin cambios en esta entrega; D-15 sigue (categoría archivada) y el cambio se demuestra con el cambio documental y el ECO. Si decide reactivar la categoría MOC, es un cambio de datos (activar `data/sgi_moc_data.xml:16`) que va en otra entrega.
- **Q4 (= Q11). Contratistas: ¿quién los evalúa en SST (REPSE, SUA, DC-3, inducción)? ¿Se instala Frontdesk?** **Por omisión:** el Jefe MAST registra «Evaluación SST vigente hasta» en el contacto; el permiso **solo avisa** (parámetro `quimibond_sgi.permit_contractor_eval_required` = 0). Con su OK se enciende y bloquea. Frontdesk no se instala.
- **Q5. IPER de riesgo alto con solo EPP: ¿se permite con justificación escrita?** **Por omisión:** no (el EPP es el último recurso en 45001 8.1.2); debe existir un control de mayor nivel.
- **Q6. Incidente desde la incapacidad.** **Por omisión:** se usa el tipo que ya existe, «Riesgo de trabajo (IMSS)» (no se crea uno nuevo); el incidente nace «moderado» (lesión con días perdidos) y quien investiga lo reclasifica; el aviso va solo al Jefe MAST (Salud ocupacional no tiene miembros); una incapacidad que empieza hasta 3 días después de otra ligada a un incidente abierto se suma al mismo.
- **Q7. Dependencia nueva `hr_holidays` en `quimibond_sgi`.** Ya está instalado en producción. **Por omisión:** sí. Alternativa: un satélite `quimibond_sgi_holidays` con `auto_install` (más archivos, y hay que confirmar que Odoo.sh lo instale solo).
- **Q8. ¿Quién cuenta como «trabajador» en el equipo de investigación?** **Por omisión:** un empleado sin personal a su cargo, o un integrante de la Comisión de Seguridad e Higiene (grupo 330, hoy sin miembros; la pista de datos de la semana 1 pide llenarlo).
- **Q9. Eficacia del incidente: ¿todos (también casi accidentes) y sin plazo mínimo?** **Por omisión:** todos, sin esperar N días (la NC espera 90). Alternativa: un parámetro de días mínimos como `nc_effectiveness_days`.
- **Q10. Competencias por tipo de permiso: ¿cuáles exige cada tipo?** **Por omisión:** la lista queda vacía (no se exige nada) hasta que el Jefe MAST la capture; propuesta para capturar: alturas → NOM-009, espacio confinado → NOM-033, caliente → NOM-027, eléctrico → NOM-029 (las competencias existen en el tipo «Capacitación SGI y STPS (F-P-A01-04)» si ya están dadas de alta). **Ojo:** ese tipo no es de certificación (`is_certification = False`, MCP 2026-10-02); si Odoo 19 no guarda la vigencia (`valid_to`) en competencias que no son certificación, el candado solo revisa que la persona tenga la competencia, no que esté vigente. Para que la DC-3 venza, el tipo debe marcarse como certificación (decisión de datos de RH, no de esta entrega).
- **Q11. Aviso de permiso vencido cada hora al jefe del área y al Jefe MAST.** **Por omisión:** sí, uno por permiso y persona (no se repite cada hora: se actualiza el mismo aviso).

---

## Revisión del plan (2026-10-02, contra 05f476e9)

Hecha. Líneas y anclas citadas comprobadas (riesgo, acción, incidente, permiso, LOTO, aspecto, legal, cron, menús, ACL, vistas). `sgi_bypass_allowed` no hace falta en el incidente (el código usa `env.su`); la página nueva no repite campos de la ficha (`days_lost` sigue en «Evento»); `test_fichas_busquedas.test_02` no mira los botones de lo legal y los del incidente y riesgo no cambian; `test_vistas_pulido_45.test_01` exige acción = menú (se cumple en los dos menús nuevos). Cambios aplicados en esta revisión: helper `_locked` en lugar de `assertRaisesRegex` (sin savepoint, dejaba el incidente «cerrado» y `test_11` fallaba en `_sgi_check_closed_origin`); `force_save` en `risk_id` del asistente de traspaso; tipo de incapacidad de prueba con validación `hr` (cubre el `write`); prueba 16 con `is_certification`; `_sgi_link_incident` sin nota en la segunda pasada ni días en incidentes cerrados; guardia de grupo en `default_get` del asistente, renglones aplicados fuera de la ficha y contadores que suman; `create` del contacto con la misma guardia que `write`; `depends` del contratista; aviso de permiso vencido sin jefe de área fuera del SGI; filtro «Vencidos sin cerrar» reutilizado; verificaciones de 1.10 hechas por MCP.
