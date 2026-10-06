# Entrega «SGI en Conocimiento» — `quimibond_sgi_knowledge` 19.0.1.2.0 (plan de implementación)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Estado (2026-10-06):** implementado en `quimibond_sgi_knowledge` 19.0.1.2.0 (sin tocar `quimibond_sgi`; base `origin/main` en `34323408`, con el núcleo ya en 19.0.57.114.0). Desviaciones en el README del satélite (sección 1.2.0) y en el mensaje del commit. Lo que sigue es el plan tal como se aprobó.
>
> **Estado del plan al aprobarse:** plan; nada implementado. Base: `origin/main` en `ec86fd1d` (merge del PR #545). `quimibond_sgi` está en **`19.0.57.113.0`** en `main` y en producción (MCP, `ir.module.module`, 2026-10-06); `quimibond_sgi_knowledge` en **`19.0.1.1.0`** en ambos. **Esta entrega no toca `quimibond_sgi`** (sección 2.7): solo el satélite sube a `19.0.1.2.0`. Si durante la implementación resultara indispensable tocar el núcleo, toma el siguiente número libre (hoy `19.0.57.114.0`; otra sesión también publica versiones del SGI: **renumerar al hacer merge**, carpeta de migración, sección del CHANGELOG y este archivo).

**Goal (aprobado por el CEO, todas las opciones por omisión):** que la documentación de texto del SGI viva en la app **Conocimiento** (`knowledge`): un espacio de trabajo «SGI» con los manuales por rol (sembrados desde el repo), un artículo por proceso con sus instructivos, controles operacionales y protocolos, y los reglamentos; ayuda contextual desde SGI → Inicio; un importador por lotes de los 63 documentos controlados vigentes de esos tipos; y la publicación como revisión controlada (DOC-5) extendida a CO, protocolos y reglamentos. **Sin cambiar la huella de «Mi procedimiento» ni la del MIID por esta entrega, sin borrar nada y sin tocar los documentos controlados al importar.**

**Architecture:** todo en el satélite `addons/quimibond_sgi_knowledge` (ya depende de `quimibond_sgi` y `knowledge`, `auto_install`).
- **Modelos:** `knowledge.article` (extensión: tipo SGI, clave de siembra, huella de siembra, proceso, clave del documento, estado de publicación); `sgi.process` (artículo del proceso); `sgi.activity.role` (artículo legible para la pantalla de Mi procedimiento, fuera de la huella); asistentes nuevos `sgi.knowledge.import` (importador) y el `sgi.instruction.publish` existente generalizado (cuatro tipos).
- **Datos:** `data/manuales/*.html` generados en desarrollo desde `docs/sgi/**/*.md` con `tools/sgi_knowledge_html.py` (con `--check` en CI); `data/sgi_knowledge_data.xml` con `<function>` que siembra en cada actualización, la acción «Ayuda» y un cron diario de miembros.
- **Vistas del satélite:** lista/búsqueda propias de `knowledge.article` (no heredan de Knowledge), menús propios colgados de carpetas del SGI (como `quimibond_sgi_mapa`), herencias de vistas **de otro módulo** (`quimibond_sgi`) solo para botones: ficha del documento, ficha del proceso, tarjeta/lista de Mi procedimiento.
- **Migración** `migrations/19.0.1.2.0/post-migrate.py`: sincroniza miembros y avisa al Jefe MAST (la siembra la hace el `<function>`).
- **Sin cambios en `quimibond_sgi`**, sin ACL nuevas para usuarios (solo para los asistentes), sin borrar registros.

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh; `knowledge` 19.0.1.0, `documents` 19.0.1.4, `attachment_indexation` 19.0.2.1, `sign`, `approvals`), Python 3, XML de vistas, `odoo.tests.TransactionCase`. Checadores: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`, `flake8`, y el nuevo `tools/sgi_knowledge_html.py --check`.

**Base de código verificada:** HEAD `ec86fd1d`. Las líneas citadas son de ese HEAD; si cambian, buscar por nombre. **Odoo 19 (Knowledge, pypdf) no está en este contenedor**: lo que depende de su API va marcado **«VERIFICAR:»** y se juntó en la sección 9.

---

## 0. Reglas (de `CLAUDE.md` y de los planes 57.98.0 a 57.113.0)

1. **Versión:** `addons/quimibond_sgi_knowledge/__manifest__.py` → `'19.0.1.2.0'`. El satélite **no tiene CHANGELOG** (`ls addons/quimibond_sgi_knowledge`: README, manifest, models, report, security, static, tests, views), así que `check_addons.py` no exige sección; el README lleva un párrafo «1.2.0». Modelos nuevos sin bump son **error** de `check_addons.py`: con el bump pasa.
2. **Menús:** la regla «solo en `views/sgi_menus.xml`» es del módulo `quimibond_sgi` (`tests/test_menu_tree.py::test_04`, l.108-116, solo mira xmlids de `quimibond_sgi`). Los satélites cuelgan sus menús de carpetas del SGI desde su propio XML: precedentes `addons/quimibond_sgi_mapa/views/sgi_mapa_views.xml:75-77` (bajo `quimibond_sgi.menu_sgi_transition`) y `addons/quimibond_sgi_revisado/views/mrp_revision_log_views.xml:60`. `test_menu_tree::test_01` (l.65-80) **quita de la comparación** los menús de satélites instalados (`_satellites()`, l.59-63) y `tools/sgi_menu_tree.txt` **no** los lista. `_sgi_menu_tree_offenders` (`models/sgi_cleanup.py:109-130`) acepta todo lo que cuelga de las ocho entradas. **No se toca `sgi_menus.xml` ni el árbol.**
3. **Sin herencias propias:** la regla es «un módulo no hereda sus propias vistas». El satélite **sí** hereda vistas de `quimibond_sgi` (precedente DOC-5: `views/sgi_instruction_knowledge_views.xml:4-18`). Anclas estables (`field name=…` que ya existe), nunca `xpath` por posición. Las vistas propias del satélite (lista de artículos, asistentes) van completas en su `<record>`.
4. **Pruebas:** registradas en `tests/__init__.py`, `@tagged('post_install', '-at_install')`. Corren **solo en Odoo.sh** (build de desarrollo de la rama) con **`--test-tags /quimibond_sgi_knowledge`** (y `/quimibond_sgi` para no romper el núcleo). La base del build es copia de producción: ninguna prueba cuenta registros globales (`sgi_hide_real_documents`, `addons/quimibond_sgi/tests/common_documents.py:16`).
5. **«Usted» y glosario** (Jefe MAST, NC, CoA) en todo texto al usuario, incluidos los manuales y los mensajes del chatter. `tests/test_usted.py` solo recorre `quimibond_sgi`; el satélite agrega su propia prueba con las mismas expresiones (Task K7).
6. **Nada se borra:** ni artículos, ni documentos, ni adjuntos. Lo que sobra se archiva (`active=False`) o se renombra. **Ojo:** en Knowledge «mandar a la papelera» (`to_delete=True`) borra solo a los N días; el código nunca usa papelera.
7. **Producción es de solo lectura** para nosotros. Despliegue según `docs/RUNBOOK_DESPLIEGUE.md` (`odoo-update quimibond_sgi_knowledge`); verificación por MCP de solo lectura (sección 7). **Nunca abrir el documento 3560 (P-I01)**: tiene credenciales.
8. **Checadores locales** (0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   python3 tools/sgi_knowledge_html.py && python3 tools/sgi_knowledge_html.py --check
   flake8 addons/quimibond_sgi_knowledge tools/sgi_knowledge_html.py
   ```

---

## 1. Contexto y alcance

### 1.1 Lo que ya existe en el satélite (DOC-5, 53.0.0; satélite desde 57.9.0)

| Pieza | Dónde | Qué hace |
|---|---|---|
| `sgi.process.activity.instruction_article_id` | `models/sgi_instruction_knowledge.py:18-21` | M2o al artículo donde se escribe el instructivo |
| `instruction_article_stale` | `:22-24`, cálculo `:32-38` | Verdadero si `doc.sgi_article_id == artículo` y `doc.sgi_content_hash != _sgi_article_hash()` |
| `_sgi_article_hash()` | `:26-30` | sha256 de `nombre + "\n" + body` (HTML crudo) |
| `action_publish_instruction` | `:40-49` | Abre el asistente con la actividad y la clave del IT ligado |
| `documents.document.sgi_article_id` | `:52-57` | M2o `readonly=True`, «Artículo del que se congeló esta revisión» |
| `sgi.instruction.publish` (TransientModel) | `:60-125` | Solo Jefe MAST (`:83-84`); renderiza el PDF (`report/report_knowledge_instruction.xml`), crea un documento **nuevo** `sgi_doc_type='instructivo'`, `sgi_state='vigente'` (el `create` del núcleo obsoleta la revisión anterior por clave), revisión = máx + 1, `sgi_job_ids` = puestos que ejecutan, huella, `action_generate_acks()`, y escribe `activity.instruction_id` |
| Vista | `views/sgi_instruction_knowledge_views.xml:4-18` | Hereda `quimibond_sgi.sgi_process_activity_view_form` después de `instruction_id`; botón solo `group_sgi_manager` |
| ACL | `security/ir.model.access.csv` | Asistente solo Jefe MAST |
| Prueba | `tests/test_instruction_knowledge.py:23-54` (`test_05`) | Publica, acuses, «no cambió», stale, revisión 1, anterior obsoleta |

**Lo que DOC-5 no hace y esta entrega necesita** (evidencia en 1.3): no copia del vigente `sgi_previous_code`, `sgi_parent_document_id`, `folder_id`, `sgi_area_id`, título; nombra el archivo «CLAVE título (Rev. NN).pdf» (cambia `sgi_title`); solo re-apunta **una** actividad (la del asistente) aunque otras usen la misma revisión; sin actividad no sabe a qué puestos pedir acuse.

### 1.2 Producción (2026-10-06, MCP, solo lectura, compañía 1)

| Qué | Cifra |
|---|---|
| Módulos | `knowledge` 19.0.1.0, `website_knowledge`, `documents` 19.0.1.4, `attachment_indexation` 19.0.2.1 (extrae texto de PDF a `ir.attachment.index_content`), `sign`, `approvals`, `html_editor`; `quimibond_sgi` 19.0.57.113.0; `quimibond_sgi_knowledge` 19.0.1.1.0 |
| Artículos de Conocimiento | **226** (activos e inactivos): 82 de espacio de trabajo (15 `write`, 1 `read`, 66 heredan), 141 privados, 3 sin categoría |
| ¿Existe un espacio «SGI»? | **Sí: id 102 «SGI»** (junio 2025), `internal_permission='write'`, 2 miembros, **20 artículos** («0. Anexos», «1. Manual De Información Documentada», «2.0 Procedimientos», «3. Instructivos De Trabajo» … «13. Revisión Por Dirección»), casi vacíos (resúmenes de una línea o vacíos), última edición 2025-06-17, 0 favoritos. Ver decisión N1 |
| Uso de DOC-5 | 0 actividades con `instruction_article_id`; 0 documentos con `sgi_article_id` (README del satélite y MCP) |
| Idiomas activos | `en_US`, `es_MX`, `es_ES` |
| Usuarios internos | 38 activos (42 portales); **31** directos en «Usuario SGI» (316), **2** en «Jefe MAST y SGI» (318), 1 en Dirección (319), 0 en Auditor (317) |
| Procesos de la compañía 1 | **14**: E1, E2 (estratégicos); C1–C6 (clave); S1–S6 (soporte). C2 en `piloto`, el resto `borrador`. `owner_id` es `hr.employee` |
| Categoría de cambio documental | **id 12** «Modificación de documento SGI», `sgi_sign_required=True`; **0 solicitudes** en toda su historia |
| Documentos controlados vigentes de los 4 tipos | **63, todos PDF**: instructivo **49**, control_operacional **5**, protocolo **5**, reglamento **4**. Todos con `access_internal='view'`, `lock_uid` vacío, `sgi_job_ids` **vacío** (ninguno tiene acuses por puesto), proceso SGI capturado en los 63 |
| Por proceso | C1: 1 IT · C2: 2 IT · C4: **17 IT-C4** + 6 IT-P-I01 · C5: 16 IT · C6: 1 IT · E1: 1 IT · E2: 5 IT + **5 CO** (CO-E2-01…05) + 5 PROT (PROT-E2-01…05) + **4 reglamentos** (R-P-A14-01…04) |
| Familia P-I01 (exclusión L-001) | **6** IT (IT-P-I01-01, -03, -05, -07, -08, -14), hijos de 3560: el importador **no** los toca (sección 3.4) → **57 importables** |
| Ligados a una actividad (`instruction_id`) | **16 documentos / 17 actividades** (IT-E1-01 lo usan E1.09 y E2.14): C1.04, C2.04, C2.08, C4.05, C4.08, C4.09, C4.12, C4.14, C4.17 (IT-P-I01-08, excluido), C5.03, C5.04, C5.16, C6.05, E1.09, E2.14, E2.22, E2.24 (IT-C4-13). **15 importables** con actividad; 42 sin actividad conocida; ningún CO, PROT o reglamento tiene actividad |
| Tamaños | de 111 KB a 5.4 MB (IT-P-I01-07, excluido); el mayor importable 4.6 MB (IT-C5-01) |

### 1.3 Evidencia de código que condiciona el diseño

- **Huella de «Mi procedimiento»** (`addons/quimibond_sgi/models/sgi_my_procedure.py:482-502`): por entrada detallada entran `activity.id, role_codes, when, number, name, parts, escalates_to, inputs, outputs, related, instruction`; además `short`, `received`, `documents` (`[[clave, revisión]]` de `_sgi_mp_documents`, l.386-396, que salen de `activities.instruction_id | format_document_ids | related_procedure_id` y de `sgi_job_ids`, l.368-384) y `epp`. `instruction` = `instruction_id.sgi_code or name` (`_sgi_mp_entry_extra`, l.433-480, clave l.465). `parts` = `_sgi_sentence_parts()` (`models/sgi_activity_spec.py:548-578`), que **no** mira el instructivo. **Conclusión:** `instruction_article_id`, `sgi_article_id` y cualquier campo nuevo **no entran** a la huella; **sí** entrarían un cambio de `instruction_id` o una revisión nueva de un documento que el puesto usa. El importador **nunca** escribe `instruction_id`; la publicación sí (es una revisión nueva: releer y firmar es lo correcto).
- **Pantalla de Mi procedimiento** (`models/sgi_my_procedure_screen.py:343-416`): campos calculados de `sgi.activity.role` (`mp_instruction_id` related, l.363-365; botón «Instructivo» → `action_open_instruction`, `models/sgi_structure.py:108-113`, abre el PDF). La pantalla **no** pasa por `_sgi_my_procedure_hash`; un campo y un botón más en la tarjeta no la tocan. El PDF (`report/report_my_procedure.xml:115-116`) no se toca.
- **Huella del MIID** (`models/sgi_miid.py:448-534`, `_sgi_hash` l.536-540, fuera de huella solo `MIID_UNHASHED`, l.60): mira secciones, procesos, flujos, política, objetivos, tipos de documento, **`controls` = CO vigentes `{clave: [revisión, título, clave anterior]}`** (l.488-492), NC, normas, procedimientos anteriores, anexos. **No** mira artículos ni `sgi_article_id`. **Conclusión:** importar no la cambia; **publicar un CO** cambia `controls` (revisión) y, si se pierden título o clave anterior, también eso: la publicación copia `sgi_previous_code` y conserva el título (sección 3.5).
- **Candados del documento** (`models/sgi_document.py`): `SGI_TRANSITION_FIELDS` (l.37-41) solo Jefe MAST o sistema (`_sgi_check_transition_write`, l.1077-1094; `sgi_bypass_allowed`, `models/sgi_base.py:21-27`) — `sgi_article_id` **no** es de transición; `write` (l.1233-1310) solo reacciona a `active`, `sgi_state`, `sgi_revision`, `sgi_code`, `sgi_is_controlled`. `readonly=True` de `sgi_article_id` es solo de interfaz. Escribirlo con `sudo()` en un vigente no dispara obsoletado, acuses ni candado. Responsable obligatorio en controlados (`models/sgi_document_owner.py:40-46`).
- **Exclusión P-I01** (`sgi_document.py:47-50`, `_sgi_dropbox_excluded_domain` l.301-305): fuera la familia (`sgi_legacy_family`, calculada de `sgi_previous_code or sgi_code`, l.278-282) y los hijos (`sgi_parent_document_id.sgi_legacy_family`). Una revisión nueva **sin** `sgi_previous_code` ni padre **se escaparía** de la exclusión: otra razón para copiarlos.
- **Lectura de controlados** (`_sgi_share_controlled`, l.630-645): piloto y vigente → `access_internal='view'` para todo usuario interno. Lo que el importador copie a Conocimiento ya lo lee hoy cualquier usuario interno.
- **Acuses** (`action_generate_acks`, l.1312-1327): empleados cuyo `job_id` está en `doc.sgi_job_ids`. Los 63 tienen `sgi_job_ids` vacío. El flujo de cambios toma los puestos de las actividades activas del proceso cuando el documento no tiene (`models/sgi_doc_change.py:166-182`, `_sgi_notify_process`).
- **Flujo formal de cambios (DOC-1)** (`models/sgi_doc_change.py`): `_SGI_REVISION_FIELDS` (l.110-114) se copian del vigente a la revisión nueva; `_sgi_publish_revision` (l.123-155) crea el documento nuevo y obsoleta el anterior; `_sgi_apply_doc_change` (l.184-237) al aprobar. Con Sign (`models/sgi_doc_change_sign.py:1-19`): Elaboró → Revisó (dueño) → Aprobó (Jefe MAST). DOC-5 **no** pasa por la categoría 12: publica directo el Jefe MAST. Ninguno de los dos re-apunta `instruction_id` de las actividades a la revisión nueva.
- **Siembra que respeta ediciones** (`models/sgi_miid.py:216-264`, `_sgi_seed_update`): solo actualiza secciones sin el mensaje `MIID_EDITED_MSG` (l.48) y compara texto plano (`_miid_plain`), no HTML. Se copia la idea con una huella guardada (3.2).

### 1.4 Fuera de alcance

Procedimientos (`procedimiento`), formatos, DAT, anexos y diagramas; traducción de artículos; publicación en sitio web (`website_knowledge` instalado pero los artículos SGI no se publican); retirar el árbol 2025 (N1); que Knowledge sea documento controlado (los manuales son **material de consulta**, no llevan clave ni revisión).

---

## 2. Decisiones

### 2.1 Aprobadas por el CEO (por omisión)

1. Espacio **«SGI»** en Conocimiento: «Cómo usar el sistema» (un artículo por manual de `docs/sgi/usuarios/*.md` y `docs/sgi/administracion/manual-jefe-mast.md`, más «Glosario» y «Primeros pasos»), un artículo por proceso (14) con sus instructivos, CO y protocolos como hijos, y «Reglamentos». Todo usuario SGI lee; el Jefe MAST escribe.
2. Manuales sembrados desde el repo, **fuente de verdad = código (opción A)**: cada actualización refresca un artículo solo si nadie lo editó.
3. «Ayuda» en SGI → Inicio abre el manual del rol de quien entra; ligas desde pantallas clave. **La huella de Mi procedimiento y la del MIID no cambian por esta entrega.**
4. Importador por lotes de los 63 vigentes (IT 49, CO 5, PROT 5, REG 4): borrador por documento bajo su proceso, con el PDF original adjunto, ligado al documento y a su actividad cuando se sabe; idempotente; nunca modifica el documento controlado ni su revisión; registra lo que no pudo ligar.
5. «Publicar» (DOC-5) extendido a CO, protocolos y reglamentos, con el mismo nivel de control.
6. Flujo: dueños de proceso redactan, el Jefe MAST aprueba la publicación; se empieza por los 5 CO y los instructivos de C4.

### 2.2 Sub-decisiones nuevas (con la opción recomendada por omisión)

| # | Pregunta | Por omisión | Alternativa |
|---|---|---|---|
| **N1** | ¿Qué hacer con el espacio «SGI» de 2025 (id 102, 20 artículos casi vacíos)? | **Renombrarlo** a «SGI (estructura 2025, sin uso)» al sembrar, con nota en su chatter; no se mueve, no se archiva, no cambian sus permisos. El Jefe MAST decide después si lo archiva | (b) adoptarlo como raíz nueva (hereda `write` para todos y nombres viejos: no) · (c) archivarlo (nada se borra, pero Knowledge confunde archivar con papelera: riesgo 8) |
| **N2** | ¿Cómo pasar Markdown a HTML? | **En desarrollo**, con `tools/sgi_knowledge_html.py` (convertidor propio, sin dependencias: títulos, párrafos, listas, tablas, negritas, cursivas, código en línea, ligas) que escribe `addons/quimibond_sgi_knowledge/data/manuales/*.html` comprometidos; `--check` en el CI | Instalar `markdown` en Odoo.sh y convertir al sembrar (agrega una dependencia que el CI y Odoo.sh deben tener; Odoo 19 no trae convertidor de Markdown del lado del servidor — VERIFICAR V1) |
| **N3** | ¿Qué campo liga un borrador importado con su documento? | **`documents.document.sgi_article_id`** (lo pedido), con ayuda nueva «Artículo de Conocimiento del documento: borrador importado de su PDF o artículo del que se congeló esta revisión». El aviso «cambió desde la publicación» cuenta solo si la revisión **se congeló del artículo** (`sgi_content_hash` lleno) | Campo nuevo en el artículo y `sgi_article_id` solo para revisiones congeladas (dos ligas para lo mismo) |
| **N4** | ¿Quién ve un borrador importado antes de publicarse? | **Solo el dueño del proceso y el Jefe MAST** (artículo con `internal_permission='none'` y miembros). Al publicar se abre a todos y se **bloquea** (`is_locked`); quien quiera cambiarlo lo desbloquea (queda «cambió desde la publicación» y el Jefe MAST recibe aviso) | Visible para todos con aviso al inicio del texto (riesgo ISO 7.5.3: versión no aprobada en uso) |
| **N5** | ¿Publicar CO, protocolos y reglamentos pasa por la solicitud de cambio documental (categoría 12, Sign)? | **No: mismo camino que DOC-5** (lo publica el Jefe MAST desde el artículo; queda en el chatter del documento quién redactó y quién publicó). Es el control que hoy tienen los IT y la categoría 12 no se ha usado nunca (0 solicitudes) | (b) «Publicar» crea una solicitud de la categoría 12 con el PDF adjunto y Sign aplica la revisión (Elaboró → Revisó → Aprobó). Más evidencia; más pasos; habría que enseñar a `_sgi_apply_doc_change` a ligar el artículo |
| **N6** | ¿Los 6 IT de la familia P-I01? | **Fuera del importador siempre** (L-001, `SGI_DROPBOX_ALWAYS_EXCLUDED`), listados en el resumen como «excluidos por L-001». Si el Jefe MAST quiere uno, lo escribe a mano en Conocimiento | Importarlos tras revisión manual (rompe la regla L-001) |

---

## 3. Diseño

### 3.1 Modelos y campos (todos en el satélite)

**`knowledge.article` (extensión, `models/sgi_knowledge_article.py`):**

| Campo | Tipo | Para qué |
|---|---|---|
| `sgi_kind` | Selection `raiz`, `carpeta` («Cómo usar el sistema», «Reglamentos»), `manual`, `proceso`, `documento` | Qué es para el SGI; vacío = artículo ajeno al SGI |
| `sgi_seed_key` | Char, index | Clave de siembra (`raiz`, `ayuda`, `manual:operador-o-supervisor`, `manual:glosario`, `proceso:C4`, `reglamentos`). Única por compañía (restricción SQL parcial **no**: solo búsqueda + prueba; evita fallar en una base con duplicados heredados) |
| `sgi_seed_hash` | Char, readonly | Huella del texto plano normalizado que dejó la última siembra (3.2) |
| `sgi_process_id` | M2o `sgi.process`, index | Proceso del artículo (proceso y documento) |
| `sgi_document_code` | Char, index | Clave SGI del documento (`IT-C4-02`): llave del importador, sobrevive a revisiones |
| `sgi_document_id` | M2o `documents.document`, calculado sin guardar | La revisión **vigente** con esa clave (búsqueda por `sgi_code`, `sgi_state='vigente'`, sudo) |
| `sgi_doc_type` | Selection (los 4 tipos) | Tipo del documento que explica |
| `sgi_publish_state` | Selection calculada sin guardar: `borrador` (nunca publicado), `publicado` (la vigente se congeló de este artículo y la huella coincide), `cambio` (se congeló de este artículo y la huella ya no coincide), `pdf` (la vigente es el PDF del Dropbox y el artículo es el borrador importado) | Lista de trabajo y botones |
| `sgi_import_note` | Text, readonly | Qué pasó al importar (texto extraído, páginas, «sin texto: posible escaneo», actividad ligada o sugerida) |

Método `_sgi_article_hash()` se **mueve** a `knowledge.article` (el de la actividad lo llama: misma fórmula `nombre + "\n" + body`, así `test_05` y las huellas ya guardadas —0 en producción— no cambian).

**`documents.document`:** solo cambia el `help` de `sgi_article_id` (N3). **Sin** campos nuevos.

**`sgi.process.activity`:** `_compute_instruction_article_stale` exige además `doc.sgi_content_hash` (N3): un borrador importado no prende el aviso. Se queda `instruction_article_id`.

**`sgi.process` (extensión):** `sgi_article_id` M2o `knowledge.article` (readonly, «Artículo del proceso en Conocimiento») y botón «Abrir en Conocimiento».

**`sgi.activity.role` (extensión de la pantalla):** `mp_article_id` M2o calculado (sudo): el `instruction_article_id` de la actividad **solo si** su `sgi_publish_state` es `publicado` o `cambio` (es decir, lo pueden leer todos). Botón «Leer en Conocimiento». **No** toca `_sgi_mp_entry_extra` ni `_sgi_my_procedure_data` (1.3).

**Asistentes:** `sgi.knowledge.import` (3.4) y `sgi.instruction.publish` generalizado (3.5).

### 3.2 Siembra del espacio y de los manuales (`_sgi_kb_seed`)

Llamada desde `data/sgi_knowledge_data.xml` con `<function model="knowledge.article" name="_sgi_kb_seed"/>` **sin** `noupdate` (corre en cada instalación y cada `-u`; precedente `addons/quimibond_sgi/data/sgi_parameters.xml:13`). Superusuario, idempotente, nunca borra.

1. **Raíz** (`sgi_seed_key='raiz'`, nombre «SGI», icono «📘», `internal_permission='read'`, sin padre → categoría `workspace`). Si no existe: antes de crearla, N1 — buscar artículos raíz de espacio de trabajo **sin** `sgi_seed_key` llamados exactamente «SGI»; renombrarlos a «SGI (estructura 2025, sin uso)» y dejar en su chatter: «Se renombró al crear el espacio «SGI» del sistema (quimibond_sgi_knowledge 1.2.0). Su contenido no cambió.» Solo una vez (si ya se llama así, nada).
2. **Miembros** (`_sgi_kb_sync_members`): en la raíz, `write` para el partner de cada usuario activo de `group_sgi_manager` (incluye Administrador SGI por implicación); si el grupo está vacío, el partner del superusuario (Knowledge exige al menos un escritor cuando `internal_permission` no es `write` — VERIFICAR V2). En cada artículo de proceso, `write` para el partner del usuario del dueño (`owner_id.user_id`), si lo tiene. El método solo **crea** los miembros que falten (o sube `read` a `write`) y **nunca quita**: quitar a quien dejó de ser Jefe MAST o dueño lo hace a mano el administrador de Conocimiento (lo dice el README; la prueba lo documenta). Cron diario `quimibond_sgi_knowledge.ir_cron_sgi_kb_members` (03:40).
3. **Carpetas**: «Cómo usar el sistema» (`carpeta:ayuda`, secuencia 10), un artículo por proceso de la compañía del SGI ordenado E1, E2, C1…C6, S1…S6 (`proceso:<código>`, nombre «C4 — Producción», cuerpo corto generado: tipo de proceso, dueño, «Actividades del proceso en SGI → Sistema → Actividades»; se refresca con la misma regla de edición), y «Reglamentos» (`carpeta:reglamentos`, secuencia 90). Todos heredan el permiso de la raíz. Escribe `sgi.process.sgi_article_id`.
4. **Manuales** (hijos de «Cómo usar el sistema»), tabla `SGI_KB_MANUALS` en `models/sgi_knowledge_article.py`:

| Clave | Título | Archivo de datos | Fuente en el repo |
|---|---|---|---|
| `manual:primeros-pasos` | Primeros pasos | `data/manuales/primeros-pasos.html` | `docs/sgi/primeros-pasos.md` (**nuevo**) |
| `manual:operador-o-supervisor` | Manual del operador o supervisor | `operador-o-supervisor.html` | `docs/sgi/usuarios/operador-o-supervisor.md` |
| `manual:jefe-de-area` | Manual del jefe de área o dueño de proceso | `jefe-de-area.html` | `docs/sgi/usuarios/jefe-de-area.md` |
| `manual:mast` | Manual del Jefe MAST (día a día) | `mast.html` | `docs/sgi/usuarios/mast.md` |
| `manual:manual-jefe-mast` | Manual del Jefe MAST (configuración) | `manual-jefe-mast.html` | `docs/sgi/administracion/manual-jefe-mast.md` |
| `manual:direccion` | Manual de Dirección | `direccion.html` | `docs/sgi/usuarios/direccion.md` |
| `manual:rh` | Manual de RH | `rh.html` | `docs/sgi/usuarios/rh.md` |
| `manual:auditor` | Manual del auditor | `auditor.html` | `docs/sgi/usuarios/auditor.md` |
| `manual:glosario` | Glosario | `glosario.html` | `docs/sgi/glosario.md` (**nuevo**) |

5. **Regla de actualización (opción A):** por cada manual o carpeta sembrada:
   - `nuevo = html` del archivo (con ligas resueltas, 3.3); `plano(x)` = `html2plaintext(x)` con espacios colapsados (como `_miid_plain`).
   - No existe → crear con `body=nuevo`; leer de vuelta `body` (el sanitizador de Knowledge puede reescribirlo) y guardar `sgi_seed_hash = sha256(plano(body))`.
   - Existe y `sha256(plano(body actual)) == sgi_seed_hash` (nadie lo editó) y `plano(nuevo) != plano(actual)` → escribir `body=nuevo`, recalcular huella, chatter «Actualizado con la versión del sistema (quimibond_sgi_knowledge <versión>).»
   - Existe y la huella **no** coincide (alguien lo editó) → no se toca; chatter **una vez por versión** (buscar el mensaje con la versión): «La versión <versión> del sistema trae texto nuevo para este manual, pero no se aplicó porque el artículo se editó en Conocimiento. Compárelo con docs/sgi/… en el repositorio.» y `_logger.warning`.
   - Archivado por alguien → no se reactiva ni se recrea (búsqueda con `active_test=False`).
   Comparar texto plano y no HTML evita que un guardado del editor que solo normaliza etiquetas se cuente como edición (VERIFICAR V3).

### 3.3 `tools/sgi_knowledge_html.py` (N2)

- Lee las 9 fuentes de la tabla de 3.2 y escribe `addons/quimibond_sgi_knowledge/data/manuales/<nombre>.html` con un comentario de cabecera «generado por tools/sgi_knowledge_html.py desde <fuente>; no editar a mano».
- Subconjunto de Markdown que usan hoy los manuales (`grep` en 2026-10-06: tablas en los 7, **sin** bloques de código, ligas relativas): `#`…`####`, párrafos, listas `-`/`1.` con un nivel de anidación, tablas con `|---|`, `**`, `*`/`_`, `` ` ``, `[texto](liga)`, `«»` tal cual. HTML escapado salvo esas marcas.
- Ligas: `[x](operador-o-supervisor.md)`, `../usuarios/mast.md`, etc. → `<a href="sgi-kb:manual:operador-o-supervisor">x</a>` (marcador que `_sgi_kb_seed` cambia por la liga del artículo, VERIFICAR V4 para el formato de URL de Knowledge 19); ligas a `../tecnica/…` o `../transicion/…` → solo el texto en negritas con «(documentación técnica del repositorio)»; ligas `http(s)` → tal cual con `target="_blank"`.
- `--check`: genera en memoria y sale con 1 si algún `.html` difiere (mismo contrato que `tools/sgi_docs.py`). Se agrega al CI en `.github/workflows/ci.yml` junto a `sgi_docs.py --check` (no necesita `pip install` nuevo).
- Sin pruebas pytest propias (el repo solo corre pytest en `addons/quimibond_intelligence/tests`): lo cubren `--check` en el CI y, en el build, `test_kb_seed::test_03` (los archivos existen y no queda ningún `sgi-kb:` sin resolver tras la siembra).

### 3.4 Importador por lotes (`sgi.knowledge.import`)

**Acceso:** solo Jefe MAST (ACL + `has_group` en el método, como DOC-5 l.83-84). Menú «Importar documentos a Conocimiento» en **SGI → Administración → Transición** (`parent="quimibond_sgi.menu_sgi_transition"`, grupo `group_sgi_manager`), y acción de servidor en la lista de **Documentos** del SGI (`binding_model_id` = `documents.document`, solo Jefe MAST) para «Importar a Conocimiento» la selección.

**Campos:** `document_ids` (M2m `documents.document`, dominio: controlado, vigente, `sgi_doc_type in (instructivo, control_operacional, protocolo, reglamento)`, compañía del SGI, **más** `_sgi_dropbox_excluded_domain()`); atajos `preset` (`co_c4` = los 5 CO + IT de C4 [**22** en producción]; `todos` = los 57); `batch_size` (10 por omisión); `summary` (Text, resultado).

**Por documento (`_sgi_kb_import_one(doc)`), en un `savepoint` (un error no tumba el lote):**
1. **Saltar** si: `doc._sgi_is_dropbox_excluded()` (N6, «excluido por L-001»); `doc.access_internal != 'view'` («restringido: no se copia a Conocimiento»); ya hay artículo con `sgi_document_code == doc.sgi_code` (activo o archivado) → «ya importado» (idempotencia; si al documento le falta `sgi_article_id`, se completa la liga).
2. **Padre:** `reglamento` → «Reglamentos»; los demás → artículo del proceso (`doc.sgi_process_id.sgi_article_id`; si el documento no tiene proceso, «sin proceso: no se importó» — en producción 0 casos).
3. **Texto** (`_sgi_pdf_text(doc.attachment_id)`): (a) `attachment.index_content` si `attachment_indexation` lo llenó (pdfminer; VERIFICAR V5); (b) si viene vacío, `odoo.tools.pdf` (`PdfReader(io.BytesIO(raw)).pages[i].extract_text()`, VERIFICAR V6 el nombre exportado en Odoo 19); (c) nada → «sin texto: posible PDF escaneado». Más de 40 páginas o 8 MB → solo liga, sin texto («demasiado grande para extraer en línea»).
4. **Cuerpo:** aviso arriba (`<div class="alert alert-info" role="status">` — la regla de roles del checker): «Borrador importado del PDF de la revisión <NN> (<fecha>). El texto se extrajo sin formato ni imágenes y puede tener errores: compárelo con el PDF adjunto antes de publicar.»; liga «Abrir el PDF vigente» al documento; luego un `<p>` por párrafo (cortes por línea en blanco; saltos simples como `<br>`), escapado.
5. **Crear** el artículo: `name` = «<clave> <título>» (`doc.sgi_title`), `parent_id`, `sgi_kind='documento'`, `sgi_document_code`, `sgi_doc_type`, `sgi_process_id`, `sgi_import_note`; **permisos N4**: desincronizado del padre con `internal_permission='none'` y miembros `write` = dueño del proceso + Jefe MAST (VERIFICAR V2: `_desync_access_from_parents` o crear con `internal_permission` y miembros explícitos).
6. **Adjunto:** copia del `ir.attachment` del documento con `res_model='knowledge.article'`, `res_id` (el almacén deduplica por checksum: no ocupa disco nuevo).
7. **Ligas:** `doc.sudo().write({'sgi_article_id': article.id})` (N3; no es campo de transición, no dispara nada, 1.3); por cada actividad activa con `instruction_id` en la familia de la clave (todas las revisiones con esa `sgi_code`) **y** sin `instruction_article_id` → `instruction_article_id = article` (nunca `instruction_id`). Si no hay: sugerencias (actividades del mismo proceso cuyo `name`, `how_steps`, `check_against`, `done_criteria` o `place_note` contiene la clave o la clave anterior) **solo al resumen**, sin ligar.
8. **Registro:** línea en `summary` y en el chatter de la raíz «SGI» (un mensaje por corrida con la tabla: importados, ya importados, excluidos, restringidos, sin texto, con actividad, sugerencias); `_logger.info` (nunca `ERROR`: tumba las pruebas del build).

**Nunca** escribe en el documento otra cosa que `sgi_article_id`; nunca crea revisión, acuse, ni toca `sgi_state`, `datas`, `name`.

### 3.5 Publicación generalizada (DOC-5 para los cuatro tipos)

`sgi.instruction.publish` pasa a tener `article_id` (requerido), `activity_id` (opcional), `doc_type` (los 4; por omisión el `sgi_doc_type` del artículo o `instructivo`), `code` (por omisión la del vigente), `current_id` (calculado: vigente con esa clave), `job_ids`. Se abre desde: la actividad (como hoy, `action_publish_instruction`), el documento (botón en su ficha, 3.6) y la lista de trabajo (3.7). Solo Jefe MAST (sin cambio).

`action_publish`:
1. Huella del artículo; si el vigente tiene la misma `sgi_content_hash` → «El artículo no cambió desde la revisión vigente NN.» (sin cambio).
2. **Valores de la revisión nueva:** se copian del vigente `_SGI_REVISION_FIELDS` (`models/sgi_doc_change.py:110-114`, se importa la tupla de la clase o se repite en el satélite con referencia), **más** `sgi_previous_code`, `sgi_previous_code_date` (transición: permitido porque el método corre `sudo()` — `sgi_bypass_allowed` acepta `env.su`), `sgi_parent_document_id`; nombre del archivo «<clave> <sgi_title del vigente>.pdf» (el título no cambia y el MIID no ve un cambio de título; la revisión va en el chatter). Sin vigente (IT nuevo): como hoy.
3. **Puestos** (`_compute_job_ids`): actividades con `instruction_id` en la familia → `responsible_job_ids`; si no, `sgi_job_ids` del vigente; si no y es CO, protocolo o reglamento → los de las actividades activas del proceso (misma regla que `_sgi_notify_process`, l.166-176). El asistente muestra «Sin puestos: nadie recibirá acuse» si queda vacío.
4. Crea el documento (`sgi_state='vigente'`; el `create` del núcleo obsoleta el anterior, re-apunta la familia y respeta revisión creciente), `sgi_article_id`, `sgi_content_hash`, `action_generate_acks()`.
5. **Re-apunta** `instruction_id` de **todas** las actividades que apuntaban a una revisión anterior de la clave (hoy DOC-5 solo una; IT-E1-01 tiene dos) y pone `instruction_article_id` donde falte.
6. Artículo: se re-sincroniza con el padre (lo leen todos, N4) y `is_locked=True`; chatter del documento «<tipo> publicado desde el artículo «…», revisión NN. Redactó: <último editor>; publicó: <Jefe MAST>.» y del artículo «Publicado como revisión NN de <clave>.»
7. Consecuencia esperada (no es regresión): la revisión nueva cambia `documents` en la huella de Mi procedimiento de los puestos que usan ese documento (releer y firmar) y, si es CO, `controls` del MIID (revisión). Eso **es** el control de cambios.

N5 (b) queda descrito para el CEO; no se implementa.

### 3.6 Ligas contextuales

- **«Ayuda»** en **SGI → Inicio** (`parent="quimibond_sgi.menu_sgi_panel"`, secuencia 90, sin grupos: lo ve todo Usuario SGI): `ir.actions.server` → `env['knowledge.article']._sgi_kb_help_action()`. Elige el manual por grupo, en este orden: `group_sgi_manager` → `manual:mast`; `group_sgi_director` → `manual:direccion`; `group_sgi_auditor` → `manual:auditor`; `hr.group_hr_user` → `manual:rh`; `group_sgi_process_owner` o `group_sgi_efficiency_capture` → `manual:jefe-de-area`; si no → `manual:operador-o-supervisor`; si el artículo no existe o está archivado → «Cómo usar el sistema». Abre el artículo en Knowledge (VERIFICAR V4: acción cliente de Knowledge con `res_id` o `act_url`).
- **Ficha del documento** (`quimibond_sgi.sgi_document_view_form`, `views/sgi_document_views.xml:4-240`, herencia del satélite anclada **después de `sgi_owner_id`**, que aparece una sola vez en esa vista; `sgi_code` y `sgi_revision` se repiten y no sirven de ancla): `sgi_article_id` visible si tiene valor, botones «Abrir en Conocimiento» y «Publicar desde Conocimiento» (Jefe MAST, solo los 4 tipos).
- **Ficha del proceso** (`quimibond_sgi.sgi_process_view_form`, `views/sgi_process_views.xml:209`; ancla: un campo que aparezca una sola vez, a elegir al implementar): botón «Artículo en Conocimiento».
- **Mi procedimiento** (`quimibond_sgi.sgi_activity_role_view_kanban_mp` y `sgi_activity_role_view_list_my_procedure`, herencia del satélite): botón «Leer en Conocimiento» junto a «Instructivo» (`views/sgi_my_procedure_views.xml:74` kanban, `:117-118` y `:457-458` listas), visible si `mp_article_id`. «Instructivo» sigue abriendo el PDF controlado. **Fuera de la huella** (1.3) y del PDF.
- **Actividad:** se queda lo de DOC-5.

### 3.7 Lista de trabajo y flujo (decisión 6)

- Vista propia `sgi_kb_article_view_list` (lista completa en su `<record>`, `priority` 50 para no ser la de Knowledge) y búsqueda `sgi_kb_article_view_search` (`<group>` **sin** atributos). Columnas: proceso, clave, tipo, nombre, `sgi_publish_state` (badge), última edición y quién, actividad ligada. Filtros: «Por publicar» (`borrador`, `pdf`), «Cambió desde la publicación», «Controles operacionales», «Instructivos de C4», «Primero» (CO + IT de C4). Botones de fila: «Abrir», «Pedir publicación» (dueño: `sgi.cron._sgi_schedule(article, "Publicar <clave> desde Conocimiento", …, jefe_mast, key='kb_publicar')`, `models/sgi_cron.py:99`), «Publicar» (Jefe MAST).
- Menú **SGI → Sistema → Conocimiento del SGI** (`parent="quimibond_sgi.menu_sgi_processes"`, secuencia 95, grupos dueño de proceso, Jefe MAST, Dirección, Auditor). Acción con `search_default_primero` mientras haya CO o IT de C4 en `borrador`/`pdf` (simple: contexto fijo con ese filtro; el usuario lo quita).
- Texto guía en el `help` de la acción (usted): «Los dueños de proceso corrigen el borrador en Conocimiento, comparándolo con el PDF adjunto, y piden la publicación. El Jefe MAST revisa y publica: el artículo se congela como revisión nueva del documento con su PDF y los acuses de lectura. Empiece por los cinco controles operacionales y los instructivos de C4.»
- `instruction_article_stale` / `cambio`: lo recoge el aviso diario del Jefe MAST con el mismo cron de miembros (una actividad por artículo con `key='kb_cambio'`, no se duplica).

### 3.8 Permisos (resumen)

| Quién | Raíz «SGI», ayuda, procesos, reglamentos, publicados | Borradores importados de un proceso | Importar, publicar |
|---|---|---|---|
| Todo usuario interno (≈ Usuario SGI: 31 de 38) | lee (`internal_permission='read'`) | no ve (N4) | no |
| Dueño del proceso | lee; escribe en su proceso (miembro `write`) | escribe los de su proceso | «Pedir publicación» |
| Jefe MAST (2) | escribe (miembro `write` en la raíz) | escribe todos | sí |
| Administrador del sistema | lo que Knowledge le da | ídem | ídem |

Knowledge no tiene permiso por grupo: «lee todo Usuario SGI» se aproxima con `read` para todo usuario interno, el mismo alcance que ya tienen los controlados vigentes (`access_internal='view'`, 1.3). Lo restringido (`access_internal != 'view'`) no se importa.

### 3.9 Orden de carga y riesgos de Odoo.sh

- Manifest `data`, en este orden: `security/ir.model.access.csv`, `views/sgi_instruction_knowledge_views.xml`, `views/sgi_knowledge_views.xml` (lista, búsqueda, asistentes, herencias), `report/report_knowledge_instruction.xml`, `data/sgi_knowledge_data.xml` (acción «Ayuda», cron, `<function>` de siembra), `views/sgi_knowledge_menus.xml` (**al final**: los padres `quimibond_sgi.menu_sgi_panel`, `menu_sgi_processes`, `menu_sgi_transition` ya existen en 57.113.0; `check_forward_refs` lo revisa).
- Herencias de vistas de `quimibond_sgi` con anclas de campo únicas en su vista (`instruction_id` en la actividad, `sgi_owner_id` en el documento, el botón `action_mp_instruction` en la tarjeta y las listas de Mi procedimiento): si el núcleo cambia esas vistas en paralelo, el build de la rama lo dice («no puede ser localizado»). Ninguna vista propia del satélite se hereda a sí misma.
- El `<function>` corre en una base que ya tiene el espacio de 2025: N1 lo renombra antes de crear la raíz.
- `knowledge.article` ya tiene la columna `body` grande; 9 manuales + 15 procesos/carpetas + hasta 57 borradores ≈ 80 artículos: sin riesgo de tamaño.

---

## 4. Tareas

### Task K0: Rama
- [ ] `git fetch origin && git checkout -b claude/sgi-conocimiento origin/main`. Confirmar `quimibond_sgi_knowledge` en `19.0.1.1.0` y que nadie lo subió.

### Task K1: Pruebas nuevas (fallan antes del código)
Archivos nuevos en `addons/quimibond_sgi_knowledge/tests/`, registrados en `tests/__init__.py`, `setUpClass` con `sgi_hide_real_documents(cls.env)` y `sgi_skip_role_check` como `test_instruction_knowledge.py:17-21`.

- [ ] `test_kb_seed.py`
  - **test_01_espacio:** `_sgi_kb_seed()` dos veces → una sola raíz con `sgi_seed_key='raiz'`, categoría `workspace`, `internal_permission='read'`, al menos un miembro `write`; «Cómo usar el sistema», «Reglamentos» y un artículo por proceso de prueba (crear `sgi.process` `ZK1`) con `sgi.process.sgi_article_id`.
  - **test_02_espacio_viejo:** crear un artículo raíz «SGI» sin clave (`internal_permission='write'`) → tras sembrar se llama «SGI (estructura 2025, sin uso)», sigue activo, su permiso no cambió, un mensaje; segunda siembra no agrega mensaje.
  - **test_03_manuales:** los 9 artículos existen; `body` no contiene `sgi-kb:`; huella guardada = huella del cuerpo.
  - **test_04_opcion_a:** cambiar el archivo «virtualmente» (parchear `_sgi_kb_manual_html` con `patch.object` para devolver otro HTML) → el manual sin editar se actualiza; editar `body` de otro como usuario Jefe MAST → la siembra no lo pisa, deja un mensaje una vez por versión.
  - **test_05_archivado_no_revive:** archivar un manual → sembrar → sigue archivado, no hay duplicado.
- [ ] `test_kb_import.py`
  - Armado: proceso `ZK2`, dueño con usuario; documento controlado vigente `IT-ZK2-01` (PDF mínimo: generar con `self.env['ir.actions.report']._render_qweb_pdf` del reporte del satélite sobre un artículo, o un PDF fijo de bytes en el test); actividad con `instruction_id` = documento; un CO `CO-ZK2-01` sin actividad; un documento hijo de la familia excluida (`sgi_parent_document_id` con `sgi_legacy_family='P-I01'`).
  - **test_01_importa:** corre el asistente con los tres → 2 artículos; el IT bajo el artículo del proceso con adjunto PDF y `sgi_article_id`; actividad con `instruction_article_id`, **`instruction_id` igual**; el excluido en el resumen como «L-001».
  - **test_02_idempotente:** segunda corrida → 0 nuevos, «ya importado» ×2.
  - **test_03_no_toca_el_documento:** `sgi_state`, `sgi_revision`, `datas`, `name`, `sgi_content_hash`, acuses: iguales antes y después; `instruction_article_stale` es False (N3).
  - **test_04_borrador_restringido:** un `group_sgi_user` sin membresía no puede leer el borrador (`check_access('read')` lanza `AccessError`); el dueño sí escribe.
  - **test_05_restringido_no_se_copia:** documento con `access_internal='none'` (sudo) → «restringido», sin artículo.
- [ ] `test_kb_hashes.py` (**la red de la decisión 3**)
  - **test_01_mi_procedimiento_igual:** armar puesto, empleado, actividad con rol «ejecuta» e IT ligado (copiar el armado de `addons/quimibond_sgi/tests/test_my_procedure.py:17-60`, con `quimibond_sgi.mp_sign_required=False`); `h0 = job._sgi_my_procedure_data()['hash']`; sembrar, importar el IT, ligar `instruction_article_id`, abrir `mp_article_id` → `h1 == h0`.
  - **test_02_miid_igual:** `sgi.miid` de la compañía (armado de `addons/quimibond_sgi/tests/test_miid.py` l.~60); `Miid._sgi_hash(miid._sgi_snapshot())` igual antes y después de sembrar e importar un CO.
  - **test_03_publicar_si_cambia:** publicar el CO desde su artículo → `controls[clave][0]` sube 1, título y clave anterior iguales (3.5 paso 2).
- [ ] `test_kb_publish.py`
  - **test_01_co:** CO vigente rev 0 con `sgi_previous_code='P-Z99'`, padre y carpeta → publicar desde su artículo → rev 1 vigente, rev 0 obsoleta, nueva con `sgi_previous_code`, padre, carpeta, `sgi_doc_type='control_operacional'`, `sgi_title` igual, `sgi_article_id`, huella; artículo `is_locked` y legible por un usuario SGI.
  - **test_02_puestos:** sin actividad ni puestos → toma los del proceso; acuses creados.
  - **test_03_reapunta_todas:** dos actividades con el mismo IT → ambas apuntan a la revisión nueva.
  - **test_04_solo_mast:** un dueño de proceso no publica (`UserError`), sí «Pedir publicación» (una actividad para el Jefe MAST, no duplicada).
  - **test_05_stale:** editar el artículo publicado → `sgi_publish_state='cambio'` y `instruction_article_stale`.
- [ ] `test_kb_ligas.py`
  - **test_01_ayuda_por_rol:** usuarios de prueba Jefe MAST, Dirección, Auditor, RH, dueño, operador → `_sgi_kb_help_action()` apunta al artículo esperado.
  - **test_02_menus:** `menu_sgi_kb_help` cuelga de `quimibond_sgi.menu_sgi_panel` y lo ve un usuario SGI (`_visible_menu_ids`); `menu_sgi_kb_import` bajo `menu_sgi_transition`, no lo ve un usuario SGI.
  - **test_03_vistas:** `get_views` de la tarjeta y la lista de Mi procedimiento con el botón `action_mp_article`; la ficha del documento con `sgi_article_id`.
- [ ] `test_kb_usted.py`: importa `TUTEO`, `GLOSARIO`, `SUSTANTIVOS`, `GLOSARIO_EXCEPCIONES`, `QUOTED` de `odoo.addons.quimibond_sgi.tests.test_usted` y recorre `.py` y `.xml` del satélite (misma lógica que `_offending`, l.47-71, con la raíz del satélite) **y** los `data/manuales/*.html` (texto plano).
- [ ] `test_instruction_knowledge.py::test_05` sin cambios: debe seguir pasando.
- [ ] Push; en el build fallan solo las nuevas.

### Task K2: Modelos
- [ ] `models/sgi_knowledge_article.py`: campos de 3.1, `SGI_KB_MANUALS`, `_sgi_kb_seed`, `_sgi_kb_sync_members`, `_sgi_kb_seed_one(key, name, parent, html)`, `_sgi_kb_plain`, `_sgi_kb_manual_html(file)` (lee con `file_open('quimibond_sgi_knowledge/data/manuales/%s')`), `_sgi_kb_resolve_links(html)`, `_sgi_article_hash`, `_compute_sgi_document_id`, `_compute_sgi_publish_state`, `_sgi_kb_help_action`, `action_sgi_request_publish`, `action_sgi_publish`, `_cron_sgi_kb_daily` (miembros + avisos `kb_cambio`).
- [ ] `models/sgi_instruction_knowledge.py`: `_sgi_article_hash` de la actividad delega en el artículo; stale con `sgi_content_hash` (N3); `help` de `sgi_article_id`; asistente generalizado (3.5). Mantener `action_publish_instruction` con `default_activity_id` (y ahora `default_article_id`).
- [ ] `models/sgi_knowledge_process.py`: `sgi.process.sgi_article_id` + `action_sgi_open_article`; `sgi.activity.role.mp_article_id` + `action_mp_article`.
- [ ] `models/sgi_knowledge_import.py`: asistente de 3.4, `_sgi_pdf_text`, `_sgi_kb_import_one`.
- [ ] `models/__init__.py`: el artículo **antes** del asistente de publicación (el asistente lo usa); imports locales dentro de funciones (regla de imports entre módulos de `CLAUDE.md`).
- [ ] Todo campo nuevo con `help` en usted (lo pide `sgi_docs.py --check` como aviso).

### Task K3: Datos, vistas y menús
- [ ] `tools/sgi_knowledge_html.py` (3.3) y `data/manuales/*.html` generados; `docs/sgi/glosario.md` (Jefe MAST, MAST, SGI, MIID, NC, CoA, CO, IT, PROT, MP, acuse, revisión, vigente/obsoleto/piloto, dueño de proceso, actividad, entregable, «Del Dropbox a Odoo»; definiciones de una línea en usted, tomadas de `addons/quimibond_sgi/README.md` y los manuales) y `docs/sgi/primeros-pasos.md` (entrar, Mis pendientes, Mi procedimiento y firmar, Reportar, Ayuda; ≤ 60 líneas).
- [ ] `data/sgi_knowledge_data.xml`: acción de servidor «Ayuda», cron diario (03:40, `model_knowledge_article`, `_cron_sgi_kb_daily`), `<function model="knowledge.article" name="_sgi_kb_seed"/>`.
- [ ] `views/sgi_knowledge_views.xml`: lista y búsqueda propias de `knowledge.article`, acción «Conocimiento del SGI» con `help` de 3.7, formulario del importador (`<footer>` con «Importar» y «Cancelar»), formulario del asistente de publicación ampliado (en su `<record>` existente, `views/sgi_instruction_knowledge_views.xml:20-41`: agrega `doc_type`, `current_id` de solo lectura y el aviso «Sin puestos»), herencias de la ficha del documento, del proceso y de la tarjeta/lista de Mi procedimiento (anclas en 3.6).
- [ ] `views/sgi_knowledge_menus.xml`: `menu_sgi_kb_help` (Inicio, 90), `menu_sgi_kb_articles` (Sistema, 95, grupos de 3.7), `menu_sgi_kb_import` (Transición, 20, `group_sgi_manager`). Nombres en español, sin «-» en `groups`.
- [ ] `security/ir.model.access.csv`: `sgi.knowledge.import` solo Jefe MAST (1,1,1,1). El asistente de publicación ya está.
- [ ] Manifest: `version` `19.0.1.2.0`, `data` en el orden de 3.9; `description` con un párrafo de esta entrega.

### Task K4: CI
- [ ] `.github/workflows/ci.yml`: paso «Manuales del SGI en Conocimiento» con `python3 tools/sgi_knowledge_html.py --check` junto al de `sgi_docs.py`.

### Task K5: Migración (sección 5)
- [ ] `addons/quimibond_sgi_knowledge/migrations/19.0.1.2.0/post-migrate.py`.

### Task K6: Documentación
- [ ] `addons/quimibond_sgi_knowledge/README.md`: «1.2.0 (2026-10-…)»: espacio, siembra (opción A), importador, publicación de los 4 tipos, Ayuda, N1–N6 con lo que conteste el CEO, cifras de producción de 1.2.
- [ ] `docs/sgi/README.md`: «Los manuales también están en Conocimiento → SGI → Cómo usar el sistema (SGI → Inicio → Ayuda)»; `docs/sgi/usuarios/mast.md` y `jefe-de-area.md`: sección corta «Instructivos en Conocimiento» (redactar, pedir publicación, publicar) — al cambiar, regenerar los `.html`.
- [ ] `docs/audit/decisiones.md`: «2026-10-06 — Documentación del SGI en Conocimiento (quimibond_sgi_knowledge 1.2.0): decisiones 1–6 y N1–N6».
- [ ] `python3 tools/sgi_docs.py` y commit de `docs/sgi/tecnica/` (el script lee satélites).

### Task K7: Revisión y build
- [ ] Checadores de la regla 8; `flake8` limpio.
- [ ] Push; build de la rama con `--test-tags /quimibond_sgi_knowledge,/quimibond_sgi`; leer `update.log` (sin «no puede ser localizado», sin `WARNING` de roles de accesibilidad, sin `ERROR` de la siembra) y las pruebas.
- [ ] En el build (copia de producción), a mano: abrir Conocimiento → «SGI»; ver el espacio 2025 renombrado; correr el importador con «CO y C4» y revisar 3 artículos contra su PDF (calidad del texto: sección 8).

---

## 5. Migración (`migrations/19.0.1.2.0/post-migrate.py`)

Corre después de cargar los datos del módulo (y por tanto después de `_sgi_kb_seed`). Superusuario. Idempotente:
1. `env['knowledge.article']._sgi_kb_seed()` otra vez (no hace nada si ya corrió; cubre una base donde el `<function>` falló por un dato raro: el `<function>` envuelve cada artículo en `savepoint` y registra `WARNING`).
2. `_sgi_kb_sync_members()`.
3. Aviso al Jefe MAST (`sgi.cron._sgi_schedule(raiz, "Documentación del SGI en Conocimiento", nota, mast_user_id, key='kb_conocimiento_120')`): «Se creó el espacio «SGI» en Conocimiento con los manuales por rol. Para pasar los instructivos, controles operacionales, protocolos y reglamentos vigentes: SGI → Administración → Transición → Importar documentos a Conocimiento (empiece con «CO y C4»). El espacio de 2025 se renombró a «SGI (estructura 2025, sin uso)»: decida si lo archiva.» Una sola vez (la clave).
4. `_logger.info` con: artículos creados, espacio viejo renombrado (sí/no), miembros agregados.
**No importa documentos** (lo decide el Jefe MAST con el asistente) y no toca `documents.document` ni `sgi.process.activity`.

Lo que crea en producción: 1 raíz «SGI», 2 carpetas, 14 artículos de proceso, 9 manuales (26 artículos), miembros `write` (2 Jefes MAST + dueños de proceso con usuario), 1 cron, 3 menús, 1 acción «Ayuda», 1 actividad para el Jefe MAST, 1 renombre del id 102.

---

## 6. Pruebas (resumen)

| Archivo | Qué cubre |
|---|---|
| `tests/test_kb_seed.py` | espacio, N1, manuales, opción A, archivados |
| `tests/test_kb_import.py` | importador: liga, idempotencia, no toca el documento, permisos N4, restringidos, L-001 |
| `tests/test_kb_hashes.py` | **Mi procedimiento y MIID sin cambio**; publicar un CO sí cambia `controls` solo en la revisión |
| `tests/test_kb_publish.py` | publicación de los 4 tipos, copia de metadatos, puestos, re-apuntado, solo MAST, stale |
| `tests/test_kb_ligas.py` | Ayuda por rol, menús y su visibilidad, botones en vistas |
| `tests/test_kb_usted.py` | usted y glosario en el satélite y en los manuales |
| `tests/test_instruction_knowledge.py` | sin cambio (DOC-5 de siempre) |
| Núcleo `--test-tags /quimibond_sgi` | `test_menu_tree` (satélites excluidos), `test_menus_capitulos`, `test_my_procedure`, `test_miid`, `test_usted`: deben seguir verdes |

---

## 7. Verificación en producción (MCP, solo lectura, compañía 1)

Antes (línea base):
1. `ir.module.module` `quimibond_sgi_knowledge` → `19.0.1.1.0`.
2. `aggregate_records('knowledge.article', ['category','internal_permission'], [['active','in',[True,False]]])` → 226.
3. Huellas de Mi procedimiento: `search_records('documents.document', [['sgi_code','=like','MP-%'],['sgi_state','=','vigente']], ['sgi_code','sgi_content_hash'], limit=100)` (95) y `search_records('hr.job', [['sgi_mp_hash_current','!=',False]], ['id','sgi_mp_hash_current'], limit=100)`: guardar cuántos puestos están desactualizados.
4. MIID: `search_records('sgi.miid', [['company_id','=',1]], ['state'])`.

Después del `odoo-update quimibond_sgi_knowledge`:
1. Versión `19.0.1.2.0`.
2. `search_records('knowledge.article', [['sgi_seed_key','!=',False]], ['name','sgi_seed_key','parent_id','internal_permission','category'], limit=50)` → 26 (raíz `read`/`workspace`).
3. `get_record('knowledge.article', 102, ['name','internal_permission','active'])` → «SGI (estructura 2025, sin uso)», `write`, activo.
4. `search_records('knowledge.article.member', [['article_id.sgi_seed_key','=','raiz']], ['partner_id','permission'])` → los 2 Jefes MAST `write` (no anotar nombres en documentos).
5. `search_records('sgi.process', [['company_id','=',1],['sgi_article_id','=',False]], ['code'])` → 0.
6. Repetir la línea base 3 → **mismo número** de puestos desactualizados; ningún `MP-*` con revisión nueva.
7. `search_records('documents.document', [['sgi_article_id','!=',False]], ['sgi_code'])` → 0 (nada se importa solo).
8. `mail.activity` del aviso `kb_conocimiento_120` → 1.
9. Tras la primera corrida del importador (la hace el Jefe MAST): `search_records('knowledge.article', [['sgi_kind','=','documento']], ['name','sgi_document_code','sgi_publish_state','sgi_import_note'], limit=100)` → 22 con «CO y C4»; `search_records('sgi.process.activity', [['instruction_article_id','!=',False]], ['number'])` → 6 (C4.05, C4.08, C4.09, C4.12, C4.14, E2.24); documentos de los 22 con `sgi_revision`, `sgi_state` iguales a la línea base (lista de 1.2).
10. A mano: un usuario SGI (no dueño) entra a SGI → Inicio → Ayuda (abre su manual) y **no** ve los borradores importados en Conocimiento.

---

## 8. Riesgos

| Riesgo | Mitigación |
|---|---|
| **Huella de Mi procedimiento cambia** (95 puestos a firmar) | el importador nunca escribe `instruction_id`; `mp_article_id` es campo de pantalla; `test_kb_hashes::test_01`; verificación 7.6 antes de que el Jefe MAST publique cualquier MP |
| **Huella del MIID cambia** sin revisión nueva | el snapshot no lee artículos ni `sgi_article_id` (1.3); `test_kb_hashes::test_02`; publicar un CO cambia `controls` a propósito y conserva título y clave anterior (`test_03`) |
| **Conocimiento expone un documento restringido** | solo se importa lo que tiene `access_internal='view'` (lo que ya lee todo interno); familia P-I01 fuera siempre (N6, 6 documentos); borradores solo para dueño y Jefe MAST (N4); el importador se prueba con un documento `none` |
| Usuarios internos que no son del SGI leen el espacio (Knowledge no filtra por grupo) | mismo alcance que los controlados vigentes; 31 de 38 internos ya son Usuario SGI; si el CEO lo quiere más cerrado: raíz `none` + miembros por grupo con el cron (más mantenimiento) |
| **Calidad del texto extraído** (tablas desordenadas, columnas mezcladas, sin imágenes, encabezados y pies repetidos, PDF escaneado sin texto) | aviso al inicio de cada borrador; PDF adjunto para comparar; el dueño revisa antes de pedir publicación; «sin texto» queda en `sgi_import_note` y en el resumen; nada se publica solo |
| PDF grande o lento de leer (hasta 4.6 MB importable) | `index_content` ya extraído al subir (sin costo); lote de 10; tope de 40 páginas / 8 MB; `savepoint` por documento |
| El sanitizador o el editor de Knowledge reescribe el HTML y la siembra cree que alguien editó | huella del **texto plano** guardada **después** de escribir (3.2); `test_kb_seed::test_04` (VERIFICAR V3 en el build abriendo y cerrando un manual) |
| Knowledge «archiva» mandando a la papelera (se borra a los N días) | el código nunca usa papelera ni `action_archive` de Knowledge; N1 renombra en vez de archivar; README advierte al Jefe MAST |
| Publicar un CO/PROT/REG sin pasar por la categoría 12 (menos evidencia que Sign) | N5 explícito para el CEO; chatter con redactó/publicó; el artículo guarda su historial (`html_field_history`) |
| Revisión nueva re-apunta actividades y cambia «documentos» de Mi procedimiento | es el efecto correcto de una revisión (releer y firmar); se dice en el asistente: «Los puestos que usan este documento verán su Mi procedimiento desactualizado» |
| Herencias del satélite sobre vistas del núcleo se rompen si el núcleo cambia esas vistas | anclas de campo estables; el build de la rama corre ambos módulos; regla 3 |
| Otra sesión toca `quimibond_sgi_knowledge` o el núcleo en paralelo | renumerar al empezar (estado arriba) |
| Conflicto de nombre con el espacio 2025 | N1 lo renombra antes de crear el nuevo |

---

## 9. VERIFICAR (no se pudo comprobar sin Odoo 19 en el contenedor)

- **V1** — Que Odoo 19 no traiga convertidor de Markdown del lado del servidor (si `odoo.tools` lo trae, N2 sigue igual: el HTML comprometido evita depender de él).
- **V2** — Modelo de permisos de Knowledge 19: restricción «al menos un miembro con escritura» cuando `internal_permission != 'write'`; cómo desincronizar un hijo (`_desync_access_from_parents` / `is_desynchronized`) y agregar miembros (`_add_members(partners, permission)` o crear `knowledge.article.member` con sudo). Campos confirmados por MCP: `internal_permission` (`write`/`read`/`none`), `article_member_ids` (`partner_id`, `permission`), `category` (`workspace`/`private`/`shared`, de solo lectura), `is_desynchronized`, `is_locked`, `is_article_visible_by_everyone`, `body` (html), `parent_id`, `root_article_id`, `html_field_history`, `to_delete`, `article_url`.
- **V3** — Que abrir un artículo en el editor de Knowledge sin cambiarlo no reescriba `body`.
- **V4** — URL interna de un artículo en Odoo 19 (`/knowledge/article/<id>` u `/odoo/knowledge/<id>`) y la acción para abrirlo desde una acción de servidor (`action_home_page` con `res_id`, o `act_url`). Si `is_article_visible_by_everyone` debe ir en `True` en la raíz para que salga en la barra lateral de todos.
- **V5** — Que `ir.attachment.index_content` de los PDF del SGI esté lleno (`attachment_indexation` instalado; el modelo no está expuesto al MCP).
- **V6** — Nombre exportado por `odoo.tools.pdf` en Odoo 19 (`PdfReader` / `PdfFileReader`) y `extract_text()`.
- **V7** — Que las anclas elegidas sigan siendo únicas al implementar (`grep -c` dentro del `<record>`): `sgi_owner_id` en `sgi_document_view_form` hoy aparece 1 vez; `sgi_code` 2+ veces.
- **V8** — Que `<menuitem>` de un satélite bajo `quimibond_sgi.menu_sgi_panel` no altere `test_interfaz::test_11-14` ni `test_menus_capitulos::test_05` (listas sí/no explícitas: no deberían).
