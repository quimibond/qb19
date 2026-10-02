# Auditoría SGI 2026-10 — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Llevar a código, datos y despliegue los hallazgos de la auditoría del SGI del 1-oct-2026 (reporte: https://claude.ai/artifact/9Q1KZp3yg9SaZ4wj7cHWn5), en entregas que se despliegan solas.

**Architecture:** Diez versiones de `quimibond_sgi` (57.91.0 a 57.100.0; la numeración se corrió uno porque `main` usó 57.90.0 para los indicadores de ventas, PR #507), cada una con su sección de CHANGELOG, sus pruebas registradas y, si hace falta, su migración, más una pista de datos que operan MAST y Dirección en producción sin código. Este documento trae el paso a paso con código de las dos primeras entregas (57.91.0 candados de evidencia, 57.92.0 bandeja). Las demás quedan como fichas con alcance, archivos, pruebas y puerta de decisión. Su plan detallado se escribe al iniciarlas, porque dependen de decisiones de Dirección que siguen abiertas.

**Tech Stack:** Odoo 19 Enterprise (Odoo.sh), Python 3, XML de vistas, `odoo.tests.TransactionCase`. Checadores del repo: `tools/check_addons.py`, `tools/check_odoo_views.py`, `tools/sgi_docs.py`.

---

## 0. Reglas que aplican a todas las entregas

Leer antes de empezar: `CLAUDE.md` (raíz), `addons/quimibond_sgi/README.md` (glosario y «usted»), la sección más reciente de `addons/quimibond_sgi/CHANGELOG.md`.

1. **Versión y CHANGELOG:** cada entrega sube `addons/quimibond_sgi/__manifest__.py` `'version'` y agrega `## 19.0.57.NN.0 — AAAA-MM-DD` arriba en `addons/quimibond_sgi/CHANGELOG.md`, con las secciones Agregado / Cambiado / Corregido / Seguridad / Migración / Pruebas que apliquen. `tools/check_addons.py --base-ref origin/main` lo exige.
2. **Pruebas registradas:** todo `tests/test_*.py` nuevo va en `addons/quimibond_sgi/tests/__init__.py` (el checker lo exige). Clases con `@tagged('post_install', '-at_install')`.
3. **Dónde corren las pruebas:** el CI de GitHub **no** instala el SGI (Enterprise) y no hay Odoo local. Las pruebas del SGI corren en el build de desarrollo de Odoo.sh de la rama con `--test-tags /quimibond_sgi` (no el suite completo: las pruebas de nómina lo detienen). El ciclo TDD es: escribir prueba y código → checadores locales → push a la rama → leer el log de pruebas del build. **Antes del código**, hacer un push con solo la prueba nueva cuando el fallo esperado no sea obvio, para verla fallar en el build.
4. **Checadores locales** (desde la raíz del repo, deben salir en 0 errores):
   ```bash
   pip install lxml flake8   # una vez
   python3 tools/check_addons.py --base-ref origin/main
   python3 tools/check_odoo_views.py --base-ref origin/main
   python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
   flake8 addons/quimibond_sgi
   ```
5. **Sin herencias propias** de vistas del SGI (regla 2 de CLAUDE.md): las vistas propias se editan en su `<record>`.
6. **Menús** solo en `views/sgi_menus.xml` y su árbol en `tools/sgi_menu_tree.txt` (`tests/test_menu_tree.py` lo compara).
7. **Textos para usuarios en «usted»** y con el glosario del README (Jefe MAST, NC, CoA, no conformidad).
8. **Despliegue:** PR rama → `main` (staging), revisar el build; luego PR `main` → `quimibond` y `odoo-update quimibond_sgi` según `docs/RUNBOOK_DESPLIEGUE.md`. Verificar con las consultas de cada entrega (solo lectura).
9. **Producción es de solo lectura para quien desarrolla.** Ninguna migración cambia datos de negocio sin el visto bueno escrito de Jose en el PR.

---

## 1. Mapa de entregas

| Versión | Entrega | Hallazgos | Depende de | Puerta de decisión |
|---|---|---|---|---|
| 57.91.0 | Candados de evidencia | K-01, K-02, K-06, K-07, FUNC-C13 | — | Ninguna |
| 57.92.0 | Bandeja: Mis pendientes útil | U-02, U-03, U-05, U-06 | — | Ninguna (D-05 se decide en la pista de datos) |
| 57.93.0 | NC y auditoría con evidencia | N-02, N-03, N-12, K-03 | 57.91.0 | Ninguna |
| 57.94.0 | SGI en planta (kiosco con PIN) | U-01, U-08, I-03, I-05 | 57.92.0 | Q7: tabletas y captura de PIN por RH |
| 57.95.0 → **entregada como 57.96.0** (2026-10-02) | SST y ambiente | N-06, N-07 | 57.93.0 | Q9 (MOC), Q10 (matriz ambiental), Q11 (contratistas) |
| 57.96.0 → **entregada como 57.97.0** (2026-10-02) | Cláusulas y revisión por la dirección | N-05, N-09 | — | Q13 (requisitos de cliente) |
| 57.97.0 → 57.98.0 o siguiente libre | Interfaz | I-01, I-02, I-04, I-06, U-07 | 57.92.0 | Q20 (arranque de Dirección) |
| 57.98.0 | Salud del SGI (tablero de adopción) | Sección 8 del reporte, D-01 | 57.93.0, 57.94.0 | Metas del tablero |
| 57.99.0 → **entregada como 57.95.0** (2026-10-02) | Rendimiento y robustez | K-08, K-05, D-06 | 57.92.0 | Ninguna |
| 57.100.0 | Integridad, competencias, PPAP, IA | K-04, N-13, N-14, D7 | 57.96.0 | Q12 (PPAP), Q16 (IA) |

Los IDs (K-01, U-02, N-02…) y las preguntas (Q1…Q20) son los del reporte de auditoría.

**Renumeración (2026-10-02):** «Rendimiento y robustez» (ficha 57.99.0) salió
como **19.0.57.95.0** porque no tiene puerta de decisión
(`docs/superpowers/plans/2026-10-02-sgi-57-95-0-rendimiento-robustez.md`). Las
fichas con puerta (SST y ambiente, Cláusulas, Interfaz, Salud del SGI y las
siguientes) toman el siguiente número libre (57.96.0 en adelante) cuando se
inicien; sus títulos de abajo conservan el número original del mapa. «SST y
ambiente» (ficha 57.95.0) salió como **19.0.57.96.0**
(`docs/superpowers/plans/2026-10-02-sgi-57-96-0-sst-ambiente.md`); «Cláusulas y
revisión por la dirección» (ficha 57.96.0) salió como **19.0.57.97.0**
(`docs/superpowers/plans/2026-10-02-sgi-57-97-0-clausulas-revision.md`);
«Interfaz» (ficha 57.97.0) toma 57.98.0 o el siguiente libre.

---

## 2. Pista de datos (sin código, en producción)

La operan las personas indicadas desde la interfaz de Odoo. Quien desarrolla solo verifica con consultas de lectura. Marcar cada casilla cuando se confirme con la consulta.

### Semana 1 (antes del 9-oct)

- [ ] **Auditores de octubre** (Jose, MAST): asignar el grupo «Auditor SGI» a quienes auditarán y un auditor líder a cada una de las 5 líneas de octubre del programa 2026.
  Verificar: `sgi.audit.program.line` `[('planned_month','=','10'), ('lead_auditor_id','=',False)]` → 0 (el campo de mes puede llamarse distinto; confirmar con `get_fields`).
- [ ] **Grupos** (Jose): confirmar o quitar al uid 130 de Jefe MAST; asignar miembros a Salud ocupacional y a la CSH; sacar a los uid 177 y 179 (inactivos) de Usuario SGI.
  Verificar: `res.groups` 318 `user_ids`; 328 y 330 con miembros; `res.users [('active','=',False),('group_ids','in',[316])]` → 0.
- [ ] **Mediciones históricas** (decisión de Jose, Q4): elegir entre (a) MAST valida en lote las capturadas de enero a julio con la nota «histórico, validado en lote el AAAA-MM-DD por decisión de Dirección», o (b) dejarlas y que 57.92.0 las saque de la bandeja por antigüedad. Registrar la decisión en `docs/audit/decisiones.md` (ver la tarea 1.0).
- [ ] **Puntos de control archivados** (Jose y MAST, Q5): decidir si se restauran los 34 `quality.point` (ids 175–213) o si los 73 formatos regresan a «en curso».
  Verificar: `documents.document [('sgi_is_controlled','=',True),('sgi_migration_state','=','migrado'),('sgi_migration_point_id.active','=',False)]` → 0.
- [ ] **NC canceladas** (MAST): capturar el motivo en las 14 NC canceladas sin motivo y dejar una nota en el chatter de la secuencia NCI sobre el hueco 0147–0205 (Q14).
  Verificar: `quality.alert [('company_id','=',1),('stage_id.sgi_is_cancel_stage','=',True),('sgi_cancel_reason','=',False)]` → 0.

### Semana 2

- [ ] **SLA de reclamaciones** (Calidad): SLA en los equipos de Helpdesk 2 y 14.
- [ ] **Proveedores** (Compras): `action_recompute` de la evaluación y estado «nuevo» en los proveedores de categorías críticas.
- [ ] **Calibración** (MAST, Q6): cargar las fechas reales de la última calibración de los 143 equipos o confirmar que están vencidos.
- [ ] **Ola 1 de vigencia** (Jose, Q1): fecha para C5, C4 y E2 en vigente; sus indicadores críticos a «oficial» con `nc_on_red`.
- [ ] **Rutinas del Dropbox** (MAST): repartir entre los dueños de proceso las 37 rutinas pendientes con plazo en octubre.

---

## Entrega 57.91.0 — Candados de evidencia

**Objetivo:** que ni un documento controlado ni una NC con folio se pierdan por error, que solo MAST o el dueño del proceso cierren una NC, que ningún proceso pesado se dispare por RPC y que el texto del proveedor no entre como HTML.

**Archivos:**
- Modificar: `addons/quimibond_sgi/models/sgi_document.py` (`DocumentsDocument.write` ~l.1123, nuevo `unlink`; `SgiDocumentAck.document_id` l.1268)
- Modificar: `addons/quimibond_sgi/models/sgi_nonconformity.py` (imports; `_sgi_check_stage_move` ~l.431; `QualityAlert.unlink` nuevo; `SgiActionLine.alert_id` l.764; `message_post` de l.1062, 1100, 1117)
- Modificar: `addons/quimibond_sgi/models/sgi_supplier_nc.py` (l.115-117)
- Modificar: `addons/quimibond_sgi/models/sgi_approval_native.py` (`cron_sgi_sync_approvals` ~l.472)
- Modificar: `addons/quimibond_sgi/models/sgi_checklist.py` (`cron_generate` ~l.130)
- Modificar: `addons/quimibond_sgi/models/sgi_process_procedure.py` (`cron_measure_activities` ~l.1345)
- Modificar: `addons/quimibond_sgi/models/sgi_indicator_trajectory.py` (`cron_missing_trajectories` ~l.152)
- Modificar: `addons/quimibond_sgi/models/sgi_catalog.py` (`sgi_drop_empty_studio_models` ~l.868)
- Crear: `addons/quimibond_sgi/tests/test_candados_evidencia.py`
- Modificar: `addons/quimibond_sgi/tests/__init__.py`, `addons/quimibond_sgi/tests/test_entrega1c.py`
- Modificar: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`
- Crear: `docs/audit/decisiones.md`

### Task 1.0: Rama y registro de decisiones

**Files:**
- Create: `docs/audit/decisiones.md`

- [x] **Step 1: Rama.** Se trabaja en la rama asignada a la sesión (`claude/confident-mendel-8yg7xu`, ya basada en `main`); si se trabaja fuera de esa sesión: `git fetch origin main && git checkout -B claude/sgi-57-90-candados origin/main`.

- [x] **Step 2: Crear el índice de decisiones** que `CLAUDE.md:53`, `addons/quimibond_sgi/README.md:149` y `qb_mcp_politica/README.md:6` citan y no existe

```markdown
# Decisiones de Jose sobre el SGI

Índice de las decisiones citadas en el código y el CHANGELOG como D-xx. La
fuente de cada una es la entrada del CHANGELOG indicada; aquí solo se listan
para encontrarlas. Las nuevas se agregan arriba con fecha.

El CHANGELOG mezcla dos series que son decisiones distintas: `D-xx` (dos dígitos, decisiones de Jose de la auditoría de septiembre) y `D-0xx` (tres dígitos, hallazgos de diseño de la auditoría 2026-09). La tabla las separa.

| Serie | Clave | Decisión (resumen) | Primera entrada del CHANGELOG |
|---|---|---|---|
| D-xx | D-03 | El SGI es solo de la compañía 1 | (llenar con el grep) |
| D-xx | D-04 | Mis pendientes con todo adentro (convive con las actividades nativas) | `models/sgi_my_pending.py:11` (no está en el CHANGELOG) |
```

Llenar una fila por clave con
`grep -rnoE "D-[0-9]{2,3}\b" addons/quimibond_sgi/CHANGELOG.md addons/quimibond_sgi/models addons/quimibond_sgi/README.md | awk -F: '!seen[$3]++'`
(algunas claves, como D-04, solo aparecen en el código)
(la primera línea donde aparece cada clave); el resumen sale del texto de esa entrada. No inventar decisiones: si una clave no tiene texto claro, dejar «ver CHANGELOG línea N».

- [x] **Step 3: Commit**

```bash
git add docs/audit/decisiones.md
git commit -m "docs: índice de decisiones D-xx del SGI (la referencia de CLAUDE.md no existía)"
```

### Task 1.1: Pruebas de los candados (fallan antes del código)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_candados_evidencia.py`
- Modify: `addons/quimibond_sgi/tests/__init__.py` (agregar al final)

- [x] **Step 1: Escribir las pruebas**

```python
# -*- coding: utf-8 -*-
"""57.91.0 (auditoría 2026-10, K-01, K-02, K-06, K-07, FUNC-C13): la evidencia
no se pierde ni se altera por error.

- Un documento controlado no va a la papelera ni se borra (Odoo lo borra a
  los 30 días y la cascada se llevaba sus acuses).
- Una NC con folio no se borra; sus acciones no se van en cascada.
- Solo el Jefe MAST o el dueño del proceso cierran una NC.
- Los procesos pesados no se disparan por RPC.
- La respuesta del proveedor se guarda escapada en el chatter."""
from datetime import date

from psycopg2 import IntegrityError

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools import mute_logger

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestCandadosEvidencia(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        sgi_hide_real_documents(env)
        # Quien puede editar en Documentos sin ser Jefe MAST: el camino real
        # por el que 3359, 5119 y 4995 terminaron en la papelera.
        cls.docs_editor = new_test_user(
            env, login='k01_docs_editor',
            groups='base.group_user,quimibond_sgi.group_sgi_user,documents.group_documents_manager')
        cls.mast = new_test_user(
            env, login='k01_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.sgi_user = new_test_user(
            env, login='k01_sgi_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner_user = new_test_user(
            env, login='k01_owner', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = env['hr.employee'].create({'name': 'K01 Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create({
            'code': 'ZK1', 'name': 'Proceso candados', 'owner_id': cls.owner.id})
        cls.team_int = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.employee = env['hr.employee'].create({'name': 'K01 Lector'})

    def _doc(self, state='vigente', code='F-ZK1-01'):
        return self.env['documents.document'].create({
            'name': 'K01 %s' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': code, 'sgi_state': state,
            # «formato» exige proceso con clave nueva (_check_sgi_code).
            'sgi_process_id': self.process.id})

    def _closable_nc(self):
        # La segunda NC del mismo proceso sale reincidente y pide una acción
        # CORRECTIVA terminada (_sgi_check_can_close): se registra correctiva.
        alert = self.env['quality.alert'].create({
            'title': 'K01 NC', 'team_id': self.team_int.id,
            'sgi_process_id': self.process.id})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Corregir K01', 'responsible_id': self.env.user.id,
            'action_type': 'correctiva',
            'date_commit': date.today(), 'date_done': date.today(), 'progress': '100'})
        alert.write({'sgi_root_cause': 'Causa K01', 'sgi_effectiveness_note': 'Eficaz',
                     'sgi_effectiveness_date': date.today()})
        return alert

    def _assert_restricted(self, record):
        # Mismo patrón que test_integridad: «restrict» lo frena la base
        # (IntegrityError) o el ORM (UserError); cualquiera vale.
        raised = None
        try:
            with mute_logger('odoo.sql_db'), self.env.cr.savepoint():
                record.unlink()
                self.env.flush_all()
        except (IntegrityError, UserError) as exc:
            raised = exc
        self.assertIsNotNone(raised, "%s se borró estando en uso." % record.display_name)
        self.env.invalidate_all()
        self.assertTrue(record.exists())

    # ---- K-01 ---------------------------------------------------------------
    def test_01_documento_controlado_no_va_a_la_papelera(self):
        doc = self._doc()
        with self.assertRaises(UserError):
            doc.with_user(self.docs_editor).write({'active': False})
        with self.assertRaises(UserError):
            doc.with_user(self.docs_editor).action_archive()
        self.assertTrue(doc.active)

    def test_02_mast_si_archiva_y_el_borrador_no_tiene_candado(self):
        doc = self._doc(code='F-ZK1-02')
        doc.with_user(self.mast).write({'active': False})
        self.assertFalse(doc.active)
        draft = self._doc(state='borrador', code='F-ZK1-03')
        draft.with_user(self.docs_editor).write({'active': False})
        self.assertFalse(draft.active)

    def test_03_documento_controlado_no_se_borra(self):
        doc = self._doc(code='F-ZK1-04')
        with self.assertRaises(UserError):
            doc.with_user(self.docs_editor).unlink()

    def test_04_el_acuse_detiene_el_borrado_fisico(self):
        doc = self._doc(code='F-ZK1-05')
        self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.employee.id})
        # La autolimpieza de la papelera corre como superusuario: lo que la
        # detiene es la llave foránea, no el candado de Python. Se archiva
        # primero para seguir el camino real (papelera → borrado).
        doc.sudo().write({'active': False})
        self._assert_restricted(doc.sudo())

    # ---- K-02 ---------------------------------------------------------------
    def test_05_nc_con_folio_no_se_borra(self):
        alert = self.env['quality.alert'].create({'title': 'K02 NC', 'team_id': self.team_int.id})
        self.assertTrue(alert.sgi_folio)
        with self.assertRaises(UserError):
            alert.with_user(self.sgi_user).unlink()

    def test_06_accion_detiene_el_borrado_de_su_nc(self):
        alert = self.env['quality.alert'].create({'title': 'K02 NC 2', 'team_id': self.team_int.id})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Acción K02', 'responsible_id': self.env.user.id,
            'date_commit': date.today()})
        self._assert_restricted(alert.sudo())

    # ---- FUNC-C13 -----------------------------------------------------------
    def test_07_usuario_sgi_no_cierra_una_nc(self):
        alert = self._closable_nc()
        with self.assertRaises(UserError):
            alert.with_user(self.sgi_user).write({'stage_id': self.stage_closed.id})

    def test_08_dueno_del_proceso_y_mast_si_cierran(self):
        alert = self._closable_nc()
        alert.with_user(self.owner_user).write({'stage_id': self.stage_closed.id})
        self.assertEqual(alert.stage_id, self.stage_closed)
        other = self._closable_nc()
        other.with_user(self.mast).write({'stage_id': self.stage_closed.id})
        self.assertEqual(other.stage_id, self.stage_closed)

    # ---- K-06 ---------------------------------------------------------------
    def test_09_procesos_pesados_solo_del_sistema(self):
        calls = [
            lambda: self.env['sgi.activity.role'].with_user(self.mast).cron_sgi_sync_approvals(),
            lambda: self.env['sgi.checklist.template'].with_user(self.mast).cron_generate(),
            lambda: self.env['sgi.process.activity'].with_user(self.mast).cron_measure_activities(),
            lambda: self.env['sgi.indicator'].with_user(self.mast).cron_missing_trajectories(),
        ]
        for call in calls:
            with self.assertRaises(AccessError):
                call()

    # ---- K-07 ---------------------------------------------------------------
    def test_10_respuesta_del_proveedor_escapada(self):
        alert = self.env['quality.alert'].create({'title': 'K07 NC', 'team_id': self.team_int.id})
        alert.sudo().write({'sgi_supplier_state': 'enviada'})
        alert._sgi_supplier_answer('<script>x</script>Causa', '<b>Acción</b>')
        body = "".join(alert.message_ids.mapped('body'))
        self.assertIn('&lt;script&gt;', body)
        self.assertNotIn('<script>', body)
        self.assertIn('<b>Respuesta del proveedor</b>', body, "El formato propio sí se conserva.")
```

En `tests/__init__.py`, al final:

```python
from . import test_candados_evidencia
```

- [x] **Step 2: Revisar contra el código real antes de empujar**

Confirmar que existen los nombres usados: `documents.group_documents_manager` (en el shell de Odoo.sh: `grep -n "group_documents_manager" /home/odoo/src/enterprise/documents/security/*.xml`; si el nombre cambió en Odoo 19, usar el de ese archivo), `sgi_quality_team_internal`, `sgi_nc_int_stage_closed`, `_sgi_supplier_answer(cause, action)` (`models/sgi_supplier_nc.py`), `sgi_supplier_state`. Si `_sgi_supplier_answer` tiene otra firma, ajustar la llamada.

- [ ] **Step 3: Push solo de la prueba y verla fallar en el build de Odoo.sh**

```bash
git add addons/quimibond_sgi/tests/test_candados_evidencia.py addons/quimibond_sgi/tests/__init__.py
git commit -m "quimibond_sgi: pruebas de los candados de evidencia (fallan antes del código)"
git push -u origin claude/sgi-57-90-candados
```

Esperado en el log del build (`--test-tags /quimibond_sgi`): FAIL en test_01, 04, 06, 07, 09 y 10; test_02 y test_08 pasan. test_03 y test_05 pueden pasar ya antes del código (Documents puede negarse a borrar un documento activo y el usuario de Calidad puede no tener permiso de borrar: `AccessError` es subclase de `UserError`); si pasan, no es error, el candado los cubre igual.

### Task 1.2: K-01, documento controlado sin papelera ni borrado

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_document.py`

- [x] **Step 1: Candado al archivar, al inicio de `DocumentsDocument.write`** (antes de `self._sgi_check_transition_write(vals)`, ~l.1124)

```python
    def write(self, vals):
        # 57.91.0 (K-01): archivar manda el documento a la papelera y la
        # autolimpieza lo BORRA a los documents.deletion_delay días, con sus
        # acuses. Ya pasó con 3359, 5119 y 4995 (57.82.0).
        if 'active' in vals and not vals['active']:
            self._sgi_check_can_trash()
        self._sgi_check_transition_write(vals)
```

- [x] **Step 2: El método del candado y `unlink`**, junto a `_sgi_check_transition_write` (~l.1057)

```python
    _SGI_TRASH_LOCKED_STATES = ('vigente', 'piloto', 'obsoleto')

    def _sgi_check_can_trash(self):
        """57.91.0 (K-01): un controlado vigente, en piloto u obsoleto no va a
        la papelera ni se borra; se marca obsoleto. El Jefe MAST y el sistema
        sí pueden (limpiezas decididas)."""
        if sgi_bypass_allowed(self.env):
            return
        locked = self.filtered(lambda d: d.sgi_is_controlled
                               and d.sgi_state in self._SGI_TRASH_LOCKED_STATES)
        if locked:
            raise UserError(
                "Un documento controlado no se manda a la papelera ni se borra: "
                "Odoo lo elimina a los 30 días junto con sus acuses de lectura. "
                "Márquelo obsoleto o pida al Jefe MAST que lo retire. (%s)"
                % ", ".join(locked.mapped('display_name')))

    def unlink(self):
        self._sgi_check_can_trash()
        return super().unlink()
```

Confirmar que `sgi_bypass_allowed` y `UserError` ya están importados en `sgi_document.py` (`grep -n "^from\|^import" models/sgi_document.py`); si falta, agregar `from .sgi_base import sgi_bypass_allowed`.

Revisar si `documents.document` en Odoo 19 manda a la papelera por otro método que no pase por `write` (`action_archive` llama a `write({'active': False})`). En el shell de Odoo.sh: `grep -n "def action_archive\|def action_move\|def _move_to_trash\|active = False" /home/odoo/src/enterprise/documents/models/*.py`. Si algún camino escribe `active` por SQL, sobrescribirlo también con `_sgi_check_can_trash()` y agregar su caso a `test_01`.

- [x] **Step 3: El acuse detiene el borrado físico** (l.1268)

```python
    document_id = fields.Many2one('documents.document', string="Documento", required=True,
                                  ondelete='restrict', help="Documento que se debe leer.")
```

Antes de cambiarlo, comprobar que ningún código del SGI borra documentos con acuses: `grep -rn "unlink()" addons/quimibond_sgi*/models | grep -i doc` (en la auditoría y en la revisión del plan salió vacío). Si Mi procedimiento republica borrando el documento anterior, cambiar ese camino a obsoleto.

- [x] **Step 3b: Que la limpieza nocturna de la papelera no se atore**

Con `restrict`, si el Jefe MAST archiva un controlado con acuses, a los 30 días la autolimpieza de Documents intenta borrarlo junto con los demás de la papelera; la llave foránea abortaría el lote completo y la papelera dejaría de vaciarse. Para no depender del nombre del método de Documents en Odoo 19 (no verificable fuera de Odoo.sh), un `@api.autovacuum` propio rescata a diario esos documentos: los saca de la papelera mucho antes de los 30 días.

```python
    @api.autovacuum
    def _gc_sgi_rescue_trashed_controlled(self):
        """57.91.0 (K-01): un controlado con acuses de lectura es evidencia
        (ISO 7.5): si alguien lo manda a la papelera, se rescata como obsoleto
        antes de que la autolimpieza de Documents lo borre (30 días) y su
        llave foránea atore el vaciado de la papelera."""
        acked = self.env['sgi.document.ack'].sudo().with_context(active_test=False).search(
            [('document_id.active', '=', False)]).document_id
        rescued = acked.filtered('sgi_is_controlled')
        if not rescued:
            return
        rescued.sudo().write({'active': True, 'sgi_state': 'obsoleto'})
        for doc in rescued:
            doc.sudo().message_post(body="Rescatado de la papelera: tiene acuses de lectura "
                                         "y es evidencia del SGI. Quedó obsoleto.")
        _logger.info("SGI: %d documentos controlados rescatados de la papelera: %s",
                     len(rescued), rescued.ids)
```

Confirmar que `_logger` existe en el archivo y que el dominio `document_id.active` alcanza documentos archivados (`active_test=False` en el contexto del search del acuse).

Prueba en `test_candados_evidencia.py`:

```python
    def test_11_la_papelera_rescata_lo_que_tiene_acuses(self):
        doc = self._doc(code='F-ZK1-06')
        self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.employee.id})
        doc.with_user(self.mast).write({'active': False})
        self.env['documents.document']._gc_sgi_rescue_trashed_controlled()
        self.assertTrue(doc.active)
        self.assertEqual(doc.sgi_state, 'obsoleto')
```

- [x] **Step 4: Checadores locales** (sección 0, punto 4). Esperado: 0 errores.

- [x] **Step 5: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_document.py
git commit -m "quimibond_sgi: un documento controlado no va a la papelera ni se borra (K-01)"
```

### Task 1.3: K-02, NC con folio sin borrado

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py`

- [x] **Step 1: `unlink` en `QualityAlert`**, después de `write` (~l.668)

```python
    def unlink(self):
        """57.91.0 (K-02): una NC con folio es evidencia (ISO 10.2) y su folio
        no puede dejar hueco. Se cancela con «Cancelar NC»."""
        if not sgi_bypass_allowed(self.env):
            with_folio = self.filtered('sgi_folio')
            if with_folio:
                raise UserError(
                    "Una NC con folio no se borra: use «Cancelar NC» y capture el motivo. (%s)"
                    % ", ".join(with_folio.mapped('sgi_folio')))
        return super().unlink()
```

- [x] **Step 2: Las acciones detienen el borrado de su NC** (l.764)

```python
    alert_id = fields.Many2one('quality.alert', string="No Conformidad", ondelete='restrict',
                               help="No conformidad a la que pertenece la acción.")
```

Revisar que ninguna prueba ni código borre NC con acciones: `grep -rn "quality.alert'\].*unlink\|alert.*\.unlink()" addons/quimibond_sgi*/ | grep -v "def unlink"`. Si alguna prueba lo hace, borrar primero las acciones en esa prueba.

- [x] **Step 3: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py
git commit -m "quimibond_sgi: una NC con folio no se borra y sus acciones no se van en cascada (K-02)"
```

### Task 1.4: FUNC-C13, solo MAST o el dueño cierran una NC

**Files:**
- Modify: `addons/quimibond_sgi/models/sgi_nonconformity.py` (`_sgi_check_stage_move`, ~l.431)

- [x] **Step 1: Agregar el chequeo dentro del ciclo `for alert in self:`**, después del de cancelación

```python
            # 57.91.0 (FUNC-C13): cerrar es de quien responde por el proceso.
            if new_stage.sgi_is_closing_stage and not force \
                    and not alert._sgi_user_can_close():
                raise UserError(
                    "La NC %s solo la cierra el Jefe MAST o el dueño del proceso (%s). "
                    "Pídale que revise la eficacia y la cierre."
                    % (alert.sgi_folio, alert.sgi_process_id.owner_id.sudo().name or "sin dueño"))
```

Y el método, junto a `_sgi_check_stage_move`:

```python
    def _sgi_user_can_close(self):
        self.ensure_one()
        if sgi_bypass_allowed(self.env):
            return True
        owner_user = self.sgi_process_id.owner_id.sudo().user_id
        return bool(owner_user) and owner_user == self.env.user
```

El chequeo solo aplica a NC con folio (el ciclo ya hace `continue` sin folio). Las pruebas existentes cierran como superusuario (`test_nc_flow.test_02`), así que no cambian; verificado con `grep` en la auditoría: ninguna prueba cierra con `with_user` de un usuario sin MAST.

- [x] **Step 2: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_nonconformity.py
git commit -m "quimibond_sgi: solo el Jefe MAST o el dueño del proceso cierran una NC (FUNC-C13)"
```

### Task 1.5: K-06, procesos pesados solo del sistema

**Files:**
- Modify: `models/sgi_approval_native.py`, `models/sgi_checklist.py`, `models/sgi_process_procedure.py`, `models/sgi_indicator_trajectory.py`, `models/sgi_catalog.py`
- Modify: `tests/test_entrega1c.py` (`test_04`)

- [x] **Step 1: Guarda en la primera línea de cada método**

En cada archivo agregar el import (respetando el orden de imports; `sgi_guard` no define modelos, importarlo a nivel de módulo es seguro):

```python
from .sgi_guard import sgi_require_system
```

y como primera línea del cuerpo de `cron_sgi_sync_approvals`, `cron_generate`, `cron_measure_activities` (en `sgi_process_procedure.py`; la extensión de `sgi_activity_spec.py:1161` llama `super()` antes de hacer nada, así que queda cubierta) y `cron_missing_trajectories`:

```python
        sgi_require_system(self.env)  # 57.91.0 (K-06)
```

En `sgi_drop_empty_studio_models` (`sgi_catalog.py`), reemplazar la guarda de `group_sgi_admin` por `sgi_require_system(self.env)`.

Callers verificados en la auditoría: solo `data/*_cron.xml` (corren como OdooBot, `_is_superuser()`), `sgi_cron.py:780` (dentro del cron) y pruebas como superusuario. No hay botones que los llamen.

- [x] **Step 2: Agregar los cuatro a la prueba de F-008** en `tests/test_entrega1c.py::test_04_sgi_processes_only_for_the_system`

```python
        for model, method in (('sgi.activity.role', 'cron_sgi_sync_approvals'),
                              ('sgi.checklist.template', 'cron_generate'),
                              ('sgi.process.activity', 'cron_measure_activities'),
                              ('sgi.indicator', 'cron_missing_trajectories')):
            with self.assertRaises(AccessError):
                getattr(self.env[model].with_user(self.user), method)()
```

`tests/test_studio_cleanup.py` sigue pasando con la guarda nueva (`AccessError` es subclase de `UserError`).

- [x] **Step 3: Commit**

```bash
git add addons/quimibond_sgi/models addons/quimibond_sgi/tests/test_entrega1c.py
git commit -m "quimibond_sgi: cuatro procesos de cron y el borrado de modelos de Studio solo del sistema (K-06)"
```

### Task 1.6: K-07, texto de terceros escapado en el chatter

**Files:**
- Modify: `models/sgi_supplier_nc.py` (l.115-117), `models/sgi_nonconformity.py` (l.1062, 1100, 1117)

- [x] **Step 1: Proveedor**

```python
from markupsafe import Markup
...
        self.sudo().message_post(body=Markup(
            "<b>Respuesta del proveedor</b> por el portal.<br/><b>Causa:</b> %s<br/><b>Acción:</b> %s"
        ) % (cause[:5000], action[:5000]))
```

- [x] **Step 2: Motivos de cierre forzado y cancelación** en `sgi_nonconformity.py` (agregar `from markupsafe import Markup` a los imports). Mismo patrón en los tres `message_post`:

```python
        alert.message_post(body=Markup(
            "<b>Cierre forzado</b> por %s.<br/>Motivo: %s") % (self.env.user.name, self.reason))
```

```python
            alert.message_post(body=Markup(
                "<b>Solicitud de cancelación</b> de %s.<br/>Motivo: %s") % (self.env.user.name, reason))
```

```python
        alert.message_post(body=Markup(
            "<b>NC cancelada</b> por %s (Jefe MAST).<br/>Motivo: %s%s") % (
                self.env.user.name, reason,
                Markup("<br/>Solicitada por %s.") % requested_by.name if requested_by else ''))
```

- [x] **Step 3: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_supplier_nc.py addons/quimibond_sgi/models/sgi_nonconformity.py
git commit -m "quimibond_sgi: respuesta del proveedor y motivos escapados en el chatter (K-07)"
```

### Task 1.7: Versión, CHANGELOG, documentación y build

**Files:**
- Modify: `addons/quimibond_sgi/__manifest__.py`, `addons/quimibond_sgi/CHANGELOG.md`, `docs/sgi/tecnica/*` (generado)

- [x] **Step 1: Subir la versión** a `'19.0.57.91.0'`.

- [x] **Step 2: Entrada del CHANGELOG** arriba de la de 57.89.0

```markdown
## 19.0.57.91.0 — 2026-10-XX

**Seguridad: candados de evidencia** (auditoría 2026-10: K-01, K-02, K-06, K-07, FUNC-C13).

- **K-01:** un documento controlado vigente, en piloto u obsoleto no se manda a
  la papelera ni se borra (Odoo lo elimina a los 30 días con sus acuses; pasó
  con 3359, 5119 y 4995, ver 57.82.0). Solo el Jefe MAST o el sistema.
  `sgi.document.ack.document_id` pasa a `ondelete='restrict'`.
- **K-02:** una NC con folio no se borra (se cancela con motivo);
  `sgi.action.line.alert_id` pasa a `ondelete='restrict'`.
- **FUNC-C13:** solo el Jefe MAST o el dueño del proceso cierran una NC.
- **K-06:** `cron_sgi_sync_approvals`, `cron_generate`, `cron_measure_activities`,
  `cron_missing_trajectories` y `sgi_drop_empty_studio_models` solo los corre el sistema.
- **K-07:** respuesta del proveedor por el portal y motivos de cierre forzado y
  cancelación escapados en el chatter (`Markup`), con tope de 5,000 caracteres.
  Queda para 57.93.0: prueba `HttpCase` del portal y código de error en lugar
  de texto libre en la URL.
- **Cambiado:** `sgi_drop_empty_studio_models` ya no lo corre el Administrador
  SGI; solo el shell o un administrador del sistema.
- **Papelera:** un vaciado automático diario (`_gc_sgi_rescue_trashed_controlled`)
  rescata como obsoleto el controlado con acuses que esté en la papelera, antes
  de que la autolimpieza de Documents (30 días) intente borrarlo y la llave
  foránea atore el vaciado.

**Migración:** ninguna. El ORM rehace las dos llaves foráneas al actualizar.

**Pruebas:** `test_candados_evidencia` (nueva, 11 casos); `test_entrega1c.test_04` ampliada.
```

- [x] **Step 3: Documentación técnica generada**

```bash
python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check
```

- [x] **Step 4: Checadores** (sección 0, punto 4). Esperado: 0 errores.

- [ ] **Step 5: Commit y push; leer el build**

```bash
git add -A addons/quimibond_sgi docs/sgi
git commit -m "quimibond_sgi 19.0.57.91.0: candados de evidencia"
git push
```

Esperado en el build de la rama: `test_candados_evidencia` 11/11 OK y ningún fallo nuevo en `--test-tags /quimibond_sgi` respecto del build de `main`. Si una prueba existente borra NC o documentos controlados, corregir la prueba (borrar primero lo dependiente o usar `sgi_bypass` como MAST), nunca quitar el candado.

- [ ] **Step 6: Verificación después del despliegue a producción** (solo lectura)

`ir.module.module [('name','=','quimibond_sgi')]` → `latest_version = 19.0.57.91.0`. Con `get_fields('sgi.document.ack')` el campo `document_id` debe reportar `ondelete: restrict` (si el MCP no lo expone, revisar el `update.log` del deploy en busca de errores al crear la llave foránea: Odoo registra y se salta las restricciones que no puede crear).

---

## Entrega 57.92.0 — Bandeja: Mis pendientes que dice qué hacer

**Objetivo:** que Mis pendientes abra con lo atrasado y por vencer a la vista, deje validar mediciones en lote, lleve directo a donde se hace cada cosa, incluya los avisos de los crons y hable en «usted».

**Archivos:**
- Modify: `addons/quimibond_sgi/models/sgi_my_pending.py`
- Modify: `addons/quimibond_sgi/views/sgi_my_pending_views.xml`
- Modify: `addons/quimibond_sgi/tests/test_bandeja.py`
- Create: `addons/quimibond_sgi/tests/test_usted.py`
- Modify (barrido de tuteo): los archivos listados en la Task 2.5
- Modify: `__manifest__.py`, `CHANGELOG.md`, `docs/sgi/usuarios/operador-o-supervisor.md`

### Task 2.0: Rama

- [x] **Step 1:** después de que 57.91.0 entre a `main`:

```bash
git fetch origin main && git checkout -B claude/sgi-57-91-bandeja origin/main
```

### Task 2.1: Validar en lote (U-02)

**Files:**
- Modify: `models/sgi_my_pending.py` (junto a `action_validate_measure`, ~l.619)
- Modify: `views/sgi_my_pending_views.xml`
- Test: `tests/test_bandeja.py`

- [x] **Step 1: Prueba** (agregar al final de `TestBandeja`)

```python
    # ---- 57.92.0 (U-02): validar en lote ------------------------------------
    def test_20_validar_seleccionadas(self):
        indicator = self._indicator('Z8A-L', calc_mode='otif_ventas', frequency='weekly')
        mondays = [self.today - timedelta(days=self.today.weekday() + 7 * n) for n in (1, 2)]
        measures = self.env['sgi.indicator.measure'].create([
            {'indicator_id': indicator.id, 'period_date': d, 'state': 'capturado', 'value': 90.0}
            for d in mondays])
        rows = self.Pending.with_user(self.user)._sgi_build(self.emp)
        mine = rows.filtered(lambda r: r.kind == 'validacion' and r.res_id in measures.ids)
        other = rows.filtered(lambda r: r.kind != 'validacion')[:1]
        self.assertEqual(len(mine), 2)
        (mine | other).with_user(self.user).action_validate_selected()
        self.assertEqual(set(measures.mapped('state')), {'validado'})
        self.assertFalse(mine.exists(), "Los renglones validados desaparecen.")
        if other:
            self.assertTrue(other.exists(), "Los que no son mediciones se quedan.")
```

- [x] **Step 2: Método**

```python
    def action_validate_selected(self):
        """57.92.0 (U-02): «Validar seleccionadas». Ignora los renglones que no
        son mediciones; valida con los permisos de quien abre la lista (solo el
        dueño del indicador o el Jefe MAST, ``_sgi_check_validate_access``)."""
        rows = self.filtered(lambda r: r.kind == 'validacion'
                             and r.res_model == 'sgi.indicator.measure')
        if not rows:
            raise UserError("Seleccione al menos una medición por validar.")
        measures = self.env['sgi.indicator.measure'].browse(rows.mapped('res_id')).exists()
        measures.action_validate()
        rows.unlink()
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}
```

- [x] **Step 3: Botón de cabecera** en `sgi_my_pending_view_list`, como primer hijo de `<list>`

```xml
                <header>
                    <button name="action_validate_selected" type="object"
                            string="Validar seleccionadas" class="btn-primary"/>
                </header>
```

- [x] **Step 4: Commit**

```bash
git add addons/quimibond_sgi/models/sgi_my_pending.py addons/quimibond_sgi/views/sgi_my_pending_views.xml addons/quimibond_sgi/tests/test_bandeja.py
git commit -m "quimibond_sgi: Mis pendientes valida mediciones en lote (U-02)"
```

### Task 2.2: Abrir desplegada y con lo urgente primero (U-02)

**Files:**
- Modify: `views/sgi_my_pending_views.xml`, `models/sgi_my_pending.py` (`_sgi_action`, ~l.567)
- Test: `tests/test_bandeja.py`

- [x] **Step 1: Prueba**

```python
    def test_21_abre_desplegada_con_lo_urgente(self):
        action = self.Pending.with_user(self.user).action_open_mine()
        self.assertEqual(action['context'].get('search_default_actionable'), 1)
        arch = self.env.ref('quimibond_sgi.sgi_my_pending_view_list').arch
        self.assertIn('expand="1"', arch)
```

- [x] **Step 2: Vista.** En `<list …>` agregar `expand="1"`. En la búsqueda, primer filtro:

```xml
                <filter name="actionable" string="Atrasadas o por vencer"
                        domain="[('state', 'in', ('atrasada', 'por_vencer'))]"/>
```

- [x] **Step 3: Acción.** En `_sgi_action`, después de armar `context`:

```python
        if not group_by_person:
            context['search_default_actionable'] = 1
```

y el `help` en «usted»:

```python
            'help': "<p class='o_view_nocontent_smiling_face'>Sin pendientes</p>"
                    "<p>No tiene nada atrasado ni por vencer. Las actividades sin medición "
                    "automática no salen aquí: revíselas en Mi procedimiento.</p>",
```

El filtro por omisión aplica también a `action_show_pending` (botón de Mi procedimiento), que es lo deseado; Mi equipo agrupa por persona y no lo recibe.

- [x] **Step 4: Commit**

```bash
git commit -am "quimibond_sgi: Mis pendientes abre desplegada con lo atrasado y por vencer (U-02)"
```

### Task 2.3: «Ir» y «Leer» llevan a donde se hace (U-05)

**Files:**
- Modify: `models/sgi_my_pending.py` (`action_open`, ~l.597; nuevo `action_sign_ack`)
- Modify: `views/sgi_my_pending_views.xml`
- Test: `tests/test_bandeja.py`

- [x] **Step 1: Pruebas**

```python
    def test_22_ir_a_hacerlo_y_leer(self):
        # Un menú con acción (Inicio → Documentos vigentes).
        menu = self.env.ref('quimibond_sgi.menu_sgi_current_documents')
        self.activity.sudo().write({'odoo_menu_id': menu.id})
        row = self.Pending.create({'kind': 'actividad', 'name': 'Hacer 8.1',
                                   'res_model': 'sgi.process.activity', 'res_id': self.activity.id})
        action = row.action_open()  # superusuario: solo se revisa el destino
        self.assertNotEqual(action.get('res_model'), 'sgi.process.activity',
                            "Lleva al menú donde se hace, no a la ficha del catálogo.")
        doc = self.env['documents.document'].create({
            'name': 'Leer 8A', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z8A-01', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.emp.id})
        row = self.Pending.create({'kind': 'acuse', 'name': 'Leer', 'employee_id': self.emp.id,
                                   'res_model': 'sgi.document.ack', 'res_id': ack.id})
        action = row.action_open()
        self.assertEqual((action['res_model'], action['res_id']), ('documents.document', doc.id))
        row.with_user(self.user).action_sign_ack()
        self.assertEqual(ack.state, 'leido')
```

`menu_sgi_current_documents` (`views/sgi_menus.xml:46`) tiene acción; el estado leído de `sgi.document.ack` es `'leido'` (`models/sgi_document.py:1276`). Como `action_open` corre con el usuario que abre la lista, la prueba llama a `row.action_open()` como superusuario solo para el destino; la firma sí va con `with_user(self.user)`.

- [x] **Step 2: Código.** En `action_open`, antes del `return` final:

```python
        if self.kind == 'actividad' and self.res_model == 'sgi.process.activity':
            activity = self.env['sgi.process.activity'].sudo().browse(self.res_id).exists()
            if activity and (activity.odoo_menu_id or activity.odoo_action_id or activity.odoo_ref):
                return activity.action_open_odoo()
        if self.kind == 'acuse' and self.res_model == 'sgi.document.ack':
            ack = self.env['sgi.document.ack'].sudo().browse(self.res_id).exists()
            if ack:
                return {'type': 'ir.actions.act_window', 'res_model': 'documents.document',
                        'res_id': ack.document_id.id, 'view_mode': 'form', 'target': 'current'}
```

Si `odoo_action_id` no existe en `sgi.process.activity` (lo agrega `sgi_activity_spec.py`), usar `'odoo_action_id' in activity._fields and activity.odoo_action_id`.

Nuevo método:

```python
    def action_sign_ack(self):
        """57.92.0 (U-05): «Leído y entendido» desde el renglón. El candado de
        identidad de ``sgi.document.ack`` decide si quien abre la lista puede."""
        self.ensure_one()
        if self.kind != 'acuse' or self.res_model != 'sgi.document.ack':
            raise UserError("Este renglón no es un acuse de lectura.")
        ack = self.env['sgi.document.ack'].browse(self.res_id).exists()
        if not ack:
            raise UserError("El acuse ya no existe.")
        ack.action_mark_read()
        self.unlink()
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}
```

- [x] **Step 3: Vista.** Sustituir el botón «Abrir» por:

```xml
                <button name="action_open" type="object" string="Ir" icon="fa-arrow-right"
                        invisible="kind == 'acuse'"/>
                <button name="action_open" type="object" string="Leer" icon="fa-book"
                        invisible="kind != 'acuse'"/>
                <button name="action_sign_ack" type="object" string="Leído y entendido"
                        icon="fa-check" class="btn-link" invisible="kind != 'acuse'"
                        confirm="¿Confirma que leyó y entendió este documento? Su acuse queda registrado con la fecha de hoy."/>
```

- [x] **Step 4: Commit**

```bash
git commit -am "quimibond_sgi: «Ir» lleva al menú de la actividad y «Leer» al documento con su acuse (U-05)"
```

### Task 2.4: Avisos de los crons en Mis pendientes (U-03)

Respeta D-04: las actividades nativas siguen existiendo; Mis pendientes solo las muestra.

**Files:**
- Modify: `models/sgi_my_pending.py` (`PENDING_KINDS`, `_sgi_pending_records`, `_sgi_row`, `_sgi_row_users`, `_sgi_pending_values`, `action_open`, nuevo `action_done_notice`)
- Modify: `views/sgi_my_pending_views.xml`
- Test: `tests/test_bandeja.py`

- [x] **Step 1: Prueba**

```python
    def test_23_avisos_de_los_crons(self):
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'Aviso 8A', 'team_id': team.id})
        notice = alert.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=self.today - timedelta(days=1),
            summary='Revisar aviso 8A', user_id=self.user.id)
        far = alert.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=self.today + timedelta(days=30),
            summary='Aviso lejano 8A', user_id=self.user.id)
        row = self._row('aviso', notice.id)
        self.assertTrue(row)
        self.assertEqual(row['state'], 'atrasada')
        self.assertIn('Revisar aviso 8A', row['name'])
        self.assertFalse(self._row('aviso', far.id), "Solo vencidos o de los próximos 7 días.")
        # La NC de la prueba no tiene responsables: su aviso no se cubre con el renglón «nc».
        # Lo que ya tiene renglón propio no se duplica.
        doc = self.env['documents.document'].create({
            'name': 'Acuse aviso 8A', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-Z8A-02', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        dup = doc.activity_schedule('mail.mail_activity_data_todo', date_deadline=self.today,
                                    summary='Acuse 8A', user_id=self.user.id)
        dup.sgi_cron_key = 'acuse_pendiente:1'
        self.assertFalse(self._row('aviso', dup.id))
        # «Hecho» la cierra.
        rec = self.Pending.with_user(self.user)._sgi_build(self.emp)
        line = rec.filtered(lambda r: r.kind == 'aviso' and r.res_id == notice.id)
        line.with_user(self.user).action_done_notice()
        self.assertFalse(notice.exists() and notice.active)
```

- [x] **Step 2: Tipo nuevo** al final de `PENDING_KINDS`:

```python
    ('aviso', "Aviso"),
```

y una constante junto a `HORIZON_DAYS`:

```python
# 57.92.0 (U-03): actividades nativas (avisos de los crons) que se muestran.
NOTICE_MODELS = ('quality.alert', 'documents.document', 'maintenance.request',
                 'helpdesk.ticket', 'project.task')
```

- [x] **Step 3: Fuente.** Al final de `_sgi_pending_records`, antes de `return records`:

```python
        # 57.92.0 (U-03): avisos de los crons y actividades de las apps del SGI,
        # vencidos o de los próximos 7 días, sin los que ya tienen renglón propio.
        Activity = env['mail.activity'].sudo()
        notices = Activity.search(
            [('user_id', 'in', ids), ('date_deadline', '<=', today + timedelta(days=SOON_DAYS)),
             '|', ('res_model', '=like', 'sgi.%'), ('res_model', 'in', NOTICE_MODELS)],
            order='date_deadline, id')
        covered = Activity
        if 'aprobacion' in records:
            covered |= records['aprobacion'].mapped('mail_activity_id')
        covered |= env['sgi.action.line'].sudo().search(
            [('activity_id', 'in', notices.ids)]).mapped('activity_id')
        # Los avisos de plazo de una NC ya salen en su renglón «nc».
        nc_ids = set(records['nc'].ids) if records.get('nc') else set()
        covered |= notices.filtered(lambda a: a.res_model == 'quality.alert' and a.res_id in nc_ids)
        has_key = 'sgi_cron_key' in Activity._fields

        def own_row(act):
            key = (act.sgi_cron_key or '') if has_key else ''
            return (key.startswith('acuse_pendiente:') or key == 'revision_bienal'
                    or (act.res_model == 'sgi.indicator'
                        and (act.summary or '').startswith('Capturar indicador ')))
        records['aviso'] = notices.filtered(lambda a: a not in covered and not own_row(a))
```

- [x] **Step 4: Renglón y destinatario.** En `_sgi_row`, antes del bloque final de documentos:

```python
        if kind == 'aviso':
            what = rec.summary or rec.activity_type_id.name or "Aviso"
            return {'name': "%s — %s" % (what, rec.res_name or rec.res_model),
                    'date_due': rec.date_deadline, 'process_id': False}
```

En `_sgi_row_users`, antes del `return` final:

```python
        if kind == 'aviso':
            return rec.user_id
```

`_sgi_pending_values` ya guarda `res_model=rec._name` (`mail.activity`) y `res_id=rec.id`; no cambia.

- [x] **Step 5: Abrir y «Hecho».** En `action_open`, al principio después de `ensure_one`:

```python
        if self.kind == 'aviso' and self.res_model == 'mail.activity':
            act = self.env['mail.activity'].sudo().browse(self.res_id).exists()
            if act:
                return {'type': 'ir.actions.act_window', 'res_model': act.res_model,
                        'res_id': act.res_id, 'view_mode': 'form', 'target': 'current'}
```

Método nuevo:

```python
    def action_done_notice(self):
        """57.92.0 (U-03): marca hecho el aviso, solo si es de quien abre la lista."""
        self.ensure_one()
        act = self.env['mail.activity'].browse(self.res_id).exists() \
            if self.kind == 'aviso' and self.res_model == 'mail.activity' else False
        if not act:
            raise UserError("El aviso ya no existe.")
        if act.user_id != self.env.user:
            raise UserError("Solo la persona a quien está asignado el aviso puede marcarlo hecho.")
        act.action_feedback(feedback="Hecho desde Mis pendientes.")
        self.unlink()
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}
```

- [x] **Step 6: Vista.** Botón y filtro:

```xml
                <button name="action_done_notice" type="object" string="Hecho" icon="fa-check"
                        class="btn-link" invisible="kind != 'aviso'"/>
```

```xml
                <filter name="kind_notice" string="Avisos" domain="[('kind', '=', 'aviso')]"/>
```

- [x] **Step 7: Manual.** En `docs/sgi/usuarios/operador-o-supervisor.md`, donde dice «una sola lista», agregar que los avisos de Odoo del SGI (vencidos o de la semana) salen como «Aviso» con el botón «Hecho».

- [x] **Step 8: Commit**

```bash
git commit -am "quimibond_sgi: los avisos de los crons salen en Mis pendientes con «Hecho» (U-03)"
```

### Task 2.5: Barrido de «tú» a «usted» y glosario (U-06)

**Files:**
- Create: `addons/quimibond_sgi/tests/test_usted.py`
- Modify: los archivos que la prueba señale (punto de partida abajo)

- [x] **Step 1: Prueba que lee las fuentes del módulo**

```python
# -*- coding: utf-8 -*-
"""57.92.0 (U-06): textos para el usuario en «usted» y con el glosario del
README (Jefe MAST, NC, CoA). Lee las fuentes del módulo: no necesita datos."""
import os
import re

from odoo.tests import BaseCase, tagged

# Solo formas que no pueden ser tercera persona. «pide», «revisa», «agrega»,
# «elige», «levanta» o «contesta» también son «usted/él» («Persona que levanta
# la NC»): como imperativo de «tú» solo cuentan al inicio de oración o tras «¿».
TUTEO = re.compile(
    r"\b(tienes|puedes|quieres|confirmas|leíste|tu|tus|te ligue|apruébala|apágalo|adjúntala)\b"
    r"|(?:^|[.!?¿:]\s*)(Pide|Elige|Revisa|Agrega|Captura|Programa|Contesta|Levanta|Usa)\b")
GLOSARIO = re.compile(r"Jefe de MAST|\bNCs\b|\bCOA\b|No Conformidad\b")
# Cadenas entre comillas en .py y valores/atributos en .xml.
QUOTED = re.compile(r"\"([^\"\n]{4,})\"|'([^'\n]{4,})'")
SKIP_DIRS = {'tests', 'migrations', 'static', 'tools', 'demo'}


def _offending(pattern):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hits = []
    for folder, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if not name.endswith(('.py', '.xml')):
                continue
            path = os.path.join(folder, name)
            with open(path, encoding='utf-8') as handle:
                for number, line in enumerate(handle, 1):
                    stripped = line.strip()
                    if stripped.startswith('#') or stripped.startswith('<!--'):
                        continue
                    for match in QUOTED.finditer(line):
                        text = match.group(1) or match.group(2)
                        if ' ' in text and pattern.search(text):
                            hits.append("%s:%d: %s" % (os.path.relpath(path, root), number, text))
    return hits


@tagged('post_install', '-at_install')
class TestUsted(BaseCase):

    def test_01_sin_tuteo(self):
        hits = _offending(TUTEO)
        self.assertFalse(hits, "Textos en «tú»:\n" + "\n".join(hits))

    def test_02_glosario(self):
        hits = _offending(GLOSARIO)
        self.assertFalse(hits, "Fuera del glosario:\n" + "\n".join(hits))
```

Registrar en `tests/__init__.py`: `from . import test_usted`.

Correr la búsqueda en local sin Odoo para tener la lista de trabajo:

```bash
cd addons/quimibond_sgi && python3 - <<'EOF'
import os, re, sys
sys.path.insert(0, 'tests')
src = open('tests/test_usted.py', encoding='utf-8').read()
ns = {}
exec(src.split('@tagged')[0].replace('from odoo.tests import BaseCase, tagged', '')
     .replace('os.path.dirname(os.path.dirname(os.path.abspath(__file__)))', '"."'), ns)
for pattern in ('TUTEO', 'GLOSARIO'):
    print("\n".join(ns['_offending'](ns[pattern])))
EOF
```

Esperado: alrededor de 35 a 50 líneas de tuteo y unas 70 de glosario (con la expresión amplia la revisión del plan contó 111 y 74; la acotada quita los verbos ambiguos). La prueba solo mira cadenas entre comillas: los textos dentro de nodos XML (cuerpos de reportes y `body_html` de plantillas) se revisan a mano con `grep -n "tienes\|puedes\|\btu\b" report/*.xml data/sgi_mail_templates.xml` (la auditoría las listó en `sgi_my_procedure_views.xml:191`, `sgi_my_pending.py:666`, `sgi_my_procedure_screen.py:757-799,1053`, `sgi_current_documents*.py/xml`, `data/sgi_mail_templates.xml:81,87,105`, `sgi_weekly_overdue.py:32`, `sgi_nonconformity.py:445,677,1094,1105`, `sgi_coa*.py/xml`, `sgi_mp_change*.py/xml`, `sgi_incident.py:105`, `sgi_checklist.py:302`, `sgi_document.py:452,672`, `sgi_doc_change_sign.py:251`, `sgi_supplier_nc.py:72`, `sgi_integration.py:283`, `data/sgi_mp_change_category_data.xml:12`, y las notas de los crons en `sgi_cron.py`). Si la expresión da falsos positivos (por ejemplo, «Revisa» como sustantivo en un nombre de etapa), afinarla en la prueba con una lista de excepciones explícita y comentada, no relajarla.

- [x] **Step 2: Reescribir cada cadena.** Ejemplos de reemplazo:

| Antes | Después |
|---|---|
| Tu usuario no está ligado a un empleado. Pide a RH que te ligue… | Su usuario no está ligado a un empleado. Pida a RH que lo ligue en su ficha de empleado para ver su procedimiento. |
| No tienes nada atrasado… | No tiene nada atrasado ni por vencer. |
| Esa persona no está en tu equipo; solo ves a tu gente… | Esa persona no está en su equipo; solo ve a su gente, sus departamentos y los puestos de sus procesos. |
| ¿Confirmas que leíste…? Tu acuse… | ¿Confirma que leyó y entendió este documento? Su acuse queda registrado con la fecha de hoy. |
| SGI: tienes N pendiente(s) | SGI: tiene N pendientes atrasados |
| Captura el motivo… | Capture el motivo… |
| Jefe de MAST | Jefe MAST |
| NCs ligadas | NC ligadas |
| COA | CoA |
| ⚠ en asuntos de correo | [Urgente] |

Las plantillas de correo (`data/sgi_mail_templates.xml`) son `noupdate` y MAST puede haberlas ajustado (`data/sgi_mail_templates.xml:7`). Cambiar el XML basta para bases nuevas; para producción, una post-migración que reescriba `subject` y `body_html` **pisa lo que MAST haya editado**, así que por la regla 9 necesita el visto bueno escrito de Jose en el PR. Alternativa sin migración: que MAST corrija los tres textos desde Ajustes → Técnico → Plantillas de correo, y anotarlo en la pista de datos.

`report/report_nc.xml:22` dice «no es una No Conformidad»; si el barrido lo cambia, actualizar la aserción de `tests/test_vistas_pulido.py:359`.

- [x] **Step 3: Commit**

```bash
git add -A addons/quimibond_sgi
git commit -m "quimibond_sgi: textos en «usted» y con el glosario, con prueba que lo vigila (U-06)"
```

### Task 2.6: Versión, CHANGELOG, árbol de menús y build

- [x] **Step 1:** `__manifest__.py` → `'19.0.57.92.0'`.
- [x] **Step 2:** CHANGELOG `## 19.0.57.92.0` con: Agregado («Validar seleccionadas», avisos, «Ir», «Leer», «Leído y entendido»), Cambiado (abre desplegada con «Atrasadas o por vencer»; textos en «usted»), Migración (si hubo plantillas `noupdate`), Pruebas (`test_bandeja` 20–23, `test_usted`).
- [x] **Step 3:** `python3 tools/sgi_docs.py && python3 tools/sgi_docs.py --check` y los demás checadores.
- [ ] **Step 4:** Commit y push; build con `--test-tags /quimibond_sgi`. Esperado: `test_bandeja` y `test_usted` en verde, sin fallos nuevos.
- [ ] **Step 5: Verificación en producción** (solo lectura, tras desplegar): `ir.module.module` en 57.92.0. Pedir a dos dueños de proceso que abran Mis pendientes y confirmen que ven los avisos y pueden validar en lote; registrar el resultado en el PR.

---

## Fichas de las entregas 57.93.0 a 57.100.0

Cada ficha se convierte en un plan detallado (mismo formato que arriba) al iniciar la entrega. Todas siguen la sección 0.

### 57.93.0 — NC y auditoría con evidencia

- **Alcance:** N-02 (campo `sgi_effective` Eficaz / No eficaz; «No eficaz» regresa a Seguimiento y pide acción nueva; fecha de eficacia ≥ `sgi_effectiveness_due` salvo cierre forzado; evidencia obligatoria en `action_mark_done` de acciones correctivas), N-03 (`nc_menor`/`nc_mayor` exigen `alert_id`; programa sugerido incluye procesos en borrador; aviso de cobertura de 3 años), N-12 (`_sgi_is_customer_return` por `location_id.usage == 'customer'`), K-03 (con la NC, el incidente o la revisión cerrados, acciones y hallazgos solo los edita MAST).
- **Archivos:** `models/sgi_nonconformity.py` (l.117-121, 398-429, 578-619, 828, 1001), `models/sgi_audit.py` (l.113-139, 454-475, hallazgo `write`), `models/sgi_integration.py` (l.41-49), vistas de NC y acción.
- **Migración:** post-migrate que solo crea el campo (0 NC cerradas hoy: nada que rellenar). Para la evidencia, las acciones ya terminadas quedan exentas (filtro por fecha de creación de la versión).
- **Pendiente de 57.91.0 (FUNC-C13):** una NC se puede crear directamente en «Cerrada» (`create` no pasa por `_sgi_check_stage_move`): aplicar el mismo candado al crear. La actividad «Verificar eficacia» va a `sgi_effectiveness_by`, que puede no ser el dueño del proceso: asignarla al dueño (o a quien puede cerrar) para que no reciba el mensaje de FUNC-C13 al cerrar.
- **Pruebas:** cierre antes de la fecha → `UserError`; «No eficaz» reabre; acción correctiva sin evidencia no se termina; `nc_menor` sin NC no cierra la auditoría; devolución con entrega en tres pasos crea NC (reproducir primero con una ruta de 3 pasos en la prueba para confirmar la hipótesis de la auditoría); editar acción de NC cerrada como Usuario SGI → `UserError`.

### 57.94.0 — SGI en planta

- **Puerta:** Q7 (cuántas tabletas, qué cuentas, RH captura PIN y en qué plazo). Sin PIN capturados la entrega no sirve.
- **Alcance:** U-01: acción cliente «SGI en planta» para cuentas de tableta (mosaico de empleados del departamento con foto; teclado numérico; menú de la persona: documentos por leer, reportar casi accidente, mi EPP, checklist de mi equipo). Cada registro guarda `employee_id` y «firmado con PIN en la tableta X». Reusar la validación de PIN del checklist (`sgi_checklist.py:254-310`) en un helper común. Encender `quimibond_sgi.checklist_pin_required` por parámetro cuando RH termine. I-03 («Marcar el resto como Bien»), I-05 (kanban móvil en 6 acciones de piso). U-08: `docs/sgi/usuarios/rh.md` y vista para RH «Empleados sin puesto, sin PIN o sin correo».
- **Archivos:** nuevo `models/sgi_floor_kiosk.py`, `static/src/floor_kiosk/*` (OWL), vistas, `sgi_menus.xml` + `tools/sgi_menu_tree.txt`, `sgi_document.py` (acuse con PIN: el candado de identidad acepta empleado + PIN válido), `sgi_incident.py`, `sgi_epp.py`.
- **Pruebas:** PIN válido firma a nombre del empleado; PIN inválido no; el registro no queda a nombre de la cuenta compartida; tour de la pantalla.

### 57.95.0 — SST y ambiente (entregada como 57.96.0)

- **Puertas:** Q9 (gestión del cambio), Q10 (matriz ambiental oficial), Q11 (contratistas).
- **Alcance:** N-06 (`control_hierarchy` en `sgi.risk` y `sgi.action.line`; IPER alto con solo EPP no cierra; `investigation_team_ids` en incidente con al menos un trabajador o integrante de la CSH; eficacia del incidente; `expired` guardado en el permiso + paso de cron cada hora; cierre bloqueado con LOTO aplicado; competencia requerida por tipo de permiso; «evaluación SST vigente hasta» en el contacto del contratista), N-07 (`sgi.env.aspect` única fuente con `life_cycle_stage`; «ambiental» fuera del selector de `sgi.risk` para nuevos; botones rápidos de lo legal abren el asistente con evidencia), incidente desde `hr.leave` con tipo «Incapacidad por riesgo de trabajo».
- **Migración:** post-migrate que pasa los 5 riesgos ambientales a aspectos con `risk_id` (log antes y después). El cron nuevo es un registro nuevo en un XML nuevo (no edita uno `noupdate`).
- **Pruebas:** una por candado y el traspaso de riesgos.

### 57.96.0 — Cláusulas y revisión por la dirección (entregada como 57.97.0)

- **Puerta:** Q13 (requisitos de cliente en CLI o en legal).
- **Alcance:** N-05 (cláusulas 6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4, 9.1.2 como datos nuevos con xmlid; no tocar las CLI sin xmlid, A-029; clasificación y cláusula obligatorias al pasar la NC a Seguimiento), N-09 (cargadores de incidentes, contexto, aspectos y mejoras; filtro `company_id` en el scrap, `sgi_management_review.py:344`; tipo `acuerdo` en `sgi.action.line`; conclusiones 9.3.3 obligatorias; acuerdos abiertos pasan a la siguiente revisión).
- **Pruebas:** carga de entradas, cierre con acuerdos abiertos, NC sin cláusula no pasa a Seguimiento.

### 57.97.0 — Interfaz (→ 57.98.0 o siguiente libre)

- **Puerta:** Q20 (arranque de Dirección en el Tablero).
- **Alcance:** I-01 (`sgi_format_footer` a `div.footer` con página x de y y fecha de emisión), I-02 (Documentos vigentes con clave, tipo, proceso y filtro por omisión), I-04 (filtro «Míos» único y `search_default` en 10 acciones), I-06 (tabla de colores y prueba sobre `decoration-*`), U-07 (menús por rol, «Reportar», «Checklists de hoy» bajo Inicio).
- **Pruebas:** `test_menu_tree` actualizado, prueba de colores, render de un reporte con el pie.

### 57.98.0 — Salud del SGI

- **Puerta:** metas del tablero aprobadas por Dirección.
- **Alcance:** los 10 indicadores de la sección 8 del reporte como `sgi.indicator` de nivel Dirección en E2 con `calc_mode` propio (procesos vigentes, personas activas en 30 días, planta identificable, acuses al día, mediciones validadas a tiempo, rojos con respuesta, NC eficaces, avisos vencidos y su concentración, programa de auditoría cumplido, formatos migrados utilizables) y un correo semanal a Dirección.
- **Pruebas:** cálculo de cada `calc_mode` con datos de prueba.

### 57.99.0 — Rendimiento y robustez (entregada como 57.95.0)

- **Alcance:** K-08 (un aviso por documento o por jefe en lugar de uno por acuse, `sgi_cron.py:679-687`; resumen guardado por empleado para Mi equipo; columna `sgi_cron_kind` indexada en `mail.activity` con backfill en post-migrate; recálculo nocturno de respaldo de las cuatro listas de Mi procedimiento con el número de cambios en el log), K-05 (dependencias de `sgi_picking_ids` y `sgi_payment_date`; separar propuesto de ajustado), D-06 (post-migrate: compañía 1 en los documentos controlados sin compañía, con log; regla de compañía en `sgi.legacy.routine`).
- **Pruebas:** el recálculo nocturno reporta 0 cambios en régimen; búsqueda de Mi equipo sin recorrer a toda la empresa.

### 57.100.0 — Integridad, competencias, PPAP e IA

- **Puertas:** Q12 (clientes que exigen PPAP, CoA y contingencia), Q16 (autorización de IA).
- **Alcance:** K-04 (semáforo y metas congelados al validar), N-13 (`hr_skills_survey`/`hr_skills_slides`: examen o curso aprobado crea la competencia con vigencia; encuesta de eficacia a 90 días), N-14 (`sgi_requires_ppap` calculado en el ECO, aviso al embarcar sin CoA; pruebas para `quimibond_sgi_plm`, que no tiene), IA (sugerencia de cláusula y clasificación, borrador de 5 porqués que el responsable edita; nunca escribe `sgi_root_cause` ni cierra).
- **Pruebas:** medición validada conserva su color al cambiar la meta; ECO de cliente automotriz marca PPAP; la sugerencia de IA no escribe campos de decisión.

---

## Seguimiento

- Cada entrega se cierra con: build verde, PR a `main`, PR a `quimibond`, `odoo-update quimibond_sgi`, verificación de solo lectura y una línea en este plan con la fecha.
- El tablero de 57.98.0 se revisa cada semana con Dirección; antes de esa versión, las cifras de la sección 2 del reporte se re-miden a mano cada dos semanas con las consultas del reporte.
