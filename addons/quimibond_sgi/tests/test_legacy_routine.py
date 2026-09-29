# -*- coding: utf-8 -*-
"""«Del Dropbox a Odoo», rutina por rutina (19.0.57.0.0; entrega 6). Pruebas
P-L1 a P-L5 y P-L9 de docs/audit/12-transicion.md §3.11 (P-L6 en
test_dropbox_key, P-L7 en test_dropbox_section, P-L8 en test_cleanup_45)."""
import base64
import importlib.util
import os

import psycopg2

from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

from .common_documents import sgi_hide_real_documents, sgi_neutralize_dropbox_contradictions
from .common_users import sgi_test_user

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_migration(version, name):
    path = os.path.join(_MODULE_DIR, 'migrations', version, name)
    spec = importlib.util.spec_from_file_location('sgi_mig_%s' % version.replace('.', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged('post_install', '-at_install')
class TestLegacyRoutine(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_neutralize_dropbox_contradictions(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.Doc = cls.env['documents.document']
        cls.Routine = cls.env['sgi.legacy.routine']
        cls.process = cls.env['sgi.process'].create({'code': 'E9', 'name': 'Proceso rutinas'})
        cls.other = cls.env['sgi.process'].create({'code': 'S9', 'name': 'Proceso que sustituye'})
        Activity = cls.env['sgi.process.activity']
        cls.a1 = Activity.create({'process_id': cls.process.id, 'name': 'Actividad uno', 'step': 1})
        cls.a2 = Activity.create({'process_id': cls.process.id, 'name': 'Actividad dos', 'step': 2})
        cls.proc_a = cls._proc('P-V71')
        cls.proc_b = cls._proc('P-V72')
        cls.mast = sgi_test_user(cls.env, login='e6r_mast')
        cls.user = sgi_test_user(cls.env, login='e6r_user', groups='quimibond_sgi.group_sgi_user')

    @classmethod
    def _proc(cls, code, **extra):
        vals = {'name': '%s Procedimiento de prueba.pdf' % code, 'type': 'binary',
                'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento', 'sgi_code': code,
                'sgi_state': 'vigente', 'sgi_process_id': cls.process.id}
        vals.update(extra)
        return cls.Doc.create(vals)

    def _row(self, clave, n, estado='cubierta', actividades='E9.01', motivo='', rutina=None):
        return {'clave': clave, 'n': n, 'rutina': rutina or 'Rutina %s %s' % (clave, n),
                'frecuencia': 'Mensual', 'responsable_anterior': 'Compras', 'estado': estado,
                'actividades': actividades, 'motivo': motivo}

    def test_numerales_de_prueba(self):
        self.assertEqual((self.a1.number, self.a2.number), ('E9.01', 'E9.02'))

    # P-L1
    def test_l1_restricciones(self):
        base = {'procedure_id': self.proc_a.id, 'name': 'Rutina'}
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self.Routine.create(dict(base, n=1, state='cubierta'))
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self.Routine.create(dict(base, n=2, state='reemplazada'))
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self.Routine.create(dict(base, n=3, state='pendiente', reason='Nadie',
                                     activity_ids=[(6, 0, self.a1.ids)]))
        ok = self.Routine.create(dict(base, n=4, state='cubierta', activity_ids=[(6, 0, self.a1.ids)]))
        self.assertEqual(ok.activity_numbers, 'E9.01')
        self.assertEqual(ok.procedure_code, 'P-V71')
        self.assertEqual(ok.process_id, self.process)
        self.assertTrue(ok.resolved_date, "Nace resuelta.")
        with mute_logger('odoo.sql_db'), self.assertRaises(psycopg2.IntegrityError), self.cr.savepoint():
            self.Routine.create(dict(base, n=4, state='eliminada', reason='Duplicada'))
            self.env.flush_all()
        with mute_logger('odoo.sql_db'), self.assertRaises(psycopg2.IntegrityError), self.cr.savepoint():
            self.Routine.create(dict(base, n=0, state='eliminada', reason='Cero'))
            self.env.flush_all()
        # Nadie borra: se archiva.
        with self.assertRaises(Exception), self.cr.savepoint():
            ok.with_user(self.mast).unlink()
        ok.with_user(self.mast).write({'active': False})
        self.assertFalse(ok.active)

    # P-L2
    def test_l2_sustituido_sin_pendientes(self):
        replaced = self._proc('P-V73', sgi_replaced_by_process_id=self.other.id)
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self.Routine.create({'procedure_id': replaced.id, 'n': 1, 'name': 'Pendiente',
                                 'state': 'pendiente', 'reason': 'Sin dueño'})
        pending = self.Routine.create({'procedure_id': self.proc_b.id, 'n': 1, 'name': 'Pendiente',
                                       'state': 'pendiente', 'reason': 'Sin dueño'})
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self.proc_b.write({'sgi_replaced_by_process_id': self.other.id})
        pending.write({'state': 'eliminada', 'reason': 'Se deja de hacer'})
        self.assertTrue(pending.resolved_date)
        self.proc_b.write({'sgi_replaced_by_process_id': self.other.id})

    def test_l2b_pendiente_con_fecha_limite(self):
        """Decisión de Jose (2026-09-29): toda pendiente nace con fecha límite
        del 16 de octubre de 2026 (parámetro), salvo que traiga la suya."""
        from datetime import date
        pending = self.Routine.create({'procedure_id': self.proc_b.id, 'n': 11, 'name': 'Pendiente',
                                       'state': 'pendiente', 'reason': 'Sin dueño'})
        self.assertEqual(pending.decision_deadline, date(2026, 10, 16))
        own = self.Routine.create({'procedure_id': self.proc_b.id, 'n': 12, 'name': 'Con fecha',
                                   'state': 'pendiente', 'reason': 'Sin dueño',
                                   'decision_deadline': '2026-10-02'})
        self.assertEqual(own.decision_deadline, date(2026, 10, 2))
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.legacy_decision_deadline', '2026-11-30')
        other = self.Routine.create({'procedure_id': self.proc_b.id, 'n': 13, 'name': 'Otra',
                                     'state': 'pendiente', 'reason': 'Sin dueño'})
        self.assertEqual(other.decision_deadline, date(2026, 11, 30))

    # P-L3
    def test_l3_prueba_carga_idempotente_y_archiva(self):
        rows = [self._row('P-V71', 1), self._row('P-V71', 2, 'reemplazada', '', 'Lo hace Odoo solo')]
        Routine = self.Routine.with_user(self.mast)
        dry = Routine.load_routines({'routines': rows})
        self.assertTrue(dry['dry_run'], "Por defecto, modo de prueba.")
        self.assertEqual(dry['summary']['created'], 2)
        self.assertFalse(self.Routine.search([('procedure_id', '=', self.proc_a.id)]), "No escribió.")
        real = Routine.load_routines({'routines': rows}, dry_run=False)
        self.assertEqual(real['summary']['created'], 2, real['errors'])
        routines = self.Routine.search([('procedure_id', '=', self.proc_a.id)])
        self.assertEqual(sorted(routines.mapped('state')), ['cubierta', 'reemplazada'])
        self.assertEqual(self.proc_a.sgi_routine_count, 2)
        self.assertEqual(self.proc_a.sgi_routine_resolved_pct, 100.0)
        self.assertIn(routines.filtered(lambda r: r.n == 1), self.a1.sudo().sgi_legacy_routine_ids)
        again = Routine.load_routines({'routines': rows}, dry_run=False)
        self.assertEqual((again['summary']['created'], again['summary']['updated']), (0, 0))
        self.assertEqual(again['summary']['unchanged'], 2)
        archived = Routine.load_routines({'routines': rows[:1], 'archive_missing': True}, dry_run=False)
        self.assertEqual(archived['summary']['archived'], 1)
        gone = self.Routine.with_context(active_test=False).search(
            [('procedure_id', '=', self.proc_a.id), ('n', '=', 2)])
        self.assertTrue(gone.exists() and not gone.active, "Se archiva, no se borra.")
        with self.assertRaises(AccessError):
            self.Routine.with_user(self.user).load_routines({'routines': rows})

    # P-L4
    def test_l4_transaccion_por_procedimiento(self):
        rows = [self._row('P-V71', 1), self._row('P-V71', 2, 'cubierta', ''),  # mala
                self._row('P-V72', 1), self._row('P-V72', 2, 'pendiente', '', 'Sin decidir')]
        result = self.Routine.with_user(self.mast).load_routines({'routines': rows}, dry_run=False)
        self.assertFalse(result['ok'])
        self.assertTrue(any(e['clave'] == 'P-V71' for e in result['errors']))
        self.assertFalse(self.Routine.search([('procedure_id', '=', self.proc_a.id)]),
                         "Una fila mala deja sin cargar su procedimiento.")
        self.assertEqual(len(self.Routine.search([('procedure_id', '=', self.proc_b.id)])), 2,
                         "El otro procedimiento sí se carga.")
        self.assertEqual(self.proc_b.sgi_routine_pending_count, 1)

    # P-L5
    def test_l5_excluida_sin_contenido(self):
        secret = 'TEXTO-DE-P-I01-QUE-NO-SALE'
        rows = [self._row('P-I01', 7, 'reemplazada', '', secret, rutina=secret),
                self._row('P-V71', 1, 'eliminada', '', 'usuario: fulano', rutina='Algo')]
        result = self.Routine.with_user(self.mast).load_routines({'routines': rows})
        rules = {e['regla'] for e in result['errors']}
        self.assertIn('excluida', rules)
        self.assertIn('credencial', rules)
        text = repr(result)
        self.assertNotIn(secret, text, "El reporte no repite el texto de la fila excluida.")
        self.assertNotIn('fulano', text, "Ni el de una fila con forma de credencial.")
        excluded = [e for e in result['errors'] if e['regla'] == 'excluida'][0]
        self.assertFalse(excluded['n'], "Ni siquiera el número de la rutina.")

    def test_pendientes_decision_y_responsable(self):
        rows = [self._row('P-V72', 1, 'pendiente', '', 'Sin decidir')]
        pending = [{'clave': 'P-V72', 'n': '1', 'decision': 'Crear actividad',
                    'responsable': 'e6r_mast', 'fecha': '2026-10-16'}]
        result = self.Routine.with_user(self.mast).load_routines(
            {'routines': rows, 'pending': pending}, dry_run=False)
        self.assertFalse([e for e in result['errors'] if e['clave'] == 'P-V72'], result['errors'])
        routine = self.Routine.search([('procedure_id', '=', self.proc_b.id)])
        self.assertEqual(routine.decision, 'actividad')
        self.assertEqual(routine.decision_owner_id, self.mast)
        self.assertEqual(str(routine.decision_deadline), '2026-10-16')

    def test_viene_de_solo_auditor_mast_direccion(self):
        with self.assertRaises(AccessError):
            self.a1.with_user(self.user).action_sgi_view_legacy_routines()
        action = self.a1.with_user(self.mast).action_sgi_view_legacy_routines()
        self.assertEqual(action['res_model'], 'sgi.legacy.routine')

    def test_asistente_prueba_primero_y_mismo_archivo(self):
        header = "clave,procedimiento,n,rutina,frecuencia,responsable_anterior,estado,actividades_odoo," \
                 "motivo,Revisión,Comentario\n"
        csv_text = header + "P-V71,Procedimiento de prueba,1,Rutina uno,Mensual,Compras,cubierta,E9.01,,OK,\n"
        wizard = self.env['sgi.legacy.routine.import'].with_user(self.mast).create({
            'file': base64.b64encode(csv_text.encode('utf-8')), 'filename': 'rutinas.csv'})
        with self.assertRaises(UserError):
            wizard.action_load()  # sin probar
        wizard.action_test()
        self.assertEqual(wizard.state, 'tested')
        self.assertTrue(wizard.dry_run_ok, wizard.line_ids.mapped('message'))
        self.assertFalse(self.Routine.search([('procedure_id', '=', self.proc_a.id)]))
        with self.assertRaises(UserError):
            wizard.action_load()  # sin confirmar
        # Otro archivo después de probar: no carga.
        wizard.write({'file': base64.b64encode((csv_text + "P-V71,,2,Otra,,,eliminada,,Ya no\n").encode()),
                      'confirm': True})
        with self.assertRaises(UserError):
            wizard.action_load()
        wizard.write({'file': base64.b64encode(csv_text.encode('utf-8'))})
        wizard.action_test()
        wizard.write({'confirm': True})
        wizard.action_load()
        self.assertEqual(wizard.state, 'loaded')
        self.assertFalse(wizard.file, "El archivo no se queda.")
        routine = self.Routine.search([('procedure_id', '=', self.proc_a.id)])
        self.assertEqual((routine.n, routine.state, routine.review_state), (1, 'cubierta', 'ok'))
        with self.assertRaises(AccessError):
            self.env['sgi.legacy.routine.import'].with_user(self.user).create({})

    # P-L9: migraciones de la transición, idempotentes (§3.10).
    def test_l9_migracion_clase_d_idempotente(self):
        dat = self.Doc.create({'name': 'DAT P-V71-01 Datos.pdf', 'type': 'binary',
                               'sgi_is_controlled': True, 'sgi_doc_type': 'dat',
                               'sgi_code': 'DAT P-V71-01', 'sgi_state': 'vigente',
                               'sgi_process_id': self.process.id})
        family = self.Doc.create({'name': 'DAT P-I01-77 Prueba.pdf', 'type': 'binary',
                                  'sgi_is_controlled': True, 'sgi_doc_type': 'dat',
                                  'sgi_code': 'DAT P-I01-77', 'sgi_state': 'vigente',
                                  'sgi_process_id': self.process.id})
        post = _load_migration('19.0.56.39.0', 'post-migrate.py')
        post.migrate(self.env.cr, '19.0.56.38.0')
        self.env.invalidate_all()
        self.assertEqual((dat.sgi_migration_class, dat.sgi_migration_state), ('d', 'na'))
        self.assertFalse(family.sgi_migration_class, "P-I01 y su familia no se tocan.")
        self.assertEqual(family.sgi_migration_state, 'pendiente')
        # Respaldo previo (en una base que ya migró, la tabla es la de esa
        # corrida y no se pisa: CREATE TABLE IF NOT EXISTS).
        self.env.cr.execute("SELECT to_regclass('documents_document_bak_563900')")
        self.assertTrue(self.env.cr.fetchone()[0], "Respaldo previo.")
        self.assertEqual(self.Doc._sgi_classify_legacy_documents(), {}, "La segunda vez no hace nada.")
