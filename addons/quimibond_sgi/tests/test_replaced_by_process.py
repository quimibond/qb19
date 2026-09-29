# -*- coding: utf-8 -*-
"""Procedimiento ↔ proceso (19.0.56.31.0): el DOCUMENTO es la única fuente de
verdad (decisión 3 de Jose; C-001, C-002, C-014, C-015, D-017, L-003, L-005).
Pruebas P-T1…P-T7 de 03-modelo.md y P-L8/P-L9 de 12-transicion.md (P-T7 y
P-L8 también en test_cleanup_45)."""
import importlib.util
import json
import base64
import os

from odoo import Command
from odoo.exceptions import AccessError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_test_user

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_migration(version, name):
    path = os.path.join(_MODULE_DIR, 'migrations', version, name)
    spec = importlib.util.spec_from_file_location('sgi_mig_%s' % version.replace('.', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged('post_install', '-at_install')
class TestReplacedByProcess(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.Process = cls.env['sgi.process'].with_context(active_test=False)
        cls.Doc = cls.env['documents.document']
        cls.old = cls.Process.create({'code': 'XRPO', 'name': 'Viejo RP'})
        cls.p1 = cls.Process.create({'code': 'XRP1', 'name': 'Nuevo RP 1'})
        cls.p2 = cls.Process.create({'code': 'XRP2', 'name': 'Nuevo RP 2'})

    def _doc(self, code, doc_type='procedimiento', state='vigente', controlled=True, **extra):
        vals = {'name': code, 'type': 'binary', 'sgi_is_controlled': controlled,
                'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': state,
                'sgi_process_id': self.old.id}
        vals.update(extra)
        return self.Doc.create(vals)

    # P-T1
    def test_t1_el_documento_manda_y_el_proceso_lo_lee(self):
        doc = self._doc('P-A81')
        doc.sgi_replaced_by_process_id = self.p1
        self.assertEqual(self.p1.replaced_document_ids, doc)
        # El inverso escrito por ORM (Command.link) termina en el documento.
        other = self._doc('P-A82')
        self.p1.write({'replaced_document_ids': [Command.link(other.id)]})
        self.assertEqual(other.sgi_replaced_by_process_id, self.p1)
        self.assertEqual(self.p1.replaced_document_ids, doc | other)
        self.assertFalse(self.p2.replaced_document_ids)

    # P-T2 (+ C-002)
    def test_t2_vigente_hasta_que_el_proceso_entra_en_vigor(self):
        doc = self._doc('P-A83', sgi_migration_state='en_curso')
        doc.sgi_replaced_by_process_id = self.p1
        self.p1.write({'state': 'piloto'})
        self.assertEqual(doc.sgi_state, 'vigente', "En borrador o piloto conviven.")
        self.p1.write({'state': 'vigente'})
        self.assertEqual(doc.sgi_state, 'obsoleto')
        self.assertTrue(doc.sgi_obsolete_date)
        self.assertIn('XRP1', doc.sgi_obsolete_reason)
        self.assertEqual(doc.sgi_migration_state, 'baja')
        messages = len(doc.message_ids)
        self.p1._sgi_obsolete_replaced_documents()
        self.assertEqual(len(doc.message_ids), messages, "Correr dos veces no hace nada.")

    # P-T3 (+ C-014)
    def test_t3_carga_sin_uno_deja_la_m2o_vacia_y_no_borra(self):
        a, b = self._doc('P-A84'), self._doc('P-A85')
        payload = {'processes': [{'code': 'XRP1', 'name': 'Nuevo RP 1',
                                  'replaced_documents': ['P-A84', 'P-A85']}]}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        self.assertEqual(self.p1.replaced_document_ids, a | b)
        payload['processes'][0]['replaced_documents'] = ['P-A84']
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        self.assertFalse(b.sgi_replaced_by_process_id)
        self.assertTrue(b.exists(), "Quitarlo de la lista no borra el documento.")
        self.assertEqual(self.p1.replaced_document_ids, a)
        # Idempotente: la misma carga no cambia nada.
        again = self.Process.load_payload(payload)
        self.assertFalse([c for c in again['changes'] if c['action'] == 'replaced_documents'])

    def test_t3b_restrict_no_deja_borrar_el_proceso(self):
        doc = self._doc('P-A86')
        doc.sgi_replaced_by_process_id = self.p2
        with self.assertRaises(Exception), self.cr.savepoint(), mute_logger('odoo.sql_db'):
            self.p2.unlink()
            self.env.flush_all()
        self.assertEqual(self.env['documents.document']._fields[
            'sgi_replaced_by_process_id'].ondelete, 'restrict')

    # P-T4
    def test_t4_carga_rechaza_un_procedimiento_de_otro_proceso(self):
        doc = self._doc('P-A87')
        doc.sgi_replaced_by_process_id = self.p2
        result = self.Process.load_payload({'processes': [{
            'code': 'XRP1', 'name': 'Nuevo RP 1', 'replaced_documents': ['P-A87']}]})
        self.assertFalse(result['ok'])
        self.assertIn('XRP2', str(result['errors']))
        self.assertEqual(doc.sgi_replaced_by_process_id, self.p2, "No se reasigna en silencio.")

    # P-T5 (C-015)
    def test_t5_restriccion(self):
        fmt = self._doc('F-P-A88-01', doc_type='formato')
        with self.assertRaises(ValidationError), self.cr.savepoint():
            fmt.write({'sgi_replaced_by_process_id': self.p1.id})
        loose = self._doc('P-A89', controlled=False)
        with self.assertRaises(ValidationError), self.cr.savepoint():
            loose.write({'sgi_replaced_by_process_id': self.p1.id})
        archived = self.Process.create({'code': 'XRPA', 'name': 'Archivado RP', 'active': False})
        proc = self._doc('P-A90')
        with self.assertRaises(ValidationError), self.cr.savepoint():
            proc.write({'sgi_replaced_by_process_id': archived.id})
        # «No aplica» no convive con un proceso que lo sustituye; «migrado» ya no.
        proc.write({'sgi_replaced_by_process_id': self.p1.id})
        with self.assertRaises(ValidationError), self.cr.savepoint():
            proc.write({'sgi_migration_state': 'na'})
        with self.assertRaises(ValidationError), self.cr.savepoint():
            proc.write({'sgi_migration_state': 'migrado'})

    def test_t5b_solo_el_jefe_mast_lo_captura(self):
        doc = self._doc('P-A91')
        user = sgi_test_user(self.env, login='sgi_rp_usuario', groups='quimibond_sgi.group_sgi_user')
        with self.assertRaises(AccessError):
            doc.with_user(user).write({'sgi_replaced_by_process_id': self.p1.id})
        manager = sgi_test_user(self.env, login='sgi_rp_jefe')
        doc.with_user(manager).write({'sgi_replaced_by_process_id': self.p1.id})
        self.assertEqual(doc.sgi_replaced_by_process_id, self.p1)

    # P-T6 (L-003)
    def test_t6_premigracion_respalda_y_no_copia(self):
        cr = self.env.cr
        doc_both = self._doc('P-A92')
        doc_only = self._doc('P-A93')
        doc_both.sgi_replaced_by_process_id = self.p1
        self.env.flush_all()
        cr.execute("""CREATE TABLE IF NOT EXISTS sgi_process_replaced_doc_rel
                      (process_id integer, document_id integer)""")
        cr.execute("""INSERT INTO sgi_process_replaced_doc_rel VALUES
                      (%s, %s), (%s, %s), (%s, %s)""",
                   (self.p1.id, doc_both.id, self.p2.id, doc_both.id, self.p2.id, doc_only.id))
        pre = _load_migration('19.0.56.31.0', 'pre-migrate.py')
        with self.assertLogs('sgi_mig_19_0_56_31_0', level='WARNING') as logs:
            pre.migrate(cr, '19.0.56.30.0')
            pre.migrate(cr, '19.0.56.30.0')
        self.assertTrue(any('conflictos' in line for line in logs.output))
        self.env.invalidate_all()
        self.assertEqual(doc_both.sgi_replaced_by_process_id, self.p1, "No se sobrescribe.")
        self.assertFalse(doc_only.sgi_replaced_by_process_id, "No se copia del proceso (L-003).")
        backup = self.env['ir.attachment'].search([
            ('res_model', '=', 'sgi.process'), ('res_id', '=', self.p2.id),
            ('name', '=', 'procedimientos_sustituidos_56.31.0_XRP2.json')])
        self.assertEqual(len(backup), 1, "El respaldo no se duplica al correr dos veces.")
        rows = json.loads(base64.b64decode(backup.datas))
        self.assertEqual({r['document_id'] for r in rows}, {doc_both.id, doc_only.id})

    # P-L9 (L-005 / C-003)
    def test_l9_estados_de_migracion_idempotente(self):
        replaced = self._doc('P-A94')
        replaced.sgi_replaced_by_process_id = self.p1
        pending = self._doc('P-A95')
        control = self._doc('P-A96')
        apart = self._doc('P-A97')
        manual = self._doc('P-A98')
        legacy = replaced | pending | control | apart
        mine = legacy | manual
        # Así estaban en producción (antes de la regla que ya no deja
        # escribir «migrado» en un procedimiento): por SQL. Los demás
        # procedimientos de la base no cuentan en esta prueba.
        self.env.flush_all()
        self.env.cr.execute("""
            UPDATE documents_document SET sgi_migration_state = 'baja'
             WHERE sgi_migration_state = 'migrado' AND id NOT IN %s""", (tuple(mine.ids),))
        self.env.cr.execute("""
            UPDATE documents_document SET sgi_migration_state = 'migrado'
             WHERE id IN %s""", (tuple(legacy.ids),))
        self.env.invalidate_all()
        result = self.Doc._sgi_migrate_procedure_states(
            na_codes=('P-A96',), skip_codes=('P-A97',))
        self.assertEqual(result, {'en_curso': 2, 'na': 1})
        self.assertEqual(replaced.sgi_migration_state, 'en_curso')
        self.assertEqual(pending.sgi_migration_state, 'en_curso')
        self.assertEqual(control.sgi_migration_state, 'na')
        self.assertEqual(apart.sgi_migration_state, 'migrado', "P-I01 se retira aparte.")
        self.assertEqual(manual.sgi_migration_state, 'pendiente', "No pisa lo que MAST cambió.")
        self.assertEqual(self.Doc._sgi_migrate_procedure_states(
            na_codes=('P-A96',), skip_codes=('P-A97',)), {})
