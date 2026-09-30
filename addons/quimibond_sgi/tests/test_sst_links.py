# -*- coding: utf-8 -*-
"""57.54.0 — liga de las actividades críticas de seguridad, salud y ambiente
a su pantalla y formato (post-migrate): escribe solo lo vacío, respeta lo que
ya estaba y es idempotente."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

from ..models.sgi_sst_links import SGI_SST_ACTIVITY_LINKS
from .common_documents import sgi_hide_real_documents

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@tagged('post_install', '-at_install')
class TestSstLinks(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        job = cls.env['hr.job'].create({'name': 'ZS Jefe MAST ligas'})
        cls.process = cls.env['sgi.process'].create({'code': 'XSL', 'name': 'Ligas SST prueba'})
        Activity = cls.env['sgi.process.activity']
        cls.acts = Activity.browse()
        for step in (1, 2, 3):
            cls.acts |= Activity.create({
                'process_id': cls.process.id, 'step': step, 'name': 'Actividad %d' % step,
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        cls.doc = cls.env['documents.document'].create({
            'name': 'F-P-S02-01 prueba.xlsx', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formulario_odoo', 'sgi_code': 'F-P-S02-01', 'sgi_revision': 1,
            'sgi_state': 'vigente'})
        # 57.67.0: el numeral lleva el paso a dos dígitos desde 29.2.0
        # («%s.%02d», como E2.18 en la tabla real).
        cls.table = (
            ('XSL.01', 'quimibond_sgi.menu_sgi_env_aspects', "SGI → Aspectos", ('F-P-S02-01',)),
            ('XSL.02', 'quimibond_sgi.menu_sgi_loto', "SGI → Bloqueo", ('F-P-S02-01', 'F-P-S02-98')),
            ('XSL.03', 'quimibond_sgi.menu_sgi_work_permits', "SGI → Permisos", ()),
            ('XSL.99', 'quimibond_sgi.menu_sgi_loto', "No existe", ()),
        )

    def test_01_escribe_solo_lo_vacio_e_idempotente(self):
        act1, act2, act3 = self.acts.sorted('step')
        self.assertEqual(act1.number, 'XSL.01')
        incidents = self.env.ref('quimibond_sgi.menu_sgi_incidents')
        act3.write({'odoo_menu_id': incidents.id, 'odoo_ref': 'Lo capturó MAST'})
        Activity = self.env['sgi.process.activity']
        written = Activity._sgi_link_activity_screens(self.table, company=self.process.company_id)
        self.assertEqual(set(written), {'XSL.01', 'XSL.02'})
        self.assertEqual(act1.odoo_menu_id, self.env.ref('quimibond_sgi.menu_sgi_env_aspects'))
        self.assertEqual(act1.odoo_ref, "SGI → Aspectos")
        self.assertEqual(act1.format_document_ids, self.doc)
        self.assertEqual(act2.odoo_menu_id, self.env.ref('quimibond_sgi.menu_sgi_loto'))
        self.assertEqual(act2.format_document_ids, self.doc, "La clave que no existe se salta.")
        # Lo que MAST ya había capturado se respeta.
        self.assertEqual(act3.odoo_menu_id, incidents)
        self.assertEqual(act3.odoo_ref, 'Lo capturó MAST')
        # Segunda corrida: nada que escribir, nada cambia.
        again = Activity._sgi_link_activity_screens(self.table, company=self.process.company_id)
        self.assertEqual(again, {})
        self.assertEqual(act1.odoo_ref, "SGI → Aspectos")

    def test_02_tabla_real_apunta_a_menus_existentes(self):
        numbers = [row[0] for row in SGI_SST_ACTIVITY_LINKS]
        self.assertEqual(len(numbers), len(set(numbers)), "Numeral repetido en la tabla.")
        for number, menu_xmlid, where, _codes in SGI_SST_ACTIVITY_LINKS:
            self.assertTrue(self.env.ref(menu_xmlid, raise_if_not_found=False),
                            "%s: no existe el menú %s." % (number, menu_xmlid))
            self.assertTrue(where)
        for number in ('E2.23', 'E2.28', 'E2.30', 'E2.34', 'E2.35', 'E2.37', 'S5.14', 'S4.34'):
            self.assertIn(number, numbers)
        # Mismo formato que el numeral calculado (paso a dos dígitos o más).
        for number in numbers:
            self.assertRegex(number, r'^[A-Z0-9]+\.\d{2,}$')

    def test_03_post_migrate_corre(self):
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.54.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_54_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.migrate(self.env.cr, '19.0.57.53.0')
        # Idempotente: una segunda corrida no escribe nada.
        self.assertEqual(self.env['sgi.process.activity']._sgi_link_activity_screens(), {})
