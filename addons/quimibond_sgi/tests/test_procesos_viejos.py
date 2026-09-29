# -*- coding: utf-8 -*-
"""57.4.0 (A-002, B-002): el mapa viejo sale del módulo sin borrar nada.

- La pre-migración pasa los XML IDs de procesos y flujos a ``__export__``
  con el prefijo ``quimibond_sgi_legado_``, archiva lo que siguiera activo y
  es idempotente.
- La post-migración respalda ``sgi.process.responsibility`` en CSV y no borra
  las filas.
- Ningún proceso ni flujo lleva ya XML ID de ``quimibond_sgi``: el SGI se
  instala sin procesos (decisión 6).
"""
import base64
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_migration(version, name):
    path = os.path.join(_MODULE_DIR, 'migrations', version, name)
    spec = importlib.util.spec_from_file_location(
        'sgi_mig_%s_%s' % (version.replace('.', '_'), name[:3]), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged('post_install', '-at_install')
class TestProcesosViejos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Process = cls.env['sgi.process']
        cls.p_a = Process.create({'code': 'XPM-A', 'name': 'Proceso XPM A'})
        cls.p_b = Process.create({'code': 'XPM-B', 'name': 'Proceso XPM B'})
        cls.flow = cls.env['sgi.process.flow'].create({
            'name': 'Flujo XPM', 'from_process_id': cls.p_a.id,
            'to_process_id': cls.p_b.id})
        cls.IMD = cls.env['ir.model.data'].sudo()
        cls.IMD.create([
            {'module': 'quimibond_sgi', 'name': 'proc_xpm_prueba', 'model': 'sgi.process',
             'res_id': cls.p_a.id, 'noupdate': True},
            {'module': 'quimibond_sgi', 'name': 'flow_xpm_prueba',
             'model': 'sgi.process.flow', 'res_id': cls.flow.id, 'noupdate': True},
        ])

    def _xmlids(self, module):
        return self.IMD.search([('module', '=', module),
                                ('model', 'in', ('sgi.process', 'sgi.process.flow')),
                                ('res_id', 'in', (self.p_a | self.p_b).ids + self.flow.ids)])

    def test_01_pre_migrate_moves_and_archives(self):
        pre = _load_migration('19.0.57.4.0', 'pre-migrate.py')
        pre.migrate(self.env.cr, '19.0.57.3.0')
        self.env.invalidate_all()
        self.assertFalse(self.IMD.search([('module', '=', 'quimibond_sgi'),
                                          ('model', 'in', ('sgi.process', 'sgi.process.flow'))]))
        names = set(self._xmlids('__export__').mapped('name'))
        self.assertIn('quimibond_sgi_legado_proc_xpm_prueba', names)
        self.assertIn('quimibond_sgi_legado_flow_xpm_prueba', names)
        # Se archiva lo que tenía XML ID viejo; nunca se borra.
        self.assertTrue(self.p_a.exists())
        self.assertFalse(self.p_a.active)
        self.assertFalse(self.flow.active)
        self.assertTrue(self.p_b.active, "Un proceso sin XML ID viejo no se toca.")
        # Idempotente.
        pre.migrate(self.env.cr, '19.0.57.3.0')
        self.assertEqual(len(self._xmlids('__export__')), 2)

    def test_02_post_migrate_backs_up_without_deleting(self):
        job = self.env['hr.job'].create({'name': 'Puesto XPM'})
        resp = self.env['sgi.process.responsibility'].create({
            'process_id': self.p_a.id, 'job_id': job.id, 'name': 'Rol XPM',
            'responsibilities': 'Hace algo'})
        self.env.flush_all()
        post = _load_migration('19.0.57.4.0', 'post-migrate.py')
        post.migrate(self.env.cr, '19.0.57.3.0')
        post.migrate(self.env.cr, '19.0.57.3.0')
        att = self.env['ir.attachment'].search([
            ('res_model', '=', 'sgi.process'), ('res_id', '=', self.p_a.id),
            ('name', '=', 'responsabilidades_respaldo_57.4.0_XPM-A.csv')])
        self.assertEqual(len(att), 1, "Un solo respaldo aunque corra dos veces.")
        text = base64.b64decode(att.datas).decode('utf-8')
        self.assertIn('Rol XPM', text)
        self.assertIn('Puesto XPM', text)
        self.assertTrue(resp.exists(), "La fila se queda.")

    def test_03_module_ships_no_processes(self):
        """Los datos del módulo no traen procesos ni flujos."""
        from odoo.tools.misc import file_open
        with file_open('quimibond_sgi/__manifest__.py') as handle:
            manifest = handle.read()
        self.assertNotIn("'data/sgi_process_data.xml'", manifest)
        self.assertNotIn("'data/sgi_process_flows_extra.xml'", manifest)
