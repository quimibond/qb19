# -*- coding: utf-8 -*-
"""El mapa de producción (data/mapa.json) y su carga manual.

- El archivo trae los conteos de producción del 2026-09-29 (14 procesos, 60
  etapas, 310 actividades…), numerales únicos, nada sensible y ninguna llave
  que el cargador no entienda.
- Solo un Administrador SGI usa el asistente; la carga real exige probar
  antes el mismo archivo.
- En una base sin el mapa (la de pruebas), con los puestos dados de alta,
  el modo de prueba no da errores; la carga deja 14 procesos, 60 etapas y
  310 actividades, y cargar otra vez no crea nada.
"""
import json
from collections import Counter

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools import file_open

from odoo.addons.quimibond_sgi.models.sgi_load import _KEYS_PAYLOAD, _unknown_keys

from ..models.sgi_mapa_wizard import MAPA_PATH


def _mapa():
    with file_open(MAPA_PATH, 'rb') as handle:
        return json.loads(handle.read().decode('utf-8'))


@tagged('post_install', '-at_install')
class TestMapaFile(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mapa = _mapa()
        cls.activities = [a for p in cls.mapa['processes'] for a in p['activities']]

    def test_01_conteos_de_produccion(self):
        counts = self.mapa['meta']['counts']
        stages = {(p['code'], a.get('stage')) for p in self.mapa['processes']
                  for a in p['activities']}
        self.assertEqual(len(self.mapa['processes']), 14)
        self.assertEqual(len(stages), 60)
        self.assertEqual(len(self.activities), 310)
        self.assertEqual(sum(len(a['roles']) for a in self.activities), 1072)
        self.assertEqual(sum(len(a['inputs']) for a in self.activities), 246)
        self.assertEqual(len(self.mapa['deliverables']), 319)
        self.assertEqual(len(self.mapa['families']), 14)
        self.assertEqual(len(self.mapa['objectives']), 10)
        self.assertEqual(len(self.mapa['control_plans']), 10)
        self.assertEqual(sum(len(i['terms']) for i in self.mapa['indicators']), 50)
        self.assertEqual((counts['processes'], counts['stages'], counts['activities']),
                         (14, 60, 310), "meta.counts coincide con el contenido.")

    def test_02_el_cargador_entiende_todo(self):
        self.assertEqual(_unknown_keys(self.mapa, _KEYS_PAYLOAD), [])
        self.assertTrue(self.mapa['tolerant'], "El mapa se carga en bases distintas de producción.")

    def test_03_numerales_congelados_y_roles(self):
        for process in self.mapa['processes']:
            numbers = Counter(a['number'] for a in process['activities'])
            self.assertEqual(max(numbers.values()), 1, "%s repite numerales" % process['code'])
            for act in process['activities']:
                self.assertTrue(act['number'].startswith(process['code'] + '.'), act['number'])
                roles = Counter(r['role'] for r in act['roles'])
                self.assertEqual(roles['ejecuta'], 1, "%s: un solo «Ejecuta»" % act['number'])
                for role in act['roles']:
                    self.assertEqual(len({'job', 'family', 'relative'} & set(role)), 1)
        codes = {d['code'] for d in self.mapa['deliverables']}
        for act in self.activities:
            self.assertLessEqual(set(act['outputs']), codes, act['number'])
            self.assertLessEqual({i['code'] for i in act['inputs']}, codes, act['number'])

    def test_04_sin_ids_ni_datos_sensibles(self):
        text = json.dumps(self.mapa, ensure_ascii=False).lower()
        for word in ('password', 'contraseña', 'api_key', 'access_token'):
            self.assertNotIn(word, text)
        for act in self.activities:
            for code in [act.get('instruction'), act.get('related_procedure')] + act.get('formats', []):
                self.assertNotEqual(code, 'P-I01', "P-I01 no viaja en el mapa.")
            self.assertNotIn('owner_employee_id', act)
        for process in self.mapa['processes']:
            self.assertNotIn('owner_employee_id', process)
            self.assertNotIn('replaced_documents', process)
        for indicator in self.mapa['indicators']:
            self.assertNotIn('responsible', indicator)
            self.assertNotIn('responsible_employee_id', indicator)
        from odoo.addons.quimibond_sgi.models import sgi_domain_refs
        for deliverable in self.mapa['deliverables']:
            for key in ('domain', 'complete_domain'):
                self.assertEqual(sgi_domain_refs.leaf_ids(deliverable.get(key) or ''), [],
                                 "%s: ningún id fijo en %s" % (deliverable['code'], key))


@tagged('post_install', '-at_install')
class TestMapaLoad(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mapa = _mapa()
        cls.admin = new_test_user(cls.env, login='mapa_admin',
                                  groups='base.group_user,quimibond_sgi.group_sgi_admin')
        cls.manager = new_test_user(cls.env, login='mapa_manager',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        # La base de pruebas no tiene los puestos de producción: el cargador
        # nunca los crea, así que se dan de alta aquí con una persona cada uno.
        names = set()
        for family in cls.mapa['families']:
            names.update(family['jobs'])
        for process in cls.mapa['processes']:
            if process.get('owner_job'):
                names.add(process['owner_job'])
            for act in process['activities']:
                names.update(r['job'] for r in act['roles'] if r.get('job'))
        Job = cls.env['hr.job']
        Employee = cls.env['hr.employee']
        company = cls.env.company
        for name in sorted(names):
            job, error = Job._sgi_find_job(name, company)
            if error:
                job = Job.create({'name': name, 'company_id': company.id})
            if not Employee.search_count([('job_id', '=', job.id)]):
                Employee.create({'name': 'Mapa %s' % name, 'job_id': job.id,
                                 'company_id': company.id})
        cls.codes = [p['code'] for p in cls.mapa['processes']]

    def _wizard(self):
        # Como superusuario: la carga lee puestos y empleados (en producción
        # la corre un administrador SGI con acceso a Empleados).
        return self.env['sgi.mapa.load.wizard'].create({})

    def test_01_solo_administrador(self):
        with self.assertRaises(AccessError):
            self.env['sgi.mapa.load.wizard'].with_user(self.manager).create({})
        with self.assertRaises(AccessError):
            self.env['sgi.process'].with_user(self.manager).export_payload()
        self.assertTrue(self.env['sgi.mapa.load.wizard'].with_user(self.admin).create({}))
        wizard = self._wizard()
        with self.assertRaises(UserError):
            wizard.action_load()

    def test_02_prueba_y_carga(self):
        Process = self.env['sgi.process'].with_context(active_test=False)
        before = Process.search_count([('code', 'in', self.codes)])
        wizard = self._wizard()
        wizard.action_test()
        errors = wizard.line_ids.filtered(lambda l: l.action == 'error')
        self.assertFalse(errors, "\n".join("%s %s: %s" % (l.kind, l.key, l.message)
                                          for l in errors[:20]))
        self.assertEqual(Process.search_count([('code', 'in', self.codes)]), before,
                         "El modo de prueba no escribe.")
        with self.assertRaises(UserError):
            wizard.action_load()        # sin «Entiendo que se escribe»
        wizard.confirm = True
        wizard.action_load()
        errors = wizard.line_ids.filtered(lambda l: l.action == 'error')
        self.assertFalse(errors, "\n".join("%s %s: %s" % (l.kind, l.key, l.message)
                                          for l in errors[:20]))
        processes = Process.search([('code', 'in', self.codes), ('active', '=', True)])
        self.assertEqual(len(processes), 14)
        self.assertEqual(len(processes.stage_ids), 60)
        activities = self.env['sgi.process.activity'].search([('process_id', 'in', processes.ids)])
        self.assertEqual(len(activities), 310)
        # Cargar otra vez no duplica ni crea nada.
        again = self.env['sgi.process'].load_payload(self.mapa)
        self.assertFalse([c for c in again['changes'] if c['action'] == 'created'],
                         [c for c in again['changes'] if c['action'] == 'created'][:10])
        self.assertEqual(Process.search_count([('code', 'in', self.codes)]), 14)
        self.assertEqual(self.env['sgi.process.activity'].search_count(
            [('process_id', 'in', processes.ids)]), 310)
