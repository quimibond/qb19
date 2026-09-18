# -*- coding: utf-8 -*-
"""El centinela: la huella se toma al crear, un cambio de fórmula se detecta
una sola vez, aceptar la huella lo limpia, el ruido del editor no cuenta, la
siembra es idempotente y el aviso llega a los responsables de nómina.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger


class TestRuleSentinel(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        stype = cls.env['hr.payroll.structure.type'].create({'name': 'QB prueba'})
        cls.struct = cls.env['hr.payroll.structure'].create({
            'name': 'QB prueba', 'code': 'QB_TEST', 'type_id': stype.id,
        })
        cls.rule = cls.env['hr.salary.rule'].create({
            'name': 'Regla de prueba', 'code': 'QB_TEST_RULE', 'struct_id': cls.struct.id,
            'category_id': cls.env.ref('hr_payroll.BASIC').id,
            'condition_select': 'none',
            'amount_select': 'code', 'amount_python_compute': 'result = 1',
        })
        cls.Sentinel = cls.env['qb.nomina.rule.sentinel']

    def test_huella_al_crear(self):
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.assertTrue(s.fingerprint_accepted)
        self.assertEqual(s.fingerprint_accepted, s.fingerprint_current)
        self.assertFalse(s.changed)
        self.assertTrue(s.accepted_at)

    @mute_logger('odoo.addons.quimibond_nomina.models.rule_sentinel')
    def test_detecta_cambio_una_vez_y_aceptar_limpia(self):
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.rule.amount_python_compute = 'result = 2'
        self.assertEqual(s._check(), s)
        self.assertTrue(s.changed)
        self.assertTrue(s.changed_at)
        # Ya marcado: no se vuelve a avisar.
        self.assertFalse(s._check())
        self.assertTrue(s.changed)
        s.action_accept()
        self.assertFalse(s.changed)
        self.assertFalse(s.changed_at)
        self.assertEqual(s.fingerprint_accepted, s.fingerprint_current)

    @mute_logger('odoo.addons.quimibond_nomina.models.rule_sentinel')
    def test_condicion_tambien_cuenta(self):
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.rule.write({'condition_select': 'python', 'condition_python': 'result = False'})
        self.assertTrue(s._check())
        self.assertTrue(s.changed)

    def test_ruido_del_editor_no_es_cambio(self):
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.rule.amount_python_compute = 'result = 1\r\n   \n'
        self.assertFalse(s._check())
        self.assertFalse(s.changed)

    @mute_logger('odoo.addons.quimibond_nomina.models.rule_sentinel')
    def test_volver_a_la_linea_base_desmarca(self):
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.rule.amount_python_compute = 'result = 2'
        s._check()
        self.rule.amount_python_compute = 'result = 1'
        self.assertFalse(s._check())
        self.assertFalse(s.changed)

    def test_siembra_idempotente(self):
        creados = self.Sentinel._seed(codes=('QB_TEST_RULE',), struct_code='QB_TEST')
        self.assertEqual(creados.rule_id, self.rule)
        self.assertFalse(self.Sentinel._seed(codes=('QB_TEST_RULE',), struct_code='QB_TEST'))
        self.assertEqual(self.Sentinel.search_count([('rule_id', '=', self.rule.id)]), 1)

    @mute_logger('odoo.addons.quimibond_nomina.models.rule_sentinel')
    def test_aviso_a_responsables_de_nomina(self):
        Users = self.env['res.users']
        groups_field = 'group_ids' if 'group_ids' in Users._fields else 'groups_id'
        manager = Users.create({
            'name': 'Nómina prueba', 'login': 'qb_nomina_test', 'email': 'nomina.prueba@example.com',
            groups_field: [(4, self.env.ref('hr_payroll.group_hr_payroll_manager').id)],
        })
        s = self.Sentinel.create({'rule_id': self.rule.id})
        self.rule.amount_python_compute = 'result = 3'
        antes = self.env['mail.mail'].search_count([])
        self.Sentinel._cron_check()
        mails = self.env['mail.mail'].search([], order='id desc', limit=1)
        self.assertEqual(self.env['mail.mail'].search_count([]), antes + 1)
        self.assertIn(manager.email, mails.email_to)
        self.assertIn('QB_TEST_RULE', mails.subject)
        self.assertTrue(s.changed)
