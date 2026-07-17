# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import ValidationError, UserError


@tagged('post_install', '-at_install')
class TestProcessMap(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.p_ventas = cls.env.ref('quimibond_sgi.proc_ventas')
        cls.p_plan = cls.env.ref('quimibond_sgi.proc_planeacion')

    def test_01_flow_from_equals_to(self):
        with self.assertRaises(ValidationError):
            self.env['sgi.process.flow'].create({
                'name': 'Ciclo inválido',
                'from_process_id': self.p_ventas.id,
                'to_process_id': self.p_ventas.id,
            })

    def test_02_flow_valid(self):
        flow = self.env['sgi.process.flow'].create({
            'name': 'Entregable válido',
            'from_process_id': self.p_ventas.id,
            'to_process_id': self.p_plan.id,
        })
        self.assertTrue(flow.id)

    def test_03_no_parent_recursion(self):
        # La recursión se bloquea (ValidationError propia o UserError nativo de _parent_store)
        with self.assertRaises(UserError):
            self.p_ventas.parent_id = self.p_ventas.id
