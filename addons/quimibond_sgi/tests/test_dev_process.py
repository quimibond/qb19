# -*- coding: utf-8 -*-
"""57.123.0: C1.04 partida sin renumerar, aprobaciones ligadas a su botón y escalamiento de segundo nivel."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevProcess(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Activity = cls.env['sgi.process.activity']
        cls.Role = cls.env['sgi.activity.role']
        Process = cls.env['sgi.process']
        Job = cls.env['hr.job']
        Deliverable = cls.env['sgi.deliverable']
        cls.process = Process.search([('code', '=', 'C1')], limit=1) or Process.create({'code': 'C1', 'name': 'C1 prueba'})
        cls.job_product = Job.create({'name': 'DISEÑO Y DESARROLLO DE PRODUCTO (prueba)'})
        cls.job_process = Job.create({'name': 'DISEÑO Y DESARROLLO DE PROCESOS (prueba)'})
        cls.job_director = Job.create({'name': 'DIRECTOR DE OPERACIONES (prueba)'})
        cls.job_sales = Job.create({'name': 'ADMINISTRADOR DE VENTAS (prueba)'})
        bom_model = cls.env['ir.model']._get('mrp.bom')
        cls.bom = Deliverable.search([('code', '=', 'C1-BOM')], limit=1) or Deliverable.create({
            'code': 'C1-BOM', 'name': 'Lista de materiales del artículo nuevo', 'odoo_model_id': bom_model.id})

        def act(step, name, roles, **extra):
            existing = cls.Activity.search([('number', '=', 'C1.%02d' % step), ('process_id', '=', cls.process.id)], limit=1)
            if existing:
                return existing
            return cls.Activity.create(dict({
                'process_id': cls.process.id, 'name': name, 'step': step, 'sequence': step * 10,
                'role_ids': [(0, 0, r) for r in roles]}, **extra))
        cls.a02 = act(2, 'Analizar la muestra (prueba)',
                      [{'role': 'ejecuta', 'job_id': cls.job_product.id},
                       {'role': 'aprueba', 'job_id': cls.job_sales.id}])
        cls.a04 = act(4, 'Dar de alta el artículo de desarrollo con su lista de materiales y ruta preliminar',
                      [{'role': 'ejecuta', 'job_id': cls.job_product.id},
                       {'role': 'participa', 'job_id': cls.job_process.id},
                       {'role': 'escala', 'target_type': 'relative', 'relative_role': 'dueno_proceso', 'after_days': 2}],
                      how_steps="Capturar las claves → usar «Generar artículos» → Diseño de producto arma la lista de "
                                "materiales → Diseño de procesos asigna la ruta y los centros de trabajo.",
                      done_criteria="Los artículos existen con su código según la regla, lista de materiales y ruta antes de costear.",
                      measure_method='entregable', output_deliverable_ids=[(4, cls.bom.id)],
                      measure_deliverable_id=cls.bom.id)
        cls.a05 = act(5, 'Costear el artículo (prueba)', [{'role': 'ejecuta', 'job_id': cls.job_sales.id}],
                      input_ids=[(0, 0, {'deliverable_id': cls.bom.id, 'max_days': 2})])
        cls.a07 = act(7, 'Aprobar la solicitud (prueba)',
                      [{'role': 'ejecuta', 'job_id': cls.job_process.id},
                       {'role': 'aprueba', 'job_id': cls.job_director.id}])
        cls.a11 = act(11, 'Medir la muestra (prueba)',
                      [{'role': 'ejecuta', 'job_id': cls.job_sales.id},
                       {'role': 'aprueba', 'job_id': cls.job_product.id}])
        cls.a19 = act(19, 'Cambios a producto liberado (prueba)',
                      [{'role': 'ejecuta', 'job_id': cls.job_product.id},
                       {'role': 'escala', 'job_id': cls.job_director.id, 'after_days': 5}])

    def test_01_split_c1_04_without_renumbering(self):
        before = self.Activity.search_count([('process_id', '=', self.process.id)])
        new = self.Activity._sgi_dev_split_c1_04()
        self.assertEqual(new.number, 'C1.04b')
        self.assertNotEqual(new.step, 4, "El paso sigue siendo único; el numeral es propio.")
        self.assertEqual(new.sequence, self.a04.sequence + 5, "Se intercala entre C1.04 y C1.05.")
        self.assertEqual(self.a05.number, 'C1.05', "Nadie se renumera.")
        self.assertEqual(new.role_ids.filtered(lambda r: r.role == 'ejecuta').job_id, self.job_process)
        self.assertEqual(new.role_ids.filtered(lambda r: r.role == 'participa').job_id, self.job_product)
        self.assertEqual(new.role_ids.filtered(lambda r: r.role == 'escala').relative_role, 'dueno_proceso')
        self.assertIn(self.bom, new.input_ids.deliverable_id, "C1.04b recibe la lista de materiales.")
        route = new.output_deliverable_ids.filtered(lambda d: d.code == 'C1-RUTA')
        self.assertTrue(route)
        self.assertEqual((route.odoo_model_name, new.measure_deliverable_id), ('mrp.bom', route))
        self.assertEqual(new.measure_model_id.model, 'mrp.bom')
        self.assertEqual(self.a04.name, 'Dar de alta el artículo de desarrollo con su lista de materiales')
        self.assertNotIn('Diseño de procesos asigna', self.a04.how_steps)
        self.assertTrue(self.a04.how_steps.endswith('arma la lista de materiales.'))
        self.assertIn('y lista de materiales antes de costear', self.a04.done_criteria)
        self.assertIn(self.bom, self.a04.output_deliverable_ids, "C1.04 sigue entregando la lista de materiales.")
        route_in = self.a05.input_ids.filtered(lambda i: i.deliverable_id == route)
        self.assertEqual(route_in.max_days, 2, "C1.05 recibe la ruta con el mismo plazo que la lista de materiales.")
        again = self.Activity._sgi_dev_split_c1_04()
        self.assertEqual(again, new, "Idempotente.")
        self.assertEqual(self.Activity.search_count([('process_id', '=', self.process.id)]), before + 1)

    def test_02_approvals_point_to_their_button(self):
        sales_role = self.a02.role_ids.filtered(lambda r: r.role == 'aprueba')
        self.assertEqual(sales_role.approval_state, 'sin_configurar')
        touched = self.Activity._sgi_dev_link_c1_approvals()
        self.assertIn(sales_role, touched)
        self.assertEqual((sales_role.approval_model_id.model, sales_role.approval_method),
                         ('project.project', 'action_sgi_dev_review_approve'))
        director_role = self.a07.role_ids.filtered(lambda r: r.role == 'aprueba')
        self.assertEqual(director_role.approval_method, 'action_sgi_dev_approve_request')
        lab_role = self.a11.role_ids.filtered(lambda r: r.role == 'aprueba')
        self.assertEqual((lab_role.approval_model_id.model, lab_role.approval_method), ('sgi.dev.lab.request', 'action_verdict'))
        self.assertNotEqual(sales_role.approval_state, 'sin_configurar')
        self.assertFalse(self.Activity._sgi_dev_link_c1_approvals(), "Idempotente: ya ligadas no se vuelven a tocar.")

    def test_03_second_level_escalation_follows_the_parameter(self):
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.dev_escalation_director_days', '')
        self.assertFalse(self.Activity._sgi_dev_sync_c1_escalation(), "Parámetro vacío: no se crea nada.")
        self.assertFalse(self.a04.role_ids.filtered(lambda r: r.role == 'escala' and r.job_id == self.job_director))
        Param.set_param('quimibond_sgi.dev_escalation_director_days', '4')
        done = self.Activity._sgi_dev_sync_c1_escalation()
        level2 = self.a04.role_ids.filtered(lambda r: r.role == 'escala' and r.job_id == self.job_director)
        self.assertEqual(level2.after_days, 4)
        self.assertIn(level2, done)
        self.assertTrue(self.a04.role_ids.filtered(lambda r: r.role == 'escala' and r.relative_role == 'dueno_proceso'),
                        "El primer nivel (dueño del proceso) se conserva.")
        self.assertEqual(self.a19.role_ids.filtered(lambda r: r.role == 'escala' and r.job_id == self.job_director).after_days, 4,
                         "El nivel a Dirección que ya existía toma los días del parámetro.")
        self.assertFalse(self.a05.role_ids.filtered(lambda r: r.role == 'escala'),
                         "Donde no ejecuta Diseño y Desarrollo no se agrega nada.")
        self.assertFalse(self.Activity._sgi_dev_sync_c1_escalation(), "Idempotente.")
        settings = self.env['res.config.settings'].create({'sgi_dev_escalation_director_days': 3})
        settings.set_values()
        self.assertEqual(level2.after_days, 3, "Guardar Ajustes vuelve a sincronizar.")

    def test_04_approve_request_and_lab_verdict_buttons(self):
        partner = self.env['res.partner'].create({'name': 'CLIENTE PROCESO PRUEBA', 'is_company': True})
        dev = self.env['project.project'].create({'name': 'x', 'sgi_is_ft': True, 'partner_id': partner.id,
                                                  'sgi_dev_product_name': 'Jersey proceso'})
        self.assertFalse(dev.sgi_dev_approved_by_id)
        dev.action_sgi_dev_approve_request()
        self.assertEqual(dev.sgi_dev_approved_by_id, self.env.user)
        self.assertTrue(dev.sgi_dev_approved_date, "La firma «Aprobó» lleva fecha.")
        request = self.env['sgi.dev.lab.request'].create({'project_id': dev.id, 'line_ids': [(6, 0, dev.sgi_dev_line_ids[:2].ids)]})
        with self.assertRaises(Exception):
            request.action_verdict()
        request.write({'state': 'medida'})
        request.line_ids.write({'sample_value': 1.0})
        request.action_verdict()
        self.assertEqual(request.verdict_by_id, self.env.user)
        self.assertTrue(request.date_verdict)
