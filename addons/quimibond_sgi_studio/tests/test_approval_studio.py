# -*- coding: utf-8 -*-
"""El rol «Aprueba» de tipo «Botón de Odoo» con la regla de aprobación de
Studio. Pruebas mudadas de quimibond_sgi/tests/test_approval_native.py
(test_02 a test_04) en 57.9.0 (A-010, J-018); test_05 cubre la aserción de
«una solicitud no bloquea ningún botón» que allá leía approval_rule_id y
test_06 la de Mis pendientes (antes quimibond_sgi test_bandeja.test_07)."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestApprovalStudio(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_buyer = cls.env['hr.job'].create({'name': 'COMPRADOR APROB MP'})
        cls.job_boss = cls.env['hr.job'].create({'name': 'DIRECTOR APROB MP'})
        cls.boss_user = new_test_user(cls.env, login='apr_boss', groups='base.group_user,purchase.group_purchase_user')
        cls.env['hr.employee'].create({'name': 'Director Aprob', 'job_id': cls.job_boss.id,
                                       'user_id': cls.boss_user.id})
        process = cls.env['sgi.process'].create({'code': 'ZAPR', 'name': 'Proceso Aprobaciones MP'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Crear la orden de compra', 'number': '9.1',
            'measure_model_id': cls.env['ir.model']._get('purchase.order').id,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_buyer.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job_boss.id,
                                 'condition': 'si la orden rebasa el monto fijado'})]})
        cls.role = cls.activity.role_ids.filtered(lambda r: r.role == 'aprueba')

    def test_02_sincroniza_regla_nativa(self):
        self.role.write({
            'condition_field_id': self.env['ir.model.fields']._get('purchase.order', 'amount_total').id,
            'condition_operator': '>', 'condition_value': '50000'})
        self.role.action_sgi_sync_approval()
        rule = self.role.approval_rule_id
        self.assertTrue(rule.active)
        self.assertEqual((rule.model_id.model, rule.method), ('purchase.order', 'button_confirm'))
        self.assertEqual(rule.domain, "[('amount_total', '>', 50000.0)]")
        self.assertEqual(rule.approver_ids, self.boss_user)
        self.assertEqual(rule.sgi_role_id, self.role)
        self.assertEqual(self.role.approval_state, 'activa')
        # Otra persona entra al puesto: la regla queda por sincronizar y el cron la pone al día.
        other = new_test_user(self.env, login='apr_boss2', groups='base.group_user')
        self.env['hr.employee'].create({'name': 'Director Aprob 2', 'job_id': self.job_boss.id,
                                        'user_id': other.id})
        self.role.invalidate_recordset(['approval_state', 'approval_user_ids'])
        self.assertEqual(self.role.approval_state, 'por_sincronizar')
        self.env['sgi.activity.role'].cron_sgi_sync_approvals()
        self.assertEqual(rule.approver_ids, self.boss_user | other)
        # Si la actividad se archiva, la regla también.
        self.activity.active = False
        self.assertFalse(rule.active)

    def test_03_boton_inexistente_y_sin_personas(self):
        self.role.approval_method = 'boton_que_no_existe'
        with self.assertRaises(UserError):
            self.role.action_sgi_sync_approval()
        self.role.write({'approval_method': 'button_confirm', 'job_id': self.job_buyer.id})
        self.assertEqual(self.role.approval_state, 'sin_aprobadores', "El puesto no tiene personas con usuario.")
        self.role.action_sgi_sync_approval()
        self.assertFalse(self.role.approval_rule_id, "Sin aprobadores no se crea una regla que nadie puede aprobar.")

    def test_04_regla_manual_en_el_boton_se_adopta(self):
        """56.6.0: si ya hay una regla hecha a mano en el mismo botón, no se
        crea otra (el documento pediría dos aprobaciones): se adopta."""
        manual = self.env['studio.approval.rule'].sudo().create({
            'name': 'Regla a mano MP', 'model_id': self.env['ir.model']._get('purchase.order').id,
            'method': 'button_confirm', 'approver_ids': [(6, 0, self.boss_user.ids)]})
        self.role.invalidate_recordset()
        self.assertEqual(self.role.approval_conflict_rule_ids, manual)
        self.assertEqual(self.role.approval_state, 'conflicto')
        with self.assertRaises(UserError):
            self.role.action_sgi_sync_approval()
        self.role.action_sgi_adopt_rule()
        self.assertEqual(self.role.approval_rule_id, manual)
        self.assertEqual(manual.sgi_role_id, self.role)
        self.role.invalidate_recordset()
        self.assertFalse(self.role.approval_conflict_rule_ids)
        self.assertEqual(self.role.approval_state, 'activa')

    def test_05_solicitud_no_bloquea_ningun_boton(self):
        self.role.approval_kind = 'solicitud'
        self.role.action_sgi_sync_approval()
        self.assertTrue(self.role.approval_category_id)
        self.assertEqual(self.role.approval_state, 'activa')
        self.assertFalse(self.role.approval_rule_id, "Una solicitud no bloquea ningún botón.")

    def test_06_aprobacion_de_studio_en_mis_pendientes(self):
        """Antes quimibond_sgi/tests/test_bandeja.py test_07 (con skip si no
        había Studio); aquí Studio siempre está."""
        approver = new_test_user(self.env, login='zs_8a_apr', groups='base.group_user,purchase.group_purchase_user')
        partner = self.env['res.partner'].create({'name': 'Proveedor 8A'})
        order = self.env['purchase.order'].create({'partner_id': partner.id})
        rule = self.env['studio.approval.rule'].sudo().create({
            'name': 'Regla 8A', 'model_id': self.env['ir.model']._get('purchase.order').id,
            'method': 'button_confirm', 'approver_ids': [(6, 0, approver.ids)]})
        activity = order.activity_schedule('mail.mail_activity_data_todo',
                                           summary='Conceder aprobación', user_id=approver.id)
        self.env['studio.approval.request'].sudo().create({
            'rule_id': rule.id, 'res_id': order.id, 'mail_activity_id': activity.id})
        rows = self.env['sgi.my.pending']._sgi_pending_values(approver)[approver.id]
        self.assertTrue([r for r in rows if r['kind'] == 'aprobacion' and r['res_model'] == 'purchase.order'
                         and r['res_id'] == order.id])

    def test_07_nunca_una_condicion_invalida_en_la_regla(self):
        """1.0.3: la regla de Studio nunca guarda un campo que su documento no
        tiene (producción 2026-09-30, reglas 65 a 67), y la migración limpia
        solo las hojas inválidas."""
        program_model = self.env['ir.model']._get('sgi.audit.program')
        self.role.write({'approval_model_id': program_model.id, 'approval_method': 'action_approve'})
        # Aunque el rol trajera la condición (se escribe sin pasar por write()),
        # la sincronización no la lleva a la regla.
        self.env.cr.execute("UPDATE sgi_activity_role SET approval_domain = %s WHERE id = %s",
                            ("[('company_id', '=', 1)]", self.role.id))
        self.role.invalidate_recordset(['approval_domain'])
        self.role.action_sgi_sync_approval()
        rule = self.role.approval_rule_id
        self.assertTrue(rule.active)
        self.assertFalse(rule.domain)
        # Escrita directo en la regla (Studio, MCP), tampoco se queda.
        rule.write({'domain': "[('company_id', '=', 1)]"})
        self.assertFalse(rule.domain)
        # La migración: una regla vieja con una hoja inválida y otra válida
        # pierde solo la inválida; una regla válida no cambia.
        Rule = self.env['studio.approval.rule']
        bad = Rule.create({'model_id': self.env['ir.model']._get('purchase.order').id, 'method': 'button_cancel',
                           'name': 'Prueba limpia'})
        self.env.cr.execute("UPDATE studio_approval_rule SET domain = %s WHERE id = %s",
                            ("[('no_existe', '=', 1), ('amount_total', '>', 10.0)]", bad.id))
        good = Rule.create({'model_id': self.env['ir.model']._get('purchase.order').id, 'method': 'button_draft',
                            'name': 'Prueba válida', 'domain': "[('company_id', '=', 1)]"})
        (bad | good).invalidate_recordset(['domain'])
        changes = (bad | good)._sgi_sanitize_domains()
        self.assertEqual([c[0] for c in changes], [bad])
        self.assertEqual(bad.domain, "[('amount_total', '>', 10.0)]")
        self.assertEqual(good.domain, "[('company_id', '=', 1)]")
        self.assertFalse((bad | good)._sgi_sanitize_domains(), "Idempotente.")


@tagged('post_install', '-at_install')
class TestApprovalStudioRequester(TransactionCase):
    """1.0.6: nadie aprueba lo que él mismo pidió, también en el botón de
    Studio (la entrada de aprobación no se crea; el suplente sí puede)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_design = cls.env['hr.job'].create({'name': 'DISEÑO APROB REQ'})
        cls.job_sub = cls.env['hr.job'].create({'name': 'SUPLENTE APROB REQ'})
        cls.u_design = new_test_user(cls.env, login='req_design', groups='base.group_user')
        cls.u_sub = new_test_user(cls.env, login='req_sub', groups='base.group_user')
        cls.env['hr.employee'].create([
            {'name': 'Diseño Req', 'job_id': cls.job_design.id, 'user_id': cls.u_design.id},
            {'name': 'Suplente Req', 'job_id': cls.job_sub.id, 'user_id': cls.u_sub.id}])
        process = cls.env['sgi.process'].create({'code': 'ZREQ', 'name': 'Proceso requester'})
        job_exec = cls.env['hr.job'].create({'name': 'EJECUTOR APROB REQ'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Firmar la solicitud de modificación', 'number': '9.3',
            'measure_model_id': cls.env['ir.model']._get('sgi.dev.change.request').id,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job_exec.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job_design.id,
                                 'approval_method': 'action_approve'})]})
        cls.role = cls.activity.role_ids.filtered(lambda r: r.role == 'aprueba')
        cls.role.action_sgi_sync_approval()
        cls.rule = cls.role.approval_rule_id
        project = cls.env['project.project'].create({'name': 'Desarrollo requester', 'sgi_is_ft': True})
        cls.request = cls.env['sgi.dev.change.request'].create({
            'project_id': project.id, 'description': 'Cambio pedido por Diseño',
            'requested_by_id': cls.u_design.id})

    def _entry(self, user):
        return self.env['studio.approval.entry'].sudo().create({
            'rule_id': self.rule.id, 'res_id': self.request.id, 'user_id': user.id, 'approved': True})

    def test_01_quien_pidio_no_aprueba_y_el_suplente_si(self):
        self.assertEqual(self.request.requested_by_id, self.u_design)
        self.assertEqual(self.rule.approver_ids, self.u_design)
        with self.assertRaises(UserError) as cm:
            self._entry(self.u_design)
        self.assertIn("suplente", str(cm.exception))
        # Con suplente nombrado la regla lo incluye y él sí aprueba.
        self.role.substitute_job_id = self.job_sub
        self.env['sgi.activity.role'].cron_sgi_sync_approvals()
        self.assertEqual(self.rule.approver_ids, self.u_design | self.u_sub)
        with self.assertRaises(UserError) as cm:
            self._entry(self.u_design)
        self.assertIn('Suplente Req', str(cm.exception))
        entry = self._entry(self.u_sub)
        self.assertTrue(entry.approved)
        # Lo que pide otro, Diseño sí lo aprueba.
        other = self.env['sgi.dev.change.request'].create({
            'project_id': self.request.project_id.id, 'description': 'Cambio pedido por el suplente',
            'requested_by_id': self.u_sub.id})
        entry = self.env['studio.approval.entry'].sudo().create({
            'rule_id': self.rule.id, 'res_id': other.id, 'user_id': self.u_design.id, 'approved': True})
        self.assertTrue(entry.approved)
