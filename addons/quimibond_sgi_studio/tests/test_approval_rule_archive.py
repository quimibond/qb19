# -*- coding: utf-8 -*-
"""quimibond_sgi 56.27.0 (aquí desde 57.9.0): archivar una regla de aprobación de Studio cierra sus avisos
abiertos (actividad hecha con nota), sin aprobar ni rechazar nada."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestApprovalRuleArchive(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.approver = new_test_user(cls.env, login='arch_rule_boss',
                                     groups='base.group_user,purchase.group_purchase_user')
        partner = cls.env['res.partner'].create({'name': 'Proveedor regla archivada'})
        cls.order = cls.env['purchase.order'].create({'partner_id': partner.id})
        # Mismo patrón que test_approval_native.test_04 (regla hecha a mano en Studio).
        cls.rule = cls.env['studio.approval.rule'].sudo().create({
            'name': 'Regla archivada 1d', 'model_id': cls.env['ir.model']._get('purchase.order').id,
            'method': 'button_confirm', 'approver_ids': [(6, 0, cls.approver.ids)]})
        # La solicitud se arma a mano como la deja Studio: una actividad sobre el
        # documento para el aprobador y la solicitud que la liga a la regla.
        cls.activity = cls.order.activity_schedule(
            'mail.mail_activity_data_todo', summary='Conceder aprobación', user_id=cls.approver.id)
        cls.request = cls.env['studio.approval.request'].sudo().create({
            'rule_id': cls.rule.id, 'res_id': cls.order.id, 'mail_activity_id': cls.activity.id})

    def _entries(self):
        return self.env['studio.approval.entry'].sudo().search_count([('rule_id', '=', self.rule.id)])

    def test_01_archivar_cierra_el_aviso_sin_decidir(self):
        entries_before = self._entries()
        self.rule.action_archive()
        self.assertFalse(self.rule.active)
        activity = self.activity.with_context(active_test=False).exists()
        self.assertTrue(activity, "La actividad queda como historia (archivada), no se borra.")
        self.assertFalse(activity.active, "La actividad ya no está pendiente.")
        self.assertIn('Regla de aprobación archivada', activity.feedback or '')
        self.assertFalse(self.order.activity_ids.filtered(lambda a: a.user_id == self.approver))
        messages = self.order.message_ids.filtered(
            lambda m: 'Regla de aprobación archivada' in (m.body or ''))
        self.assertEqual(len(messages), 1, "Una sola nota en el chatter del documento.")
        self.assertEqual(self._entries(), entries_before, "No se aprueba ni se rechaza nada.")
        self.assertTrue(self.request.exists(), "Nada se borra: la solicitud se queda.")
        self.assertEqual(self.request.mail_activity_id, activity,
                         "La solicitud sigue ligada a su actividad, ya archivada.")
        # Reactivar no recrea el aviso.
        self.rule.action_unarchive()
        self.assertTrue(self.rule.active)
        self.assertFalse(self.order.activity_ids.filtered(lambda a: a.user_id == self.approver),
                         "Reactivar no crea actividades nuevas en el documento.")
        self.assertEqual(self._entries(), entries_before)

    def test_02_otra_escritura_no_toca_los_avisos(self):
        self.rule.name = 'Regla archivada 1d (renombrada)'
        self.assertTrue(self.activity.active)
        self.assertTrue(self.request.exists())

    def test_03_actividad_fuera_del_documento_deja_nota_aparte(self):
        """Si el aviso no está sobre el documento, la nota va aparte a su chatter."""
        partner = self.order.partner_id
        other = partner.activity_schedule('mail.mail_activity_data_todo', user_id=self.approver.id)
        self.request.mail_activity_id = other
        self.activity.unlink()
        self.rule.action_archive()
        self.assertFalse(other.with_context(active_test=False).active)
        self.assertTrue(self.order.message_ids.filtered(
            lambda m: 'Regla de aprobación archivada' in (m.body or '')))
