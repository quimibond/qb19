# -*- coding: utf-8 -*-
"""57.51.0 — permiso de trabajo de alto riesgo (ISO 45001 8.1, E2.18): flujo
borrador → solicitado → autorizado → cerrado, dos autorizaciones de personas
distintas, verificaciones que bloquean y candado de evidencia."""
from datetime import datetime, timedelta

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged

from .common_users import assert_locked, sgi_test_user


@tagged('post_install', '-at_install')
class TestWorkPermit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.requester = sgi_test_user(cls.env, 'zs_ptar_solicita', 'quimibond_sgi.group_sgi_user')
        cls.boss = sgi_test_user(cls.env, 'zs_ptar_jefe', 'quimibond_sgi.group_sgi_user')
        cls.mast = sgi_test_user(cls.env, 'zs_ptar_mast', 'quimibond_sgi.group_sgi_manager')
        cls.other = sgi_test_user(cls.env, 'zs_ptar_otro', 'quimibond_sgi.group_sgi_user')
        cls.worker = cls.env['hr.employee'].create({'name': 'ZS Electricista'})

    def _permit(self, **vals):
        start = datetime(2046, 5, 4, 8, 0)
        base = {'name': 'Cambio de interruptor ZS', 'work_type': 'electrico',
                'location': 'Subestación 1', 'date_start': start,
                'date_end': start + timedelta(hours=8), 'area_manager_id': self.boss.id,
                'executor_ids': [(6, 0, self.worker.ids)]}
        base.update(vals)
        return self.env['sgi.work.permit'].with_user(self.requester).create(base)

    def _answer_all(self, permit, answer='si'):
        permit.check_ids.write({'answer': answer})

    def test_01_carga_verificaciones_del_tipo(self):
        permit = self._permit()
        self.assertTrue(permit.folio.startswith('PTAR-'))
        names = permit.check_ids.mapped('name')
        self.assertIn("Ausencia de tensión comprobada con probador", names)
        self.assertIn("Guantes dieléctricos", names)
        self.assertTrue(permit.check_ids.filtered(lambda c: c.category == 'epp'))
        count = len(permit.check_ids)
        permit.action_load_checks()
        self.assertEqual(len(permit.check_ids), count, "No repite verificaciones.")
        permit.work_type = 'alturas'
        permit.action_load_checks()
        self.assertIn("Punto de anclaje verificado", permit.check_ids.mapped('name'))

    def test_02_fechas(self):
        with self.assertRaises(ValidationError):
            self._permit(date_end=datetime(2046, 5, 4, 7, 0))

    def test_03_flujo_completo(self):
        permit = self._permit()
        with self.assertRaises(UserError):
            permit.action_submit()  # sin peligros ni respuestas
        permit.hazards = 'Choque eléctrico, arco'
        with self.assertRaises(UserError):
            permit.action_submit()  # verificaciones sin contestar
        self._answer_all(permit, 'no')
        with self.assertRaises(UserError):
            permit.action_submit()  # verificaciones en «No»
        self._answer_all(permit)
        permit.action_submit()
        self.assertEqual(permit.state, 'solicitado')
        self.assertIn(self.boss, permit.activity_ids.user_id)
        # Ya solicitado, las verificaciones no se tocan.
        with self.assertRaises(UserError):
            permit.check_ids[:1].answer = 'na'
        # Otro usuario no autoriza por el área; un usuario sin MAST no autoriza Seguridad.
        with self.assertRaises(UserError):
            permit.with_user(self.other).action_approve_area()
        with self.assertRaises(UserError):
            permit.with_user(self.boss).action_approve_sst()
        permit.with_user(self.boss).action_approve_area()
        self.assertEqual(permit.state, 'solicitado')
        self.assertEqual(permit.area_approved_by_id, self.boss)
        self.assertTrue(permit.area_approved_date)
        permit.with_user(self.mast).action_approve_sst()
        self.assertEqual(permit.state, 'autorizado')
        self.assertEqual(permit.sst_approved_by_id, self.mast)
        with self.assertRaises(UserError):
            permit.action_close()  # sin condiciones de cierre
        permit.close_note = 'Tableros cerrados, energía restablecida'
        permit.action_close()
        self.assertEqual(permit.state, 'cerrado')
        self.assertEqual(permit.closed_by_id, self.requester)
        # Cerrado es evidencia: el usuario no lo reabre; MAST sí.
        assert_locked(self, permit.action_reset)
        permit.with_user(self.mast).action_reset()
        self.assertEqual(permit.state, 'borrador')
        self.assertFalse(permit.area_approved_by_id)

    def test_04_dos_personas_distintas(self):
        permit = self._permit(area_manager_id=self.mast.id, hazards='Arco eléctrico')
        self._answer_all(permit)
        permit.action_submit()
        permit.with_user(self.mast).action_approve_area()
        with self.assertRaises(UserError):
            permit.with_user(self.mast).action_approve_sst()
        mast2 = sgi_test_user(self.env, 'zs_ptar_mast2', 'quimibond_sgi.group_sgi_manager')
        permit.with_user(mast2).action_approve_sst()
        self.assertEqual(permit.state, 'autorizado')

    def test_05_vencido_y_reporte(self):
        permit = self._permit(hazards='Arco eléctrico')
        self._answer_all(permit, 'na')
        permit.action_submit()
        permit.with_user(self.boss).action_approve_area()
        permit.with_user(self.mast).action_approve_sst()
        permit.sudo().write({'date_start': datetime(2020, 1, 1, 8, 0),
                             'date_end': datetime(2020, 1, 1, 18, 0)})
        self.assertTrue(permit.expired)
        self.assertTrue(permit.sgi_format_info().startswith('F-P-A14-03'))
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_work_permit_document', permit.ids)[0].decode()
        self.assertIn('Permiso de trabajo de alto riesgo', html)
        self.assertIn('Formato controlado del SGI', html)
        self.assertIn(permit.folio, html)
