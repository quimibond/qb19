# -*- coding: utf-8 -*-
"""57.52.0 — bloqueo y etiquetado de energías, LOTO (NOM-004, S5.14): el
bloqueo no se aplica sin aviso, fuentes con punto de bloqueo, un candado y
tarjeta por trabajador y energía cero; cada quien retira su candado; el
retiro exige que no quede ninguno."""
from psycopg2 import IntegrityError

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

from .common_users import assert_locked, sgi_test_user


@tagged('post_install', '-at_install')
class TestLoto(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mech = sgi_test_user(cls.env, 'zs_loto_mecanico', 'quimibond_sgi.group_sgi_user')
        cls.helper = sgi_test_user(cls.env, 'zs_loto_ayudante', 'quimibond_sgi.group_sgi_user')
        cls.mast = sgi_test_user(cls.env, 'zs_loto_mast', 'quimibond_sgi.group_sgi_manager')
        Employee = cls.env['hr.employee']
        cls.emp_mech = Employee.create({'name': 'ZS Mecánico', 'user_id': cls.mech.id})
        cls.emp_helper = Employee.create({'name': 'ZS Ayudante', 'user_id': cls.helper.id})
        cls.equipment = cls.env['maintenance.equipment'].create({'name': 'ZS Carda 3'})

    def _loto(self):
        return self.env['sgi.loto'].with_user(self.mech).create({
            'equipment_id': self.equipment.id, 'name': 'Cambio de guarnición',
            'energy_ids': [(0, 0, {'energy_type': 'electrica', 'isolation_point': 'Interruptor TG-3'}),
                           (0, 0, {'energy_type': 'neumatica'})],
            'lock_ids': [(0, 0, {'employee_id': self.emp_mech.id, 'lock_number': 'C-01',
                                 'tag_number': 'T-01'}),
                         (0, 0, {'employee_id': self.emp_helper.id, 'lock_number': 'C-02'})],
        })

    def test_01_aplicar_exige_todo(self):
        self._applied_loto()

    def _applied_loto(self):
        loto = self._loto()
        self.assertTrue(loto.folio.startswith('LOTO-'))
        with self.assertRaises(UserError) as caught:
            loto.action_apply()
        message = str(caught.exception)
        self.assertIn("Avise a los afectados", message)
        self.assertIn("punto de bloqueo", message)
        self.assertIn("su tarjeta", message)
        self.assertIn("energía cero", message)
        loto.write({'affected_notified': True, 'zero_energy_verified': True,
                    'zero_energy_method': 'Intento de arranque y probador en cero'})
        loto.energy_ids.filtered(lambda e: e.energy_type == 'neumatica').isolation_point = 'Válvula V-12'
        loto.lock_ids.filtered(lambda lock: lock.employee_id == self.emp_helper).tag_number = 'T-02'
        loto.action_apply()
        self.assertEqual(loto.state, 'bloqueado')
        self.assertEqual(loto.active_locks, 2)
        self.assertTrue(all(loto.lock_ids.mapped('date_placed')))
        self.assertEqual(loto.verified_by_id, self.mech)
        # Aplicado, las fuentes y los candados ya no se editan.
        with self.assertRaises(UserError):
            loto.energy_ids[:1].method = 'Otro'
        with self.assertRaises(UserError):
            loto.lock_ids[:1].lock_number = 'C-99'
        with self.assertRaises(UserError):
            loto.action_cancel()
        return loto

    def test_02_cada_quien_retira_su_candado(self):
        loto = self._applied_loto()
        helper_lock = loto.lock_ids.filtered(lambda lock: lock.employee_id == self.emp_helper)
        mech_lock = loto.lock_ids - helper_lock
        with self.assertRaises(UserError):
            helper_lock.with_user(self.mech).action_remove_lock()
        with self.assertRaises(UserError):
            loto.action_remove()  # quedan candados
        mech_lock.with_user(self.mech).action_remove_lock()
        self.assertEqual(mech_lock.removed_by_id, self.mech)
        self.assertEqual(loto.active_locks, 1)
        # El Jefe MAST puede retirarlo por él; queda quién lo hizo.
        helper_lock.with_user(self.mast).action_remove_lock()
        self.assertEqual(helper_lock.removed_by_id, self.mast)
        with self.assertRaises(UserError):
            mech_lock.with_user(self.mech).action_remove_lock()  # ya retirado
        with self.assertRaises(UserError):
            loto.action_remove()  # falta aviso al área y condiciones
        loto.write({'area_notified_end': True, 'removal_note': 'Guardas colocadas'})
        loto.action_remove()
        self.assertEqual(loto.state, 'retirado')
        self.assertTrue(loto.date_removed)
        # Retirado es evidencia: solo MAST lo reabre.
        assert_locked(self, loto.action_reset)
        loto.with_user(self.mast).action_reset()
        self.assertEqual(loto.state, 'borrador')

    def test_03_un_candado_por_trabajador_y_reporte(self):
        loto = self._loto()
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'):
            with self.env.cr.savepoint():
                loto.lock_ids = [(0, 0, {'employee_id': self.emp_mech.id, 'lock_number': 'C-03'})]
                loto.flush_recordset()
        self.assertTrue(loto.sgi_format_info())
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_loto_document', loto.ids)[0].decode()
        self.assertIn('Bloqueo y etiquetado de energías', html)
        self.assertIn('Interruptor TG-3', html)
        self.assertIn('Formato controlado del SGI', html)
