# -*- coding: utf-8 -*-
"""57.176.0 (Jose, 2026-10-21): la medición se valida sola cuando no hay nada
que mirar. Oficial, sin rojo y sin muestra chica: sola (las manuales, solo si
las capturó su responsable). Rojos, capturas ajenas, indicadores en prueba y
los de salud del SGI: se quedan para el dueño."""
from datetime import date

from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import sgi_set_mast

USER = 'base.group_user,quimibond_sgi.group_sgi_user'


@tagged('post_install', '-at_install')
class TestAutovalidacion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.owner = new_test_user(cls.env, login='zav_duenio', groups=USER)
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']

    def _indicator(self, code, **vals):
        base = {'code': code, 'name': 'Autovalida %s' % code, 'calc_mode': 'otif_ventas',
                'direction': 'higher_better', 'target_objective': 90.0, 'target_acceptable': 80.0,
                'responsible_id': self.owner.id, 'status': 'oficial'}
        base.update(vals)
        return self.Indicator.create(base)

    def _measure(self, indicator, value, period=date(2040, 1, 1)):
        return self.Measure.create({'indicator_id': indicator.id, 'period_date': period,
                                    'state': 'capturado', 'value': value})

    def test_01_automatico_oficial(self):
        indicator = self._indicator('ZAV-1')
        green = self._measure(indicator, 95.0)
        red = self._measure(indicator, 10.0, period=date(2040, 2, 1))
        self.assertEqual(green.state, 'validado', "Oficial y verde: se valida sola.")
        self.assertTrue(green.sgi_validated_date, "Guarda su fecha de validación.")
        self.assertEqual(red.state, 'capturado', "El rojo se queda para el dueño (causa, plan y NC).")

    def test_02_prueba_y_salud_no(self):
        trial = self._indicator('ZAV-2', status='prueba')
        self.assertEqual(self._measure(trial, 95.0).state, 'capturado', "En prueba no se valida.")
        health = self.env.ref('quimibond_sgi.sgi_ind_salud_rojos')
        health.sudo().write({'status': 'oficial'})
        self.assertEqual(self._measure(health, 95.0).state, 'capturado',
                         "Las de salud del SGI no se validan.")

    def test_03_manual_por_quien_la_captura(self):
        indicator = self._indicator('ZAV-3', calc_mode='manual')
        own, foreign = self.Measure.create([
            {'indicator_id': indicator.id, 'period_date': date(2040, 3, 1)},
            {'indicator_id': indicator.id, 'period_date': date(2040, 4, 1)}])
        own.with_user(self.owner).write({'value': 95.0, 'state': 'capturado'})
        self.assertEqual(own.state, 'validado', "La capturó su responsable: su captura es la confirmación.")
        # El Jefe MAST también captura cualquier indicador; la valida el responsable.
        mast = sgi_set_mast(self.env, login='zav_mast')
        foreign.with_user(mast).write({'value': 95.0, 'state': 'capturado'})
        self.assertEqual(foreign.state, 'capturado', "Capturada por otra persona: la valida el responsable.")

    def test_04_al_pasar_a_oficial(self):
        indicator = self._indicator('ZAV-4', status='prueba')
        green = self._measure(indicator, 95.0)
        red = self._measure(indicator, 10.0, period=date(2040, 2, 1))
        indicator.action_set_official()
        self.assertEqual((green.state, red.state), ('validado', 'capturado'),
                         "Lo verde capturado en prueba se valida al pasar a oficial; el rojo, no.")
        pending = self.env['sgi.my.pending']._sgi_pending_records(self.owner)['validacion']
        self.assertIn(red, pending)
        self.assertNotIn(green, pending)
