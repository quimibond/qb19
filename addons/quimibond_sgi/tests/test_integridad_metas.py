# -*- coding: utf-8 -*-
"""57.100.0 (K-04): la medición validada conserva las metas con las que se
juzgó; cambiar la meta del indicador o un escalón no la recolorea. Reabrirla
(solo el Jefe MAST) las suelta; validarla otra vez guarda las de ese momento.

Las pruebas crean sus propios indicadores (claves ZK04-*) con periodos de
2046: la base del build es copia de producción."""
import importlib.util
import os
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from .common_users import sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_POST = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.100.0', 'post-migrate.py')


def _post_migrate():
    """El post-migrate de 57.100.0, cargado dentro de la prueba (un archivo que
    falta no rompe la carga del paquete de pruebas)."""
    spec = importlib.util.spec_from_file_location('sgi_mig_57_100_0', _POST)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged('post_install', '-at_install')
class TestIntegridadMetas(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mast = sgi_set_mast(cls.env, login='sgi_mast_k04')
        cls.ind = cls.env['sgi.indicator'].create({
            'code': 'ZK04-01', 'name': 'Indicador K-04', 'calc_mode': 'manual',
            'direction': 'higher_better', 'target_objective': 90.0,
            'target_acceptable': 80.0, 'responsible_id': cls.mast.id})

    def _measure(self, value, period=date(2046, 1, 1), state='capturado', ind=None):
        return self.env['sgi.indicator.measure'].create({
            'indicator_id': (ind or self.ind).id, 'period_date': period,
            'value': value, 'state': state})

    def test_01_validada_conserva_color_al_cambiar_meta(self):
        captured = self._measure(85.0, date(2046, 1, 1))
        validated = self._measure(85.0, date(2046, 2, 1))
        validated.with_user(self.mast).action_validate()
        self.assertEqual(validated.semaphore, 'amarillo')
        self.assertTrue(validated.sgi_targets_frozen)
        self.assertTrue(validated.sgi_frozen_date)
        self.ind.write({'target_objective': 80.0, 'target_acceptable': 70.0})
        self.assertEqual(captured.semaphore, 'verde', "La capturada sigue a la meta nueva.")
        self.assertEqual(validated.semaphore, 'amarillo', "La validada no cambia de color.")
        self.assertEqual(validated.target_objective, 90.0, "Muestra el objetivo con el que se validó.")
        self.assertEqual(validated.target_acceptable, 80.0)
        self.assertTrue(any('conservan las metas' in (body or '')
                            for body in self.ind.message_ids.mapped('body')),
                        "El indicador avisa que hay validadas con metas guardadas.")

    def test_01b_frontera_sin_redondeo(self):
        """I-1: las metas se guardan tal cual (sin redondear a 2 decimales):
        85.002 contra un objetivo de 85.004 sigue amarillo al congelar."""
        ind = self.env['sgi.indicator'].create({
            'code': 'ZK04-01B', 'name': 'Indicador K-04 frontera', 'calc_mode': 'manual',
            'direction': 'higher_better', 'target_objective': 85.004,
            'target_acceptable': 80.0, 'responsible_id': self.mast.id})
        m = self._measure(85.002, ind=ind)
        self.assertEqual(m.semaphore, 'amarillo')
        m.with_user(self.mast).action_validate()
        self.assertEqual(m.sgi_frozen_objective, 85.004)
        self.assertEqual(m.semaphore, 'amarillo')

    def test_02_escalon_de_trayectoria_no_recolorea(self):
        ind = self.env['sgi.indicator'].create({
            'code': 'ZK04-02', 'name': 'Indicador con trayectoria', 'calc_mode': 'manual',
            'direction': 'higher_better',
            'target_objective': 100.0, 'target_acceptable': 90.0, 'baseline_value': 40.0,
            'baseline_date': date(2046, 1, 1), 'target_date': date(2046, 12, 31),
            'responsible_id': self.mast.id})
        step = ind.step_ids.filtered(lambda s: s.date_from == date(2046, 1, 1))
        self.assertTrue(step, "La trayectoria se genera sola al crear el indicador.")
        captured = self._measure(60.0, date(2046, 3, 1), ind=ind)
        m = self._measure(60.0, date(2046, 2, 1), ind=ind)
        m.with_user(self.mast).action_validate()
        color = m.semaphore
        self.assertEqual(color, captured.semaphore)
        step.write({'objective': 70.0, 'acceptable': 65.0, 'reason': 'Prueba K-04'})
        self.assertEqual(captured.semaphore, 'rojo', "La capturada sigue al escalón corregido.")
        self.assertEqual(m.semaphore, color, "La validada no cambia de color.")

    def test_03_rango_congelado(self):
        ind = self.env['sgi.indicator'].create({
            'code': 'ZK04-03', 'name': 'Indicador de rango', 'calc_mode': 'manual',
            'direction': 'range', 'range_min': 10.0, 'range_max': 20.0,
            'range_tolerance': 2.0, 'responsible_id': self.mast.id})
        m = self._measure(15.0, ind=ind)
        m.with_user(self.mast).action_validate()
        ind.write({'range_min': 16.0, 'range_max': 30.0})
        self.assertEqual(m.semaphore, 'verde')
        self.assertEqual(m.sgi_frozen_direction, 'range')
        self.assertEqual(m.sgi_frozen_range_min, 10.0)
        self.assertEqual(m.sgi_frozen_range_max, 20.0)

    def test_04_reabrir_libera_y_revalidar_congela_lo_nuevo(self):
        m = self._measure(85.0)
        m.with_user(self.mast).action_validate()
        self.ind.write({'target_objective': 80.0, 'target_acceptable': 70.0})
        self.assertEqual(m.semaphore, 'amarillo')
        m.with_user(self.mast).action_reset()
        self.assertEqual(m.state, 'pendiente')
        self.assertFalse(m.sgi_targets_frozen)
        self.assertTrue(any('Metas liberadas' in (body or '') for body in m.message_ids.mapped('body')))
        m.write({'state': 'capturado'})
        self.assertEqual(m.semaphore, 'verde', "Reabierta, sigue la meta actual.")
        m.with_user(self.mast).action_validate()
        self.assertTrue(m.sgi_targets_frozen)
        self.assertEqual(m.sgi_frozen_objective, 80.0)

    def test_05_mast_corrige_valor_con_metas_congeladas(self):
        m = self._measure(85.0)
        m.with_user(self.mast).action_validate()
        self.ind.write({'target_objective': 50.0, 'target_acceptable': 40.0})
        m.with_user(self.mast).write({'value': 95.0})
        self.assertEqual(m.semaphore, 'verde', "95 contra 90/80 congelados.")
        m.with_user(self.mast).write({'value': 60.0})
        self.assertEqual(m.semaphore, 'rojo', "60 contra 90/80, no contra 50/40.")

    def test_06_nace_validada_y_congela(self):
        m = self._measure(85.0, state='validado')
        self.assertTrue(m.sgi_targets_frozen)
        self.assertEqual(m.sgi_frozen_objective, 90.0)
        self.assertEqual(m.sgi_frozen_acceptable, 80.0)
        self.assertEqual(m.sgi_frozen_direction, 'higher_better')

    def test_07_nadie_escribe_las_metas_congeladas(self):
        m = self._measure(85.0)
        m.with_user(self.mast).action_validate()
        with self.assertRaises(UserError):
            m.with_user(self.mast).write({'sgi_frozen_objective': 1.0})
        with self.assertRaises(UserError):
            m.with_user(self.mast).write({'sgi_targets_frozen': False})
        self.assertEqual(m.sgi_frozen_objective, 90.0)

    def test_08_desglose_de_validada_conserva_color(self):
        m = self._measure(85.0)
        row = self.env['sgi.indicator.measure.split'].create({
            'measure_id': m.id, 'market': 'nacional', 'value': 85.0})
        self.assertEqual(row.semaphore, 'amarillo')
        m.with_user(self.mast).action_validate()
        self.ind.write({'target_objective': 80.0, 'target_acceptable': 70.0})
        self.assertEqual(row.semaphore, 'amarillo')

    def test_09_post_migrate_congela_las_validadas_sin_cambiar_color(self):
        m = self._measure(85.0)
        m.with_user(self.mast).action_validate()
        self.env.flush_all()
        self.env.cr.execute(
            "UPDATE sgi_indicator_measure SET sgi_targets_frozen = false WHERE id = %s", [m.id])
        m.invalidate_recordset()
        before = m.semaphore
        mig = _post_migrate()
        mig.migrate(self.env.cr, '19.0.57.99.0')
        m.invalidate_recordset()
        self.assertTrue(m.sgi_targets_frozen)
        self.assertEqual(m.semaphore, before)
        self.assertEqual(m.sgi_frozen_objective, 90.0)
        mig.migrate(self.env.cr, '19.0.57.99.0')  # idempotente
        m.invalidate_recordset()
        self.assertTrue(m.sgi_targets_frozen)
