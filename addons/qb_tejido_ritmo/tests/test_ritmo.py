# -*- coding: utf-8 -*-
"""El recálculo en Odoo con rollos inyectados: intervalos, paros generales,
ritmos, capacidad medida del centro y la fuente «pesaje» del costeo."""
from datetime import date, datetime, timedelta
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged

from ..models.ritmo import QbTejidoRitmo


@tagged('post_install', '-at_install')
class TestRitmo(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        cal = cls.env['resource.calendar'].create({'name': 'Tejido 24/7', 'tz': 'America/Mexico_City'})
        cls.wc1 = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR T1', 'resource_calendar_id': cal.id})
        cls.wc2 = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR T2', 'resource_calendar_id': cal.id})
        cls.centro = cls.env['qb.centro'].create({
            'code': 'TEJT', 'name': 'Tejido prueba', 'nature': 'directo', 'driver': 'workorder',
            'workcenter_ids': [(6, 0, [cls.wc1.id, cls.wc2.id])]})
        cls.crudo = cls.env['product.product'].create({
            'name': 'WJ044Q22HNT235', 'default_code': 'WJ044Q22HNT235', 'type': 'consu',
            'is_storable': True, 'uom_id': kg.id})
        cls.crudo2 = cls.env['product.product'].create({
            'name': 'WJ035Q22HNT200', 'default_code': 'WJ035Q22HNT200', 'type': 'consu',
            'is_storable': True, 'uom_id': kg.id})
        cls.Ritmo = cls.env['qb.tejido.ritmo']
        # Lunes de hace dos semanas, 8:00 de planta.
        hoy = date.today()
        cls.lunes = hoy - timedelta(days=hoy.weekday() + 14)
        cls.inicio = datetime.combine(cls.lunes, datetime.min.time()) + timedelta(hours=8)

    def _rollos(self):
        rollos, t = [], self.inicio
        i = 1
        # Máquina 1: 30 rollos cada hora, un paro de 12 h a la mitad.
        for k in range(30):
            if k == 15:
                t += timedelta(hours=12)
            rollos.append({'id': i, 'wc': self.wc1.id, 'wc_name': self.wc1.name, 't': t, 'kg': 38.0,
                           'user': 130, 'mo': None, 'product': self.crudo.id,
                           'product_code': 'WJ044Q22HNT235', 'roll_number': 'R%03d' % i})
            t += timedelta(hours=1)
            i += 1
        # Máquina 2: cada 2 h, otro artículo, evita el paro general.
        t = self.inicio
        for k in range(25):
            rollos.append({'id': i, 'wc': self.wc2.id, 'wc_name': self.wc2.name, 't': t, 'kg': 45.0,
                           'user': 130, 'mo': None, 'product': self.crudo2.id,
                           'product_code': 'WJ035Q22HNT200', 'roll_number': 'R%03d' % i})
            t += timedelta(hours=2)
            i += 1
        return rollos

    def _recalcular(self):
        rollos = self._rollos()
        with patch.object(QbTejidoRitmo, '_leer_rollos', return_value=rollos):
            return self.Ritmo.recalcular(self.lunes - timedelta(days=1), self.lunes + timedelta(days=8))

    def test_recalculo_completo(self):
        n_int, n_rit = self._recalcular()
        self.assertEqual(n_int, 55)
        Int = self.env['qb.tejido.intervalo']
        paro = Int.search([('workcenter_id', '=', self.wc1.id), ('clase', '=', 'paro')])
        self.assertEqual(len(paro), 1)
        self.assertAlmostEqual(paro.horas_paro, 12.0, 2)
        self.assertAlmostEqual(paro.horas_corrida, 1.0, 2)
        self.assertEqual(paro.product_id, self.crudo)
        self.assertEqual(paro.semana, self.lunes)
        # El motivo del paro sobrevive al recálculo.
        paro.motivo_paro = 'falta_hilo'
        self._recalcular()
        paro = Int.search([('workcenter_id', '=', self.wc1.id), ('clase', '=', 'paro')])
        self.assertEqual(paro.motivo_paro, 'falta_hilo')
        self.assertEqual(Int.search_count([('workcenter_id', 'in', (self.wc1.id, self.wc2.id))]), 55)
        # Ritmo del artículo en la máquina 1 (semana y mes).
        r = self.Ritmo.search([('workcenter_id', '=', self.wc1.id), ('product_id', '=', self.crudo.id),
                               ('tipo', '=', 'semana')])
        self.assertEqual(len(r), 1)
        self.assertEqual(r.rollos, 29)
        self.assertAlmostEqual(r.kg_h_corrida, 38.0, 2)
        self.assertAlmostEqual(r.horas_paro, 12.0, 2)
        self.assertTrue(r.kg_h_efectiva < r.kg_h_corrida)
        self.assertTrue(r.suficiente)
        tot = self.Ritmo.search([('workcenter_id', '=', self.wc1.id), ('es_total', '=', True),
                                 ('tipo', '=', 'semana')])
        self.assertEqual(len(tot), 1)
        self.assertTrue(tot.horas_programadas > 100)
        self.assertAlmostEqual(tot.horas_corrida + tot.horas_paro + tot.horas_cambio
                               + tot.horas_sin_medir + tot.horas_sin_explicar,
                               tot.horas_programadas, 2)
        self.assertTrue(0 < tot.pct_corrida < 100)

    def test_paro_general_desmarcado(self):
        """Un hueco de planta se registra; si se desmarca, el siguiente
        recálculo deja de restarlo."""
        rollos = self._rollos()
        # Mover la máquina 2 para que el hueco de 12 h sea de toda la planta.
        for r in rollos:
            if r['wc'] == self.wc2.id and r['t'] > self.inicio + timedelta(hours=14):
                r['t'] += timedelta(hours=12)
        with patch.object(QbTejidoRitmo, '_leer_rollos', return_value=rollos):
            self.Ritmo.recalcular(self.lunes - timedelta(days=1), self.lunes + timedelta(days=8))
        Paro = self.env['qb.tejido.paro.general']
        pg = Paro.search([('inicio_local', '>=', self.inicio)])
        self.assertEqual(len(pg), 1)
        self.assertTrue(pg.horas >= 12.0)
        Int = self.env['qb.tejido.intervalo']
        self.assertEqual(Int.search_count([('clase', '=', 'paro'),
                                           ('workcenter_id', 'in', (self.wc1.id, self.wc2.id))]), 0)
        pg.es_paro = False
        with patch.object(QbTejidoRitmo, '_leer_rollos', return_value=rollos):
            self.Ritmo.recalcular(self.lunes - timedelta(days=1), self.lunes + timedelta(days=8))
        self.assertEqual(Int.search_count([('clase', '=', 'paro'),
                                           ('workcenter_id', 'in', (self.wc1.id, self.wc2.id))]), 2)
        self.assertTrue(pg.exists())           # el registro desmarcado se conserva

    def test_centro_y_fuente_pesaje(self):
        self._recalcular()
        # Filas mensuales del mes de los rollos → capacidad medida si el mes ya cerró.
        mes = self.lunes.replace(day=1)
        if mes < date.today().replace(day=1):
            self.centro._calcular_ritmo_medido()
            self.assertTrue(self.centro.throughput_medido > 0)
            self.assertTrue(self.centro.capacidad_medida_h_mes > 0)
            self.centro.action_aplicar_ritmo()
            self.assertAlmostEqual(self.centro.capacidad_h_mes, round(self.centro.capacidad_medida_h_mes, 2), 2)
            self.assertIn('Pesaje', self.centro.capacidad_motivo)
        # Fuente «pesaje» en las horas por producto: kg/h efectiva del artículo.
        ef = self.Ritmo.efectiva_por_producto(self.centro.workcenter_ids, meses=3)
        self.assertIn(self.crudo.id, ef)
        H = self.env['qb.producto.horas']
        H.recalcular(self.crudo)
        fila = H.search([('product_id', '=', self.crudo.id), ('centro_id', '=', self.centro.id)])
        self.assertEqual(fila.fuente, 'pesaje')
        self.assertEqual(fila.calidad, 'alta')
        self.assertAlmostEqual(fila.horas_propias, 1.0 / ef[self.crudo.id][0], 6)
        self.assertIn('Pesaje', fila.detalle)
