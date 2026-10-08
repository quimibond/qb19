# -*- coding: utf-8 -*-
"""El motor puro: descansos, clasificación y ritmos con datos sintéticos."""
from datetime import date, datetime, timedelta

from odoo.tests import TransactionCase, tagged

from ..models import motor as M


def _rollos(wc, product, code, inicio, n, cada_h, kg, start_id=0, user=130):
    out, t = [], inicio
    for i in range(n):
        out.append({'id': start_id + i, 'wc': wc, 'wc_name': 'CIRCULAR %d' % wc,
                    'product': product, 'product_code': code, 't': t, 'kg': kg, 'user': user})
        t += timedelta(hours=cada_h)
    return out, t


@tagged('post_install', '-at_install')
class TestMotor(TransactionCase):

    def setUp(self):
        super().setUp()
        self.P = dict(M.PARAMS_DEFAULT)
        self.desc = M.Descansos(reglas=[
            (None, date(2026, 9, 24), 4, 18.0, 5, 19.0),
            (date(2026, 9, 25), None, 4, 18.0, 6, 19.0)])

    def test_descansos_con_vigencia(self):
        # Semana del 28-sep (regla nueva): viernes 18:00 → domingo 19:00 = 49 h.
        self.assertAlmostEqual(self.desc.horas(datetime(2026, 9, 28), datetime(2026, 10, 5)), 49.0)
        self.assertAlmostEqual(self.desc.horas_programadas(datetime(2026, 9, 28), datetime(2026, 10, 5)), 119.0)
        # Semana del 14-sep (regla vieja): viernes 18:00 → sábado 19:00 = 25 h.
        self.assertAlmostEqual(self.desc.horas(datetime(2026, 9, 14), datetime(2026, 9, 21)), 25.0)
        # Un intervalo que cruza el descanso lo descuenta.
        self.assertAlmostEqual(self.desc.horas(datetime(2026, 10, 2, 17), datetime(2026, 10, 4, 20)), 49.0)
        # Festivo: se une al descanso sin doble conteo.
        d = M.Descansos(reglas=self.desc.reglas,
                        festivos=[(datetime(2026, 10, 2, 12), datetime(2026, 10, 3, 12))])
        self.assertAlmostEqual(d.horas(datetime(2026, 10, 2), datetime(2026, 10, 5)), 55.0)

    def test_limpieza(self):
        rollos = [
            {'id': 1, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 1, 'product_code': 'WJ044Q22HNT235', 't': datetime(2026, 9, 28), 'kg': 40, 'user': 130},
            {'id': 2, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 2, 'product_code': 'MUESTRA PILOTO TEJIDO', 't': datetime(2026, 9, 28), 'kg': 40, 'user': 130},
            {'id': 3, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 3, 'product_code': 'ZJ044R22HNT235', 't': datetime(2026, 9, 28), 'kg': 40, 'user': 130},
            {'id': 4, 'wc': 9, 'wc_name': 'MAQUILA', 'product': 1, 'product_code': 'WJ044Q22HNT235', 't': datetime(2026, 9, 28), 'kg': 40, 'user': 130},
            {'id': 5, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 1, 'product_code': 'WJ044Q22HNT235', 't': datetime(2026, 9, 28), 'kg': 500, 'user': 130},
            {'id': 6, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 1, 'product_code': 'WJ044Q22HNT235', 't': datetime(2026, 9, 28), 'kg': 40, 'user': 129},
        ]
        limpios = M.limpiar(rollos, self.P, ['MUESTRA PILOTO TEJIDO', 'CALCETÍN'], ['ZJ'],
                            ['MAQUILA'], ['129'])
        self.assertEqual([r['id'] for r in limpios], [1])

    def test_clasificacion(self):
        """Corrida, paro (3× la referencia), cambio de artículo, pesado
        junto, captura atrasada y paro general."""
        rollos, t = _rollos(1, 10, 'WJ044Q22HNT235', datetime(2026, 9, 28, 8), 20, 1.0, 38)
        t += timedelta(hours=10)                                  # paro de 10 h
        mas, t = _rollos(1, 10, 'WJ044Q22HNT235', t, 10, 1.0, 38, start_id=20)
        rollos += mas
        t += timedelta(hours=2)
        mas, t = _rollos(1, 11, 'WJ035Q22HNT200', t, 10, 2.0, 50, start_id=30)   # cambio a B
        rollos += mas
        # Pesado junto: dos rollos con 5 min de diferencia (no llega a 3 = atrasada).
        rollos.append({'id': 40, 'wc': 1, 'wc_name': 'CIRCULAR 1', 'product': 11,
                       'product_code': 'WJ035Q22HNT200', 't': t - timedelta(hours=2) + timedelta(minutes=5),
                       'kg': 50, 'user': 130})
        # Otra máquina cada 3 h (evita el paro general) y una captura atrasada de 4 rollos.
        otros, t2 = _rollos(2, 10, 'WJ044Q22HNT235', datetime(2026, 9, 28, 8), 25, 3.0, 40, start_id=100)
        rollos += otros
        for i in range(4):
            t2 += timedelta(minutes=2)
            rollos.append({'id': 200 + i, 'wc': 2, 'wc_name': 'CIRCULAR 2', 'product': 10,
                           'product_code': 'WJ044Q22HNT235', 't': t2, 'kg': 40, 'user': 130})
        filas, paros = M.clasificar(rollos, self.desc, self.P)
        self.assertEqual(paros, [])
        clases = {}
        for f in filas:
            clases[f['clase']] = clases.get(f['clase'], 0) + 1
        self.assertEqual(clases['primer_rollo'], 2)
        self.assertEqual(clases['paro'], 1)
        self.assertEqual(clases['cambio'], 1)
        self.assertEqual(clases['pesado_junto'], 1)
        self.assertEqual(clases['captura_atrasada'], 4)
        paro = next(f for f in filas if f['clase'] == 'paro')
        self.assertAlmostEqual(paro['netas'], 11.0)
        self.assertAlmostEqual(paro['referencia'], 1.0)
        self.assertAlmostEqual(paro['h_paro'], 10.0)
        self.assertAlmostEqual(paro['h_corrida'], 1.0)       # la referencia cuenta como corrida
        self.assertEqual(paro['kg_corrida'], 38)
        cambio = next(f for f in filas if f['clase'] == 'cambio')
        self.assertAlmostEqual(cambio['netas'], 3.0)
        self.assertAlmostEqual(cambio['referencia'], 2.0)
        self.assertAlmostEqual(cambio['h_cambio'], 1.0)
        self.assertAlmostEqual(cambio['h_corrida'], 2.0)
        # Modo de la prueba de aceptación: la referencia no cuenta como corrida.
        P2 = dict(self.P, referencia_cuenta_corrida=False)
        filas2, _ = M.clasificar(rollos, self.desc, P2)
        paro2 = next(f for f in filas2 if f['clase'] == 'paro')
        self.assertAlmostEqual(paro2['h_corrida'], 0.0)
        self.assertAlmostEqual(paro2['h_ref_paro'], 1.0)
        self.assertEqual(paro2['kg_paro'], 38)

    def test_paro_general_y_ritmos(self):
        """Un hueco de 8 h sin pesajes en toda la planta se resta a las dos
        máquinas; los ritmos salen por máquina, artículo y periodo."""
        a, t = _rollos(1, 10, 'WJ044Q22HNT235', datetime(2026, 9, 28, 8), 10, 1.0, 38)
        b, _ = _rollos(2, 11, 'WJ035Q22HNT200', datetime(2026, 9, 28, 8), 10, 1.0, 45, start_id=100)
        hueco = t + timedelta(hours=8)
        a2, _ = _rollos(1, 10, 'WJ044Q22HNT235', hueco, 10, 1.0, 38, start_id=20)
        b2, _ = _rollos(2, 11, 'WJ035Q22HNT200', hueco, 10, 1.0, 45, start_id=120)
        rollos = a + b + a2 + b2
        filas, paros = M.clasificar(rollos, self.desc, self.P)
        self.assertEqual(len(paros), 1)
        self.assertAlmostEqual(paros[0][2], 9.0)      # de 17:00 a 02:00 del día siguiente
        tras = [f for f in filas if f['id'] in (20, 120)]
        for f in tras:
            self.assertAlmostEqual(f['paro_general'], 9.0)
            self.assertAlmostEqual(f['netas'], 0.0)
            self.assertEqual(f['clase'], 'sin_referencia')   # sin tiempo medible, no es paro
        rows = M.ritmos(filas, self.P, horas_programadas=lambda wc, per, tipo: 119.0)
        r1 = next(r for r in rows if r['tipo'] == 'semana' and r['wc'] == 1 and r['product'] == 10)
        self.assertEqual(r1['rollos'], 19)
        self.assertEqual(r1['rollos_corrida'], 18)
        self.assertAlmostEqual(r1['kg_h_corrida'], 38.0, 3)
        self.assertAlmostEqual(r1['kg_h_tecnica'], 38.0, 3)
        self.assertAlmostEqual(r1['kg_h_efectiva'], 38.0, 3)
        self.assertTrue(r1['suficiente'])
        tot = next(r for r in rows if r['tipo'] == 'semana' and r['wc'] == 1 and r['product'] is None)
        self.assertAlmostEqual(tot['h_programadas'], 119.0)
        self.assertAlmostEqual(tot['h_corrida'], 18.0)
        self.assertAlmostEqual(tot['pct_corrida'], 18 / 119.0, 4)
        self.assertAlmostEqual(tot['h_sin_explicar'], 101.0)
        # Pocos rollos: sin dato suficiente.
        pocos, _ = _rollos(3, 12, 'XJ130Q21HNT096', datetime(2026, 9, 28, 8), 3, 1.0, 60, start_id=300)
        filas3, _ = M.clasificar(rollos + pocos, self.desc, self.P)
        rows3 = M.ritmos(filas3, self.P)
        r3 = next(r for r in rows3 if r['tipo'] == 'mes' and r['wc'] == 3 and r['product'] == 12)
        self.assertFalse(r3['suficiente'])
        self.assertIsNone(r3['kg_h_corrida'])
