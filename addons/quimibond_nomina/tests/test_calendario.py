# -*- coding: utf-8 -*-
"""Calendario de pagos (``models/calendario.py``): el divisor del Infonavit
(pagos del bimestre) y las nóminas que paga el mes, con un solo conteo.

Los divisores de septiembre–octubre (9) y noviembre–diciembre de 2026 (8) se
midieron contra NOI (semanas 37–41 y 46). No corre en el CI (depende de
Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.tests.common import TransactionCase

from ..models.calendario import _qb_bimestre_de, _qb_dias_de_pago, _qb_mes_de, _qb_pagos_en_ventana

VIERNES = 4


class TestCalendario(TransactionCase):

    def _pagos_bimestre(self, anio, mes):
        return _qb_pagos_en_ventana(*_qb_bimestre_de(date(anio, mes, 1)), VIERNES)

    def test_divisor_por_bimestre(self):
        casos = [
            ((2026, 9), 9),     # sep–oct 2026: NOI semanas 37 a 41 (2,158.72 ÷ 9 = 239.84)
            ((2026, 11), 8),    # nov–dic 2026: NOI semana 46 (2,158.72 ÷ 8 = 269.84)
            ((2027, 1), 9),     # ene–feb 2027: el 1-ene-2027 es viernes
            ((2027, 3), 9),
            ((2026, 1), 9),
            ((2026, 7), 9),
        ]
        for (anio, mes), esperado in casos:
            with self.subTest(bimestre='%d-%02d' % (anio, mes)):
                self.assertEqual(self._pagos_bimestre(anio, mes), esperado)

    def test_bimestre_de(self):
        self.assertEqual(_qb_bimestre_de(date(2026, 11, 6)), (date(2026, 11, 1), date(2026, 12, 31)))
        self.assertEqual(_qb_bimestre_de(date(2026, 12, 31)), (date(2026, 11, 1), date(2026, 12, 31)))
        self.assertEqual(_qb_bimestre_de(date(2026, 10, 30)), (date(2026, 9, 1), date(2026, 10, 31)))
        self.assertEqual(_qb_bimestre_de(date(2028, 2, 15)), (date(2028, 1, 1), date(2028, 2, 29)))
        self.assertEqual(_qb_bimestre_de(date(2027, 2, 28)), (date(2027, 1, 1), date(2027, 2, 28)))

    def test_bimestres_suman_los_viernes_del_anio(self):
        """Los seis bimestres de un año cubren el año sin huecos ni traslapes."""
        for anio, viernes in ((2025, 52), (2026, 52), (2027, 53), (2028, 52)):
            with self.subTest(anio=anio):
                suma = sum(self._pagos_bimestre(anio, mes) for mes in (1, 3, 5, 7, 9, 11))
                self.assertEqual(suma, viernes)
                self.assertEqual(_qb_pagos_en_ventana(date(anio, 1, 1), date(anio, 12, 31), VIERNES), viernes)

    def test_ventana_sin_pagos(self):
        # Lunes 2 a jueves 5 de noviembre de 2026: ningún viernes.
        self.assertEqual(_qb_pagos_en_ventana(date(2026, 11, 2), date(2026, 11, 5), VIERNES), 0)
        self.assertEqual(_qb_pagos_en_ventana(date(2026, 11, 7), date(2026, 11, 6), VIERNES), 0)
        self.assertEqual(_qb_pagos_en_ventana(None, date(2026, 11, 6), VIERNES), 0)
        # Un solo día que sí es viernes.
        self.assertEqual(_qb_pagos_en_ventana(date(2026, 11, 6), date(2026, 11, 6), VIERNES), 1)

    def test_pagos_del_mes(self):
        self.assertEqual(_qb_dias_de_pago(*_qb_mes_de(date(2026, 10, 9)), VIERNES),
                         [date(2026, 10, d) for d in (2, 9, 16, 23, 30)])
        self.assertEqual(_qb_pagos_en_ventana(*_qb_mes_de(date(2026, 9, 1)), VIERNES), 4)
        self.assertEqual(_qb_pagos_en_ventana(*_qb_mes_de(date(2026, 10, 1)), 3), 5)   # jueves
