# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesCalidad(SenalesCommon):

    def test_sgi_no_instalado_devuelve_none(self):
        if 'sgi.risk' in self.env:
            self.skipTest('SGI instalado')
        for s in ('indicador_rojo', 'accion_correctiva_vencida', 'calibracion_vencida', 'legal_incumplido', 'riesgo_sin_tratar', 'ppap_rechazado', 'auditoria_pendiente', 'fuente_sgi_apagada'):
            self.assertIsNone(self.filas(s), s)

    def test_nc_abierta_sin_quality_devuelve_none(self):
        if 'quality.alert' not in self.env:
            self.assertIsNone(self.filas('nc_abierta'))
