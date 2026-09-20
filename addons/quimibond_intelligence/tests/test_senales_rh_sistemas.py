# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesRhSistemas(SenalesCommon):

    def test_enterprise_no_instalado_devuelve_none(self):
        for s, modelos in (('aprobacion_rh', ('approval.request', 'hr.leave')), ('evaluacion_vencida', ('hr.appraisal', 'sgi.competence.gap')), ('ticket_abierto', ('helpdesk.ticket',))):
            if not any(m in self.env for m in modelos):
                self.assertIsNone(self.filas(s), s)
