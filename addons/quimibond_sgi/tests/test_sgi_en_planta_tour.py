# -*- coding: utf-8 -*-
"""57.94.0 (U-01): recorrido de la pantalla «SGI en planta» en el navegador."""
from odoo.tests import HttpCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestSgiEnPlantaTour(HttpCase):

    def test_firma_de_acuse_en_la_tableta(self):
        env = self.env
        sgi_hide_real_documents(env)
        company = env['sgi.config']._sgi_company()
        dept = env['hr.department'].create({'name': 'ZK Tour', 'company_id': company.id})
        worker = env['hr.employee'].create({'name': 'ZK Operadora del tour', 'department_id': dept.id,
                                            'company_id': company.id, 'pin': '2468'})
        user = new_test_user(env, login='zk_tour_tableta', password='zk_tour_tableta',
                             groups='base.group_user,quimibond_sgi.group_sgi_floor_tablet')
        tablet = env['sgi.floor.tablet'].create({'name': 'ZK Tableta del tour', 'user_id': user.id,
                                                 'department_ids': [(6, 0, dept.ids)]})
        doc = env['documents.document'].create({
            'name': 'ZK Instructivo del tour', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A92-09', 'sgi_state': 'vigente'})
        ack = env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': worker.id})
        self.start_tour('/odoo/action-quimibond_sgi.sgi_floor_kiosk_action', 'sgi_floor_kiosk_tour',
                        login='zk_tour_tableta')
        ack.invalidate_recordset()
        self.assertEqual(ack.state, 'leido')
        self.assertEqual(ack.sgi_pin_tablet_id, tablet)
