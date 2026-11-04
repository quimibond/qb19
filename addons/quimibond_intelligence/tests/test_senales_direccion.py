# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesDireccion(SenalesCommon):

    def _act(self, hace):
        return self.env['mail.activity'].create({'res_model_id': self.env['ir.model']._get_id('res.partner'), 'res_id': self.cliente.id,
                                                 'activity_type_id': self.env.ref('mail.mail_activity_data_todo').id,
                                                 'user_id': self.env.user.id, 'date_deadline': self.hace(hace), 'summary': 'x'})

    def test_carga_actividades_separa_zombis(self):
        a = self._act(5)
        z = self._act(200)
        self._act(-1)
        filas = self.filas('carga_actividades', {'reglas_calidad': {'zombie_dias': 180}})
        viva = self.fila_de(filas, 'carga_actividades:user:%d' % self.env.user.id)
        zombie = self.fila_de(filas, 'carga_actividades:user:%d:zombie' % self.env.user.id)
        self.assertEqual([d['id'] for d in viva['documentos']], [a.id])
        self.assertEqual([d['id'] for d in zombie['documentos']], [z.id])
        self.assertEqual(zombie['payload']['fecha_base'], str(self.hace(200)))
        self.assertEqual(viva['payload']['grupo'], 'vivas')

    def test_obligacion_legado_lote_vacio(self):
        # qb_obligation se retiró: la señal manda un lote vacío para resolver lo que quedó en Supabase.
        self.assertEqual(self.filas('obligacion_legado'), [])
