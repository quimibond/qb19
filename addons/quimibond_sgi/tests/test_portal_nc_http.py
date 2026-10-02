# -*- coding: utf-8 -*-
"""57.93.0 (K-07, lo que quedó de 57.91.0): el portal del proveedor probado
por HTTP. Un token inválido o de otra NC lleva a /my sin tocar nada; un
segundo envío no reescribe la respuesta; la URL lleva un código de error y
no texto (un enlace armado no pone texto en una página de Quimibond)."""
import re
from urllib.parse import quote

from odoo.tests import HttpCase, tagged

from .common_users import sgi_set_mast

CSRF = re.compile(r'name="csrf_token" value="([^"]+)"')


@tagged('post_install', '-at_install')
class TestPortalNcHttp(HttpCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_set_mast(cls.env, login='k07_mast')  # a quién avisa la respuesta
        team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        supplier = cls.env['res.partner'].create({
            'name': 'Proveedor portal K07', 'is_company': True, 'supplier_rank': 1,
            'email': 'calidad@k07.test'})
        cls.nc = cls.env['quality.alert'].create({
            'title': 'K07 hilo fuera de especificación', 'team_id': team.id,
            'partner_id': supplier.id})
        cls.nc.action_sgi_send_to_supplier()
        cls.other = cls.env['quality.alert'].create({
            'title': 'K07 otra NC', 'team_id': team.id, 'partner_id': supplier.id})
        cls.other.action_sgi_send_to_supplier()

    def _get(self, alert, token, extra=''):
        return self.url_open('/my/nc/%d?access_token=%s%s' % (alert.id, token, extra),
                             allow_redirects=False)

    def _csrf(self):
        # El token CSRF sale del formulario real (misma sesión del opener).
        page = self._get(self.other, self.other.access_token)
        self.assertEqual(page.status_code, 200)
        return CSRF.search(page.text).group(1)

    def _post(self, alert, token, cause='Lote mezclado', action='Segregar y reponer'):
        return self.url_open('/my/nc/%d/answer' % alert.id, data={
            'csrf_token': self._csrf(), 'access_token': token,
            'cause': cause, 'action': action}, allow_redirects=False)

    def _location(self, response):
        self.assertIn(response.status_code, (301, 302, 303))
        return response.headers['Location']

    def test_01_token_invalido(self):
        self.assertTrue(self._location(self._get(self.nc, 'token-falso')).endswith('/my'))
        self.assertTrue(self._location(self._post(self.nc, 'token-falso')).endswith('/my'))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'enviada')

    def test_02_token_de_otra_nc(self):
        self.assertTrue(self._location(self._get(self.nc, self.other.access_token)).endswith('/my'))
        self._post(self.nc, self.other.access_token)
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'enviada')

    def test_03_respuesta_y_segundo_envio(self):
        token = self.nc.access_token
        self.assertIn('saved=1', self._location(self._post(self.nc, token)))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_state, 'contestada')
        self.assertIn('error=estado', self._location(
            self._post(self.nc, token, cause='Otra causa inventada')))
        self.nc.invalidate_recordset()
        self.assertEqual(self.nc.sgi_supplier_cause, 'Lote mezclado')

    def test_04_codigo_de_error_y_no_texto(self):
        token = self.nc.access_token
        self.assertIn('error=faltan', self._location(self._post(self.nc, token, cause='   ')))
        page = self._get(self.nc, token, '&error=faltan')
        self.assertIn('La causa y la acción son obligatorias', page.text)
        page = self._get(self.nc, token, '&error=' + quote('Llame al 555-0000 para cobrar'))
        self.assertEqual(page.status_code, 200)
        self.assertNotIn('555-0000', page.text)
