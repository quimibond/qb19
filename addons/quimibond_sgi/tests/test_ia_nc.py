# -*- coding: utf-8 -*-
"""57.100.0 (sección 7 del reporte, «IA que propone y la persona decide»):
la IA propone cláusula, clasificación y borrador de porqués e Ishikawa en
campos de sugerencia; la persona decide con un botón. Nunca escribe la causa
raíz ni cambia la etapa. Sale apagada (quimibond_sgi.ai_enabled).

Sin red: el método que llama al proveedor (o el cliente de Anthropic) se
sustituye en cada prueba. Ninguna prueba lee una llave real."""
import json
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools import mute_logger

from ..models.sgi_ai import sgi_ai_scrub
from .common_users import sgi_set_mast

_LOGGER = 'odoo.addons.quimibond_sgi.models.sgi_ai'


def _msg(text, stop='end_turn'):
    """Respuesta falsa con la forma de anthropic.types.Message."""
    block = type('B', (), {'type': 'text', 'text': text})()
    usage = type('U', (), {'input_tokens': 10, 'output_tokens': 5})()
    return type('M', (), {'stop_reason': stop, 'content': [block], 'usage': usage})()


@tagged('post_install', '-at_install')
class TestIaNc(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Param = env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_sgi.ai_enabled', 'True')
        cls.Param.set_param('quimibond_sgi.ai_backend', 'anthropic')
        cls.Param.set_param('quimibond_sgi.ai_model', 'claude-opus-5-5')
        cls.mast = sgi_set_mast(env, login='z_ia_mast')
        cls.user = new_test_user(
            env, login='z_ia_user', name='Usuaria IA Prueba',
            groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner = env['hr.employee'].create({'name': 'Dueño IA', 'user_id': cls.mast.id})
        cls.process = env['sgi.process'].create(
            {'code': 'ZIA', 'name': 'Proceso IA', 'owner_id': owner.id})
        cls.team = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.clause = env['sgi.norm.clause'].search([('norm_id.active', '=', True)], limit=1)
        cls.customer = env['res.partner'].create(
            {'name': 'Cliente Secreto SA', 'email': 'compras@secreto.test'})
        cls.nc = env['quality.alert'].create({
            'title': 'ZIA NC de prueba', 'team_id': cls.team.id,
            'stage_id': cls.stage_open.id, 'sgi_process_id': cls.process.id,
            'partner_id': cls.customer.id, 'user_id': cls.user.id,
            'sgi_deviation': 'Rollo fuera de gramaje; avisó compras@secreto.test al 55 1234 5678.',
        })

    def _fake(self, payload):
        Client = type(self.env['sgi.ai.client'])
        return patch.object(Client, '_sgi_ai_request', return_value=(payload, 'modelo-prueba'))

    def _ok(self):
        return {'clausula_id': self.clause.id, 'clasificacion': 'menor',
                'motivo': 'Falla de método', 'porques': ['a', 'b', 'c'],
                'ishikawa': {'mano_de_obra': '', 'metodo': 'm', 'maquina': '', 'material': '',
                             'medicion': '', 'medio_ambiente': ''}}

    def test_00_nc_de_prueba(self):
        self.assertTrue(self.nc.sgi_folio)
        self.assertTrue(self.clause)

    def test_01_apagada_por_omision(self):
        self.Param.set_param('quimibond_sgi.ai_enabled', 'False')
        self.nc.invalidate_recordset(['sgi_ai_available'])
        self.assertFalse(self.nc.with_user(self.user).sgi_ai_available)
        with self.assertRaises(UserError):
            self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.Param.set_param('quimibond_sgi.ai_enabled', 'True')
        self.nc.invalidate_recordset(['sgi_ai_available'])
        self.assertTrue(self.nc.with_user(self.user).sgi_ai_available)

    def test_02_sugerencia_no_toca_campos_de_decision(self):
        fields_ = ('sgi_classification', 'sgi_norm_clause_id', 'sgi_root_cause', 'stage_id',
                   'sgi_why_1', 'sgi_ishikawa_notes')
        before = {f: self.nc[f] for f in fields_}
        with self._fake(self._ok()):
            self.nc.with_user(self.user).action_sgi_ai_suggest()
        for f, v in before.items():
            self.assertEqual(self.nc[f], v, f)
        self.assertEqual(self.nc.sgi_ai_clause_id, self.clause)
        self.assertEqual(self.nc.sgi_ai_classification, 'menor')
        self.assertIn('2. b', self.nc.sgi_ai_whys)
        self.assertIn('Método: m', self.nc.sgi_ai_ishikawa)
        self.assertEqual(self.nc.sgi_ai_model, 'modelo-prueba')
        self.assertTrue(self.nc.sgi_ai_date)
        self.assertTrue(any('Es un borrador' in (b or '') for b in self.nc.message_ids.mapped('body')))

    def test_03_usar_sugerencia_lo_hace_la_persona(self):
        with self._fake(self._ok()):
            self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.nc.with_user(self.user).action_sgi_ai_use_classification()
        self.assertEqual(self.nc.sgi_classification, 'menor')
        self.assertEqual(self.nc.sgi_norm_clause_id, self.clause)
        self.assertFalse(self.nc.sgi_root_cause)
        self.assertEqual(self.nc.stage_id, self.stage_open)

    def test_04_copiar_porques_solo_vacios_y_nunca_causa_raiz(self):
        self.nc.write({'sgi_why_1': 'Mío'})
        with self._fake(self._ok()):
            self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.nc.with_user(self.user).action_sgi_ai_use_whys()
        self.assertEqual(self.nc.sgi_why_1, 'Mío')
        self.assertEqual(self.nc.sgi_why_2, 'b')
        self.assertEqual(self.nc.sgi_why_3, 'c')
        self.assertFalse(self.nc.sgi_why_4)
        self.assertIn('Método: m', self.nc.sgi_ishikawa_notes)
        self.assertFalse(self.nc.sgi_root_cause)

    def test_05_lo_que_se_manda_no_trae_datos_personales(self):
        system, user_text = self.nc._sgi_ai_payload()
        blob = system + user_text
        self.assertNotIn('Cliente Secreto', blob)
        self.assertNotIn('compras@secreto.test', blob)
        self.assertNotIn('1234 5678', blob)
        self.assertNotIn(self.user.name, blob)
        self.assertNotIn(self.mast.name, blob)
        self.assertIn('Rollo fuera de gramaje', blob)
        self.assertIn('[%d]' % self.clause.id, blob)

    def test_06_scrub(self):
        self.assertEqual(sgi_ai_scrub('Escriba a ana@x.mx o al 55 1234 5678'),
                         'Escriba a [correo] o al [teléfono]')
        self.assertIn('[RFC]', sgi_ai_scrub('RFC PNT920101AB1'))
        self.assertIn('[teléfono]', sgi_ai_scrub('Tel. (55) 1234-5678'))
        # Fechas y folios no son teléfonos.
        for text in ('Lote del 2026-10-05', 'Folio NCI-2026-0012', 'Pedido del 05/10/2026'):
            self.assertEqual(sgi_ai_scrub(text), text)

    def test_07_clausula_inventada_se_descarta(self):
        bad = dict(self._ok(), clausula_id=999999999, clasificacion='gravisima')
        with self._fake(bad):
            self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.assertFalse(self.nc.sgi_ai_clause_id)
        self.assertFalse(self.nc.sgi_ai_classification)
        self.assertTrue(self.nc.sgi_ai_whys)

    def test_07b_nada_utilizable(self):
        with self._fake({'clausula_id': 0, 'clasificacion': 'x', 'porques': []}):
            with self.assertRaises(UserError):
                self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.assertFalse(self.nc.sgi_ai_date)

    def test_08_falla_del_proveedor_es_aviso_no_error(self):
        Client = type(self.env['sgi.ai.client'])
        with patch.object(Client, '_sgi_ai_request', side_effect=UserError("La IA no respondió.")):
            with self.assertRaises(UserError):
                self.nc.with_user(self.user).action_sgi_ai_suggest()
        self.assertFalse(self.nc.sgi_ai_date)

    def test_09_nc_cerrada_o_sin_folio_no_pide(self):
        team = self.env['quality.alert.team'].create({'name': 'ZIA equipo sin folio'})
        loose = self.env['quality.alert'].create({'title': 'ZIA sin folio', 'team_id': team.id})
        self.assertFalse(loose.sgi_folio)
        with self._fake(self._ok()):
            with self.assertRaises(UserError):
                loose.with_user(self.user).action_sgi_ai_suggest()
        cancel = self.env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')
        self.env.flush_all()
        self.env.cr.execute("UPDATE quality_alert SET stage_id = %s WHERE id = %s",
                            (cancel.id, self.nc.id))
        self.nc.invalidate_recordset()
        with self._fake(self._ok()):
            with self.assertRaises(UserError):
                self.nc.with_user(self.user).action_sgi_ai_suggest()

    def test_10_campos_de_ia_no_se_escriben_por_rpc(self):
        with self.assertRaises(UserError):
            self.nc.with_user(self.user).write({'sgi_ai_classification': 'mayor'})
        with self.assertRaises(UserError):
            self.nc.with_user(self.mast).write({'sgi_ai_whys': 'inventado'})

    def test_11_anthropic_arma_la_llamada_sin_red(self):
        """El cliente de Anthropic se sustituye: revisa modelo, límite de
        salida, respaldo del servidor y que no mande «thinking» ni
        «temperature»."""
        Client = type(self.env['sgi.ai.client'])
        with patch.object(Client, '_sgi_anthropic_client') as factory:
            factory.return_value.messages.create.return_value = _msg(json.dumps(self._ok()))
            data, model = self.env['sgi.ai.client']._sgi_ai_request('s', 'u', {})
        kwargs = factory.return_value.messages.create.call_args.kwargs
        self.assertEqual(model, 'claude-opus-5-5')
        self.assertEqual(kwargs['model'], 'claude-opus-5-5')
        self.assertEqual(kwargs['max_tokens'], 16000)
        self.assertNotIn('thinking', kwargs)
        self.assertNotIn('temperature', kwargs)
        self.assertNotIn('stream', kwargs)
        self.assertEqual(kwargs['extra_body'], {'fallbacks': 'default'})
        self.assertEqual(kwargs['extra_headers'],
                         {'anthropic-beta': 'server-side-fallback-2026-07-01'})
        self.assertEqual(data['clasificacion'], 'menor')

    def test_12_rechazo_o_corte_es_aviso(self):
        Client = type(self.env['sgi.ai.client'])
        for stop in ('refusal', 'max_tokens'):
            with patch.object(Client, '_sgi_anthropic_client') as factory, \
                    mute_logger(_LOGGER):
                factory.return_value.messages.create.return_value = _msg('{}', stop=stop)
                with self.assertRaises(UserError):
                    self.env['sgi.ai.client']._sgi_ai_request('s', 'u', {})
        with patch.object(Client, '_sgi_anthropic_client') as factory, mute_logger(_LOGGER):
            factory.return_value.messages.create.return_value = _msg('no es JSON')
            with self.assertRaises(UserError):
                self.env['sgi.ai.client']._sgi_ai_request('s', 'u', {})
        with patch.object(Client, '_sgi_anthropic_client') as factory, mute_logger(_LOGGER):
            factory.return_value.messages.create.side_effect = RuntimeError('sin red')
            with self.assertRaises(UserError):
                self.env['sgi.ai.client']._sgi_ai_request('s', 'u', {})
