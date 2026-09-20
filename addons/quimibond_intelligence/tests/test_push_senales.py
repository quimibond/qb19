# -*- coding: utf-8 -*-
import json

from odoo.tests import TransactionCase, tagged

from ..models.senales import base
from ..models.supabase_client import SupabaseError


class ClienteFalso:
    def __init__(self, config, fallar_en=()):
        self.config, self.fallar_en, self.calls, self.logs = config, set(fallar_en), [], []

    def fetch(self, table, params=None):
        assert table == 'senales_config'
        return self.config

    def rpc_strict(self, fn, params, timeout=120.0):
        self.calls.append((fn, params))
        if fn == 'senales_ingestar' and params['p_senal'] in self.fallar_en:
            raise SupabaseError('HTTP 500 simulado')
        return {'ok': True, 'nuevas': len(params.get('p_filas', []))} if fn == 'senales_ingestar' else {'ok': True}

    def insert(self, table, rows, batch_size=200):
        self.logs.extend(rows)
        return len(rows)

    def close(self):
        pass


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestPushSenales(TransactionCase):

    def setUp(self):
        super().setUp()
        self.ICP = self.env['ir.config_parameter'].sudo()
        self.ICP.set_param('quimibond_intelligence.senales_last_run', '')
        self.sync = self.env['quimibond.sync']
        # Señales de prueba en el registro (se limpian al final).
        self._orig = dict(base.REGISTRO)
        base.REGISTRO.clear()
        base.REGISTRO['_a'] = lambda env, cfg: [base.fila('_a:1', [], valor=1)]
        base.REGISTRO['_b'] = lambda env, cfg: []
        base.REGISTRO['_none'] = lambda env, cfg: None
        base.REGISTRO['_boom'] = lambda env, cfg: 1 / 0
        self.addCleanup(lambda: (base.REGISTRO.clear(), base.REGISTRO.update(self._orig)))
        self.config = [{'senal': s, 'umbrales': {}, 'cada_horas': 1, 'reglas_calidad': {}} for s in ('_a', '_b', '_none', '_boom', '_sin_consulta')]

    def test_manda_un_lote_por_senal_y_termina(self):
        c = ClienteFalso([x for x in self.config if x['senal'] in ('_a', '_b', '_none')])
        n = self.sync._push_senales(c)
        ingestas = [p for f, p in c.calls if f == 'senales_ingestar']
        self.assertEqual([p['p_senal'] for p in ingestas], ['_a', '_b'])          # _none no manda lote
        self.assertEqual(ingestas[0]['p_filas'][0]['clave'], '_a:1')
        self.assertEqual({p['p_corrida'] for p in ingestas}, {c.calls[-1][1]['p_corrida']})  # misma corrida
        self.assertEqual(c.calls[-1][0], 'senales_push_terminado')
        self.assertEqual(n, 1)
        last = json.loads(self.ICP.get_param('quimibond_intelligence.senales_last_run'))
        self.assertEqual(set(last), {'_a', '_b'})

    def test_respeta_turno(self):
        c = ClienteFalso([{'senal': '_a', 'umbrales': {}, 'cada_horas': 6, 'reglas_calidad': {}}])
        self.sync._push_senales(c)
        self.sync._push_senales(c)
        self.assertEqual(len([1 for f, _ in c.calls if f == 'senales_ingestar']), 1, 'fuera de turno no se manda lote')

    def test_error_en_una_senal_no_detiene_las_demas_pero_deja_error(self):
        c = ClienteFalso([x for x in self.config if x['senal'] in ('_a', '_boom', '_b')], fallar_en=('_a',))
        # No usar assertRaises: el de Odoo envuelve el bloque en un savepoint y deshace el sync.log que aquí se verifica.
        try:
            self.sync._push_senales(c)
            self.fail('debió levantar SupabaseError')
        except SupabaseError:
            pass
        self.assertEqual([p['p_senal'] for f, p in c.calls if f == 'senales_ingestar'], ['_a', '_b'])  # _boom no manda; _b sí
        self.assertEqual(c.calls[-1][0], 'senales_push_terminado')
        log = self.env['quimibond.sync.log'].search([('name', '=', 'Push señales con errores')], limit=1)
        self.assertTrue(log and '_a' in log.summary and '_boom' in log.summary)
        last = json.loads(self.ICP.get_param('quimibond_intelligence.senales_last_run'))
        self.assertNotIn('_a', last, 'la fallida se reintenta en el siguiente push')

    def test_senal_sin_consulta_se_ignora_con_aviso(self):
        c = ClienteFalso([x for x in self.config if x['senal'] == '_sin_consulta'])
        self.assertEqual(self.sync._push_senales(c), 0)
        self.assertEqual([f for f, _ in c.calls], ['senales_push_terminado'])

    def test_push_to_supabase_incluye_senales(self):
        self.assertIn('senales', self.sync.PUSH_MODELS)
        self.assertIn('senales', self.sync.FULL_PUSH_METHODS)
        self.assertIn('senales', self.sync._push_models_allowed())

    def test_push_users_manda_buzon_personas(self):
        class C(ClienteFalso):
            def upsert(self, table, rows, on_conflict, batch_size=200):
                return len(rows)

            def rpc(self, fn, params):
                self.calls.append((fn, params))
                return 0
        c = C([])
        self.sync._push_users(c)
        fn, params = [x for x in c.calls if x[0] == 'buzon_personas_reemplazar'][0]
        self.assertIsInstance(params['p_filas'], list)  # [] si qb_memoria no está; filas {buzon, odoo_user_id, area} si está
