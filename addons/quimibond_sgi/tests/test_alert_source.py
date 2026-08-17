# -*- coding: utf-8 -*-
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAlertSource(TransactionCase):
    """Registro de fuentes de NC automáticas: interruptor, rastro y estampado."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Alert = cls.env['quality.alert']
        cls.Source = cls.env['sgi.alert.source']
        cls.source = cls.env.ref('quimibond_sgi.sgi_alert_source_indicator_red')
        cls.manual_source = cls.env.ref('quimibond_sgi.sgi_alert_source_audit_finding')

    def _vals(self, title="NC de prueba"):
        return {'title': title, 'sgi_origin_type': 'proceso'}

    def test_01_enabled_creates_and_stamps_source(self):
        self.source.enabled = True
        alert = self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals())
        self.assertTrue(alert, "Con la fuente activa la NC debe crearse.")
        self.assertEqual(alert.sgi_source_id, self.source,
                         "La NC debe quedar estampada con la fuente que la generó.")

    def test_02_disabled_returns_empty_and_creates_nothing(self):
        self.source.enabled = False
        before = self.Alert.search_count([])
        alert = self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals())
        self.assertFalse(alert, "Con la fuente apagada debe devolver recordset vacío.")
        self.assertEqual(self.Alert.search_count([]), before,
                         "No debe crearse ninguna NC con la fuente apagada.")

    def test_03_suppression_leaves_audit_trail(self):
        self.source.enabled = False
        start = self.source.suppressed_count
        self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals())
        self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals())
        self.assertEqual(self.source.suppressed_count, start + 2,
                         "Cada omisión debe contarse para poder dimensionarla.")
        self.assertTrue(self.source.last_suppressed_on)

    def test_03b_reentrant_caller_does_not_inflate_the_count(self):
        """El cron reevalúa el mismo hecho cada corrida: una omisión, un conteo."""
        self.source.enabled = False
        start = self.source.suppressed_count
        self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals())
        for _ in range(5):  # corridas siguientes del cron sobre la misma medición
            self.Alert.sgi_auto_create(
                'indicador_semaforo_rojo', self._vals(), count_suppression=False)
        self.assertEqual(self.source.suppressed_count, start + 1,
                         "Sólo la primera omisión del hecho debe contarse.")
        self.assertTrue(self.source.last_suppressed_on,
                        "La fecha de última omisión sí debe refrescarse siempre.")

    def test_04_toggle_is_tracked_in_chatter(self):
        """El auditor debe poder ver quién apagó la fuente y cuándo."""
        before = len(self.source.message_ids)
        self.source.enabled = not self.source.enabled
        # El tracking de mail.thread se materializa en el precommit del cursor,
        # no en el flush. En un test no hay commit, así que hay que dispararlo a
        # mano; y `message_ids` se invalida aparte porque mail.message.res_id es
        # un entero, no un m2o, y Odoo no puede refrescar el inverso solo.
        self.env.flush_all()
        self.env.cr.precommit.run()
        self.source.invalidate_recordset(['message_ids'])
        self.assertGreater(len(self.source.message_ids), before,
                           "Apagar/encender una fuente debe registrarse en el historial.")
        self.assertTrue(
            self.source.message_ids.filtered(lambda m: m.tracking_value_ids),
            "El historial debe guardar el cambio del interruptor como valor "
            "rastreado, no una nota suelta: es lo que sustenta la auditoría.")

    def test_05_manual_source_disabled_warns_instead_of_silence(self):
        self.manual_source.enabled = False
        with self.assertRaises(UserError):
            self.Alert.sgi_auto_create('auditoria_hallazgo', self._vals())

    def test_06_unknown_code_fails_open(self):
        """Perder una NC por un dato faltante es peor que registrar una de más."""
        alert = self.Alert.sgi_auto_create('fuente_que_no_existe', self._vals())
        self.assertTrue(alert, "Una clave no declarada debe crear la NC igual.")
        self.assertFalse(alert.sgi_source_id)

    def test_07_code_is_unique(self):
        from psycopg2 import IntegrityError
        from odoo.tools import mute_logger
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'):
            with self.env.cr.savepoint():
                self.Source.sudo().create({
                    'code': 'indicador_semaforo_rojo', 'name': 'Duplicada'})

    def test_08_alert_count_per_source(self):
        self.source.enabled = True
        start = self.source.alert_count
        self.Alert.sgi_auto_create('indicador_semaforo_rojo', self._vals("NC contada"))
        self.source.invalidate_recordset(['alert_count'])
        self.assertEqual(self.source.alert_count, start + 1)
