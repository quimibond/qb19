# -*- coding: utf-8 -*-
"""57.6.0 (entrega 2, e2-legado): D-13 (E1-02 con ``acuerdos_rxd``), D-16
(sin la encuesta de auditoría legado) y B-010 (modos de cálculo retirados)."""
from datetime import date

from odoo.tests import TransactionCase, tagged

RETIRED_MODES = (
    'desperdicio', 'desperdicio_scrap', 'disponibilidad_mantto', 'preventivo_cumplido',
    'plantilla_rh', 'inventario_ciclico', 'compras_sin_devolucion', 'margen_ventas',
    'compras_vs_ventas',
)


@tagged('post_install', '-at_install')
class TestLegado(TransactionCase):

    def _review(self, state='realizada'):
        review = self.env['sgi.management.review'].create({
            'period_from': date(2044, 1, 1), 'period_to': date(2044, 6, 30)})
        review.state = state
        return review

    def _agreement(self, review, deadline, done=None):
        return self.env['sgi.management.review.agreement'].create({
            'review_id': review.id, 'name': 'Acuerdo %s' % deadline,
            'responsible_id': self.env.user.id, 'deadline': deadline,
            'done_date': done})

    # ---- D-13 ----------------------------------------------------------
    def test_01_acuerdos_rxd_cuenta_acuerdos_a_tiempo(self):
        review = self._review()
        self._agreement(review, date(2044, 3, 10), done=date(2044, 3, 9))    # a tiempo
        self._agreement(review, date(2044, 3, 20), done=date(2044, 3, 25))   # tarde
        self._agreement(review, date(2044, 3, 28))                            # sin cumplir
        self._agreement(review, date(2044, 4, 5), done=date(2044, 4, 1))     # otro mes
        draft = self._review(state='borrador')
        self._agreement(draft, date(2044, 3, 15), done=date(2044, 3, 1))     # borrador: no cuenta
        indicator = self.env['sgi.indicator'].create({
            'code': 'TST-RXD', 'name': 'Acuerdos RxD', 'calc_mode': 'acuerdos_rxd'})
        self.assertEqual(indicator._sgi_compute_value(date(2044, 3, 1), date(2044, 3, 31)), 33.33)
        self.assertIsNone(indicator._sgi_compute_value(date(2044, 5, 1), date(2044, 5, 31)))
        self.assertTrue(indicator._sgi_compute_note(date(2044, 5, 1), date(2044, 5, 31)))
        self.assertIn('Revisión por la Dirección', indicator.source_info)
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': date(2044, 3, 1),
            'value': 33.33, 'state': 'capturado'})
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'sgi.management.review.agreement')
        self.assertEqual(self.env[action['res_model']].search_count(action['domain']), 3)

    def test_02_e1_02_se_activa_solo_si_sigue_manual_sin_formula(self):
        Indicator = self.env['sgi.indicator']
        # 57.66.0: también el archivado, y bajando el cambio antes del
        # create() (índice único de la clave; producción tiene su E1-02).
        Indicator.with_context(active_test=False).search(
            [('code', '=', 'E1-02')]).write({'code': 'E1-02-OTRO'})
        Indicator.flush_model(['code'])
        manual = Indicator.create({'code': 'E1-02', 'name': 'Acuerdos', 'calc_mode': 'manual'})
        self.assertEqual(Indicator._sgi_activate_acuerdos_rxd(), [manual.id])
        self.assertEqual(manual.calc_mode, 'acuerdos_rxd')
        self.assertEqual(Indicator._sgi_activate_acuerdos_rxd(), [], "Idempotente.")
        # Lo que MAST decidió (otro modo) no se toca.
        manual.calc_mode = 'configurable'
        self.assertEqual(Indicator._sgi_activate_acuerdos_rxd(), [])
        self.assertEqual(manual.calc_mode, 'configurable')

    # ---- D-16 ----------------------------------------------------------
    def test_03_auditoria_sin_encuesta_legado(self):
        Audit = self.env['sgi.audit']
        for name in ('survey_id', 'survey_input_ids'):
            self.assertNotIn(name, Audit._fields)
        self.assertNotIn('survey_line_id', self.env['sgi.audit.finding']._fields)
        for method in ('action_answer_checklist', 'action_generate_findings_from_checklist'):
            self.assertFalse(hasattr(Audit, method))
        # La encuesta legado ya no es del módulo (su XML ID pasó a __export__).
        self.assertFalse(self.env['ir.model.data'].sudo().search_count([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'survey.survey'),
            ('name', '=', 'sgi_survey_' + 'audit_9001')]))
        self.assertNotIn('sgi_revision_legacy', self.env['documents.document']._fields)
        # La evaluación del auditor (encuesta de producción) se queda.
        self.assertTrue(hasattr(Audit, 'action_evaluate_auditors'))

    # ---- B-010 ---------------------------------------------------------
    def test_04_modos_retirados(self):
        modes = dict(self.env['sgi.indicator']._fields['calc_mode'].selection)
        for mode in RETIRED_MODES:
            self.assertNotIn(mode, modes)
        self.assertIn('acuerdos_rxd', modes)
        self.assertNotIn('sgi_waste_categ_id', self.env['res.config.settings']._fields)
        # Las siembras ya no los usan (una base nueva se parece a producción).
        for xmlid, mode in (('sgi_ind_desperdicio', 'desperdicio_kg'),
                            ('sgi_ind_disponibilidad', 'manual'),
                            ('sgi_ind_preventivo', 'configurable'),
                            ('sgi_ind_cobertura_plantilla', 'manual'),
                            ('sgi_ind_ex_margen', 'margen_ebitda'),
                            ('sgi_ind_ex_compras_ventas', 'compras_mp_vs_ventas')):
            indicator = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if indicator:  # copia de producción: MAST pudo cambiarlo
                self.assertNotIn(indicator.calc_mode, RETIRED_MODES)
