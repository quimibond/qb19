# -*- coding: utf-8 -*-
"""ISR sobre el acumulado del mes: lo que NOI hace y Odoo no.

NOI no calcula el ISR de cada periodo por separado. Calcula el ISR del mes
calendario completo (tarifa mensual sobre el gravable acumulado del mes, con
el subsidio al empleo mensual dentro del mismo cálculo) y en la **última
nómina del mes** retiene la diferencia contra lo ya retenido en los periodos
anteriores del mismo mes. Verificado contra los reportes de NOI de las
quincenas 18 y 19 de Toluca (2026): sumando el ISR de las dos quincenas y
dividiéndolo entre el gravable del mes sale una curva estrictamente monótona
en las 50 personas, y ``tarifa_mensual(grav18 + grav19) − subsidio_mes − ISR18``
reproduce el ISR de la quincena 19 con residuo máximo de 1 centavo.

Odoo (regla ``ISR`` de «Paga regular») calcula cada periodo por separado con
la tarifa diaria × días del periodo. Eso reproduce a NOI dentro de 4 centavos
en el primer periodo del mes y falla en el último: cuando las dos quincenas
son parejas el efecto es de centavos; cuando no lo son, de cientos de pesos
(Jessica Francisco, Q18 29,975 / Q19 9,758: Odoo 1,139.53 vs NOI 834.11).

Aquí viven los tres cálculos que las reglas ``ISR`` y ``SUBSIDY`` de la
estructura llaman por ``payslip._qb_isr_mensual(gross)``:

1. **Cuál es la última nómina del mes** (``_qb_isr_es_ultima_del_mes``):
   quincenal, la que termina el último día del mes; semanal, la semana cuyo
   ``date_to`` cae en el mes y después de la cual no hay otra semana que
   termine en ese mes (``date_to + 7 días`` ya es de otro mes); mensual,
   siempre. Cualquier otra periodicidad no se ajusta. Una semana que cruza de
   mes pertenece al mes de su ``date_to`` (septiembre 2026: semanas 37 a 40;
   la 41, del 28-sep al 4-oct, es de octubre).

2. **De dónde sale el acumulado** (``_qb_isr_recibos_previos_del_mes``): los
   recibos del mismo empleado, misma estructura, misma compañía, sin nota de
   crédito, con ``date_to`` en el mismo mes y anterior al del recibo, en
   estado validado o pagado. Si alguno sigue en borrador, **no se ajusta** y
   el periodo se calcula como hoy: ajustar contra un acumulado incompleto es
   peor que no ajustar. Lo mismo si el contrato empezó antes del mes y no hay
   ningún recibo previo (falta uno). Para las corridas piloto, que viven en
   borrador, el parámetro ``quimibond_nomina.isr_mensual_incluye_borrador``
   = ``1`` cuenta también los borradores (y, si hay dos recibos del mismo
   periodo, toma el más reciente y lo avisa en el log).

3. **La tarifa mensual y el subsidio** vienen de los parámetros de la
   localización, nunca escritos a mano: ``l10n_mx_isr_tables['monthly']``,
   ``l10n_mx_subsidy_salary_limit``, ``l10n_mx_uma['monthly']`` ×
   ``l10n_mx_uma_percentage_for_subsidy``. El subsidio mensual aplica si el
   gravable del mes no rebasa el límite y nunca excede el ISR del mes (es un
   crédito contra el impuesto, no se entrega). Lo retenido antes en el mes es
   la suma de las líneas ``ISR`` e ``ISR_ADJUSTMENT`` de los recibos previos;
   el subsidio ya aplicado, la suma de sus líneas ``SUBSIDY``.

En la última nómina del mes::

    isr_del_periodo      = tarifa_mensual(gravable_mes) − Σ ISR ya retenido en el mes
    subsidio_del_periodo = min(subsidio_mes, tarifa_mensual) − Σ subsidio ya aplicado

y las dos pueden salir negativas: un ISR negativo es una devolución (la
«compensación de ISR» de NOI) y un subsidio negativo recupera el que se
aplicó en un periodo anterior cuando el mes completo ya rebasa el límite.

``ISR_ADJUSTMENT`` (la entrada manual «Ajuste del ISR», tipo 10) sigue
existiendo y **se suma** al ISR del periodo como hoy; el ajuste automático no
la sustituye. En los periodos que no son el último del mes cuenta como
retenido para el ajuste de fin de mes. No hace falta capturarla para cuadrar
el mes: el ajuste automático ya lo hace."""
import logging
from datetime import timedelta

from odoo import models
from odoo.tools import float_round

_logger = logging.getLogger(__name__)

PARAM_INCLUYE_BORRADOR = 'quimibond_nomina.isr_mensual_incluye_borrador'
ESTADOS_CERRADOS = ('validated', 'paid')
CODIGO_GRAVABLE = 'GROSS'
CODIGOS_RETENIDO = ('ISR', 'ISR_ADJUSTMENT')
CODIGO_SUBSIDIO = 'SUBSIDY'


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    # ------------------------------------------------------------------
    # 1. ¿Es la última nómina del mes?
    # ------------------------------------------------------------------
    def _qb_isr_es_ultima_del_mes(self):
        """Ver el criterio en la cabecera del módulo. Explícito a propósito:
        no se deduce del lote ni de cuántos recibos haya."""
        self.ensure_one()
        fin = self.date_to
        periodicidad = self.version_id.schedule_pay
        if not fin or not periodicidad:
            return False
        if periodicidad == 'bi-weekly':
            return (fin + timedelta(days=1)).month != fin.month
        if periodicidad == 'weekly':
            return (fin + timedelta(days=7)).month != fin.month
        if periodicidad == 'monthly':
            return True
        return False

    # ------------------------------------------------------------------
    # 2. Los recibos anteriores del mismo mes
    # ------------------------------------------------------------------
    def _qb_isr_incluye_borradores(self):
        valor = (self.env['ir.config_parameter'].sudo().get_param(PARAM_INCLUYE_BORRADOR) or '').strip().lower()
        return valor in ('1', 'true', 'si', 'sí', 'yes')

    def _qb_isr_recibos_previos_del_mes(self):
        """``(recibos, completo)``: los recibos anteriores del mes y si el
        acumulado se puede usar. ``completo`` es False cuando alguno sigue en
        borrador (salvo con el parámetro de piloto) o cuando el contrato ya
        corría antes del mes y no hay ningún recibo previo."""
        self.ensure_one()
        fin = self.date_to
        inicio_mes = fin.replace(day=1)
        Payslip = self.env['hr.payslip'].sudo()
        recibos = Payslip.search([
            ('employee_id', '=', self.employee_id.id),
            ('struct_id', '=', self.struct_id.id),
            ('company_id', '=', self.company_id.id),
            ('credit_note', '=', False),
            ('state', '!=', 'cancel'),
            ('id', '!=', self.id),
            ('date_to', '>=', inicio_mes),
            ('date_to', '<', fin),
        ], order='date_to, id')
        incluye_borrador = self._qb_isr_incluye_borradores()
        abiertos = recibos.filtered(lambda r: r.state not in ESTADOS_CERRADOS)
        if abiertos and not incluye_borrador:
            _logger.info('quimibond_nomina: recibo %s: %d recibo(s) del mes en borrador (%s); el ISR '
                         'se calcula por periodo, sin ajuste mensual', self.id, len(abiertos),
                         ', '.join(abiertos.mapped('display_name')))
            return recibos, False
        if incluye_borrador:
            por_periodo = {}
            for r in recibos:
                previo = por_periodo.get((r.date_from, r.date_to))
                if previo:
                    _logger.warning('quimibond_nomina: recibo %s: dos recibos del periodo %s–%s (%s y %s); '
                                    'para el acumulado del mes se toma el más reciente (%s)',
                                    self.id, r.date_from, r.date_to, previo.id, r.id, r.id)
                por_periodo[(r.date_from, r.date_to)] = r
            recibos = Payslip.browse(sorted(r.id for r in por_periodo.values()))
        if not recibos:
            version = self.version_id
            inicio_contrato = version.contract_date_start or version.date_start
            if inicio_contrato and inicio_contrato < inicio_mes:
                _logger.warning('quimibond_nomina: recibo %s: el contrato corre desde %s y no hay ningún '
                                'recibo previo del mes; el ISR se calcula por periodo, sin ajuste mensual',
                                self.id, inicio_contrato)
                return recibos, False
        return recibos, True

    # ------------------------------------------------------------------
    # 3. Tarifa mensual y subsidio
    # ------------------------------------------------------------------
    @staticmethod
    def _qb_isr_tarifa(base, tabla):
        """Cuota fija + excedente × tasa del renglón donde cae ``base``."""
        for low, high, fix, rate in tabla:
            if low <= base <= high:
                return (base - low) * rate + fix
        return 0.0

    def _qb_isr_subsidio_mes(self, gravable_mes, fecha):
        """Subsidio al empleo del mes: el monto mensual si el gravable del mes
        no rebasa el límite; 0 si lo rebasa. Sin topar todavía al ISR."""
        limite = self._rule_parameter('l10n_mx_subsidy_salary_limit', fecha)
        if not (0 < gravable_mes <= limite):
            return 0.0
        uma = self._rule_parameter('l10n_mx_uma', fecha)
        pct = self._rule_parameter('l10n_mx_uma_percentage_for_subsidy', fecha)
        # Igual que SUBSIDY_CURRENT_MONTH para la nómina mensual.
        return float_round(uma['monthly'] * pct, precision_digits=2, rounding_method='UP')

    def _qb_isr_mensual(self, gravable_periodo):
        """El ajuste de fin de mes para este recibo, o ``None`` si no aplica
        (no es la última nómina del mes, o el acumulado no está completo).

        Devuelve un dict con ``isr`` y ``subsidio`` del periodo (lo que las
        reglas escriben; ambos pueden ser negativos) y, para revisar,
        ``gravable_mes``, ``isr_mes``, ``subsidio_mes``, ``retenido_previo``,
        ``subsidio_previo`` y ``previos`` (ids de los recibos usados)."""
        self.ensure_one()
        if not self._qb_isr_es_ultima_del_mes():
            return None
        previos, completo = self._qb_isr_recibos_previos_del_mes()
        if not completo:
            return None
        lineas = previos.mapped('line_ids')

        def suma(codigos):
            return sum(l.total for l in lineas if l.code in codigos)

        gravable_previo = suma((CODIGO_GRAVABLE,))
        retenido_previo = -suma(CODIGOS_RETENIDO)      # las deducciones son negativas
        subsidio_previo = suma((CODIGO_SUBSIDIO,))
        gravable_mes = gravable_previo + (gravable_periodo or 0.0)
        fecha = self.date_to
        tablas = self._rule_parameter('l10n_mx_isr_tables', fecha)
        isr_mes = float_round(self._qb_isr_tarifa(gravable_mes, tablas['monthly']), precision_digits=2)
        subsidio_mes = min(self._qb_isr_subsidio_mes(gravable_mes, fecha), isr_mes)
        res = {
            'isr': float_round(isr_mes - retenido_previo, precision_digits=2),
            'subsidio': float_round(subsidio_mes - subsidio_previo, precision_digits=2),
            'gravable_mes': gravable_mes,
            'isr_mes': isr_mes,
            'subsidio_mes': subsidio_mes,
            'retenido_previo': retenido_previo,
            'subsidio_previo': subsidio_previo,
            'previos': previos.ids,
        }
        _logger.debug('quimibond_nomina: recibo %s ajuste mensual de ISR: %s', self.id, res)
        return res
