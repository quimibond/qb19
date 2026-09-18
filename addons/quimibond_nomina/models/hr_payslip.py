# -*- coding: utf-8 -*-
"""Valores del CFDI de nómina que el módulo de Odoo saca mal.

``l10n_mx_hr_payroll_account_edi`` arma el CFDI con
``_l10n_mx_edi_add_payslip_cfdi_values``. Aquí se llama al original y se
agrega ``cfdi_values['qb_nomina']``; la plantilla heredada
(``data/cfdi_nomina_templates.xml``) lee de ahí los tres atributos.

Las líneas del recibo se buscan POR CÓDIGO y nunca con ``env.ref()``. Es
deliberado: el módulo resuelve las reglas por XML id, y cuando la regla de
fábrica está archivada y en su lugar corre una copia activa con el mismo código
(la situación de esta base) el ``env.ref()`` devuelve la archivada, no produce
línea y el CFDI sale con ``0.00`` sin error ni aviso.

Tercer parche: el nodo ``nomina12:HorasExtra`` (ver ``horas_extra.py`` para
el cálculo y el criterio de ``Dias``). El módulo de Odoo escribe cada
``nomina12:Percepcion`` como elemento vacío; aquí ``_qb_add_horas_extra``
anota en ``cfdi_values['qb_horas_extra_por_indice']`` los nodos de cada
percepción 019 (por índice dentro de ``percepcion_list``) y la vista
``quimibond_nomina.cfdiv40_nomina_horas_extra`` los imprime dentro del
elemento. Esa vista se configura sola en cada instalación/actualización
(``qb_nomina_ensure_horas_extra_view``): lee la plantilla real, saca de ahí la
variable del ``t-foreach`` y se apaga si Odoo ya emite el nodo de origen, para
que nunca salgan dos."""
import logging

from lxml import etree

from odoo import api, models
from odoo.exceptions import AccessError

from . import horas_extra as he

_logger = logging.getLogger(__name__)

# Códigos de las líneas del recibo de donde salen los salarios del Receptor.
CODE_SDI = 'INT_DAY_WAGE_BASE'     # SalarioDiarioIntegrado: el SDI declarado
CODE_SBC = 'INT_DAY_WAGE'          # SalarioBaseCotApor: el mismo, topado a 25 UMA
UMA_CAP = 25

# Reglas cuyas líneas suman ImportePagado de las horas extra (gravado + exento).
RULES_HORAS_EXTRA = ('HE_EXEMPT', 'HE_TAX')
# Llave de cfdi_values que lee la vista heredada: {índice en percepcion_list: [nodos]}.
KEY_HORAS_EXTRA = 'qb_horas_extra_por_indice'
TEMPLATE_XMLID = 'l10n_mx_hr_payroll_account_edi.cfdiv40_nomina'
VIEW_XMLID = 'quimibond_nomina.cfdiv40_nomina_horas_extra'


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    def _qb_nomina_line_total(self, code):
        """Total de las líneas del recibo con ese código, o None si no hay."""
        self.ensure_one()
        lines = self.line_ids.filtered(lambda l: l.code == code)
        if not lines:
            return None
        return sum(lines.mapped('total'))

    def _qb_nomina_cfdi_values(self):
        """Dict con valores planos (sin recordsets: ``_clean_cfdi_values`` los
        destruye) para los atributos que la plantilla heredada sobrescribe."""
        self.ensure_one()
        version = self.version_id.sudo()
        company = self.company_id.sudo()
        registro = (version.l10n_mx_employer_registration or company.l10n_mx_imss_id or '').strip()

        sdi = self._qb_nomina_line_total(CODE_SDI)
        sbc = self._qb_nomina_line_total(CODE_SBC)
        if not sdi:
            # Lo que hacía el módulo: el salario diario simple. No mejora nada,
            # pero tampoco deja el atributo vacío.
            sdi = self.l10n_mx_daily_salary or 0.0
            _logger.warning('quimibond_nomina: recibo %s sin línea %s; SalarioDiarioIntegrado '
                            'sale del salario diario simple (%.2f)', self.id, CODE_SDI, sdi)
        if not sbc:
            sbc = sdi
            try:
                sbc = min(sbc, UMA_CAP * self._rule_parameter('l10n_mx_uma')['daily'])
            except Exception:  # noqa: BLE001 — sin parámetro de UMA se manda sin topar
                pass
            _logger.warning('quimibond_nomina: recibo %s sin línea %s; SalarioBaseCotApor '
                            'sale del SDI (%.2f)', self.id, CODE_SBC, sbc)
        return {
            'registro_patronal': registro or False,
            'salario_diario_integrado': '%.2f' % sdi,
            'salario_base_cot_apor': '%.2f' % sbc,
        }

    # ------------------------------------------------------------------
    # nomina12:HorasExtra
    # ------------------------------------------------------------------
    def _qb_horas_extra_por_tipo(self):
        """``{'01': horas dobles, '02': horas triples}`` desde las entradas del
        recibo (``HE_DOBLE`` id 16, ``HE_TRIPLE`` id 17; ``amount`` = horas).
        Las horas sencillas (``H_SENCILLA``) no cuentan: van al concepto 038."""
        self.ensure_one()
        horas = {}
        for inp in self.input_line_ids:
            tipo = he.TIPO_HORAS_POR_INPUT.get(inp.input_type_id.code)
            if tipo and inp.amount:
                horas[tipo] = horas.get(tipo, 0.0) + inp.amount
        return horas

    def _qb_add_horas_extra(self, cfdi_values):
        """Anota en ``cfdi_values[KEY_HORAS_EXTRA]`` los nodos ``HorasExtra`` de
        cada percepción 019 de ``percepcion_list``, por índice.

        Falla en silencio hacia el lado seguro: si hay horas sin percepción 019,
        percepción 019 sin horas, o no aparecen las líneas ``HE_EXEMPT``/``HE_TAX``,
        se registra un aviso y el CFDI sale como estaba. Más vale un CFDI sin
        nodo (el PAC lo rechaza y se ve) que uno con un nodo inventado."""
        self.ensure_one()
        cfdi_values[KEY_HORAS_EXTRA] = {}
        horas = self._qb_horas_extra_por_tipo()
        lista = cfdi_values.get('percepcion_list') or []
        indices = [i for i, item in enumerate(lista) if he.es_percepcion_019(item)]
        if not horas and not indices:
            return {}
        if not horas:
            _logger.warning('quimibond_nomina: recibo %s trae percepción 019 en el CFDI pero ninguna '
                            'entrada HE_DOBLE/HE_TRIPLE; sale sin nodo HorasExtra', self.id)
            return {}
        if not indices:
            _logger.warning('quimibond_nomina: recibo %s trae %s horas extra capturadas pero el CFDI no '
                            'trae percepción 019; sale sin nodo HorasExtra', self.id, horas)
            return {}
        totales = [self._qb_nomina_line_total(code) for code in RULES_HORAS_EXTRA]
        if all(t is None for t in totales):
            _logger.warning('quimibond_nomina: recibo %s sin líneas %s; sale sin nodo HorasExtra',
                            self.id, '/'.join(RULES_HORAS_EXTRA))
            return {}
        importe_total = sum(t or 0.0 for t in totales)
        dias_periodo = (self.date_to - self.date_from).days + 1
        if len(indices) == 1:
            nodos = {indices[0]: he.horas_extra_nodos(horas, importe_total, dias_periodo)}
        else:
            # Más de una percepción 019 (p. ej. P19 gravada y P19_2 exenta por
            # separado): cada una lleva sus nodos con su propio importe, que
            # se lee del mismo elemento. Si no se reconoce, mejor nada.
            nodos = {}
            for i in indices:
                importe = he.importe_de_percepcion(lista[i])
                if importe is None:
                    _logger.warning('quimibond_nomina: recibo %s con %d percepciones 019 y no se '
                                    'reconoce su importe; sale sin nodo HorasExtra', self.id, len(indices))
                    return {}
                nodos[i] = he.horas_extra_nodos(horas, importe, dias_periodo)
        cfdi_values[KEY_HORAS_EXTRA] = nodos
        return nodos

    def _l10n_mx_edi_add_payslip_cfdi_values(self, cfdi_values, *args, **kwargs):
        res = super()._l10n_mx_edi_add_payslip_cfdi_values(cfdi_values, *args, **kwargs)
        if len(self) == 1:
            cfdi_values['qb_nomina'] = self._qb_nomina_cfdi_values()
            try:
                self._qb_add_horas_extra(cfdi_values)
            except Exception:  # noqa: BLE001 — nunca tumbar el CFDI por el nodo
                _logger.exception('quimibond_nomina: recibo %s: no se pudieron calcular los nodos '
                                  'HorasExtra; el CFDI sale sin ellos', self.id)
                cfdi_values[KEY_HORAS_EXTRA] = {}
        return res

    # ------------------------------------------------------------------
    # La vista que imprime los nodos se configura sola
    # ------------------------------------------------------------------
    @api.model
    def qb_nomina_ensure_horas_extra_view(self):
        """Deja lista la herencia de la plantilla del CFDI que imprime
        ``HorasExtra``. Se llama desde ``data/cfdi_horas_extra.xml`` en cada
        instalación/actualización del módulo (``<function>``).

        Por qué no es una herencia estática: el xpath tiene que meter los nodos
        dentro de ``nomina12:Percepcion`` y leerlos por el índice del
        ``t-foreach`` que la genera, y el nombre de esa variable sólo se sabe
        leyendo la plantilla real (Enterprise). Un xpath que no resuelve tumba la
        instalación del módulo entero. Así que se lee la plantilla aquí, y:

        * si Odoo ya emite ``HorasExtra`` (corrigieron el defecto de origen, hay
          un ticket abierto), la vista se apaga: dos nodos harían inválido el CFDI;
        * si la plantilla no tiene la forma esperada, la vista se apaga y se
          avisa en el log: el CFDI sale sin nodo, no roto;
        * si todo cuadra, la vista se activa con el arch generado."""
        if not self.env.is_admin():
            raise AccessError('Sólo el administrador puede reconfigurar la vista del CFDI.')
        View = self.env['ir.ui.view'].sudo().with_context(active_test=False)
        base = self.env.ref(TEMPLATE_XMLID, raise_if_not_found=False)
        mine = self.env.ref(VIEW_XMLID, raise_if_not_found=False)
        if not base or not mine:
            _logger.warning('quimibond_nomina: falta %s o %s; sin nodo HorasExtra',
                            TEMPLATE_XMLID, VIEW_XMLID)
            return False
        mine = mine.sudo()

        def apagar(motivo):
            if mine.active or (mine.arch_db or '').strip() != '<data/>':
                mine.write({'active': False, 'arch': '<data/>'})
            _logger.warning('quimibond_nomina: herencia HorasExtra apagada: %s', motivo)
            return False

        otras = View.search([('inherit_id', '=', base.id), ('id', '!=', mine.id)])
        if any('HorasExtra' in (v.arch_db or '') for v in [base, *otras]):
            return apagar('la plantilla del CFDI ya emite nomina12:HorasExtra (¿Odoo corrigió el '
                          'defecto?); no se agrega para no duplicar el nodo')
        try:
            root = etree.fromstring((base.arch_db or '').encode('utf-8'))
        except etree.XMLSyntaxError as exc:
            return apagar('no se pudo leer la plantilla %s: %s' % (TEMPLATE_XMLID, exc))
        var = he.loop_var_de_percepcion(root)
        if not var:
            return apagar('la plantilla %s no tiene un nomina12:Percepcion generado por un t-foreach '
                          'sobre percepcion_list; revisar la herencia contra la plantilla nueva'
                          % TEMPLATE_XMLID)
        arch = he.arch_herencia_horas_extra(var)
        try:
            with self.env.cr.savepoint():
                mine.write({'arch': arch, 'active': True})
        except Exception as exc:  # noqa: BLE001 — la instalación sigue, sin nodo
            _logger.exception('quimibond_nomina: la herencia HorasExtra no pasó la validación de la vista')
            return apagar('la vista no validó: %s' % exc)
        _logger.info('quimibond_nomina: herencia HorasExtra activa (variable del t-foreach: %s)', var)
        return True
