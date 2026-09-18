# -*- coding: utf-8 -*-
"""Valores del CFDI de nómina que el módulo de Odoo saca mal.

``l10n_mx_hr_payroll_account_edi`` arma el CFDI con
``_l10n_mx_edi_add_payslip_cfdi_values``. Aquí se llama al original y se
corrige por dos vías, para que el XML salga bien y para que al inspeccionar
``cfdi_values`` se vea lo corregido (así se verifica en staging):

1. Se sobrescriben las llaves del diccionario que arma el módulo
   (``salario_diario_integrado``, ``salario_base_cot_apor``,
   ``registro_patronal``, ``clave_ent_fed``, ``num_empleado``), estén donde
   estén (``nomina_receptor``, ``nomina_emisor``…): sólo se tocan llaves que
   ya existen, nunca se inventan.
2. Se agrega ``cfdi_values['qb_nomina']`` y la plantilla heredada
   (``data/cfdi_nomina_templates.xml``) sobrescribe los atributos con eso.
   Es la red de seguridad por si el módulo cambia el nombre de una llave.

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

# Parámetro con la entidad federativa por defecto (c_Estado del SAT) si el
# empleado no tiene dirección laboral con estado ni la compañía tampoco.
PARAM_CLAVE_ENT_FED = 'quimibond_nomina.clave_ent_fed'

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
            'sdi': round(sdi, 2),
            'sbc': round(sbc, 2),
            'clave_ent_fed': self._qb_nomina_clave_ent_fed(),
            'num_empleado': self._qb_nomina_num_empleado(),
        }

    def _qb_nomina_clave_ent_fed(self):
        """``ClaveEntFed`` del Receptor: entidad federativa donde el empleado
        prestó el servicio (c_Estado del SAT, obligatorio cuando hay ISR
        retenido; el módulo de Odoo lo deja vacío y el PAC rechaza).

        Los códigos de estado de México en Odoo son los del SAT (``MEX``,
        ``CMX``…). Orden: dirección laboral del empleado → ubicación de trabajo
        → estado de la compañía → parámetro ``quimibond_nomina.clave_ent_fed``.
        NOI manda ``MEX`` (Toluca)."""
        self.ensure_one()
        employee = self.employee_id.sudo()
        company = self.company_id.sudo()
        candidatos = (
            employee.address_id.state_id.code,
            employee.work_location_id.address_id.state_id.code if employee.work_location_id else False,
            company.state_id.code,
            (self.env['ir.config_parameter'].sudo().get_param(PARAM_CLAVE_ENT_FED) or '').strip(),
        )
        for code in candidatos:
            code = (code or '').strip().upper()
            if len(code) == 3 and code.isalpha():
                return code
        _logger.warning('quimibond_nomina: recibo %s sin entidad federativa (dirección laboral, '
                        'compañía o parámetro %s); ClaveEntFed sale vacío', self.id, PARAM_CLAVE_ENT_FED)
        return False

    def _qb_nomina_num_empleado(self):
        """``NumEmpleado`` del Receptor: la *Referencia de empleado*
        (``registration_number``), que es el número de trabajador de NOI, o
        False para NO emitir el atributo (es opcional en el Anexo 20).

        Antes se rellenaba con la credencial o con el id de Odoo: la credencial
        de Ricardo Salgado es ``041460744711`` y NOI manda ``32``; el id de
        Genaro es 325 y NOI manda 1. Un número inventado es peor que ninguno.
        Hoy sólo un empleado tiene la referencia capturada; RH tiene que
        cargarla con el número de NOI."""
        self.ensure_one()
        value = str(self.employee_id.sudo().registration_number or '').strip()
        if not value:
            _logger.info('quimibond_nomina: recibo %s sin referencia de empleado; NumEmpleado no se emite',
                         self.id)
            return False
        return value[:15]      # el SAT admite hasta 15 caracteres

    # Llaves del diccionario del módulo de Odoo que se corrigen, y con qué
    # valor de _qb_nomina_cfdi_values. Los importes van como número, que es
    # como los deja el módulo (la plantilla los formatea).
    PATCH_KEYS = {
        'registro_patronal': 'registro_patronal',
        'salario_diario_integrado': 'sdi',
        'salario_base_cot_apor': 'sbc',
        'clave_ent_fed': 'clave_ent_fed',
        'num_empleado': 'num_empleado',
    }

    @api.model
    def _qb_nomina_patch_cfdi_values(self, cfdi_values, vals):
        """Escribe los valores corregidos sobre las llaves que el módulo de
        Odoo ya puso en ``cfdi_values`` (en el nivel raíz o en cualquier dict
        anidado un nivel, p. ej. ``nomina_receptor``). Devuelve las llaves
        tocadas. No inventa llaves: si el módulo cambia de nombres, la
        plantilla heredada sigue cubriendo el XML."""
        tocadas = []
        contenedores = [cfdi_values] + [v for v in cfdi_values.values() if isinstance(v, dict)]
        for d in contenedores:
            for key, fuente in self.PATCH_KEYS.items():
                if key not in d:
                    continue
                nuevo = vals.get(fuente)
                if key == 'num_empleado' and not nuevo:
                    # Sin referencia de empleado el atributo NO se emite: se
                    # vacía lo que haya puesto el módulo (credencial, NSS…).
                    d[key] = False
                    tocadas.append(key)
                    continue
                if nuevo in (None, False, ''):
                    continue
                if isinstance(d[key], str) and not isinstance(nuevo, str):
                    nuevo = '%.2f' % nuevo
                d[key] = nuevo
                tocadas.append(key)
        if 'salario_diario_integrado' not in tocadas:
            _logger.warning('quimibond_nomina: cfdi_values no trae la llave salario_diario_integrado; '
                            'sólo la plantilla heredada corrige el XML (llaves: %s)', sorted(cfdi_values))
        return tocadas

    # ------------------------------------------------------------------
    # nomina12:HorasExtra
    # ------------------------------------------------------------------
    def _qb_horas_extra_dias_capturados(self):
        """Cantidad de la entrada ``HE_DIAS`` del recibo (días en que hubo
        tiempo extra, capturados por RH), o None si no viene."""
        self.ensure_one()
        total = sum(inp.amount for inp in self.input_line_ids
                    if inp.input_type_id.code == he.INPUT_HE_DIAS and inp.amount)
        return total or None

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

    @api.model
    def _qb_fusionar_percepciones_019(self, cfdi_values):
        """Deja UNA sola percepción 019 en ``percepcion_list``. Devuelve su
        índice, o None si no hay ninguna.

        Por qué: Odoo genera una percepción por regla, así que las horas
        extra salen partidas en dos (``P19`` gravada por ``HE_TAX`` y
        ``P19_2`` exenta por ``HE_EXEMPT``). El SAT exige que TODA percepción
        019 lleve al menos un hijo ``HorasExtra``, y con la lista partida sólo
        hay dos salidas, ambas malas: colgar el nodo a las dos declara el
        doble de horas; colgarlo a una deja a la otra sin hijo y el PAC
        rechaza. NOI emite una sola con los dos importes dentro, y eso es lo
        que el SAT ya acepta.

        Se suman ``importe_gravado`` e ``importe_exento`` y se conservan la
        clave y el concepto de la gravada. Sólo toca las 019; con una o
        ninguna no hace nada (idempotente, por si Odoo deja de partirlas)."""
        lista = cfdi_values.get('percepcion_list')
        if not isinstance(lista, list):
            return None
        indices = [i for i, item in enumerate(lista) if isinstance(item, dict) and he.es_percepcion_019(item)]
        if not indices:
            return None
        if len(indices) == 1:
            return indices[0]
        items = [lista[i] for i in indices]
        gravada = next((it for it in items if self._qb_importe(it.get('importe_gravado'))), items[0])
        fusion = dict(gravada)
        for campo in ('importe_gravado', 'importe_exento'):
            fusion[campo] = round(sum(self._qb_importe(it.get(campo)) for it in items), 2)
        lista[indices[0]] = fusion
        for i in reversed(indices[1:]):
            del lista[i]
        _logger.info('quimibond_nomina: %d percepciones 019 fusionadas en una (gravado %.2f, exento %.2f)',
                     len(items), fusion['importe_gravado'], fusion['importe_exento'])
        return indices[0]

    @staticmethod
    def _qb_importe(value):
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _qb_add_horas_extra(self, cfdi_values):
        """Fusiona las percepciones 019 y anota en
        ``cfdi_values[KEY_HORAS_EXTRA]`` los nodos ``HorasExtra`` de la
        percepción 019 resultante, por índice dentro de ``percepcion_list``.

        ``ImportePagado`` = gravado + exento de la percepción ya fusionada
        (es lo que el propio CFDI declara para esas horas); si no se puede
        leer, se cae a las líneas ``HE_EXEMPT`` + ``HE_TAX`` del recibo.

        Falla en silencio hacia el lado seguro: si hay horas sin percepción 019,
        percepción 019 sin horas, o ningún importe, se registra un aviso y el
        CFDI sale como estaba. Más vale un CFDI sin nodo (el PAC lo rechaza y se
        ve) que uno con un nodo inventado."""
        self.ensure_one()
        cfdi_values[KEY_HORAS_EXTRA] = {}
        horas = self._qb_horas_extra_por_tipo()
        indice = self._qb_fusionar_percepciones_019(cfdi_values)
        if not horas and indice is None:
            return {}
        if not horas:
            _logger.warning('quimibond_nomina: recibo %s trae percepción 019 en el CFDI pero ninguna '
                            'entrada HE_DOBLE/HE_TRIPLE; sale sin nodo HorasExtra', self.id)
            return {}
        if indice is None:
            _logger.warning('quimibond_nomina: recibo %s trae %s horas extra capturadas pero el CFDI no '
                            'trae percepción 019; sale sin nodo HorasExtra', self.id, horas)
            return {}
        importe_total = he.importe_de_percepcion(cfdi_values['percepcion_list'][indice])
        if not importe_total:
            totales = [self._qb_nomina_line_total(code) for code in RULES_HORAS_EXTRA]
            if all(t is None for t in totales):
                _logger.warning('quimibond_nomina: recibo %s sin importe en la percepción 019 ni líneas %s; '
                                'sale sin nodo HorasExtra', self.id, '/'.join(RULES_HORAS_EXTRA))
                return {}
            importe_total = sum(t or 0.0 for t in totales)
        dias_periodo = (self.date_to - self.date_from).days + 1
        dias = self._qb_horas_extra_dias_capturados()
        if not dias:
            # info, no warning: pasa en todos los recibos hasta que RH capture HE_DIAS
            _logger.info('quimibond_nomina: recibo %s sin entrada HE_DIAS; Dias del nodo HorasExtra se '
                         'ESTIMA a partir de las horas (%s)', self.id, horas)
        nodos = {indice: he.horas_extra_nodos(horas, importe_total, dias_periodo, dias)}
        cfdi_values[KEY_HORAS_EXTRA] = nodos
        return nodos

    # ------------------------------------------------------------------
    # Conceptos en español
    # ------------------------------------------------------------------
    LISTAS_CON_CONCEPTO = ('percepcion_list', 'deduccion_list', 'otro_pago_list')

    def _qb_conceptos_en_espanol(self, cfdi_values, lang='es_MX'):
        """Vuelve a leer el ``concepto`` de cada percepción, deducción y otro
        pago desde su registro de ``l10n.mx.concept`` (por ``clave`` =
        ``payroll_code``) en español.

        Por qué: los conceptos son lo que el trabajador lee en su recibo y NOI
        los manda en español; el módulo los toma del nombre del concepto en el
        idioma del contexto, y cuando el CFDI se arma en ``en_US`` (shell,
        cron) salen "Overtime", "Savings Fund"… Los registros ya están
        traducidos; aquí sólo se leen en ``es_MX``. No hay diccionario en el
        código: si un concepto no tiene traducción, se queda como estaba.
        Devuelve cuántos conceptos cambió."""
        if lang not in [code for code, _ in self.env['res.lang'].get_installed()]:
            return 0
        Concept = self.env['l10n.mx.concept'].sudo().with_context(lang=lang)
        cambiados = 0
        for llave in self.LISTAS_CON_CONCEPTO:
            lista = cfdi_values.get(llave)
            if not isinstance(lista, list):
                continue
            claves = {it.get('clave') for it in lista if isinstance(it, dict) and it.get('clave')}
            if not claves:
                continue
            nombres = {c.payroll_code: c.name for c in Concept.search([('payroll_code', 'in', list(claves))])}
            for item in lista:
                if not isinstance(item, dict) or 'concepto' not in item:
                    continue
                nombre = nombres.get(item.get('clave'))
                if nombre and nombre != item['concepto']:
                    item['concepto'] = nombre
                    cambiados += 1
        return cambiados

    def _l10n_mx_edi_add_payslip_cfdi_values(self, cfdi_values, *args, **kwargs):
        res = super()._l10n_mx_edi_add_payslip_cfdi_values(cfdi_values, *args, **kwargs)
        if len(self) == 1:
            vals = self._qb_nomina_cfdi_values()
            cfdi_values['qb_nomina'] = vals
            self._qb_nomina_patch_cfdi_values(cfdi_values, vals)
            try:
                self._qb_add_horas_extra(cfdi_values)
            except Exception:  # noqa: BLE001 — nunca tumbar el CFDI por el nodo
                _logger.exception('quimibond_nomina: recibo %s: no se pudieron calcular los nodos '
                                  'HorasExtra; el CFDI sale sin ellos', self.id)
                cfdi_values[KEY_HORAS_EXTRA] = {}
            try:
                self._qb_conceptos_en_espanol(cfdi_values)
            except Exception:  # noqa: BLE001 — los conceptos en inglés no invalidan el CFDI
                _logger.exception('quimibond_nomina: recibo %s: no se pudieron traducir los conceptos', self.id)
        return res

    # ------------------------------------------------------------------
    # La vista que imprime los nodos se configura sola
    # ------------------------------------------------------------------
    @api.model
    def qb_nomina_ensure_he_dias_input(self):
        """Deja la entrada ``HE_DIAS`` disponible en la estructura de nómina de
        Quimibond («Paga regular», código ``MX_REGULAR``). El registro nace en
        ``data/payslip_input_types.xml``; la estructura no tiene xmlid conocido,
        así que se liga aquí por código en cada instalación/actualización."""
        itype = self.env.ref('quimibond_nomina.input_type_he_dias', raise_if_not_found=False)
        if not itype:
            return False
        structs = self.env['hr.payroll.structure'].sudo().with_context(active_test=False).search(
            [('code', '=', 'MX_REGULAR')])
        if not structs:
            _logger.warning('quimibond_nomina: no hay estructura MX_REGULAR; la entrada HE_DIAS queda sin '
                            'estructura (agregarla a mano en Nómina → Configuración → Otras entradas)')
            return False
        faltan = structs - itype.sudo().struct_ids
        if faltan:
            itype.sudo().write({'struct_ids': [(4, st.id) for st in faltan]})
        return True

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
