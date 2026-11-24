# -*- coding: utf-8 -*-
"""Reporte «Flujo de efectivo a 13 semanas» sobre el motor de reportes de
Contabilidad (``account_reports``): una columna por semana más el total, PDF y
Excel nativos, renglones que se despliegan por contacto y abren sus documentos.
"""
from collections import defaultdict

from odoo import _, fields, models
from odoo.tools.misc import format_date

from .calendario import SEMANAS, semanas

ENTRADAS_COMPROMETIDO = [
    ('cobro_vencido', 'Clientes vencidos (cobrables)'),
    ('cobro', 'Clientes por vencer'),
]
ENTRADAS_ESTIMADO = [
    ('pedido_venta', 'Pedidos de venta por facturar'),
    ('venta_estimada', 'Ventas sin pedido todavía'),
    ('ajuste_entrada', 'Otras entradas (ajustes manuales)'),
]
SALIDAS_COMPROMETIDO = [
    ('pago_vencido', 'Proveedores vencidos'),
    ('pago', 'Proveedores por vencer'),
]
SALIDAS_ESTIMADO_FIJO = [
    ('orden_compra', 'Órdenes de compra por facturar'),
    ('compra_estimada', 'Compras de las ventas sin pedido'),
    ('ajuste_salida', 'Otras salidas (ajustes manuales)'),
]
INFO = [
    ('incobrable', 'Cartera incobrable (anterior al corte o vencida de más)'),
    ('revisar', 'Proveedores por revisar antes de pagar'),
    ('cobro_despues', 'Cobranza después de la semana 13'),
    ('pago_despues', 'Pagos después de la semana 13'),
]


class QbFlujoReportHandler(models.AbstractModel):
    _name = 'qb.flujo.report.handler'
    _inherit = 'account.report.custom.handler'
    _description = 'Flujo de efectivo a 13 semanas'

    # ------------------------------------------------------------------
    # Columnas: 13 semanas + total
    # ------------------------------------------------------------------
    def _custom_options_initializer(self, report, options, previous_options=None):
        super()._custom_options_initializer(report, options, previous_options=previous_options)
        hoy = fields.Date.to_date(options['date']['date_to'])
        headers = []
        for i, (ini, fin) in enumerate(semanas(hoy)):
            nombre = _('Sem %(n)s · %(d)s', n=i + 1, d=format_date(self.env, ini, date_format='d MMM'))
            headers.append(self._encabezado(options, ini, fin, nombre))
        headers.append(self._encabezado(options, hoy, semanas(hoy)[-1][1], _('Total')))
        options['column_headers'] = [headers] + list((options.get('column_headers') or [[]])[1:])
        try:
            report._init_options_columns(options, previous_options=previous_options)
        except TypeError:
            report._init_options_columns(options)
        options['qb_flujo_hoy'] = fields.Date.to_string(hoy)

    @staticmethod
    def _encabezado(options, ini, fin, nombre):
        date_opts = dict(options['date'], date_from=fields.Date.to_string(ini), date_to=fields.Date.to_string(fin),
                         mode='range', filter='custom', string=nombre)
        return {'name': nombre, 'forced_options': {'date': date_opts}}

    def _caret_options_initializer(self):
        return {'qb_flujo': [{'name': _('Ver documentos'), 'action': 'qb_flujo_abrir'}]}

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    def _resultados(self, report, options):
        hoy = fields.Date.to_date(options.get('qb_flujo_hoy') or options['date']['date_to'])
        out, faltan = [], self.env['res.company']
        getter = getattr(report, 'get_report_company_ids', None)
        company_ids = list(getter(options)) if getter else self.env.company.ids
        for company in self.env['res.company'].browse(company_ids):
            config = self.env['qb.flujo.config']._para_empresa(company)
            if not config:
                faltan |= company
                continue
            out.append((config, self.env['qb.flujo.motor'].with_company(company).calcular(config, hoy)))
        return out, faltan

    def _dynamic_lines_generator(self, report, options, all_column_groups_expression_totals, warnings=None):
        resultados, faltan = self._resultados(report, options)
        n = SEMANAS
        filas = defaultdict(lambda: {'semanas': [0.0] * n, 'detalle': {}})
        info = defaultdict(lambda: {'total': 0.0, 'detalle': {}})
        compromisos = {}
        caja = []
        minimo = 0.0
        meta = {}
        for config, res in resultados:
            minimo += config.saldo_minimo
            for k, v in res.get('meta', {}).items():
                meta.setdefault(k, v)
            caja += res['caja']
            for clave, fila in res['filas'].items():
                destino = filas[clave]
                for k in ('nombre', 'tipo'):
                    if k in fila:
                        destino[k] = fila[k]
                for i, v in enumerate(fila['semanas']):
                    destino['semanas'][i] += v
                for dk, det in fila['detalle'].items():
                    d = destino['detalle'].setdefault(dk, dict(det, semanas=[0.0] * n, ids=set()))
                    for i, v in enumerate(det['semanas']):
                        d['semanas'][i] += v
                    d['ids'] |= det['ids']
                if clave.startswith('comp_'):
                    compromisos[clave] = fila.get('nombre')
            for clave, inf in res['info'].items():
                destino = info[clave]
                destino['total'] += inf['total']
                for dk, det in inf['detalle'].items():
                    d = destino['detalle'].setdefault(dk, dict(det, total=0.0, ids=set()))
                    d['total'] += det['total']
                    d['ids'] |= det['ids']

        lines = []
        if faltan:
            lines.append(self._texto(report, options, 'faltan', _(
                'Sin supuestos del flujo para: %s (Contabilidad → Configuración → Flujo a 13 semanas).',
                ', '.join(faltan.mapped('name')))))

        apertura = sum(c['importe'] for c in caja)
        comp_entrada = [(k, v) for k, v in compromisos.items() if filas[k].get('tipo') == 'entrada']
        comp_salida = [(k, v) for k, v in compromisos.items() if filas[k].get('tipo') != 'entrada']

        def suma(claves):
            return [sum(filas[k]['semanas'][i] for k in claves) for i in range(n)]

        claves_entradas = [k for k, _l in ENTRADAS_COMPROMETIDO + ENTRADAS_ESTIMADO] + [k for k, _l in comp_entrada]
        claves_salidas = [k for k, _l in SALIDAS_COMPROMETIDO + SALIDAS_ESTIMADO_FIJO] + [k for k, _l in comp_salida]
        entradas, salidas = suma(claves_entradas), suma(claves_salidas)
        neto = [entradas[i] + salidas[i] for i in range(n)]
        inicial, final = [], []
        saldo = apertura
        for i in range(n):
            inicial.append(saldo)
            saldo += neto[i]
            final.append(saldo)

        # Saldo inicial, desplegable por cuenta
        caja_id = report._get_generic_line_id(None, None, markup='qb_caja')
        lines.append(self._linea(report, options, caja_id, _('Saldo inicial de caja'), inicial, 1,
                                 total=apertura, unfoldable=bool(caja)))
        if caja:
            for c in caja:
                nombre = '%s · %s %s · %s' % (
                    c['nombre'], c['moneda'], '{:,.2f}'.format(c['importe_moneda']),
                    _('estado de cuenta al %s', format_date(self.env, c['al'])) if c['fuente'] == 'extracto' and c['al']
                    else _('saldo contable'))
                lines.append(self._linea(
                    report, options, report._get_generic_line_id('account.journal', c['journal_id'],
                                                                  parent_line_id=caja_id),
                    nombre, [c['importe']] + [None] * (n - 1), 3, total=c['importe'], parent_id=caja_id))

        def seccion(markup, titulo, valores):
            lines.append(self._linea(report, options, report._get_generic_line_id(None, None, markup=markup),
                                     titulo, valores, 1))

        def subtitulo(markup, titulo, claves):
            valores = suma(claves)
            if any(not self._cero(v) for v in valores):
                lines.append(self._linea(report, options, report._get_generic_line_id(None, None, markup=markup),
                                         titulo, valores, 2, clase='fst-italic'))

        etiquetas = {}
        if meta.get('ventas'):
            etiquetas['venta_estimada'] = _('Ventas sin pedido todavía (%s)', meta['ventas'])
        if meta.get('proporcion_compras'):
            etiquetas['compra_estimada'] = _('Compras de las ventas sin pedido (%(p)s %% de la venta)',
                                             p='{:,.0f}'.format(meta['proporcion_compras'] * 100))

        def renglones(defs):
            for clave, etiqueta in defs:
                etiqueta = etiquetas.get(clave, etiqueta)
                fila = filas.get(clave)
                if not fila or all(self._cero(v) for v in fila['semanas']):
                    continue
                line_id = report._get_generic_line_id(None, None, markup=clave)
                lines.append(self._linea(report, options, line_id, etiqueta, fila['semanas'], 3,
                                         unfoldable=bool(fila['detalle'])))
                if fila['detalle']:
                    detalle = sorted(fila['detalle'].items(), key=lambda kv: -abs(sum(kv[1]['semanas'])))
                    for dk, det in detalle:
                        if all(self._cero(v) for v in det['semanas']):
                            continue
                        sub_id = report._get_generic_line_id(det['model'] if det.get('res_id') else None,
                                                             det.get('res_id'), markup='%s__%s' % (clave, dk[1]),
                                                             parent_line_id=line_id)
                        lines.append(self._linea(report, options, sub_id, det['nombre'], det['semanas'], 4,
                                                 parent_id=line_id, caret=bool(det['ids'])))

        seccion('qb_entradas', _('ENTRADAS'), entradas)
        subtitulo('qb_ent_comp', _('Comprometido: facturas de clientes'), [k for k, _l in ENTRADAS_COMPROMETIDO])
        renglones(ENTRADAS_COMPROMETIDO)
        subtitulo('qb_ent_est', _('Estimado'), [k for k, _l in ENTRADAS_ESTIMADO] + [k for k, _l in comp_entrada])
        renglones(ENTRADAS_ESTIMADO + comp_entrada)

        seccion('qb_salidas', _('SALIDAS'), salidas)
        subtitulo('qb_sal_comp', _('Comprometido: facturas de proveedores'), [k for k, _l in SALIDAS_COMPROMETIDO])
        renglones(SALIDAS_COMPROMETIDO)
        subtitulo('qb_sal_est', _('Estimado'), [k for k, _l in SALIDAS_ESTIMADO_FIJO] + [k for k, _l in comp_salida])
        renglones(comp_salida + SALIDAS_ESTIMADO_FIJO)

        lines.append(self._linea(report, options, report._get_generic_line_id(None, None, markup='qb_neto'),
                                 _('Flujo neto de la semana'), neto, 1))
        lines.append(self._linea(report, options, report._get_generic_line_id(None, None, markup='qb_final'),
                                 _('Saldo final de caja'), final, 1, total=final[-1]))
        if minimo:
            holgura = [f - minimo for f in final]
            lines.append(self._linea(report, options, report._get_generic_line_id(None, None, markup='qb_holgura'),
                                     _('Sobrante (faltante) contra el saldo mínimo de %s', '{:,.0f}'.format(minimo)),
                                     holgura, 2, total=min(holgura)))
        cortas = [str(i + 1) for i, f in enumerate(final) if f < minimo]
        if cortas:
            lines.append(self._texto(report, options, 'qb_alerta', _(
                'El saldo final queda por debajo del mínimo en las semanas %s.', ', '.join(cortas))))

        # Informativo: no suma al flujo
        for clave, etiqueta in INFO:
            inf = info.get(clave)
            if not inf or self._cero(inf['total']):
                continue
            line_id = report._get_generic_line_id(None, None, markup=clave)
            lines.append(self._linea(report, options, line_id, _('No suma · %s', etiqueta), [None] * n, 2,
                                     total=inf['total'], unfoldable=True, clase='text-muted'))
            if inf['detalle']:
                for dk, det in sorted(inf['detalle'].items(), key=lambda kv: -abs(kv[1]['total'])):
                    sub_id = report._get_generic_line_id(det['model'], det['res_id'], markup='%s__%s' % (clave, dk[1]),
                                                         parent_line_id=line_id)
                    lines.append(self._linea(report, options, sub_id, det['nombre'], [None] * n, 4,
                                             total=det['total'], parent_id=line_id, caret=bool(det['ids']),
                                             clase='text-muted'))
        return [(0, line) for line in lines]

    # ------------------------------------------------------------------
    # Celdas y renglones
    # ------------------------------------------------------------------
    def _cero(self, v):
        return self.env.company.currency_id.is_zero(v or 0.0)

    @staticmethod
    def _desplegado(options, line_id):
        return bool(options.get('unfold_all') or line_id in (options.get('unfolded_lines') or []))

    def _celda(self, report, options, column, valor):
        celda = report._build_column_dict(0.0 if valor is None else valor, column, options=options)
        if valor is None:
            celda.update({'name': '', 'no_format': None, 'is_zero': True})
        return celda

    def _linea(self, report, options, line_id, nombre, semanas, nivel, total=None, unfoldable=False,
               parent_id=None, caret=False, clase=''):
        if total is None:
            total = sum(v for v in semanas if v)
        valores = list(semanas) + [total]
        columns = [self._celda(report, options, col, valores[i] if i < len(valores) else None)
                   for i, col in enumerate(options['columns'])]
        line = {
            'id': line_id, 'name': nombre, 'level': nivel, 'columns': columns,
            'unfoldable': unfoldable, 'unfolded': unfoldable and self._desplegado(options, line_id),
        }
        if parent_id:
            line['parent_id'] = parent_id
        if caret:
            line['caret_options'] = 'qb_flujo'
        if clase:
            line['class'] = clase
        return line

    def _texto(self, report, options, markup, texto):
        return self._linea(report, options, report._get_generic_line_id(None, None, markup=markup), texto,
                           [None] * SEMANAS, 2, total=None, clase='text-warning fst-italic') | {
            'columns': [self._celda(report, options, col, None) for col in options['columns']]}

    # ------------------------------------------------------------------
    # Ver documentos
    # ------------------------------------------------------------------
    def qb_flujo_abrir(self, options, params):
        report = self.env['account.report'].browse(options['report_id'])
        line_id = params.get('line_id')
        clave, sub = None, None
        for markup, _model, _res_id in report._parse_line_id(line_id):
            texto = markup.get('markup') if isinstance(markup, dict) else markup
            if texto and '__' in str(texto):
                clave, sub = str(texto).split('__', 1)
        resultados, _faltan = self._resultados(report, options)
        ids = defaultdict(set)
        for _config, res in resultados:
            fuente = res['filas'].get(clave) or res['info'].get(clave) or {'detalle': {}}
            for dk, det in fuente['detalle'].items():
                if str(dk[1]) == sub:
                    for model, rid in det['ids']:
                        ids[model].add(rid)
        model = max(ids, key=lambda m: len(ids[m])) if ids else 'account.move.line'
        return {
            'type': 'ir.actions.act_window',
            'name': _('Documentos'),
            'res_model': model,
            'view_mode': 'list,form',
            'views': [(False, 'list'), (False, 'form')],
            'domain': [('id', 'in', sorted(ids.get(model, [])))],
            'context': {'create': False},
        }
