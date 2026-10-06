# -*- coding: utf-8 -*-
"""Período de costeo: tarifas por centro, capacidad ociosa y compuerta de
cierre.

Por cada centro directo:

    pool_fijo      = gasto fijo del centro en el mayor, suavizado N meses
                     (+ su parte de la nómina de fábrica y de los indirectos)
    pool_variable  = energía y consumibles del centro, mes real
    tarifa_fija    = pool_fijo ÷ horas normales
    tarifa_var     = pool_variable ÷ horas reales
    tarifa         = tarifa_fija + tarifa_var
    ociosidad      = pool_fijo − tarifa_fija × horas reales   (IAS 2)

Al cerrar, la tarifa se publica en «Costo por hora» de los centros de
trabajo de Odoo y Odoo costea con ella las órdenes del mes siguiente.
"""
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError


class QbPeriodo(models.Model):
    _name = 'qb.periodo'
    _description = 'Período de costeo'
    _inherit = ['mail.thread']
    _order = 'period desc'
    _rec_name = 'period'

    period = fields.Date(required=True, index=True,
                         help='Primer día del mes.')
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    state = fields.Selection([
        ('borrador', 'Borrador'), ('cerrado', 'Cerrado')],
        default='borrador', required=True, tracking=True)
    calculado_el = fields.Datetime(readonly=True)
    cerrado_el = fields.Datetime(readonly=True)
    cerrado_por = fields.Many2one('res.users', readonly=True)
    reaperturas = fields.Integer(readonly=True)
    motivo_reapertura = fields.Char()
    tarifa_ids = fields.One2many('qb.tarifa', 'periodo_id', string='Tarifas')

    suavizado_fijo_meses = fields.Integer(
        default=lambda self: int(self.env['qb.parametro'].get_float(
            'suavizado_fijo_meses', 12)),
        help='Meses promediados para el gasto fijo (la renta y la nómina '
             'llegan a saltos al mayor; la tarifa no debe saltar con ellas).')
    ventas_month = fields.Float(
        string='Ventas/mes (suavizadas)', readonly=True, digits=(16, 2))
    operacion_month = fields.Float(
        string='Operación/mes (suavizada)', readonly=True, digits=(16, 2))
    op_pct = fields.Float(
        string='Operación % de ventas', readonly=True, digits=(16, 6),
        help='Administración y ventas ÷ ventas, suavizados. Es lo que cada '
             'unidad vendida paga de oficinas y vendedores.')
    pool_fijo_total = fields.Float(readonly=True, digits=(16, 2))
    pool_variable_total = fields.Float(readonly=True, digits=(16, 2))
    ociosidad_total = fields.Float(
        readonly=True, digits=(16, 2),
        help='Costo fijo de la capacidad parada en todos los centros. Va al '
             'resultado del mes, no al producto.')
    sin_clasificar = fields.Float(
        string='Gasto sin clasificar', readonly=True, digits=(16, 2),
        help='Saldo del mes en cuentas de resultados que nadie clasificó. '
             'Debe ser 0: es gasto invisible para el costeo.')
    bloqueos = fields.Text(
        string='Qué impide cerrar', readonly=True,
        help='Vacío = se puede cerrar. Cada línea es una condición de la '
             'compuerta que no se cumple.')
    notas = fields.Text()

    _period_company_uniq = models.Constraint(
        'unique(period, company_id)', 'Ya existe ese período.')

    # ------------------------------------------------------------------
    # Ventana y mayor
    # ------------------------------------------------------------------
    @property
    def date_from(self):
        return date(self.period.year, self.period.month, 1)

    @property
    def date_to(self):
        return self.date_from + relativedelta(months=1)

    def _refs_excluidas_sql(self):
        """Asientos que no se reparten (cierre anual, enajenaciones), por
        referencia; igual que el módulo anterior."""
        refs = self.env['qb.parametro'].get_list(
            'refs_fuera_de_costeo', 'CIERRE ANUAL,ENAJENACI')
        if not refs:
            return 'TRUE', ()
        conds = ' AND '.join(['COALESCE(am.ref, \'\') NOT ILIKE %s'] * len(refs))
        return conds, tuple('%%%s%%' % r for r in refs)

    def _gasto_por_clase(self, date_from, date_to):
        """{(bucket, centro_id): monto} del mayor en la ventana, con el % de
        cada clasificación. Positivo = gasto; las ventas salen positivas
        también (se invierte el crédito)."""
        self.ensure_one()
        cond, params = self._refs_excluidas_sql()
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT c.bucket, c.centro_id,
                   SUM(CASE WHEN c.bucket = 'ventas' THEN -aml.balance
                            ELSE aml.balance END * c.pct / 100.0)
            FROM account_move_line aml
            JOIN account_move am ON am.id = aml.move_id
            JOIN qb_cuenta_clase c ON c.account_id = aml.account_id
                                  AND c.company_id = aml.company_id
            WHERE aml.parent_state = 'posted'
              AND aml.company_id = %s
              AND aml.date >= %s AND aml.date < %s
              AND """ + cond + """
            GROUP BY c.bucket, c.centro_id
        """, (self.company_id.id, date_from, date_to) + params)
        return {(b, cid): m or 0.0 for b, cid, m in self.env.cr.fetchall()}

    def _gasto_sin_clasificar(self, date_from, date_to):
        self.ensure_one()
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT COALESCE(SUM(aml.balance), 0)
            FROM account_move_line aml
            JOIN account_account aa ON aa.id = aml.account_id
            LEFT JOIN qb_cuenta_clase c ON c.account_id = aml.account_id
                                       AND c.company_id = aml.company_id
            WHERE aml.parent_state = 'posted'
              AND aml.company_id = %s
              AND aml.date >= %s AND aml.date < %s
              AND c.id IS NULL
              AND aa.account_type IN ('expense_direct_cost', 'expense',
                                      'expense_depreciation')
        """, (self.company_id.id, date_from, date_to))
        return self.env.cr.fetchone()[0] or 0.0

    def _suavizado(self, meses):
        """Gasto mensual promedio de los `meses` que terminan en el período:
        {(bucket, centro_id): monto/mes}."""
        d0 = self.date_from - relativedelta(months=meses - 1)
        tot = self._gasto_por_clase(d0, self.date_to)
        return {k: v / meses for k, v in tot.items()}

    # ------------------------------------------------------------------
    # Cálculo
    # ------------------------------------------------------------------
    def action_calcular(self):
        for rec in self:
            if rec.state == 'cerrado':
                raise UserError(
                    'El período %s está cerrado. Reábralo con motivo para '
                    'recalcularlo.' % rec.period)
            rec._calcular()
        return True

    def _calcular(self):
        self.ensure_one()
        Centro = self.env['qb.centro']
        centros = Centro.search([('company_id', '=', self.company_id.id)])
        directos = centros.filtered(lambda c: c.nature == 'directo')
        if not directos:
            raise UserError('No hay centros directos configurados.')

        fijo = self._suavizado(self.suavizado_fijo_meses or 1)
        real = self._gasto_por_clase(self.date_from, self.date_to)

        # Horas y nómina por centro directo (llaves de reparto).
        datos = {}
        for c in directos:
            hn, hn_fuente = c.horas_normales(self.date_from, self.date_to)
            hr, hr_fuente = c.horas_reales(self.date_from, self.date_to)
            datos[c.id] = {
                'horas_normales': hn, 'horas_fuente': hn_fuente,
                'horas_reales': hr, 'horas_reales_fuente': hr_fuente,
                'nomina': c.nomina_mensual(),
                'fijo': 0.0, 'variable': 0.0, 'indirecto': 0.0,
            }

        def repartir(monto, llave, destino):
            pesos = {cid: d[llave] for cid, d in datos.items()}
            if llave == 'partes_iguales' or not sum(pesos.values()):
                pesos = {cid: 1.0 for cid in datos}
            total = sum(pesos.values())
            for cid, p in pesos.items():
                datos[cid][destino] += monto * p / total

        llave_default = {
            'horas': 'horas_normales', 'nomina': 'nomina',
            'partes_iguales': 'partes_iguales'}
        # 1. Gasto con centro. Fijo y nómina vienen suavizados; la energía,
        #    del mes real. Lo que trae centro directo va directo; lo que trae
        #    centro indirecto se junta para repartir; lo que no trae centro se
        #    reparte entre los directos (nómina por sueldos, el resto por
        #    horas normales).
        pools_indirectos = {}
        fuentes = [('fijo', 'fijo', fijo), ('nomina', 'fijo', fijo),
                   ('energia', 'variable', real)]
        for bucket, destino, origen in fuentes:
            for (b, cid), monto in origen.items():
                if b != bucket:
                    continue
                if cid in datos:
                    datos[cid][destino] += monto
                elif cid:
                    pools_indirectos[cid] = pools_indirectos.get(cid, 0.0) + monto
                elif bucket == 'nomina':
                    repartir(monto, 'nomina', 'fijo')
                else:
                    repartir(monto, 'horas_normales', destino)
        # 2. Indirectos → directos con la llave de cada centro.
        for cid, monto in pools_indirectos.items():
            centro = Centro.browse(cid)
            repartir(monto, llave_default.get(centro.reparto_llave, 'horas_normales'),
                     'indirecto')

        # 3. Tarifas.
        self.tarifa_ids.unlink()
        Tarifa = self.env['qb.tarifa']
        tot_fijo = tot_var = tot_ocio = 0.0
        for c in directos:
            d = datos[c.id]
            pool_fijo = d['fijo'] + d['indirecto']
            hn = d['horas_normales']
            hr = d['horas_reales']
            superada = bool(hn) and hr > hn
            if superada:
                hn = hr
            tarifa_fija = pool_fijo / hn if hn else 0.0
            tarifa_var = d['variable'] / hr if hr else 0.0
            ociosidad = max(pool_fijo - tarifa_fija * hr, 0.0) if hn else 0.0
            Tarifa.create({
                'periodo_id': self.id, 'centro_id': c.id,
                'pool_fijo': pool_fijo, 'pool_variable': d['variable'],
                'nomina_llave': d['nomina'],
                'horas_normales': hn, 'horas_fuente': d['horas_fuente'],
                'horas_reales': hr,
                'horas_reales_fuente': d['horas_reales_fuente'],
                'tarifa_fija': tarifa_fija, 'tarifa_variable': tarifa_var,
                'ociosidad': ociosidad, 'capacidad_superada': superada,
            })
            tot_fijo += pool_fijo
            tot_var += d['variable']
            tot_ocio += ociosidad

        # 4. Operación como % de ventas.
        meses_op = int(self.env['qb.parametro'].get_float(
            'suavizado_operacion_meses', 12)) or 1
        suav = self._suavizado(meses_op)
        ventas = sum(v for (b, _c), v in suav.items() if b == 'ventas')
        operacion = sum(v for (b, _c), v in suav.items() if b == 'operacion')
        self.write({
            'calculado_el': fields.Datetime.now(),
            'ventas_month': ventas, 'operacion_month': operacion,
            'op_pct': operacion / ventas if ventas else 0.0,
            'pool_fijo_total': tot_fijo, 'pool_variable_total': tot_var,
            'ociosidad_total': tot_ocio,
            'sin_clasificar': self._gasto_sin_clasificar(
                self.date_from, self.date_to),
        })
        self.bloqueos = '\n'.join(self._bloqueos_cierre()) or False
        return True

    # ------------------------------------------------------------------
    # Compuerta de cierre
    # ------------------------------------------------------------------
    def _bloqueos_cierre(self):
        """Lista de razones por las que el período NO puede cerrar. Vacía =
        cierra. Otros módulos agregan las suyas con super()."""
        self.ensure_one()
        P = self.env['qb.parametro']
        out = []
        if not self.calculado_el:
            out.append('No se ha calculado.')
            return out
        for t in self.tarifa_ids:
            if not t.horas_normales:
                out.append('El centro %s no tiene horas normales (sin '
                           'calendario ni capacidad capturada).'
                           % t.centro_id.code)
            if t.horas_reales_fuente == 'ninguna':
                out.append('El centro %s no tiene horas reales ni estándar '
                           'en el mes.' % t.centro_id.code)
        mal = self.env['qb.cuenta.clase'].cuentas_mal_repartidas(
            self.company_id)
        if mal:
            out.append('Cuentas cuyo reparto no suma 100 %%: %s'
                       % ', '.join(mal.mapped('code')[:10]))
        tol = P.get_float('sin_clasificar_tolerancia', 1000.0)
        if abs(self.sin_clasificar) > tol:
            out.append('Hay $%s de gasto en cuentas sin clasificar.'
                       % '{:,.0f}'.format(self.sin_clasificar))
        top = self._productos_top()
        if top:
            abiertas = self.env['qb.producto.validacion'].search([
                ('product_id', 'in', top.ids), ('estado', '=', 'abierta')])
            if abiertas:
                codes = sorted({v.product_id.default_code or v.product_id.name
                                for v in abiertas})
                out.append('Validaciones abiertas en productos principales: '
                           + ', '.join(codes[:10])
                           + (' …' if len(codes) > 10 else ''))
        return out

    def _productos_top(self):
        """Los N productos de mayor venta del mes (parámetro
        `top_productos`), desde el mayor."""
        self.ensure_one()
        n = int(self.env['qb.parametro'].get_float('top_productos', 20))
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT aml.product_id
            FROM account_move_line aml
            JOIN account_account aa ON aa.id = aml.account_id
            WHERE aml.parent_state = 'posted' AND aml.company_id = %s
              AND aml.date >= %s AND aml.date < %s
              AND aa.account_type = 'income' AND aml.product_id IS NOT NULL
            GROUP BY aml.product_id
            ORDER BY SUM(-aml.balance) DESC
            LIMIT %s
        """, (self.company_id.id, self.date_from, self.date_to, n))
        return self.env['product.product'].browse(
            [r[0] for r in self.env.cr.fetchall()])

    def action_cerrar(self):
        for rec in self:
            if rec.state == 'cerrado':
                continue
            bloqueos = rec._bloqueos_cierre()
            rec.bloqueos = '\n'.join(bloqueos) or False
            if bloqueos:
                raise UserError(
                    'El período %s no puede cerrar:\n- %s'
                    % (rec.period, '\n- '.join(bloqueos)))
            rec.write({'state': 'cerrado', 'cerrado_el': fields.Datetime.now(),
                       'cerrado_por': self.env.user.id})
            rec.message_post(body='Período cerrado: tarifas congeladas.')
            rec._publicar_tarifas()
        return True

    def action_reabrir(self):
        for rec in self:
            if rec.state != 'cerrado':
                continue
            if not (rec.motivo_reapertura or '').strip():
                raise UserError(
                    'Escriba el motivo antes de reabrir %s: reabrir cambia '
                    'tarifas que Odoo ya usó.' % rec.period)
            rec.write({'state': 'borrador',
                       'reaperturas': rec.reaperturas + 1})
            rec.message_post(body='Período reabierto (%d). Motivo: %s'
                             % (rec.reaperturas, rec.motivo_reapertura))
        return True

    # ------------------------------------------------------------------
    # Publicación a Odoo
    # ------------------------------------------------------------------
    def _publicar_tarifas(self):
        self.ensure_one()
        lineas = []
        for t in self.tarifa_ids.filtered(
                lambda t: t.centro_id.publica_tarifa):
            for wc in t.centro_id.workcenter_ids:
                antes = wc.costs_hour
                wc.costs_hour = round(t.tarifa, 2)
                lineas.append('%s: $%.2f → $%.2f/h'
                              % (wc.name, antes, wc.costs_hour))
            t.publicada = True
        if lineas:
            self.message_post(body='Tarifas publicadas a Odoo:<br/>'
                              + '<br/>'.join(lineas))
        return lineas

    def action_publicar_tarifas(self):
        self.ensure_one()
        if self.state != 'cerrado':
            raise UserError('Solo se publican las tarifas de un período '
                            'cerrado: Odoo costea con ellas.')
        self._publicar_tarifas()
        return True

    # ------------------------------------------------------------------
    @api.model
    def para_cotizar(self, company=None):
        """El período con el que se cotiza: el último cerrado."""
        company = company or self.env.company
        return self.search([('company_id', '=', company.id),
                            ('state', '=', 'cerrado')],
                           order='period desc', limit=1)

    @api.model
    def cron_calcular_mes_en_curso(self):
        """Diario: recalcula el mes anterior si sigue en borrador y el mes
        en curso (solo informa; se cotiza con el último cerrado)."""
        hoy = fields.Date.today()
        for company in self.env['res.company'].search([]):
            for meses in (1, 0):
                p = date(hoy.year, hoy.month, 1) - relativedelta(months=meses)
                rec = self.search([('period', '=', p),
                                   ('company_id', '=', company.id)], limit=1)
                if not rec:
                    rec = self.create({'period': p, 'company_id': company.id})
                if rec.state == 'borrador':
                    rec.with_company(company)._calcular()


class QbTarifa(models.Model):
    _name = 'qb.tarifa'
    _description = 'Tarifa por centro y período'
    _order = 'periodo_id desc, centro_id'
    _rec_name = 'centro_id'

    periodo_id = fields.Many2one('qb.periodo', required=True,
                                 ondelete='cascade', index=True)
    period = fields.Date(related='periodo_id.period', store=True)
    state = fields.Selection(related='periodo_id.state', store=True)
    centro_id = fields.Many2one('qb.centro', required=True)
    pool_fijo = fields.Float(
        digits=(16, 2),
        help='Gasto fijo del centro por mes, suavizado: lo suyo del mayor, '
             'más su parte de la nómina de fábrica y de los indirectos.')
    pool_variable = fields.Float(
        digits=(16, 2), help='Energía y consumibles del centro, mes real.')
    nomina_llave = fields.Float(
        digits=(16, 2), string='Nómina (llave)',
        help='Σ sueldo mensual de sus departamentos; solo llave de reparto.')
    horas_normales = fields.Float(digits=(16, 2))
    horas_fuente = fields.Selection([
        ('calendario', 'Calendario de Odoo × eficiencia'),
        ('capturada', 'Capturada en el centro')], readonly=True)
    horas_reales = fields.Float(digits=(16, 2))
    horas_reales_fuente = fields.Selection([
        ('workorder', 'Órdenes de trabajo'),
        ('operacion', 'Estándar de la receta × producido'),
        ('ninguna', 'Sin datos')], readonly=True)
    utilizacion_pct = fields.Float(compute='_compute_utilizacion',
                                   digits=(6, 1))
    tarifa_fija = fields.Float(digits=(16, 4), string='Tarifa fija $/h')
    tarifa_variable = fields.Float(digits=(16, 4),
                                   string='Tarifa variable $/h')
    tarifa = fields.Float(compute='_compute_tarifa', store=True,
                          digits=(16, 4), string='Tarifa $/h')
    ociosidad = fields.Float(
        digits=(16, 2),
        help='Pool fijo − tarifa fija × horas reales: lo que la capacidad '
             'parada costó este mes.')
    capacidad_superada = fields.Boolean(
        help='Horas reales arriba de las normales: la capacidad normal está '
             'desactualizada. Se usó la real como denominador (IAS 2).')
    publicada = fields.Boolean(readonly=True)

    @api.depends('tarifa_fija', 'tarifa_variable')
    def _compute_tarifa(self):
        for rec in self:
            rec.tarifa = rec.tarifa_fija + rec.tarifa_variable

    @api.depends('horas_normales', 'horas_reales')
    def _compute_utilizacion(self):
        for rec in self:
            rec.utilizacion_pct = (100.0 * rec.horas_reales
                                   / rec.horas_normales
                                   if rec.horas_normales else 0.0)
