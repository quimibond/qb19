# -*- coding: utf-8 -*-
"""Ritmo por máquina, artículo y periodo, y el recálculo completo."""
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from . import motor
from .descanso import a_local, a_utc

_logger = logging.getLogger(__name__)


class QbTejidoRitmo(models.Model):
    _name = 'qb.tejido.ritmo'
    _description = 'Ritmo de tejido por máquina, artículo y periodo'
    _order = 'periodo desc, workcenter_id, product_id'
    _rec_name = 'workcenter_id'

    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    tipo = fields.Selection([('semana', 'Semana'), ('mes', 'Mes')], required=True, index=True)
    periodo = fields.Date(required=True, index=True,
                          help='Lunes de la semana o día 1 del mes.')
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True,
                                    index=True, ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Artículo', index=True,
                                 ondelete='set null',
                                 help='Vacío = total de la máquina en el periodo.')
    default_code = fields.Char(related='product_id.default_code', store=True)
    es_total = fields.Boolean(string='Total de la máquina')
    rollos = fields.Integer()
    rollos_corrida = fields.Integer(string='Rollos en corrida')
    kg = fields.Float(digits=(12, 2))
    kg_corrida = fields.Float(digits=(12, 2))
    horas_corrida = fields.Float(digits=(12, 2))
    horas_paro = fields.Float(digits=(12, 2))
    horas_cambio = fields.Float(digits=(12, 2))
    horas_sin_medir = fields.Float(digits=(12, 2))
    horas_programadas = fields.Float(digits=(12, 2))
    horas_sin_explicar = fields.Float(digits=(12, 2))
    pct_corrida = fields.Float(string='% en corrida', digits=(6, 1))
    n_paros = fields.Integer(string='Paros')
    kg_h_corrida = fields.Float(string='kg/h en corrida', digits=(12, 2))
    kg_h_tecnica = fields.Float(string='kg/h técnica (P90)', digits=(12, 2))
    kg_h_tipica = fields.Float(string='kg/h típica', digits=(12, 2))
    kg_h_lenta = fields.Float(string='kg/h lenta (P10)', digits=(12, 2))
    kg_h_efectiva = fields.Float(string='kg/h efectiva', digits=(12, 2),
                                 help='Incluye los paros: base del costo por kg.')
    kg_h_p5 = fields.Float(string='kg/h P5', digits=(12, 2))
    kg_h_p95 = fields.Float(string='kg/h P95', digits=(12, 2))
    kg_h_estandar = fields.Float(string='kg/h estándar', digits=(12, 2),
                                 help='60 ÷ minutos por kg de la operación de la receta.')
    kg_perdidos = fields.Float(digits=(12, 1), help='Horas de paro y cambio × kg/h en corrida.')
    ciclo_min = fields.Float(string='Ciclo por rollo (min)', digits=(12, 1))
    peso_tipico = fields.Float(string='Peso típico del rollo (kg)', digits=(12, 2))
    suficiente = fields.Boolean(string='Dato suficiente', default=True)
    calculado_el = fields.Datetime(readonly=True)

    # ------------------------------------------------------------------
    # Parámetros y entrada
    # ------------------------------------------------------------------
    @api.model
    def _params(self):
        P = self.env['qb.parametro']
        p = dict(motor.PARAMS_DEFAULT)
        p.update({
            'kg_max_rollo': P.get_float('ritmo_kg_max_rollo', 150),
            'min_minutos': P.get_float('ritmo_min_minutos', 10),
            'rollos_atrasados': int(P.get_float('ritmo_rollos_atrasados', 3)),
            'factor_paro': P.get_float('ritmo_factor_paro', 3),
            'paro_general_h': P.get_float('ritmo_paro_general_h', 5),
            'min_intervalos_ref': int(P.get_float('ritmo_min_intervalos_ref', 8)),
            'min_rollos_ritmo': int(P.get_float('ritmo_min_rollos', 5)),
            'referencia_cuenta_corrida': bool(P.get_float('ritmo_referencia_cuenta_corrida', 1)),
        })
        return p

    @api.model
    def _centros_tejido(self):
        return self.env['qb.centro'].search([
            ('company_id', '=', self.env.company.id), ('nature', '=', 'directo'),
            ('driver', '=', 'workorder')])

    @api.model
    def _leer_rollos(self, desde, hasta, workcenters):
        """Rollos pesados en [desde, hasta) (fechas locales) en esas máquinas:
        dicts para el motor. Separado para poder sustituirlo en pruebas."""
        if 'mrp.weighing.log' not in self.env or not workcenters:
            return []
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT w.id, w.workcenter_id, wc.name, w.date_time, w.weight_neto,
                   w.user_id, w.production_id, mo.product_id, pt.default_code,
                   w.roll_number
            FROM mrp_weighing_log w
            JOIN mrp_workcenter wc ON wc.id = w.workcenter_id
            LEFT JOIN mrp_production mo ON mo.id = w.production_id
            LEFT JOIN product_product pp ON pp.id = mo.product_id
            LEFT JOIN product_template pt ON pt.id = pp.product_tmpl_id
            WHERE w.workcenter_id IN %s AND w.date_time >= %s AND w.date_time < %s
            ORDER BY w.workcenter_id, w.date_time, w.id
        """, (tuple(workcenters.ids), a_utc(datetime.combine(desde, datetime.min.time())),
              a_utc(datetime.combine(hasta, datetime.min.time()))))
        out = []
        for (wid, wc, wc_name, dt, kg, user, mo, product, code, roll) in self.env.cr.fetchall():
            name = wc_name if isinstance(wc_name, str) else (wc_name or {}).get('es_MX') or \
                next(iter((wc_name or {}).values()), '')
            out.append({'id': wid, 'wc': wc, 'wc_name': name, 't': a_local(dt), 'kg': kg or 0.0,
                        'user': user, 'mo': mo, 'product': product, 'product_code': code,
                        'roll_number': roll})
        return out

    # ------------------------------------------------------------------
    # Recálculo
    # ------------------------------------------------------------------
    @api.model
    def recalcular(self, desde=None, hasta=None):
        """Recalcula intervalos, paros generales y ritmos de los rollos
        pesados entre `desde` y `hasta` (fechas locales; default: últimos
        14 días hasta mañana). Lee también los 10 días previos para tener el
        rollo anterior de cada máquina y las referencias, pero solo escribe
        la ventana. Devuelve (intervalos escritos, ritmos escritos)."""
        company = self.env.company
        P = self.env['qb.parametro']
        # Por MCP las fechas llegan como texto.
        hasta = fields.Date.to_date(hasta) or (date.today() + timedelta(days=1))
        desde = fields.Date.to_date(desde) or (hasta - timedelta(days=15))
        params = self._params()
        centros = self._centros_tejido()
        wcs = centros.mapped('workcenter_ids')
        if not wcs:
            return 0, 0
        margen = timedelta(days=10)
        rollos = self._leer_rollos(desde - margen, hasta, wcs)
        rollos = motor.limpiar(
            rollos, params,
            articulos_excluidos=P.get_list('ritmo_articulos_excluidos'),
            prefijos_excluidos=P.get_list('ritmo_prefijos_excluidos'),
            centros_excluidos=P.get_list('ritmo_centros_excluidos'),
            usuarios_atrasados=P.get_list('ritmo_usuarios_atrasados'))
        descansos = self.env['qb.tejido.descanso'].motor(wcs, desde - margen, hasta)
        paros = motor.paros_generales(rollos, descansos, params)
        # Paros que el usuario desmarcó: no se restan.
        Paro = self.env['qb.tejido.paro.general']
        no_paro = {p.inicio for p in Paro.search(
            [('company_id', '=', company.id), ('es_paro', '=', False)])}
        paros_utc = [(a_utc(a), a_utc(b)) for a, b, _h in paros]
        paros_activos = [p for p, (ua, _ub) in zip(paros, paros_utc) if ua not in no_paro]
        filas, _ = motor.clasificar(rollos, descansos, params, paros=paros_activos)
        ahora = fields.Datetime.now()
        ini_local = datetime.combine(desde, datetime.min.time())
        fin_local = datetime.combine(hasta, datetime.min.time())

        # 1. Paros generales de la ventana (upsert por inicio, se conserva es_paro).
        existentes = {p.inicio: p for p in Paro.search(
            [('company_id', '=', company.id), ('inicio', '>=', a_utc(ini_local)),
             ('inicio', '<', a_utc(fin_local))])}
        vistos = set()
        for (a, b, h) in paros:
            if not (ini_local <= a < fin_local):
                continue
            ua = a_utc(a)
            vistos.add(ua)
            vals = {'fin': a_utc(b), 'inicio_local': a, 'fin_local': b, 'horas': h,
                    'calculado_el': ahora}
            if ua in existentes:
                existentes[ua].write(vals)
            else:
                Paro.create(dict(vals, inicio=ua, company_id=company.id))
        for ua, rec in existentes.items():
            if ua not in vistos and rec.es_paro:
                rec.unlink()

        # 2. Intervalos de la ventana (upsert por pesaje; se conserva motivo_paro).
        Intervalo = self.env['qb.tejido.intervalo']
        en_ventana = [f for f in filas if ini_local <= f['t'] < fin_local]
        exist = {r.weighing_id: r for r in Intervalo.search(
            [('company_id', '=', company.id), ('fecha_hora', '>=', a_utc(ini_local)),
             ('fecha_hora', '<', a_utc(fin_local))])}
        vistos = set()
        n_int = 0
        for f in en_ventana:
            vals = {
                'roll_number': f.get('roll_number'), 'workcenter_id': f['wc'],
                'production_id': f.get('mo') or False, 'product_id': f.get('product') or False,
                'fecha_hora': a_utc(f['t']), 'fecha_local': f['t'],
                'semana': f['semana'], 'mes': f['mes'], 'turno': f['turno'], 'kg': f['kg'],
                'horas_brutas': f['brutas'], 'horas_descanso': f['descanso'],
                'horas_paro_general': f['paro_general'], 'horas_netas': f['netas'],
                'horas_referencia': f['referencia'] or 0.0, 'clase': f['clase'],
                'horas_corrida': f['h_corrida'], 'horas_paro': f['h_paro'],
                'horas_cambio': f['h_cambio'], 'horas_sin_medir': f['h_sin_medir'],
                'kg_corrida': f['kg_corrida'], 'calculado_el': ahora,
            }
            rec = exist.get(f['id'])
            if rec:
                if rec.clase != 'paro' and vals['clase'] != 'paro':
                    vals['motivo_paro'] = False
                rec.write(vals)
            else:
                Intervalo.create(dict(vals, weighing_id=f['id'], company_id=company.id))
            vistos.add(f['id'])
            n_int += 1
        for wid, rec in exist.items():
            if wid not in vistos:
                rec.unlink()

        # 3. Ritmos de los periodos tocados: se recalculan completos con todas
        #    las filas de esos periodos (las de la ventana y las previas).
        semanas = {f['semana'] for f in en_ventana}
        meses = {f['mes'] for f in en_ventana}
        filas_per = [f for f in filas if f['semana'] in semanas or f['mes'] in meses]
        # Las semanas tocadas pueden empezar antes de la ventana leída: completar
        # con los intervalos ya guardados de esos periodos que no vinieron.
        ids_leidos = {f['id'] for f in filas}
        dom = [('company_id', '=', company.id), ('weighing_id', 'not in', list(ids_leidos))]
        if semanas or meses:
            dom += ['|', ('semana', 'in', list(semanas)), ('mes', 'in', list(meses))]
            for r in Intervalo.search(dom):
                filas_per.append({
                    'id': r.weighing_id, 'wc': r.workcenter_id.id,
                    'wc_name': r.workcenter_id.name, 'product': r.product_id.id or None,
                    'product_code': r.default_code, 't': r.fecha_local, 'kg': r.kg,
                    'semana': r.semana, 'mes': r.mes, 'netas': r.horas_netas,
                    'clase': r.clase, 'h_corrida': r.horas_corrida, 'h_paro': r.horas_paro,
                    'h_cambio': r.horas_cambio, 'h_sin_medir': r.horas_sin_medir,
                    'h_ref_paro': 0.0, 'kg_corrida': r.kg_corrida, 'kg_paro': 0.0})

        def horas_programadas(wc, per, tipo):
            a = datetime.combine(per, datetime.min.time())
            b = a + (timedelta(days=7) if tipo == 'semana' else relativedelta(months=1))
            return descansos.horas_programadas(a, b)

        estandar_cache = {}
        Horas = self.env['qb.producto.horas']
        centro_por_wc = {wc.id: c for c in centros for wc in c.workcenter_ids}

        def estandar(wc, product):
            key = (wc, product)
            if key not in estandar_cache:
                val = None
                prod = self.env['product.product'].browse(product) if product else None
                centro = centro_por_wc.get(wc)
                if prod and centro:
                    bom = Horas._bom_vigente(prod)
                    h = Horas._estandar(prod, bom, centro) if bom else 0.0
                    val = (1.0 / h) if h else None
                estandar_cache[key] = val
            return estandar_cache[key]

        rows = motor.ritmos(filas_per, params, horas_programadas, estandar)
        rows = [r for r in rows if (r['tipo'] == 'semana' and r['periodo'] in semanas)
                or (r['tipo'] == 'mes' and r['periodo'] in meses)]
        if semanas or meses:
            self.search([('company_id', '=', company.id), '|',
                         '&', ('tipo', '=', 'semana'), ('periodo', 'in', list(semanas)),
                         '&', ('tipo', '=', 'mes'), ('periodo', 'in', list(meses))]).unlink()
        vals_list = []
        for r in rows:
            vals_list.append({
                'company_id': company.id, 'tipo': r['tipo'], 'periodo': r['periodo'],
                'workcenter_id': r['wc'], 'product_id': r['product'] or False,
                'es_total': r['product'] is None, 'rollos': r['rollos'],
                'rollos_corrida': r['rollos_corrida'], 'kg': r['kg'],
                'kg_corrida': r['kg_corrida'], 'horas_corrida': r['h_corrida'],
                'horas_paro': r['h_paro'], 'horas_cambio': r['h_cambio'],
                'horas_sin_medir': r['h_sin_medir'],
                'horas_programadas': r.get('h_programadas') or 0.0,
                'horas_sin_explicar': r.get('h_sin_explicar') or 0.0,
                'pct_corrida': 100.0 * (r.get('pct_corrida') or 0.0),
                'n_paros': r['n_paros'], 'kg_h_corrida': r.get('kg_h_corrida') or 0.0,
                'kg_h_tecnica': r.get('kg_h_tecnica') or 0.0,
                'kg_h_tipica': r.get('kg_h_tipica') or 0.0,
                'kg_h_lenta': r.get('kg_h_lenta') or 0.0,
                'kg_h_efectiva': r.get('kg_h_efectiva') or 0.0,
                'kg_h_p5': r.get('kg_h_p5') or 0.0, 'kg_h_p95': r.get('kg_h_p95') or 0.0,
                'kg_h_estandar': r.get('kg_h_estandar') or 0.0,
                'kg_perdidos': r.get('kg_perdidos') or 0.0,
                'ciclo_min': r.get('ciclo_min') or 0.0,
                'peso_tipico': r.get('peso_tipico') or 0.0,
                'suficiente': bool(r.get('suficiente')), 'calculado_el': ahora,
            })
        if vals_list:
            self.create(vals_list)
        # 4. Capacidad medida del centro.
        for c in centros:
            c._calcular_ritmo_medido()
        _logger.info('qb_tejido_ritmo: %d intervalos y %d ritmos (%s → %s)',
                     n_int, len(vals_list), desde, hasta)
        return n_int, len(vals_list)

    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    # ------------------------------------------------------------------
    # Lectura para el costeo
    # ------------------------------------------------------------------
    @api.model
    def efectiva_por_producto(self, workcenters, meses=3, hasta=None):
        """{product_id: (kg_h_efectiva, rollos, kg_h_corrida)} ponderado por
        kilos en los últimos `meses` meses (filas mensuales por artículo)."""
        hasta = hasta or date.today()
        desde = (hasta.replace(day=1) - relativedelta(months=meses - 1))
        acc = defaultdict(lambda: [0.0, 0.0, 0.0, 0])   # kg, h_efectivas, h_corrida, rollos
        for r in self.search([('company_id', '=', self.env.company.id), ('tipo', '=', 'mes'),
                              ('es_total', '=', False), ('suficiente', '=', True),
                              ('workcenter_id', 'in', workcenters.ids),
                              ('periodo', '>=', desde), ('product_id', '!=', False)]):
            if not r.kg_h_efectiva or not r.kg_corrida:
                continue
            a = acc[r.product_id.id]
            a[0] += r.kg_corrida
            a[1] += r.kg_corrida / r.kg_h_efectiva
            a[2] += r.horas_corrida
            a[3] += r.rollos
        return {pid: (kg / h_ef, n, kg / h_corr if h_corr else 0.0)
                for pid, (kg, h_ef, h_corr, n) in acc.items() if h_ef > 0}
