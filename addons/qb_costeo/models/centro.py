# -*- coding: utf-8 -*-
"""Centro de costo: un proceso con capacidad, cuentas y gente.

Un centro DIRECTO carga horas a los productos (tejido, tintorería, acabado,
entretelas, inspección). Un centro INDIRECTO (almacén, calidad,
mantenimiento, limpieza) no carga horas: su gasto se reparte entre los
directos con una llave. Entretela, importados e inspección no son casos
especiales del código: son centros con su driver.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

DRIVERS = [
    ('workorder', 'Órdenes de trabajo (horas reales)'),
    ('operacion', 'Operación de la receta (tiempo estándar)'),
    ('kg_ciclo', 'Kilos por carga × ciclo de color (tintorería)'),
    ('m_velocidad', 'Metros ÷ velocidad de rama (acabado)'),
    ('fijo_unidad', 'Horas fijas por unidad (inspección)'),
]

LLAVES = [
    ('horas', 'Horas normales de los directos'),
    ('nomina', 'Nómina de los directos'),
    ('partes_iguales', 'Partes iguales'),
]


class QbCentro(models.Model):
    _name = 'qb.centro'
    _description = 'Centro de costo'
    _order = 'sequence, code'

    code = fields.Char(required=True)
    name = fields.Char(required=True)
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    nature = fields.Selection([
        ('directo', 'Directo: carga horas al producto'),
        ('indirecto', 'Indirecto: se reparte a los directos'),
    ], required=True, default='directo')
    driver = fields.Selection(
        DRIVERS, string='Cómo mide horas',
        help='De dónde salen las horas por unidad de cada producto en este '
             'centro. Con órdenes de trabajo capturadas, el driver se vuelve '
             '«workorder» solo: el resto son estándares interinos.')
    workcenter_ids = fields.Many2many(
        'mrp.workcenter', 'qb_costeo_centro_workcenter_rel', 'centro_id',
        'workcenter_id', string='Centros de trabajo de Odoo',
        help='Sus máquinas en Odoo. Las horas reales y la capacidad normal '
             'salen de aquí; la tarifa se publica aquí.')
    department_ids = fields.Many2many(
        'hr.department', 'qb_costeo_centro_department_rel', 'centro_id',
        'department_id', string='Departamentos de RH',
        help='La nómina de fábrica (bucket «nomina») se reparte entre '
             'centros por el sueldo mensual de los empleados activos de '
             'estos departamentos.')
    publica_tarifa = fields.Boolean(
        string='Publicar tarifa a Odoo',
        help='Al cerrar un período, su tarifa $/h se escribe en «Costo por '
             'hora» de sus centros de trabajo; Odoo costea con ella las '
             'órdenes siguientes.')
    reparto_llave = fields.Selection(
        LLAVES, default='horas', string='Llave de reparto',
        help='Solo para centros indirectos: cómo se reparte su gasto entre '
             'los directos.')
    capacidad_h_mes = fields.Float(
        string='Capacidad normal capturada (h/mes)', digits=(16, 2),
        help='Déjelo en 0 para que salga del calendario de los centros de '
             'trabajo (horas del calendario × eficiencia). Capture un valor '
             'solo si el calendario no refleja la capacidad real, y diga '
             'por qué en el motivo.')
    capacidad_motivo = fields.Char(string='Motivo de la capacidad capturada')
    velocidad_min = fields.Float(
        string='Velocidad mínima creíble (u/h)', digits=(16, 2),
        help='Banda para las horas medidas: una orden cuya velocidad '
             '(unidades producidas ÷ horas de sus órdenes de trabajo) quede '
             'abajo de este valor se descarta como dato malo (máquina parada '
             'con la orden abierta). 0 = sin límite.')
    velocidad_max = fields.Float(
        string='Velocidad máxima creíble (u/h)', digits=(16, 2),
        help='Banda para las horas medidas: una orden más rápida que esto se '
             'descarta (horas sin capturar). 0 = sin límite.')
    kg_por_carga = fields.Float(
        string='Kg por carga', digits=(16, 2),
        help='Driver «kg_ciclo»: kilos que entran a una carga de tintorería.')
    velocidad_m_h = fields.Float(
        string='Velocidad (m/h)', digits=(16, 2),
        help='Driver «m_velocidad»: velocidad de rama por default cuando el '
             'producto no tiene la suya.')
    horas_por_unidad = fields.Float(
        string='Horas por unidad', digits=(16, 6),
        help='Driver «fijo_unidad»: horas fijas por unidad (inspección y '
             'empaque de importados).')
    notas = fields.Text()

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)', 'Ya existe un centro con ese código.')

    @api.constrains('nature', 'driver', 'capacidad_h_mes', 'capacidad_motivo')
    def _check_consistencia(self):
        for rec in self:
            if rec.nature == 'directo' and not rec.driver:
                raise ValidationError(
                    'El centro %s es directo: diga cómo mide horas.'
                    % rec.code)
            if rec.capacidad_h_mes and not (rec.capacidad_motivo or '').strip():
                raise ValidationError(
                    'El centro %s tiene capacidad capturada: escriba el '
                    'motivo.' % rec.code)

    # ------------------------------------------------------------------
    # Capacidad normal y horas reales
    # ------------------------------------------------------------------
    def horas_normales(self, date_from, date_to):
        """Horas normales del centro en la ventana: calendario de cada centro
        de trabajo × eficiencia, o la capturada (prorrateada por mes)."""
        self.ensure_one()
        meses = _meses(date_from, date_to)
        if self.capacidad_h_mes:
            return self.capacidad_h_mes * meses, 'capturada'
        total = 0.0
        start = fields.Datetime.to_datetime(date_from)
        end = fields.Datetime.to_datetime(date_to)
        for wc in self.workcenter_ids:
            cal = wc.resource_calendar_id
            if not cal:
                continue
            horas = cal.get_work_hours_count(start, end, compute_leaves=False)
            total += horas * (wc.time_efficiency or 100.0) / 100.0
        return total, 'calendario'

    def horas_reales(self, date_from, date_to):
        """Horas trabajadas en la ventana: órdenes de trabajo terminadas
        (`duration`); si el centro no tiene ninguna, el tiempo estándar de las
        operaciones de la receta de las órdenes de fabricación terminadas."""
        self.ensure_one()
        wcs = self.workcenter_ids.ids
        if not wcs:
            return 0.0, 'ninguna'
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT COALESCE(SUM(wo.duration), 0) / 60.0
            FROM mrp_workorder wo
            WHERE wo.workcenter_id IN %s AND wo.state = 'done'
              AND wo.date_finished >= %s AND wo.date_finished < %s
        """, (tuple(wcs), date_from, date_to))
        horas = self.env.cr.fetchone()[0] or 0.0
        if horas:
            return horas, 'workorder'
        # Sin órdenes de trabajo: estándar de la receta × cantidad producida.
        self.env.cr.execute("""
            WITH producido AS (
                SELECT sm.production_id, SUM(sm.quantity) AS qty
                FROM stock_move sm
                JOIN mrp_production mo ON mo.id = sm.production_id
                WHERE sm.state = 'done' AND sm.product_id = mo.product_id
                  AND mo.state = 'done'
                  AND mo.date_finished >= %s AND mo.date_finished < %s
                GROUP BY sm.production_id
            )
            SELECT COALESCE(SUM(op.time_cycle_manual / 60.0 * p.qty
                                / NULLIF(bom.product_qty, 0)), 0)
            FROM producido p
            JOIN mrp_production mo ON mo.id = p.production_id
            JOIN mrp_bom bom ON bom.id = mo.bom_id
            JOIN mrp_routing_workcenter op ON op.bom_id = bom.id
            WHERE op.workcenter_id IN %s
        """, (date_from, date_to, tuple(wcs)))
        horas = self.env.cr.fetchone()[0] or 0.0
        return horas, ('operacion' if horas else 'ninguna')

    def nomina_mensual(self):
        """Σ sueldo mensual (hr.version vigente) de los empleados activos de
        sus departamentos. Es la llave con la que se reparte la nómina de
        fábrica del mayor; el monto real sale del mayor, no de aquí."""
        self.ensure_one()
        if not self.department_ids:
            return 0.0
        Emp = self.env['hr.employee'].sudo()
        emps = Emp.search([('department_id', 'in', self.department_ids.ids),
                           ('company_id', '=', self.company_id.id)])
        total = 0.0
        for e in emps:
            ver = e.current_version_id or e.version_id
            total += _wage_mensual(ver)
        return total


_FACTOR_MES = {
    'annually': 1 / 12.0, 'semi-annually': 1 / 6.0, 'quarterly': 1 / 3.0,
    'bi-monthly': 1 / 2.0, 'monthly': 1.0, 'semi-monthly': 2.0,
    'bi-weekly': 2.165, 'weekly': 4.33, 'daily': 30.4,
}


def _wage_mensual(version):
    if not version or 'wage' not in version._fields:
        return 0.0
    wage = version.wage or 0.0
    sched = version.schedule_pay if 'schedule_pay' in version._fields else ''
    return wage * _FACTOR_MES.get(sched or 'monthly', 1.0)


def _meses(date_from, date_to):
    d0 = fields.Date.to_date(date_from)
    d1 = fields.Date.to_date(date_to)
    return max((d1.year - d0.year) * 12 + d1.month - d0.month, 1)
