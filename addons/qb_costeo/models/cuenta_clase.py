# -*- coding: utf-8 -*-
"""Clasificación de cuentas del mayor: cada cuenta de resultados dice a qué
bucket va y, si es gasto de fábrica, de qué centro es (con su porcentaje).

Una cuenta sin clasificar es gasto invisible para el costeo. El cron diario
avisa de las cuentas nuevas; la acción «Sin clasificar» las lista.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

BUCKETS = [
    ('ventas', 'Ventas'),
    ('mp', 'Materia prima (costo primo y ajustes de inventario)'),
    ('energia', 'Energía y consumibles (variable del centro)'),
    ('fijo', 'Fijo del centro (renta, mantenimiento, depreciación)'),
    ('nomina', 'Nómina de fábrica (se reparte por RH)'),
    ('operacion', 'Operación (administración y ventas)'),
    ('absorcion', 'Absorción por centro de trabajo (contra 504.01.0099)'),
    ('financiero', 'Resultado financiero (no es costo)'),
    ('no_costeo', 'Fuera del costeo'),
]

# Buckets que se reparten a centros.
BUCKETS_CENTRO = ('energia', 'fijo', 'nomina')

# Mapa desde los buckets del módulo anterior (qb_capacidad_costeo).
_LEGADO = {
    'ventas': 'ventas', 'mp': 'mp', 'importacion': 'mp',
    'energia': 'energia', 'mod': 'nomina',
    'overhead_fab': 'fijo', 'depreciacion': 'fijo',
    'arrend_maquinaria': 'fijo', 'absorcion_odoo': 'absorcion',
    'operacion': 'operacion', 'no_costeo': 'no_costeo',
}


class QbCuentaClase(models.Model):
    _name = 'qb.cuenta.clase'
    _description = 'Clasificación de cuenta para el costeo'
    _order = 'account_id, centro_id'
    _rec_name = 'account_id'

    account_id = fields.Many2one(
        'account.account', required=True, index=True, ondelete='cascade')
    bucket = fields.Selection(BUCKETS, required=True)
    centro_id = fields.Many2one(
        'qb.centro',
        help='Centro al que va el gasto. Vacío en «energia», «fijo» o '
             '«nomina» = se reparte entre los directos con la llave del '
             'período.')
    pct = fields.Float(
        string='% de la cuenta', default=100.0,
        help='Una cuenta puede partirse entre centros con varias filas; la '
             'suma por cuenta debe dar 100.')
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    nota = fields.Char()

    @api.constrains('account_id', 'pct', 'bucket', 'centro_id')
    def _check_pct(self):
        """Cada fila entre 0 y 100; la suma por cuenta no pasa de 100. Que
        la suma llegue a 100 lo revisa `cuentas_mal_repartidas` (la compuerta
        del período), porque las filas se capturan una por una."""
        for rec in self:
            if rec.bucket not in BUCKETS_CENTRO and rec.centro_id:
                raise ValidationError(
                    'La cuenta %s va al bucket «%s», que no se reparte por '
                    'centro.' % (rec.account_id.code, rec.bucket))
            if not 0 < rec.pct <= 100:
                raise ValidationError('El porcentaje va entre 0 y 100.')
            filas = self.search([('account_id', '=', rec.account_id.id),
                                 ('company_id', '=', rec.company_id.id)])
            total = sum(filas.mapped('pct'))
            if total > 100.01:
                raise ValidationError(
                    'La cuenta %s suma %.1f %% entre sus filas; no puede '
                    'pasar de 100.' % (rec.account_id.code, total))

    @api.model
    def cuentas_mal_repartidas(self, company=None):
        """Cuentas cuyas filas no suman 100 %."""
        company = company or self.env.company
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT account_id FROM qb_cuenta_clase
            WHERE company_id = %s GROUP BY account_id
            HAVING ABS(SUM(pct) - 100) > 0.01
        """, (company.id,))
        return self.env['account.account'].browse(
            [r[0] for r in self.env.cr.fetchall()])

    # ------------------------------------------------------------------
    @api.model
    def cuentas_sin_clasificar(self, company=None):
        company = company or self.env.company
        Account = self.env['account.account']
        clasificadas = self.search([('company_id', '=', company.id)])\
            .mapped('account_id').ids
        return Account.with_company(company).search([
            ('account_type', 'in', ('income', 'income_other',
                                    'expense_direct_cost', 'expense',
                                    'expense_depreciation', 'expense_other')),
            ('id', 'not in', clasificadas),
        ])

    def action_sin_clasificar(self):
        cuentas = self.cuentas_sin_clasificar()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Cuentas sin clasificar',
            'res_model': 'account.account',
            'view_mode': 'list,form',
            'domain': [('id', 'in', cuentas.ids)],
        }

    @api.model
    def cron_cuentas_nuevas(self):
        """Diario: deja en el log las cuentas de resultados sin clasificar.
        El período no cierra con cuentas sin clasificar que tengan saldo."""
        for company in self.env['res.company'].search([]):
            cuentas = self.cuentas_sin_clasificar(company)
            if cuentas:
                _logger.warning(
                    'qb_costeo: %d cuentas de resultados sin clasificar en '
                    '%s: %s', len(cuentas), company.name,
                    ', '.join(cuentas.mapped('code')[:20]))

    @api.model
    def importar_clasificacion_legada(self):
        """Copia la clasificación de `qb_capacidad_costeo` si la base la trae
        (misma base, módulos en paralelo). Lee las tablas base (la vista
        `qb.costeo.cuenta.map` es un `_table_query` y no existe en la base):
        por cuenta, la clase activa más específica (cuenta > patrón largo >
        patrón corto), igual que el módulo anterior. Idempotente: no toca
        cuentas ya clasificadas aquí. Devuelve cuántas importó."""
        self.env.cr.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'qb_cuenta_class_account_rel'
        """)
        if not self.env.cr.fetchone():
            return 0
        self.env.cr.execute("""
            SELECT DISTINCT ON (rel.account_id)
                   rel.account_id, c.bucket,
                   COALESCE(c.allocation_pct, 100.0), c.company_id,
                   ce.code
            FROM qb_cuenta_class_account_rel rel
            JOIN qb_costeo_cuenta_class c ON c.id = rel.class_id
            LEFT JOIN qb_costeo_centro ce ON ce.id = c.centro_id
            WHERE c.active
            ORDER BY rel.account_id,
                     COALESCE(c.account_id = rel.account_id, FALSE) DESC,
                     char_length(COALESCE(c.code_pattern, '')) DESC,
                     c.id
        """)
        filas = self.env.cr.fetchall()
        n = 0
        ya = {(r.account_id.id, r.company_id.id) for r in
              self.with_context(active_test=False).search([])}
        Centro = self.env['qb.centro']
        for account_id, bucket, pct, company_id, centro_code in filas:
            if (account_id, company_id) in ya:
                continue
            b = _LEGADO.get(bucket or '', 'no_costeo')
            centro = Centro
            if centro_code and b in BUCKETS_CENTRO:
                centro = Centro.search([('code', '=', centro_code),
                                        ('company_id', '=', company_id)],
                                       limit=1)
            self.create({
                'account_id': account_id, 'bucket': b,
                'centro_id': centro.id if centro else False,
                'pct': 100.0, 'company_id': company_id,
                'nota': 'importada de qb_capacidad_costeo (%s%s)'
                        % (bucket, ', %s' % centro_code if centro_code else ''),
            })
            ya.add((account_id, company_id))
            n += 1
        _logger.info('qb_costeo: %d cuentas importadas del módulo anterior', n)
        return n
