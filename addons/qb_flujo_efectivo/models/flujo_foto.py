# -*- coding: utf-8 -*-
"""Foto semanal del flujo y comparación de lo proyectado contra lo real.

Cada semana (el lunes, con el cron diario) se guarda la proyección de las 13
semanas. Cuando cada semana termina se llena lo real:

* Saldo real: la caja al cierre de la semana, con la misma regla del reporte
  (último estado de cuenta o saldo contable, a pesos al tipo de cambio de ese
  día).
* Flujo neto real: saldo real al cierre menos saldo real al cierre de la semana
  anterior. Es la cifra confiable: no depende de clasificar movimientos.
* Entradas y salidas reales: movimientos bancarios de las cuentas de caja,
  sin los contactos que no son flujo (traspasos, compra-venta de dólares). Es
  orientativo: un traspaso sin contacto entre cuentas propias infla los dos
  lados.
"""
from datetime import timedelta

from odoo import _, api, fields, models


class QbFlujoFoto(models.Model):
    _name = 'qb.flujo.foto'
    _description = 'Foto semanal del flujo de efectivo a 13 semanas'
    _order = 'fecha desc, id desc'
    _rec_name = 'fecha'

    config_id = fields.Many2one('qb.flujo.config', required=True, ondelete='cascade', index=True)
    company_id = fields.Many2one(related='config_id.company_id', store=True)
    currency_id = fields.Many2one(related='config_id.currency_id')
    fecha = fields.Date(string='Fecha de la foto', required=True, index=True)
    caja_inicial = fields.Monetary(string='Caja al tomarla', currency_field='currency_id')
    linea_ids = fields.One2many('qb.flujo.foto.linea', 'foto_id', string='Semanas')

    @api.model
    def tomar(self, config, hoy=None):
        hoy = hoy or fields.Date.context_today(self)
        motor = self.env['qb.flujo.motor'].with_company(config.company_id)
        res = motor.calcular(config, hoy)
        tot = motor.totales(res)
        foto = self.create({'config_id': config.id, 'fecha': hoy, 'caja_inicial': res['caja_total']})
        self.env['qb.flujo.foto.linea'].create([{
            'foto_id': foto.id, 'semana': i + 1, 'inicio': ini, 'fin': fin,
            'entradas_proy': tot['entradas'][i], 'salidas_proy': tot['salidas'][i],
            'neto_proy': tot['neto'][i], 'saldo_proy': tot['final'][i],
        } for i, (ini, fin) in enumerate(res['semanas'])])
        return foto

    @api.model
    def _cron_foto_semanal(self):
        """Diario: toma la foto si esta semana no tiene una y llena lo real de las semanas que ya cerraron."""
        hoy = fields.Date.context_today(self)
        lunes = hoy - timedelta(days=hoy.weekday())
        for config in self.env['qb.flujo.config'].search([]):
            if not self.search_count([('config_id', '=', config.id), ('fecha', '>=', lunes)]):
                self.tomar(config, hoy)
        self.env['qb.flujo.foto.linea'].search([('real_listo', '=', False), ('fin', '<', hoy)]).llenar_real()
        return True


class QbFlujoFotoLinea(models.Model):
    _name = 'qb.flujo.foto.linea'
    _description = 'Semana de una foto del flujo: proyectado contra real'
    _order = 'foto_id desc, semana'

    foto_id = fields.Many2one('qb.flujo.foto', required=True, ondelete='cascade', index=True)
    config_id = fields.Many2one(related='foto_id.config_id', store=True)
    company_id = fields.Many2one(related='foto_id.company_id', store=True)
    currency_id = fields.Many2one(related='foto_id.currency_id')
    fecha_foto = fields.Date(related='foto_id.fecha', store=True, string='Fecha de la foto')
    semana = fields.Integer(string='Semana de la foto', help='1 = la semana en que se tomó la foto.')
    inicio = fields.Date(required=True)
    fin = fields.Date(required=True, index=True)
    entradas_proy = fields.Monetary(string='Entradas proyectadas', currency_field='currency_id')
    salidas_proy = fields.Monetary(string='Salidas proyectadas', currency_field='currency_id')
    neto_proy = fields.Monetary(string='Neto proyectado', currency_field='currency_id')
    saldo_proy = fields.Monetary(string='Saldo proyectado', currency_field='currency_id')
    entradas_real = fields.Monetary(string='Entradas reales', currency_field='currency_id')
    salidas_real = fields.Monetary(string='Salidas reales', currency_field='currency_id')
    saldo_inicio_real = fields.Monetary(string='Saldo real al empezar', currency_field='currency_id')
    saldo_real = fields.Monetary(string='Saldo real', currency_field='currency_id')
    neto_real = fields.Monetary(string='Neto real', currency_field='currency_id')
    desviacion_neto = fields.Monetary(string='Desviación del neto', currency_field='currency_id',
                                      help='Neto real menos neto proyectado.')
    desviacion_saldo = fields.Monetary(string='Desviación del saldo', currency_field='currency_id',
                                       help='Saldo real menos saldo proyectado.')
    real_listo = fields.Boolean(string='Semana cerrada', index=True)

    def _caja_al(self, config, fecha):
        res = {'caja': []}
        self.env['qb.flujo.motor'].with_company(config.company_id)._caja(config, fecha, res)
        return sum(c['importe'] for c in res['caja'])

    def _movimientos(self, config, ini, fin):
        company = config.company_id
        fuera = config.partner_traspaso_ids.commercial_partner_id.ids
        lines = self.env['account.bank.statement.line'].search([
            ('company_id', '=', company.id), ('journal_id', 'in', config.caja_ids.journal_id.ids),
            ('date', '>=', ini), ('date', '<=', fin),
        ])
        entradas = salidas = 0.0
        for st in lines:
            if st.partner_id and st.partner_id.commercial_partner_id.id in fuera:
                continue
            moneda = st.journal_id.currency_id or company.currency_id
            importe = moneda._convert(st.amount, company.currency_id, company, st.date)
            if importe > 0:
                entradas += importe
            else:
                salidas += importe
        return entradas, salidas

    def llenar_real(self):
        for rec in self:
            config = rec.config_id
            saldo = rec._caja_al(config, rec.fin)
            saldo_ini = rec._caja_al(config, rec.inicio - timedelta(days=1))
            entradas, salidas = rec._movimientos(config, rec.inicio, rec.fin)
            neto = saldo - saldo_ini
            rec.write({
                'saldo_real': saldo, 'saldo_inicio_real': saldo_ini, 'neto_real': neto,
                'entradas_real': entradas, 'salidas_real': salidas,
                'desviacion_neto': neto - rec.neto_proy, 'desviacion_saldo': saldo - rec.saldo_proy,
                'real_listo': True,
            })
        return True

    @api.depends('foto_id.fecha', 'semana', 'inicio')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = _('Foto %(f)s · semana %(s)s', f=rec.fecha_foto, s=rec.semana)
