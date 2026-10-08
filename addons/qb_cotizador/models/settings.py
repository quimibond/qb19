# -*- coding: utf-8 -*-
"""Ajustes del cotizador: la cara de los parámetros del sistema.

Lo que el brief deja sin definir queda **vacío** a propósito (suplente,
margen mínimo, descuento de la escalera); mientras no tenga valor no hay
aviso ni efecto."""
from odoo import fields, models

PARAM_APROBADOR = 'qb_cotizador.aprobador_job_id'
PARAM_SUPLENTE = 'qb_cotizador.suplente_job_id'
PARAM_VENTAS = 'qb_cotizador.ventas_job_id'
PARAM_MARGEN_MINIMO = 'qb_cotizador.margen_minimo_pct'
PARAM_VALIDEZ_DIAS = 'qb_cotizador.validez_dias'
PARAM_SEGUIMIENTO_DIAS = 'qb_cotizador.seguimiento_dias_habiles'
PARAM_ARCHIVAR_DIAS = 'qb_cotizador.borrador_archivar_dias'
PARAM_ESCALERA_PCT = 'qb_cotizador.escalera_descuento_pct'
PARAM_REVISION_TOLERANCIA = 'qb_cotizador.revision_tolerancia_pct'

# Nombres con los que se buscan los puestos al instalar (producción, 2026-10-08).
APROBADOR_JOB_NAME = 'DIRECTOR DE FINANZAS Y ADMINISTRACION'
VENTAS_JOB_NAME = 'ADMINISTRADOR DE VENTAS Y MARKETING'


class ResConfigSettingsCotizador(models.TransientModel):
    _inherit = 'res.config.settings'

    qb_cotizador_aprobador_job_id = fields.Many2one(
        'hr.job', string='Puesto que aprueba las cotizaciones',
        config_parameter=PARAM_APROBADOR,
        help='Procedimiento C1: solo este puesto pasa una cotización de «Por aprobar» a '
             '«Presentada». Por omisión, Director de Finanzas y Administración (puesto 183).')
    qb_cotizador_suplente_job_id = fields.Many2one(
        'hr.job', string='Suplente para aprobar', config_parameter=PARAM_SUPLENTE,
        help='Puesto que aprueba cuando el titular no está. Vacío: no está definido quién '
             '(brief C1 2026-10-06).')
    qb_cotizador_ventas_job_id = fields.Many2one(
        'hr.job', string='Puesto de Ventas que da seguimiento', config_parameter=PARAM_VENTAS,
        help='Recibe la actividad de seguimiento a los días hábiles de presentada y la de '
             'decidir al vencer. Por omisión, Administrador de Ventas y Marketing (puesto 207).')
    qb_cotizador_margen_minimo_pct = fields.Float(
        string='Margen neto mínimo (%)', config_parameter=PARAM_MARGEN_MINIMO,
        help='Debajo de este margen neto la cotización avisa al pedir aprobación. Vacío o 0: '
             'sin aviso (el brief no define el valor).')
    qb_cotizador_validez_dias = fields.Integer(
        string='Días de validez de una cotización', config_parameter=PARAM_VALIDEZ_DIAS, default=15,
        help='«Válida hasta» = fecha de presentación + estos días naturales.')
    qb_cotizador_seguimiento_dias_habiles = fields.Integer(
        string='Días hábiles para el seguimiento', config_parameter=PARAM_SEGUIMIENTO_DIAS, default=5,
        help='A los N días hábiles de presentada sin respuesta, actividad a Ventas.')
    qb_cotizador_borrador_archivar_dias = fields.Integer(
        string='Días para archivar un borrador sin movimiento',
        config_parameter=PARAM_ARCHIVAR_DIAS, default=30)
    qb_cotizador_escalera_descuento_pct = fields.Float(
        string='Descuento por duplicar el volumen (%)', config_parameter=PARAM_ESCALERA_PCT,
        help='Escalera de volumen (½×, 1×, 2×, 4×): descuento por cada duplicación, nunca '
             'debajo del piso a planta llena y sin que baje la contribución mensual. Vacío o '
             '0: no se calcula escalera.')
    qb_cotizador_revision_tolerancia_pct = fields.Float(
        string='Tolerancia del recálculo por revisión (%)',
        config_parameter=PARAM_REVISION_TOLERANCIA, default=0.5,
        help='Al recalcular una cotización por una revisión del desarrollo, un cambio del piso '
             'lleno menor a este % cuenta como «no cambió».')
