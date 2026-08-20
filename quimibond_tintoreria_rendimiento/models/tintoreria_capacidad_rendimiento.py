# -*- coding: utf-8 -*-
from odoo import api, fields, models


class TintoreriaCapacidadRendimiento(models.Model):
    _name = 'tintoreria.capacidad.rendimiento'
    _description = 'Tabla de Rendimientos y RB Tintorería'
    _rec_name = 'nombre'
    _order = 'codigo'

    codigo = fields.Char(string='Código', required=True,
                          help='Código del centro de trabajo, ej. HTJ1')
    nombre = fields.Char(string='Nombre', required=True,
                          help='Nombre del centro de trabajo, ej. TINTORERIA 1')
    workcenter_id = fields.Many2one(
        'mrp.workcenter', string='Centro de trabajo (Odoo)',
        help='Vínculo opcional al work center real de manufactura, una vez configurado.')
    active = fields.Boolean(default=True)

    capacidad_kg = fields.Float(string='Capacidad (kg)', required=True,
                                 help='Capacidad máxima absoluta de la máquina, en kg.')

    capacidad_grupo_a = fields.Float(
        string='Capacidad Grupo A — Rend. 3-6 m/kg (kg)',
        help='kg permitidos cuando el rendimiento del artículo está entre 3 y 6 m/kg. '
             '0 = no aplica / N/A.')
    capacidad_grupo_b = fields.Float(
        string='Capacidad Grupo B — Rend. 7-10 m/kg (kg)',
        help='kg permitidos cuando el rendimiento del artículo está entre 7 y 10 m/kg. '
             '0 = no aplica / N/A.')
    capacidad_grupo_c = fields.Float(
        string='Capacidad Grupo C — Rend. 11-15 m/kg (kg)',
        help='kg permitidos cuando el rendimiento del artículo está entre 11 y 15 m/kg. '
             '0 = no aplica / N/A.')

    relacion_bano = fields.Float(
        string='Relación de Baño — RB (L/kg)', digits=(12, 4),
        help='Litros de baño necesarios por cada kg de tela, usada para calcular '
             'la cantidad de químicos/agua de la orden de Tintorería '
             '(litros = RB × kg de la orden).')

    notas = fields.Text(string='Notas')

    _codigo_uniq = models.Constraint(
        'unique(codigo)',
        'Ya existe un centro de trabajo de tintorería con ese código.',
    )

    def capacidad_para_rendimiento(self, rendimiento):
        """Devuelve la capacidad en kg (banda de rendimiento) aplicable para un
        rendimiento de tela dado (m/kg). Si el rendimiento no cae en ninguna
        banda con capacidad definida (0 o fuera de rango), regresa 0.0.

        Bandas fijas actuales: A = 3-6, B = 7-10, C = 11-15 m/kg.
        """
        self.ensure_one()
        if 3 <= rendimiento <= 6:
            return self.capacidad_grupo_a
        if 7 <= rendimiento <= 10:
            return self.capacidad_grupo_b
        if 11 <= rendimiento <= 15:
            return self.capacidad_grupo_c
        return 0.0

    def litros_para_kg(self, kg):
        """Litros de baño necesarios para procesar `kg` de tela en este centro."""
        self.ensure_one()
        return self.relacion_bano * kg
