# -*- coding: utf-8 -*-
from odoo import api, fields, models


class TintoreriaCapacidadRendimiento(models.Model):
    _name = 'tintoreria.capacidad.rendimiento'
    _description = 'Tabla de Rendimientos y RB Tintorería'
    _rec_name = 'codigo'
    _order = 'codigo'

    workcenter_id = fields.Many2one(
        'mrp.workcenter', string='Centro de trabajo', required=True,
        domain=[('name', '=ilike', 'Tintoreria%')],
        help='Solo se pueden seleccionar centros de trabajo cuyo nombre '
             'empiece con "Tintoreria". El código se toma directamente '
             'de este centro de trabajo (el nombre ya se ve en este '
             'mismo campo, por eso no se repite en un campo aparte).')
    codigo = fields.Char(
        string='Código', related='workcenter_id.code', store=True, readonly=True,
        help='Código del centro de trabajo, tomado de Manufactura > '
             'Centros de Trabajo — no editable aquí.')
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

    _workcenter_uniq = models.Constraint(
        'unique(workcenter_id)',
        'Ya existe una configuración de rendimiento para ese centro de trabajo.',
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

    @api.model
    def _load_default_data(self):
        """Carga inicial de las 5 tintorerías conocidas (HTJ1-HTJ5), tomada
        del catálogo de capacidades de origen. Es idempotente a propósito:
        si ya existe una configuración para ese centro de trabajo (por
        haberse creado antes, por una migración previa, o por cualquier
        estado intermedio de la base de datos), no la toca ni la
        duplica — así nunca choca con la restricción de unicidad de
        `workcenter_id`, sin importar en qué momento o cuántas veces se
        ejecute esta carga.
        """
        Workcenter = self.env['mrp.workcenter']
        defaults = [
            # código, capacidad_kg, grupo_a, grupo_b, grupo_c, relacion_bano, notas
            ('HTJ1', 1000, 900, 600, 450, 10, ''),
            ('HTJ2', 950, 450, 300, 300, 7, ''),
            ('HTJ3', 600, 600, 0, 0, 6, 'Grupo B y C: N/A en archivo de origen.'),
            ('HTJ4', 300, 300, 150, 150, 6, ''),
            ('HTJ5', 1200, 900, 600, 450, 7, ''),
        ]
        for codigo, cap, grupo_a, grupo_b, grupo_c, rb, notas in defaults:
            workcenter = Workcenter.search([('code', '=', codigo)], limit=1)
            if not workcenter:
                continue
            if self.search_count([('workcenter_id', '=', workcenter.id)]):
                continue
            self.create({
                'workcenter_id': workcenter.id,
                'capacidad_kg': cap,
                'capacidad_grupo_a': grupo_a,
                'capacidad_grupo_b': grupo_b,
                'capacidad_grupo_c': grupo_c,
                'relacion_bano': rb,
                'notas': notas,
            })
