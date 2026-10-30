# -*- coding: utf-8 -*-
"""Duración de la lista en las órdenes de trabajo (2.22.0, Dirección General 2026-10-30).

Odoo 19 cambia la duración esperada de una orden de trabajo cuando se escribe una sola de sus fechas: al estirar o
encoger la barra en el Gantt, la duración pasa a ser el nuevo rango. Al moverla después, conserva esa duración y
calcula la fecha fin con ella. Caso real (circular 9, 2026-10-29): TL/OP-TEJ/H14243 traía 22,701 min contra los 4,540
de la lista (635 kg × 7.15 min/kg); al ponerla el día 7 terminaba el 22 de noviembre.

Desde 2.22.0, en una orden de trabajo que no ha iniciado:

- Cambiar una sola de sus fechas **mueve** la orden y conserva su duración: la fecha fin se calcula con la duración
  desde la fecha de inicio (la nueva o la que ya tenía). En el Gantt, estirar o encoger la barra ya no cambia la
  duración; arrastrarla completa la mueve, como siempre.
- La duración se cambia solo a propósito: en el campo Duración esperada (o en el formulario, cambiando la fecha
  fin, que la recalcula a la vista del usuario antes de guardar).
- «Duración de la lista» la regresa a la de la lista de materiales con la máquina en que está la orden (la misma cuenta
  de Odoo: preparación + limpieza + ciclos × tiempo del ciclo entre la eficiencia) y recalcula la fecha fin. No toca
  las órdenes con la máquina fijada (``_ft_maquina_fija``: el jet de la Programación de Tintorería lleva el tiempo por
  baño, que no es el de la lista).

Las órdenes iniciadas, terminadas o canceladas siguen como en Odoo. La clave de contexto ``ft_duracion_libre`` deja
el comportamiento nativo para quien lo necesite.
"""
from odoo import api, fields, models

TOLERANCIA_MIN = 1.0  # minutos: abajo de esto no es diferencia
TOLERANCIA_REL = 0.01  # 1 % de la duración de la lista


class MrpWorkorderDuracionLista(models.Model):
    _inherit = 'mrp.workorder'

    ft_duracion_lista = fields.Float(
        string='Duración de la lista', digits=(16, 2), compute='_compute_ft_duracion_lista',
        help='Minutos que da la lista de materiales para esta cantidad en esta máquina (preparación + limpieza + '
             'ciclos × tiempo del ciclo, entre la eficiencia). Vacío si la orden no viene de una operación de la lista, '
             'ya terminó o su máquina se fijó a mano (el jet programado lleva el tiempo por baño).')
    ft_duracion_difiere = fields.Boolean(
        string='Duración distinta de la lista', compute='_compute_ft_duracion_lista',
        search='_search_ft_duracion_difiere',
        help='La duración esperada difiere de la de la lista en más de 1 minuto y más de 1 %.')

    def _ft_duracion_de_lista(self):
        """Minutos de la lista para esta orden de trabajo; 0 si no aplica."""
        self.ensure_one()
        if (not self.operation_id or not self.workcenter_id or self.state in ('done', 'cancel')
                or self._ft_maquina_fija()):
            return 0.0
        return self._get_duration_expected()

    @api.depends('operation_id.time_cycle', 'workcenter_id.time_efficiency', 'qty_production', 'qty_producing',
                 'duration_expected', 'state')
    def _compute_ft_duracion_lista(self):
        for workorder in self:
            lista = workorder._ft_duracion_de_lista()
            workorder.ft_duracion_lista = lista
            diferencia = abs(workorder.duration_expected - lista)
            workorder.ft_duracion_difiere = bool(lista) and diferencia > max(TOLERANCIA_MIN, lista * TOLERANCIA_REL)

    def _search_ft_duracion_difiere(self, operator, value):
        """Solo «distinta» / «igual» sobre órdenes abiertas que vienen de una operación (como el aviso de máquina)."""
        abiertas = self.search([('state', 'not in', ('done', 'cancel')), ('operation_id', '!=', False)])
        distintas = abiertas.filtered('ft_duracion_difiere')
        # Odoo 19 normaliza «!= False» a «not in [False]»; el valor llega como colección o suelto.
        values = list(value) if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)) else [value]
        if operator in ('!=', 'not in'):
            want = not any(values)
        elif operator in ('=', 'in'):
            want = any(values)
        else:
            return NotImplemented
        return [('id', 'in' if want else 'not in', distintas.ids)]

    def action_ft_duracion_lista(self):
        """Regresa la duración esperada a la de la lista y recalcula la fecha fin desde la de inicio."""
        for workorder in self:
            lista = workorder._ft_duracion_de_lista()
            if not lista:
                continue
            workorder.write({'duration_expected': lista})
            if workorder.date_start and workorder.state != 'progress':
                workorder.write({'duration_expected': lista,
                                 'date_finished': workorder._calculate_date_finished()})
        return True

    def write(self, vals):
        fecha = [key for key in ('date_start', 'date_finished') if key in vals]
        if (len(fecha) != 1 or not vals[fecha[0]] or 'duration_expected' in vals or 'state' in vals
                or self.env.context.get('bypass_duration_calculation') or self.env.context.get('ft_duracion_libre')):
            return super().write(vals)
        # Una sola fecha: Odoo recalcularía la duración con el rango nuevo. Con las dos fechas, Odoo conserva la
        # duración y calcula la fecha fin desde la de inicio; aquí se completan las dos.
        solo_inicio = fecha[0] == 'date_start'
        conservar = self.filtered(lambda wo: wo.state not in ('progress', 'done', 'cancel')
                                  and (solo_inicio or wo.date_start))
        for workorder in conservar:
            nuevos = dict(vals)
            if solo_inicio:
                nuevos['date_finished'] = vals['date_start']  # Odoo la recalcula con la duración
            else:
                nuevos['date_start'] = workorder.date_start
            super(MrpWorkorderDuracionLista, workorder).write(nuevos)
        resto = self - conservar
        return super(MrpWorkorderDuracionLista, resto).write(vals) if resto else True
