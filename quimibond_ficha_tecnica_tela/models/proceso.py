# -*- coding: utf-8 -*-
"""Proceso del artículo en la máquina (2.8.0, Dirección General 2026-10-14).

La configuración por fecha guarda lo que es **de la máquina** (galga,
capacidad del jet…). Hay datos que son **del artículo en esa máquina**: la
velocidad a la que pasa cada artículo en cada rama (m/min), y después el
tiempo de ciclo en cada jet, la velocidad en la inspeccionadora, etc.

Un proceso es un artículo (código de la tela, ligado al producto cuando el
código existe una sola vez) en una máquina, con un valor por cada parámetro
de ámbito «Del artículo en la máquina» del tipo de la máquina. Los
parámetros se dan de alta por tipo en Configuración → Tipos de máquina, así
que un área nueva no necesita código: parámetros, procesos (a mano o desde
Excel) y listo.

- Que exista el proceso significa que el artículo **corre en esa máquina**
  (la planta lo marca así en sus hojas); un valor vacío es un dato que la
  planta no ha dado, no un cero.
- La máquina muestra el promedio de cada parámetro numérico entre sus
  artículos (p. ej. la velocidad promedio de la rama): se calcula, no se
  captura, para que no se desactualice.
- Los parámetros con clave «velocidad» en m/min dan los metros por hora
  (velocidad × 60) y, si hay «merma» (%), los metros por hora netos.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

from .maquina_config import _fmt


class FichaTecnicaProceso(models.Model):
    _name = 'ficha.tecnica.proceso'
    _description = 'Proceso del artículo en la máquina'
    _order = 'articulo, workcenter_id'

    articulo = fields.Char(string='Artículo', required=True, index=True,
                           help='Código del artículo tal como lo maneja la planta (referencia interna del producto).')
    product_id = fields.Many2one('product.product', string='Producto', index=True,
                                 compute='_compute_product_id', store=True, readonly=False, precompute=True,
                                 help='Se liga solo cuando la referencia interna existe en un solo producto; si '
                                      'no, queda vacío y se puede elegir a mano.')
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True, index=True,
                                    ondelete='restrict', domain="[('ft_tipo_id', '!=', False)]")
    tipo_id = fields.Many2one(related='workcenter_id.ft_tipo_id', string='Tipo de máquina', store=True)
    valor_ids = fields.One2many('ficha.tecnica.proceso.valor', 'proceso_id', string='Parámetros',
                                context={'active_test': False})
    resumen = fields.Char(string='Parámetros', compute='_compute_resumen')
    metros_hora = fields.Float(string='m/hora', digits=(16, 1), compute='_compute_metros_hora',
                               help='Velocidad (m/min) × 60.')
    metros_hora_neto = fields.Float(string='m/hora con merma', digits=(16, 1), compute='_compute_metros_hora',
                                    help='m/hora × (1 − merma %). Igual a m/hora si no hay merma.')
    fecha = fields.Date(string='Fecha del dato', default=fields.Date.context_today,
                        help='Cuándo dio la planta este dato.')
    fuente = fields.Char(string='Fuente', help='De dónde sale el dato (hoja, persona, prueba).')
    notas = fields.Text(string='Notas')
    active = fields.Boolean(default=True)

    _articulo_workcenter_uniq = models.Constraint(
        'unique(articulo, workcenter_id)', 'Ese artículo ya tiene proceso en esa máquina.')

    @api.depends('articulo')
    def _compute_product_id(self):
        Product = self.env['product.product']
        for proceso in self:
            code = (proceso.articulo or '').strip()
            if proceso.product_id.default_code == code and code:
                continue
            found = Product.search([('default_code', '=', code)], limit=2) if code else Product
            proceso.product_id = found if len(found) == 1 else False

    @api.depends('articulo', 'workcenter_id.name')
    def _compute_display_name(self):
        for proceso in self:
            proceso.display_name = ' · '.join(p for p in (proceso.articulo, proceso.workcenter_id.name) if p)

    @api.depends('valor_ids.valor', 'valor_ids.valor_texto', 'valor_ids.parametro_id.name',
                 'valor_ids.parametro_id.unit')
    def _compute_resumen(self):
        for proceso in self:
            parts = []
            for line in proceso.valor_ids:
                shown = line._display_value()
                if shown:
                    parts.append('%s: %s' % (line.parametro_id.name, shown))
            proceso.resumen = ' · '.join(parts)

    @api.depends('valor_ids.valor', 'valor_ids.parametro_id.code', 'valor_ids.parametro_id.unit')
    def _compute_metros_hora(self):
        for proceso in self:
            lines = {line.code: line for line in proceso.valor_ids}
            velocidad = lines.get('velocidad')
            por_hora = 0.0
            if velocidad and (velocidad.unit or '').replace(' ', '').lower() == 'm/min':
                por_hora = velocidad.valor * 60.0
            merma = lines.get('merma')
            proceso.metros_hora = por_hora
            proceso.metros_hora_neto = por_hora * (1.0 - (merma.valor if merma else 0.0) / 100.0)

    def _ft_value(self, code):
        """Valor del parámetro ``code`` (número o texto según el parámetro); False si no está o va vacío."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.code == code)[:1]
        if not line:
            return False
        return (line.valor_texto if line.kind == 'text' else line.valor) or False

    def _ft_set_value(self, code, value):
        """Escribe el valor del parámetro ``code`` del tipo de la máquina (número o texto)."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.code == code)[:1]
        if not line:
            param = self.tipo_id._ft_parametros('articulo').filtered(lambda p: p.code == code)[:1]
            if not param:
                raise ValidationError('El tipo de máquina «%s» no tiene el parámetro del artículo «%s»; dele de '
                                      'alta en Configuración → Tipos de máquina.' % (
                                          self.tipo_id.name or '(sin tipo)', code))
            line = self.env['ficha.tecnica.proceso.valor'].create({'proceso_id': self.id, 'parametro_id': param.id})
        line.write({'valor_texto': value or False} if line.kind == 'text' else {'valor': value or 0.0})
        return line

    @api.model
    def _ft_buscar(self, articulo, workcenter):
        """El proceso de un artículo (código o producto) en una máquina; vacío si no corre ahí."""
        if isinstance(articulo, models.BaseModel):
            articulo = articulo.default_code
        if not articulo or not workcenter:
            return self.browse()
        return self.search([('articulo', '=', articulo.strip()), ('workcenter_id', '=', workcenter.id)], limit=1)

    def _ft_sync_valores(self):
        """Un renglón por parámetro del artículo del tipo de la máquina (vacío si no se capturó)."""
        Valor = self.env['ficha.tecnica.proceso.valor']
        for proceso in self:
            proceso.valor_ids.filtered(lambda v: v.parametro_id.tipo_id != proceso.tipo_id).unlink()
            present = proceso.valor_ids.parametro_id
            if not proceso.tipo_id:
                continue
            missing = proceso.tipo_id._ft_parametros('articulo').filtered(lambda p: p not in present)
            if missing:
                Valor.create([{'proceso_id': proceso.id, 'parametro_id': p.id} for p in missing])

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('articulo'):
                vals['articulo'] = vals['articulo'].strip()
        procesos = super().create(vals_list)
        procesos._ft_sync_valores()
        return procesos

    def write(self, vals):
        if vals.get('articulo'):
            vals['articulo'] = vals['articulo'].strip()
        res = super().write(vals)
        if 'workcenter_id' in vals:
            self._ft_sync_valores()
        return res


class FichaTecnicaProcesoValor(models.Model):
    _name = 'ficha.tecnica.proceso.valor'
    _inherit = ['ficha.tecnica.valor.mixin']
    _description = 'Valor de parámetro del artículo en la máquina'
    _order = 'proceso_id, sequence, id'

    proceso_id = fields.Many2one('ficha.tecnica.proceso', string='Proceso', required=True, index=True,
                                 ondelete='cascade')
    # Guardados para la tabla dinámica (artículo × máquina) y los filtros.
    articulo = fields.Char(related='proceso_id.articulo', string='Artículo', store=True, index=True)
    workcenter_id = fields.Many2one(related='proceso_id.workcenter_id', string='Máquina', store=True, index=True)
    tipo_id = fields.Many2one(related='proceso_id.tipo_id', string='Tipo de máquina', store=True)
    active = fields.Boolean(related='proceso_id.active', store=True)

    _proceso_parametro_uniq = models.Constraint('unique(proceso_id, parametro_id)',
                                                'El parámetro ya está en ese proceso.')

    @api.constrains('parametro_id', 'proceso_id')
    def _check_tipo(self):
        for line in self:
            if line.parametro_id.tipo_id != line.proceso_id.tipo_id:
                raise ValidationError('El parámetro «%s» es de %s y la máquina %s es de %s.' % (
                    line.parametro_id.name, line.parametro_id.tipo_id.name, line.proceso_id.workcenter_id.name,
                    line.proceso_id.tipo_id.name or '(sin tipo)'))
            if line.parametro_id.ambito != 'articulo':
                raise ValidationError('El parámetro «%s» es de la máquina: se captura en su configuración por '
                                      'fecha, no en el proceso del artículo.' % line.parametro_id.name)


class MrpWorkcenterProceso(models.Model):
    _inherit = 'mrp.workcenter'

    ft_proceso_ids = fields.One2many('ficha.tecnica.proceso', 'workcenter_id', string='Procesos por artículo')
    ft_proceso_count = fields.Integer(string='Artículos', compute='_compute_ft_proceso')
    ft_proceso_promedios = fields.Char(string='Promedio entre sus artículos', compute='_compute_ft_proceso',
                                       help='Promedio de cada parámetro numérico del artículo entre los artículos '
                                            'que corren en la máquina y lo tienen capturado.')

    @api.depends('ft_proceso_ids.valor_ids.valor')
    def _compute_ft_proceso(self):
        for wc in self:
            procesos = wc.ft_proceso_ids
            wc.ft_proceso_count = len(procesos)
            parts = []
            for param in (wc.ft_tipo_id._ft_parametros('articulo') if wc.ft_tipo_id else []):
                if param.kind != 'num':
                    continue
                valores = [v.valor for v in procesos.valor_ids if v.parametro_id == param and v.valor]
                if valores:
                    avg = sum(valores) / len(valores)
                    parts.append('%s: %s%s (%d)' % (param.name, _fmt(round(avg, 1)),
                                                    ' %s' % param.unit if param.unit else '', len(valores)))
            wc.ft_proceso_promedios = ' · '.join(parts)

    def action_ft_procesos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Procesos — %s' % self.name,
                'res_model': 'ficha.tecnica.proceso', 'view_mode': 'list,form',
                'domain': [('workcenter_id', '=', self.id)], 'context': {'default_workcenter_id': self.id}}


class FichaTecnicaAcabadoProceso(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    proceso_ids = fields.Many2many('ficha.tecnica.proceso', string='Procesos por máquina',
                                   compute='_compute_proceso_ids')

    def _compute_proceso_ids(self):
        Proceso = self.env['ficha.tecnica.proceso']
        for ficha in self:
            codes = {c for c in (ficha.articulo, ficha.product_acabado_id.default_code) if c}
            ficha.proceso_ids = Proceso.search([('articulo', 'in', list(codes))]) if codes else Proceso


class ProductTemplateProceso(models.Model):
    _inherit = 'product.template'

    ft_proceso_count = fields.Integer(string='Procesos por máquina', compute='_compute_ft_proceso_count')

    def _ft_proceso_domain(self):
        self.ensure_one()
        codes = [c for c in self.product_variant_ids.mapped('default_code') if c]
        return ['|', ('product_id', 'in', self.product_variant_ids.ids), ('articulo', 'in', codes)]

    def _compute_ft_proceso_count(self):
        Proceso = self.env['ficha.tecnica.proceso']
        for tmpl in self:
            tmpl.ft_proceso_count = Proceso.search_count(tmpl._ft_proceso_domain()) if tmpl.id else 0

    def action_ft_procesos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Procesos por máquina — %s' % self.display_name,
                'res_model': 'ficha.tecnica.proceso', 'view_mode': 'list,form',
                'domain': self._ft_proceso_domain(),
                'context': {'default_articulo': self.default_code or False}}


class ProductProductProceso(models.Model):
    _inherit = 'product.product'

    # 2.8.1: la ficha de la variante hereda los botones de la plantilla, pero
    # _inherits solo delega campos (ft_proceso_count), no métodos: sin esto
    # Odoo marca la vista de variante como inválida («action_ft_procesos no
    # es una acción válida en product.product»).
    def action_ft_procesos(self):
        self.ensure_one()
        return self.product_tmpl_id.action_ft_procesos()
