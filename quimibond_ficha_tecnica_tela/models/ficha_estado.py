# -*- coding: utf-8 -*-
"""2.33.0 (Jose, 2026-11-24): la ficha técnica manda y la lista de materiales sale de ella, en orden.

Antes la ficha no tenía estado propio: lo que se veía («Lista para activar», «Activa con pendientes») era el de sus
listas, «Generar listas» se podía apretar con la ficha a medias y la lista se activaba desde la lista. Ahora la ficha
de acabado tiene su estado, con la barra de etapas de Odoo:

- **Borrador**: Ingeniería captura. Sin listas.
- **En revisión** («Enviar a revisión», solo con la ficha completa): se generan las listas archivadas y cada área ve y
  corrige sus pendientes. «Generar listas» solo aquí.
- **Activa** («Activar», solo con cero pendientes): copia la ficha a las listas, las activa las dos y Odoo fabrica con
  ellas. Una lista del flujo ya no se activa a mano desde la lista.
- **Obsoleta** («Marcar obsoleta»): la tela ya no se fabrica; se archivan la ficha y sus listas.

Para cambiar una ficha activa: «Modificar» la regresa a En revisión. La lista activa sigue fabricando mientras tanto y
al volver a activar se pone al día con la ficha.

La ficha además muestra todo lo de la tela, también sus dos listas de materiales (componentes, operaciones y
subproductos), solo para ver: se cambian en la ficha y se pasan con «Corregir con la ficha».
"""
from odoo import _, api, fields, models
from odoo.exceptions import UserError

from .ficha_producto import FT_CORREGIBLES

ESTADOS_FICHA = [
    ('borrador', 'Borrador'),
    ('revision', 'En revisión'),
    ('activa', 'Activa'),
    ('obsoleta', 'Obsoleta'),
]


class FichaTecnicaAcabadoEstado(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    estado = fields.Selection(
        ESTADOS_FICHA, string='Estado', default='borrador', required=True, index=True, copy=False,
        group_expand='_ft_expandir_estados',
        help='Borrador: Ingeniería captura. En revisión: las listas se generan archivadas y cada área corrige sus '
             'pendientes. Activa: Odoo fabrica con sus listas. Obsoleta: la tela ya no se fabrica.')
    ft_faltantes = fields.Char(string='Falta para enviar a revisión', compute='_compute_ft_faltantes')
    # La revisión de la tela (la misma de la noche y de «Revisar»).
    ft_revision_tela_id = fields.Many2one('ficha.tecnica.revision', string='Revisión de las listas',
                                          compute='_compute_ft_listas')
    ft_pendientes_n = fields.Integer(string='Núm. de pendientes', compute='_compute_ft_listas')
    ft_pendientes_ids = fields.Many2many('ficha.tecnica.revision.pendiente', string='Pendientes de la tela',
                                         compute='_compute_ft_listas')
    ft_revisada_el = fields.Datetime(string='Listas revisadas el', compute='_compute_ft_listas')
    ft_corregible = fields.Boolean(string='Pendientes que corrige la ficha', compute='_compute_ft_listas')
    # Las dos listas del flujo, para verlas dentro de la ficha.
    ft_lista_kg_id = fields.Many2one('mrp.bom', string='Lista de materiales en kg', compute='_compute_ft_listas')
    ft_lista_kg_activa = fields.Boolean(string='Lista en kg en uso', compute='_compute_ft_listas')
    ft_lista_kg_line_ids = fields.Many2many('mrp.bom.line', string='Componentes en kg', compute='_compute_ft_listas')
    ft_lista_kg_op_ids = fields.Many2many('mrp.routing.workcenter', string='Operaciones en kg',
                                          compute='_compute_ft_listas', context={'active_test': False})
    ft_lista_kg_sub_ids = fields.Many2many('mrp.bom.byproduct', string='Subproductos en kg',
                                           compute='_compute_ft_listas')
    ft_lista_m_id = fields.Many2one('mrp.bom', string='Lista de materiales en metros', compute='_compute_ft_listas')
    ft_lista_m_activa = fields.Boolean(string='Lista en metros en uso', compute='_compute_ft_listas')
    ft_lista_m_line_ids = fields.Many2many('mrp.bom.line', string='Componentes en metros',
                                           compute='_compute_ft_listas')
    ft_lista_m_op_ids = fields.Many2many('mrp.routing.workcenter', string='Operaciones en metros',
                                         compute='_compute_ft_listas', context={'active_test': False})
    ft_lista_m_sub_ids = fields.Many2many('mrp.bom.byproduct', string='Subproductos en metros',
                                          compute='_compute_ft_listas')
    ft_ordenes_n = fields.Integer(string='Órdenes de fabricación de la tela', compute='_compute_ft_listas')

    def _ft_expandir_estados(self, estados, domain):
        """El kanban muestra las cuatro columnas aunque alguna esté vacía."""
        return [clave for clave, _nombre in ESTADOS_FICHA]

    @api.depends('product_acabado_id', 'product_proceso_id', 'tejido_id', 'peso_acabado', 'ancho_acabado',
                 'rendimiento_tela_acabada', 'consumo_crudo')
    def _compute_ft_faltantes(self):
        for ficha in self:
            ficha.ft_faltantes = ', '.join(ficha._ft_faltantes()) or False

    def _ft_faltantes(self):
        """Lo que la ficha necesita para generar sus listas (lo mismo que pide «Generar listas»)."""
        self.ensure_one()
        faltan = []
        if not self.product_acabado_id:
            faltan.append(_('la tela acabada (m)'))
        if not self.product_proceso_id:
            faltan.append(_('el acabado en proceso (kg)'))
        if not self.tejido_id.product_proceso_id:
            faltan.append(_('el crudo (ficha de tejido)'))
        if not self.peso_acabado:
            faltan.append(_('el peso'))
        if not self.ancho_acabado:
            faltan.append(_('el ancho'))
        if not self.rendimiento_tela_acabada:
            faltan.append(_('el rendimiento'))
        if not self.ruta_familia_id:
            faltan.append(_('la ruta de la familia del crudo'))
        if not self.consumo_crudo and not self.env['ficha.tecnica.revision']._ft_bom_de(self.product_proceso_id):
            faltan.append(_('el consumo de crudo'))
        return faltan

    @api.depends('product_acabado_id', 'product_proceso_id', 'estado')
    def _compute_ft_listas(self):
        Revision = self.env['ficha.tecnica.revision']
        Production = self.env['mrp.production']
        for ficha in self:
            revision = Revision.search([('acabado_id', '=', ficha.id)], limit=1) if ficha.id else Revision
            ficha.ft_revision_tela_id = revision
            ficha.ft_pendientes_n = revision.n_pendientes
            ficha.ft_pendientes_ids = revision.pendiente_ids
            ficha.ft_revisada_el = revision.fecha
            ficha.ft_corregible = any(p.tipo in FT_CORREGIBLES for p in revision.pendiente_ids)
            kg = Revision._ft_bom_de(ficha.product_proceso_id)
            m = Revision._ft_bom_de(ficha.product_acabado_id)
            ficha.ft_lista_kg_id = kg
            ficha.ft_lista_kg_activa = kg.active
            ficha.ft_lista_kg_line_ids = kg.bom_line_ids
            ficha.ft_lista_kg_op_ids = kg.with_context(active_test=False).operation_ids
            ficha.ft_lista_kg_sub_ids = kg.byproduct_ids
            ficha.ft_lista_m_id = m
            ficha.ft_lista_m_activa = m.active
            ficha.ft_lista_m_line_ids = m.bom_line_ids
            ficha.ft_lista_m_op_ids = m.with_context(active_test=False).operation_ids
            ficha.ft_lista_m_sub_ids = m.byproduct_ids
            productos = (ficha.product_proceso_id | ficha.product_acabado_id).ids
            ficha.ft_ordenes_n = Production.search_count([('product_id', 'in', productos)]) if productos else 0

    # ------------------------------------------------------------------
    # Pasos
    # ------------------------------------------------------------------
    def _ft_revisar_tela(self):
        self.env.invalidate_all()
        self.env['ficha.tecnica.revision']._ft_actualizar(self)

    def action_ft_enviar_revision(self):
        """Borrador → En revisión: con la ficha completa, genera las listas archivadas y la revisa."""
        for ficha in self:
            if ficha.estado != 'borrador':
                raise UserError(_('%s ya no está en borrador.') % ficha.articulo)
            faltan = ficha._ft_faltantes()
            if faltan:
                raise UserError(_('A la ficha %s le falta %s para enviarla a revisión.')
                                % (ficha.articulo, ', '.join(faltan)))
        self.write({'estado': 'revision', 'active': True})
        avisos = []
        for ficha in self:
            avisos += ficha._ft_generar_listas()
        self._ft_revisar_tela()
        return self._ft_aviso(_('En revisión'), _('Listas generadas para revisión.'), avisos)

    def action_ft_modificar(self):
        """Activa → En revisión: para cambiar la ficha. Sus listas siguen activas y fabricando."""
        if self.filtered(lambda f: f.estado != 'activa'):
            raise UserError(_('Solo una ficha activa se pasa a modificar.'))
        self.write({'estado': 'revision'})
        self._ft_revisar_tela()
        return True

    def action_ft_activar(self):
        """En revisión → Activa: copia la ficha a las listas, revisa y, sin pendientes, activa las dos listas."""
        for ficha in self:
            if ficha.estado != 'revision':
                raise UserError(_('%s no está en revisión.') % ficha.articulo)
            listas = ficha.ft_lista_kg_id | ficha.ft_lista_m_id
            if listas:
                listas.action_ft_datos_ficha()
            ficha._ft_revisar_tela()
            revision = self.env['ficha.tecnica.revision'].search([('acabado_id', '=', ficha.id)], limit=1)
            if not revision.listo:
                detalle = '\n'.join('• %s: %s' % (dict(p._fields['area'].selection).get(p.area, p.area),
                                                  p.detalle or dict(p._fields['tipo'].selection).get(p.tipo))
                                    for p in revision.pendiente_ids[:10])
                raise UserError(_('%s tiene %s pendiente(s); se activa cuando no quede ninguno:\n%s')
                                % (ficha.articulo, revision.n_pendientes, detalle))
            listas = (ficha.ft_lista_kg_id | ficha.ft_lista_m_id).filtered(lambda b: not b.active)
            ficha.estado = 'activa'
            listas.with_context(ft_activar_lista=True).write({'active': True})
        self._ft_revisar_tela()
        return True

    def action_ft_obsoleta(self):
        """La tela ya no se fabrica: se archivan la ficha y sus dos listas."""
        for ficha in self:
            (ficha.ft_lista_kg_id | ficha.ft_lista_m_id).filtered('active').write({'active': False})
        self.write({'estado': 'obsoleta', 'active': False})
        return True

    def action_ft_volver_borrador(self):
        """En revisión (con las listas aún archivadas) u Obsoleta → Borrador."""
        for ficha in self:
            if ficha.estado == 'revision' and (ficha.ft_lista_kg_activa or ficha.ft_lista_m_activa):
                raise UserError(_('Las listas de %s ya fabrican: actívela de nuevo o márquela obsoleta.')
                                % ficha.articulo)
            if ficha.estado not in ('revision', 'obsoleta'):
                raise UserError(_('%s no puede volver a borrador desde aquí.') % ficha.articulo)
        self.with_context(active_test=False).write({'estado': 'borrador', 'active': True})
        return True

    def action_ft_generar_listas(self):
        """2.33.0: solo con la ficha en revisión (las listas salen de una ficha completa)."""
        fuera = self.filtered(lambda f: f.estado in ('borrador', 'obsoleta'))
        if fuera:
            raise UserError(_('Primero envíe a revisión: %s.') % ', '.join(fuera.mapped('articulo')))
        return super().action_ft_generar_listas()

    def action_ft_corregir(self):
        """«Corregir con la ficha» desde la ficha: iguala las listas con la ficha y la revisa."""
        for ficha in self:
            (ficha.ft_lista_kg_id | ficha.ft_lista_m_id).action_ft_datos_ficha()
        self._ft_revisar_tela()
        return True

    def action_ft_revisar_listas(self):
        self._ft_revisar_tela()
        return True

    def action_ft_ver_pendientes(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': _('Pendientes — %s') % self.articulo,
                'res_model': 'ficha.tecnica.revision.pendiente', 'view_mode': 'list',
                'domain': [('id', 'in', self.ft_pendientes_ids.ids)], 'context': {'create': False}}

    def action_ft_ver_ordenes(self):
        self.ensure_one()
        productos = (self.product_proceso_id | self.product_acabado_id).ids
        return {'type': 'ir.actions.act_window', 'name': _('Órdenes — %s') % self.articulo,
                'res_model': 'mrp.production', 'view_mode': 'list,form',
                'domain': [('product_id', 'in', productos)], 'context': {'create': False}}

    def action_ft_ver_crudo(self):
        self.ensure_one()
        tmpl = self.tejido_id.product_proceso_id.product_tmpl_id
        return {'type': 'ir.actions.act_window', 'name': tmpl.display_name, 'res_model': 'product.template',
                'view_mode': 'form', 'res_id': tmpl.id}

    def action_ft_abrir_lista_kg(self):
        return self._ft_abrir_lista(self.ft_lista_kg_id)

    def action_ft_abrir_lista_m(self):
        return self._ft_abrir_lista(self.ft_lista_m_id)

    def _ft_abrir_lista(self, bom):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': bom.display_name, 'res_model': 'mrp.bom',
                'view_mode': 'form', 'res_id': bom.id, 'context': {'active_test': False}}

    def _ft_aviso(self, titulo, mensaje, avisos):
        if avisos:
            mensaje += ' ' + _('Avisos:') + ' ' + ' '.join(avisos[:15])
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'title': titulo, 'message': mensaje, 'type': 'warning' if avisos else 'success', 'sticky': bool(avisos),
            'next': {'type': 'ir.actions.client', 'tag': 'reload'}}}


class FichaTecnicaRutaEstado(models.Model):
    _inherit = 'ficha.tecnica.ruta'

    def action_ft_generar_listas(self):
        """2.33.0: «Generar listas de la familia» solo para las telas en revisión; las demás se dicen."""
        fichas = self.acabado_ids
        en_revision = fichas.filtered(lambda f: f.estado == 'revision')
        if not en_revision:
            raise UserError(_('Ninguna tela de la familia está en revisión: envíe sus fichas a revisión primero.'))
        return en_revision.action_ft_generar_listas()


class MrpBomEstadoFicha(models.Model):
    _inherit = 'mrp.bom'

    def _ft_ficha_del_flujo(self):
        """La ficha de acabado de una lista del flujo (en kg o en metros), si la tiene."""
        self.ensure_one()
        from .ruta_familia import CODIGO_KG, CODIGO_M  # local: orden de carga
        Acabado = self.env['ficha.tecnica.acabado'].with_context(active_test=False)
        productos = self.product_id or self.product_tmpl_id.with_context(active_test=False).product_variant_ids
        if self.code == CODIGO_KG:
            return Acabado.search([('product_proceso_id', 'in', productos.ids)], limit=1)
        if self.code == CODIGO_M:
            return Acabado.search([('product_acabado_id', 'in', productos.ids)], limit=1)
        return Acabado

    def write(self, vals):
        if vals.get('active') and not self.env.context.get('ft_activar_lista'):
            for bom in self.filtered(lambda b: not b.active):
                ficha = bom._ft_ficha_del_flujo()
                if ficha and ficha.estado != 'activa':
                    raise UserError(_('La lista de %s sale de su ficha técnica: se activa desde la ficha (botón '
                                      '«Activar»), no desde la lista.') % ficha.articulo)
        return super().write(vals)


class ProductTemplateEstadoFicha(models.Model):
    _inherit = 'product.template'

    ft_ficha_estado = fields.Selection(ESTADOS_FICHA, string='Estado de la ficha técnica',
                                       compute='_compute_ft_ficha_estado', search='_search_ft_ficha_estado')

    @api.depends('product_variant_ids')
    def _compute_ft_ficha_estado(self):
        for tmpl in self:
            tmpl.ft_ficha_estado = tmpl.ficha_tecnica_acabado_id.estado or False

    def _search_ft_ficha_estado(self, operator, value):
        if operator not in ('=', 'in'):
            return NotImplemented
        valores = self._ft_valores(value)
        fichas = self.env['ficha.tecnica.acabado'].search([('estado', 'in', list(valores))])
        return [('id', 'in', (fichas.product_acabado_id | fichas.product_proceso_id).product_tmpl_id.ids)]
