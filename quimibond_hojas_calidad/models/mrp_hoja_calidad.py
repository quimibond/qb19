# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError, ValidationError

TIPOS = [('rama', 'Registro Rama'), ('inspeccion', 'Revisión Tela Acabada')]


def codigo_articulo(producto):
    """Código del artículo en metros: el producto en kg lleva una «I» al inicio (DAT P-D02-01), el libro es del
    artículo (misma convención que ``mrp.inspeccion.restriccion._codigo_base``)."""
    codigo = (producto.default_code or '').strip()
    return codigo[1:] if codigo.startswith('I') else codigo


class MrpHojaCalidadPlantilla(models.Model):
    """Libro en blanco de un artículo y un proceso: de aquí se copian los libros nuevos (con las especificaciones del
    artículo en su hoja SPECS)."""
    _name = 'mrp.hoja.calidad.plantilla'
    _description = 'Plantilla de libro de calidad'
    _order = 'articulo, tipo'

    producto_id = fields.Many2one(
        'product.product', 'Producto del catálogo', index=True,
        domain=[('categ_id.complete_name', '=like', 'Producto Terminado / Tejido Circular / Industrial%')],
        help='Producto de la categoría Producto Terminado / Tejido Circular / Industrial. Al elegirlo se llena el '
             'artículo con su código (sin la «I» inicial si es el producto en kg).')
    articulo = fields.Char(
        'Artículo', required=True, compute='_compute_articulo', store=True, readonly=False,
        help='Código del artículo en metros, p. ej. WJ038Q22JNT160. Se toma del producto; también se puede teclear.')
    tipo = fields.Selection(TIPOS, 'Proceso', required=True)
    plantilla_id = fields.Many2one(
        'documents.document', 'Libro en blanco', required=True, domain=[('handler', '=', 'spreadsheet')],
        help='Documento de Documentos abierto con Odoo Spreadsheet y sin registros capturados.')
    carpeta_id = fields.Many2one(
        'documents.document', 'Carpeta de los libros', domain=[('type', '=', 'folder')],
        help='Carpeta de Documentos donde se guardan los libros de este artículo. Sin carpeta, quedan sin carpeta.')
    libro_ids = fields.One2many('mrp.hoja.calidad', 'plantilla_id', 'Libros')
    libros_count = fields.Integer('No. de libros', compute='_compute_libros_count')
    name = fields.Char(compute='_compute_name')

    @api.depends('producto_id')
    def _compute_articulo(self):
        for rec in self.filtered('producto_id'):
            rec.articulo = codigo_articulo(rec.producto_id).upper()

    @api.constrains('articulo', 'tipo')
    def _check_unico(self):
        for rec in self:
            if self.search_count([('articulo', '=', rec.articulo), ('tipo', '=', rec.tipo)]) > 1:
                raise ValidationError(_('Ya hay una plantilla de %(t)s para el artículo %(a)s.',
                                        t=dict(TIPOS)[rec.tipo], a=rec.articulo))

    @api.depends('articulo', 'tipo')
    def _compute_name(self):
        for rec in self:
            rec.name = '%s - %s' % (dict(TIPOS).get(rec.tipo, ''), rec.articulo or '')

    def _compute_libros_count(self):
        for rec in self:
            rec.libros_count = self.env['mrp.hoja.calidad'].with_context(active_test=False).search_count(
                [('plantilla_id', '=', rec.id)])

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('articulo') and vals.get('producto_id'):
                vals['articulo'] = codigo_articulo(self.env['product.product'].browse(vals['producto_id']))
            if vals.get('articulo'):
                vals['articulo'] = vals['articulo'].strip().upper()
        return super().create(vals_list)

    def write(self, vals):
        if vals.get('articulo'):
            vals['articulo'] = vals['articulo'].strip().upper()
        return super().write(vals)


class MrpHojaCalidad(models.Model):
    """Libro de un artículo y un proceso. Solo uno vigente (activo) a la vez; al llenarse se cierra (se archiva) y se
    abre otro copiando la plantilla."""
    _name = 'mrp.hoja.calidad'
    _description = 'Libro de calidad'
    _order = 'articulo, tipo, numero desc'

    plantilla_id = fields.Many2one('mrp.hoja.calidad.plantilla', 'Plantilla', required=True, ondelete='restrict')
    articulo = fields.Char(related='plantilla_id.articulo', store=True)
    tipo = fields.Selection(related='plantilla_id.tipo', store=True)
    documento_id = fields.Many2one('documents.document', 'Libro', required=True, ondelete='restrict', readonly=True)
    numero = fields.Integer('No. de libro', readonly=True)
    fecha_apertura = fields.Datetime('Abierto el', readonly=True, default=fields.Datetime.now)
    fecha_cierre = fields.Datetime('Cerrado el', readonly=True)
    active = fields.Boolean('Vigente', default=True)
    name = fields.Char(compute='_compute_name')

    @api.depends('plantilla_id.name', 'numero')
    def _compute_name(self):
        for rec in self:
            rec.name = '%s - libro %s' % (rec.plantilla_id.name or '', rec.numero)

    @api.constrains('articulo', 'tipo', 'active')
    def _check_un_vigente(self):
        for rec in self.filtered('active'):
            if self.search_count([('articulo', '=', rec.articulo), ('tipo', '=', rec.tipo), ('active', '=', True)]) > 1:
                raise ValidationError(_('El artículo %(a)s ya tiene un libro vigente de %(t)s.',
                                        a=rec.articulo, t=dict(TIPOS)[rec.tipo]))

    # ------------------------------------------------------------------ apertura
    @api.model
    def _libro_vigente(self, producto, tipo):
        """Libro vigente del artículo del producto; si no hay, lo crea copiando la plantilla. Se crea con sudo: el operador
        solo necesita permiso de lectura de esta configuración."""
        articulo = codigo_articulo(producto)
        if not articulo:
            raise UserError(_('El producto %s no tiene código: no se puede ubicar su libro.', producto.display_name))
        self = self.sudo()
        # Dos operadores pueden pulsar el botón a la vez con el libro todavía sin crear.
        self.env.cr.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ['hoja_calidad:%s:%s' % (articulo, tipo)])
        libro = self.search([('articulo', '=', articulo), ('tipo', '=', tipo)], limit=1)
        if libro:
            return libro
        plantilla = self.env['mrp.hoja.calidad.plantilla'].search(
            [('articulo', '=', articulo), ('tipo', '=', tipo)], limit=1)
        if not plantilla:
            raise UserError(_(
                'No hay plantilla del libro de %(t)s para el artículo %(a)s. Pide al responsable de Manufactura que la '
                'dé de alta en Configuración → Plantillas de libros de calidad.', t=dict(TIPOS)[tipo], a=articulo))
        return self._crear_desde_plantilla(plantilla)

    @api.model
    def _crear_desde_plantilla(self, plantilla):
        self = self.sudo()
        previos = self.with_context(active_test=False).search([('plantilla_id', '=', plantilla.id)])
        numero = max(previos.mapped('numero') or [0]) + 1
        nombre = '%s %s - libro %s' % (dict(TIPOS)[plantilla.tipo], plantilla.articulo, numero)
        valores = {
            'name': nombre,
            # Los libros son de planta: los usuarios internos los editan (por omisión Documentos los deja privados).
            'access_internal': 'edit',
            'access_via_link': 'none',
        }
        if plantilla.carpeta_id:
            valores['folder_id'] = plantilla.carpeta_id.id
        documento = plantilla.plantilla_id.sudo().copy(valores)
        return self.create({'plantilla_id': plantilla.id, 'documento_id': documento.id, 'numero': numero})

    @api.model
    def _abrir(self, producto, tipo):
        return self._libro_vigente(producto, tipo).documento_id.action_open_spreadsheet()

    def action_abrir(self):
        self.ensure_one()
        return self.documento_id.sudo().action_open_spreadsheet()

    def action_cerrar_y_abrir_nuevo(self):
        """El libro llegó a su límite de registros: se cierra y se abre otro con la plantilla en blanco."""
        self.ensure_one()
        if not self.env.user.has_group('mrp.group_mrp_manager'):
            raise UserError(_('Solo el responsable de Manufactura cierra un libro.'))
        if not self.active:
            raise UserError(_('El libro %s ya está cerrado.', self.name))
        self = self.sudo()
        plantilla = self.plantilla_id
        self.write({'active': False, 'fecha_cierre': fields.Datetime.now()})
        nuevo = self._crear_desde_plantilla(plantilla)
        return nuevo.documento_id.action_open_spreadsheet()
