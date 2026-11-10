# -*- coding: utf-8 -*-
"""Unidades técnicas (2.10.0, Dirección General 2026-10-19).

Las unidades de las fichas (tolerancias de tejido y acabado, parámetros de
máquina, de artículo y de materia prima, características) eran texto: «2.18 cm»
que en realidad eran metros dejaba sin rendimiento a tintorería. Desde 2.10.0
son un **catálogo** (``ficha.tecnica.unidad``):

- Cada unidad tiene su símbolo (g/m², cN/dtex, PTO…), su descripción y, cuando
  existe, la **unidad de Inventario** equivalente (cm, m, in, kg, g, L, h) para
  convertir. Las unidades técnicas (rpm, Ne, %…) no se crean en Inventario: ahí
  aparecerían al elegir la unidad de un producto.
- Los campos de texto se conservan (los leen el SGI, Consolti y los
  importadores) y quedan sincronizados: elegir la unidad escribe su símbolo; un
  texto que llega del Excel o de otro módulo se liga solo a la unidad cuyo
  símbolo o alias coincide. Un texto que no coincide se queda como está y la
  ficha lo avisa.
- Columnas y mallas cuentan **por pulgada**: ahí «in» es «/in», no pulgadas.
"""
import re

from odoo import api, fields, models

# Alias que cambian según el campo: columnas y mallas se cuentan por pulgada.
ALIAS_POR_CAMPO = {'columnas_tol_unit': {'in': '/in'}, 'mallas_tol_unit': {'in': '/in'}}

# (texto, unidad) de las tolerancias de la ficha de tejido y de la de acabado.
TEJIDO_UNIDADES = tuple('%s_tol_unit' % base for base in (
    'longitud_malla', 'consumo_cm_vta', 'polea_alimentacion', 'tension', 'punto_cilindro', 'punto_plato',
    'altura_plato', 'ancho_bastidor', 'estiraje', 'ancho_rollo', 'peso_promedio_rollo', 'peso_acondicionado',
    'ancho_acondicionado', 'espesor_acondicionado', 'columnas', 'mallas', 'elongacion_carga_largo',
    'elongacion_carga_ancho'))
ACABADO_UNIDADES = tuple('%s_tol_unit' % base for base in (
    'peso_acabado', 'ancho_acabado', 'encogimiento_largo', 'encogimiento_ancho', 'espesor_acabado',
    'elongacion_largo_acabado', 'elongacion_ancho_acabado'))


def _norm(texto):
    """«g/m2», «G/M²» y « g / m 2 » son lo mismo."""
    texto = (texto or '').strip().lower().replace('²', '2').replace('³', '3').replace('µ', 'u')
    return re.sub(r'\s+', '', texto)


class FichaTecnicaUnidad(models.Model):
    _name = 'ficha.tecnica.unidad'
    _description = 'Unidad técnica (fichas técnicas)'
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10,
                              help='Orden en que aparecen las unidades al elegirlas (Fichas técnicas → Configuración '
                                   '→ Unidades).')
    name = fields.Char(string='Símbolo', required=True,
                       help='Como se escribe la unidad (g/m², cN/dtex, PTO…). Se captura en Configuración → '
                            'Unidades; es lo que se copia al texto de la unidad en las fichas.')
    descripcion = fields.Char(string='Descripción',
                              help='Qué mide la unidad, en palabras (ej. «Punto (escala del ajuste en la máquina)»). '
                                   'Se captura en Configuración → Unidades.')
    uom_id = fields.Many2one('uom.uom', string='Unidad de Inventario',
                             help='La unidad de Odoo equivalente, cuando existe (cm, m, in, kg…): con ella se '
                                  'convierte, por ejemplo el ancho a metros para el rendimiento. Las unidades '
                                  'técnicas (rpm, PTO, %…) no llevan.')
    alias = fields.Char(string='También se escribe',
                        help='Otras formas en que llega el texto, separadas por coma (gr, g/m2, pto…). Con ellas se '
                             'ligan solos los textos de las fichas y de los Excel.')
    active = fields.Boolean(default=True,
                            help='Desmarcada, la unidad ya no se ofrece al elegir, pero las fichas que la usan la '
                                 'conservan y sus textos se siguen ligando.')

    _name_uniq = models.Constraint('unique(name)', 'Ya existe una unidad con ese símbolo.')

    @api.model
    def _ft_resolver(self, texto, campo=None):
        """La unidad de un texto (por símbolo o alias, sin importar mayúsculas ni espacios); vacía si no hay una
        sola. ``campo`` aplica los alias propios del campo (columnas y mallas: «in» = «/in»)."""
        clave = _norm(texto)
        if not clave:
            return self.browse()
        clave = _norm(ALIAS_POR_CAMPO.get(campo, {}).get(clave, clave))
        found = self.with_context(active_test=False).search([]).filtered(
            lambda u: clave == _norm(u.name) or clave in {_norm(a) for a in (u.alias or '').split(',') if a.strip()})
        return found if len(found) == 1 else self.browse()

    def _ft_factor_a(self, uom):
        """Cuántas ``uom`` es una de esta unidad (0 si no se puede convertir)."""
        self.ensure_one()
        if not (self.uom_id and uom and self.uom_id._has_common_reference(uom)):
            return 0.0
        return self.uom_id._compute_quantity(1.0, uom, round=False)


class FichaTecnicaUnidadMixin(models.AbstractModel):
    """Sincroniza cada campo de unidad en texto con su campo de unidad del catálogo (``<texto>_id``)."""
    _name = 'ficha.tecnica.unidad.mixin'
    _description = 'Unidades del catálogo junto a su texto'

    # Nombres de los campos de texto; el del catálogo es «<texto>_id».
    _ft_unidad_campos = ()

    def _ft_unidades_vals(self, vals, crear=False):
        """Completa ``vals``: la unidad elegida escribe su símbolo en el texto; un texto sin unidad se liga a la
        del catálogo. Al crear, también los textos por defecto. Con ``ft_unidad_sin_texto`` en el contexto
        (migración) el texto no se toca."""
        Unidad = self.env['ficha.tecnica.unidad']
        vals = dict(vals)
        for texto in self._ft_unidad_campos:
            campo = texto + '_id'
            if campo in vals:
                if not self.env.context.get('ft_unidad_sin_texto'):
                    vals[texto] = Unidad.browse(vals[campo]).name if vals[campo] else False
            elif texto in vals:
                vals[campo] = Unidad._ft_resolver(vals[texto], texto).id or False
            elif crear and self._fields[texto].default:
                vals[campo] = Unidad._ft_resolver(self._fields[texto].default(self), texto).id or False
        return vals

    @api.model_create_multi
    def create(self, vals_list):
        return super().create([self._ft_unidades_vals(vals, crear=True) for vals in vals_list])

    def write(self, vals):
        if self._ft_unidad_campos and vals.keys() & {c for t in self._ft_unidad_campos for c in (t, t + '_id')}:
            vals = self._ft_unidades_vals(vals)
        return super().write(vals)

    def _ft_unidades_sin_ligar(self):
        """Los textos de unidad que no corresponden a ninguna unidad del catálogo («campo: texto»)."""
        self.ensure_one()
        return ['%s: «%s»' % (self._fields[t].string, self[t]) for t in self._ft_unidad_campos
                if self[t] and not self[t + '_id']]

    @api.model
    def _ft_ligar_unidades(self):
        """Liga a su unidad los registros con texto y sin unidad, sin tocar el texto (migración 2.10.0).
        Devuelve {texto sin unidad: veces}."""
        sin_ligar = {}
        records = self.with_context(active_test=False, ft_unidad_sin_texto=True).search([])
        Unidad = self.env['ficha.tecnica.unidad']
        for record in records:
            vals = {}
            for texto in self._ft_unidad_campos:
                if record[texto] and not record[texto + '_id']:
                    unidad = Unidad._ft_resolver(record[texto], texto)
                    if unidad:
                        vals[texto + '_id'] = unidad.id
                    else:
                        sin_ligar[record[texto]] = sin_ligar.get(record[texto], 0) + 1
            if vals:
                record.write(vals)
        return sin_ligar


class FichaTecnicaTejidoUnidades(models.Model):
    _name = 'ficha.tecnica.tejido'
    _inherit = ['ficha.tecnica.tejido', 'ficha.tecnica.unidad.mixin']
    _ft_unidad_campos = TEJIDO_UNIDADES

    longitud_malla_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Longitud de malla', ondelete='restrict',
        help='Unidad de la longitud de malla (polea 1 y 2) y de su tolerancia, del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo.')
    consumo_cm_vta_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Consumo cm/vta', ondelete='restrict',
        help='Unidad del consumo por vuelta (polea 1 y 2) y de su tolerancia, del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo.')
    polea_alimentacion_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Polea de alimentación', ondelete='restrict',
        help='Unidad del ajuste de la polea de alimentación y de su tolerancia, del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo.')
    tension_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Tensión', ondelete='restrict',
        help='Unidad de la tensión y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en Ficha de '
             'tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel se liga '
             'solo.')
    punto_cilindro_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Punto cilindro', ondelete='restrict',
        help='Unidad del punto del cilindro y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en '
             'Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel '
             'se liga solo.')
    punto_plato_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Punto plato', ondelete='restrict',
        help='Unidad del punto del plato y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en '
             'Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel '
             'se liga solo.')
    altura_plato_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Altura plato', ondelete='restrict',
        help='Unidad de la altura del plato y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en '
             'Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel '
             'se liga solo.')
    ancho_bastidor_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Ancho de bastidor', ondelete='restrict',
        help='Unidad del ancho de bastidor y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en '
             'Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel '
             'se liga solo.')
    estiraje_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Estiraje', ondelete='restrict',
        help='Unidad del estiraje y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en Ficha de '
             'tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del Excel se liga '
             'solo.')
    ancho_rollo_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Ancho de rollo', ondelete='restrict',
        help='Unidad del ancho de rollo de crudo y de su tolerancia, del catálogo (Configuración → Unidades). Se '
             'elige en Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega '
             'del Excel se liga solo.')
    peso_promedio_rollo_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Peso promedio de rollo', ondelete='restrict',
        help='Unidad del peso promedio de rollo y de su tolerancia, del catálogo (Configuración → Unidades). Se elige '
             'en Ficha de tejido → Ajustes de tejido y escribe sola el texto de la unidad; el texto que llega del '
             'Excel se liga solo.')
    peso_acondicionado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Peso acondicionado', ondelete='restrict',
        help='Unidad del peso acondicionado y de sus tolerancias (+ y −), del catálogo (Configuración → Unidades). Se '
             'elige en Ficha de tejido → Tela acondicionada y escribe sola el texto de la unidad; el texto que llega '
             'del Excel se liga solo. El rendimiento de tejido toma el peso en g/m².')
    ancho_acondicionado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Ancho acondicionado', ondelete='restrict',
        help='Unidad del ancho de crudo relajado y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de tejido → Tela acondicionada y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo. Con ella el ancho pasa a metros para el rendimiento de tejido.')
    espesor_acondicionado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Espesor acondicionado', ondelete='restrict',
        help='Unidad del espesor acondicionado y de sus tolerancias (+ y −), del catálogo (Configuración → Unidades). '
             'Se elige en Ficha de tejido → Tela acondicionada y escribe sola el texto de la unidad; el texto que '
             'llega del Excel se liga solo.')
    columnas_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Columnas', ondelete='restrict',
        help='Unidad de las columnas y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en Ficha '
             'de tejido → Tela acondicionada y escribe sola el texto de la unidad; el texto que llega del Excel se '
             'liga solo. Columnas y mallas se cuentan por pulgada (/in).')
    mallas_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Mallas', ondelete='restrict',
        help='Unidad de las mallas y de su tolerancia, del catálogo (Configuración → Unidades). Se elige en Ficha de '
             'tejido → Tela acondicionada y escribe sola el texto de la unidad; el texto que llega del Excel se liga '
             'solo. Columnas y mallas se cuentan por pulgada (/in).')
    elongacion_carga_largo_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Elongación carga largo', ondelete='restrict',
        help='Unidad de la elongación bajo carga a lo largo y de sus tolerancias (+ y −), del catálogo (Configuración '
             '→ Unidades). Se elige en Ficha de tejido → Tela acondicionada y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo.')
    elongacion_carga_ancho_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Elongación carga ancho', ondelete='restrict',
        help='Unidad de la elongación bajo carga a lo ancho y de sus tolerancias (+ y −), del catálogo (Configuración '
             '→ Unidades). Se elige en Ficha de tejido → Tela acondicionada y escribe sola el texto de la unidad; el '
             'texto que llega del Excel se liga solo.')
    unidades_aviso = fields.Char(string='Aviso de unidades', compute='_compute_unidades_aviso',
                                 help='Se calcula: las unidades en texto de la ficha que no corresponden a ninguna '
                                      'unidad del catálogo (Configuración → Unidades). Se corrige eligiendo la unidad '
                                      'o agregando el texto como alias.')

    def _compute_unidades_aviso(self):
        for ficha in self:
            sin = ficha._ft_unidades_sin_ligar()
            ficha.unidades_aviso = ('Unidades que no están en el catálogo: %s.' % ', '.join(sin)) if sin else False


class FichaTecnicaAcabadoUnidades(models.Model):
    _name = 'ficha.tecnica.acabado'
    _inherit = ['ficha.tecnica.acabado', 'ficha.tecnica.unidad.mixin']
    _ft_unidad_campos = ACABADO_UNIDADES

    peso_acabado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Peso', ondelete='restrict',
        help='Unidad del peso de especificación y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto '
             'que llega del Excel se liga solo. El rendimiento teórico solo usa el peso si la unidad es de gramaje '
             '(g/m²) o falta.')
    ancho_acabado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Ancho', ondelete='restrict',
        help='Unidad del ancho de venta y de sus tolerancias (+ y −), del catálogo (Configuración → Unidades). Se '
             'elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto que llega del '
             'Excel se liga solo. Con ella el ancho pasa a metros; sin unidad, más de 10 se toma como cm.')
    encogimiento_largo_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Encogimiento largo', ondelete='restrict',
        help='Unidad del encogimiento a lo largo y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto '
             'que llega del Excel se liga solo.')
    encogimiento_ancho_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Encogimiento ancho', ondelete='restrict',
        help='Unidad del encogimiento a lo ancho y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto '
             'que llega del Excel se liga solo.')
    espesor_acabado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Espesor', ondelete='restrict',
        help='Unidad del espesor y de sus tolerancias (+ y −), del catálogo (Configuración → Unidades). Se elige en '
             'Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto que llega del Excel se '
             'liga solo.')
    elongacion_largo_acabado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Elongación largo', ondelete='restrict',
        help='Unidad de la elongación a lo largo y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto '
             'que llega del Excel se liga solo.')
    elongacion_ancho_acabado_tol_unit_id = fields.Many2one(
        'ficha.tecnica.unidad', string='Unidad — Elongación ancho', ondelete='restrict',
        help='Unidad de la elongación a lo ancho y de sus tolerancias (+ y −), del catálogo (Configuración → '
             'Unidades). Se elige en Ficha de acabado → Tela acabada y escribe sola el texto de la unidad; el texto '
             'que llega del Excel se liga solo.')
    unidades_aviso = fields.Char(string='Aviso de unidades', compute='_compute_unidades_aviso',
                                 help='Se calcula: las unidades en texto de la ficha que no corresponden a ninguna '
                                      'unidad del catálogo (Configuración → Unidades). Se corrige eligiendo la unidad '
                                      'o agregando el texto como alias.')

    def _compute_unidades_aviso(self):
        for ficha in self:
            sin = ficha._ft_unidades_sin_ligar()
            ficha.unidades_aviso = ('Unidades que no están en el catálogo: %s.' % ', '.join(sin)) if sin else False


class FichaTecnicaParametroUnidades(models.Model):
    _name = 'ficha.tecnica.maquina.parametro'
    _inherit = ['ficha.tecnica.maquina.parametro', 'ficha.tecnica.unidad.mixin']
    _ft_unidad_campos = ('unit',)

    unit_id = fields.Many2one('ficha.tecnica.unidad', string='Unidad (catálogo)', ondelete='restrict',
                              help='Unidad del parámetro, del catálogo (Configuración → Unidades). Se elige en la '
                                   'pestaña Parámetros del tipo de máquina o de materia prima y escribe sola la unidad '
                                   'en texto, que es la que leen los cálculos (m/min de la velocidad, min u h del '
                                   'tiempo de ciclo).')


class FichaTecnicaCaracteristicaUnidades(models.Model):
    _name = 'ficha.tecnica.caracteristica'
    _inherit = ['ficha.tecnica.caracteristica', 'ficha.tecnica.unidad.mixin']
    _ft_unidad_campos = ('unit',)

    unit_id = fields.Many2one('ficha.tecnica.unidad', string='Unidad (catálogo)', ondelete='restrict',
                              help='Unidad de la característica, del catálogo (Configuración → Unidades). Se elige en '
                                   'Configuración → Características y escribe sola la unidad en texto, que pasa a los '
                                   'renglones de características de las fichas.')
