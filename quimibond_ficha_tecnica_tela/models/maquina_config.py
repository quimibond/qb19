# -*- coding: utf-8 -*-
"""Ficha de la máquina: tipos, parámetros y configuraciones por fecha.

2.3.0 (Dirección General 2026-10-13): los datos de una circular (diámetro,
galga, agujas, alimentadores, velocidad máxima) son de la máquina, no del
artículo. Una circular puede cambiar de cilindro y con eso cambian galga y
diámetro, así que se guardan como **configuraciones con fecha** («vigente
desde»): la vigente es la de fecha más reciente que ya empezó. La ficha de
tejido guarda la configuración con la que se elaboró y toma de ella sus datos
de máquina; un cambio de cilindro posterior no altera las fichas anteriores.

2.4.0 (Dirección General 2026-10-13): la ficha es para **cualquier tipo de
máquina** (tejido, tintorería, acabado, inspección, abridora, devanadora…).
Cada centro de trabajo tiene un **tipo de máquina**
(``ficha.tecnica.maquina.tipo``) y cada tipo su lista de **parámetros**
(``ficha.tecnica.maquina.parametro``: clave, nombre, unidad, numérico o
texto). Una configuración guarda un **valor por parámetro de su tipo**
(``ficha.tecnica.maquina.valor``). Los parámetros se dan de alta en
Configuración → Tipos de máquina; el módulo solo trae los de tejido circular,
que ya estaban definidos (los demás no se inventan). Galga, diámetro, agujas,
alimentadores y velocidad máxima siguen como columnas de la configuración,
calculadas desde los valores con esas claves, porque la ficha de tejido y la
búsqueda de máquinas por galga los usan.

Marca y modelo son de la máquina (no cambian con el cilindro) y viven en el
centro de trabajo.

2.8.0 (Dirección General 2026-10-14): cada parámetro tiene un **ámbito**. Los
de la máquina (``maquina``) van en la configuración por fecha, como hasta
ahora; los del artículo (``articulo``, p. ej. la velocidad de cada artículo en
la rama) van en el **proceso del artículo en la máquina**
(``ficha.tecnica.proceso``, ``models/proceso.py``). Los dos guardan sus valores
con el mismo mixin (``ficha.tecnica.valor.mixin``).

2.9.0 (Dirección General 2026-10-19): el mismo motor sirve para las
**materias primas** (``models/materia_prima.py``). Un tipo tiene **clase**
(máquina o materia prima); los parámetros de un tipo de materia prima son de
ámbito «De la materia prima» y se capturan en la ficha del material. Un
parámetro de texto puede llevar una **lista de opciones** (S, Z, sin
torsión): el valor tiene que ser una de ellas.
"""
import operator as py_operator

from odoo import Command, api, fields, models
from odoo.exceptions import UserError, ValidationError

OPERATORS = {'=': py_operator.eq, '!=': py_operator.ne, '<': py_operator.lt, '<=': py_operator.le,
             '>': py_operator.gt, '>=': py_operator.ge}
PARAM_KINDS = [('num', 'Numérico'), ('text', 'Texto')]
# 2.8.0: de quién es el dato.
PARAM_AMBITOS = [('maquina', 'De la máquina'), ('articulo', 'Del artículo en la máquina'),
                 ('material', 'De la materia prima')]
# 2.9.0: qué describe el tipo.
TIPO_CLASES = [('maquina', 'Máquina'), ('material', 'Materia prima')]
# Modelo de valores de cada ámbito (para no cambiar de ámbito un parámetro con valores ni borrarlo).
VALOR_MODELS = {'maquina': 'ficha.tecnica.maquina.valor', 'articulo': 'ficha.tecnica.proceso.valor',
                'material': 'ficha.tecnica.material.valor'}
# Columnas de la configuración que salen del valor del parámetro con la misma
# clave (las usan la ficha de tejido y la búsqueda de máquinas por galga).
CONFIG_VALUE_FIELDS = ('diametro', 'galga', 'no_agujas', 'no_alimentadores', 'velocidad_max')
INT_VALUE_FIELDS = ('no_agujas', 'no_alimentadores')
# 2.30.0 (auditoría del 10-nov): claves que leen los cálculos de este módulo y de otros (Programación y Dosificación
# de Tintorería, costeo, SGI). Se buscan por clave: renombrar, cambiar de tipo, archivar o borrar una deja el cálculo
# en cero sin error, así que no se permite. (tipo de máquina, clave).
CODIGOS_DEL_CALCULO = {
    ('tejido_circular', 'diametro'), ('tejido_circular', 'galga'), ('tejido_circular', 'no_agujas'),
    ('tejido_circular', 'no_alimentadores'), ('tejido_circular', 'velocidad_max'),
    ('tejido_circular', 'salida_tela'),
    ('tintoreria', 'capacidad'), ('tintoreria', 'capacidad_grupo_a'), ('tintoreria', 'capacidad_grupo_b'),
    ('tintoreria', 'capacidad_grupo_c'), ('tintoreria', 'relacion_bano'), ('tintoreria', 'minimo_bano'),
    ('tintoreria', 'kg_carga'), ('tintoreria', 'relacion_bano_articulo'), ('tintoreria', 'tiempo_ciclo'),
    ('acabado', 'velocidad'), ('acabado', 'merma'), ('acabado', 'ancho_rama'),
    ('inspeccion', 'velocidad_inspeccion'),
    ('abridora', 'velocidad'), ('abridora', 'velocidad_tubular'), ('abridora', 'velocidad_abierta'),
    ('devanadora', 'velocidad'),
}


def _fmt(value):
    return ('%.3f' % value).rstrip('0').rstrip('.')


class FichaTecnicaMaquinaTipo(models.Model):
    """Tipo de máquina (tejido circular, tintorería, acabado…) con sus parámetros."""
    _name = 'ficha.tecnica.maquina.tipo'
    _description = 'Tipo de máquina (ficha de la máquina)'
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10,
                              help='Orden del tipo en las listas (Configuración → Tipos de máquina o Tipos de '
                                   'materia prima).')
    code = fields.Char(string='Clave', required=True, index=True,
                       help='Clave corta y estable del tipo (p. ej. «tejido_circular», «tintoreria», «acabado»). '
                            'Los cálculos buscan sus parámetros con esta clave: cambiarla en un tipo en uso deja '
                            'esos cálculos sin datos.')
    name = fields.Char(string='Tipo de máquina', required=True,
                       help='Nombre del tipo de máquina o de materia prima, como se ve en las máquinas y las fichas. '
                            'Se captura en Configuración → Tipos de máquina o Tipos de materia prima.')
    clase = fields.Selection(TIPO_CLASES, string='Clase', required=True, default='maquina',
                             help='«Máquina»: lo llevan los centros de trabajo. «Materia prima»: lo llevan las fichas '
                                  'de materia prima (hilo de filamento, hilo hilado, químico…).')
    categ_ids = fields.Many2many('product.category', string='Categorías de producto',
                                 help='Solo materias primas: los productos de estas categorías (y sus hijas) proponen '
                                      'este tipo al crear su ficha.')
    parametro_ids = fields.One2many('ficha.tecnica.maquina.parametro', 'tipo_id', string='Parámetros',
                                    context={'active_test': False},
                                    help='Los datos que se capturan para este tipo (pestaña Parámetros): clave, '
                                         'nombre, unidad, número o texto y ámbito (de la máquina, del artículo en '
                                         'la máquina o de la materia prima).')
    workcenter_ids = fields.One2many('mrp.workcenter', 'ft_tipo_id', string='Máquinas',
                                     help='Centros de trabajo con este tipo (pestaña Máquinas). El tipo se asigna en '
                                          'el centro de trabajo, pestaña «Ficha de la máquina».')
    workcenter_count = fields.Integer(string='No. de máquinas', compute='_compute_workcenter_count',
                                      help='Se calcula: cuántos centros de trabajo tienen este tipo.')
    active = fields.Boolean(default=True,
                            help='Desmarcado, el tipo queda archivado y ya no se ofrece al elegir el tipo de una '
                                 'máquina o de una materia prima.')

    _code_uniq = models.Constraint('unique(code)', 'La clave del tipo de máquina debe ser única.')

    def write(self, vals):
        # 2.30.0: la clave de un tipo con parámetros de los cálculos (tintoreria, acabado, inspeccion…) no se renombra
        # ni se archiva: los cálculos buscan (clave del tipo, clave del parámetro).
        if ('code' in vals or vals.get('active') is False):
            tipos_del_calculo = {tipo for tipo, _code in CODIGOS_DEL_CALCULO}
            for tipo in self.filtered(lambda t: t.code in tipos_del_calculo):
                if ('code' in vals and vals['code'] != tipo.code) or vals.get('active') is False:
                    raise UserError('La clave «%s» del tipo «%s» la leen los cálculos de tiempos, capacidad o costeo: '
                                    'no se renombra ni se archiva. Puede cambiar el nombre.' % (tipo.code, tipo.name))
        return super().write(vals)

    def unlink(self):
        tipos_del_calculo = {tipo for tipo, _code in CODIGOS_DEL_CALCULO}
        if self.filtered(lambda t: t.code in tipos_del_calculo):
            raise UserError('Ese tipo de máquina lo leen los cálculos por su clave: no se borra.')
        return super().unlink()

    @api.depends('workcenter_ids')
    def _compute_workcenter_count(self):
        for tipo in self:
            tipo.workcenter_count = len(tipo.workcenter_ids)

    # 2.23.0: los parámetros que usan los cálculos del módulo (tiempos, capacidad de jets). En producción ya existen
    # (los dio de alta la planta, sin xmlid); una instalación nueva no los tenía. Solo se crean los que falten.
    PARAMETROS_DEL_CALCULO = [
        # (tipo, clave, nombre, unidad, clase, ámbito)
        ('tintoreria', 'capacidad', 'Capacidad', 'kg', 'num', 'maquina'),
        ('tintoreria', 'capacidad_grupo_a', 'Capacidad grupo A (rendimiento de 3 a menos de 7 m/kg)', 'kg', 'num', 'maquina'),
        ('tintoreria', 'capacidad_grupo_b', 'Capacidad grupo B (rendimiento de 7 a menos de 11 m/kg)', 'kg', 'num', 'maquina'),
        ('tintoreria', 'capacidad_grupo_c', 'Capacidad grupo C (rendimiento de 11 a 16 m/kg)', 'kg', 'num', 'maquina'),
        ('tintoreria', 'relacion_bano', 'Relación de baño', False, 'text', 'maquina'),
        ('acabado', 'velocidad', 'Velocidad', 'm/min', 'num', 'articulo'),
        ('acabado', 'merma', 'Merma', '%', 'num', 'articulo'),
        ('acabado', 'ancho_rama', 'Ancho bajando de rama', 'm', 'num', 'articulo'),
        ('inspeccion', 'velocidad_inspeccion', 'Velocidad de inspección (rollos con 1 empalme)', 'm/h', 'num', 'articulo'),
        ('inspeccion', 'velocidad_inspeccion_4e', 'Velocidad de inspección (rollos con 4 empalmes)', 'm/h', 'num',
         'articulo'),
        ('abridora', 'velocidad', 'Velocidad', 'm/min', 'num', 'articulo'),
        ('abridora', 'velocidad_tubular', 'Velocidad con tela tubular', 'm/min', 'num', 'articulo'),
        ('abridora', 'velocidad_abierta', 'Velocidad con tela abierta', 'm/min', 'num', 'articulo'),
        ('devanadora', 'velocidad', 'Velocidad', 'm/min', 'num', 'articulo'),
    ]

    @api.model
    def _ft_asegurar_parametros(self):
        """Crea los parámetros del cálculo que falten en su tipo de máquina (por clave); no toca los que existen."""
        Param = self.env['ficha.tecnica.maquina.parametro']
        creados = Param
        for tipo_code, code, name, unit, kind, ambito in self.PARAMETROS_DEL_CALCULO:
            tipo = self.search([('code', '=', tipo_code)], limit=1)
            if not tipo or tipo.with_context(active_test=False).parametro_ids.filtered(lambda p: p.code == code):
                continue
            vals = {'tipo_id': tipo.id, 'code': code, 'name': name, 'kind': kind, 'ambito': ambito}
            if unit:
                vals['unit'] = unit
            creados |= Param.create(vals)
        return creados

    def _ft_parametros(self, ambito):
        """2.8.0: los parámetros activos del tipo con ese ámbito («maquina», «articulo» o «material»), en su
        orden."""
        self.ensure_one()
        return self.parametro_ids.filtered(lambda p: p.active and p.ambito == ambito)


class FichaTecnicaMaquinaParametro(models.Model):
    """Parámetro que se captura en la ficha de las máquinas de un tipo."""
    _name = 'ficha.tecnica.maquina.parametro'
    _description = 'Parámetro de la ficha de la máquina'
    _order = 'tipo_id, sequence, id'

    tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', required=True,
                              index=True, ondelete='cascade',
                              help='Tipo de máquina o de materia prima al que pertenece el parámetro. Se da de alta '
                                   'en la pestaña Parámetros del tipo (Configuración → Tipos de máquina).')
    sequence = fields.Integer(default=10,
                              help='Orden del parámetro al capturarlo; se cambia arrastrando el renglón en la '
                                   'pestaña Parámetros del tipo.')
    code = fields.Char(string='Clave', required=True,
                       help='Clave corta y estable dentro del tipo; los cálculos leen el dato por esta clave '
                            '(velocidad, merma, kg_carga, tiempo_ciclo, galga…). Desde 2.30.0 las claves que usan los '
                            'cálculos no se renombran, no se archivan y no se borran.')
    name = fields.Char(string='Parámetro', required=True,
                       help='Nombre del dato como se ve al capturarlo (p. ej. «Velocidad», «Kg por carga»). Se '
                            'puede cambiar aunque lo use un cálculo.')
    unit = fields.Char(string='Unidad',
                       help='Símbolo de la unidad en texto (in, rpm, kg, m/min, °C…); se llena solo al elegir la '
                            'unidad del catálogo. Hay cálculos que lo leen: m/hora solo sale con la velocidad en '
                            '«m/min» y el tiempo de ciclo se toma en minutos si dice «min» (si no, en horas).')
    kind = fields.Selection(PARAM_KINDS, string='Tipo de dato', required=True, default='num',
                            help='Numérico: se captura en «Valor». Texto: en «Valor (texto)», libre o de la lista de '
                                 'opciones. No se cambia en un parámetro que usan los cálculos.')
    ambito = fields.Selection(PARAM_AMBITOS, string='Ámbito', required=True, default='maquina',
                              help='«De la máquina»: se captura en Máquinas → Configuraciones por fecha (galga, '
                                   'capacidad del jet, relación de baño…). «Del artículo en la máquina»: en Máquinas → '
                                   'Procesos por artículo (velocidad, merma, kg por carga, tiempo de ciclo…). «De la '
                                   'materia prima»: en la ficha de materia prima. No cambia si ya tiene valores.')
    opciones = fields.Char(string='Opciones',
                           help='Solo texto: valores permitidos separados por coma (p. ej. «S, Z, Sin torsión»). '
                                'Vacío: texto libre.')
    notas = fields.Char(string='Cómo se mide', help='De dónde sale el dato o cómo se mide. Solo informativo.')
    active = fields.Boolean(default=True,
                            help='Archivado, el parámetro ya no se agrega a configuraciones, procesos ni fichas de '
                                 'materia prima; los valores capturados se quedan. Los que usan los cálculos no se '
                                 'archivan.')

    _tipo_code_uniq = models.Constraint('unique(tipo_id, code)', 'Ese tipo de máquina ya tiene un parámetro '
                                                                   'con esa clave.')

    @api.constrains('ambito', 'tipo_id')
    def _check_ambito(self):
        for param in self:
            if (param.tipo_id.clase == 'material') != (param.ambito == 'material'):
                raise ValidationError('El parámetro «%s»: un tipo de materia prima lleva parámetros «De la materia '
                                      'prima» y un tipo de máquina, de la máquina o del artículo.' % param.name)
            for ambito, model in VALOR_MODELS.items():
                if ambito != param.ambito and model in self.env and \
                        self.env[model].search_count([('parametro_id', '=', param.id)], limit=1):
                    raise ValidationError('El parámetro «%s» ya tiene valores capturados con otro ámbito: no se '
                                          'cambia; dé de alta otro parámetro.' % param.name)

    def _ft_opciones(self):
        """2.9.0: las opciones permitidas (lista vacía = texto libre)."""
        self.ensure_one()
        return [o.strip() for o in (self.opciones or '').split(',') if o.strip()]

    def _ft_es_del_calculo(self):
        """2.30.0: True si un cálculo lee este parámetro por su clave (``CODIGOS_DEL_CALCULO``)."""
        self.ensure_one()
        return (self.tipo_id.code, self.code) in CODIGOS_DEL_CALCULO

    def write(self, vals):
        # 2.30.0: las claves de los cálculos no se renombran, no cambian de tipo ni de clase de dato, no se archivan.
        if {'code', 'tipo_id', 'kind'} & set(vals) or vals.get('active') is False:
            for param in self.filtered(lambda p: p._ft_es_del_calculo()):
                cambia = (('code' in vals and vals['code'] != param.code)
                          or ('tipo_id' in vals and vals['tipo_id'] != param.tipo_id.id)
                          or ('kind' in vals and vals['kind'] != param.kind)
                          or vals.get('active') is False)
                if cambia:
                    raise UserError('«%s» (%s) lo leen los cálculos de tiempos, capacidad o costeo por su clave '
                                    '«%s»: no se renombra, no cambia de tipo y no se archiva. Puede cambiar el '
                                    'nombre, la unidad y las notas.' % (param.name, param.tipo_id.name, param.code))
        return super().write(vals)

    def unlink(self):
        del_calculo = self.filtered(lambda p: p._ft_es_del_calculo())
        if del_calculo:
            raise UserError('«%s» lo leen los cálculos por su clave: no se borra.' % del_calculo[0].name)
        for model in VALOR_MODELS.values():
            lines = self.env[model].search([('parametro_id', 'in', self.ids)])
            if lines.filtered(lambda v: v.valor or v.valor_texto):
                raise UserError('El parámetro ya tiene valores capturados: archívelo en lugar de borrarlo.')
            lines.unlink()
        return super().unlink()


class FichaTecnicaValorMixin(models.AbstractModel):
    """2.8.0: el valor de un parámetro (número o texto, con su unidad), igual en la configuración de la máquina
    y en el proceso del artículo en la máquina."""
    _name = 'ficha.tecnica.valor.mixin'
    _description = 'Valor de un parámetro de la ficha de la máquina'

    parametro_id = fields.Many2one('ficha.tecnica.maquina.parametro', string='Parámetro', required=True,
                                   ondelete='restrict',
                                   help='El dato de este renglón. Los renglones salen solos de los parámetros del '
                                        'tipo (Configuración → Tipos de máquina o Tipos de materia prima).')
    sequence = fields.Integer(related='parametro_id.sequence', store=True,
                              help='Se toma del orden del parámetro en su tipo.')
    code = fields.Char(related='parametro_id.code',
                       help='Se toma del parámetro: la clave con la que lo leen los cálculos.')
    kind = fields.Selection(related='parametro_id.kind',
                            help='Se toma del parámetro: numérico (se captura en «Valor») o texto (en «Valor '
                                 '(texto)»).')
    unit = fields.Char(related='parametro_id.unit',
                       help='Se toma del parámetro: su unidad. Se cambia en el tipo, en Configuración.')
    valor = fields.Float(string='Valor', digits=(16, 3),
                         help='El dato numérico capturado, en la unidad del parámetro. Un cero se toma como dato que '
                              'no se ha dado (el renglón se ve atenuado).')
    valor_texto = fields.Char(string='Valor (texto)',
                              help='El dato de texto capturado (solo parámetros de texto). Si el parámetro tiene '
                                   'opciones, tiene que ser una de ellas.')
    opciones = fields.Char(related='parametro_id.opciones',
                           help='Se toma del parámetro: los valores de texto permitidos.')

    def _ft_normalizar_texto(self, vals):
        """2.9.0: con lista de opciones, el texto se guarda como la opción (sin importar mayúsculas ni espacios);
        uno que no está en la lista es un error."""
        texto = (vals.get('valor_texto') or '').strip()
        param = self.env['ficha.tecnica.maquina.parametro'].browse(vals.get('parametro_id')) \
            if vals.get('parametro_id') else self.parametro_id
        opciones = param._ft_opciones() if param else []
        if not texto or not opciones:
            return vals
        for opcion in opciones:
            if opcion.lower() == texto.lower():
                return dict(vals, valor_texto=opcion)
        raise ValidationError('«%s» no es una opción de «%s»: %s.' % (texto, param.name, ', '.join(opciones)))

    @api.model_create_multi
    def create(self, vals_list):
        return super().create([self._ft_normalizar_texto(vals) if vals.get('valor_texto') else vals
                               for vals in vals_list])

    def write(self, vals):
        if not vals.get('valor_texto'):
            return super().write(vals)
        for line in self:
            super(FichaTecnicaValorMixin, line).write(line._ft_normalizar_texto(vals))
        return True

    def _display_value(self):
        self.ensure_one()
        if self.kind == 'text':
            return self.valor_texto or ''
        if not self.valor:
            return ''
        return '%s %s' % (_fmt(self.valor), self.unit) if self.unit else _fmt(self.valor)


class FichaTecnicaMaquinaValor(models.Model):
    """Valor de un parámetro en una configuración de máquina."""
    _name = 'ficha.tecnica.maquina.valor'
    _inherit = ['ficha.tecnica.valor.mixin']
    _description = 'Valor de parámetro en la configuración de la máquina'
    _order = 'config_id, sequence, id'

    config_id = fields.Many2one('ficha.tecnica.maquina.config', string='Configuración', required=True,
                                index=True, ondelete='cascade',
                                help='Configuración por fecha de la máquina a la que pertenece el valor (Máquinas → '
                                     'Configuraciones por fecha).')

    _config_parametro_uniq = models.Constraint('unique(config_id, parametro_id)',
                                               'El parámetro ya está en esa configuración.')

    @api.constrains('parametro_id', 'config_id')
    def _check_tipo(self):
        for line in self:
            if line.parametro_id.tipo_id != line.config_id.tipo_id:
                raise ValidationError('El parámetro «%s» es de %s y la configuración es de %s.' % (
                    line.parametro_id.name, line.parametro_id.tipo_id.name, line.config_id.tipo_id.name or '(sin tipo)'))
            if line.parametro_id.ambito != 'maquina':
                raise ValidationError('El parámetro «%s» es del artículo en la máquina: se captura en el proceso '
                                      'del artículo, no en la configuración de la máquina.' % line.parametro_id.name)


class FichaTecnicaMaquinaConfig(models.Model):
    _name = 'ficha.tecnica.maquina.config'
    _description = 'Configuración de máquina por fecha'
    _order = 'workcenter_id, date_from desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', required=True, index=True,
                                    ondelete='cascade',
                                    help='Centro de trabajo (máquina) de esta configuración. Se captura en Máquinas → '
                                         'Configuraciones por fecha o en el centro de trabajo, pestaña «Ficha de la '
                                         'máquina».')
    tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', required=True, index=True,
                              compute='_compute_tipo_id', store=True, readonly=False, precompute=True,
                              domain="[('clase', '=', 'maquina')]",
                              help='Sale del tipo de la máquina; define qué parámetros se capturan. Si la máquina no '
                                   'tenía tipo, toma el que se elija aquí.')
    date_from = fields.Date(string='Vigente desde', required=True, default=fields.Date.context_today,
                            help='Desde cuándo la máquina corre con esta configuración (p. ej. el cambio de '
                                 'cilindro). La vigente es la de fecha más reciente que ya empezó.')
    date_to = fields.Date(string='Vigente hasta', compute='_compute_date_to',
                          help='Se calcula: un día antes de la siguiente configuración de la máquina; vacío si es la '
                               'última.')
    valor_ids = fields.One2many('ficha.tecnica.maquina.valor', 'config_id', string='Parámetros',
                                help='Un renglón por cada parámetro «De la máquina» de su tipo (galga, capacidad del '
                                     'jet, relación de baño…); se captura aquí. Un vacío es un dato que no se ha '
                                     'dado, no un cero.')
    resumen = fields.Char(string='Resumen de parámetros', compute='_compute_resumen',
                          help='Se calcula: los parámetros capturados con su unidad, en una línea.')
    diametro = fields.Float(string='Diámetro (in)', digits=(16, 2), compute='_compute_value_fields',
                            inverse='_inverse_value_fields', store=True,
                            help='Diámetro del cilindro de la circular, en pulgadas. Es el mismo dato que el '
                                 'parámetro «diametro» de la configuración. Se copia a la ficha de tejido.')
    galga = fields.Float(string='Galga', digits=(16, 0), compute='_compute_value_fields',
                         inverse='_inverse_value_fields', store=True,
                         help='Galga del cilindro; es el mismo dato que el parámetro «galga» de la configuración. Se '
                              'copia a la ficha de tejido y con ella se ofrecen las circulares de la galga requerida.')
    no_agujas = fields.Integer(string='No. de agujas', compute='_compute_value_fields',
                               inverse='_inverse_value_fields', store=True,
                               help='Número de agujas del cilindro; es el mismo dato que el parámetro «no_agujas» de '
                                    'la configuración. Se copia a la ficha de tejido.')
    no_alimentadores = fields.Integer(string='No. de alimentadores', compute='_compute_value_fields',
                                      inverse='_inverse_value_fields', store=True,
                                      help='Número de alimentadores de la circular; es el mismo dato que el parámetro '
                                           '«no_alimentadores» de la configuración. Se copia a la ficha de tejido.')
    velocidad_max = fields.Float(string='Velocidad máxima (rpm)', digits=(16, 1), compute='_compute_value_fields',
                                 inverse='_inverse_value_fields', store=True,
                                 help='Velocidad máxima de la circular con esta configuración, en rpm; es el mismo '
                                      'dato que el parámetro «velocidad_max». La ficha de tejido avisa si su '
                                      'velocidad la supera.')
    motivo = fields.Char(string='Motivo', help='Por qué cambió la configuración (cambio de cilindro, alta…).')
    notas = fields.Text(string='Notas',
                        help='Notas de la configuración: de dónde salen los datos, observaciones. Solo informativo.')
    is_current = fields.Boolean(string='Vigente hoy', compute='_compute_is_current',
                                help='Se calcula: es la configuración que rige hoy en la máquina (la de fecha de '
                                     'inicio más reciente que ya empezó).')
    ficha_tejido_ids = fields.One2many('ficha.tecnica.tejido', 'maquina_config_id', string='Fichas de tejido',
                                       help='Fichas de tejido elaboradas con esta configuración (la ficha la toma por '
                                            'su máquina y su fecha de elaboración). Con fichas, la configuración no se '
                                            'borra.')
    ficha_tejido_count = fields.Integer(string='No. de fichas de tejido', compute='_compute_ficha_tejido_count',
                                        help='Se calcula: cuántas fichas de tejido se elaboraron con esta '
                                             'configuración.')

    _workcenter_date_uniq = models.Constraint(
        'unique(workcenter_id, date_from)',
        'La máquina ya tiene una configuración con esa fecha de inicio.',
    )

    @api.depends('workcenter_id')
    def _compute_tipo_id(self):
        for config in self:
            if config.workcenter_id.ft_tipo_id or not config.tipo_id:
                config.tipo_id = config.workcenter_id.ft_tipo_id

    @api.depends('workcenter_id.name', 'galga', 'diametro', 'date_from')
    def _compute_display_name(self):
        for config in self:
            parts = [config.workcenter_id.name or '']
            if config.galga:
                parts.append('E%g' % config.galga)
            if config.diametro:
                parts.append('%g"' % config.diametro)
            if config.date_from:
                parts.append('desde %s' % config.date_from.strftime('%d/%m/%Y'))
            config.display_name = ' · '.join(p for p in parts if p)

    @api.depends('valor_ids.valor', 'valor_ids.valor_texto', 'valor_ids.parametro_id.name',
                 'valor_ids.parametro_id.unit')
    def _compute_resumen(self):
        for config in self:
            parts = []
            for line in config.valor_ids:
                shown = line._display_value()
                if shown:
                    parts.append('%s: %s' % (line.parametro_id.name, shown))
            config.resumen = ' · '.join(parts)

    @api.depends('valor_ids.valor', 'valor_ids.parametro_id.code')
    def _compute_value_fields(self):
        for config in self:
            values = {line.parametro_id.code: line.valor for line in config.valor_ids}
            for name in CONFIG_VALUE_FIELDS:
                value = values.get(name) or 0.0
                config[name] = int(round(value)) if name in INT_VALUE_FIELDS else value

    def _inverse_value_fields(self):
        for config in self:
            # Primero se leen todas: crear un renglón recalcula las demás columnas desde los renglones.
            wanted = {name: config[name] for name in CONFIG_VALUE_FIELDS}
            for name, value in wanted.items():
                config._ft_set_value(name, value)

    def _ft_set_value(self, code, value):
        """Escribe el valor del parámetro ``code`` del tipo de la configuración (crea el renglón si falta).
        Un valor distinto de cero en un parámetro que el tipo no tiene es un error."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.parametro_id.code == code)[:1]
        if line:
            if line.valor != (value or 0.0):
                line.valor = value or 0.0
            return line
        if not value:
            return line
        param = self.env['ficha.tecnica.maquina.parametro'].with_context(active_test=False).search(
            [('tipo_id', '=', self.tipo_id.id), ('code', '=', code), ('ambito', '=', 'maquina')], limit=1)
        if not param:
            raise UserError('El tipo de máquina «%s» no tiene el parámetro «%s»; dele de alta en Configuración '
                            '→ Tipos de máquina.' % (self.tipo_id.name or '(sin tipo)', code))
        return self.env['ficha.tecnica.maquina.valor'].create(
            {'config_id': self.id, 'parametro_id': param.id, 'valor': value})

    def _ft_value(self, code):
        """Valor del parámetro ``code`` (número o texto según el parámetro); False si no está."""
        self.ensure_one()
        line = self.valor_ids.filtered(lambda v: v.parametro_id.code == code)[:1]
        if not line:
            return False
        return line.valor_texto if line.kind == 'text' else line.valor

    def _ft_sync_valores(self):
        """Un renglón por parámetro activo del tipo (vacío si no se capturó); quita los de otro tipo."""
        Valor = self.env['ficha.tecnica.maquina.valor']
        for config in self:
            config.valor_ids.filtered(lambda v: v.parametro_id.tipo_id != config.tipo_id).unlink()
            present = config.valor_ids.parametro_id
            missing = config.tipo_id._ft_parametros('maquina').filtered(lambda p: p not in present)
            if missing:
                Valor.create([{'config_id': config.id, 'parametro_id': p.id} for p in missing])

    @api.onchange('workcenter_id', 'tipo_id')
    def _onchange_tipo_id(self):
        for config in self:
            if config.valor_ids.parametro_id.tipo_id == config.tipo_id and \
                    config.tipo_id._ft_parametros('maquina') <= config.valor_ids.parametro_id:
                continue
            kept = {line.parametro_id.id: line for line in config.valor_ids
                    if line.parametro_id.tipo_id == config.tipo_id}
            commands = [Command.clear()]
            for param in config.tipo_id._ft_parametros('maquina'):
                old = kept.get(param.id)
                commands.append(Command.create({'parametro_id': param.id, 'valor': old.valor if old else 0.0,
                                                'valor_texto': old.valor_texto if old else False}))
            config.valor_ids = commands

    @api.model_create_multi
    def create(self, vals_list):
        configs = super().create(vals_list)
        for config in configs:
            if config.tipo_id and not config.workcenter_id.ft_tipo_id:
                config.workcenter_id.sudo().ft_tipo_id = config.tipo_id
        configs._ft_sync_valores()
        return configs

    def write(self, vals):
        res = super().write(vals)
        if 'tipo_id' in vals:
            self._ft_sync_valores()
        return res

    @api.depends('workcenter_id.ft_config_ids.date_from', 'date_from')
    def _compute_date_to(self):
        for config in self:
            later = config.workcenter_id.ft_config_ids.filtered(
                lambda c: c.date_from and config.date_from and c.date_from > config.date_from)
            nxt = min(later.mapped('date_from')) if later else False
            config.date_to = fields.Date.subtract(nxt, days=1) if nxt else False

    def _compute_is_current(self):
        today = fields.Date.context_today(self)
        for config in self:
            config.is_current = config.workcenter_id._ft_config_at(today) == config

    @api.depends('ficha_tejido_ids')
    def _compute_ficha_tejido_count(self):
        for config in self:
            config.ficha_tejido_count = len(config.ficha_tejido_ids)

    def unlink(self):
        used = self.filtered('ficha_tejido_ids')
        if used:
            raise UserError('La configuración %s ya la usan fichas de tejido: no se borra. Registre una nueva '
                            'configuración con su fecha.' % used[0].display_name)
        return super().unlink()


class MrpWorkcenterFicha(models.Model):
    _inherit = 'mrp.workcenter'

    ft_tipo_id = fields.Many2one('ficha.tecnica.maquina.tipo', string='Tipo de máquina', index=True,
                                 domain="[('clase', '=', 'maquina')]",
                                 help='Tipo de máquina (tejido circular, tintorería, acabado…); define qué parámetros '
                                      'llevan su configuración por fecha y sus procesos por artículo. Se captura en la '
                                      'pestaña «Ficha de la máquina» del centro de trabajo.')
    ft_marca = fields.Char(string='Marca de la máquina',
                           help='Marca de la máquina; se captura en la pestaña «Ficha de la máquina» del centro de '
                                'trabajo y se copia a las fichas de tejido.')
    ft_modelo = fields.Char(string='Modelo de la máquina',
                            help='Modelo de la máquina; se captura en la pestaña «Ficha de la máquina» del centro de '
                                 'trabajo.')
    ft_config_ids = fields.One2many('ficha.tecnica.maquina.config', 'workcenter_id', string='Configuraciones',
                                    help='Configuraciones por fecha de la máquina (pestaña «Ficha de la máquina» o '
                                         'Máquinas → Configuraciones por fecha).')
    ft_config_id = fields.Many2one('ficha.tecnica.maquina.config', string='Configuración vigente',
                                   compute='_compute_ft_current',
                                   help='Se calcula: la configuración que rige hoy, la de fecha de inicio más '
                                        'reciente que ya empezó.')
    ft_galga = fields.Float(string='Galga vigente', digits=(16, 0), compute='_compute_ft_current',
                            search='_search_ft_galga',
                            help='Se calcula: la galga de la configuración vigente hoy. Con ella la ficha de tejido '
                                 'ofrece solo las circulares de la galga requerida.')
    # 2.5.0: calculados con la vigente (no «related» sobre ft_config_id, que no se puede buscar y dejaba
    # aviso en el build).
    ft_valor_ids = fields.Many2many('ficha.tecnica.maquina.valor', string='Parámetros vigentes',
                                    compute='_compute_ft_current',
                                    help='Se calcula: los parámetros de la configuración vigente hoy.')
    ft_resumen = fields.Char(string='Parámetros vigentes (resumen)', compute='_compute_ft_current',
                             help='Se calcula: los parámetros de la configuración vigente hoy, en una línea.')
    ft_ficha_tejido_count = fields.Integer(string='Fichas de tejido', compute='_compute_ft_ficha_tejido_count',
                                           help='Se calcula: cuántas fichas de tejido tienen esta máquina como '
                                                'circular.')

    def _ft_config_at(self, day=None):
        """La configuración vigente en ``day`` (hoy si no se da): la de fecha de inicio más reciente que ya
        empezó. Vacía si la máquina no tiene ninguna para esa fecha."""
        self.ensure_one()
        day = fields.Date.to_date(day) if day else fields.Date.context_today(self)
        configs = self.ft_config_ids.filtered(lambda c: c.date_from and c.date_from <= day)
        return configs.sorted(lambda c: (c.date_from, c.id), reverse=True)[:1]

    @api.depends('ft_config_ids.date_from', 'ft_config_ids.galga', 'ft_config_ids.valor_ids',
                 'ft_config_ids.resumen')
    def _compute_ft_current(self):
        for wc in self:
            config = wc._ft_config_at()
            wc.ft_config_id = config
            wc.ft_valor_ids = config.valor_ids
            wc.ft_resumen = config.resumen
            wc.ft_galga = config.galga

    def _search_ft_galga(self, operator, value):
        """Máquinas cuya configuración vigente hoy tiene esa galga (para ofrecer solo las compatibles)."""
        compare = OPERATORS.get(operator)
        if not compare:
            raise UserError('La galga vigente no se puede buscar con «%s».' % operator)
        today = fields.Date.context_today(self)
        current = {}
        for config in self.env['ficha.tecnica.maquina.config'].search(
                [('date_from', '<=', today)], order='workcenter_id, date_from desc, id desc'):
            current.setdefault(config.workcenter_id.id, config)
        return [('id', 'in', [wc_id for wc_id, config in current.items()
                              if compare(config.galga, float(value or 0.0))])]

    def _compute_ft_ficha_tejido_count(self):
        data = self.env['ficha.tecnica.tejido']._read_group(
            [('maquina_tejido', 'in', self.ids)], ['maquina_tejido'], ['__count'])
        counts = {wc.id: count for wc, count in data}
        for wc in self:
            wc.ft_ficha_tejido_count = counts.get(wc.id, 0)

    def action_ft_fichas_tejido(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': 'Fichas de tejido — %s' % self.name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form',
                'domain': [('maquina_tejido', '=', self.id)], 'context': {'default_maquina_tejido': self.id}}
