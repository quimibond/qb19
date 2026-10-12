# -*- coding: utf-8 -*-
"""Movimientos afiliatorios del IMSS: lo que NOI genera con «Enlace SUA» y
«Avisos al IMSS (IDSE)», hecho desde Odoo.

Dos archivos por lote y registro patronal:

* **SUA, movimientos** (``MOVT.TXT``, 49 posiciones por línea): lo que se
  importa en el SUA (el programa del IMSS que calcula las cuotas
  obrero-patronales) en *Actualizar → Trabajadores → Importar datos*. El
  formato está verificado al carácter contra los archivos que NOI generó para
  el 4.º bimestre de 2026 (``MOVS MOD 4TO BIM 2026 QTOL 01-SEP-2026.txt``):
  registro patronal (11), NSS (11), tipo de movimiento (2), fecha
  ``DDMMAAAA`` (8), folio de incapacidad (8, en blanco), días (2, ``00``) y
  salario diario integrado (7: cinco enteros y dos decimales, sin punto).
* **IDSE** (``.dat`` por tipo de movimiento, 168 posiciones por línea): lo que
  se sube firmado al portal «IMSS desde su empresa». Es la estructura
  DISPMAG/SAIIA-01 del IMSS: registro patronal (11), NSS (11), apellido
  paterno (27), apellido materno (27), nombres (27), SBC (6: cuatro enteros y
  dos decimales), 6 en blanco, tipo de trabajador (1), tipo de salario (1),
  semana o jornada reducida (1), fecha ``DDMMAAAA`` (8), UMF (3), 2 en
  blanco, tipo de movimiento (2), guía (5), clave del trabajador (10), 1 en
  blanco (causa de baja en las bajas), CURP (18) e identificador ``9``.
  **Este layout no se ha cotejado todavía contra un archivo real**: el IDSE
  valida al subir y NOI puede generarlo a disco (*Avisos al IMSS → Disco*,
  ``salarios.dat``); al primer archivo real se ajusta lo que difiera.

Los movimientos viven en ``qb.nomina.imss.movimiento`` (uno por trabajador,
tipo y fecha) y se agrupan en un ``qb.nomina.imss.lote`` por registro
patronal, que genera los archivos y deja constancia de qué se mandó. Se
detectan desde los datos de Odoo (``wizard/imss_detectar.py``) o salen del
cálculo bimestral del salario variable (``wizard/imss_sdi_bimestre.py``).

Tipos de movimiento (los mismos códigos en SUA e IDSE): ``08`` alta o
reingreso, ``07`` modificación de salario, ``02`` baja. Causas de baja del
IMSS: 1 término de contrato, 2 separación voluntaria, 3 abandono de empleo,
4 defunción, 5 clausura, 6 otra, 7 ausentismo, 8 rescisión de contrato,
9 jubilación, A pensión.
"""
import base64
import re
import unicodedata
from datetime import date

from odoo import _, api, fields, models
from odoo.exceptions import UserError

TIPO_TRABAJADOR = [
    ('1', "Permanente"),
    ('2', "Eventual de la ciudad"),
    ('3', "Eventual de la construcción"),
    ('4', "Eventual del campo"),
]
TIPO_SALARIO = [('0', "Fijo"), ('1', "Variable"), ('2', "Mixto")]
SEMANA_REDUCIDA = [
    ('0', "Semana completa"),
    ('1', "Semana reducida: 1 día"),
    ('2', "Semana reducida: 2 días"),
    ('3', "Semana reducida: 3 días"),
    ('4', "Semana reducida: 4 días"),
    ('5', "Semana reducida: 5 días"),
    ('6', "Jornada reducida"),
]
CAUSA_BAJA = [
    ('1', "Término de contrato"),
    ('2', "Separación voluntaria"),
    ('3', "Abandono de empleo"),
    ('4', "Defunción"),
    ('5', "Clausura"),
    ('6', "Otra"),
    ('7', "Ausentismo"),
    ('8', "Rescisión de contrato"),
    ('9', "Jubilación"),
    ('A', "Pensión"),
]
TIPO_MOVIMIENTO = [
    ('08', "Alta o reingreso"),
    ('07', "Modificación de salario"),
    ('02', "Baja"),
]
# Motivo de salida de Odoo (nombre, sin acentos ni mayúsculas) → causa de baja
# del IMSS. Lo que no esté aquí queda sin causa y el lote no se genera hasta
# que alguien la escoja en el movimiento.
CAUSA_POR_MOTIVO = {
    'despedido': '8',
    'renuncio': '2',
    'renuncia': '2',
    'separacion voluntaria': '2',
    'retirado': '9',
    'jubilacion': '9',
    'termino de contrato': '1',
    'abandono de empleo': '3',
    'ausentismo': '7',
    'defuncion': '4',
}
PREPOSICIONES = {'DE', 'DEL', 'LA', 'LAS', 'LOS', 'Y', 'E', 'MC', 'MAC', 'VON', 'VAN', 'DA', 'DAS', 'DI', 'DO', 'DOS', 'DU'}
NOMBRES_QUE_SE_SALTAN = {'MARIA', 'JOSE', 'MA', 'J'}
PARAM_GUIA = 'quimibond_nomina.idse_guia.%s'   # guía IDSE por registro patronal (5 dígitos)
LARGO_IDSE = 168
LARGO_SUA = 49


def _sin_acentos(texto):
    """Mayúsculas sin acentos ni diéresis; la Ñ se conserva para que cada
    formato la sustituya como le toca (``#`` en IDSE, ``/`` en SUA)."""
    texto = (texto or '').upper().replace('Ñ', '\x00')
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    return texto.replace('\x00', 'Ñ')


def _campo_texto(texto, largo, enie='#'):
    """Texto de ancho fijo: solo A-Z, espacio y la Ñ sustituida; recortado o
    rellenado con espacios a la derecha."""
    texto = _sin_acentos(texto).replace('Ñ', enie)
    texto = re.sub(r'[^A-Z0-9 #/]', '', texto)
    texto = re.sub(r' +', ' ', texto).strip()
    return texto[:largo].ljust(largo)


def _campo_numero(valor, largo):
    """Entero en ancho fijo con ceros a la izquierda (lo que no quepa, se recorta por la izquierda)."""
    texto = str(int(valor or 0))
    return texto[-largo:].rjust(largo, '0')


def _solo_digitos(texto):
    return re.sub(r'\D', '', texto or '')


def registro_patronal_plano(texto):
    """``C-675994510-1`` (como lo captura RH y lo timbra el despacho) → ``C6759945101`` (como lo piden SUA e IDSE)."""
    return re.sub(r'[^A-Z0-9]', '', (texto or '').upper())


def fecha_ddmmaaaa(fecha):
    return fecha.strftime('%d%m%Y') if fecha else '        '


# ----------------------------------------------------------------------
# Datos del IMSS en el contrato y en el empleado
# ----------------------------------------------------------------------
class HrVersion(models.Model):
    _inherit = 'hr.version'

    qb_imss_umf = fields.Char(
        string="UMF", size=3, groups='hr.group_hr_user',
        help="Unidad de Medicina Familiar del trabajador (3 dígitos). La pide el IDSE en las altas y reingresos.")
    qb_imss_tipo_trabajador = fields.Selection(
        TIPO_TRABAJADOR, string="Tipo de trabajador (IMSS)", default='1', groups='hr.group_hr_user')
    qb_imss_tipo_salario = fields.Selection(
        TIPO_SALARIO, string="Tipo de salario (IMSS)", default='2', groups='hr.group_hr_user',
        help="Mixto cuando además del sueldo fijo hay percepciones variables (bonos, comisiones, prima "
             "dominical…) que integran al salario cada bimestre. Así están dados de alta en NOI.")
    qb_imss_semana_reducida = fields.Selection(
        SEMANA_REDUCIDA, string="Semana o jornada reducida (IMSS)", default='0', groups='hr.group_hr_user')

    def _qb_imss_registro_patronal(self):
        """Registro patronal del contrato o de la compañía, sin guiones (11 posiciones)."""
        self.ensure_one()
        return registro_patronal_plano(
            self.l10n_mx_employer_registration or self.company_id.l10n_mx_imss_id or '')

    def _qb_imss_propiedades(self):
        """``payroll_properties`` como dict {nombre: valor}, venga como venga del ORM."""
        self.ensure_one()
        props = self.payroll_properties
        if isinstance(props, dict):
            return dict(props)
        return {p.get('name'): p.get('value') for p in (props or []) if isinstance(p, dict) and p.get('name')}

    def _qb_imss_sdi(self):
        """SDI declarado ante el IMSS (propiedad ``sdi_imss`` del contrato), 0.0 si no está."""
        self.ensure_one()
        try:
            return round(float(self._qb_imss_propiedades().get('sdi_imss') or 0.0), 2)
        except (TypeError, ValueError):
            return 0.0

    def _qb_imss_escribir_sdi(self, sdi):
        """Escribe la propiedad ``sdi_imss`` del contrato sin tocar las demás."""
        self.ensure_one()
        props = self._qb_imss_propiedades()
        props['sdi_imss'] = round(sdi, 2)
        self.write({'payroll_properties': props})

    def _qb_imss_salario_diario(self):
        """Salario diario simple a partir del sueldo del contrato y su periodicidad."""
        self.ensure_one()
        divisor = {'daily': 1, 'weekly': 7, '10_days': 10, '14_days': 14, 'bi-weekly': 15,
                   'monthly': 30, 'bi-monthly': 60}.get(self.schedule_pay or 'monthly', 30)
        return round((self.wage or 0.0) / divisor, 2)


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    qb_imss_umf = fields.Char(related='version_id.qb_imss_umf', readonly=False, groups='hr.group_hr_user')
    qb_imss_tipo_trabajador = fields.Selection(
        related='version_id.qb_imss_tipo_trabajador', readonly=False, groups='hr.group_hr_user')
    qb_imss_tipo_salario = fields.Selection(
        related='version_id.qb_imss_tipo_salario', readonly=False, groups='hr.group_hr_user')
    qb_imss_semana_reducida = fields.Selection(
        related='version_id.qb_imss_semana_reducida', readonly=False, groups='hr.group_hr_user')
    qb_apellido_paterno = fields.Char(string="Apellido paterno", groups='hr.group_hr_user',
                                      help="Para los archivos del IMSS. Vacío: se deduce del nombre con la CURP.")
    qb_apellido_materno = fields.Char(string="Apellido materno", groups='hr.group_hr_user')
    qb_nombres = fields.Char(string="Nombre(s)", groups='hr.group_hr_user')

    def _qb_imss_nombre_partes(self):
        """(apellido paterno, apellido materno, nombres), en mayúsculas sin acentos.

        Odoo guarda el nombre completo en un solo campo (``NOMBRES APELLIDO
        APELLIDO``). Si RH capturó las tres partes, se usan. Si no, se
        prueban todos los cortes posibles y se queda el que coincide con la
        CURP (primera letra y primera vocal interna del paterno, inicial del
        materno, inicial del nombre; MARIA y JOSE se saltan como hace el
        RENAPO). Sin CURP o sin coincidencia: las dos últimas palabras son
        los apellidos."""
        self.ensure_one()
        if self.qb_apellido_paterno or self.qb_nombres:
            return (_sin_acentos(self.qb_apellido_paterno), _sin_acentos(self.qb_apellido_materno),
                    _sin_acentos(self.qb_nombres))
        palabras = _sin_acentos(self.legal_name or self.name).split()
        if len(palabras) < 2:
            return (' '.join(palabras), '', '')
        if len(palabras) == 2:
            return (palabras[1], '', palabras[0])
        curp = (self.l10n_mx_curp or '').upper()
        if len(curp) >= 4:
            for fin_nombres in range(1, len(palabras) - 1):
                for fin_paterno in range(fin_nombres + 1, len(palabras)):
                    nombres = palabras[:fin_nombres]
                    paterno = ' '.join(palabras[fin_nombres:fin_paterno])
                    materno = ' '.join(palabras[fin_paterno:])
                    if self._qb_imss_curp_coincide(curp, paterno, materno, nombres):
                        return (paterno, materno, ' '.join(nombres))
        return (palabras[-2], palabras[-1], ' '.join(palabras[:-2]))

    @staticmethod
    def _qb_imss_curp_coincide(curp, paterno, materno, nombres):
        """¿Las cuatro primeras letras de la CURP salen de esta partición del
        nombre? Regla del RENAPO: inicial y primera vocal interna del paterno,
        inicial del materno, inicial del nombre; se saltan las preposiciones
        (DE, DEL, LA, LOS…) y, en los nombres compuestos, MARIA y JOSE."""
        def util(palabras, saltar_nombres=False):
            palabras = [re.sub(r'[^A-Z]', '', p.replace('Ñ', 'X')) for p in palabras]
            palabras = [p for p in palabras if p and p not in PREPOSICIONES]
            if saltar_nombres and len(palabras) > 1 and palabras[0] in NOMBRES_QUE_SE_SALTAN:
                palabras = palabras[1:]
            return palabras[0] if palabras else ''

        pat = util(paterno.split())
        mat = util(materno.split())
        nombre = util(nombres, saltar_nombres=True)
        if not pat or not nombre:
            return False
        vocal = next((c for c in pat[1:] if c in 'AEIOU'), 'X')
        return (curp[0] == pat[0] and curp[1] == vocal
                and curp[2] == (mat[0] if mat else 'X') and curp[3] == nombre[0])


# ----------------------------------------------------------------------
# Movimientos y lotes
# ----------------------------------------------------------------------
class ImssMovimiento(models.Model):
    _name = 'qb.nomina.imss.movimiento'
    _description = "Movimiento afiliatorio IMSS"
    _order = 'fecha desc, employee_id'

    employee_id = fields.Many2one('hr.employee', string="Trabajador", required=True, index=True,
                                  context={'active_test': False}, ondelete='restrict')
    version_id = fields.Many2one('hr.version', string="Contrato", ondelete='set null')
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    registro_patronal = fields.Char(string="Registro patronal", required=True, size=11,
                                    help="Sin guiones, 11 posiciones (C6759945101).")
    tipo = fields.Selection(TIPO_MOVIMIENTO, string="Movimiento", required=True)
    fecha = fields.Date(string="Fecha del movimiento", required=True)
    sdi = fields.Float(string="Salario diario integrado", digits=(16, 2),
                       help="El SBC que se declara. En las bajas no se usa.")
    causa_baja = fields.Selection(CAUSA_BAJA, string="Causa de baja")
    origen = fields.Selection([
        ('alta', "Alta detectada en el contrato"),
        ('baja', "Baja detectada en el empleado"),
        ('version', "Cambio de SDI en el contrato"),
        ('bimestre', "Cálculo bimestral del salario variable"),
        ('manual', "Capturado a mano"),
    ], string="Origen", default='manual', required=True)
    state = fields.Selection([('borrador', "Por exportar"), ('exportado', "Exportado")],
                             string="Estado", default='borrador', required=True, index=True)
    lote_id = fields.Many2one('qb.nomina.imss.lote', string="Lote", ondelete='set null', index=True)
    nota = fields.Char(string="Nota")
    nss = fields.Char(string="NSS", compute='_compute_identidad')
    curp = fields.Char(string="CURP", compute='_compute_identidad')

    _uniq = models.Constraint(
        'unique(employee_id, tipo, fecha)',
        'Ya hay un movimiento de ese tipo para ese trabajador en esa fecha.')

    @api.depends('employee_id')
    def _compute_identidad(self):
        for mov in self:
            emp = mov.employee_id.sudo()
            mov.nss = _solo_digitos(emp.ssnid)
            mov.curp = (emp.l10n_mx_curp or '').strip().upper()

    @api.depends('employee_id', 'tipo', 'fecha')
    def _compute_display_name(self):
        etiquetas = dict(TIPO_MOVIMIENTO)
        for mov in self:
            mov.display_name = "%s · %s · %s" % (
                etiquetas.get(mov.tipo, mov.tipo or ''), mov.fecha or '', mov.employee_id.name or '')

    @api.onchange('employee_id')
    def _onchange_employee_id(self):
        for mov in self:
            version = mov.employee_id.version_id
            if version and not mov.version_id:
                mov.version_id = version
            if version and not mov.registro_patronal:
                mov.registro_patronal = version._qb_imss_registro_patronal()
            if version and not mov.sdi and mov.tipo != '02':
                mov.sdi = version._qb_imss_sdi()

    def write(self, vals):
        if any(m.state == 'exportado' for m in self) and set(vals) - {'nota', 'state', 'lote_id'}:
            raise UserError(_("Un movimiento ya exportado no se edita: regrese el lote a borrador primero."))
        return super().write(vals)

    def unlink(self):
        if any(m.state == 'exportado' for m in self):
            raise UserError(_("Un movimiento ya exportado no se borra: regrese el lote a borrador primero."))
        return super().unlink()

    @staticmethod
    def _causa_para_motivo(motivo):
        """Causa de baja del IMSS para un ``hr.departure.reason`` (por nombre); '' si no se conoce."""
        if not motivo:
            return ''
        nombre = re.sub(r'[^a-z ]', '', _sin_acentos(motivo.name).lower().replace('ñ', 'n')).strip()
        return CAUSA_POR_MOTIVO.get(nombre, '')

    # --- validación ----------------------------------------------------
    def _errores(self):
        """Lo que impide mandar este movimiento, en texto; lista vacía si está completo."""
        self.ensure_one()
        emp = self.employee_id.sudo()
        errores = []
        if len(self.registro_patronal or '') != 11:
            errores.append(_("registro patronal de 11 posiciones"))
        if len(self.nss or '') != 11:
            errores.append(_("NSS de 11 dígitos (tiene «%s»)") % (emp.ssnid or ''))
        if self.tipo == '02':
            if not self.causa_baja:
                errores.append(_("causa de baja"))
        else:
            if not self.sdi or self.sdi <= 0:
                errores.append(_("salario diario integrado"))
            if self.sdi and self.sdi >= 10000:
                errores.append(_("el SDI no cabe en el archivo (máximo 9,999.99)"))
        if len(self.curp or '') != 18:
            errores.append(_("CURP de 18 caracteres"))
        if self.tipo == '08':
            version = self.version_id or emp.version_id
            if not _solo_digitos(version.qb_imss_umf) or len(_solo_digitos(version.qb_imss_umf)) > 3:
                errores.append(_("UMF (clínica) en el contrato"))
        return errores

    # --- líneas de los archivos ----------------------------------------
    def _linea_sua(self):
        """Línea de ``MOVT.TXT`` (49 posiciones), igual a la que genera NOI."""
        self.ensure_one()
        sdi = 0 if self.tipo == '02' else int(round((self.sdi or 0.0) * 100))
        linea = (
            self.registro_patronal.ljust(11)[:11]
            + _campo_numero(self.nss, 11)
            + self.tipo
            + fecha_ddmmaaaa(self.fecha)
            + ' ' * 8            # folio de incapacidad
            + '00'               # días (incapacidad / ausentismo)
            + _campo_numero(sdi, 7)
        )
        assert len(linea) == LARGO_SUA, len(linea)
        return linea

    def _linea_idse(self, guia):
        """Línea del archivo IDSE (168 posiciones, estructura DISPMAG)."""
        self.ensure_one()
        emp = self.employee_id.sudo()
        version = (self.version_id or emp.version_id).sudo()
        paterno, materno, nombres = emp._qb_imss_nombre_partes()
        es_baja = self.tipo == '02'
        sbc = '' if es_baja else _campo_numero(int(round((self.sdi or 0.0) * 100)), 6)
        clave = re.sub(r'[^A-Z0-9]', '', (emp.registration_number or '').upper())
        linea = (
            self.registro_patronal.ljust(11)[:11]
            + _campo_numero(self.nss, 11)
            + _campo_texto(paterno, 27)
            + _campo_texto(materno, 27)
            + _campo_texto(nombres, 27)
            + sbc.ljust(6)
            + ' ' * 6
            + ('' if es_baja else (version.qb_imss_tipo_trabajador or '1')).ljust(1)
            + ('' if es_baja else (version.qb_imss_tipo_salario or '2')).ljust(1)
            + ('' if es_baja else (version.qb_imss_semana_reducida or '0')).ljust(1)
            + fecha_ddmmaaaa(self.fecha)
            + ('' if es_baja else _campo_numero(_solo_digitos(version.qb_imss_umf), 3)).ljust(3)
            + ' ' * 2
            + self.tipo
            + _campo_numero(_solo_digitos(guia), 5)
            + clave[:10].ljust(10)
            + ((self.causa_baja or ' ') if es_baja else ' ')
            + (self.curp or '').ljust(18)[:18]
            + '9'
        )
        assert len(linea) == LARGO_IDSE, len(linea)
        return linea


class ImssLote(models.Model):
    _name = 'qb.nomina.imss.lote'
    _description = "Lote de movimientos IMSS (SUA e IDSE)"
    _inherit = ['mail.thread']
    _order = 'fecha desc, id desc'

    name = fields.Char(string="Nombre", required=True, default=lambda self: _("Nuevo lote"))
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    registro_patronal = fields.Char(string="Registro patronal", required=True, size=11,
                                    help="Sin guiones (C6759945101 Toluca, Y6087828106 México). Un lote por registro.")
    guia = fields.Char(string="Guía IDSE", size=5,
                       help="Número de guía de 5 dígitos: subdelegación del IMSS del registro patronal (2) + "
                            "origen del movimiento (3; 400 = dispositivo magnético). Se recuerda por registro patronal.")
    fecha = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    movimiento_ids = fields.One2many('qb.nomina.imss.movimiento', 'lote_id', string="Movimientos")
    movimiento_count = fields.Integer(compute='_compute_counts')
    state = fields.Selection([('borrador', "Borrador"), ('generado', "Archivos generados")],
                             string="Estado", default='borrador', required=True, tracking=True)
    archivo_sua = fields.Binary(string="SUA: movimientos", readonly=True, attachment=True)
    archivo_sua_nombre = fields.Char(readonly=True)
    archivo_idse_altas = fields.Binary(string="IDSE: altas y reingresos", readonly=True, attachment=True)
    archivo_idse_altas_nombre = fields.Char(readonly=True)
    archivo_idse_modificaciones = fields.Binary(string="IDSE: modificaciones de salario", readonly=True,
                                                attachment=True)
    archivo_idse_modificaciones_nombre = fields.Char(readonly=True)
    archivo_idse_bajas = fields.Binary(string="IDSE: bajas", readonly=True, attachment=True)
    archivo_idse_bajas_nombre = fields.Char(readonly=True)
    resumen = fields.Text(string="Resumen", readonly=True)

    @api.depends('movimiento_ids')
    def _compute_counts(self):
        for lote in self:
            lote.movimiento_count = len(lote.movimiento_ids)

    @api.onchange('registro_patronal')
    def _onchange_registro_patronal(self):
        for lote in self:
            rp = registro_patronal_plano(lote.registro_patronal)
            if rp and rp != lote.registro_patronal:
                lote.registro_patronal = rp
            if rp and not lote.guia:
                lote.guia = self.env['ir.config_parameter'].sudo().get_param(PARAM_GUIA % rp) or ''

    def action_tomar_pendientes(self):
        """Trae al lote todos los movimientos por exportar de este registro patronal."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError(_("El lote ya generó sus archivos."))
        pendientes = self.env['qb.nomina.imss.movimiento'].search([
            ('state', '=', 'borrador'), ('lote_id', '=', False),
            ('registro_patronal', '=', self.registro_patronal), ('company_id', '=', self.company_id.id)])
        pendientes.write({'lote_id': self.id})
        return True

    def action_generar(self):
        """Genera los archivos SUA e IDSE del lote y marca los movimientos como exportados."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError(_("El lote ya generó sus archivos; regréselo a borrador para rehacerlos."))
        movs = self.movimiento_ids.sorted(lambda m: (m.tipo, m.fecha, m.employee_id.name or ''))
        if not movs:
            raise UserError(_("El lote no tiene movimientos."))
        rp = registro_patronal_plano(self.registro_patronal)
        fuera = movs.filtered(lambda m: m.registro_patronal != rp)
        if fuera:
            raise UserError(_("Estos movimientos son de otro registro patronal: %s")
                            % ', '.join(fuera.mapped('employee_id.name')))
        if len(_solo_digitos(self.guia)) != 5:
            raise UserError(_("Capture la guía IDSE de 5 dígitos del registro patronal %s.") % rp)
        errores = []
        for mov in movs:
            faltan = mov._errores()
            if faltan:
                errores.append("%s (%s %s): %s" % (mov.employee_id.name, dict(TIPO_MOVIMIENTO)[mov.tipo],
                                                  mov.fecha, '; '.join(faltan)))
        if errores:
            raise UserError(_("Falta información para generar los archivos:\n%s") % '\n'.join(errores))

        self.env['ir.config_parameter'].sudo().set_param(PARAM_GUIA % rp, _solo_digitos(self.guia))
        sufijo = "%s_%s" % (rp, self.fecha.strftime('%Y%m%d'))

        def archivo(lineas):
            if not lineas:
                return False
            return base64.b64encode(('\r\n'.join(lineas) + '\r\n').encode('latin-1', 'replace'))

        sua = [m._linea_sua() for m in movs]
        altas = [m._linea_idse(self.guia) for m in movs if m.tipo == '08']
        modificaciones = [m._linea_idse(self.guia) for m in movs if m.tipo == '07']
        bajas = [m._linea_idse(self.guia) for m in movs if m.tipo == '02']
        vals = {
            'state': 'generado',
            'archivo_sua': archivo(sua), 'archivo_sua_nombre': 'MOVT_%s.txt' % sufijo,
            'archivo_idse_altas': archivo(altas), 'archivo_idse_altas_nombre': 'IDSE_altas_%s.dat' % sufijo,
            'archivo_idse_modificaciones': archivo(modificaciones),
            'archivo_idse_modificaciones_nombre': 'IDSE_modificaciones_%s.dat' % sufijo,
            'archivo_idse_bajas': archivo(bajas), 'archivo_idse_bajas_nombre': 'IDSE_bajas_%s.dat' % sufijo,
            'resumen': _("%(n)d movimientos: %(a)d altas/reingresos, %(m)d modificaciones, %(b)d bajas. "
                         "SUA: %(n)d líneas de 49. IDSE: %(a)d + %(m)d + %(b)d líneas de 168.") % {
                'n': len(movs), 'a': len(altas), 'm': len(modificaciones), 'b': len(bajas)},
        }
        if self.name == _("Nuevo lote"):
            vals['name'] = "IMSS %s %s" % (rp, self.fecha.strftime('%Y-%m-%d'))
        self.write(vals)
        movs.write({'state': 'exportado'})
        self.message_post(body=vals['resumen'])
        return True

    def action_borrador(self):
        """Deja el lote y sus movimientos como estaban antes de generar (para corregir y rehacer)."""
        for lote in self:
            lote.movimiento_ids.write({'state': 'borrador'})
            lote.write({'state': 'borrador', 'archivo_sua': False, 'archivo_idse_altas': False,
                        'archivo_idse_modificaciones': False, 'archivo_idse_bajas': False, 'resumen': False})
        return True

    def action_ver_movimientos(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': _("Movimientos del lote"),
            'res_model': 'qb.nomina.imss.movimiento', 'view_mode': 'list,form',
            'domain': [('lote_id', '=', self.id)], 'context': {'default_lote_id': self.id},
        }

    def unlink(self):
        if any(l.state == 'generado' for l in self):
            raise UserError(_("Un lote con archivos generados no se borra: regréselo a borrador primero."))
        self.movimiento_ids.write({'lote_id': False})
        return super().unlink()


# ----------------------------------------------------------------------
# Factor de integración (lo que NOI llama «factor de int.»)
# ----------------------------------------------------------------------
PARAM_DIAS_AGUINALDO = 'quimibond_nomina.sdi_dias_aguinaldo'      # Quimibond paga 16 (NOI)
PARAM_PRIMA_VACACIONAL = 'quimibond_nomina.sdi_prima_vacacional'  # 0.25 por ley
PARAM_UMA_DIARIA = 'quimibond_nomina.uma_diaria'                  # respaldo si no hay parámetro de nómina
PARAM_CODIGOS_VARIABLES = 'quimibond_nomina.sdi_variables_codigos'
CODIGOS_VARIABLES_DEFAULT = 'COMMISSIONS,BONUS,LOGRO_OBJ,PDOM_EXEMPT,PDOM_TAX,TAX_TRANSPORT,NO_TAX_TRANSPORT'
CODIGO_DIAS_COTIZADOS = 'DIAS_IMSS'
UMA_TOPE_VECES = 25


def dias_vacaciones_lft(anio_de_servicio):
    """Días de vacaciones por año de servicio (LFT art. 76, reforma 2023):
    12, 14, 16, 18, 20 los primeros cinco años; después 22 y dos días más por
    cada cinco años."""
    anio = max(1, int(anio_de_servicio))
    if anio <= 5:
        return 10 + 2 * anio
    return 22 + 2 * ((anio - 6) // 5)


def factor_integracion(anio_de_servicio, dias_aguinaldo=16, prima_vacacional=0.25):
    """1 + (aguinaldo + vacaciones × prima) / 365, a cuatro decimales como NOI."""
    vac = dias_vacaciones_lft(anio_de_servicio)
    return round(1 + (dias_aguinaldo + vac * prima_vacacional) / 365.0, 4)


def anio_de_servicio(fecha_ingreso, fecha):
    """En qué año de servicio está el trabajador en ``fecha`` (1 el primero)."""
    if not fecha_ingreso or fecha < fecha_ingreso:
        return 1
    anios = fecha.year - fecha_ingreso.year
    if (fecha.month, fecha.day) < (fecha_ingreso.month, fecha_ingreso.day):
        anios -= 1
    return anios + 1


def bimestre_rango(anio, bimestre):
    """(primer día, último día, fecha de aplicación) del bimestre 1..6."""
    bimestre = int(bimestre)
    mes_ini = 2 * bimestre - 1
    inicio = date(anio, mes_ini, 1)
    if bimestre == 6:
        fin = date(anio, 12, 31)
        aplica = date(anio + 1, 1, 1)
    else:
        aplica = date(anio, mes_ini + 2, 1)
        fin = date.fromordinal(aplica.toordinal() - 1)
    return inicio, fin, aplica


class ImssLoteMenu(models.Model):
    _inherit = 'qb.nomina.imss.lote'

    MENU_XMLID = 'quimibond_nomina.menu_qb_nomina_imss'

    @api.model
    def qb_nomina_colgar_menu_imss(self):
        """Cuelga el menú «IMSS» de la raíz de la app de Nómina y lo activa. Se
        llama desde views/imss_views.xml en cada instalación y actualización,
        por lo mismo que el menú del centinela (ver ``rule_sentinel.py``): un
        ``parent`` por xmlid que no resuelve tumba la instalación."""
        menu = self.env.ref(self.MENU_XMLID, raise_if_not_found=False)
        if not menu:
            return False
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        icono = self.env['qb.nomina.rule.sentinel'].PAYROLL_APP_ICON
        root = Menu.search([('parent_id', '=', False), ('web_icon', '=', icono)], limit=1)
        if not root:
            menu.sudo().write({'active': False})
            return False
        menu.sudo().write({'parent_id': root.id, 'active': True})
        return True
