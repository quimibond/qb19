# -*- coding: utf-8 -*-
import re
import unicodedata

from odoo import api, fields, models


def nombre_plano(texto):
    """Mayúsculas sin acentos ni signos (Ñ → N): como lo guarda un reloj y como se comparan los nombres."""
    texto = unicodedata.normalize('NFKD', (texto or '').upper())
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    return re.sub(r' +', ' ', re.sub(r'[^A-Z0-9 ]', ' ', texto)).strip()


def puntaje_nombre(nombre_reloj, nombre_empleado):
    """Qué tanto se parece el nombre capturado en el reloj al del empleado (0 a 1): palabras del reloj
    que aparecen en el nombre del empleado. Una palabra de 1-2 letras («J», «MA») cuenta si es inicial."""
    del_reloj = [p for p in nombre_plano(nombre_reloj).split() if p]
    del_empleado = nombre_plano(nombre_empleado).split()
    if not del_reloj or not del_empleado:
        return 0.0
    aciertos = 0
    for palabra in del_reloj:
        if palabra in del_empleado:
            aciertos += 1
        elif len(palabra) <= 2 and any(e.startswith(palabra) for e in del_empleado):
            aciertos += 0.5
    return round(aciertos / len(del_reloj), 2)


class ChecadorUsuario(models.Model):
    """Usuario dado de alta en un reloj: su número, su nombre en el reloj y el empleado detrás.

    Dos usos:

    - **Liga manual** para un reloj cuyos números no son los de nómina (Ciruelos, Hikvision: 1, 3, 5…
      contra C-26, C-33…). Capturar el número en el empleado (``qb_checador_pin``) chocaría con los
      relojes de Toluca, donde el usuario 3 es otra persona; esta tabla vale solo para su reloj y gana
      sobre las demás ligas.
    - **Lo que el reloj dice que tiene** (desde 1.6.0): los ZKTeco mandan sus usuarios y huellas por
      ADMS y aquí queda una fila por usuario (``origen = reloj``). Si el número no lleva a nadie por el
      número de checador del empleado, se sugiere un empleado por nombre y RH lo confirma.
    """
    _name = 'qb.checador.usuario'
    _description = "Usuario de un reloj checador"
    _order = 'equipo_id, id'

    equipo_id = fields.Many2one('qb.checador.equipo', string="Reloj", required=True, ondelete='cascade', index=True)
    pin = fields.Char(string="Número en el reloj", required=True,
                      help="El número de usuario (employeeNo / PIN) tal como lo manda el reloj.")
    nombre_reloj = fields.Char(string="Nombre en el reloj", help="Como está capturado en el reloj.")
    employee_id = fields.Many2one('hr.employee', string="Empleado", ondelete='cascade',
                                  help="Vacío: el reloj lo tiene pero todavía no se sabe quién es. Sus checadas "
                                       "quedan sin empleado y sus huellas no se mandan a otros relojes.")
    sugerido_id = fields.Many2one('hr.employee', string="Sugerido por nombre", readonly=True, ondelete='set null')
    puntaje = fields.Float(string="Parecido", readonly=True, digits=(3, 2),
                           help="Qué tanto se parece el nombre del reloj al del sugerido (1 = todas las palabras).")
    privilegio = fields.Char(string="Privilegio en el reloj", readonly=True,
                             help="0 = usuario normal; 14 = administrador del reloj.")
    origen = fields.Selection([('manual', "Capturado"), ('reloj', "Lo mandó el reloj")], string="Origen",
                              default='manual', required=True, readonly=True)
    huella_count = fields.Integer(string="Huellas", compute='_compute_huella_count')

    _pin_uniq = models.Constraint('unique(equipo_id, pin)', 'Ese número ya está ligado en este reloj.')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('pin'):
                vals['pin'] = vals['pin'].strip()
        usuarios = super().create(vals_list)
        usuarios._religar()
        return usuarios

    def write(self, vals):
        if vals.get('pin'):
            vals['pin'] = vals['pin'].strip()
        res = super().write(vals)
        if 'pin' in vals or 'employee_id' in vals:
            self._religar()
        return res

    def _compute_huella_count(self):
        Huella = self.env['qb.checador.huella'].sudo()
        for u in self:
            u.huella_count = Huella.search_count([('equipo_id', '=', u.equipo_id.id), ('pin', '=', u.pin)]) \
                if u.equipo_id and u.pin else 0

    def _religar(self):
        """Con empleado: las checadas de ese número que quedaron sin empleado se ligan ya (entran al
        siguiente emparejado) y sus huellas pasan a ser del empleado."""
        Checada = self.env['qb.checada'].sudo()
        Huella = self.env['qb.checador.huella'].sudo()
        for u in self.filtered('employee_id'):
            dominio = [('equipo_id', '=', u.equipo_id.id), ('pin', '=', u.pin),
                       ('state', '=', 'sin_empleado'), ('attendance_id', '=', False)]
            if u.equipo_id.checadas_desde:
                dominio.append(('timestamp', '>=', u.equipo_id.checadas_desde))
            sueltas = Checada.search(dominio)
            sueltas.write({'employee_id': u.employee_id.id, 'state': 'nueva', 'nota': False})
            Huella.search([('equipo_id', '=', u.equipo_id.id), ('pin', '=', u.pin)]).write(
                {'employee_id': u.employee_id.id})

    def _sugerir(self):
        """Para los usuarios sin empleado, el empleado activo cuyo nombre más se le parece (≥ 0.66 y sin empate)."""
        pendientes = self.filtered(lambda u: not u.employee_id and u.nombre_reloj)
        if not pendientes:
            return
        empleados = self.env['hr.employee'].sudo().search([('company_id', 'in', pendientes.equipo_id.company_id.ids)])
        for u in pendientes:
            calificados = sorted(((puntaje_nombre(u.nombre_reloj, e.name), e) for e in empleados
                                  if e.company_id == u.equipo_id.company_id), key=lambda x: -x[0])
            mejor = calificados[0] if calificados else (0.0, empleados.browse())
            empate = len(calificados) > 1 and calificados[1][0] == mejor[0]
            if mejor[0] >= 0.66 and not empate:
                u.write({'sugerido_id': mejor[1].id, 'puntaje': mejor[0]})
            else:
                u.write({'sugerido_id': False, 'puntaje': mejor[0]})

    def action_aceptar_sugerencia(self):
        for u in self.filtered(lambda x: x.sugerido_id and not x.employee_id):
            u.employee_id = u.sugerido_id

    def action_borrar_del_reloj(self):
        """Manda al reloj la orden de borrar a este usuario (con sus huellas) y quita la fila."""
        for u in self:
            u.equipo_id._encolar('DATA DELETE USERINFO PIN=%s' % u.pin,
                                 "Borrar usuario %s %s" % (u.pin, u.nombre_reloj or ''), u.employee_id)
        self.unlink()
