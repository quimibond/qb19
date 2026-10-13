# -*- coding: utf-8 -*-
"""Movimientos afiliatorios IMSS: formato de las líneas (SUA al carácter
contra el archivo de NOI del 4.º bimestre de 2026), partes del nombre con la
CURP, factor de integración, detección de altas y bajas y cálculo bimestral.

No corre en el CI (depende de Enterprise); en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.exceptions import UserError
from odoo.tests.common import TransactionCase

from odoo.addons.quimibond_nomina.models.imss import (
    anio_de_servicio, bimestre_rango, dias_vacaciones_lft, factor_integracion, registro_patronal_plano)


class TestImss(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.company = env.company
        cls.company.l10n_mx_imss_id = 'C-675994510-1'
        cls.emp = env['hr.employee'].create({
            'name': 'JORGE MATIAS SALINAS', 'registration_number': 'S-250', 'ssnid': '16998226027',
            'l10n_mx_curp': 'MASJ830911HMCTLR06', 'contract_date_start': date(2026, 9, 23),
            'wage': 2763.32, 'schedule_pay': 'weekly',
        })
        cls.version = cls.emp.version_id
        cls.version.write({'qb_imss_umf': '7', 'qb_imss_tipo_trabajador': '1', 'qb_imss_tipo_salario': '2'})
        cls.Mov = env['qb.nomina.imss.movimiento']

    # --- helpers puros --------------------------------------------------
    def test_registro_patronal_plano(self):
        self.assertEqual(registro_patronal_plano('C-675994510-1'), 'C6759945101')
        self.assertEqual(registro_patronal_plano('Y6087828106'), 'Y6087828106')
        self.assertEqual(self.version._qb_imss_registro_patronal(), 'C6759945101')
        self.version.l10n_mx_employer_registration = 'Y6087828106'
        self.assertEqual(self.version._qb_imss_registro_patronal(), 'Y6087828106')

    def test_factor_de_integracion_como_noi(self):
        # Tabla del Excel de RH (MOD SALARIAL VARIABLES 4o BIMESTRE 2026): aguinaldo 16, prima 25 %.
        esperado = {1: 1.0521, 2: 1.0534, 3: 1.0548, 4: 1.0562, 5: 1.0575, 6: 1.0589, 10: 1.0589,
                    11: 1.0603, 16: 1.0616, 21: 1.0630, 26: 1.0644, 30: 1.0644}
        for anio, factor in esperado.items():
            self.assertEqual(factor_integracion(anio, 16, 0.25), factor, 'año %d' % anio)
        self.assertEqual(dias_vacaciones_lft(1), 12)
        self.assertEqual(dias_vacaciones_lft(5), 20)
        self.assertEqual(dias_vacaciones_lft(6), 22)
        self.assertEqual(dias_vacaciones_lft(11), 24)
        self.assertEqual(dias_vacaciones_lft(31), 32)

    def test_anio_de_servicio_y_bimestre(self):
        self.assertEqual(anio_de_servicio(date(2025, 9, 8), date(2026, 9, 1)), 1)
        self.assertEqual(anio_de_servicio(date(2025, 9, 1), date(2026, 9, 1)), 2)
        self.assertEqual(anio_de_servicio(date(2000, 10, 16), date(2026, 9, 1)), 26)
        self.assertEqual(bimestre_rango(2026, 4), (date(2026, 7, 1), date(2026, 8, 31), date(2026, 9, 1)))
        self.assertEqual(bimestre_rango(2026, 6), (date(2026, 11, 1), date(2026, 12, 31), date(2027, 1, 1)))
        self.assertEqual(bimestre_rango(2026, 1), (date(2026, 1, 1), date(2026, 2, 28), date(2026, 3, 1)))

    def test_salario_diario_del_contrato(self):
        self.assertEqual(self.version._qb_imss_salario_diario(), 394.76)   # 2,763.32 / 7, como la solicitud de alta

    # --- nombre -----------------------------------------------------------
    def test_partes_del_nombre_con_curp(self):
        self.assertEqual(self.emp._qb_imss_nombre_partes(), ('MATIAS', 'SALINAS', 'JORGE'))
        compuesto = self.env['hr.employee'].create({'name': 'MAIRANI HERNANDEZ DE LA ROSA',
                                                    'l10n_mx_curp': 'HERM990101MMCRSR01'})
        self.assertEqual(compuesto._qb_imss_nombre_partes(), ('HERNANDEZ', 'DE LA ROSA', 'MAIRANI'))
        maria = self.env['hr.employee'].create({'name': 'MARIA DE LOS ANGELES MARTINEZ MENDOZA',
                                                'l10n_mx_curp': 'MAMA800101MDFRNN09'})
        self.assertEqual(maria._qb_imss_nombre_partes(), ('MARTINEZ', 'MENDOZA', 'MARIA DE LOS ANGELES'))
        sin_curp = self.env['hr.employee'].create({'name': 'José Antonio Flores Vázquez'})
        self.assertEqual(sin_curp._qb_imss_nombre_partes(), ('FLORES', 'VAZQUEZ', 'JOSE ANTONIO'))
        sin_curp.write({'qb_apellido_paterno': 'Flores', 'qb_apellido_materno': 'Vázquez', 'qb_nombres': 'José Antonio'})
        self.assertEqual(sin_curp._qb_imss_nombre_partes(), ('FLORES', 'VAZQUEZ', 'JOSE ANTONIO'))

    # --- líneas -----------------------------------------------------------
    def _mov(self, tipo, fecha, sdi=0.0, causa=False, emp=None):
        emp = emp or self.emp
        return self.Mov.create({
            'employee_id': emp.id, 'version_id': emp.version_id.id, 'registro_patronal': 'C6759945101',
            'tipo': tipo, 'fecha': fecha, 'sdi': sdi, 'causa_baja': causa,
        })

    def test_linea_sua_igual_a_noi(self):
        # Primera línea de «MOVS MOD 4to BIM 2026 SEMANAL.txt» (NOI, enlace SUA): NSS 16007608025, SDI 889.43.
        otro = self.env['hr.employee'].create({'name': 'PRUEBA SUA', 'ssnid': '16007608025',
                                               'l10n_mx_curp': 'PRSU900101HMCRXX01'})
        mov = self._mov('07', date(2026, 9, 1), 889.43, emp=otro)
        self.assertEqual(mov._linea_sua(), 'C6759945101160076080250701092026        000088943')
        self.assertEqual(len(mov._linea_sua()), 49)
        baja = self._mov('02', date(2026, 9, 22), causa='3')
        self.assertEqual(baja._linea_sua(), 'C6759945101169982260270201092026        000000000'
                         .replace('01092026', '22092026'))

    def test_linea_idse(self):
        mov = self._mov('08', date(2026, 9, 23), 427.12)
        linea = mov._linea_idse('13400')
        self.assertEqual(len(linea), 168)
        self.assertEqual(linea[0:11], 'C6759945101')
        self.assertEqual(linea[11:22], '16998226027')
        self.assertEqual(linea[22:49].strip(), 'MATIAS')
        self.assertEqual(linea[49:76].strip(), 'SALINAS')
        self.assertEqual(linea[76:103].strip(), 'JORGE')
        self.assertEqual(linea[103:109], '042712')
        self.assertEqual(linea[115:118], '120')          # permanente, mixto, semana completa
        self.assertEqual(linea[118:126], '23092026')
        self.assertEqual(linea[126:129], '007')
        self.assertEqual(linea[131:133], '08')
        self.assertEqual(linea[133:138], '13400')
        self.assertEqual(linea[138:148].strip(), 'S250')
        self.assertEqual(linea[149:167], 'MASJ830911HMCTLR06')
        self.assertEqual(linea[167], '9')
        baja = self._mov('02', date(2026, 10, 6), causa='1')
        linea = baja._linea_idse('13400')
        self.assertEqual(len(linea), 168)
        self.assertEqual(linea[103:118], ' ' * 15)
        self.assertEqual(linea[131:133], '02')
        self.assertEqual(linea[148], '1')

    def test_enie_y_acentos(self):
        emp = self.env['hr.employee'].create({'name': 'SELENA MANRIQUEZ YAÑEZ', 'ssnid': '85160159407',
                                              'l10n_mx_curp': 'MAYS990101MMCNXL01'})
        mov = self._mov('07', date(2026, 9, 1), 469.83, emp=emp)
        linea = mov._linea_idse('13400')
        self.assertEqual(linea[49:76].strip(), 'YA#EZ')

    def test_errores_antes_de_generar(self):
        sin_nss = self.env['hr.employee'].create({'name': 'SIN NSS', 'l10n_mx_curp': 'SINS900101HMCNXX01'})
        mov = self._mov('07', date(2026, 9, 1), 500.0, emp=sin_nss)
        self.assertTrue(any('NSS' in e for e in mov._errores()))
        baja = self._mov('02', date(2026, 9, 2))
        self.assertTrue(any('causa' in e for e in baja._errores()))
        self.assertEqual(self._mov('07', date(2026, 9, 3), 500.0)._errores(), [])

    def test_lote_genera_y_regresa(self):
        mov = self._mov('07', date(2026, 9, 1), 427.12)
        lote = self.env['qb.nomina.imss.lote'].create({'registro_patronal': 'C6759945101', 'guia': '13400',
                                                       'fecha': date(2026, 9, 1)})
        lote.action_tomar_pendientes()
        self.assertEqual(lote.movimiento_ids, mov)
        lote.action_generar()
        self.assertEqual(lote.state, 'generado')
        self.assertEqual(mov.state, 'exportado')
        self.assertTrue(lote.archivo_sua and lote.archivo_idse_modificaciones)
        self.assertFalse(lote.archivo_idse_altas)
        self.assertEqual(lote.archivo_sua_nombre, 'MOVT_C6759945101_20260901.txt')
        with self.assertRaises(UserError):
            mov.sdi = 1
        with self.assertRaises(UserError):
            mov.unlink()
        self.assertEqual(self.env['ir.config_parameter'].sudo().get_param('quimibond_nomina.idse_guia.C6759945101'), '13400')
        lote.action_borrador()
        self.assertEqual((lote.state, mov.state), ('borrador', 'borrador'))

    def test_lote_detiene_lo_incompleto(self):
        self._mov('02', date(2026, 9, 22))   # sin causa
        lote = self.env['qb.nomina.imss.lote'].create({'registro_patronal': 'C6759945101', 'guia': '13400'})
        lote.action_tomar_pendientes()
        with self.assertRaises(UserError):
            lote.action_generar()

    # --- detección --------------------------------------------------------
    def test_detectar_alta_y_baja(self):
        self.version._qb_imss_escribir_sdi(427.12)
        self.assertEqual(self.version._qb_imss_sdi(), 427.12)
        motivo = self.env['hr.departure.reason'].create({'name': 'Abandono de empleo'})
        baja = self.env['hr.employee'].create({
            'name': 'LUIS ENRIQUE POZOS FLORES', 'ssnid': '11139701558', 'l10n_mx_curp': 'POFL970413HDFZLS06',
            'contract_date_start': date(2026, 9, 7), 'wage': 2322.60, 'schedule_pay': 'weekly',
        })
        baja.write({'departure_reason_id': motivo.id, 'departure_date': date(2026, 9, 22), 'active': False})
        w = self.env['qb.nomina.imss.detectar'].create({'fecha_desde': date(2026, 9, 1), 'fecha_hasta': date(2026, 9, 30)})
        w.action_detectar()
        movs = self.Mov.search([('employee_id', 'in', (self.emp | baja).ids)])
        por_tipo = {(m.employee_id, m.tipo): m for m in movs}
        alta = por_tipo[(self.emp, '08')]
        self.assertEqual((alta.fecha, alta.sdi, alta.origen), (date(2026, 9, 23), 427.12, 'alta'))
        mov_baja = por_tipo[(baja, '02')]
        self.assertEqual((mov_baja.fecha, mov_baja.causa_baja, mov_baja.origen), (date(2026, 9, 22), '3', 'baja'))
        w.action_detectar()   # segunda corrida: nada se duplica
        self.assertEqual(self.Mov.search_count([('employee_id', 'in', (self.emp | baja).ids)]), len(movs))

    # --- bimestre ---------------------------------------------------------
    def test_sdi_bimestre_sin_variables(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_nomina.uma_diaria', '117.31')
        self.version._qb_imss_escribir_sdi(400.0)
        w = self.env['qb.nomina.imss.sdi.bimestre'].create({'anio': 2026, 'bimestre': '5'})
        w.action_calcular()
        linea = w.linea_ids.filtered(lambda l: l.employee_id == self.emp)
        self.assertEqual(len(linea), 1)
        self.assertEqual(linea.factor, 1.0521)            # primer año de servicio al 1-nov-2026
        self.assertEqual(linea.sdi_nuevo, round(394.76 * 1.0521, 2))
        self.assertEqual(w.tope, round(25 * 117.31, 2))
        self.assertTrue(linea.aplicar)
        w.action_aplicar()
        self.assertEqual(self.version._qb_imss_sdi(), linea.sdi_nuevo)
        mov = self.Mov.search([('employee_id', '=', self.emp.id), ('tipo', '=', '07')])
        self.assertEqual((mov.fecha, mov.sdi, mov.origen), (date(2026, 11, 1), linea.sdi_nuevo, 'bimestre'))
