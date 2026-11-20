# -*- coding: utf-8 -*-
"""Asistente «Reportes de nómina (NOI)» (1.23.0).

Saca, por lote (corrida de nómina) o por periodo, los dos reportes de NOI que
RH usa para revisar y cerrar la nómina:

* **Reporte de la nómina**: un bloque por recibo con los datos del trabajador
  (RFC, CURP, NSS, puesto, días, salario diario, SDI) y sus percepciones y
  deducciones con clave NOI y clave SAT; al final el resumen por concepto con
  gravado y exento, total gravable y neto. El previo (recibos en borrador) y el
  final (validados) son el mismo reporte.
* **Percepción/deducción**: por concepto, los trabajadores con su importe y el
  total del concepto; se pueden elegir los conceptos o sacar todos.

Ambos en PDF o en Excel. Sólo lee: no recalcula ni valida recibos."""
import base64
import io

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from ..models.reporte_calculo import (clave_empleado_orden, conceptos_de_recibo, por_concepto, resumen,
                                      totales_de_recibo)


class NominaReporte(models.TransientModel):
    _name = 'qb.nomina.reporte'
    _description = "Reportes de nómina (formato NOI)"

    modo = fields.Selection([('lote', "Por lote"), ('periodo', "Por periodo")], string="Buscar",
                            required=True, default='lote')
    payslip_run_ids = fields.Many2many('hr.payslip.run', string="Lotes de nómina")
    date_from = fields.Date(string="Desde")
    date_to = fields.Date(string="Hasta")
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    reporte = fields.Selection([('nomina', "Reporte de la nómina"),
                                ('percepcion_deduccion', "Percepción/deducción")],
                               string="Reporte", required=True, default='nomina')
    concepto_ids = fields.Many2many(
        'qb.nomina.concepto.noi', string="Conceptos",
        help="Vacío: todos, incluidas las reglas del recibo que no están en el catálogo.")
    formato = fields.Selection([('pdf', "PDF"), ('xlsx', "Excel")], string="Formato", required=True, default='pdf')
    incluir_borrador = fields.Boolean(
        string="Incluir recibos en borrador", default=True,
        help="El previo de la nómina se revisa con los recibos en borrador; el final, sólo con los validados.")
    archivo = fields.Binary(readonly=True, attachment=False)
    archivo_nombre = fields.Char(readonly=True)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if self.env.context.get('active_model') == 'hr.payslip.run' and self.env.context.get('active_ids'):
            res['payslip_run_ids'] = [(6, 0, self.env.context['active_ids'])]
            res['modo'] = 'lote'
        return res

    # --- recibos ------------------------------------------------------------
    def _recibos(self):
        self.ensure_one()
        estados = ['validated', 'paid'] + (['draft'] if self.incluir_borrador else [])
        dominio = [('company_id', '=', self.company_id.id), ('state', 'in', estados)]
        if self.modo == 'lote':
            if not self.payslip_run_ids:
                raise UserError(_("Elija al menos un lote de nómina."))
            dominio.append(('payslip_run_id', 'in', self.payslip_run_ids.ids))
        else:
            if not (self.date_from and self.date_to):
                raise UserError(_("Ponga las fechas del periodo."))
            dominio += [('date_from', '>=', self.date_from), ('date_to', '<=', self.date_to)]
        slips = self.env['hr.payslip'].search(dominio)
        if not slips:
            raise UserError(_("No hay recibos con esos filtros."))
        return slips.sorted(lambda s: (s.date_from, s.payslip_run_id.id or 0,
                                       clave_empleado_orden(s.employee_id.registration_number)))

    @staticmethod
    def _lineas(slip):
        lineas = {}
        for line in slip.line_ids:
            regla = line.salary_rule_id
            concepto = regla.l10n_mx_concept if 'l10n_mx_concept' in regla._fields else False
            acc = lineas.setdefault(line.code, {
                'total': 0.0, 'nombre': regla.name or line.name, 'en_recibo': regla.appears_on_payslip,
                'cfdi_tipo': concepto.cfdi_type if concepto else None,
                'sat': concepto.sat_code if concepto else '',
                'gravable': concepto.is_taxable if concepto else True,
            })
            acc['total'] += line.total
        return lineas

    def _empleado(self, slip, lineas):
        emp, version = slip.employee_id, slip.version_id
        sd = slip.l10n_mx_daily_salary if 'l10n_mx_daily_salary' in slip._fields else 0.0
        horas_dia = (version.resource_calendar_id.hours_per_day or 8.0) if version else 8.0
        registro = ''
        if version and 'l10n_mx_employer_registration' in version._fields:
            registro = version.l10n_mx_employer_registration or ''
        return {
            'id': emp.id,
            'clave': emp.registration_number or '',
            'nombre': emp.name,
            'rfc': emp.l10n_mx_rfc or '',
            'curp': emp.l10n_mx_curp or '',
            'nss': emp.ssnid or '',
            'fecha_alta': (version.contract_date_start or version.date_start) if version else False,
            'puesto': slip.job_id.name or emp.job_title or '',
            'departamento': slip.department_id.name or '',
            # La selección de schedule_pay es una función en Odoo 19: se lee ya resuelta.
            'periodicidad': dict(version._fields['schedule_pay']._description_selection(self.env)).get(
                version.schedule_pay, '') if version else '',
            'sueldo': version.wage if version else 0.0,
            'salario_diario': sd,
            'horas_dia': horas_dia,
            'salario_hora': round(sd / horas_dia, 2) if horas_dia else 0.0,
            'sdi': lineas.get('INT_DAY_WAGE', {}).get('total', 0.0),
            'dias': lineas.get('DIAS_IMSS', {}).get('total', 0.0),
            'registro_patronal': registro or slip.company_id.l10n_mx_imss_id or '',
        }

    def _qb_datos(self):
        """Todo lo que imprimen el PDF y el Excel."""
        self.ensure_one()
        slips = self._recibos()
        conceptos = self.env['qb.nomina.concepto.noi'].search([])._qb_para_calculo()
        recibos = []
        for slip in slips:
            lineas = self._lineas(slip)
            renglones = conceptos_de_recibo(lineas, conceptos)
            neto = lineas.get('NET', {}).get('total')
            recibos.append({
                'slip': slip,
                'empleado': self._empleado(slip, lineas),
                'periodo': (slip.date_from, slip.date_to),
                'lote': slip.payslip_run_id.name or '',
                'renglones': renglones,
                'percepciones': [r for r in renglones if r['tipo'] == 'percepcion'],
                'deducciones': [r for r in renglones if r['tipo'] == 'deduccion'],
                'totales': totales_de_recibo(renglones, neto),
            })
        llaves = None
        if self.concepto_ids:
            llaves = {c.clave or c.name for c in self.concepto_ids}
        fechas = [r['periodo'] for r in recibos]
        company = self.company_id
        return {
            'empresa': company.name,
            'rfc': company.vat or '',
            'registro_patronal': company.l10n_mx_imss_id or '',
            'desde': min(f[0] for f in fechas),
            'hasta': max(f[1] for f in fechas),
            'lotes': ', '.join(sorted({r['lote'] for r in recibos if r['lote']})),
            'recibos': recibos,
            'resumen': resumen(recibos),
            'por_concepto': por_concepto(recibos, llaves),
            'con_diferencia': [r for r in recibos if abs(r['totales']['diferencia']) >= 0.01],
            'impreso': fields.Datetime.context_timestamp(self, fields.Datetime.now()),
            'usuario': self.env.user.name,
        }

    @staticmethod
    def qb_dinero(valor):
        return '{:,.2f}'.format(valor or 0.0)

    @staticmethod
    def qb_fecha(valor):
        return valor.strftime('%d/%m/%Y') if valor else ''

    # --- acciones -----------------------------------------------------------
    def action_generar(self):
        self.ensure_one()
        self._recibos()  # valida filtros antes de abrir el PDF
        if self.formato == 'pdf':
            xmlid = ('quimibond_nomina.action_report_qb_nomina_noi' if self.reporte == 'nomina'
                     else 'quimibond_nomina.action_report_qb_percepcion_deduccion')
            return self.env.ref(xmlid).report_action(self)
        datos = self._qb_datos()
        contenido = (self._xlsx_nomina(datos) if self.reporte == 'nomina' else self._xlsx_percepcion_deduccion(datos))
        nombre = '%s %s al %s.xlsx' % (
            'Reporte de la nomina' if self.reporte == 'nomina' else 'Percepcion deduccion',
            datos['desde'].strftime('%Y-%m-%d'), datos['hasta'].strftime('%Y-%m-%d'))
        self.write({'archivo': base64.b64encode(contenido), 'archivo_nombre': nombre})
        return {
            'type': 'ir.actions.act_url',
            'url': '/web/content/?model=%s&id=%d&field=archivo&filename_field=archivo_nombre&download=true'
                   % (self._name, self.id),
            'target': 'self',
        }

    def _nombre_reporte(self):
        self.ensure_one()
        etiqueta = dict(self._fields['reporte']._description_selection(self.env))[self.reporte]
        if self.modo == 'lote' and len(self.payslip_run_ids) == 1:
            return '%s - %s' % (etiqueta, self.payslip_run_ids.name)
        return etiqueta

    # --- Excel --------------------------------------------------------------
    @staticmethod
    def _libro():
        import xlsxwriter
        salida = io.BytesIO()
        libro = xlsxwriter.Workbook(salida, {'in_memory': True})
        fmt = {
            'titulo': libro.add_format({'bold': True, 'font_size': 12}),
            'enc': libro.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'text_wrap': True,
                                     'valign': 'vcenter'}),
            'dinero': libro.add_format({'num_format': '#,##0.00'}),
            'dinero_tot': libro.add_format({'num_format': '#,##0.00', 'bold': True, 'top': 1}),
            'tot': libro.add_format({'bold': True, 'top': 1}),
            'fecha': libro.add_format({'num_format': 'dd/mm/yyyy'}),
            'num': libro.add_format({'num_format': '0.00'}),
        }
        return salida, libro, fmt

    def _xlsx_encabezado(self, hoja, fmt, datos, titulo):
        hoja.write(0, 0, datos['empresa'], fmt['titulo'])
        hoja.write(1, 0, '%s   RFC: %s   Registro patronal: %s' % (titulo, datos['rfc'], datos['registro_patronal']))
        hoja.write(2, 0, 'Periodo: %s al %s   %s' % (self.qb_fecha(datos['desde']), self.qb_fecha(datos['hasta']),
                                                     datos['lotes']))
        return 4

    def _xlsx_nomina(self, datos):
        salida, libro, fmt = self._libro()
        res = datos['resumen']
        cols_p = res['percepciones']
        cols_d = res['deducciones']
        cols_i = res['informativos']

        hoja = libro.add_worksheet('Nomina')
        fila = self._xlsx_encabezado(hoja, fmt, datos, 'Reporte de la nómina')
        fijos = ['Clave', 'Nombre del trabajador', 'RFC', 'CURP', 'N.S.S.', 'Fecha de alta', 'Puesto',
                 'Departamento', 'Periodo', 'Sueldo', 'Salario diario', 'SDI', 'Días trabajados']
        enc = (fijos + ['%s %s' % (c['clave'], c['nombre']) for c in cols_p] + ['Total percepciones']
               + ['%s %s' % (c['clave'], c['nombre']) for c in cols_d] + ['Total deducciones', 'Neto pagado']
               + ['%s %s' % (c['clave'], c['nombre']) for c in cols_i] + ['Total gravado', 'Total exento'])
        for j, texto in enumerate(enc):
            hoja.write(fila, j, texto.strip(), fmt['enc'])
        hoja.set_row(fila, 45)
        hoja.freeze_panes(fila + 1, 2)
        for rec in datos['recibos']:
            fila += 1
            e, t = rec['empleado'], rec['totales']
            por_llave = {r['llave']: r['importe'] for r in rec['renglones']}
            valores = [e['clave'], e['nombre'], e['rfc'], e['curp'], e['nss'], e['fecha_alta'], e['puesto'],
                       e['departamento'], '%s al %s' % (self.qb_fecha(rec['periodo'][0]),
                                                        self.qb_fecha(rec['periodo'][1])),
                       e['sueldo'], e['salario_diario'], e['sdi'], e['dias']]
            valores += [por_llave.get(c['llave'], 0.0) for c in cols_p] + [t['percepciones']]
            valores += [por_llave.get(c['llave'], 0.0) for c in cols_d] + [t['deducciones'], t['neto']]
            valores += [por_llave.get(c['llave'], 0.0) for c in cols_i] + [t['gravado'], t['exento']]
            for j, v in enumerate(valores):
                if j == 5:
                    if v:
                        hoja.write_datetime(fila, j, v, fmt['fecha'])
                elif j == 12:
                    hoja.write_number(fila, j, v or 0.0, fmt['num'])
                elif j >= 9:
                    hoja.write_number(fila, j, v or 0.0, fmt['dinero'])
                else:
                    hoja.write(fila, j, v or '')
        fila += 1
        hoja.write(fila, 1, 'Total (%d recibos)' % len(datos['recibos']), fmt['tot'])
        for j in range(len(fijos), len(enc)):
            col = xlsx_col(j)
            hoja.write_formula(fila, j, '=SUM(%s%d:%s%d)' % (col, 6, col, fila), fmt['dinero_tot'])
        hoja.set_column(0, 0, 8)
        hoja.set_column(1, 1, 36)
        hoja.set_column(2, 8, 16)
        hoja.set_column(9, len(enc), 13)

        self._xlsx_resumen(libro, fmt, datos)
        libro.close()
        return salida.getvalue()

    def _xlsx_resumen(self, libro, fmt, datos):
        res = datos['resumen']
        hoja = libro.add_worksheet('Resumen')
        fila = self._xlsx_encabezado(hoja, fmt, datos, 'Resumen por concepto')
        for j, texto in enumerate(['Tipo', 'Clave NOI', 'Clave SAT', 'Descripción', 'Importe', 'Importe gravado',
                                   'Importe exento', 'Trabajadores']):
            hoja.write(fila, j, texto, fmt['enc'])
        etiquetas = {'percepcion': 'Percepción', 'deduccion': 'Deducción', 'informativo': 'Informativo'}
        for f in res['percepciones'] + res['deducciones'] + res['informativos']:
            fila += 1
            hoja.write_row(fila, 0, [etiquetas[f['tipo']], f['clave'], f['sat'], f['nombre']])
            hoja.write_number(fila, 4, f['importe'], fmt['dinero'])
            if f['tipo'] == 'percepcion':
                hoja.write_number(fila, 5, f['gravado'], fmt['dinero'])
                hoja.write_number(fila, 6, f['exento'], fmt['dinero'])
            hoja.write_number(fila, 7, f['trabajadores'])
        fila += 2
        for texto, valor in [('Total de percepciones', res['total_percepciones']),
                             ('Total gravable', res['total_gravado']), ('Total exento', res['total_exento']),
                             ('Total de deducciones', res['total_deducciones']), ('Neto pagado', res['neto']),
                             ('Subsidio para el empleo aplicado', res['total_informativo'])]:
            hoja.write(fila, 3, texto, fmt['tot'])
            hoja.write_number(fila, 4, valor, fmt['dinero_tot'])
            fila += 1
        hoja.write(fila, 3, 'Recibos')
        hoja.write_number(fila, 4, res['recibos'])
        if res['diferencia']:
            fila += 1
            hoja.write(fila, 3, 'Diferencia neto del recibo contra percepciones − deducciones (revisar catálogo)')
            hoja.write_number(fila, 4, res['diferencia'], fmt['dinero'])
        hoja.set_column(0, 2, 11)
        hoja.set_column(3, 3, 40)
        hoja.set_column(4, 7, 15)

    def _xlsx_percepcion_deduccion(self, datos):
        salida, libro, fmt = self._libro()
        hoja = libro.add_worksheet('Percepcion deduccion')
        fila = self._xlsx_encabezado(hoja, fmt, datos, 'Percepción/deducción')
        for j, texto in enumerate(['Tipo', 'Clave NOI', 'Clave SAT', 'Concepto', 'Clave', 'Nombre del trabajador',
                                   'Percepciones', 'Deducciones']):
            hoja.write(fila, j, texto, fmt['enc'])
        hoja.autofilter(fila, 0, fila, 7)
        hoja.freeze_panes(fila + 1, 0)
        etiquetas = {'percepcion': 'Percepción', 'deduccion': 'Deducción', 'informativo': 'Deducción'}
        for c in datos['por_concepto']:
            col = 6 if c['tipo'] == 'percepcion' else 7
            for e in c['empleados']:
                fila += 1
                hoja.write_row(fila, 0, [etiquetas[c['tipo']], c['clave'], c['sat'], c['nombre'], e['clave'],
                                         e['nombre']])
                hoja.write_number(fila, col, e['importe'], fmt['dinero'])
        hoja.set_column(0, 2, 11)
        hoja.set_column(3, 3, 30)
        hoja.set_column(4, 4, 8)
        hoja.set_column(5, 5, 36)
        hoja.set_column(6, 7, 14)

        tot = libro.add_worksheet('Totales por concepto')
        fila = self._xlsx_encabezado(tot, fmt, datos, 'Percepción/deducción: totales')
        for j, texto in enumerate(['Tipo', 'Clave NOI', 'Clave SAT', 'Concepto', 'Trabajadores', 'Percepciones',
                                   'Deducciones']):
            tot.write(fila, j, texto, fmt['enc'])
        for c in datos['por_concepto']:
            fila += 1
            tot.write_row(fila, 0, [etiquetas[c['tipo']], c['clave'], c['sat'], c['nombre'], len(c['empleados'])])
            tot.write_number(fila, 5 if c['tipo'] == 'percepcion' else 6, c['total'], fmt['dinero'])
        tot.set_column(0, 2, 11)
        tot.set_column(3, 3, 30)
        tot.set_column(4, 6, 14)
        libro.close()
        return salida.getvalue()


def xlsx_col(indice):
    """0 → A, 25 → Z, 26 → AA."""
    texto = ''
    indice += 1
    while indice:
        indice, resto = divmod(indice - 1, 26)
        texto = chr(65 + resto) + texto
    return texto
