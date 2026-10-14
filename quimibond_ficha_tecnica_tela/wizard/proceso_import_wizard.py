# -*- coding: utf-8 -*-
"""2.8.0: importar procesos del artículo en la máquina desde Excel.

Una fila por artículo y máquina. Columnas «Artículo» y «Máquina» (código o
nombre del centro de trabajo); las demás se cotejan con los parámetros «Del
artículo en la máquina» del tipo de esa máquina, por clave o por nombre (sin
importar acentos, mayúsculas ni la unidad entre paréntesis). Columnas
opcionales: «Fecha», «Fuente», «Notas». Una celda vacía no borra lo que ya
estaba: lo no dado queda vacío.
"""
import base64
import datetime
import io

from odoo import fields, models
from odoo.exceptions import UserError

from ._tabular_import_utils import clean_num, normalize_header, row_is_empty

try:
    import openpyxl
except ImportError:
    openpyxl = None

FIXED = {
    'articulo': 'articulo', 'codigo': 'articulo', 'codigo acabado': 'articulo', 'articulo acabado': 'articulo',
    'maquina': 'maquina', 'equipo': 'maquina', 'centro de trabajo': 'maquina',
    'fecha': 'fecha', 'fecha del dato': 'fecha',
    'fuente': 'fuente',
    'notas': 'notas',
}


def _strip_unit(header):
    """«Velocidad (m/min)» → «Velocidad»."""
    text = str(header or '')
    return text.split('(', 1)[0] if '(' in text else text


class FichaTecnicaProcesoImportWizard(models.TransientModel):
    _name = 'ficha.tecnica.proceso.import.wizard'
    _description = 'Importar procesos del artículo en la máquina desde Excel'

    file_data = fields.Binary(string='Archivo Excel (.xlsx)', required=True)
    file_name = fields.Char(string='Nombre de archivo')
    update_if_exists = fields.Boolean(string='Actualizar si ya existe (mismo artículo y máquina)', default=True)
    fuente = fields.Char(string='Fuente', help='Se pone en los procesos que no traigan su propia columna «Fuente» '
                                               '(p. ej. «Hoja RAMAS 1,2 y 3, Producción, 2026-10-14»).')

    def _find_workcenter(self, ref, cache):
        key = str(ref or '').strip()
        if key not in cache:
            Workcenter = self.env['mrp.workcenter']
            found = Workcenter.search([('code', '=ilike', key), ('ft_tipo_id', '!=', False)], limit=2)
            if len(found) != 1:
                found = Workcenter.search([('name', '=ilike', key), ('ft_tipo_id', '!=', False)], limit=2)
            cache[key] = found if len(found) == 1 else Workcenter
        return cache[key]

    def action_import(self):
        self.ensure_one()
        if openpyxl is None:
            raise UserError('La librería openpyxl no está disponible en este servidor.')
        try:
            wb = openpyxl.load_workbook(io.BytesIO(base64.b64decode(self.file_data)), data_only=True)
        except Exception as exc:
            raise UserError('No se pudo leer el archivo. Verifique que sea un .xlsx válido. Detalle: %s' % exc)
        rows = list(wb.active.iter_rows(values_only=True))
        if not rows:
            raise UserError('El archivo está vacío.')
        return self._import_rows(rows[0], rows[1:])

    def _import_rows(self, header_row, data_rows):
        """Importa filas ya leídas (encabezado + datos); regresa la notificación."""
        fixed, others = {}, {}
        for idx, cell in enumerate(header_row):
            key = normalize_header(cell)
            if key in FIXED:
                fixed[FIXED[key]] = idx
            elif key:
                others[idx] = (key, normalize_header(_strip_unit(cell)))
        if not {'articulo', 'maquina'} <= set(fixed):
            raise UserError('El archivo necesita las columnas «Artículo» y «Máquina».')

        Proceso = self.env['ficha.tecnica.proceso']
        cache, created, updated, errors, unmatched = {}, 0, 0, [], set()

        def cell(row, idx):
            return row[idx] if idx is not None and idx < len(row) else None

        for row_num, row in enumerate(data_rows, start=2):
            if row_is_empty(row):
                continue
            articulo = str(cell(row, fixed['articulo']) or '').strip()
            maquina = cell(row, fixed['maquina'])
            if not articulo:
                errors.append('Fila %s: sin artículo, se omitió.' % row_num)
                continue
            workcenter = self._find_workcenter(maquina, cache)
            if not workcenter:
                errors.append('Fila %s (%s): no hay una sola máquina con ficha llamada «%s».' % (
                    row_num, articulo, maquina or ''))
                continue
            params = {}
            for param in workcenter.ft_tipo_id._ft_parametros('articulo'):
                params[normalize_header(param.code)] = param
                params.setdefault(normalize_header(param.name), param)
            values = {}
            for idx, (key, key_sin_unidad) in others.items():
                param = params.get(key) or params.get(key_sin_unidad)
                if not param:
                    unmatched.add(str(header_row[idx]))
                    continue
                raw = cell(row, idx)
                if raw is None or str(raw).strip() == '':
                    continue
                values[param.code] = str(raw).strip() if param.kind == 'text' else clean_num(raw)
            head = {}
            fecha = cell(row, fixed.get('fecha'))
            if isinstance(fecha, (datetime.date, datetime.datetime)):
                head['fecha'] = fecha
            fuente = str(cell(row, fixed.get('fuente')) or '').strip() or self.fuente
            if fuente:
                head['fuente'] = fuente
            notas = str(cell(row, fixed.get('notas')) or '').strip()
            if notas:
                head['notas'] = notas
            try:
                with self.env.cr.savepoint():
                    proceso = Proceso.with_context(active_test=False).search(
                        [('articulo', '=', articulo), ('workcenter_id', '=', workcenter.id)], limit=1)
                    if proceso:
                        if not self.update_if_exists:
                            errors.append('Fila %s (%s, %s): ya existe y «Actualizar» está desmarcado.' % (
                                row_num, articulo, workcenter.name))
                            continue
                        proceso.write(dict(head, active=True))
                        updated += 1
                    else:
                        proceso = Proceso.create(dict(head, articulo=articulo, workcenter_id=workcenter.id))
                        created += 1
                    for code, value in values.items():
                        proceso._ft_set_value(code, value)
            except Exception as exc:
                errors.append('Fila %s (%s): %s' % (row_num, articulo, exc))

        unmatched -= {''}
        if unmatched:
            errors.insert(0, 'Columnas que no son parámetros del artículo de esas máquinas (se ignoraron): %s.'
                          % ', '.join(sorted(unmatched)))
        message = 'Procesos creados: %s. Actualizados: %s.' % (created, updated)
        if errors:
            message += '\n\nAvisos (%s):\n' % len(errors) + '\n'.join(errors[:30])
            if len(errors) > 30:
                message += '\n... y %s más.' % (len(errors) - 30)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Importación de procesos por artículo',
                'message': message,
                'sticky': bool(errors),
                'type': 'warning' if errors else 'success',
                'next': self.env.ref('quimibond_ficha_tecnica_tela.action_ficha_tecnica_proceso').read()[0],
            },
        }
