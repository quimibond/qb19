# -*- coding: utf-8 -*-
import base64
import io

from odoo import fields, models
from odoo.exceptions import UserError

from ._tabular_import_utils import build_header_index, clean_num, row_is_empty

try:
    import openpyxl
except ImportError:
    openpyxl = None


HEADER_MAP = {
    'articulo': 'articulo',
    'articulo acabado': 'articulo',
    'revision': 'revision',
    'articulo tejido': 'tejido_ref',
    'codigo tejido': 'tejido_ref',
    'base tejido': 'tejido_ref',
    'producto acabado': 'product_acabado_ref',
    'referencia producto acabado': 'product_acabado_ref',
    'codigo producto acabado': 'product_acabado_ref',
    'rendimiento': 'rendimiento_tela_acabada',
    'rendimiento mts kg': 'rendimiento_tela_acabada',
    'rendimiento tela acabada': 'rendimiento_tela_acabada',
    'peso': 'peso_acabado',
    'peso acabado': 'peso_acabado',
    'peso tolerancia': 'peso_acabado_tol',
    'peso tolerancia unidad': 'peso_acabado_tol_unit',
    'ancho': 'ancho_acabado',
    'ancho acabado': 'ancho_acabado',
    'ancho tolerancia': 'ancho_acabado_tol',
    'ancho tolerancia unidad': 'ancho_acabado_tol_unit',
    'encogimiento a lo largo': 'encogimiento_largo',
    'encogimiento largo': 'encogimiento_largo',
    'encogimiento a lo largo tolerancia': 'encogimiento_largo_tol',
    'encogimiento largo tolerancia': 'encogimiento_largo_tol',
    'encogimiento largo tolerancia unidad': 'encogimiento_largo_tol_unit',
    'encogimiento a lo ancho': 'encogimiento_ancho',
    'encogimiento ancho': 'encogimiento_ancho',
    'encogimiento a lo ancho tolerancia': 'encogimiento_ancho_tol',
    'encogimiento ancho tolerancia': 'encogimiento_ancho_tol',
    'encogimiento ancho tolerancia unidad': 'encogimiento_ancho_tol_unit',
    'espesor': 'espesor_acabado',
    'espesor acabado': 'espesor_acabado',
    'espesor tolerancia': 'espesor_acabado_tol',
    'espesor tolerancia unidad': 'espesor_acabado_tol_unit',
    'elongacion largo': 'elongacion_largo_acabado',
    'elongacion largo tolerancia': 'elongacion_largo_acabado_tol',
    'elongacion largo tolerancia unidad': 'elongacion_largo_acabado_tol_unit',
    'elongacion ancho': 'elongacion_ancho_acabado',
    'elongacion ancho tolerancia': 'elongacion_ancho_acabado_tol',
    'elongacion ancho tolerancia unidad': 'elongacion_ancho_acabado_tol_unit',
    'notas': 'notas',
}

FLOAT_FIELDS = {
    'rendimiento_tela_acabada', 'peso_acabado', 'ancho_acabado', 'espesor_acabado',
    'encogimiento_largo', 'encogimiento_ancho',
    'elongacion_largo_acabado', 'elongacion_ancho_acabado',
}
# Campos de texto (tolerancia y unidad) se pasan tal cual, sin conversión numérica.


class FichaTecnicaAcabadoImportWizard(models.TransientModel):
    _name = 'ficha.tecnica.acabado.import.wizard'
    _description = 'Importar Fichas Técnicas de Acabado desde Excel (masivo)'

    file_data = fields.Binary(string='Archivo Excel (.xlsx)', required=True)
    file_name = fields.Char(string='Nombre de archivo')
    update_if_exists = fields.Boolean(
        string='Actualizar si ya existe (mismo producto de acabado)', default=True)

    def action_import(self):
        self.ensure_one()
        if openpyxl is None:
            raise UserError(
                'La librería openpyxl no está disponible en este servidor. '
                'Solicite a su administrador que la instale.')

        try:
            wb = openpyxl.load_workbook(
                io.BytesIO(base64.b64decode(self.file_data)), data_only=True)
        except Exception as exc:
            raise UserError('No se pudo leer el archivo. Verifique que sea un '
                             '.xlsx válido. Detalle: %s' % exc)

        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            raise UserError('El archivo está vacío.')

        header_row, data_rows = rows[0], rows[1:]
        col_index = build_header_index(header_row, HEADER_MAP)
        required_present = {'articulo', 'tejido_ref', 'product_acabado_ref'}
        if not required_present.issubset(set(col_index.values())):
            faltantes = required_present - set(col_index.values())
            raise UserError(
                'Faltan columnas obligatorias en el archivo: %s. Se requieren '
                'al menos "Artículo", "Artículo Tejido" (base) y '
                '"Producto Acabado" (referencia del producto).' % ', '.join(sorted(faltantes)))

        FichaAcabado = self.env['ficha.tecnica.acabado']
        FichaTejido = self.env['ficha.tecnica.tejido']
        Product = self.env['product.product']

        created, updated, errors = 0, 0, []

        for row_num, row in enumerate(data_rows, start=2):
            if row_is_empty(row):
                continue
            raw = {}
            for idx, field_name in col_index.items():
                raw[field_name] = row[idx] if idx < len(row) else None

            articulo = (str(raw.get('articulo')).strip()
                        if raw.get('articulo') else '')
            if not articulo:
                errors.append('Fila %s: sin artículo, se omitió.' % row_num)
                continue

            try:
                tejido_ref = str(raw.get('tejido_ref') or '').strip()
                tejido = FichaTejido.search([('articulo', '=', tejido_ref)], limit=1)
                if not tejido:
                    errors.append(
                        'Fila %s (%s): no se encontró ficha de tejido con '
                        'artículo "%s". Se omitió la fila.' % (row_num, articulo, tejido_ref))
                    continue

                product_ref = str(raw.get('product_acabado_ref') or '').strip()
                product = Product.search(
                    ['|', ('default_code', '=', product_ref), ('name', '=', product_ref)],
                    limit=1)
                if not product:
                    errors.append(
                        'Fila %s (%s): no se encontró producto de tela acabada '
                        '"%s". Se omitió la fila.' % (row_num, articulo, product_ref))
                    continue

                values = {
                    'articulo': articulo,
                    'revision': (raw.get('revision') or '0'),
                    'tejido_id': tejido.id,
                    'product_acabado_id': product.id,
                }
                for field_name, cell_value in raw.items():
                    if field_name in ('articulo', 'revision', 'tejido_ref', 'product_acabado_ref'):
                        continue
                    if field_name in FLOAT_FIELDS:
                        values[field_name] = clean_num(cell_value)
                    else:
                        values[field_name] = cell_value

                existing = FichaAcabado.search(
                    [('product_acabado_id', '=', product.id)], limit=1)

                if existing:
                    if not self.update_if_exists:
                        errors.append(
                            'Fila %s (%s): ya existe ficha de acabado para ese '
                            'producto y "Actualizar si ya existe" está '
                            'desmarcado, se omitió.' % (row_num, articulo))
                        continue
                    existing.write(values)
                    updated += 1
                else:
                    FichaAcabado.create(values)
                    created += 1

            except Exception as exc:
                errors.append('Fila %s (%s): error inesperado — %s' % (row_num, articulo, exc))

        message = 'Fichas de acabado creadas: %s. Actualizadas: %s.' % (created, updated)
        if errors:
            message += '\n\nAvisos (%s):\n' % len(errors) + '\n'.join(errors[:30])
            if len(errors) > 30:
                message += '\n... y %s más.' % (len(errors) - 30)

        list_action = self.env.ref(
            'quimibond_ficha_tecnica_tela.action_ficha_tecnica_acabado').read()[0]

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Importación de Fichas Técnicas de Acabado',
                'message': message,
                'sticky': bool(errors),
                'type': 'warning' if errors else 'success',
                'next': list_action,
            },
        }
