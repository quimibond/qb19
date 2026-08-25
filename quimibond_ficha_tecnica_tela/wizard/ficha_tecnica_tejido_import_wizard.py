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


# Alias de encabezado (ya normalizados: minúsculas, sin acentos, un solo
# espacio) -> nombre técnico de campo. Se pueden agregar más alias por
# columna sin romper nada; los encabezados que no matcheen se ignoran.
HEADER_MAP = {
    'articulo': 'articulo',
    'revision': 'revision',
    'producto proceso': 'product_proceso_ref',
    'producto en proceso': 'product_proceso_ref',
    'referencia producto proceso': 'product_proceso_ref',
    'maquina': 'maquina_tejido',
    'marca maquina': 'marca_maquina',
    'galga': 'galga',
    'diametro': 'diametro',
    'no agujas': 'no_agujas',
    'no de agujas': 'no_agujas',
    'no alimentadores': 'no_alimentadores',
    'no de alimentadores': 'no_alimentadores',
    'velocidad': 'velocidad',
    'velocidad rpm min': 'velocidad',
    'vueltas por rollo': 'vueltas_por_rollo',
    # Tabla 1 — por polea/hilo
    'longitud malla polea1': 'longitud_malla_polea1',
    'longitud malla polea2': 'longitud_malla_polea2',
    'longitud malla tolerancia': 'longitud_malla_tol',
    'longitud malla tolerancia unidad': 'longitud_malla_tol_unit',
    'consumo cm vta polea1': 'consumo_cm_vta_polea1',
    'consumo cm vta polea2': 'consumo_cm_vta_polea2',
    'consumo cm vta tolerancia': 'consumo_cm_vta_tol',
    'consumo cm vta tolerancia unidad': 'consumo_cm_vta_tol_unit',
    'polea alimentacion polea1': 'polea_alimentacion_polea1',
    'polea alimentacion polea2': 'polea_alimentacion_polea2',
    'polea alimentacion tolerancia': 'polea_alimentacion_tol',
    'polea alimentacion tolerancia unidad': 'polea_alimentacion_tol_unit',
    # Tabla 2 — datos generales
    'tension': 'tension',
    'tension tolerancia': 'tension_tol',
    'tension tolerancia unidad': 'tension_tol_unit',
    'punto cilindro': 'punto_cilindro',
    'punto cilindro tolerancia': 'punto_cilindro_tol',
    'punto cilindro tolerancia unidad': 'punto_cilindro_tol_unit',
    'punto plato': 'punto_plato',
    'punto plato tolerancia': 'punto_plato_tol',
    'punto plato tolerancia unidad': 'punto_plato_tol_unit',
    'altura plato': 'altura_plato',
    'altura plato tolerancia': 'altura_plato_tol',
    'altura plato tolerancia unidad': 'altura_plato_tol_unit',
    'ancho bastidor': 'ancho_bastidor',
    'ancho bastidor tolerancia': 'ancho_bastidor_tol',
    'ancho bastidor tolerancia unidad': 'ancho_bastidor_tol_unit',
    'estiraje': 'estiraje',
    'estiraje tolerancia': 'estiraje_tol',
    'estiraje tolerancia unidad': 'estiraje_tol_unit',
    'ancho rollo': 'ancho_rollo',
    'ancho rollo tolerancia': 'ancho_rollo_tol',
    'ancho rollo tolerancia unidad': 'ancho_rollo_tol_unit',
    'peso promedio rollo': 'peso_promedio_rollo',
    'peso promedio rollo tolerancia': 'peso_promedio_rollo_tol',
    'peso promedio rollo tolerancia unidad': 'peso_promedio_rollo_tol_unit',
    # Tela acondicionada
    'peso acondicionado': 'peso_acondicionado',
    'peso acondicionado tolerancia': 'peso_acondicionado_tol',
    'peso acondicionado tolerancia unidad': 'peso_acondicionado_tol_unit',
    'ancho acondicionado': 'ancho_acondicionado',
    'ancho acondicionado tolerancia': 'ancho_acondicionado_tol',
    'ancho acondicionado tolerancia unidad': 'ancho_acondicionado_tol_unit',
    'espesor acondicionado': 'espesor_acondicionado',
    'espesor acondicionado tolerancia': 'espesor_acondicionado_tol',
    'espesor acondicionado tolerancia unidad': 'espesor_acondicionado_tol_unit',
    'columnas': 'columnas',
    'columnas tolerancia': 'columnas_tol',
    'columnas tolerancia unidad': 'columnas_tol_unit',
    'mallas': 'mallas',
    'mallas tolerancia': 'mallas_tol',
    'mallas tolerancia unidad': 'mallas_tol_unit',
    'elongacion carga largo': 'elongacion_carga_largo',
    'elongacion carga largo tolerancia': 'elongacion_carga_largo_tol',
    'elongacion carga largo tolerancia unidad': 'elongacion_carga_largo_tol_unit',
    'elongacion carga ancho': 'elongacion_carga_ancho',
    'elongacion carga ancho tolerancia': 'elongacion_carga_ancho_tol',
    'elongacion carga ancho tolerancia unidad': 'elongacion_carga_ancho_tol_unit',
    # Hilos (hasta 2)
    'hilo1 tipo': 'hilo1_tipo',
    'hilo1 titulo': 'hilo1_titulo',
    'hilo1 torsion': 'hilo1_torsion',
    'hilo1 pct': 'hilo1_pct',
    'hilo1 lote': 'hilo1_lote',
    'hilo1 proveedor': 'hilo1_proveedor',
    'hilo2 tipo': 'hilo2_tipo',
    'hilo2 titulo': 'hilo2_titulo',
    'hilo2 torsion': 'hilo2_torsion',
    'hilo2 pct': 'hilo2_pct',
    'hilo2 lote': 'hilo2_lote',
    'hilo2 proveedor': 'hilo2_proveedor',
    'notas': 'notas',
}

FLOAT_FIELDS = {
    'galga', 'velocidad',
    'longitud_malla_polea1', 'longitud_malla_polea2', 'longitud_malla_tol',
    'consumo_cm_vta_polea1', 'consumo_cm_vta_polea2', 'consumo_cm_vta_tol',
    'polea_alimentacion_polea1', 'polea_alimentacion_polea2', 'polea_alimentacion_tol',
    'ancho_bastidor', 'ancho_bastidor_tol',
    'ancho_rollo', 'ancho_rollo_tol',
    'peso_promedio_rollo', 'peso_promedio_rollo_tol',
    'peso_acondicionado', 'ancho_acondicionado', 'espesor_acondicionado',
    'elongacion_carga_largo', 'elongacion_carga_ancho',
    'hilo1_pct', 'hilo2_pct',
}
INT_FIELDS = {'no_agujas', 'no_alimentadores', 'vueltas_por_rollo', 'columnas', 'mallas'}


class FichaTecnicaTejidoImportWizard(models.TransientModel):
    _name = 'ficha.tecnica.tejido.import.wizard'
    _description = 'Importar Fichas Técnicas de Tejido desde Excel (masivo)'

    file_data = fields.Binary(string='Archivo Excel (.xlsx)', required=True)
    file_name = fields.Char(string='Nombre de archivo')
    update_if_exists = fields.Boolean(
        string='Actualizar si ya existe (mismo artículo + revisión)', default=True)

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
        if 'articulo' not in col_index.values():
            raise UserError(
                'No se encontró una columna de "Artículo" en el archivo. '
                'Verifique los encabezados de la primera fila.')

        Ficha = self.env['ficha.tecnica.tejido']
        Product = self.env['product.product']
        Partner = self.env['res.partner']

        created, updated, errors = 0, 0, []

        for row_num, row in enumerate(data_rows, start=2):
            if row_is_empty(row):
                continue
            raw = {}
            for idx, field_name in col_index.items():
                raw[field_name] = row[idx] if idx < len(row) else None

            articulo = (raw.get('articulo') or '').strip() if raw.get('articulo') else ''
            if not articulo:
                errors.append('Fila %s: sin artículo, se omitió.' % row_num)
                continue

            try:
                values = {'articulo': articulo, 'revision': (raw.get('revision') or '0')}
                for field_name, cell_value in raw.items():
                    if field_name in ('articulo', 'revision', 'product_proceso_ref') \
                            or field_name.startswith('hilo'):
                        continue
                    if field_name in FLOAT_FIELDS:
                        values[field_name] = clean_num(cell_value)
                    elif field_name in INT_FIELDS:
                        values[field_name] = int(clean_num(cell_value))
                    else:
                        values[field_name] = cell_value

                product_ref = raw.get('product_proceso_ref')
                if product_ref:
                    product = Product.search(
                        ['|', ('default_code', '=', str(product_ref).strip()),
                         ('name', '=', str(product_ref).strip())], limit=1)
                    if product:
                        values['product_proceso_id'] = product.id
                    else:
                        errors.append(
                            'Fila %s (%s): producto "%s" no encontrado, se '
                            'importó sin vincular producto.' % (row_num, articulo, product_ref))

                hilo_lines = []
                for n in (1, 2):
                    tipo = raw.get('hilo%d_tipo' % n)
                    if not tipo:
                        continue
                    hilo_vals = {
                        'numero': n,
                        'tipo_hilo': tipo,
                        'titulo_hilo': raw.get('hilo%d_titulo' % n),
                        'torsion': raw.get('hilo%d_torsion' % n),
                        'porcentaje': clean_num(raw.get('hilo%d_pct' % n)),
                        'lote': raw.get('hilo%d_lote' % n),
                    }
                    proveedor_ref = raw.get('hilo%d_proveedor' % n)
                    if proveedor_ref:
                        proveedor = Partner.search([
                            ('name', '=', str(proveedor_ref).strip()),
                            ('supplier_rank', '>', 0),
                        ], limit=1)
                        if proveedor:
                            hilo_vals['proveedor_id'] = proveedor.id
                        else:
                            errors.append(
                                'Fila %s (%s): proveedor "%s" no encontrado (o no está '
                                'marcado como proveedor) para hilo %s, se importó sin '
                                'vincular proveedor.' % (row_num, articulo, proveedor_ref, n))
                    hilo_lines.append((0, 0, hilo_vals))

                existing = Ficha.search([
                    ('articulo', '=', values['articulo']),
                    ('revision', '=', values['revision']),
                ], limit=1)

                if existing:
                    if not self.update_if_exists:
                        errors.append(
                            'Fila %s (%s): ya existe y "Actualizar si ya '
                            'existe" está desmarcado, se omitió.' % (row_num, articulo))
                        continue
                    if hilo_lines:
                        values['hilo_line_ids'] = [(5, 0, 0)] + hilo_lines
                    existing.write(values)
                    updated += 1
                else:
                    if hilo_lines:
                        values['hilo_line_ids'] = hilo_lines
                    Ficha.create(values)
                    created += 1

            except Exception as exc:
                errors.append('Fila %s (%s): error inesperado — %s' % (row_num, articulo, exc))

        message = 'Fichas de tejido creadas: %s. Actualizadas: %s.' % (created, updated)
        if errors:
            message += '\n\nAvisos (%s):\n' % len(errors) + '\n'.join(errors[:30])
            if len(errors) > 30:
                message += '\n... y %s más.' % (len(errors) - 30)

        list_action = self.env.ref(
            'quimibond_ficha_tecnica_tela.action_ficha_tecnica_tejido').read()[0]

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Importación de Fichas Técnicas de Tejido',
                'message': message,
                'sticky': bool(errors),
                'type': 'warning' if errors else 'success',
                'next': list_action,
            },
        }
