# -*- coding: utf-8 -*-
"""Pytest puro del lector del forecast de Ventas (forecast.xlsx, F-P-A28-13).

El libro se arma aquí con el layout real (hoja por cliente, semanas en
columnas, bloques de año apilados, subbloques por planta, PO intercalado,
hoja PRONOSTICO con la unidad) y datos inventados: el archivo de Ventas no
va en el repo (trae volúmenes de clientes)."""
import os
import sys
from datetime import date, datetime

import openpyxl

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

fx = load('forecast_xlsx')

WEEK_COL = 3  # columna C (1-based) = semana 1


def _week_header(ws, first_col=WEEK_COL, row=3):
    ws.cell(row=2, column=first_col, value='ENERO')
    ws.cell(row=row, column=first_col - 1, value='semana')
    for week in range(1, 53):
        ws.cell(row=row, column=first_col + week - 1, value=float(week))


def _book():
    wb = openpyxl.Workbook()
    # Cliente A: bloque 2025 (no se lee), 2026 y 2027, parte del cliente en B.
    ws = wb.active
    ws.title = 'CLIENTE A'
    _week_header(ws)
    ws['A4'] = 2025.0
    ws['B5'] = 'FECHA DE ENTREGA'
    ws['A6'] = 'ZZJ053Q22JNT160'
    ws['B6'] = 'PARTE-1'
    ws.cell(row=6, column=WEEK_COL + 1, value=999.0)
    ws['A10'] = 2026.0
    ws['B11'] = 'FECHA DE ENTREGA'
    ws.cell(row=11, column=WEEK_COL + 1, value=datetime(2026, 1, 8))
    ws['A12'] = 'PRODUCTO'
    ws['B12'] = 'PO'
    ws.cell(row=12, column=WEEK_COL + 1, value='12033/PV1')
    ws['A13'] = 'ZZJ053Q22JNT160'
    ws['B13'] = 'PARTE-1'
    ws.cell(row=13, column=WEEK_COL + 1, value=1000.0)    # semana 2
    ws.cell(row=13, column=WEEK_COL + 2, value=0.0)       # cero: no es pronóstico
    ws.cell(row=13, column=WEEK_COL + 3, value='por confirmar')
    ws['A14'] = 'ZZJ042Q22JNT160'
    ws['B14'] = 'PARTE-2'
    ws.cell(row=14, column=WEEK_COL + 9, value=2500.0)    # semana 10
    ws['B15'] = 'Total'
    ws.cell(row=15, column=WEEK_COL + 1, value=1000.0)
    ws['A17'] = 2027.0
    ws['B18'] = 'FECHA DE ENTREGA'
    ws['A19'] = 'PRODUCTO'
    ws['B19'] = 'PO'
    ws['A20'] = 'ZZJ053Q22JNT160'
    ws['B20'] = 'PARTE-1'
    ws.cell(row=20, column=WEEK_COL, value=300.0)         # semana 1 de 2027
    # Cliente B: dos plantas dentro del año; la segunda trae un número suelto al lado.
    ws = wb.create_sheet('CLIENTE B')
    _week_header(ws)
    ws['A4'] = 2026.0
    ws['A5'] = 'NORTE'
    ws['B6'] = 'FECHA DE ENTREGA'
    ws['A7'] = 'PRODUCTO'
    ws['B7'] = 'PO'
    ws['A8'] = 'ZZJ045Q22JNT155'
    ws['B8'] = 244101.0
    ws.cell(row=8, column=WEEK_COL + 4, value=500.0)
    ws['B10'] = 'SUR'
    ws.cell(row=10, column=WEEK_COL + 20, value=22.0)
    ws['B11'] = 'FECHA DE ENTREGA'
    ws['A12'] = 'PRODUCTO'
    ws['B12'] = 'PO 70152'
    ws['A13'] = 'ZZJ045Q22JNT155'
    ws['B13'] = 244101.0
    ws.cell(row=13, column=WEEK_COL + 4, value=700.0)
    # Cliente C: semana 1 en la columna B, sin parte, sin fila de año (el año sale de las fechas).
    ws = wb.create_sheet('CLIENTE C')
    _week_header(ws, first_col=2)
    ws['A4'] = 'FECHA DE ENTREGA'
    ws.cell(row=4, column=3, value=datetime(2026, 1, 12))
    ws['A5'] = 'PRODUCTO / PO'
    ws['A6'] = 'ZZN040Q66JNT163'
    ws.cell(row=6, column=3, value=1500.0)                # semana 2
    # PO intercalado bajo el producto (como ZWISSTEX).
    ws['B7'] = 'PO'
    ws.cell(row=7, column=3, value='14792/PV2')
    # Resumen con la unidad por cliente y marcas que no son unidad.
    ws = wb.create_sheet('PRONOSTICO 2026')
    rows = [
        ('CLIENTE', 'UNIDADES', 'PRODUCTO / SEMANA'),
        ('CLIENTE A', 'm', 'ZZJ053Q22JNT160'),
        (None, 'Muestra', 'ZZJ042Q22JNT160'),
        ('CLIENTE B NORTE', 'yd', 'ZZJ045Q22JNT155'),
        ('CLIENTE B SUR', 'yd', 'ZZJ045Q22JNT155'),
        ('CLIENTE C', 'kg', 'ZZN040Q66JNT163'),
    ]
    for r, values in enumerate(rows, start=7):
        for c, value in enumerate(values, start=1):
            ws.cell(row=r, column=c, value=value)
    return wb


def _rows(**kwargs):
    return fx.parse_workbook(_book(), **kwargs)


def test_reads_client_sheets_not_summary():
    result = _rows(years={2026, 2027})
    assert result['sheets'] == ['CLIENTE A', 'CLIENTE B', 'CLIENTE C']


def test_year_blocks_weeks_and_iso_monday():
    rows = [r for r in _rows(years={2026, 2027})['rows'] if r['sheet'] == 'CLIENTE A']
    by_key = {(r['year'], r['code'], r['week']): r for r in rows}
    assert set(by_key) == {(2026, 'ZZJ053Q22JNT160', 2), (2026, 'ZZJ042Q22JNT160', 10),
                           (2027, 'ZZJ053Q22JNT160', 1)}
    first = by_key[(2026, 'ZZJ053Q22JNT160', 2)]
    assert first['qty'] == 1000.0
    assert first['monday'] == date(2026, 1, 5)
    assert first['delivery'] == date(2026, 1, 8)
    assert first['po'] == '12033/PV1'
    assert first['part'] == 'PARTE-1'
    assert by_key[(2027, 'ZZJ053Q22JNT160', 1)]['monday'] == date(2027, 1, 4)


def test_old_years_zero_and_total_are_skipped():
    rows = _rows(years={2026})['rows']
    assert all(r['year'] == 2026 for r in rows)
    assert not [r for r in rows if r['qty'] == 999.0]  # bloque 2025
    assert all(r['qty'] > 0 for r in rows)


def test_text_in_a_week_is_a_warning_not_a_quantity():
    result = _rows(years={2026})
    assert any('por confirmar' in w for w in result['warnings'])


def test_units_and_tags_from_summary():
    rows = {(r['sheet'], r['code']): r for r in _rows(years={2026})['rows']}
    assert rows[('CLIENTE A', 'ZZJ053Q22JNT160')]['unit'] == 'm'
    assert rows[('CLIENTE A', 'ZZJ042Q22JNT160')]['unit'] == 'm'
    assert rows[('CLIENTE A', 'ZZJ042Q22JNT160')]['tag'] == 'Muestra'
    assert rows[('CLIENTE C', 'ZZN040Q66JNT163')]['unit'] == 'kg'


def test_plants_inside_a_sheet():
    rows = [r for r in _rows(years={2026})['rows'] if r['sheet'] == 'CLIENTE B']
    plants = {(r['plant'], r['qty']) for r in rows}
    assert plants == {('Norte', 500.0), ('Sur', 700.0)}
    assert all(r['unit'] == 'yd' for r in rows)
    assert {r['part'] for r in rows} == {'244101'}
    sur = [r for r in rows if r['plant'] == 'Sur'][0]
    assert sur['po'] == 'PO 70152'


def test_sheet_without_year_row_and_week_one_in_column_b():
    rows = [r for r in _rows(years={2026})['rows'] if r['sheet'] == 'CLIENTE C']
    assert len(rows) == 1
    row = rows[0]
    assert (row['year'], row['week'], row['qty'], row['part']) == (2026, 2, 1500.0, None)
    assert row['po'] == '14792/PV2'  # PO intercalado bajo el producto


def test_norm_and_match_client():
    assert fx.norm('Saltillo Lamination (León)') == 'SALTILLO LAMINATION LEON'
    clients = ['SHAWMUT SILAO', 'SHAWMUT TENNESSEE', 'WOODBRIDGE INOAC TECHNICAL PRODUCTS SOL']
    assert fx.match_client('SHAWMUT', 'Tennessee', clients) == 'SHAWMUT TENNESSEE'
    assert fx.match_client('Woodbridge Inoac', None, clients) == 'WOODBRIDGE INOAC TECHNICAL PRODUCTS SOL'
    assert fx.match_client('OTRO', None, clients) is None
