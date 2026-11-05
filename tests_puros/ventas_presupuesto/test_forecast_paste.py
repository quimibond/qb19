# -*- coding: utf-8 -*-
"""Pytest puro de «Pegar semanas desde Excel» (bloque copiado de Excel: tabuladores y saltos de línea).
Datos inventados."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

fp = load('forecast_paste')


def _block(*rows):
    return '\r\n'.join('\t'.join(str(c) for c in row) for row in rows) + '\r\n'


def test_header_with_week_numbers_wins():
    text = _block(['', 'semana', 45, 46, 47, 48],
                  ['ZZJ053Q22JNT160', 'PARTE-1', '1,000', '', 0, '2500.5'])
    result = fp.parse_paste(text, first_week=1)
    assert result['weeks'] == [(0, 45), (0, 46), (0, 47), (0, 48)]
    row = result['rows'][0]
    assert row['ids'] == ['ZZJ053Q22JNT160', 'PARTE-1']
    assert row['qty'] == {(0, 45): 1000.0, (0, 46): None, (0, 47): 0.0, (0, 48): 2500.5}


def test_without_header_uses_first_week_and_wraps_year():
    text = _block(['ZZJ042Q22JNT160', 300, 400, 500])
    result = fp.parse_paste(text, first_week=51)
    assert result['weeks'] == [(0, 51), (0, 52), (1, 1)]
    assert result['rows'][0]['qty'] == {(0, 51): 300.0, (0, 52): 400.0, (1, 1): 500.0}


def test_header_wraps_to_next_year():
    text = _block(['', 51, 52, 1, 2], ['ZZJ042', 1, 2, 3, 4])
    assert fp.parse_paste(text)['weeks'] == [(0, 51), (0, 52), (1, 1), (1, 2)]


def test_product_row_with_small_numbers_is_not_a_header():
    text = _block(['ZZJ042Q22JNT160', 10, 11, 12, 13])
    result = fp.parse_paste(text, first_week=20)
    assert result['weeks'][0] == (0, 20)
    assert result['rows'][0]['qty'][(0, 20)] == 10.0


def test_text_in_a_week_is_a_warning_and_totals_are_skipped():
    text = _block(['', 'semana', 1, 2, 3, 4],
                  ['ZZJ053', 'por confirmar', '', 7, 8, 9],
                  ['TOTAL', '', 5, 7, 8, 9])
    result = fp.parse_paste(text)
    assert [r['ids'][0] for r in result['rows']] == ['ZZJ053']
    assert result['rows'][0]['ids'] == ['ZZJ053', 'por confirmar']


def test_text_cell_inside_weeks_warns():
    text = _block(['', 1, 2, 3, 4], ['ZZJ053', 5, 'pendiente', 6, 7])
    result = fp.parse_paste(text)
    assert (0, 2) not in result['rows'][0]['qty']
    assert any('pendiente' in w for w in result['warnings'])


def test_no_header_and_no_first_week_asks_for_it():
    result = fp.parse_paste(_block(['ZZJ053', 5, 6]))
    assert not result['rows'] and result['warnings']


def test_number_and_norm_code():
    assert fp.number(' 1,234.5 ') == 1234.5
    assert fp.number('-') is None
    assert fp.number('abc') is False
    assert fp.norm_code(' wj045 q22jnt160. ') == 'WJ045Q22JNT160'
