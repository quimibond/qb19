# -*- coding: utf-8 -*-
"""Pytest puro del emparejamiento parte del cliente → producto.

Partes, descripciones y códigos inventados con la forma de los reales (no
hay datos de clientes en el repo). Correr con:
    pytest tests_puros/ventas_presupuesto/
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

pm = load('part_match')


def _cand(code, **kw):
    data = {'product_id': hash(code) % 10000, 'default_code': code}
    data.update(kw)
    return data


def test_parse_internal_code():
    assert pm.parse_internal_code('WJ053Q22JNT160') == {'grammage': 53.0, 'width_cm': 160.0}
    assert pm.parse_internal_code('IWJ045Q22JNT160') == {'grammage': 45.0, 'width_cm': 160.0}
    assert pm.parse_internal_code('WD3846NT163M2') == {'grammage': 38.0, 'width_cm': 163.0}
    assert pm.parse_internal_code('WC090Q11JNT165M2') == {'grammage': 90.0, 'width_cm': 165.0}


def test_parse_customer_description():
    info = pm.parse_customer_part('900001', '100%PES 53GR 1600MM')
    assert info['grammage'] == 53.0 and info['width_cm'] == 160.0
    info = pm.parse_customer_part('900002', 'CKS PES 32gsm')
    assert info['grammage'] == 32.0 and info['width_cm'] is None
    info = pm.parse_customer_part('900003', 'BACK SCRIM PES/100 IH LAMINATED 62"')
    assert info['width_cm'] == 157.5
    info = pm.parse_customer_part('XR99001/1640', 'SOPORTE')
    assert info['width_cm'] == 164.0
    info = pm.parse_customer_part('4000001', '64" WD3846NT163M2 QUIMIBOND (163 CM)')
    assert 'WD3846NT163M2' in info['codes']
    assert info["width_cm"] == 163.0  # el cm explícito manda sobre las pulgadas


def test_embedded_code_wins_and_is_high():
    part = {'part': '4000001', 'description': '64" WD3846NT163M2 QUIMIBOND (163 CM)'}
    candidates = [
        _cand('WD3846NT163M2', sold_to_customer=True, n_orders=3),
        _cand('WD3846NT163', sold_to_customer=True, n_orders=20),
        _cand('WJ053Q22JNT160', sold_to_customer=True, n_orders=40),
    ]
    ranked = pm.rank(part, candidates)
    assert ranked[0]['default_code'] == 'WD3846NT163M2'
    assert ranked[1]['default_code'] == 'WD3846NT163'  # mismo código sin M2
    assert pm.classify(ranked) == 'media'  # la variante sin M2 queda a < 30


def test_order_mention_and_po_resolve_part_without_description():
    # El caso Lear: la descripción no trae nuestra referencia, pero un pedido
    # menciona la parte y los pedidos de la PO llevan el producto.
    part = {'part': 'L0000000NCPAA', 'description': 'BACK SCRIM PES/100 IH LAMINATED 62"',
            'po': '1000001'}
    candidates = [
        _cand('IWJ045Q22JNT160', sold_to_customer=True, n_orders=38,
              mentions_part=True, with_po=True, grammage=45.0, width_m=1.60),
        _cand('WJ053Q22JNT160', sold_to_customer=True, n_orders=2),
    ]
    ranked = pm.rank(part, candidates)
    assert ranked[0]['default_code'] == 'IWJ045Q22JNT160'
    assert pm.classify(ranked) == 'alta'


def test_grammage_separates_two_fabrics_of_same_customer():
    part = {'part': '900001', 'description': 'CKS 53gsm'}
    candidates = [
        _cand('WJ053Q22JNT160', sold_to_customer=True, n_orders=10),
        _cand('WJ060Q22JNT160', sold_to_customer=True, n_orders=10),
    ]
    ranked = pm.rank(part, candidates)
    assert [r['default_code'] for r in ranked] == ['WJ053Q22JNT160', 'WJ060Q22JNT160']
    assert ranked[0]['score'] - ranked[1]['score'] == pm.SCORE_GRAMMAGE_OK - pm.SCORE_GRAMMAGE_BAD
    # Solo historia + gramaje: candidato, pero Ventas confirma.
    assert pm.classify(ranked) == 'media'


def test_ficha_beats_code_heuristic():
    # El código diría 38 g/m², la ficha dice 46: manda la ficha.
    part = {'part': '900001', 'description': 'PES 46 GR'}
    ranked = pm.rank(part, [_cand('WD3846NT163', grammage=46.0, sold_to_customer=True)])
    assert any('gramaje 46' in r for r in ranked[0]['reasons'])


def test_no_evidence_is_no_match():
    part = {'part': '900009', 'description': 'MATERIAL'}
    assert pm.rank(part, [_cand('WJ053Q22JNT160')]) == []
    assert pm.classify([]) == 'sin_match'
