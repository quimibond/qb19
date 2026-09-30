# -*- coding: utf-8 -*-
"""Emparejamiento parte del cliente → producto de Quimibond.

Python puro (sin Odoo): recibe la parte del cliente y los candidatos ya
leídos de la base, y devuelve los candidatos ordenados con su puntaje y el
porqué. Lo usa ``qb.customer.part`` para sugerir el producto y se prueba con
pytest fuera de Odoo.

Evidencia, de más a menos fuerte:
  1. La descripción del cliente trae nuestra referencia (FXI: «WD3846NT163M2»).
  2. Un pedido nuestro menciona la parte del cliente en una línea o nota.
  3. El producto se ha vendido con la PO que trae el release.
  4. El producto se le ha vendido a ese cliente.
  5. Gramaje y ancho de la descripción contra la ficha o contra el código.
Un gramaje o ancho que no cuadra resta: dos telas del mismo cliente se
distinguen por ahí.

Nunca decide sola: ``classify`` devuelve «alta» (se puede aceptar), «media»
(Ventas confirma) o «sin_match».
"""
import re

# --- Puntajes ---------------------------------------------------------------
SCORE_CODE_EXACT = 100
SCORE_CODE_NEAR = 80
SCORE_ORDER_MENTIONS_PART = 60
SCORE_ORDER_WITH_PO = 40
SCORE_SOLD_TO_CUSTOMER = 25
SCORE_SOLD_FREQUENCY_MAX = 10
SCORE_GRAMMAGE_OK = 20
SCORE_GRAMMAGE_BAD = -30
SCORE_WIDTH_OK = 15
SCORE_WIDTH_BAD = -20

GRAMMAGE_TOL = 2.0     # g/m²
GRAMMAGE_BAD = 5.0
WIDTH_TOL_CM = 3.0
WIDTH_BAD_CM = 8.0

HIGH_SCORE = 100
HIGH_MARGIN = 30
MEDIUM_SCORE = 50

INCH_CM = 2.54

# Referencias internas: letras, dígitos y letras, sin espacios (WJ053Q22JNT160,
# IWJ045Q22JNT160, WD3846NT163M2, WC090Q11JNT165). Mínimo 8 caracteres con
# letras y dígitos para no confundir números de parte del cliente.
_CODE_RE = re.compile(r'\b([A-Z]{1,4}\d{2,4}[A-Z0-9]{4,})\b')

_GRAMMAGE_RES = (
    re.compile(r'(\d{2,3}(?:[.,]\d+)?)\s*(?:G/M2|G/M²|GSM|GRS?|GR/M2|GM2|G)\b'),
    re.compile(r'(\d{2,3}(?:[.,]\d+)?)\s*GRAMOS'),
)
_WIDTH_RES = (
    (re.compile(r'(\d{3,4}(?:[.,]\d+)?)\s*MM\b'), 0.1),
    (re.compile(r'(\d{2,3}(?:[.,]\d+)?)\s*CM\b'), 1.0),
    (re.compile(r'(\d{2,3}(?:[.,]\d+)?)\s*(?:"|IN\b|INCH|PULG)'), INCH_CM),
    (re.compile(r'(\d(?:[.,]\d+)?)\s*M\b(?!2)'), 100.0),
)
# Ancho pegado al número de parte con diagonal: XR27028/1640 (mm).
_PART_WIDTH_RE = re.compile(r'/(\d{3,4})$')


def _num(text):
    return float(text.replace(',', '.'))


def normalize_code(code):
    """Mayúsculas, sin espacios ni guiones."""
    return re.sub(r'[\s\-_.]', '', (code or '').upper())


def base_code(code):
    """Código sin los adornos que no cambian la tela: sufijo M2 (se vende por
    metro cuadrado) y prefijo I (variante de facturación)."""
    code = normalize_code(code)
    if code.endswith('M2') and len(code) > 6:
        code = code[:-2]
    if code.startswith('I') and len(code) > 6 and code[1:2].isalpha():
        code = code[1:]
    return code


def parse_internal_code(code):
    """Gramaje (g/m²) y ancho (cm) que se leen del código interno.

    WJ053Q22JNT160 → 53, 160; WD3846NT163M2 → 38, 163; WC090Q11JNT165 → 90,
    165. Es heurística: la ficha manda cuando existe."""
    code = base_code(code)
    result = {'grammage': None, 'width_cm': None}
    m = re.match(r'^[A-Z]+(\d+)', code)
    if m:
        digits = m.group(1)
        if len(digits) in (2, 3):
            result['grammage'] = float(int(digits))
        elif len(digits) >= 4:
            result['grammage'] = float(int(digits[:2]))
    m = re.search(r'(\d{3})$', code)
    if m and 80 <= int(m.group(1)) <= 400:
        result['width_cm'] = float(int(m.group(1)))
    return result


def parse_customer_part(part, description):
    """Lo que se puede leer de la parte y su descripción del cliente."""
    text = (description or '').upper()
    grammage = None
    for regex in _GRAMMAGE_RES:
        m = regex.search(text)
        if m:
            value = _num(m.group(1))
            if 8 <= value <= 600:
                grammage = value
                break
    width = None
    for regex, factor in _WIDTH_RES:
        m = regex.search(text)
        if m:
            value = _num(m.group(1)) * factor
            if 50 <= value <= 400:
                width = round(value, 1)
                break
    if width is None:
        m = _PART_WIDTH_RE.search((part or '').strip())
        if m:
            value = int(m.group(1)) / 10.0
            if 50 <= value <= 400:
                width = value
    codes = [c for c in _CODE_RE.findall(text) if any(ch.isdigit() for ch in c)]
    return {'grammage': grammage, 'width_cm': width, 'codes': codes}


def _candidate_specs(candidate):
    """Gramaje y ancho del candidato: la ficha primero, el código después."""
    parsed = parse_internal_code(candidate.get('default_code'))
    grammage = candidate.get('grammage') or parsed['grammage']
    width = candidate.get('width_m')
    width_cm = width * 100.0 if width else parsed['width_cm']
    return grammage, width_cm


def score_candidate(part, candidate):
    """(puntaje, [motivos]) de un candidato para una parte del cliente.

    part: dict con 'part', 'description', 'po' (opcional).
    candidate: dict con 'product_id', 'default_code', 'grammage', 'width_m'
      (de la ficha, opcionales), 'sold_to_customer' (bool), 'n_orders' (int),
      'mentions_part' (bool: un pedido nuestro menciona la parte),
      'with_po' (bool: vendido con la PO del release)."""
    info = parse_customer_part(part.get('part'), part.get('description'))
    code = normalize_code(candidate.get('default_code'))
    score, reasons = 0, []

    embedded = {normalize_code(c) for c in info['codes']}
    if code and code in embedded:
        score += SCORE_CODE_EXACT
        reasons.append("la descripción del cliente trae la referencia %s" % code)
    elif code and base_code(code) in {base_code(c) for c in embedded}:
        score += SCORE_CODE_NEAR
        reasons.append("la descripción trae la referencia %s (sin M2 / prefijo)"
                       % code)

    if candidate.get('mentions_part'):
        score += SCORE_ORDER_MENTIONS_PART
        reasons.append("un pedido nuestro menciona la parte %s" % part.get('part'))
    if candidate.get('with_po'):
        score += SCORE_ORDER_WITH_PO
        reasons.append("vendido con la PO %s del cliente" % part.get('po'))
    if candidate.get('sold_to_customer'):
        score += SCORE_SOLD_TO_CUSTOMER
        n_orders = candidate.get('n_orders') or 0
        bonus = min(SCORE_SOLD_FREQUENCY_MAX, n_orders)
        score += bonus
        reasons.append("se le ha vendido a este cliente (%d pedidos)" % n_orders)

    grammage, width_cm = _candidate_specs(candidate)
    if info['grammage'] and grammage:
        diff = abs(info['grammage'] - grammage)
        if diff <= GRAMMAGE_TOL:
            score += SCORE_GRAMMAGE_OK
            reasons.append("gramaje %g ≈ %g g/m²" % (info['grammage'], grammage))
        elif diff > GRAMMAGE_BAD:
            score += SCORE_GRAMMAGE_BAD
            reasons.append("gramaje distinto: cliente %g, producto %g g/m²"
                           % (info['grammage'], grammage))
    if info['width_cm'] and width_cm:
        diff = abs(info['width_cm'] - width_cm)
        if diff <= WIDTH_TOL_CM:
            score += SCORE_WIDTH_OK
            reasons.append("ancho %g ≈ %g cm" % (info['width_cm'], width_cm))
        elif diff > WIDTH_BAD_CM:
            score += SCORE_WIDTH_BAD
            reasons.append("ancho distinto: cliente %g, producto %g cm"
                           % (info['width_cm'], width_cm))
    return score, reasons


def rank(part, candidates, limit=5):
    """Candidatos ordenados por puntaje (mayor primero), solo los positivos.

    Devuelve [{'product_id', 'default_code', 'score', 'reasons'}]."""
    ranked = []
    for candidate in candidates:
        score, reasons = score_candidate(part, candidate)
        if score > 0:
            ranked.append({
                'product_id': candidate.get('product_id'),
                'default_code': candidate.get('default_code'),
                'score': score,
                'reasons': reasons,
            })
    ranked.sort(key=lambda r: (-r['score'], r['default_code'] or ''))
    return ranked[:limit]


def classify(ranked):
    """'alta' si el primero es claro, 'media' si hay candidato pero Ventas
    confirma, 'sin_match' si no hay nada que sugerir."""
    if not ranked or ranked[0]['score'] < MEDIUM_SCORE:
        return 'sin_match'
    margin = ranked[0]['score'] - (ranked[1]['score'] if len(ranked) > 1 else 0)
    if ranked[0]['score'] >= HIGH_SCORE and margin >= HIGH_MARGIN:
        return 'alta'
    return 'media'
