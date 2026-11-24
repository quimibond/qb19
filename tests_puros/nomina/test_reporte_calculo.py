# -*- coding: utf-8 -*-
"""Pytest puro de los reportes de nómina con formato NOI (quimibond_nomina 1.23.0).

Los recibos son los de la semana 48 de 2026 en Odoo (lote 137, borrador) y los
importes esperados son los del «Reporte de la nómina» y el «Percepción/deducción»
de NOI de esa semana que mandó RH el 20-nov-2026."""
import importlib.util
import os
import re

import pytest

_RUTA = os.path.join(os.path.dirname(__file__), '..', '..', 'addons', 'quimibond_nomina', 'models',
                     'reporte_calculo.py')
_spec = importlib.util.spec_from_file_location('qb_reporte_calculo', _RUTA)
rc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rc)

_XML = os.path.join(os.path.dirname(__file__), '..', '..', 'addons', 'quimibond_nomina', 'data',
                    'conceptos_noi.xml')


def _catalogo():
    """El catálogo tal como lo siembra data/conceptos_noi.xml."""
    texto = open(_XML, encoding='utf-8').read()
    res = []
    for bloque in re.findall(r'<record [^>]*>(.*?)</record>', texto, re.S):
        campos = dict(re.findall(r'<field name="(\w+)">([^<]*)</field>', bloque))
        res.append({'id': len(res) + 1, 'clave': campos.get('clave', ''), 'nombre': campos['name'],
                    'tipo': campos['tipo'], 'sat': campos.get('clave_sat', ''), 'codigos': campos['codigos'],
                    'codigos_exento': campos.get('codigos_exento', ''),
                    'en_especie': '<field name="en_especie" eval="True"/>' in bloque})
    return res


CFDI = {  # código → (tipo CFDI, clave SAT, gravable), como l10n_mx_concept en producción
    'DESC_FALTAS': ('perception', '001', True), 'FAP': ('perception', '005', False),
    'FAE': ('deduction', '004', False), 'FA_EMPRESA': ('deduction', '004', False),
    'HE_EXEMPT': ('perception', '019', False), 'HE_TAX': ('perception', '019', True),
    'H_SENC': ('perception', '038', True), 'PDOM_EXEMPT': ('perception', '020', False),
    'PDOM_TAX': ('perception', '020', True), 'CUOTA_SIND': ('deduction', '019', False),
    'PA': ('perception', '049', True), 'PP': ('perception', '010', True),
    'LOGRO_OBJ': ('perception', '038', True), 'ISR': ('deduction', '002', False),
    'SUBSIDY': ('other', '002', False), 'INFONAVIT': ('deduction', '010', False),
    'FONACOT': ('deduction', '011', False), 'PRESTAMO': ('deduction', '004', False),
    'CHILD_SUPPORT': ('deduction', '007', False), 'IMSS_EMPLOYEE_TOTAL': ('deduction', '001', False),
    'RETRO_SUELDO': ('perception', '001', True),
    'NO_TAX_MEAL_VOUCHER': ('perception', '029', False), 'TAX_MEAL_VOUCH': ('perception', '029', True),
}


def _lineas(**totales):
    res = {}
    for cod, total in totales.items():
        tipo, sat, grav = CFDI.get(cod, (None, '', True))
        res[cod] = {'total': total, 'nombre': cod, 'en_recibo': True, 'cfdi_tipo': tipo, 'sat': sat,
                    'gravable': grav}
    return res


GENARO = _lineas(  # S-1, recibo 5726
    BASIC=3619.91, FAE=-235.29, FAP=235.29, FA_EMPRESA=-235.29, HE_EXEMPT=581.77, HE_TAX=581.77,
    H_SENC=1745.31, RETRO_SUELDO=0, CUOTA_SIND=-62.57, PA=0, PP=853.26, LOGRO_OBJ=620.56, ISR=-1150.27,
    INFONAVIT=0, FONACOT=0, PRESTAMO=-300, CHILD_SUPPORT=0, IMSS_EMPLOYEE_TOTAL=140.22, NET=6114.23,
    GROSS=7420.81)
SANDRA = _lineas(  # S-141, recibo 5676: faltas, prima dominical, ISR = subsidio
    BASIC=2375.94, DESC_FALTAS=-593.98, FAE=-115.83, FAP=115.83, FA_EMPRESA=-115.83, PDOM_EXEMPT=117.31,
    PDOM_TAX=9.97, CUOTA_SIND=-17.82, ISR=-112.3, SUBSIDY=112.3, FONACOT=-419.5, IMSS_EMPLOYEE_TOTAL=47.66,
    NET=1308.43)
EDDI = _lineas(  # S-297, recibo 5651: subsidio menor que el ISR
    BASIC=2322.6, FAE=-150.97, FAP=150.97, FA_EMPRESA=-150.97, CUOTA_SIND=-23.23, ISR=-170.04,
    SUBSIDY=123.34, IMSS_EMPLOYEE_TOTAL=63.2, NET=2038.5)


def _por_clave(renglones):
    return {r['clave'] or r['nombre']: r for r in renglones}


def test_recibo_como_noi_genaro():
    r = rc.conceptos_de_recibo(GENARO, _catalogo())
    p = _por_clave(r)
    assert [x['clave'] for x in r] == ['P001', 'P003', 'P006', 'P015', 'P016', 'P017',
                                       'D001', 'D002', 'D004', 'D005', 'D006', 'D007']
    assert p['P001']['importe'] == 3619.91 and p['P001']['sat'] == '001'
    assert p['P003']['importe'] == 1163.54
    assert (p['P003']['gravado'], p['P003']['exento']) == (581.77, 581.77)
    assert p['P006']['exento'] == 235.29 and p['P006']['gravado'] == 0
    assert p['P017']['importe'] == 1745.31 and p['P017']['nombre'] == 'OTRAS PERCEPCIONES'
    assert p['D001']['importe'] == 1150.27
    assert p['D002']['importe'] == 140.22
    assert p['D004']['importe'] == 300.0
    t = rc.totales_de_recibo(r, GENARO['NET']['total'])
    assert (t['percepciones'], t['deducciones'], t['neto'], t['diferencia']) == (8237.87, 2123.64, 6114.23, 0)
    # Total gravado = sueldo imponible (GROSS) del recibo
    assert t['gravado'] == GENARO['GROSS']['total']


def test_recibo_con_faltas_y_subsidio_igual_al_isr():
    r = rc.conceptos_de_recibo(SANDRA, _catalogo())
    p = _por_clave(r)
    assert p['P001']['importe'] == 1781.96  # sueldo menos faltas, como NOI
    assert p['P019']['importe'] == 127.28
    assert 'D001' not in p  # ISR neto cero: NOI no lo imprime
    assert p['D111']['importe'] == 112.30 and p['D111']['tipo'] == 'informativo'
    t = rc.totales_de_recibo(r, SANDRA['NET']['total'])
    assert (t['percepciones'], t['deducciones'], t['neto'], t['diferencia']) == (2025.07, 716.64, 1308.43, 0)


def test_isr_menos_subsidio():
    p = _por_clave(rc.conceptos_de_recibo(EDDI, _catalogo()))
    assert p['D001']['importe'] == 46.70
    assert p['D111']['importe'] == 123.34
    t = rc.totales_de_recibo(list(p.values()), EDDI['NET']['total'])
    assert t['diferencia'] == 0


def test_regla_sin_concepto_sale_con_su_nombre():
    lineas = _lineas(BASIC=1000.0, PA=150.0, NET=1150.0)
    lineas['PA']['nombre'] = 'Premio de asistencia'
    r = rc.conceptos_de_recibo(lineas, _catalogo())
    pa = [x for x in r if x['nombre'] == 'Premio de asistencia'][0]
    assert pa['clave'] == '' and pa['sat'] == '049' and pa['importe'] == 150.0 and pa['gravado'] == 150.0
    assert rc.totales_de_recibo(r, 1150.0)['diferencia'] == 0


def test_falta_concepto_deja_diferencia():
    lineas = _lineas(BASIC=1000.0, NET=900.0)
    lineas['OTRA'] = {'total': -100.0, 'nombre': 'Otra', 'en_recibo': True, 'cfdi_tipo': None, 'sat': '',
                      'gravable': False}
    r = rc.conceptos_de_recibo(lineas, _catalogo())
    assert rc.totales_de_recibo(r, 900.0)['diferencia'] == -100.0


def test_resumen_y_por_concepto():
    cat = _catalogo()
    recibos = []
    for i, (clave, nombre, lineas) in enumerate([('S-141', 'SANDRA', SANDRA), ('S-1', 'GENARO', GENARO),
                                                 ('S-297', 'EDDI', EDDI)]):
        ren = rc.conceptos_de_recibo(lineas, cat)
        recibos.append({'empleado': {'id': i, 'clave': clave, 'nombre': nombre}, 'renglones': ren,
                        'totales': rc.totales_de_recibo(ren, lineas['NET']['total'])})
    res = rc.resumen(recibos)
    p001 = [f for f in res['percepciones'] if f['clave'] == 'P001'][0]
    assert p001['importe'] == pytest.approx(3619.91 + 1781.96 + 2322.60)
    assert p001['trabajadores'] == 3
    assert res['neto'] == pytest.approx(6114.23 + 1308.43 + 2038.50)
    assert res['total_informativo'] == pytest.approx(112.30 + 123.34)
    assert res['diferencia'] == 0
    pc = rc.por_concepto(recibos)
    p001 = pc[0]
    assert p001['clave'] == 'P001'
    assert [e['clave'] for e in p001['empleados']] == ['S-1', 'S-141', 'S-297']  # orden numérico
    solo = rc.por_concepto(recibos, {'D001'})
    assert [c['clave'] for c in solo] == ['D001']
    assert [e['clave'] for e in solo[0]['empleados']] == ['S-1', 'S-297']  # Sandra sin ISR neto


def test_semana48_totales_del_lote_contra_noi():
    """Totales por regla del lote 137 (aggregate de hr.payslip.line) contra la hoja final de NOI."""
    lote = _lineas(
        BASIC=258450.22, DESC_FALTAS=-3641.32, FAP=16562.59, FAE=-16562.59, FA_EMPRESA=-16562.59,
        HE_EXEMPT=20798.07, HE_TAX=20798.31, H_SENC=53392.29, PVAC_EXEMPT=3519.3, PVAC_TAX=1319.49,
        PDOM_EXEMPT=1876.96, PDOM_TAX=557.09, CUOTA_SIND=-3838.63, PP=50608.16, LOGRO_OBJ=36843.54,
        ISR=-50122.46, SUBSIDY=605.66, INFONAVIT=-7100.27, FONACOT=-9086.57, PRESTAMO=-2350,
        CHILD_SUPPORT=-2729.85, IMSS_EMPLOYEE_TOTAL=7962.68)
    for cod in ('PVAC_EXEMPT', 'PVAC_TAX'):
        lote[cod]['cfdi_tipo'] = 'perception'
    p = _por_clave(rc.conceptos_de_recibo(lote, _catalogo()))
    noi = {'P001': 254808.90, 'P003': 41596.38, 'P006': 16562.59, 'P015': 36843.54, 'P016': 50608.16,
           'P017': 53392.29, 'P019': 2434.05, 'P010': 4838.79, 'D002': 7962.68, 'D004': 2350.00,
           'D005': 16562.59, 'D006': 16562.59, 'D007': 3838.63, 'D093': 9086.57, 'D010': 2729.85,
           'D111': 605.66}
    for clave, importe in noi.items():
        assert p[clave]['importe'] == pytest.approx(importe), clave
    # Diferencias conocidas del paralelo (semana48_odoo_vs_noi.csv): ISR 2 centavos por redondeo
    # (S-87, S-96) e Infonavit 1 centavo (S-15, ficha pendiente de RH).
    assert p['D001']['importe'] == pytest.approx(49516.80)
    assert p['D094']['importe'] == pytest.approx(7100.27)
    t = rc.totales_de_recibo(list(p.values()))
    assert t['percepciones'] == pytest.approx(461084.70)
    assert t['gravado'] == pytest.approx(418327.78)
    # NOI imprime horas extra 41,596.38 = 20,798.31 gravado + 20,798.31 exento, que suma 41,596.62: su
    # exento trae 24 centavos de más y su total exento (42,757.16) no cuadra con su total menos su gravable.
    # Odoo: gravado 20,798.31 + exento 20,798.07 = 41,596.38.
    assert t['exento'] == pytest.approx(42756.92)
    assert t['percepciones'] - t['gravado'] == pytest.approx(t['exento'])


def test_xlsx_col():
    import ast
    ruta = os.path.join(os.path.dirname(__file__), '..', '..', 'addons', 'quimibond_nomina', 'wizard',
                        'reporte_nomina.py')
    arbol = ast.parse(open(ruta, encoding='utf-8').read())
    fn = [n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == 'xlsx_col'][0]
    ns = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), ruta, 'exec'), ns)
    assert [ns['xlsx_col'](i) for i in (0, 25, 26, 27, 51, 52)] == ['A', 'Z', 'AA', 'AB', 'AZ', 'BA']


def test_clave_empleado_orden():
    claves = ['S-10', 'Q-7', 'S-2', '', 'C-26', 'S-1']
    assert sorted(claves, key=rc.clave_empleado_orden) == ['C-26', 'Q-7', 'S-1', 'S-2', 'S-10', '']


def test_bruto_sin_vacaciones_no_se_reporta():
    """Producción, 20-nov: GROSS_WITHOUT_HOLIDAY lleva el concepto P01 del CFDI y salía como renglón aparte."""
    lineas = dict(GENARO)
    lineas.update(_lineas(GROSS_WITHOUT_HOLIDAY=3619.91))
    lineas['GROSS_WITHOUT_HOLIDAY']['cfdi_tipo'] = 'perception'
    r = rc.conceptos_de_recibo(lineas, _catalogo())
    assert not [x for x in r if x['llave'] == 'regla:GROSS_WITHOUT_HOLIDAY']
    assert rc.totales_de_recibo(r, GENARO['NET']['total'])['diferencia'] == 0


def test_vales_semana47_en_especie():
    """Semana 47 (lote 136), GENARO S-1, recibo 5540: 850 de vales exentos en NO_TAX_MEAL_VOUCHER, regla que
    no sale en el recibo de Odoo pero sí suma al neto (6,116.46). Sin el concepto de vales el reporte la
    perdía y el neto quedaba 850 arriba de percepciones − deducciones."""
    lineas = _lineas(
        BASIC=3619.91, FAE=-235.29, FAP=235.29, FA_EMPRESA=-235.29, HE_EXEMPT=581.77, HE_TAX=581.77,
        H_SENC=2133.16, CUOTA_SIND=-56.33, ISR=-918.31, PRESTAMO=-300, IMSS_EMPLOYEE_TOTAL=140.22,
        NO_TAX_MEAL_VOUCHER=850.0, TAX_MEAL_VOUCH=0.0, NET=6116.46)
    lineas['NO_TAX_MEAL_VOUCHER']['en_recibo'] = False
    r = rc.conceptos_de_recibo(lineas, _catalogo())
    vales = [x for x in r if x['nombre'] == 'VALES DE DESPENSA'][0]
    assert (vales['importe'], vales['gravado'], vales['exento'], vales['especie']) == (850.0, 0.0, 850.0, True)
    assert vales['sat'] == '029'
    t = rc.totales_de_recibo(r, 6116.46)
    assert t['diferencia'] == 0
    assert (t['especie'], t['efectivo'], t['neto']) == (850.0, 5266.46, 6116.46)
    res = rc.resumen([{'empleado': {'id': 1, 'clave': 'S-1', 'nombre': 'GENARO'}, 'renglones': r, 'totales': t}])
    assert (res['total_especie'], res['total_efectivo']) == (850.0, 5266.46)


def test_sin_vales_efectivo_igual_al_neto():
    t = rc.totales_de_recibo(rc.conceptos_de_recibo(GENARO, _catalogo()), GENARO['NET']['total'])
    assert (t['especie'], t['efectivo']) == (0.0, 6114.23)


def test_por_concepto_gravado_y_exento_por_trabajador():
    recibos = []
    for i, (clave, lineas) in enumerate([('S-1', GENARO), ('S-141', SANDRA)]):
        ren = rc.conceptos_de_recibo(lineas, _catalogo())
        recibos.append({'empleado': {'id': i, 'clave': clave, 'nombre': clave}, 'renglones': ren})
    he = rc.por_concepto(recibos, {'P003'})[0]
    assert (he['empleados'][0]['gravado'], he['empleados'][0]['exento']) == (581.77, 581.77)
    pdom = rc.por_concepto(recibos, {'P019'})[0]
    assert (pdom['gravado'], pdom['exento']) == (9.97, 117.31)
