# -*- coding: utf-8 -*-
"""Qué perfil le toca a un archivo de release que llegó por correo (3.30.0). Asuntos y archivos como los
mandan los clientes (nov-2026); remitentes inventados."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

correo = load('correo')
L = correo.lista

PERFILES = [
    {'id': 1, 'domains': L('lear.com'), 'subject': L('release'), 'files': L('*.eml')},
    {'id': 2, 'domains': L('fxi.com'), 'subject': L('release'), 'files': L('sum quimibond*.xlsx')},
    {'id': 3, 'domains': L('woodbridgegroup.com'), 'subject': L('saltillo lamination release'),
     'files': L('* d.pdf')},
    {'id': 8, 'domains': L('woodbridgegroup.com'), 'subject': L('leon lamination'), 'files': L('*.d.pdf')},
    {'id': 5, 'domains': L('shawmutcorporation.com'), 'subject': [], 'files': L('sl 3254*')},
    {'id': 9, 'domains': L('shawmutcorporation.com'), 'subject': [], 'files': L('quimibond tn release*')},
    {'id': 7, 'domains': L('zwisstex.com'), 'subject': [], 'files': L('release quimibond cw*.xlsx')},
    {'id': 10, 'domains': L('grupocopo.com'), 'subject': L('release w'), 'files': L('*.pdf')},
]


def elegir(email_from, subject, filename, body=''):
    return correo.elegir(PERFILES, email_from, subject, body, filename)[0]


def test_cada_cliente_su_perfil():
    assert elegir('A <a@lear.com>', 'Supplier 6PIN0010 QUIMIBOND Lear-MTO 5500 Release 11-19-26',
                  'QUIMIBOND.eml') == 1
    assert elegir('B <b@fxi.com>', 'RELEASE  QUIMIBOND  11.16.26', 'SUM Quimibond  11.16.26.xlsx') == 2
    assert elegir('C <c@woodbridgegroup.com>', 'QUIMIBOND - SALTILLO LAMINATION RELEASE WEEK 47 2026',
                  '130551 - Quimibond 11-16-2026 D.pdf') == 3
    assert elegir('D <d@woodbridgegroup.com>', 'Release WK44 Leon Lamination Quimibond',
                  'Release Quimibond WK44.D.pdf') == 8
    assert elegir('E <e@shawmutcorporation.com>', 'Quimibond TN Release 20261117.xls',
                  'Quimibond TN Release 20261117.xls') == 9
    assert elegir('E <e@shawmutcorporation.com>', 'EDI', 'SL 3254 Release for CW47 2026.csv') == 5
    assert elegir('F <f@zwisstex.com>', 'RELEASE QUIMIBOND CW47', 'RELEASE QUIMIBOND CW47.xlsx') == 7
    assert elegir('G <g@grupocopo.com>', 'RE: RELEASE W47 //QUIMIBOND - CTM', 'Quimibond 16_11_2026.pdf') == 10


def test_lo_que_no_es_release_no_entra():
    # Saltillo manda dos PDF: solo el «D» se lee.
    assert elegir('C <c@woodbridgegroup.com>', 'QUIMIBOND - SALTILLO LAMINATION RELEASE WEEK 47 2026',
                  '130551 - Quimibond 11-16-2026.pdf') is None
    # Firmas, PPAP, órdenes de compra del mismo cliente.
    assert elegir('G <g@grupocopo.com>', 'RE: RELEASE W47 //QUIMIBOND - CTM', 'image001.png') is None
    assert elegir('G <g@grupocopo.com>', 'RE: PPAP // 362121 // Quimibond', 'COPO PPAP.pdf') is None
    assert elegir('B <b@fxi.com>', 'Ordenes de compra FXI', '4500575808.pdf') is None
    # Cliente sin perfil.
    assert elegir('H <h@tq1.com.mx>', 'RELEASE', 'programa.xlsx') is None


def test_reenviado_por_ventas_se_reconoce_por_el_cuerpo():
    body = '---------- Forwarded message ---------\nDe: Andrea <andrea@zwisstex.com>\nAsunto: RELEASE'
    assert elegir('Jessica <jessica@quimibond.com>', 'Fwd: RELEASE QUIMIBOND CW47',
                  'RELEASE QUIMIBOND CW47.xlsx', body) == 7


def test_empate_no_elige():
    perfiles = [{'id': 1, 'domains': ['x.com'], 'subject': [], 'files': []},
                {'id': 2, 'domains': ['x.com'], 'subject': [], 'files': []}]
    assert correo.elegir(perfiles, 'a@x.com', 'Release', '', 'r.pdf') == (None, [1, 2])


def test_sin_patron_acepta_solo_archivos_de_release():
    perfiles = [{'id': 1, 'domains': ['x.com'], 'subject': [], 'files': []}]
    assert correo.elegir(perfiles, 'a@x.com', '', '', 'r.xlsx')[0] == 1
    assert correo.elegir(perfiles, 'a@x.com', '', '', 'logo.png')[0] is None


def test_perfil_sin_dominio_no_se_reconoce():
    perfiles = [{'id': 1, 'domains': [], 'subject': [], 'files': []}]
    assert correo.elegir(perfiles, 'a@x.com', '', '', 'r.pdf')[0] is None
