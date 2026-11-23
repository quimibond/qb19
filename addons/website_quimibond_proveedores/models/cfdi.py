"""Lectura de un CFDI (3.3 o 4.0) subido por un proveedor.

Python puro: no toca la base. `leer_cfdi` devuelve los datos que se validan
contra la orden de compra, o lanza `CfdiInvalido` con un mensaje que se le
puede mostrar tal cual al proveedor.
"""
import re

from lxml import etree

NS_CFDI = ('http://www.sat.gob.mx/cfd/4', 'http://www.sat.gob.mx/cfd/3')
NS_TFD = 'http://www.sat.gob.mx/TimbreFiscalDigital'
RE_UUID = re.compile(r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$')
RE_RFC = re.compile(r'^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$')
MAX_XML = 2 * 1024 * 1024


class CfdiInvalido(ValueError):
    """El archivo no es un CFDI que se pueda recibir. El mensaje es para el proveedor."""


def limpiar_rfc(rfc):
    """RFC en mayúsculas y sin separadores; quita el prefijo «MX» de un NIF."""
    rfc = re.sub(r'[\s\-.]', '', (rfc or '').upper())
    if rfc.startswith('MX') and RE_RFC.match(rfc[2:]):
        rfc = rfc[2:]
    return rfc


def _numero(texto):
    try:
        return float(texto or 0)
    except (TypeError, ValueError):
        return 0.0


def leer_cfdi(contenido):
    """dict con uuid, serie, folio, fecha, emisor_rfc, emisor_nombre, receptor_rfc,
    moneda, subtotal, descuento, total, tipo, metodo_pago, version."""
    if not contenido:
        raise CfdiInvalido('El archivo XML está vacío.')
    if len(contenido) > MAX_XML:
        raise CfdiInvalido('El XML pesa más de 2 MB. Suba el XML original de la factura.')
    # Sin entidades ni red: el XML viene de fuera.
    parser = etree.XMLParser(resolve_entities=False, no_network=True, dtd_validation=False,
                             load_dtd=False, huge_tree=False, remove_comments=True)
    try:
        raiz = etree.fromstring(contenido, parser=parser)
    except (etree.XMLSyntaxError, ValueError):
        raise CfdiInvalido('El archivo no es un XML válido. Suba el XML original de la factura, no una copia en PDF.')
    nombre = etree.QName(raiz)
    if nombre.localname != 'Comprobante' or nombre.namespace not in NS_CFDI:
        raise CfdiInvalido('El XML no es una factura electrónica (CFDI).')
    ns = nombre.namespace
    emisor = raiz.find('{%s}Emisor' % ns)
    receptor = raiz.find('{%s}Receptor' % ns)
    timbre = raiz.find('{%s}Complemento/{%s}TimbreFiscalDigital' % (ns, NS_TFD))
    if emisor is None or receptor is None:
        raise CfdiInvalido('El XML no trae emisor o receptor.')
    uuid = (timbre.get('UUID') if timbre is not None else '') or ''
    uuid = uuid.strip().upper()
    if not RE_UUID.match(uuid):
        raise CfdiInvalido('La factura no está timbrada: el XML no trae folio fiscal.')
    return {
        'version': raiz.get('Version') or raiz.get('version') or '',
        'uuid': uuid,
        'serie': (raiz.get('Serie') or '').strip(),
        'folio': (raiz.get('Folio') or '').strip(),
        'fecha': (raiz.get('Fecha') or '')[:10],
        'tipo': (raiz.get('TipoDeComprobante') or '').upper(),
        'metodo_pago': raiz.get('MetodoPago') or '',
        'moneda': (raiz.get('Moneda') or 'MXN').upper(),
        'subtotal': _numero(raiz.get('SubTotal')),
        'descuento': _numero(raiz.get('Descuento')),
        'total': _numero(raiz.get('Total')),
        'emisor_rfc': limpiar_rfc(emisor.get('Rfc')),
        'emisor_nombre': (emisor.get('Nombre') or '').strip(),
        'receptor_rfc': limpiar_rfc(receptor.get('Rfc')),
    }


def clabe_valida(clabe):
    """True si la CLABE tiene 18 dígitos y su dígito verificador es correcto."""
    clabe = re.sub(r'\s', '', clabe or '')
    if not re.fullmatch(r'\d{18}', clabe):
        return False
    pesos = (3, 7, 1)
    suma = sum((int(d) * pesos[i % 3]) % 10 for i, d in enumerate(clabe[:17]))
    return (10 - suma % 10) % 10 == int(clabe[17])
