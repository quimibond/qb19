RFC_EMPRESA = 'EMP920101AB1'
RFC_ALFA = 'ALF010203XY1'
RFC_BETA = 'BET040506ZZ2'
PDF = b'%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n'


def cfdi_xml(uuid, emisor=RFC_ALFA, receptor=RFC_EMPRESA, subtotal=500.0, total=580.0, moneda='MXN', tipo='I',
             serie='F', folio='100', timbrado=True):
    """XML mínimo de un CFDI 4.0 para las pruebas (datos inventados)."""
    timbre = ('<cfdi:Complemento><tfd:TimbreFiscalDigital xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" '
              'Version="1.1" UUID="%s" FechaTimbrado="2026-11-10T10:00:00"/></cfdi:Complemento>' % uuid) if timbrado else ''
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Version="4.0" Serie="%s" Folio="%s" '
            'Fecha="2026-11-10T09:30:00" Moneda="%s" SubTotal="%.2f" Total="%.2f" TipoDeComprobante="%s" MetodoPago="PPD">'
            '<cfdi:Emisor Rfc="%s" Nombre="PROVEEDOR ALFA" RegimenFiscal="601"/>'
            '<cfdi:Receptor Rfc="%s" Nombre="EMPRESA" UsoCFDI="G01"/>'
            '<cfdi:Conceptos/>%s</cfdi:Comprobante>'
            % (serie, folio, moneda, subtotal, total, tipo, emisor, receptor, timbre)).encode()
