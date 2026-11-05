# -*- coding: utf-8 -*-
"""57.137.0 (revisión de Administración de Ventas 2026-10-08, punto 4; brief §6.8 «Aprobación para iniciar un proyecto»).

La **aprobación para iniciar el proyecto** es un documento que se genera desde el proyecto (y
la cotización, cuando ``qb_costeo_sgi`` está instalado), nunca a mano: lo llena Diseño de Producto con lo que
el cliente compartió, Administración de Ventas lo manda al cliente y el cliente lo firma, manda una orden de compra
o aprueba por WhatsApp. El documento ya lleva el **folio FT**: al generarlo se asigna si no lo
tenía (antes el folio llegaba al pasar a «Muestra»; esa asignación sigue como respaldo). La
evidencia de la respuesta del cliente se sigue registrando con «Registrar aprobación del cliente».

57.152.0 (Dirección General 2026-10-13): el documento conserva todos los campos del formato
anterior F-P-D01-09 (tejido) y F-P-D01-19 (entretelas): encabezado con clave, revisión y área del
formato vigente (``format_ref_dev_start_tejido`` / ``_entretelas`` por ``sgi_dev_line_key``), datos
generales y del proyecto, características con nominal y tolerancias separadas, información
requerida al cliente (``sgi.dev.required.doc``), requisitos legales con su leyenda, capacidad a
utilizar por recurso de la ruta (``_sgi_dev_capacity_resources``; la capacidad la pone
``qb_costeo_sgi`` desde ``qb.centro`` y nunca se inventa) y los dos recuadros de aprobación. Sale en
inglés si el cliente es extranjero.
"""
import base64
import logging

from odoo import fields, models
from odoo.exceptions import UserError

from .sgi_dev_analysis import DEV_LINES

_logger = logging.getLogger(__name__)

# 57.152.0: formato del SGI por línea (sgi.format.map, data/sgi_format_map_data.xml). Sin línea, el
# formato general de tejido (F-P-D01-09), que es el que no dice «entretelas».
START_APPROVAL_FORMAT_REFS = {
    'tejido_circular': 'format_ref_dev_start_tejido',
    'entretelas': 'format_ref_dev_start_entretelas',
}
REQUIRED_DOC_MOMENTS = [
    ('diseno', "Se requiere para el diseño y desarrollo"),
    ('factible', "Se entrega cuando el proyecto sea factible"),
    ('na', "No aplica"),
]
YARD_M = 0.9144
# Textos del documento en español y en inglés (cliente extranjero). Las listas del SGI (mercado,
# aplicación, requisitos legales, documentos) salen como se capturaron.
START_APPROVAL_LABELS = {
    'es': {
        'title': "Aprobación para iniciar un proyecto", 'title_entretelas': "Aprobación para iniciar un proyecto entretelas",
        'code': "Clave", 'revision': "Revisión", 'area': "Área", 'issued': "Emisión",
        'general': "Datos generales", 'customer': "Cliente", 'requester': "Nombre del solicitante",
        'market': "Mercado", 'address': "Dirección", 'project_name': "Nombre del proyecto", 'phone': "Teléfono",
        'email': "Correo", 'tech_sheet': "Ficha técnica", 'request_date': "Fecha de solicitud",
        'description': "Descripción del proyecto", 'characteristics': "Características del proyecto",
        'project_data': "Datos del proyecto", 'characteristic': "Característica", 'direction': "Dirección",
        'units': "Unidades", 'norm': "Norma a aplicar", 'nominal': "Valor nominal", 'tolerances': "Tolerancias",
        'no_lines': "Sin características marcadas «En especificación del cliente».",
        'application': "Aplicación", 'required_date': "Fecha requerida", 'spec_number': "No. de especificación",
        'sample_size': "Tamaño de la muestra", 'monthly': "Consumo mensual", 'packaging': "Forma de empaque",
        'line_material': "Material de línea (no. de parte)", 'new_dev': "Nuevo desarrollo",
        'product_name': "Nombre del producto", 'price': "Precio (moneda/divisas)", 'uom': "Unidad de medida",
        'quote': "Cotización", 'validity': "Vigencia", 'lamination': "Laminado", 'program': "Programa",
        'required_info': "Información requerida", 'document': "Documento",
        'moment_diseno': "Se requiere para el diseño y desarrollo",
        'moment_factible': "Se entrega cuando el proyecto sea factible", 'moment_na': "No aplica",
        'no_required': "Sin información requerida capturada.",
        'legal': "Requisitos legales y regulatorios", 'no_legal': "Sin requisitos legales capturados.",
        'legal_note': "Cualquier información de aspectos legales o regulatorios que requiera ser entregada junto "
                      "con el producto debe ser solicitada por el cliente.",
        'capacity': "Capacidad a utilizar", 'resource': "Recurso", 'sample_qty': "Cantidad de la muestra",
        'sample_pct': "% de capacidad para la muestra", 'monthly_qty': "Consumo mensual",
        'monthly_pct': "% de capacidad para el consumo mensual", 'raw_material': "Materia prima",
        'machines_row': "Circulares galga %s (producción real, promedio %s días): %s",
        'no_capacity': "Sin capacidad capturada", 'no_kg': "Sin conversión a kg (falta el rendimiento)",
        'no_route': "El artículo no tiene ruta capturada: solo se lista la materia prima.",
        'capacity_note': "Porcentaje sobre la capacidad mensual de cada recurso en kg.",
        'approval_customer': "Aprobación del inicio del proyecto (cliente)",
        'approval_pnt': "Aprobación del inicio del proyecto PNT Quimibond",
        'medium': "Medio", 'date': "Fecha", 'approved_by': "Aprobó", 'reference': "Referencia",
        'evidence': "Evidencia", 'pending_customer': "Pendiente: se registra al recibir la respuesta del cliente.",
        'generated_by': "Generó (Diseño de Producto)", 'sent_by': "Envió (Administración de Ventas)",
        'sign': "Nombre, firma y fecha",
        'closing': "Con su aprobación iniciamos el desarrollo con las características y condiciones de este "
                   "documento. Cualquier cambio posterior queda registrado como modificación del proyecto con su "
                   "revisión.",
        'largo': "Largo", 'ancho': "Ancho", 'izquierda': "Izquierda", 'centro': "Centro", 'derecha': "Derecha",
        'yes': "Sí", 'no': "No", 'max': "Máximo", 'min': "Mínimo",
    },
    'en': {
        'title': "Project start approval", 'title_entretelas': "Project start approval: interlinings",
        'code': "Code", 'revision': "Revision", 'area': "Area", 'issued': "Issued",
        'general': "General information", 'customer': "Customer", 'requester': "Requested by",
        'market': "Market", 'address': "Address", 'project_name': "Project name", 'phone': "Phone",
        'email': "E-mail", 'tech_sheet': "Technical sheet", 'request_date': "Request date",
        'description': "Project description", 'characteristics': "Project characteristics",
        'project_data': "Project data", 'characteristic': "Characteristic", 'direction': "Direction",
        'units': "Units", 'norm': "Test method", 'nominal': "Nominal value", 'tolerances': "Tolerances",
        'no_lines': "No characteristics marked as part of the customer specification.",
        'application': "Application", 'required_date': "Required date", 'spec_number': "Specification no.",
        'sample_size': "Sample size", 'monthly': "Monthly consumption", 'packaging': "Packaging",
        'line_material': "Standard material (part no.)", 'new_dev': "New development",
        'product_name': "Product name", 'price': "Price (currency)", 'uom': "Unit of measure",
        'quote': "Quotation", 'validity': "Valid until", 'lamination': "Lamination", 'program': "Program",
        'required_info': "Required information", 'document': "Document",
        'moment_diseno': "Required for design and development",
        'moment_factible': "Delivered once the project is feasible", 'moment_na': "Not applicable",
        'no_required': "No required information recorded.",
        'legal': "Legal and regulatory requirements", 'no_legal': "No legal requirements recorded.",
        'legal_note': "Any legal or regulatory information that must be delivered with the product must be "
                      "requested by the customer.",
        'capacity': "Capacity to be used", 'resource': "Resource", 'sample_qty': "Sample quantity",
        'sample_pct': "% of capacity for the sample", 'monthly_qty': "Monthly consumption",
        'monthly_pct': "% of capacity for monthly consumption", 'raw_material': "Raw material",
        'machines_row': "Gauge %s circular machines (actual output, %s-day average): %s",
        'no_capacity': "No capacity on record", 'no_kg': "No conversion to kg (yield missing)",
        'no_route': "The item has no routing yet: only raw material is listed.",
        'capacity_note': "Percentage of each resource's monthly capacity in kg.",
        'approval_customer': "Project start approval (customer)",
        'approval_pnt': "Project start approval PNT Quimibond",
        'medium': "Medium", 'date': "Date", 'approved_by': "Approved by", 'reference': "Reference",
        'evidence': "Evidence", 'pending_customer': "Pending: recorded when the customer replies.",
        'generated_by': "Prepared by (Product Design)", 'sent_by': "Sent by (Sales Administration)",
        'sign': "Name, signature and date",
        'closing': "With your approval we start the development with the characteristics and conditions in this "
                   "document. Any later change is recorded as a project modification with its revision.",
        'largo': "Length", 'ancho': "Width", 'izquierda': "Left", 'centro': "Center", 'derecha': "Right",
        'yes': "Yes", 'no': "No", 'max': "Maximum", 'min': "Minimum",
    },
}
START_APPROVAL_MEDIA_EN = {
    'correo': "E-mail", 'oc': "Purchase order", 'whatsapp': "WhatsApp",
    'cotizacion_firmada': "Signed quotation", 'direccion': "Management (internal development)",
}
START_APPROVAL_UOM_EN = {'m': "m", 'yd': "yd", 'kg': "kg"}


def _num(value, decimals=2):
    """1234.5 → '1,234.5' (sin ceros de sobra)."""
    text = ('{:,.%df}' % decimals).format(value or 0.0)
    return text.rstrip('0').rstrip('.') if '.' in text else text


def _date(value):
    return value.strftime('%d/%m/%Y') if value else ''


class SgiDevOptionLine(models.Model):
    """57.152.0: la «Información requerida al cliente» puede ser de una sola línea (Caducidad de
    materiales, solo entretelas)."""
    _inherit = 'sgi.dev.option'

    line_key = fields.Selection(DEV_LINES, string="Línea",
                                help="Solo para «Información requerida al cliente»: la línea a la que aplica el "
                                     "documento. Vacío: las dos líneas.")


class SgiDevRequiredDoc(models.Model):
    """57.152.0: información que el cliente pide o debe entregar (formato anterior F-P-D01-09 / 19,
    bloque «Información requerida»): un renglón por documento del catálogo con su momento."""
    _name = 'sgi.dev.required.doc'
    _description = "Información requerida al cliente en el desarrollo"
    _order = 'sequence, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    option_id = fields.Many2one('sgi.dev.option', string="Documento", required=True, ondelete='restrict',
                                domain="[('kind', '=', 'documento_cliente')]",
                                help="Documento del catálogo «Información requerida al cliente».")
    moment = fields.Selection(REQUIRED_DOC_MOMENTS, string="Cuándo", required=True, default='na',
                              help="Si el cliente lo pide para el diseño y desarrollo, si se entrega cuando el "
                                   "proyecto sea factible o si no aplica.")
    note = fields.Char(string="Observación", help="Texto libre; no capture aquí valores.")

    _project_option_uniq = models.Constraint(
        'unique(project_id, option_id)',
        "Ese documento ya está en la información requerida del proyecto.",
    )


class ProjectProjectDevStartApproval(models.Model):
    _inherit = 'project.project'

    sgi_dev_start_approval_attachment_id = fields.Many2one(
        'ir.attachment', string="Aprobación para iniciar (PDF)", readonly=True, copy=False)
    sgi_dev_start_approval_date = fields.Datetime(string="Aprobación para iniciar generada el", readonly=True, copy=False)
    sgi_dev_start_approval_by_id = fields.Many2one('res.users', string="Generó la aprobación para iniciar",
                                                   readonly=True, copy=False)
    sgi_dev_start_approval_sent_at = fields.Datetime(string="Aprobación para iniciar enviada al cliente el",
                                                     readonly=True, copy=False)
    sgi_dev_start_approval_sent_by_id = fields.Many2one('res.users', string="La envió", readonly=True, copy=False)
    sgi_dev_required_doc_ids = fields.One2many('sgi.dev.required.doc', 'project_id',
                                               string="Información requerida al cliente", copy=True,
                                               help="Documentos que el cliente pide para el diseño y desarrollo o "
                                                    "que se entregan cuando el proyecto sea factible.")

    # ------------------------------------------------------------------
    # 57.152.0: información requerida al cliente
    # ------------------------------------------------------------------
    def action_sgi_dev_load_required_docs(self):
        """Carga en «No aplica» los documentos del catálogo de la línea que todavía no están."""
        Option = self.env['sgi.dev.option']
        for project in self.filtered('sgi_is_ft'):
            domain = [('kind', '=', 'documento_cliente')]
            if project.sgi_dev_line_key:
                domain += ['|', ('line_key', '=', False), ('line_key', '=', project.sgi_dev_line_key)]
            else:
                domain += [('line_key', '=', False)]
            missing = Option.search(domain) - project.sgi_dev_required_doc_ids.option_id
            if missing:
                self.env['sgi.dev.required.doc'].create([
                    {'project_id': project.id, 'option_id': option.id, 'sequence': option.sequence, 'moment': 'na'}
                    for option in missing])
        return True

    # ------------------------------------------------------------------
    # 57.152.0: datos del documento
    # ------------------------------------------------------------------
    def _sgi_dev_start_approval_is_en(self):
        """El documento sale en inglés si el cliente es extranjero (país capturado y distinto de México)."""
        self.ensure_one()
        country = (self.partner_id.commercial_partner_id or self.partner_id).country_id
        return bool(country) and country.code != 'MX'

    def _sgi_dev_start_approval_labels(self):
        self.ensure_one()
        return START_APPROVAL_LABELS['en' if self._sgi_dev_start_approval_is_en() else 'es']

    def _sgi_dev_start_approval_format_ref(self):
        self.ensure_one()
        return START_APPROVAL_FORMAT_REFS.get(self.sgi_dev_line_key) or START_APPROVAL_FORMAT_REFS['tejido_circular']

    def _sgi_dev_start_approval_format(self):
        """Clave, revisión, área y emisión del formato vigente del SGI que sustituye este documento."""
        self.ensure_one()
        ref = self._sgi_dev_start_approval_format_ref()
        fmap = self.env['sgi.format.map']._sgi_ref(ref)
        doc = fmap.sgi_live_document() if fmap else self.env['documents.document']
        code, revision = fmap.sgi_live_parts() if fmap else (False, False)
        return {
            'ref': ref, 'code': code or '', 'revision': revision or '',
            'area': doc.sgi_area_id.display_name if doc else '',
            'issue_date': _date(doc.sgi_issue_date) if doc else '',
        }

    def _sgi_dev_start_approval_data(self):
        """Datos generales y del proyecto ya armados como texto (formato F-P-D01-09 / 19)."""
        self.ensure_one()
        L = self._sgi_dev_start_approval_labels()
        en = self._sgi_dev_start_approval_is_en()
        contact = self.sgi_dev_contact_id
        partner = self.partner_id.commercial_partner_id or self.partner_id

        def pick(field):
            return (contact[field] if contact else False) or (partner[field] if partner else False) or ''

        address = ''
        for who in (contact, partner):
            if who and (who.street or who.city):
                address = ', '.join(part.strip() for part in who._display_address(without_company=True).split('\n')
                                    if part.strip())
                break
        quote = self._sgi_dev_start_approval_quote_vals()
        uom_label = (START_APPROVAL_UOM_EN if en else dict(self._fields['sgi_dev_consumption_uom'].selection))
        monthly = ''
        if self.sgi_dev_consumption_monthly:
            monthly = "%s %s" % (_num(self.sgi_dev_consumption_monthly), uom_label.get(self.sgi_dev_consumption_uom) or '')
        sample = ' · '.join(filter(None, [
            ("%s m" % _num(self.sgi_dev_sample_m)) if self.sgi_dev_sample_m else '',
            ("%s kg" % _num(self.sgi_dev_sample_kg)) if self.sgi_dev_sample_kg else '']))
        product = self.sgi_dev_product_id
        base = self.sgi_dev_base_product_id
        is_line = self.sgi_dev_analysis_result == 'linea'

        def option(record):
            return ((en and record.name_en) or record.name or '') if record else ''

        price = ''
        if quote.get('precio'):
            price = ("%s %s" % (quote['precio'], quote.get('moneda') or '')).strip()
        return {
            'customer': partner.display_name or '',
            'requester': (self.sgi_dev_requester or contact.name or self.sgi_dev_requester_employee_id.name or ''),
            'market': option(self.sgi_dev_market_id),
            'address': address,
            'project_name': product.default_code or self.sgi_dev_product_name or self.name or '',
            'phone': pick('phone'),
            'email': pick('email'),
            'tech_sheet': self.sgi_ft_folio or '',
            'request_date': _date(self.sgi_dev_date or (self.create_date and self.create_date.date())),
            'description': self.sgi_dev_use or '',
            'application': option(self.sgi_dev_application_id),
            'required_date': _date(self.sgi_dev_required_date),
            'spec_number': self.sgi_dev_spec_number or '',
            'sample_size': sample,
            'monthly': monthly,
            'packaging': self.sgi_dev_packaging or '',
            'line_material': (base.default_code or base.display_name or '') if is_line else '',
            'new_dev': '' if is_line else (self.sgi_ft_folio or L['yes']),
            'product_name': product.name or self.sgi_dev_product_name or (base.name if is_line else '') or '',
            'price': price,
            'uom': quote.get('unidad') or '',
            'quote': quote.get('folio') or '',
            'validity': quote.get('vigencia') or '',
            'lamination': option(self.sgi_dev_lamination_id),
            'program': self.sgi_dev_program or '',
        }

    def _sgi_dev_start_approval_line_rows(self):
        """Renglones de características: nombre, dirección, unidades, norma, nominal y tolerancias."""
        self.ensure_one()
        L = self._sgi_dev_start_approval_labels()
        en = self._sgi_dev_start_approval_is_en()
        rows = []
        for line in self._sgi_dev_start_approval_lines():
            name = (en and line.caracteristica_id.name_en) or line.name or line.caracteristica_id.name or ''
            direction = ' '.join(L[key] for key in (line.direction, line.position) if key and key != 'na')
            nominal = tolerance = ''
            if line.kind == 'bool':
                nominal = L['yes'] if line.spec_bool else L['no']
            elif line.kind == 'text':
                nominal = line.spec_text or ''
            else:
                nominal = _num(line.spec_nominal, 3) if line.spec_nominal else ''
                pct = ' %' if line.spec_tol_pct else ''
                if line.spec_limit == 'max':
                    tolerance = L['max']
                elif line.spec_limit == 'min':
                    tolerance = L['min']
                elif line.spec_tol_minus or line.spec_tol_plus:
                    if abs(line.spec_tol_minus - line.spec_tol_plus) < 1e-9:
                        tolerance = "± %s%s" % (_num(line.spec_tol_plus, 3), pct)
                    else:
                        tolerance = "−%s%s / +%s%s" % (_num(line.spec_tol_minus, 3), pct,
                                                       _num(line.spec_tol_plus, 3), pct)
            rows.append({
                'name': name, 'direction': direction, 'unit': line.unit or '',
                'method': line.method or line.caracteristica_id.method or '',
                'nominal': nominal, 'tolerance': tolerance,
            })
        return rows

    def _sgi_dev_start_approval_required_rows(self):
        self.ensure_one()
        en = self._sgi_dev_start_approval_is_en()
        return [{'name': (en and doc.option_id.name_en) or doc.option_id.name, 'moment': doc.moment,
                 'note': doc.note or ''}
                for doc in self.sgi_dev_required_doc_ids.sorted(lambda d: (d.sequence, d.id))]

    def _sgi_dev_start_approval_legal(self):
        self.ensure_one()
        en = self._sgi_dev_start_approval_is_en()
        return [(en and legal.name_en) or legal.name for legal in self.sgi_dev_legal_ids]

    # ------------------------------------------------------------------
    # 57.152.0: capacidad a utilizar
    # ------------------------------------------------------------------
    def _sgi_dev_capacity_resources(self):
        """Recursos de la ruta del artículo en orden: [{'name', 'capacity_kg' (kg/mes o False),
        'operations'}]. El núcleo agrupa por centro de trabajo y no conoce capacidades (False);
        ``qb_costeo_sgi`` agrupa por centro de costo (``qb.centro``) y pone su capacidad medida."""
        self.ensure_one()
        groups = {}
        for _product, op in self._sgi_dev_route_operations():
            workcenter = op.workcenter_id
            group = groups.setdefault(workcenter.id, {'name': workcenter.display_name or op.name,
                                                      'capacity_kg': False, 'operations': []})
            if op.name and op.name not in group['operations']:
                group['operations'].append(op.name)
        return list(groups.values())

    def _sgi_dev_to_kg(self, qty, uom):
        """Cantidad en kg, o None si no se puede convertir (falta el rendimiento)."""
        self.ensure_one()
        if not qty:
            return 0.0
        if uom == 'kg':
            return qty
        meters = qty * YARD_M if uom == 'yd' else qty
        yield_m_kg = self._sgi_dev_yield_m_per_kg()  # sgi_dev_sample.py: 1000 / (masa × ancho) o el renglón
        return meters / yield_m_kg if yield_m_kg else None

    def _sgi_dev_start_approval_capacity(self):
        """Tabla «Capacidad a utilizar»: materia prima y una fila por recurso de la ruta, con la cantidad de
        la muestra y del consumo mensual y su porcentaje sobre la capacidad mensual (kg). Sin capacidad
        capturada no hay porcentaje: se dice así y ``missing`` lista los recursos de la ruta que faltan."""
        self.ensure_one()
        L = self._sgi_dev_start_approval_labels()
        if self.sgi_dev_sample_kg:
            sample_kg, sample_label = self.sgi_dev_sample_kg, "%s kg" % _num(self.sgi_dev_sample_kg)
        elif self.sgi_dev_sample_m:
            sample_kg, sample_label = self._sgi_dev_to_kg(self.sgi_dev_sample_m, 'm'), "%s m" % _num(self.sgi_dev_sample_m)
        else:
            sample_kg, sample_label = 0.0, ''
        uom = self.sgi_dev_consumption_uom or 'm'
        monthly_kg = self._sgi_dev_to_kg(self.sgi_dev_consumption_monthly, uom)
        monthly_label = ("%s %s" % (_num(self.sgi_dev_consumption_monthly), uom)) if self.sgi_dev_consumption_monthly else ''

        def with_kg(label, kg):
            if not label:
                return ''
            if kg and not label.endswith(' kg'):
                return "%s (%s kg)" % (label, _num(kg))
            return label

        def pct(kg, capacity):
            if not capacity:
                return L['no_capacity']
            if kg is None:
                return L['no_kg']
            if not kg:
                return ''
            return "%s %%" % _num(kg / capacity * 100.0, 3)

        resources = self._sgi_dev_capacity_resources()
        # 57.158.0: las circulares que hoy corren la galga del artículo (ficha de la máquina) con su producción
        # real en kg; no sustituye la capacidad de la ruta, la acompaña.
        machines = self._sgi_dev_capacity_machines(L)
        rows = []
        for resource in ([{'name': L['raw_material'], 'capacity_kg': False, 'operations': [], 'raw': True}]
                         + resources + machines):
            name = resource['name']
            if resource.get('operations'):
                name = "%s: %s" % (name, ', '.join(resource['operations']))
            rows.append({
                'name': name,
                'sample_qty': with_kg(sample_label, sample_kg),
                'sample_pct': pct(sample_kg, resource['capacity_kg']),
                'monthly_qty': with_kg(monthly_label, monthly_kg),
                'monthly_pct': pct(monthly_kg, resource['capacity_kg']),
            })
        return {
            'rows': rows,
            'missing': [r['name'] for r in resources if not r['capacity_kg']],
            'no_route': not resources,
        }

    def _sgi_dev_capacity_machines(self, labels):
        """57.158.0: una fila con las circulares que corren la galga del artículo y su producción real en kg
        al mes (promedio de los últimos días); vacía si no hay galga, circulares o producción."""
        self.ensure_one()
        from .sgi_dev_analysis import FEAS_PRODUCCION_DIAS, _fmt_num  # import local (orden de models/__init__)
        galga, circulares = self._sgi_dev_circulares()
        real = self._sgi_dev_produccion_real(circulares) if circulares else {}
        total = sum(real.values())
        if not total:
            return []
        names = ', '.join(wc.name for wc in circulares)
        return [{'name': labels['machines_row'] % (_fmt_num(galga), FEAS_PRODUCCION_DIAS, names),
                 'capacity_kg': total, 'operations': []}]

    def _sgi_dev_start_approval_approvals(self):
        """Los dos recuadros: aprobación del cliente (medio y evidencia, cuando se registra) y de PNT
        Quimibond (quién generó y quién envió, con fecha)."""
        self.ensure_one()
        en = self._sgi_dev_start_approval_is_en()
        media = START_APPROVAL_MEDIA_EN if en else dict(self._fields['sgi_dev_customer_approval_medium'].selection)
        return {
            'customer_registered': bool(self.sgi_dev_customer_approved_at),
            'customer_medium': media.get(self.sgi_dev_customer_approval_medium) or '',
            'customer_date': _date(self.sgi_dev_customer_approval_date),
            'customer_contact': self.sgi_dev_customer_approval_contact_id.name or '',
            'customer_ref': self.sgi_dev_customer_approval_ref or '',
            'customer_evidence': self.sgi_dev_customer_approval_filename or '',
            'generated_by': self.sgi_dev_start_approval_by_id.name or '',
            'generated_at': _date(self.sgi_dev_start_approval_date and self.sgi_dev_start_approval_date.date()),
            'sent_by': self.sgi_dev_start_approval_sent_by_id.name or '',
            'sent_at': _date(self.sgi_dev_start_approval_sent_at and self.sgi_dev_start_approval_sent_at.date()),
        }

    def _sgi_dev_start_approval_quote_vals(self):
        """Datos de la cotización que van en el documento. Gancho: el núcleo no conoce el cotizador;
        ``qb_costeo_sgi`` devuelve folio, precio, moneda, vigencia y condiciones de la cotización
        presentada o ganada más reciente. Vacío = el documento sale sin precio."""
        self.ensure_one()
        return {}

    def _sgi_dev_start_approval_lines(self):
        """Renglones de la tabla que van al cliente (los marcados «En especificación del cliente»)."""
        self.ensure_one()
        return self.sgi_dev_line_ids.filtered('in_customer_spec').sorted(lambda l: (l.sequence, l.id))

    def action_sgi_dev_start_approval(self):
        """Genera el PDF «Aprobación para iniciar el proyecto», asigna el folio FT si falta y lo deja
        en el proyecto y en el chatter.

        57.152.0: quién lo generó y cuándo se escriben antes de imprimir (salen en el recuadro de PNT
        Quimibond) y, si algún recurso de la ruta no tiene capacidad capturada, avisa cuáles."""
        warnings = []
        for project in self:
            if not project.sgi_is_ft:
                raise UserError("Solo un proyecto de desarrollo lleva aprobación para iniciar.")
            if project.sgi_dev_analysis_result in ('linea', 'no_factible'):
                raise UserError("%s: un producto de línea o no factible no abre proyecto; no hay aprobación que pedir."
                                % project.display_name)
            if not project.partner_id:
                raise UserError("%s: capture el cliente antes de generar la aprobación para iniciar." % project.display_name)
            if not project.sgi_ft_folio:
                project.action_sgi_dev_assign_folio()
            project.write({'sgi_dev_start_approval_date': fields.Datetime.now(),
                           'sgi_dev_start_approval_by_id': self.env.uid})
            report = self.env.ref('quimibond_sgi.action_report_dev_start_approval')
            pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=project.ids)
            attachment = self.env['ir.attachment'].sudo().create({
                'name': "Aprobación para iniciar %s.pdf" % (project.sgi_ft_folio or project.name),
                'type': 'binary', 'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
                'res_model': 'project.project', 'res_id': project.id,
            })
            project.write({'sgi_dev_start_approval_attachment_id': attachment.id})
            body = "Aprobación para iniciar el proyecto generada (%s)." % (project.sgi_ft_folio or '')
            warning = project._sgi_dev_start_approval_capacity_warning()
            if warning:
                body += " " + warning
                warnings.append(warning if len(self) == 1 else "%s: %s" % (project.display_name, warning))
            project.message_post(body=body, attachment_ids=attachment.ids)
        if warnings:
            return {
                'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {
                    'type': 'warning', 'sticky': True, 'title': "Aprobación para iniciar generada, con avisos",
                    'message': "\n".join(warnings),
                    'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'},
                },
            }
        return True

    def _sgi_dev_start_approval_capacity_warning(self):
        """Texto del aviso de capacidad: los recursos de la ruta sin capacidad capturada (el documento
        dice «Sin capacidad capturada»), o que el artículo no tiene ruta. Vacío si todo tiene dato."""
        self.ensure_one()
        capacity = self._sgi_dev_start_approval_capacity()
        if capacity['no_route']:
            return ("El artículo no tiene ruta capturada en su lista de materiales (C1.04b): la capacidad a "
                    "utilizar solo lista la materia prima.")
        if capacity['missing']:
            return ("Sin capacidad capturada en: %s. El documento lo dice así; capture la capacidad del centro "
                    "en Costeo para que salga el porcentaje." % ", ".join(capacity['missing']))
        return ''

    def action_sgi_dev_start_approval_mail(self):
        """Correo al cliente con el PDF (lo manda Administración de Ventas)."""
        self.ensure_one()
        if not self.sgi_dev_start_approval_attachment_id:
            self.action_sgi_dev_start_approval()
        template = self.env.ref('quimibond_sgi.mail_template_sgi_dev_start_approval')
        recipients = self.sgi_dev_contact_id or self.partner_id
        ctx = {
            'default_model': self._name, 'default_res_ids': self.ids, 'default_template_id': template.id,
            'default_composition_mode': 'comment', 'default_partner_ids': recipients.ids,
            'default_attachment_ids': self.sgi_dev_start_approval_attachment_id.ids,
            'sgi_dev_start_approval_notify': True, 'force_email': True,
        }
        return {'type': 'ir.actions.act_window', 'res_model': 'mail.compose.message', 'view_mode': 'form',
                'views': [(self.env.ref('mail.email_compose_message_wizard_form').id, 'form')],
                'target': 'new', 'context': ctx}

    def message_post(self, **kwargs):
        if self.env.context.get('sgi_dev_start_approval_notify'):
            when = fields.Datetime.now()
            for project in self.filtered(lambda p: p.sgi_is_ft and not p.sgi_dev_start_approval_sent_at):
                project.with_context(sgi_dev_start_approval_notify=False).write({
                    'sgi_dev_start_approval_sent_at': when, 'sgi_dev_start_approval_sent_by_id': self.env.uid})
        return super().message_post(**kwargs)

    def action_sgi_dev_print_start_approval(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_start_approval').report_action(self)
