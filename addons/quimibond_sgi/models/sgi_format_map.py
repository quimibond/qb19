# -*- coding: utf-8 -*-
import logging
import re

from odoo import models, fields, api
from odoo.exceptions import ValidationError

from .sgi_document import SGI_CODE_REGEX

_logger = logging.getLogger(__name__)


class SgiFormatMap(models.Model):
    """Mapeo formato SGI ↔ documento de Odoo que lo sustituye.

    El registro nativo (cotización, OC, remisión…) porta la clave del formato
    controlado que reemplaza; la revisión NUNCA se captura aquí: se lee en vivo
    del documento vigente en la app Documentos (única fuente de verdad).
    """
    _name = 'sgi.format.map'
    _description = "Formato SGI en documentos de Odoo"
    _order = 'sgi_code'

    model_id = fields.Many2one('ir.model', string="Modelo de Odoo", required=True,
                               ondelete='cascade',
                               help="El documento de Odoo que sustituye al formato en Excel.")
    model_name = fields.Char(related='model_id.model', string="Modelo técnico", store=True)
    sgi_code = fields.Char(string="Clave SGI", required=True,
                           help="Clave del formato controlado (ej. F-P-A28-04).")
    sgi_code_alt = fields.Char(
        string="Clave alternativa",
        help="Clave que aplica cuando el registro está confirmado (solo ventas: "
             "cotización vs pedido). Vacío = siempre la clave principal.")
    active = fields.Boolean(default=True)
    note = fields.Char(string="Nota")

    _model_uniq = models.Constraint(
        'unique(model_id)',
        "Ya existe un mapeo de formato para este modelo.",
    )

    @api.constrains('sgi_code', 'sgi_code_alt')
    def _check_codes(self):
        for fmap in self:
            for code in filter(None, (fmap.sgi_code, fmap.sgi_code_alt)):
                if not SGI_CODE_REGEX.match(code.strip()):
                    raise ValidationError(
                        "La clave '%s' no cumple la nomenclatura del SGI "
                        "(ej. F-P-A28-04, F-IT-P-P01-08-01)." % code)

    @api.model
    def _get_for_model(self, model_name):
        return self.search([('model_name', '=', model_name)], limit=1)

    @api.model
    def _revision_of(self, code):
        """Revisión del documento VIGENTE con esa clave, o False si no existe."""
        doc = self.env['documents.document'].sudo().search([
            ('sgi_code', '=', code),
            ('sgi_state', '=', 'vigente'),
        ], limit=1)
        return doc.sgi_revision or False


class SgiConfig(models.AbstractModel):
    """Utilidades de configuración del SGI (siembra idempotente)."""
    _name = 'sgi.config'
    _description = "Configuración SGI"

    # Parámetros operativos: default de arranque. Solo se crean si NO existen;
    # lo editado en Ajustes > Técnico > Parámetros del sistema nunca se pisa.
    _SGI_DEFAULT_PARAMS = {
        'quimibond_sgi.nc_escalation_days': '5',
        'quimibond_sgi.nc_escalation_days_external': '3',
        'quimibond_sgi.nc_recurrence_months': '12',
        'quimibond_sgi.action_escalation_manager_days': '7',
        'quimibond_sgi.action_escalation_director_days': '15',
        'quimibond_sgi.doc_review_notice_days': '60',
        'quimibond_sgi.doc_review_notice_days_final': '30',
        'quimibond_sgi.doc_ack_pending_days': '7',
        'quimibond_sgi.doc_pilot_notice_days': '7',
        'quimibond_sgi.fmea_npr_action': '100',
        'quimibond_sgi.risk_ryo_inmediata': '16',
        'quimibond_sgi.risk_ryo_media': '9',
        'quimibond_sgi.risk_ryo_intermedia': '4',
        'quimibond_sgi.supplier_weight_otd': '0.7',
        'quimibond_sgi.supplier_weight_quality': '0.3',
        'quimibond_sgi.supplier_nc_penalty': '10.0',
        'quimibond_sgi.pesaje_tolerance_kg': '3.0',
        'quimibond_sgi.waste_subproduct_category': 'SubProducto',
        'quimibond_sgi.monthly_sales_budget': '0',
        'quimibond_sgi.rh_user_id': '0',
    }

    @api.model
    def seed_parameters(self):
        Param = self.env['ir.config_parameter'].sudo()
        for key, value in self._SGI_DEFAULT_PARAMS.items():
            if Param.get_param(key) is False:
                Param.set_param(key, value)
        return True

    # Objetivo de cada proceso, tomado de las caracterizaciones/SIPOC reales del
    # SGI (ANEXO 2/3, mapeos por área). Solo se siembra donde está vacío.
    _SGI_PROCESS_PURPOSES = {
        'proc_adm': "Gestionar los recursos humanos, financieros, materiales y de información para que los procesos operativos cumplan los requisitos del cliente y la normatividad.",
        'proc_cal': "Asegurar la conformidad del producto y del proceso: liberación de materia prima, inspección y prueba, auditorías de calidad y certificados.",
        'proc_mto': "Mantener la infraestructura y la maquinaria disponibles y confiables (preventivo y correctivo) para la eficiencia productiva y la disminución de tiempos muertos.",
        'proc_dis': "Diseñar y desarrollar productos y procesos que cumplan los requisitos y expectativas del cliente, entregando muestras y especificaciones aprobadas.",
        'proc_mfg': "Transformar la materia prima en producto terminado conforme a especificaciones (tejido, acabado, entretelas y tintorería), cumpliendo el programa de producción.",
        'proc_ventas': "Convertir los requerimientos del cliente en pedidos confirmados: prospección, cotización, pedido, entrega y postventa; alimentar el pronóstico y el presupuesto.",
        'proc_planeacion': "Traducir el presupuesto y el pronóstico en el programa semanal de producción, validando inventarios y capacidades, y comunicar las fechas de entrega.",
        'proc_compras': "Abastecer insumos y materiales conformes y a tiempo: requisición, evaluación de cotizaciones, orden de compra, recepción y pago a proveedores.",
        'proc_almacen_mp': "Recibir, resguardar y surtir materia prima liberada a producción, con inventarios exactos y trazables.",
        'proc_almacen_pt': "Resguardar el producto terminado clasificado y preparar los embarques conforme a la lista de embarque.",
        'proc_prod_tac': "Tejer y acabar la tela conforme al programa y las especificaciones (tejido circular, teñido y rama), registrando producción y desperdicio.",
        'proc_prod_ent': "Producir entretelas conforme al programa y especificaciones (carda, cocina, punteado y termofijado, espolvoreo, corte).",
        'proc_tintoreria': "Teñir la tela cruda conforme a receta y estándares de color, entregando tela teñida para acabado.",
        'proc_inspeccion': "Inspeccionar, clasificar, enrollar y empacar el producto, liberando solo producto conforme al siguiente proceso.",
        'proc_laboratorio': "Realizar las pruebas de laboratorio de materia prima y producto que sustentan la liberación y los certificados de calidad.",
        'proc_logistica': "Entregar el producto al cliente en tiempo y forma: citas, transporte, evidencias de entrega, importaciones y exportaciones.",
        'proc_facturacion': "Facturar las entregas correcta y oportunamente (CFDI) y registrar los cobros.",
        'proc_cxc': "Administrar el crédito y la cobranza: condiciones de crédito, seguimiento de cartera y aplicación de pagos.",
        'proc_rh': "Proveer personal competente: reclutamiento, inducción, detección de necesidades (DNC), capacitación y evaluación de habilidades.",
        'proc_sgi': "Mantener y mejorar el SGI tri-norma: información documentada, no conformidades, auditorías, riesgos, indicadores y cumplimiento legal.",
        'proc_direccion': "Dirigir el sistema: planeación estratégica, asignación de recursos, revisión mensual de indicadores y Revisión por la Dirección.",
    }

    @api.model
    def migrate_document_families(self):
        """H21: llena sgi_parent_document_id (FK) donde esté vacío, infiriendo el
        procedimiento padre P-Xnn vigente por la nomenclatura. Idempotente: solo
        toca los documentos sin padre. Los que no matcheen quedan vacíos y se
        reporta el conteo en el log (carga manual posterior de MAST)."""
        Doc = self.env['documents.document']
        family_re = re.compile(r'(P-[AGCDEIMPSV]\d{2})')
        docs = Doc.search([
            ('sgi_is_controlled', '=', True),
            ('sgi_parent_document_id', '=', False),
        ])
        filled = unmatched = 0
        for doc in docs:
            code = (doc.sgi_code or '').strip().upper()
            match = family_re.search(code)
            if not match or code == match.group(1):
                continue  # sin clave de familia, o es el propio padre P-Xnn
            parent = Doc.search([
                ('sgi_code', '=', match.group(1)),
                ('sgi_state', '=', 'vigente'),
            ], limit=1)
            if parent and parent.id != doc.id:
                doc.sgi_parent_document_id = parent.id
                filled += 1
            else:
                unmatched += 1
        _logger.info(
            "SGI familia documental (H21): %d FK llenados por nomenclatura, "
            "%d sin procedimiento padre vigente (quedan para captura de MAST).",
            filled, unmatched)
        return True

    # --- Piloto: P-A28 VENTAS Rev.15 capturado como datos del proceso ---------
    # Responsabilidades (rol tal como aparece en el procedimiento, puesto, texto).
    _SGI_VENTAS_RESPONSIBILITIES = [
        ("Director de ventas", "Director de Ventas",
         "Elabora y da seguimiento al presupuesto anual y mensual; define las "
         "estrategias comerciales; selecciona al personal del área; mantiene la "
         "relación con los clientes potenciales."),
        ("Administrador de ventas y marketing", "Administrador de Ventas y Marketing",
         "Elabora el presupuesto anual y mensual; da seguimiento a los indicadores "
         "del SGI; controla el presupuesto; es el dueño de este procedimiento; "
         "mantiene la comunicación con las áreas."),
        ("Atención a clientes y vendedores confección", "Atención a Clientes Confección",
         "Elabora el programa semanal de entregas de confección; negocia fechas con "
         "Logística; atiende reclamaciones y postventa; vigila stocks para evitar "
         "desabasto; solicita muestras."),
        ("Coordinador de ventas entretelas confección", "Coordinador de Ventas Confección",
         "Da seguimiento al procedimiento; mantiene contacto con clientes; canaliza "
         "requisitos; registra pedidos en Odoo; realiza reuniones semanales con los "
         "vendedores; da seguimiento a reclamaciones."),
        ("Coordinador de ventas industrial", "Coordinador de Ventas Industrial",
         "Elabora el pronóstico y presupuesto industrial mensual y reporta su "
         "progreso; atiende los requisitos del cliente industrial; solicita muestras; "
         "brinda asesoría técnica; negocia y cierra; registra pedidos en Odoo; "
         "elabora cotizaciones; gestiona órdenes de compra; realiza visitas y sus "
         "reportes; da seguimiento a los indicadores de calidad del área; coordina "
         "recolecciones con Logística; elabora el programa semanal de entregas "
         "industriales."),
        ("Vendedores", "Vendedor",
         "Apoyan en el pronóstico y presupuesto; elaboran reportes de visita; "
         "realizan visitas de confección y entretelas; registran el pedido en Odoo; "
         "conocen las aplicaciones del producto."),
        ("Vendedor industrial", "Vendedor Industrial",
         "Apoya en el pronóstico y presupuesto; elabora reportes de visita; realiza "
         "visitas; registra el pedido en Odoo; conoce las aplicaciones del producto "
         "industrial/automotriz."),
    ]

    # Actividades del Desarrollo (bloque, sección, numeral, rol, texto,
    # formatos referenciados por clave, procedimiento relacionado, ref Odoo, nota).
    _SGI_VENTAS_ACTIVITIES = [
        ('inicial', "4.1 Actividades iniciales", "4.1.1",
         "Coordinador de ventas", "Antes de generar el pedido se obtiene la "
         "retroalimentación de Crédito y Cobranza conforme al P-A22.",
         [], "P-A22", "", ""),
        ('inicial', "4.1 Actividades iniciales", "4.1.2",
         "Vendedor", "El pedido se realiza conforme al IT-P-A28-01 CREACIÓN DE "
         "PEDIDOS ODOO.", ["IT-P-A28-01"], "", "Ventas > Pedidos", ""),
        ('inicial', "4.1 Actividades iniciales", "4.1.3",
         "Administrador de ventas y marketing", "Para un cliente nuevo se llena el "
         "F-P-A28-21 ALTA DE CLIENTE y se realiza su alta conforme al P-A22.",
         ["F-P-A28-21"], "P-A22", "", ""),
        ('desarrollo', "4.2.1 Pronóstico de ventas", "4.2.1",
         "Coordinador de ventas", "Se elabora el pronóstico de ventas: en confección "
         "el Coordinador de confección con el Ejecutivo de telas usan el F-P-A28-13; "
         "en industrial el Coordinador industrial con el Vendedor industrial usan el "
         "F-P-A31-01. El pronóstico se comparte con el Administrador y con el "
         "Planeador de producción como marca el P-A31.",
         ["F-P-A28-13", "F-P-A31-01"], "P-A31", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.1",
         "Coordinador de ventas entretelas confección",
         "Se elabora el presupuesto de confección en el F-P-A28-17.",
         ["F-P-A28-17"], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.2",
         "Coordinador de ventas industrial",
         "Se elabora el presupuesto industrial en el F-P-A31-02.",
         ["F-P-A31-02"], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.3",
         "Administrador de ventas y marketing",
         "El Administrador revisa el presupuesto y lo envía a alta dirección.",
         [], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.4",
         "Administrador de ventas y marketing", "Se integra el presupuesto general en "
         "el F-P-A28-18 y se envía a alta dirección.", ["F-P-A28-18"], "", "",
         "Nota 1: en junio se revalúa el presupuesto general del segundo semestre. "
         "Nota 2: el horizonte mínimo del presupuesto es de 12 meses."),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.5",
         "Administrador de ventas y marketing",
         "Se comunican los metros a fabricar al Planeador por correo.",
         [], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.6",
         "Administrador de ventas y marketing",
         "Se da seguimiento al presupuesto.", [], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.7",
         "Coordinador de ventas", "Para un producto de línea fuera de pronóstico se "
         "consulta el stock conforme al IT-P-A28-02 CONSULTA DE STOCK y se agrega al "
         "pronóstico.", ["IT-P-A28-02"], "", "", ""),
        ('desarrollo', "4.2.2 Presupuesto de ventas", "4.2.2.8",
         "Coordinador de ventas", "Una vez aprobado, se comunican los metros a "
         "fabricar al Planeador.", [], "", "", ""),
        ('desarrollo', "4.2.3 Cotización de productos", "4.2.3.1",
         "Administrador de ventas y marketing", "Se elabora el F-P-A28-12 COTIZACIÓN "
         "DE PRODUCTO atendiendo los requisitos del cliente (PPAP, CPK, SPC, etc.); "
         "la envía el Administrador.", ["F-P-A28-12"], "", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.1",
         "Vendedor", "Se solicita la información del producto: fichas técnicas, "
         "empaque, tamaño de rollos, volumen, precio target, requisitos legales y "
         "muestra.", [], "", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.2",
         "Vendedor", "La visita se plasma en el F-P-A28-04 REPORTE DE VISITA y el "
         "análisis de mercado en el F-P-A28-16, F-P-A28-15 o F-P-A28-20 según "
         "corresponda; se comparte con Diseño y Desarrollo.",
         ["F-P-A28-04", "F-P-A28-16", "F-P-A28-15", "F-P-A28-20"], "",
         "Helpdesk Servicio Técnico",
         "Las visitas se plasman en el módulo de SERVICIO TÉCNICO de Odoo."),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.3",
         "Coordinador de ventas", "Se revisan los requisitos con Diseño y Desarrollo "
         "conforme a la cláusula ISO 8.2: requisitos legales, medios de comunicación, "
         "criterios de aceptación, contratos/pedidos y sus cambios, quejas conforme al "
         "P-C01, manipulación de información, contingencias, capacidad, entrega y "
         "posteriores a la entrega, requisitos no especificados, requisitos de PNTQ, "
         "crédito conforme al P-A22 y liberación conforme al IT-P-C06-02.",
         [], "P-A22", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.4",
         "Coordinador de ventas", "Diseño comparte el precio de venta y se elabora la "
         "cotización F-P-A28-12 junto con el F-P-D01-09 APROBACIÓN PARA INICIAR UN "
         "PROYECTO, como marca el P-D01.",
         ["F-P-A28-12", "F-P-D01-09"], "P-D01", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.5",
         "Coordinador de ventas", "Las modificaciones al proyecto se gestionan "
         "conforme al P-D01.", [], "P-D01", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.6",
         "Coordinador de ventas", "Se envía al cliente el F-P-D01-11 APROBACIÓN DEL "
         "PROYECTO.", ["F-P-D01-11"], "", "", ""),
        ('desarrollo', "4.2.4 Nuevos productos y visitas", "4.2.4.7",
         "Atención a clientes y vendedores confección", "Las muestras se solicitan con "
         "el F-P-A28-03 SOLICITUD DE MUESTRA al Jefe de calidad por correo.",
         ["F-P-A28-03"], "", "", ""),
        ('final', "4.3.1 Entrega", "4.3.1.1",
         "Vendedor", "Se suben los pedidos a Odoo con su fecha de entrega y orden de "
         "compra.", [], "", "Ventas > Pedidos", ""),
        ('final', "4.3.1 Entrega", "4.3.1.2",
         "Atención a clientes y vendedores confección", "Con stock disponible y la "
         "entrega programada, se canaliza a Logística conforme al P-A16.",
         [], "P-A16", "", ""),
        ('final', "4.3.2 Retroalimentación del cliente", "4.3.2",
         "Administrador de ventas y marketing", "Se canaliza la retroalimentación del "
         "cliente al Administrador.", [], "", "", ""),
        ('final', "4.3.3 Reclamaciones", "4.3.3.1",
         "Coordinador de ventas", "Las reclamaciones se registran en el F-P-A28-19 "
         "RECLAMACIÓN INDUSTRIAL o el F-P-A28-01 RECLAMACIÓN CONFECCIÓN según "
         "corresponda.", ["F-P-A28-19", "F-P-A28-01"], "", "", ""),
        ('final', "4.3.3 Reclamaciones", "4.3.3.2",
         "Coordinador de ventas", "Se atienden conforme al P-C01; los cambios a "
         "diseño se documentan.", [], "P-C01", "", ""),
        ('final', "4.3.3 Reclamaciones", "4.3.3.3",
         "Coordinador de ventas", "Las contingencias de entrega se comunican por "
         "escrito y se atienden conforme al P-C01.", [], "P-C01", "",
         "Las notas de crédito se tramitan conforme al P-C01."),
        ('final', "4.3.4 Pedidos", "4.3.4",
         "Vendedor", "Los pedidos se registran conforme al IT-P-A28-01.",
         ["IT-P-A28-01"], "", "Ventas > Pedidos", ""),
        ('final', "4.3.5 Encuesta de satisfacción", "4.3.5",
         "Administrador de ventas y marketing", "Se aplica anualmente el F-P-A28-11 "
         "ENCUESTA DE SATISFACCIÓN; algunos clientes evalúan con su propio formato, "
         "que se turna a Calidad.", ["F-P-A28-11"], "", "", ""),
        ('final', "4.3.6 Seguimiento y análisis", "4.3.6",
         "Administrador de ventas y marketing", "El Administrador vigila el "
         "cumplimiento de los presupuestos y presenta los indicadores del SGI; ante "
         "incumplimiento se levanta una acción.", [], "", "", ""),
    ]

    _SGI_VENTAS_NORMS = [
        ("ISO 9001:2015", "Sistema de Gestión de la Calidad"),
        ("ISO 14001:2015", "Sistema de Gestión Ambiental"),
        ("ISO 45001:2018", "Sistema de Gestión de SST"),
        ("ISO 31000:2018", "Gestión del riesgo"),
    ]

    def _sgi_vigente_docs_by_codes(self, codes):
        """Documentos controlados vigentes cuyas claves están en la lista (solo
        los que existen; los que no, se conservan en el texto de la actividad)."""
        if not codes:
            return self.env['documents.document']
        return self.env['documents.document'].search([
            ('sgi_code', 'in', list(codes)),
            ('sgi_state', '=', 'vigente'),
            ('sgi_is_controlled', '=', True),
        ])

    @api.model
    def seed_procedure_ventas(self):
        """Captura el P-A28 VENTAS Rev.15 como datos del proceso Ventas.

        Se llama MANUALMENTE en shell (no en data del manifest, para no imponer
        contenido en cada update). Es idempotente: reconstruye el alcance, las
        responsabilidades y las actividades del proceso Ventas. Los formatos se
        enlazan por clave a los documentos controlados vigentes existentes; los
        que no existan quedan sin enlace pero conservados en el texto."""
        process = self.env.ref('quimibond_sgi.proc_ventas', raise_if_not_found=False)
        if not process:
            _logger.warning("SGI piloto P-A28: no existe el proceso 'proc_ventas'.")
            return False

        process.write({
            'purpose': "Establecer la metodología que guíe a las áreas "
                       "involucradas en toda la operación de la venta, así como su "
                       "control, que permita dar seguimiento a las actividades y "
                       "resultados.",
            'scope': "Este procedimiento es aplicable en todas las áreas "
                     "involucradas en la venta de Productora de No Tejidos "
                     "Quimibond.",
            'env_aspects': "Identificación conforme al P-E01 y registro en el "
                           "F-P-E01-01. Aspectos: generación de residuos (basura "
                           "orgánica e inorgánica), uso de energía eléctrica "
                           "(oficina y equipos) y consumo de recursos naturales "
                           "renovables (papelería y promocionales).",
        })

        # Marco normativo (get-or-create; ISO 31000 no viene en la data base).
        Norm = self.env['sgi.norm']
        norm_ids = []
        for code, name in self._SGI_VENTAS_NORMS:
            norm = Norm.search([('code', '=', code)], limit=1)
            if not norm:
                norm = Norm.create({'code': code, 'name': name})
            norm_ids.append(norm.id)
        process.norm_ids = [(6, 0, norm_ids)]

        # Reconstrucción idempotente de responsabilidades y actividades.
        process.job_responsibility_ids.unlink()
        process.activity_ids.unlink()

        Job = self.env['hr.job']
        resp_vals = []
        for seq, (role, job_name, text) in enumerate(
                self._SGI_VENTAS_RESPONSIBILITIES, start=1):
            job = Job.search([('name', '=', job_name)], limit=1)
            if not job:
                job = Job.create({'name': job_name})
            resp_vals.append({
                'process_id': process.id, 'sequence': seq * 10,
                'job_id': job.id, 'name': role, 'responsibilities': text,
            })
        self.env['sgi.process.responsibility'].create(resp_vals)

        act_vals = []
        for seq, (block, section, number, role, desc, fmt_codes, related,
                  odoo_ref, note) in enumerate(self._SGI_VENTAS_ACTIVITIES, start=1):
            fmt_docs = self._sgi_vigente_docs_by_codes(fmt_codes)
            related_doc = self.env['documents.document'].search([
                ('sgi_code', '=', related), ('sgi_state', '=', 'vigente'),
                ('sgi_doc_type', '=', 'procedimiento'),
            ], limit=1) if related else self.env['documents.document']
            act_vals.append({
                'process_id': process.id, 'sequence': seq * 10, 'block': block,
                'section': section, 'number': number, 'name': desc[:80],
                'description': desc, 'responsible_role': role,
                'format_document_ids': [(6, 0, fmt_docs.ids)],
                'related_procedure_id': related_doc.id or False,
                'odoo_ref': odoo_ref or False, 'note': note or False,
            })
        self.env['sgi.process.activity'].create(act_vals)

        _logger.info(
            "SGI piloto P-A28: proceso Ventas cargado — %d responsabilidades, "
            "%d actividades.", len(resp_vals), len(act_vals))
        return True

    @api.model
    def seed_process_purposes(self):
        for xmlid, text in self._SGI_PROCESS_PURPOSES.items():
            process = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if process and not process.purpose:
                process.purpose = text
        return True

    @api.model
    def harden_noupdate(self):
        """Marca noupdate=True en registros que ya existían ANTES de que su
        archivo pasara a noupdate=1 (crons, mapeo de claves): sin esto, el
        odoo-update seguiría revirtiendo lo que el usuario edite ahí."""
        self.env['ir.model.data'].sudo().search([
            ('module', '=', 'quimibond_sgi'),
            ('model', 'in', ('ir.cron', 'sgi.format.map')),
            ('noupdate', '=', False),
        ]).write({'noupdate': True})
        return True


class SgiFormatMixin(models.AbstractModel):
    """Agrega al modelo la clave del formato SGI que sustituye (pantalla y PDF)."""
    _name = 'sgi.format.mixin'
    _description = "Mixin: clave de formato SGI"

    sgi_format_banner = fields.Char(
        string="Formato SGI", compute='_compute_sgi_format_banner')

    def _sgi_format_applies(self):
        """Si este registro en particular porta la clave (hook por modelo)."""
        self.ensure_one()
        return True

    def _sgi_format_code(self, fmap):
        """Clave a usar para este registro (hook por modelo)."""
        self.ensure_one()
        return fmap.sgi_code

    def sgi_format_info(self):
        """'F-P-A28-04 · Rev. 03' | 'F-P-A28-04' (sin doc vigente) | False."""
        self.ensure_one()
        fmap = self.env['sgi.format.map'].sudo()._get_for_model(self._name)
        if not fmap or not self._sgi_format_applies():
            return False
        code = self._sgi_format_code(fmap)
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code

    def _compute_sgi_format_banner(self):
        for record in self:
            record.sgi_format_banner = record.sgi_format_info()


# --- Aplicación del mixin a los modelos mapeados -----------------------------

class SaleOrder(models.Model):
    _name = 'sale.order'
    _inherit = ['sale.order', 'sgi.format.mixin']

    def _sgi_format_code(self, fmap):
        self.ensure_one()
        if fmap.sgi_code_alt and self.state == 'sale':
            return fmap.sgi_code_alt
        return fmap.sgi_code


class PurchaseOrder(models.Model):
    _name = 'purchase.order'
    _inherit = ['purchase.order', 'sgi.format.mixin']


class StockPicking(models.Model):
    _name = 'stock.picking'
    _inherit = ['stock.picking', 'sgi.format.mixin']

    def _sgi_format_applies(self):
        self.ensure_one()
        return self.picking_type_code == 'outgoing'


class MrpProduction(models.Model):
    _name = 'mrp.production'
    _inherit = ['mrp.production', 'sgi.format.mixin']


class MaintenanceRequest(models.Model):
    _name = 'maintenance.request'
    _inherit = ['maintenance.request', 'sgi.format.mixin']


class QualityAlert(models.Model):
    _name = 'quality.alert'
    _inherit = ['quality.alert', 'sgi.format.mixin']


class StockLot(models.Model):
    _name = 'stock.lot'
    _inherit = ['stock.lot', 'sgi.format.mixin']


class SgiManagementReview(models.Model):
    _name = 'sgi.management.review'
    _inherit = ['sgi.management.review', 'sgi.format.mixin']
