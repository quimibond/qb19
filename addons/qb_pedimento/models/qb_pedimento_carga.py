# -*- coding: utf-8 -*-
"""Cargar el expediente completo de un pedimento de una vez.

Las agencias mandan el expediente en varios archivos o en un ZIP
(«REF. 82194-51473-MZ.zip», «26-03-26.zip») y a veces un solo PDF trae
varios documentos («INVOICE AND PL BIANCO.pdf»). El asistente recibe todos
los archivos, abre los ZIP, propone a qué documento del checklist va cada
uno por su nombre (patrón de cada tipo) y avisa si el nombre trae el número
de otro pedimento. Quien carga revisa la propuesta y guarda: cada archivo
queda en el expediente y en los renglones del checklist que se le asignaron.
"""
import base64
import io
import re
import zipfile

from odoo import _, api, fields, models
from odoo.exceptions import UserError

# Un ZIP del expediente trae decenas de archivos, no miles; el tope evita que
# un ZIP equivocado (o una bomba ZIP) llene la base.
ZIP_MAX_ARCHIVOS = 200
ZIP_MAX_BYTES = 100 * 1024 * 1024


def consecutivos_en_nombre(nombre):
    """Consecutivos de pedimento (7 dígitos) que trae el nombre del archivo.
    Reconoce «26-07-1612-6000590», «261637746000206», «17404306000661NORMAL»
    (patente + sección + consecutivo) y «Doda5001894»: corridas de 7, 14 o 15
    dígitos, de las que se toman los últimos 7."""
    return {run[-7:] for run in re.findall(r'\d+', nombre or '') if len(run) in (7, 14, 15)}


class QbPedimentoCarga(models.TransientModel):
    _name = 'qb.pedimento.carga'
    _description = 'Cargar expediente de pedimento'

    pedimento_id = fields.Many2one('qb.pedimento', required=True, ondelete='cascade')
    archivo_ids = fields.Many2many(
        'ir.attachment', 'qb_pedimento_carga_attachment_rel', 'carga_id', 'attachment_id', 'Archivos',
        help='Todos los archivos del expediente: PDF, XML o un ZIP con todo.')
    expediente_unico = fields.Boolean(
        'Es un solo archivo con todo el expediente',
        help='Marca los archivos como el expediente completo: cubren todos los documentos que pide la operación.')
    linea_ids = fields.One2many('qb.pedimento.carga.linea', 'carga_id', 'Archivos a guardar')
    estado = fields.Selection([('subir', 'Subir'), ('revisar', 'Revisar')], default='subir')
    tipo_disponible_ids = fields.Many2many(
        'qb.pedimento.documento.tipo', compute='_compute_tipo_disponible_ids', string='Documentos de la operación')

    @api.depends('pedimento_id')
    def _compute_tipo_disponible_ids(self):
        for rec in self:
            rec.tipo_disponible_ids = rec._tipos_aplicables() if rec.pedimento_id else False

    def _tipos_aplicables(self):
        self.ensure_one()
        ped = self.pedimento_id
        return self.env['qb.pedimento.documento.tipo'].search([])._applies_to(ped.operacion, ped.virtual)

    def _expandir(self):
        """[(nombre, bytes)] de los archivos subidos, con los ZIP abiertos."""
        archivos = []
        for att in self.archivo_ids:
            datos = base64.b64decode(att.datas or b'')
            if not (att.name or '').lower().endswith('.zip'):
                archivos.append((att.name, datos))
                continue
            try:
                zf = zipfile.ZipFile(io.BytesIO(datos))
            except zipfile.BadZipFile:
                raise UserError(_('«%s» no es un ZIP válido.', att.name))
            miembros = [m for m in zf.infolist()
                        if not m.is_dir() and not m.filename.startswith('__MACOSX/')
                        and not m.filename.rsplit('/', 1)[-1].startswith('.')]
            if len(miembros) > ZIP_MAX_ARCHIVOS or sum(m.file_size for m in miembros) > ZIP_MAX_BYTES:
                raise UserError(_('«%(z)s» trae demasiados archivos o pesa demasiado (máximo %(n)s archivos y '
                                  '%(mb)s MB). Súbalo en partes.', z=att.name, n=ZIP_MAX_ARCHIVOS,
                                  mb=ZIP_MAX_BYTES // (1024 * 1024)))
            for m in miembros:
                archivos.append((m.filename.rsplit('/', 1)[-1], zf.read(m)))
        return archivos

    def action_clasificar(self):
        self.ensure_one()
        if not self.archivo_ids:
            raise UserError(_('Suba al menos un archivo.'))
        tipos = self._tipos_aplicables()
        propio = (self.pedimento_id.numero or '')[-7:]
        lineas = [fields.Command.clear()]
        for nombre, datos in self._expandir():
            if self.expediente_unico:
                asignados = tipos
            else:
                asignados = tipos.filtered(lambda t: t._reconoce(nombre))
            otros = consecutivos_en_nombre(nombre) - {propio}
            aviso = False
            if propio and otros and propio not in consecutivos_en_nombre(nombre):
                aviso = _('El nombre trae el pedimento %s, no el de este expediente.', ', '.join(sorted(otros)))
            lineas.append(fields.Command.create({
                'nombre': nombre,
                'datos': base64.b64encode(datos),
                'tipo_ids': [fields.Command.set(asignados.ids)],
                'aviso': aviso,
            }))
        self.write({'linea_ids': lineas, 'estado': 'revisar'})
        # Los archivos subidos ya se copiaron a las líneas: no se quedan huérfanos.
        self.archivo_ids.unlink()
        return self._reabrir()

    def action_guardar(self):
        self.ensure_one()
        ped = self.pedimento_id
        Documento = self.env['qb.pedimento.documento']
        guardados = 0
        for linea in self.linea_ids.filtered(lambda l: not l.omitir):
            att = self.env['ir.attachment'].create({
                'name': linea.nombre,
                'datas': linea.datos,
                'res_model': 'qb.pedimento',
                'res_id': ped.id,
            })
            guardados += 1
            for tipo in linea.tipo_ids:
                doc = ped.documento_ids.filtered(lambda d: d.tipo_id == tipo)
                if not doc:
                    doc = Documento.create({'pedimento_id': ped.id, 'tipo_id': tipo.id})
                doc.write({'attachment_ids': [fields.Command.link(att.id)], 'no_aplica': False})
        sin_tipo = self.linea_ids.filtered(lambda l: not l.omitir and not l.tipo_ids)
        ped.message_post(body=_(
            'Se cargaron %(n)s archivos al expediente%(extra)s.', n=guardados,
            extra=_(' (%s sin documento asignado: quedan en los adjuntos)', len(sin_tipo)) if sin_tipo else ''))
        return {'type': 'ir.actions.act_window_close'}

    def _reabrir(self):
        return {
            'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
            'view_mode': 'form', 'target': 'new', 'name': _('Cargar expediente'),
        }


class QbPedimentoCargaLinea(models.TransientModel):
    _name = 'qb.pedimento.carga.linea'
    _description = 'Archivo a cargar en el expediente'

    carga_id = fields.Many2one('qb.pedimento.carga', required=True, ondelete='cascade')
    nombre = fields.Char('Archivo', required=True)
    datos = fields.Binary(attachment=False)
    tipo_ids = fields.Many2many(
        'qb.pedimento.documento.tipo', 'qb_pedimento_carga_linea_tipo_rel', 'linea_id', 'tipo_id',
        'Documento(s)', help='Un mismo archivo puede cubrir varios documentos (p.ej. factura y packing list).')
    aviso = fields.Char(readonly=True)
    omitir = fields.Boolean('No guardar')

    @api.depends('nombre')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = rec.nombre
