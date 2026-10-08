# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase

from ..models.settings import PARAM_APROBADOR, PARAM_VENTAS


class CotizadorCase(TransactionCase):
    """Un vendedor, quien aprueba (por puesto), Ventas (por puesto), el CEO
    (grupo bajo piso), un producto con costo en un período cerrado."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        grp_sales = cls.env.ref('sales_team.group_sale_salesman')
        grp_mgr = cls.env.ref('sales_team.group_sale_manager')
        cls.vendedor = Users.create({'name': 'Vendedor prueba', 'login': 'qbcot_vendedor',
                                     'group_ids': [(6, 0, [grp_sales.id])]})
        cls.finanzas = Users.create({'name': 'Finanzas prueba', 'login': 'qbcot_finanzas',
                                     'group_ids': [(6, 0, [grp_sales.id])]})
        cls.ventas = Users.create({'name': 'Ventas prueba', 'login': 'qbcot_ventas',
                                   'group_ids': [(6, 0, [grp_sales.id])]})
        cls.ceo = Users.create({'name': 'CEO prueba', 'login': 'qbcot_ceo',
                                'group_ids': [(6, 0, [grp_mgr.id,
                                                      cls.env.ref('qb_cotizador.group_autoriza_bajo_piso').id])]})
        Job = cls.env['hr.job']
        cls.job_finanzas = Job.create({'name': 'DIRECTOR DE FINANZAS PRUEBA'})
        cls.job_ventas = Job.create({'name': 'ADMINISTRADOR DE VENTAS PRUEBA'})
        Emp = cls.env['hr.employee']
        Emp.create({'name': 'Finanzas prueba', 'user_id': cls.finanzas.id, 'job_id': cls.job_finanzas.id})
        Emp.create({'name': 'Ventas prueba', 'user_id': cls.ventas.id, 'job_id': cls.job_ventas.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_APROBADOR, str(cls.job_finanzas.id))
        Param.set_param(PARAM_VENTAS, str(cls.job_ventas.id))

        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE COTIZADOR PRUEBA', 'is_company': True})
        m = cls.env.ref('uom.product_uom_meter')
        cls.tela = cls.env['product.product'].create({
            'name': 'Tela cotizador', 'default_code': 'WJ080Q21JNT165', 'type': 'consu',
            'uom_id': m.id, 'list_price': 15.0})
        cls.hermana = cls.env['product.product'].create({
            'name': 'Tela hermana', 'default_code': 'WJ090Q21JNT165', 'type': 'consu',
            'uom_id': m.id, 'list_price': 16.0})
        cls.periodo = cls.env['qb.periodo'].create({'period': date(2026, 8, 1), 'company_id': cls.company.id})
        cls.periodo.write({'state': 'cerrado', 'op_pct': 0.10})
        cls.costo = cls._costo(cls.tela, mp=6.0, energia=1.0, fab=3.0, rend=0.9, precio=14.0)
        cls._costo(cls.hermana, mp=7.0, energia=1.0, fab=3.0, rend=1.0, precio=0.0)

    @classmethod
    def _costo(cls, product, mp, energia, fab, rend, precio):
        return cls.env['qb.costo.unitario'].create({
            'periodo_id': cls.periodo.id, 'product_id': product.id, 'company_id': cls.company.id,
            'mp_unit': mp, 'energia_unit': energia, 'fabricacion_unit': fab,
            'costo_variable': mp + energia, 'costo_produccion': mp + fab, 'rendimiento': rend,
            'op_pct': 0.10, 'calidad': 'alta', 'calidad_detalle': 'prueba', 'precio_prom': precio})

    def _cot(self, user=None, **vals):
        base = {'partner_id': self.partner.id, 'product_id': self.tela.id, 'volumen': 1000,
                'precio_objetivo': 14.0}
        base.update(vals)
        return self.env['qb.cotizador.cotizacion'].with_user(user or self.vendedor).create(base)

    def _presentada(self, **vals):
        cot = self._cot(**vals)
        cot.action_calcular()
        cot.action_enviar_aprobacion()
        cot.with_user(self.finanzas).action_aprobar()
        return cot
