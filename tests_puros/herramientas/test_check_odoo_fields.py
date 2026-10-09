# -*- coding: utf-8 -*-
"""Pytest puro de tools/check_odoo_fields.py: el checker que coteja los campos
de las vistas contra los modelos (nació de `lot_producing_id`, que en Odoo 19
es `lot_producing_ids` y tumbó el build de producción el 2026-10-09).

Se arma un módulo sintético en un directorio temporal: modelos con mixin,
delegación y comodelos, y vistas con los casos que el checker debe resolver.
"""
import os
import sys
import textwrap

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, 'tools'))
import check_odoo_fields as cof  # noqa: E402

MODELS = textwrap.dedent('''
    from odoo import fields, models

    class Thread(models.AbstractModel):
        _name = 'demo.thread'
        message_ids = fields.One2many('demo.message', 'res_id')

    class Message(models.Model):
        _name = 'demo.message'
        res_id = fields.Integer()
        body = fields.Text()

    class Lot(models.Model):
        _name = 'demo.lot'
        name = fields.Char()

    class Production(models.Model):
        _name = 'demo.production'
        _inherit = ['demo.thread']
        name = fields.Char()
        lot_producing_ids = fields.Many2many('demo.lot')
        state = fields.Selection([('a', 'A')])

    class Project(models.Model):
        _name = 'demo.project'
        name = fields.Char()
        partner_id = fields.Many2one(comodel_name='demo.partner')
        mo_ids = fields.One2many('demo.production', 'project_id')

    class ProjectExt(models.Model):
        _inherit = 'demo.project'
        extra = fields.Char()

    class Partner(models.Model):
        _name = 'demo.partner'
        name = fields.Char()

    class Variant(models.Model):
        _name = 'demo.variant'
        _inherits = {'demo.partner': 'partner_id'}
        partner_id = fields.Many2one('demo.partner', required=True)
        code = fields.Char()

    class Foreign(models.Model):
        _name = 'ext.thing'
        _inherit = 'ext.thing'   # extensión de un modelo del que no hay código
        mine = fields.Char()
''')

VIEWS = textwrap.dedent('''
    <odoo>
      <record id="project_form" model="ir.ui.view">
        <field name="name">demo.project.form</field>
        <field name="model">demo.project</field>
        <field name="arch" type="xml">
          <form>
            <field name="name"/>
            <field name="extra"/>
            <field name="x_studio_lo_que_sea"/>
            <field name="nope"/>
            <field name="mo_ids">
              <list>
                <field name="name"/>
                <field name="lot_producing_id"/>
                <field name="lot_producing_ids" widget="many2many_tags"/>
                <field name="message_ids"/>
              </list>
            </field>
            <field name="partner_id" context="{'x': 1}"/>
          </form>
        </field>
      </record>
      <record id="project_form_inherit" model="ir.ui.view">
        <field name="name">demo.project.form.inherit</field>
        <field name="model">demo.project</field>
        <field name="inherit_id" ref="project_form"/>
        <field name="arch" type="xml">
          <xpath expr="//field[@name='mo_ids']/list/field[@name='name']" position="after">
            <field name="state"/>
            <field name="missing_in_production"/>
          </xpath>
          <xpath expr="//field[@name='mo_ids']" position="inside">
            <field name="lot_producing_ids"/>
          </xpath>
          <field name="name" position="after">
            <field name="extra"/>
            <field name="missing_in_project"/>
          </field>
          <xpath expr="//field[@name='partner_id']" position="attributes">
            <attribute name="readonly">1</attribute>
          </xpath>
        </field>
      </record>
      <record id="variant_list" model="ir.ui.view">
        <field name="name">demo.variant.list</field>
        <field name="model">demo.variant</field>
        <field name="arch" type="xml">
          <list>
            <field name="code"/>
            <field name="name"/>
            <field name="allowed_by_file"/>
          </list>
        </field>
      </record>
      <record id="foreign_form" model="ir.ui.view">
        <field name="name">ext.thing.form</field>
        <field name="model">ext.thing</field>
        <field name="arch" type="xml">
          <form><field name="whatever"/><field name="mine"/></form>
        </field>
      </record>
      <record id="search" model="ir.ui.view">
        <field name="name">demo.production.search</field>
        <field name="model">demo.production</field>
        <field name="arch" type="xml">
          <search>
            <field name="name"/>
            <filter name="not_a_field" string="x" domain="[]"/>
            <group><filter name="group_state" context="{'group_by': 'state'}"/></group>
          </search>
        </field>
      </record>
    </odoo>
''')


@pytest.fixture
def module(tmp_path):
    mod = tmp_path / 'demo_module'
    (mod / 'models').mkdir(parents=True)
    (mod / 'views').mkdir()
    (mod / '__manifest__.py').write_text("{'name': 'demo', 'data': ['views/views.xml']}")
    (mod / 'models' / 'models.py').write_text(MODELS)
    (mod / 'views' / 'views.xml').write_text(VIEWS)
    return mod


def _run(module, allow=()):
    index = cof.FieldIndex()
    index.add_tree(str(module))
    checker = cof.ViewChecker(index, set(allow))
    checker.check_module(str(module))
    return index, checker


def _missing(checker):
    return sorted(e.split('el campo «')[1].split('»')[0] for e in checker.errors)


def test_indice_resuelve_mixins_delegacion_y_extensiones(module):
    index, _ = _run(module)
    prod = index.fields('demo.production')
    assert 'message_ids' in prod, 'el mixin aporta sus campos'
    assert 'lot_producing_ids' in prod and 'lot_producing_id' not in prod
    assert 'id' in prod and 'write_uid' in prod, 'los campos mágicos siempre existen'
    assert 'extra' in index.fields('demo.project'), 'una extensión sin _name suma campos'
    assert 'name' in index.fields('demo.variant'), '_inherits delega los campos del padre'
    assert index.comodel('demo.project', 'mo_ids') == 'demo.production'
    assert index.comodel('demo.project', 'partner_id') == 'demo.partner', 'comodel_name= también cuenta'
    assert not index.known('ext.thing'), 'un modelo solo extendido no se conoce'


def test_detecta_el_campo_que_tumbo_produccion_y_respeta_los_contextos(module):
    _, checker = _run(module)
    faltantes = _missing(checker)
    assert faltantes == ['allowed_by_file', 'lot_producing_id', 'missing_in_production', 'missing_in_project', 'nope']
    # Lo que NO debe marcar: x_studio_*, campos del mixin en la lista incrustada, hermanos de un
    # <field position="after"> (siguen en el modelo del padre), un xpath que baja al comodelo y
    # otro con position="attributes", campos de un modelo del que no hay código, filtros de búsqueda.
    assert 'x_studio_lo_que_sea' not in faltantes
    assert 'message_ids' not in faltantes
    assert 'extra' not in faltantes and 'state' not in faltantes and 'lot_producing_ids' not in faltantes
    assert 'whatever' not in faltantes and 'not_a_field' not in faltantes and 'group_state' not in faltantes
    assert 'ext.thing' in checker.unknown_models
    error_prod = [e for e in checker.errors if 'lot_producing_id' in e][0]
    assert 'demo.production' in error_prod and 'views/views.xml:' in error_prod


def test_lista_blanca(module):
    _, checker = _run(module, allow={'demo.variant.allowed_by_file'})
    assert 'allowed_by_file' not in _missing(checker)


def test_sin_codigo_del_mixin_el_modelo_no_se_revisa(module):
    # Si el mixin no está definido (p. ej. mail.thread sin el código de Odoo), el modelo que lo
    # hereda queda incompleto y se omite en vez de dar falsos positivos.
    (module / 'models' / 'models.py').write_text(MODELS.replace("_name = 'demo.thread'", "_inherit = 'demo.thread'"))
    index, checker = _run(module)
    assert not index.complete('demo.production')
    assert 'lot_producing_id' not in _missing(checker)
    assert 'demo.production' in checker.unknown_models
    assert 'nope' in _missing(checker), 'los modelos completos se siguen revisando'
