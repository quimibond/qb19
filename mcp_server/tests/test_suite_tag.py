# -*- coding: utf-8 -*-
"""Las pruebas del proveedor no entran al tag que corre Odoo.sh.

``standard`` se ejecuta solo en cada build de rama de desarrollo, sobre
una copia de producción. Estas pruebas no están hechas para esa base.
"""
import inspect

from odoo.tests import TransactionCase, tagged
from odoo.tests.common import BaseCase


@tagged('post_install', '-at_install', 'mcp_suite')
class TestMcpNoEsStandard(TransactionCase):

    def test_el_proveedor_no_trae_standard(self):
        import odoo.addons.mcp_server.tests as paquete
        mal = []
        for nombre, mod in vars(paquete).items():
            if not inspect.ismodule(mod):
                continue
            for cls_nombre, obj in vars(mod).items():
                if not inspect.isclass(obj) or not issubclass(obj, BaseCase):
                    continue
                if obj.__module__ != mod.__name__:
                    continue
                tags = getattr(obj, 'test_tags', set()) or set()
                if 'standard' in tags and 'mcp_suite' not in tags:
                    mal.append('%s.%s' % (nombre, cls_nombre))
        self.assertFalse(mal, 'siguen en standard: %s' % ', '.join(mal))
