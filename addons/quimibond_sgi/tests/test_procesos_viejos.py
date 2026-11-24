# -*- coding: utf-8 -*-
"""57.4.0 (A-002, B-002): el SGI se instala sin procesos (decisión 6); el
mapa viejo salió del módulo. Las pruebas de la migración de 57.4.0 se
retiraron en 57.202.2: producción ya pasó por ella y no vuelve a correr."""

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestProcesosViejos(TransactionCase):

    def test_03_module_ships_no_processes(self):
        """Los datos del módulo no traen procesos ni flujos."""
        from odoo.tools.misc import file_open
        with file_open('quimibond_sgi/__manifest__.py') as handle:
            manifest = handle.read()
        self.assertNotIn("'data/sgi_process_data.xml'", manifest)
        self.assertNotIn("'data/sgi_process_flows_extra.xml'", manifest)
