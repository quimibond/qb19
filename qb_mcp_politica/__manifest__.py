# -*- coding: utf-8 -*-
{
    'name': 'Quimibond — Política del MCP',
    'version': '19.0.1.1.0',
    'license': 'OPL-1',
    'category': 'Technical',
    'summary': 'Candado en código para el MCP: modelos técnicos en solo lectura y modelos con secretos fuera del MCP (auditoría SGI, F-001).',
    'description': """
Política del MCP
================

Encima de la regla por modelo de ``mcp_server`` (Ajustes → Técnico → MCP →
MCP Available Models) impone, para todos los usuarios del MCP:

* **Solo lectura** en modelos técnicos: todo ``ir.*``, grupos, usuarios,
  compañías, plantillas de correo, Studio, tableros, recorridos, IAP.
* **Desactivados** (ni leer) los modelos con secretos: parámetros del
  sistema, llaves API, TOTP, OAuth, servidores de correo entrante, cuenta
  IAP, bitácora y bajas de usuarios.

* **Sin borrado en el SGI** (D-22): ``unlink`` en cualquier modelo ``sgi.*``
  se rechaza; se archiva (``active = False``).

No agrega pantallas ni modifica datos al instalarse. Se revierte
desinstalando el módulo. Ver README.md.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['mcp_server'],
    'data': [],
    'installable': True,
    'application': False,
    'auto_install': False,
}
