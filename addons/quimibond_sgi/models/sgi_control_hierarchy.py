# -*- coding: utf-8 -*-
"""57.96.0 (N-06, ISO 45001 8.1.2): jerarquía de controles.

Módulo sin modelos (patrón de ``sgi_guard.py``): lo importan ``sgi_risk.py`` y
``sgi_nonconformity.py`` sin mover el orden del registro. El orden de la
lista es el de la norma: primero eliminar el peligro; el equipo de
protección personal es el último recurso."""

CONTROL_HIERARCHY = [
    ('eliminacion', "1. Eliminación"),
    ('sustitucion', "2. Sustitución"),
    ('ingenieria', "3. Controles de ingeniería"),
    ('administrativo', "4. Controles administrativos (procedimientos, señalización, capacitación)"),
    ('epp', "5. Equipo de protección personal (EPP)"),
]

CONTROL_HIERARCHY_HELP = (
    "Jerarquía de controles de ISO 45001 8.1.2: eliminación, sustitución, controles de "
    "ingeniería, controles administrativos y, como último recurso, equipo de protección "
    "personal. Un IPER de riesgo alto no se controla ni se cierra si su único control es EPP.")
