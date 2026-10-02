# El SGI de Quimibond en Odoo

El Sistema de Gestión Integral (calidad, ambiente, seguridad y salud en el
trabajo) de Productora de No Tejidos Quimibond vive en Odoo, en el menú
**SGI**. Lo que antes eran procedimientos en PDF y formatos en Excel en el
Dropbox hoy son **procesos y actividades**: cada actividad dice quién la hace,
cuándo vence, dónde se hace en Odoo y cómo se comprueba que se hizo.

Normas (organismo SIDE Certificaciones, acreditado ante la ema): ISO 9001:2015
certificada desde 2021; ISO 14001:2015 certificada desde 2023; ISO 45001:2018
en certificación (auditoría de septiembre de 2026, resultado pendiente). IATF
16949 no está certificada.

## ¿Quién es usted? Qué leer

| Si usted es… | Lea |
|---|---|
| Operador, supervisor o cualquier persona con usuario de Odoo, **o que firma en la tableta de planta** | [usuarios/operador-o-supervisor.md](usuarios/operador-o-supervisor.md) |
| Jefe de área o dueño de un proceso | [usuarios/jefe-de-area.md](usuarios/jefe-de-area.md) |
| Jefe MAST y SGI (día a día) | [usuarios/mast.md](usuarios/mast.md) y, para configurar, [administracion/manual-jefe-mast.md](administracion/manual-jefe-mast.md) |
| Dirección | [usuarios/direccion.md](usuarios/direccion.md) |
| RH (fichas de empleados, PIN, eficiencias) | [usuarios/rh.md](usuarios/rh.md) |
| Auditor interno o externo | [usuarios/auditor.md](usuarios/auditor.md) |
| Alguien que busca un formato o procedimiento del Dropbox | [transicion/del-dropbox-a-odoo.md](transicion/del-dropbox-a-odoo.md) |
| Programador o administrador del sistema | [tecnica/README.md](tecnica/README.md) |

## Dónde está cada cosa

- `usuarios/`: un manual por perfil, con sus tareas del día.
- `administracion/`: el manual del Jefe MAST y SGI para configurar y mantener
  el sistema.
- `transicion/`: cómo pasar del Dropbox a Odoo y qué pasó con cada
  procedimiento; el bloque 3 de formatos (duplicados, responsables y clave
  nueva) y lo que queda para MAST, en
  [transicion/formatos-bloque-3.md](transicion/formatos-bloque-3.md).
- `tecnica/`: documentación generada del código (modelos, campos, menús,
  seguridad, crons, parámetros).

**Versión documentada:** `quimibond_sgi` 19.0.57.37.0 (30-sep-2026). Las rutas
de menú salen del árbol vigente (`addons/quimibond_sgi/tools/sgi_menu_tree.txt`).
Las capturas de pantalla están **pendientes**: se tomarán en staging con un
usuario de prueba por perfil.

**Publicación:** la fuente de los manuales es este repositorio. Su
publicación en Conocimiento, ligada desde el menú del SGI (decisiones D-26 y
D-27), está pendiente de definir el mecanismo.

**¿Encontró un error en un manual?** Avise al Jefe MAST y SGI, que es quien
mantiene los manuales de usuario. Los técnicos se regeneran con
`python3 tools/sgi_docs.py`.
