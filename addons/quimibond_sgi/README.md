# quimibond_sgi — Sistema de Gestión Integral (Fase 1)

Addon de Odoo 19 Enterprise que lleva el SGI documental de PNTQ (ISO 9001:2015 +
14001:2015, 45001 en preparación) a Odoo **extendiendo apps nativas** (Documentos,
Aprobaciones, Calidad, Helpdesk, Proyecto) y creando solo lo que Odoo no tiene
(mapa de procesos, acuses de lectura, catálogos SGI). Toda la lógica vive en el
módulo: **cero Studio, cero automation rules de UI**.

## Qué instala/configura el módulo (automático)

- **Grupos de seguridad** (privilegio "SGI"): Usuario SGI, Auditor SGI,
  Jefe MAST y SGI, Dirección de Operaciones (SGI). Cada uno implica los grupos
  base de las apps que toca (Calidad, Documentos, Helpdesk, Proyecto, Aprobaciones).
- **Catálogos**: 10 áreas documentales (G, A, C, D, E, I, M, P, S, V); normas
  ISO 9001/14001/45001 con cláusulas 4.1–10.3; secuencias anuales de folio de NC
  (`NCI-AAAA-####` / `NCE-AAAA-####`).
- **Mapa de procesos**: 5 macroprocesos + 16 procesos nivel 2 + la cadena de flujos
  COP (Crédito y Cobranza → Ventas → Planeación → … → Facturación → CxC).
- **No Conformidades** (sobre `quality.alert`): equipos "NC Internas" y "NC Externas"
  con etapas Abierta → Seguimiento → Cerrada + Cancelada; folio anual; 5 porqués;
  líneas de acción; **candados de cierre** (causa raíz + acciones terminadas +
  verificación de eficacia) con cierre forzado exclusivo del Jefe MAST.
- **Control documental** (sobre `documents.document`): clave validada con la
  nomenclatura real, tipo, área, revisión, estado (Borrador/Piloto/Vigente/Obsoleto),
  puestos aplicables, próxima revisión bienal, **acuses de lectura** con % de difusión.
- **Cambios documentales** (sobre `approval.request`): categoría "Modificación de
  documento SGI" (alta/modificación/baja, prueba piloto ≤90 días, procesos afectados);
  al aprobarse versiona el documento y dispara acuses.
- **Reclamaciones** (sobre `helpdesk.ticket`): equipo "Reclamaciones de clientes"
  con etapas y política SLA de primera respuesta; botón "Generar NC".
- **Mejora continua** (sobre `project.task`): proyecto "Mejora Continua SGI" con
  candado de cierre (fecha límite + evidencia adjunta).
- **Crons** (diario NC, diario documentos, mensual NEWS) y **reportes** QWeb
  F-P-G05-01 (NC) y F-P-G01-16 (NEWS).
- **Integraciones**: smart buttons de NC en picking/producción/contacto,
  "Mis procedimientos"/"Acuses pendientes" en empleado, documentos aplicables en puesto.

## Checklist de puesta en marcha (manual, en la instancia)

1. **Usuarios en los grupos SGI** (Ajustes → Usuarios):
   - Jefe de MAST y SGI → grupo **Jefe MAST y SGI**.
   - Dirección de Operaciones → grupo **Dirección de Operaciones (SGI)**.
   - Auditores → **Auditor SGI**; resto del personal operativo → **Usuario SGI**.
2. **Aprobadores de la categoría** "Modificación de documento SGI"
   (Aprobaciones → Configuración → Categorías): agregar los 2 aprobadores en
   secuencia (Jefe MAST y SGI → Dirección de Operaciones) y marcar el mínimo.
3. **Helpdesk**: activar/ajustar la **política SLA** del equipo "Reclamaciones de
   clientes" y configurar el **alias de correo** del equipo si se usará entrada por email.
4. **Documents**: crear (si no existe) el árbol de **carpetas** espejo de las
   secciones 00–23 del SGI y asignar permisos por área. Los documentos controlados
   se registran con su clave y revisión vigente.
5. **Parámetro** `quimibond_sgi.nc_escalation_days` (Ajustes → Técnico → Parámetros
   del sistema): días para escalar NC internas sin acción (por defecto 5; las
   externas/reclamación escalan a 3 por regla de negocio).
6. **Empleados** con su **puesto** (`hr.job`) correcto: los acuses y "Mis
   procedimientos" filtran por puesto.
7. En dev/staging los **crons están desactivados**: pruébalos con
   Ajustes → Técnico → Acciones planificadas → "Ejecutar manualmente".

## Instalación / actualización (shell Odoo.sh)

```bash
odoo-update quimibond_sgi && odoosh-restart http
```

> **NO** cambiar la versión del manifest de `quimibond_intelligence` ni de otros
> módulos del repo (un bump dispara `-u` global que falla por errores preexistentes
> de Studio).

## Tests

```bash
odoo-bin --test-tags /quimibond_sgi -u quimibond_sgi --stop-after-init --no-http
```

Cubren: secuencias de folio, candados de cierre de NC (+ cierre forzado),
validación de clave documental, obsoletización de versión vigente previa, acuses
idempotentes, piloto >90 días, aprobación que versiona el documento, reclamación → NC
y validación del mapa de procesos.
