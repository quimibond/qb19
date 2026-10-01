# Manual del auditor

Para quien tiene el grupo **Auditor SGI** (auditor interno o externo). El
Auditor **lee** todo el SGI y los registros de los procesos auditados, y
**solo escribe** la auditoría (programa, reuniones, checklist, informe) y sus
hallazgos. No ve exámenes médicos ni salarios de eficiencias. Si además opera
un puesto, lleva también el grupo Usuario SGI y lo que escribe como usuario
lo escribe con ese grupo.

**Independencia:** nadie audita su propio proceso. El auditor líder no puede
ser dueño de un proceso auditado.

## 1. Qué ve al entrar

| Menú | Para qué |
|---|---|
| **SGI → Mejora → Auditorías → Programa** | El programa anual de auditorías |
| **SGI → Mejora → Auditorías → Auditorías** | Cada auditoría con su checklist, hallazgos e informe |
| **SGI → Mejora → No conformidades / Acciones correctivas** | Las NC y sus acciones, para seguir los hallazgos |
| **SGI → Administración SGI → Documentos → Documentos / Lista maestra** | Documentos controlados con revisión, estado y difusión |
| **SGI → Administración SGI → Firmas de lectura → Acuses de lectura** | Quién firmó cada documento |
| **SGI → Administración SGI → Indicadores / Diagnóstico** | Mediciones, evidencia y salud del sistema |
| **SGI → Procesos → Del Dropbox a Odoo** | La trazabilidad de cada procedimiento y formato anterior |
| **SGI → Dirección** | Tablero, revisión por la dirección, riesgos, legal, partes interesadas |

## 2. Su trabajo

### 2.1 El programa anual

1. **Programa → Nuevo** con el año. **Programa sugerido** propone los
   renglones (procesos y meses).
2. El programa lo **aprueba** el Jefe MAST y SGI. Desde ahí, 15 días antes
   del mes de cada auditoría, le llega el aviso al auditor líder.
3. Desde cada renglón, **Crear auditoría**.

### 2.2 Una auditoría

1. **Planificar**: fecha, auditor líder, equipo, procesos y normas.
2. El **checklist** sale de las actividades de los procesos auditados.
3. **Iniciar** y registre en **Reuniones** la apertura y el cierre.
4. En **Hallazgos**, cada hallazgo lleva tipo (conformidad, observación, NC
   menor o mayor, oportunidad de mejora), cláusula, proceso y evidencia, y
   una **disposición**: generar NC, mejora o sin acción (con motivo). **Generar
   NC** crea la no conformidad ligada. Sin disposición la auditoría no cierra.
5. **Elaborar informe** y **Cerrar**: el informe queda archivado en
   Documentos.

También puede registrar un hallazgo desde la ficha de un proceso con
**Registrar hallazgo**.

### 2.3 Dónde está la evidencia

- **Documentos:** Lista maestra (clave, revisión, estado) y, por documento,
  sus acuses de lectura y el porcentaje de difusión.
- **Actividades:** cada actividad dice cómo se mide y su cumplimiento;
  **Diagnóstico → Cumplimiento de procedimientos** lo muestra por proceso.
- **Indicadores:** cada medición trae **Registros** con los documentos de
  Odoo que dieron el número.
- **NC y acciones:** historial completo en el chatter de cada NC.
- **Transición:** **Del Dropbox a Odoo** muestra qué pasó con cada
  procedimiento, formato y rutina anterior (ver
  [../transicion/del-dropbox-a-odoo.md](../transicion/del-dropbox-a-odoo.md)).
- **Seguridad e higiene:** los hallazgos de la Comisión de Seguridad e
  Higiene se leen, sin editarlos.

## 3. Lo que no ve ni hace

| No… | Por qué / quién |
|---|---|
| Ve exámenes médicos ni estudios por trabajador | Dato de salud: solo Salud ocupacional |
| Ve salarios de eficiencias | Solo RH y Nóminas |
| Edita registros de los procesos auditados | Solo lectura: el auditor registra hallazgos |
| Aprueba el programa anual | Jefe MAST y SGI |

## 4. Preguntas frecuentes

- **«¿Cómo sé qué norma cubre cada actividad?»** Cada actividad dice con qué
  cláusulas «Cumple con»; el reporte de matriz de cumplimiento las junta por
  norma.
- **«Un procedimiento del Dropbox ya no existe en Odoo.»** Búsquelo por su
  clave anterior en **Del Dropbox a Odoo → Buscador por clave anterior**:
  dice qué lo sustituye.
