# Manual del Jefe MAST y SGI (día a día)

Para quien tiene el grupo **Jefe MAST y SGI**. Aquí está el trabajo de todos
los días; la configuración (procesos, documentos, parámetros, crons) está en
[../administracion/manual-jefe-mast.md](../administracion/manual-jefe-mast.md).

## 1. Qué ve al entrar

Todo el menú **SGI**, incluida **Administración SGI** (documentos,
indicadores, aprobaciones, diagnóstico, firmas de lectura y configuración).
Usted también es Usuario SGI y Auditor: tiene su propio procedimiento y lee
todo, salvo exámenes médicos (Salud ocupacional) y salarios (RH).

## 2. Su día

### 2.1 Vaciar su bandeja

**SGI → Inicio → Mis pendientes.** A usted le llegan, además de lo suyo:

- solicitudes de **cancelación de NC** (apruébelas con **Cancelar** en la NC,
  o conteste en el chatter);
- plazos de NC vencidos que ya escalaron a MAST;
- **validaciones** de mediciones de indicadores sin dueño;
- avisos de los crons: documentos por revisar, pilotos por vencer, acuses
  pendientes, calibraciones, programa de auditorías, Mi procedimiento por
  publicar (aviso semanal).

### 2.2 No conformidades

1. **SGI → Mejora → No conformidades**: agrupe por etapa (Abierta,
   Seguimiento, Cerrada, Cancelada) y filtre las vencidas.
2. Una NC no se cierra sin causa raíz, acciones terminadas (las correctivas,
   con evidencia: una nota o un archivo) y verificación de eficacia con
   resultado **Eficaz**, registrada en la fecha programada o después (y en una
   NC mayor, los 5 porqués y la lección aplicada). Si la verificación sale
   **No eficaz**, la NC regresa a Seguimiento y pide una acción correctiva
   nueva. Solo la cierran el dueño del proceso o el Jefe MAST; ya cerrada,
   solo el Jefe MAST la modifica (el dueño del proceso puede reabrirla
   cambiando solo la etapa). Si hay que cerrarla sin cumplirlo, use **Cierre
   forzado (Jefe MAST)** con motivo; queda en el historial.
3. **SGI → Mejora → Acciones correctivas** muestra todas las acciones de
   todos los orígenes, con filtro «De …».

### 2.3 Publicar Mi procedimiento

1. **Administración SGI → Firmas de lectura → Publicar Mi procedimiento**.
2. Revise la pestaña de pendientes: puestos duplicados, empleados sin puesto,
   puestos sin roles y puestos con roles pero sin personas.
3. Publique un puesto o **Publicar todos los puestos**. A cada persona le
   llega el acuse de «leído y entendido».
4. **Acuses de lectura** muestra quién falta de firmar.

### 2.4 Indicadores

1. **Administración SGI → Indicadores → Mediciones**: filtre las pendientes
   de captura y de validación.
2. Un indicador nuevo nace «En prueba»; su dueño lo pasa a oficial (**Pasar
   a oficial**) después de revisar una vez los registros de una medición
   contra la realidad.
3. Si un automático no da valor, el campo «Último cálculo» dice por qué (sin
   fórmula, sin datos, error).

### 2.5 Revisar la salud del SGI

**Administración SGI → Diagnóstico**: Diagnóstico del SGI, Cobertura de
medición (qué actividades se miden de verdad), Cumplimiento de
procedimientos, Faltantes de especificación (lo que impide publicar un
proceso) y Cumplimiento semanal. Detalle de cómo leerlos en el manual de
administración.

### 2.6 La transición

**Procesos → Del Dropbox a Odoo → Avance de la transición**: por proceso,
procedimientos sustituidos, rutinas resueltas y documentos migrados. Usted
importa las rutinas (**Importar rutinas**, primero **Probar (modo de
prueba)**) y corrige clase y estado de los formatos.

## 3. Lo que le llega solo

Además de lo anterior: la NC mayor (correo crítico), incidentes graves, los
escalamientos de acciones vencidas y el aviso semanal de puestos con Mi
procedimiento sin publicar o desactualizado.

## 4. Lo que no puede hacer y a quién pedirlo

| No puede… | Pídalo a… |
|---|---|
| Cargar el catálogo por archivo | Administrador SGI |
| Cambiar Ajustes generales | Administrador del sistema (Ajustes) |
| Crear usuarios o cambiar grupos | Los decide Dirección; los crea Sistemas |
| Ver exámenes médicos | Salud ocupacional (Coordinador de RH) |
| Ver salarios de eficiencias | RH y Nóminas |

## 5. Preguntas frecuentes

- **«Una persona dice que no le aparece su procedimiento.»** Revise que el
  empleado tenga puesto y usuario, que el puesto tenga actividades y que esté
  publicado (pestaña de pendientes de Publicar Mi procedimiento).
- **«Apagué una fuente de NC automática y quiero saber cuántas se
  omitieron.»** En Configuración → Fuentes de NC automáticas, cada fuente
  cuenta las omisiones y guarda la última.
- **«Un aviso sigue abierto aunque ya se resolvió.»** Los crons cierran solos
  los avisos cuya causa se resolvió en su siguiente corrida diaria.
