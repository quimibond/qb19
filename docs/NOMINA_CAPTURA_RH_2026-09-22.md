# Nómina — captura en producción desde los documentos de RH (2026-09-22)

Fuente: catálogos de trabajadores de NOI (semanal Toluca, quincenal Toluca,
quincenal CDMX, septiembre 2026), archivos de dispersión y nóminas
(semana 38/39, quincena 18) que RH manda por correo y viven en la memoria
de Supabase (`email_attachments`). Cruce con Odoo **por RFC**: las 147
altas de NOI existen en Odoo (0 sin match, 0 RFC distintos, 0 fechas de
alta distintas).

Todo se escribió por MCP en producción, **sin tocar reglas, cálculo ni
tipos de entrada** y sin crear ni validar recibos. Los números de cuenta
y CLABEs no se copian aquí: están en Odoo (Empleado → Cuentas bancarias).

## Lo que se escribió

| Dato | Registros | Detalle |
|---|---|---|
| Tipo de jornada (`hr.version.l10n_mx_shift_type`) | 151 | 145 altas del catálogo + 6 altas de septiembre pasan de `01` a `03` (Mixta), igual que NOI. Verónica Luna (556) y Juan Alberto Hernández (557) se quedan en `01` porque NOI los timbra así |
| CLABE en la cuenta ya ligada (`res.partner.bank.l10n_mx_edi_clabe`) | 127 | La cuenta de Odoo coincidía con la CLABE de NOI; sólo se llenó el campo CLABE |
| Cuentas bancarias nuevas + ligadas al empleado | 17 | 14 altas que no tenían cuenta, más Kevin Romero (560), José Gómez Fernández (563) y Francisco González Hernández (564). Las dos últimas sólo con número de cuenta (BBVA), sin CLABE |
| Cuenta bancaria **reemplazada** | 6 | Odoo tenía una cuenta distinta a la que NOI dispersa: 48 Armenta Duarte (BBVA → Banorte), 297 Osvaldo García (BBVA → Santander), 447 Carmona Medina (Santander → otra Santander), 495 Gerónimo Esquivel (Azteca → BBVA), 21 Almazán Durán (BBVA → Santander), 41 Medina Moreno (Santander → otra Santander). Se ligó la de NOI; la anterior sigue existiendo como `res.partner.bank` sin ligar. Odoo sólo permite una cuenta por empleado |
| Datos personales vacíos (dirección, CP, ciudad, nacimiento, correo, teléfono, NSS) | 9 | Sólo campos vacíos; no se sobrescribió nada |
| RFC, CURP, NSS, número y fecha de alta | 2 | José Gómez Fernández (563, QNA TOL 125, alta 7-sep) y Francisco González Hernández (564, QNA TOL 126, alta 9-sep) |
| Número de empleado | 1 | Carlos César Carmona Medina (447) → 49 (estaba libre) |

## Lo que NO se pudo resolver

1. **Números de empleado: resuelto con prefijo uniforme** (decisión del
   CEO, mismo día). NOI repite claves entre nóminas (semanal / quincenal
   Toluca / CDMX) y `registration_number` es único por compañía en Odoo, así
   que los 153 activos quedaron con `S-<n>` (semanal), `Q-<n>` (quincenal
   Toluca) y `C-<n>` (CDMX). Dejarlos vacíos no era opción: en Nómina 1.2
   `NumEmpleado` es **requerido** en `Receptor` (1 a 15 caracteres,
   cualquiera menos `|`); el módulo (19.0.1.5.0) ahora detiene el CFDI con
   error si falta la referencia. Los 21 que chocaban eran:

   | Odoo | Empleado | NOI | El número lo tiene |
   |---|---|---|---|
   | 57 | Erick Robles Martínez | SEM 32 | 255 Ricardo Salgado Saldo |
   | 88 | José Antonio Flores Vázquez | QNA TOL 4 | 74 Misael Bibiano |
   | 99 | Manuel Antonio Juárez Matías | QNA TOL 5 | 294 José Alfredo Jiménez |
   | 11 | Cynthia Santana Almora | QNA TOL 7 | 300 Eduardo Martínez Montiel |
   | 21 | José Luis Almazán Durán | QNA TOL 9 | 317 Nicolás Pliego |
   | 41 | Miguel Medina Moreno | QNA TOL 14 | 19 Lorena Mondragón |
   | 272 | Néstor Hernández Moreno | QNA TOL 19 | 109 Mario Gómez Soria |
   | 273 | Luis Oswaldo Hernández Castelar | QNA TOL 21 | 53 Ricardo Irineo |
   | 244 | Jessica Francisco Sánchez | QNA TOL 22 | 48 Juan Francisco Armenta |
   | 94 | Virginia Medina Matías | QNA TOL 32 | 255 Ricardo Salgado Saldo |
   | 410 | Alyn Reyes Morales | QNA TOL 78 | 111 Juan Carlos Delgadillo |
   | 444 | Sari Yareth Rosalio | QNA TOL 83 | 287 Javier Nava |
   | 455 | Franco Huerta Huerta | QNA TOL 85 | 85 Lorena Santiago Félix |
   | 476 | Ivi Denisse Zavala | QNA TOL 111 | 280 Paula de la Luz García |
   | 544 | Danashely Rodríguez Puerta | QNA TOL 123 | 308 Beatriz Martínez Gurrea |
   | 535 | Juan José Hernández López | QMEX 2 | 312 Alejandro Esquivel Tomás |
   | 9 | María Guadalupe Guerrero García | QMEX 12 | 318 Yovani López Lozano |
   | 538 | Nelly Esquivel Hermenegildo | QMEX 20 | 90 Ariadna Lara Escalona |
   | 8 | Irma Luna Ángeles | QMEX 26 | 285 José Antonio Hernández Saavedra |
   | 541 | Zaira Hamdan Pérez | QMEX 33 | 96 Juana Castillo Rivera |
   | 556 | Verónica Luna Vázquez | QMEX 41 | 117 Guillermina Clemente |

2. **Sin CLABE:** Jorge Alfredo Díaz Martínez (561) y Luis Enrique Pozos
   Flores (562), altas del 3 y 7 de septiembre; no aparecen en ninguna
   dispersión todavía. Gómez Fernández (563) y González Hernández (564)
   tienen número de cuenta BBVA pero no CLABE.
3. **Registro patronal de Toluca.** La compañía tiene el registro con
   guiones (`C-…-1`, así lo trae NOI). Los contratos de Toluca no tienen
   registro propio y toman el de la compañía. Confirmar con RH/PAC si el
   CFDI debe llevarlo sin guiones antes del primer timbrado de prueba.
4. **`CuentaBancaria` del CFDI.** Odoo pone en el CFDI `acc_number` (la
   cuenta de 10/11 dígitos), no la CLABE. NOI timbra la CLABE. La CLABE ya
   está capturada en `l10n_mx_edi_clabe`; si se quiere igualar a NOI hay
   que decidir si `quimibond_nomina` la toma de ahí (cambio de código).
5. **Activos en Odoo que no están en ninguna nómina de NOI:** Aramiz
   Hernández (24), Gilberto López Rangel (27), Reynaldo González Mendoza
   (277), Javier Hernández Ramírez (339), "Fatima Bustamante" (527,
   duplicado de la 254 que ya causó baja) y los tres directores.
   Kevin Romero Roldán (560) aparece como baja 12-ago en el catálogo pero
   NOI lo volvió a timbrar desde el 2-sep con el mismo número 286.
