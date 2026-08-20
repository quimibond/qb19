# Quimibond - Tabla de Rendimientos y RB Tintorería

Módulo Odoo 19, independiente de "Quimibond - Ficha Técnica de Tela"
(aunque ambos alimentan la misma lógica de cálculo de tamaño de orden /
split en Tintorería que se construirá más adelante).

## Instalación

1. Copiar la carpeta `quimibond_tintoreria_rendimiento` a la carpeta de
   addons de tu instancia.
2. Actualizar la lista de aplicaciones e instalar
   "Quimibond - Tabla de Rendimientos y RB Tintorería".
3. Requiere el módulo `mrp` instalado.

## Uso

- **Manufactura → Configuración → Tabla de Rendimientos y RB Tintorería**.
- Se precarga con los 5 centros de trabajo (HTJ1-HTJ5) tomados del Excel
  de origen: capacidad máxima, capacidad por banda de rendimiento
  (Grupo A 3-6, B 7-10, C 11-15 m/kg) y Relación de Baño (RB, L/kg).
- Editable en línea desde la vista de lista.
- Cada registro se puede vincular opcionalmente a un `mrp.workcenter` real
  una vez que los centros de trabajo de Tintorería estén configurados en
  Manufactura.

## Métodos disponibles para desarrollos futuros

En el modelo `tintoreria.capacidad.rendimiento`:

- `capacidad_para_rendimiento(rendimiento)` — devuelve los kg permitidos
  en ese centro según la banda de rendimiento del artículo (0.0 si no
  aplica).
- `litros_para_kg(kg)` — calcula litros de baño necesarios
  (RB × kg de la orden).

Estos métodos están pensados para ser consumidos por el desarrollo de
"split automático de orden en Tintorería" y "cálculo de químicos/agua",
descritos en el documento de Diseño Técnico general del proceso de Tela
Acabada.
