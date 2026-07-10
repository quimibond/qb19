/** @odoo-module **/

import { registry } from "@web/core/registry";
import { FloatField, floatField } from "@web/views/fields/float/float_field";
import { useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

/**
 * Widget de captura de peso vía báscula IoT, reutilizable en cualquier
 * wizard que use 'scale.wizard.mixin' (mrp.revisado.wizard,
 * mrp.weigh.roll.wizard, mrp.subproduct.wizard, ...).
 *
 * Por qué existe como widget de campo y no como parche global de
 * FormController:
 * - Solo se ejecuta en el campo donde se declara explícitamente
 *   (widget="peso_bascula"), sin afectar el resto de formularios de Odoo.
 * - Tiene acceso directo a this.props.record (el mismo record reactivo
 *   Owl/ORM del formulario), así que actualizar el valor vía
 *   this.props.record.update({...}) es 100% seguro: es la misma API que
 *   usa el framework cuando el usuario teclea en el campo. No hay
 *   manipulación de DOM ni dispatchEvent, por lo que no hay riesgo de que
 *   Owl detecte una inconsistencia y cierre el modal.
 * - Es agnóstico al nombre del campo: usa this.props.name, así que sirve
 *   igual para 'peso_actual' (revisado) que para 'weight' (pesaje de
 *   rollo y de subproducto).
 * - Lee la URL de la báscula desde el propio record (scale_read_url,
 *   servido por el mixin desde iot.device), no de una constante en JS.
 */
export class ScaleCaptureField extends FloatField {
    static template = "iot_scale_common.ScaleCaptureField";

    setup() {
        super.setup();
        this.state = useState({ loading: false });
        this.notification = useService("notification");
    }

    get iotScaleUrl() {
        return this.props.record.data.scale_read_url || "";
    }

    get isIotMode() {
        // El campo weighing_mode debe existir en el mismo record (todos
        // los wizards que usan este widget heredan de scale.wizard.mixin).
        return this.props.record.data.weighing_mode === "iot";
    }

    async onCapturarClick() {
        if (this.state.loading) {
            return;
        }
        if (!this.iotScaleUrl) {
            this.notification.add(
                _t("Esta báscula no tiene configurada una URL de lectura. Revise Ajustes > IoT > Dispositivos."),
                { type: "warning" }
            );
            return;
        }

        this.state.loading = true;
        try {
            const response = await fetch(this.iotScaleUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "call",
                    params: {},
                    id: Math.floor(Math.random() * 100000),
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const payload = await response.json();
            const weightValue = this._extractWeight(payload);

            if (isNaN(weightValue) || weightValue < 0) {
                this.notification.add(
                    _t("La báscula no devolvió un peso válido. Verifique que el cable/conexión de la báscula esté firme y que la IoT Box tenga comunicación con el dispositivo."),
                    { type: "warning" }
                );
                return;
            }

            // Línea clave: actualiza el estado reactivo del record de
            // Owl/ORM. this.props.name es 'peso_actual', 'weight', o
            // cualquier campo float donde se use este widget.
            await this.props.record.update({
                [this.props.name]: weightValue,
            });
        } catch (err) {
            console.error("[iot_scale_common] Error al leer báscula:", err);
            this.notification.add(
                _t("No se pudo conectar con la báscula. Verifique la IoT Box y la red."),
                { type: "danger" }
            );
        } finally {
            this.state.loading = false;
        }
    }

    _extractWeight(payload) {
        let weightValue;
        if (payload && payload.result !== undefined) {
            if (
                typeof payload.result === "number" ||
                (!isNaN(parseFloat(payload.result)) && typeof payload.result !== "object")
            ) {
                weightValue = parseFloat(payload.result);
            } else if (typeof payload.result === "object" && payload.result !== null) {
                const resData = payload.result;
                weightValue =
                    resData.weight !== undefined
                        ? resData.weight
                        : resData.value !== undefined
                        ? resData.value
                        : resData.net;
            }
        }
        return parseFloat(weightValue);
    }
}

export const scaleCaptureField = {
    ...floatField,
    component: ScaleCaptureField,
};

registry.category("fields").add("peso_bascula", scaleCaptureField);
