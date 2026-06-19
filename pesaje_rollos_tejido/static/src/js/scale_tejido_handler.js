/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.ormService = useService("orm");
        this.tejidoScaleInterval = null;

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            let iotDeviceId = null;
            const rawData = root.data.iot_device_id;

            if (Array.isArray(rawData)) {
                iotDeviceId = rawData[0];
            } else if (rawData && typeof rawData === 'object') {
                iotDeviceId = rawData.id || (rawData.resIds && rawData.resIds[0]);
            } else {
                iotDeviceId = rawData;
            }

            const finalId = parseInt(iotDeviceId, 10);

            if (finalId && !isNaN(finalId)) {
                // 🔒 BYPASS DE CORS: Le pedimos al servidor de Odoo SH que traiga el peso por nosotros
                this.tejidoScaleInterval = setInterval(() => {
                    this.ormService.call('iot.device', 'action_get_value', [finalId])
                    .then(result => {
                        if (result !== undefined && result !== null) {
                            // Extraemos el valor ya sea si viene directo o en un diccionario
                            let weightValue = typeof result === 'object' ? (result.weight || result.value) : result;
                            weightValue = parseFloat(weightValue);

                            if (!isNaN(weightValue)) {
                                const targetField = 'weight';
                                if (root.data[targetField] !== weightValue) {
                                    root.update({ [targetField]: weightValue });
                                }
                            }
                        }
                    })
                    .catch(err => console.log("Reintentando lectura por canal seguro..."));
                }, 1200); // 1.2 segundos para mantener el canal limpio
            }
        }
    }
});