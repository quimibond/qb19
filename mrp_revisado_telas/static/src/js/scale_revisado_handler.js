/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.ormService = useService("orm");
        this.revisadoScaleInterval = null;

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleInterval) {
                clearInterval(this.revisadoScaleInterval);
                this.revisadoScaleInterval = null;
            }
        });
    },

    _connectToRevisadoScale() {
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
                this.revisadoScaleInterval = setInterval(() => {
                    this.ormService.call('iot.device', 'action_get_value', [finalId])
                    .then(result => {
                        if (result !== undefined && result !== null) {
                            let weightValue = typeof result === 'object' ? (result.weight || result.value) : result;
                            weightValue = parseFloat(weightValue);

                            if (!isNaN(weightValue)) {
                                if (root.data.peso_actual !== weightValue) {
                                    root.update({ peso_actual: weightValue });
                                }
                            }
                        }
                    })
                    .catch(err => console.log("Reintentando lectura por canal seguro..."));
                }, 1200);
            }
        }
    }
});