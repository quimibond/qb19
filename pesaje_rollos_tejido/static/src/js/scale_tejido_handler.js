/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpolling = useService("iot_longpolling");
        this.ormService = useService("orm");

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            if (this.tejidoScaleListener) {
                this.tejidoScaleListener = null;
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
                this.ormService.read('iot.device', [finalId], ['iot_id', 'identifier'])
                .then((result) => {
                    if (result && result.length > 0) {
                        const dev = result[0];
                        
                        this.tejidoScaleListener = (data) => {
                            if (data.status === 'success' && data.value !== undefined) {
                                const targetField = root.resModel === 'mrp.weigh.roll.wizard' ? 'weight' : 'weight';
                                if (root.data[targetField] !== data.value) {
                                    root.update({ [targetField]: parseFloat(data.value) });
                                }
                            }
                        };

                        // 🔒 CORRECCIÓN ODOO 19: Extraemos la IP limpia del campo iot_id y pasamos parámetros sueltos
                        // Evaluamos si dev.iot_id viene como relación Many2one [id, name/ip]
                        const iotIp = Array.isArray(dev.iot_id) ? dev.iot_id[1] : dev.iot_id;
                        
                        if (iotIp && dev.identifier) {
                            this.iotLongpolling.addListener(iotIp, dev.identifier, this.tejidoScaleListener);
                        }
                    }
                });
            }
        }
    }
});