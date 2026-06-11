/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpolling = useService("iot_longpolling");
        this.ormService = useService("orm"); // 🔒 CORREGIDO: Importamos el servicio ORM de Odoo 19

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
            
            // 🔒 SOLUCIÓN MAESTRA: Extraemos el ID numérico real sin importar el formato de Odoo 19
            let iotDeviceId = null;
            const rawData = root.data.iot_device_id;

            if (Array.isArray(rawData)) {
                iotDeviceId = rawData[0];
            } else if (rawData && typeof rawData === 'object') {
                iotDeviceId = rawData.id || (rawData.resIds && rawData.resIds[0]);
            } else {
                iotDeviceId = rawData;
            }

            // Forzamos a que sea un entero plano válido antes de enviarlo al ORM
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
                        this.iotLongpolling.addListener(dev.iot_id[1], dev.identifier, this.tejidoScaleListener);
                    }
                });
            }
        }
    }
});