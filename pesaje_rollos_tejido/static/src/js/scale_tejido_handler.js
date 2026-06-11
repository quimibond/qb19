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
            
            // 🔒 SOLUCIÓN: Validamos si viene como arreglo [id, name] o como entero directo
            const iotDeviceId = Array.isArray(root.data.iot_device_id) 
                ? root.data.iot_device_id[0] 
                : root.data.iot_device_id;

            if (iotDeviceId) {
                // Enviamos el ID limpio envuelto correctamente en un array plano para el orm.read
                this.ormService.read('iot.device', [iotDeviceId], ['iot_id', 'identifier'])
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