/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl"; // <-- CORREGIDO AQUÍ

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.iotLongpolling = useService("iot_longpolling");

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => { // <-- CORREGIDO AQUÍ
            if (this.tejidoScaleListener) {
                this.tejidoScaleListener = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            this.rpc("/web/dataset/call_kw/iot.device/read", {
                model: 'iot.device',
                method: 'read',
                args: [[root.data.iot_device_id[0]], ['iot_id', 'identifier']],
                kwargs: {},
            }).then((result) => {
                if (result && result.length > 0) {
                    const dev = result[0];
                    this.tejidoScaleListener = (data) => {
                        if (data.status === 'success' && data.value !== undefined) {
                            const targetField = root.resModel === 'mrp.weigh.roll.wizard' ? 'weight_bruto' : 'weight';
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
});