/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
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
            
            // 🔒 FETCH PURO: No usa useService ni componentes inestables de Odoo
            this.tejidoScaleInterval = setInterval(() => {
                fetch("/quimibond/scale/read_weight", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ jsonrpc: "2.0", params: {} })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    if (data && data.status === 'success' && data.weight !== undefined) {
                        const weightValue = parseFloat(data.weight);
                        const targetField = 'weight';
                        if (root.data[targetField] !== weightValue && weightValue >= 0) {
                            root.update({ [targetField]: weightValue });
                        }
                    }
                })
                .catch(err => console.log("Sincronizando peso..."));
            }, 1000);
        }
    }
});