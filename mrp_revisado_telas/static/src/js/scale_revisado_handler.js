/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
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
            
            // 🔒 FETCH PURO: No usa useService ni componentes inestables de Odoo
            this.revisadoScaleInterval = setInterval(() => {
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
                        if (root.data.peso_actual !== weightValue && weightValue >= 0) {
                            root.update({ peso_actual: weightValue });
                        }
                    }
                })
                .catch(err => console.log("Sincronizando peso..."));
            }, 1000);
        }
    }
});