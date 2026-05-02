/** @odoo-module **/
import { patch } from "@web/core/utils/patch";
import { FormRenderer } from "@web/views/form/form_renderer";
import { onMounted } from "@odoo/owl";

patch(FormRenderer.prototype, {
    setup() {
        super.setup();
        onMounted(() => {
            const scaleBtn = document.querySelector('.btn_read_scale');
            if (scaleBtn) {
                scaleBtn.addEventListener('click', async (ev) => {
                    ev.preventDefault();
                    await this._readScaleFromIoT();
                });
            }
        });
    },

    async _readScaleFromIoT() {
        try {
            const response = await fetch("http://localhost:8069/hw_proxy/scale_read", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ jsonrpc: "2.0", params: {} })
            });
            const data = await response.json();
            if (data.result && data.result.weight) {
                const input = document.querySelector('.weight_input input');
                if (input) {
                    input.value = data.result.weight;
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }
        } catch (e) {
            console.warn("IoT Box no disponible. Use pesaje manual.");
        }
    }
});