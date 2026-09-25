/** @odoo-module **/
// Mapa de procesos con conexiones (2026-09-25). Mismo patrón que el
// organigrama de Empleados: un componente OWL chico que pide los datos al
// servidor (sgi.process.sgi_map_data) y los dibuja. Las cajas van por banda
// (estratégicos, cadena de valor, soporte); las flechas son los flujos
// sgi.process.flow, pintadas en un SVG encima de las cajas.
import { Component, onMounted, onPatched, onWillStart, onWillUnmount, useRef, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

const SVG_NS = "http://www.w3.org/2000/svg";

export class SgiProcessMap extends Component {
    static template = "quimibond_sgi.ProcessMap";
    static props = ["*"];

    setup() {
        this.orm = useService("orm");
        this.actionService = useService("action");
        this.root = useRef("root");
        this.svg = useRef("svg");
        this.state = useState({ bands: [], flows: [], selected: null, hover: null, showAll: false, loaded: false });
        onWillStart(async () => {
            const data = await this.orm.call("sgi.process", "sgi_map_data", []);
            this.state.bands = data.bands;
            this.state.flows = data.flows;
            const ctx = (this.props.action && this.props.action.context) || {};
            if (ctx.sgi_map_process_id) {
                this.state.selected = ctx.sgi_map_process_id;
            }
            this.state.loaded = true;
        });
        this._onResize = () => this.drawFlows();
        onMounted(() => {
            window.addEventListener("resize", this._onResize);
            this.drawFlows();
        });
        onPatched(() => this.drawFlows());
        onWillUnmount(() => window.removeEventListener("resize", this._onResize));
    }

    get focusId() {
        return this.state.hover || this.state.selected;
    }

    get visibleFlows() {
        const focus = this.focusId;
        if (this.state.showAll || !focus) {
            return this.state.showAll ? this.state.flows : [];
        }
        return this.state.flows.filter((f) => f.from_id === focus || f.to_id === focus);
    }

    isRelated(processId) {
        const focus = this.focusId;
        if (!focus || focus === processId) {
            return false;
        }
        return this.state.flows.some(
            (f) => (f.from_id === focus && f.to_id === processId) || (f.to_id === focus && f.from_id === processId)
        );
    }

    boxClass(process) {
        const classes = ["o_sgi_map_box", "card", "p-2"];
        if (this.state.selected === process.id) {
            classes.push("o_sgi_map_box_selected");
        } else if (this.isRelated(process.id)) {
            classes.push("o_sgi_map_box_related");
        } else if (this.focusId) {
            classes.push("o_sgi_map_box_dimmed");
        }
        return classes.join(" ");
    }

    toggleSelect(processId) {
        this.state.selected = this.state.selected === processId ? null : processId;
    }

    setHover(processId) {
        this.state.hover = processId;
    }

    toggleShowAll() {
        this.state.showAll = !this.state.showAll;
    }

    openProcess(processId) {
        this.actionService.doAction({
            type: "ir.actions.act_window",
            res_model: "sgi.process",
            res_id: processId,
            views: [[false, "form"]],
            target: "current",
        });
    }

    _boxRect(processId) {
        const root = this.root.el;
        const box = root.querySelector(`[data-process-id="${processId}"]`);
        if (!box) {
            return null;
        }
        const r = box.getBoundingClientRect();
        const base = root.getBoundingClientRect();
        return {
            left: r.left - base.left + root.scrollLeft,
            top: r.top - base.top + root.scrollTop,
            width: r.width,
            height: r.height,
        };
    }

    _anchors(a, b) {
        // De borde a borde: vertical si los procesos están en bandas
        // distintas, horizontal si son vecinos de la misma banda.
        const cax = a.left + a.width / 2, cay = a.top + a.height / 2;
        const cbx = b.left + b.width / 2, cby = b.top + b.height / 2;
        const dx = cbx - cax, dy = cby - cay;
        if (Math.abs(dy) > Math.abs(dx) || Math.abs(dy) > a.height) {
            const down = dy > 0;
            const x1 = cax, y1 = down ? a.top + a.height : a.top;
            const x2 = cbx, y2 = down ? b.top : b.top + b.height;
            const bend = Math.max(30, Math.abs(y2 - y1) / 2);
            return { x1, y1, x2, y2, c1x: x1, c1y: y1 + (down ? bend : -bend), c2x: x2, c2y: y2 - (down ? bend : -bend) };
        }
        const right = dx > 0;
        const x1 = right ? a.left + a.width : a.left, y1 = cay;
        const x2 = right ? b.left : b.left + b.width, y2 = cby;
        const bend = Math.max(30, Math.abs(x2 - x1) / 2);
        return { x1, y1, x2, y2, c1x: x1 + (right ? bend : -bend), c1y: y1, c2x: x2 - (right ? bend : -bend), c2y: y2 };
    }

    drawFlows() {
        const svg = this.svg.el, root = this.root.el;
        if (!svg || !root) {
            return;
        }
        while (svg.lastChild) {
            svg.removeChild(svg.lastChild);
        }
        svg.setAttribute("width", root.scrollWidth);
        svg.setAttribute("height", root.scrollHeight);
        const defs = document.createElementNS(SVG_NS, "defs");
        for (const [id, cls] of [["sgi_arrow", "o_sgi_map_arrow"], ["sgi_arrow_active", "o_sgi_map_arrow_active"]]) {
            const marker = document.createElementNS(SVG_NS, "marker");
            marker.setAttribute("id", id);
            marker.setAttribute("viewBox", "0 0 10 10");
            marker.setAttribute("refX", "9");
            marker.setAttribute("refY", "5");
            marker.setAttribute("markerWidth", "8");
            marker.setAttribute("markerHeight", "8");
            marker.setAttribute("orient", "auto-start-reverse");
            const tip = document.createElementNS(SVG_NS, "path");
            tip.setAttribute("d", "M 0 0 L 10 5 L 0 10 z");
            tip.setAttribute("class", cls);
            marker.appendChild(tip);
            defs.appendChild(marker);
        }
        svg.appendChild(defs);
        const focus = this.focusId;
        for (const flow of this.visibleFlows) {
            const a = this._boxRect(flow.from_id), b = this._boxRect(flow.to_id);
            if (!a || !b) {
                continue;
            }
            const p = this._anchors(a, b);
            const path = document.createElementNS(SVG_NS, "path");
            path.setAttribute("d", `M ${p.x1} ${p.y1} C ${p.c1x} ${p.c1y}, ${p.c2x} ${p.c2y}, ${p.x2} ${p.y2}`);
            const active = focus && (flow.from_id === focus || flow.to_id === focus);
            path.setAttribute("class", "o_sgi_map_flow" + (active ? " o_sgi_map_flow_active" : ""));
            path.setAttribute("marker-end", `url(#${active ? "sgi_arrow_active" : "sgi_arrow"})`);
            const title = document.createElementNS(SVG_NS, "title");
            title.textContent = `${flow.from_code} → ${flow.to_code}: ${flow.name}`;
            path.appendChild(title);
            svg.appendChild(path);
        }
    }
}

registry.category("actions").add("sgi_process_map", SgiProcessMap);
