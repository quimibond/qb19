/** @odoo-module **/
// Motor de diagramas del SGI (2026-09-25). Mismo patrón que el organigrama de
// Empleados: un componente OWL que pide los datos al servidor
// (sgi.diagram.data(kind, res_id)) y los dibuja. Tres trazados: «bands»
// (filas horizontales, mapa de procesos), «columns» (carriles verticales,
// flujo / tortuga / árboles) y «matrix» (tabla de calor). Las flechas van en
// un SVG encima de las cajas y se resaltan al pasar el mouse o dar clic.
import { Component, onMounted, onPatched, onWillStart, onWillUnmount, useRef, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

const SVG_NS = "http://www.w3.org/2000/svg";

export class SgiDiagram extends Component {
    static template = "quimibond_sgi.Diagram";
    static props = ["*"];

    setup() {
        this.orm = useService("orm");
        this.actionService = useService("action");
        this.root = useRef("root");
        this.svg = useRef("svg");
        const ctx = (this.props.action && this.props.action.context) || {};
        this.state = useState({
            kind: ctx.sgi_diagram_kind || "process_map",
            resId: ctx.sgi_diagram_res_id || null,
            data: null,
            processes: [],
            selected: ctx.sgi_diagram_selected || null,
            hover: null,
            showAll: false,
            loading: true,
        });
        onWillStart(async () => {
            this.state.processes = await this.orm.call("sgi.diagram", "processes", []);
            await this.load();
        });
        this._onResize = () => this.drawEdges();
        onMounted(() => {
            window.addEventListener("resize", this._onResize);
            this.drawEdges();
        });
        onPatched(() => this.drawEdges());
        onWillUnmount(() => window.removeEventListener("resize", this._onResize));
    }

    async load() {
        this.state.loading = true;
        const data = await this.orm.call("sgi.diagram", "data", [this.state.kind, this.state.resId]);
        this.state.data = data;
        if (data.process_id) {
            this.state.resId = data.process_id;
        }
        if (data.selected && !this.state.selected) {
            this.state.selected = data.selected;
        }
        this.state.showAll = !!data.show_all_edges;
        this.state.loading = false;
    }

    // ---- interacción ------------------------------------------------
    get focusKey() {
        return this.state.hover || this.state.selected;
    }

    get edges() {
        return (this.state.data && this.state.data.edges) || [];
    }

    get visibleEdges() {
        const focus = this.focusKey;
        if (this.state.showAll) {
            return this.edges;
        }
        if (!focus) {
            return [];
        }
        return this.edges.filter((e) => e.from === focus || e.to === focus);
    }

    isRelated(key) {
        const focus = this.focusKey;
        if (!focus || focus === key) {
            return false;
        }
        return this.edges.some((e) => (e.from === focus && e.to === key) || (e.to === focus && e.from === key));
    }

    boxClass(item) {
        const classes = ["o_sgi_dg_box", "o_sgi_dg_color_" + (item.color || "muted")];
        if (this.state.selected === item.key) {
            classes.push("o_sgi_dg_box_selected");
        } else if (this.isRelated(item.key)) {
            classes.push("o_sgi_dg_box_related");
        } else if (this.focusKey && !this.state.showAll) {
            classes.push("o_sgi_dg_box_dimmed");
        }
        return classes.join(" ");
    }

    toggleSelect(item) {
        this.state.selected = this.state.selected === item.key ? null : item.key;
    }

    setHover(item) {
        this.state.hover = item ? item.key : null;
    }

    toggleShowAll() {
        this.state.showAll = !this.state.showAll;
    }

    async switchKind(kind) {
        this.state.kind = kind;
        this.state.selected = null;
        this.state.hover = null;
        await this.load();
    }

    async switchProcess(ev) {
        this.state.resId = parseInt(ev.target.value, 10) || null;
        this.state.selected = null;
        this.state.hover = null;
        await this.load();
    }

    openRecord(item) {
        if (!item.model || !item.res_id) {
            return;
        }
        this.actionService.doAction({
            type: "ir.actions.act_window",
            res_model: item.model,
            res_id: item.res_id,
            views: [[false, "form"]],
            target: "current",
        });
    }

    openCell(row, col) {
        // Matriz «quién hace qué»: las actividades del proceso donde el puesto tiene rol.
        this.actionService.doAction({
            type: "ir.actions.act_window",
            name: `${row.label} en ${col.label}`,
            res_model: "sgi.process.activity",
            views: [[false, "list"], [false, "form"]],
            domain: [
                ["process_id", "=", col.res_id],
                "|", ["role_ids.job_id", "=", row.res_id], ["role_ids.family_id.job_ids", "in", [row.res_id]],
            ],
            target: "current",
        });
    }

    cellClass(cell) {
        if (!cell) {
            return "o_sgi_dg_cell";
        }
        return "o_sgi_dg_cell o_sgi_dg_cell_" + (cell.level >= 3 ? "exec" : cell.level === 2 ? "approve" : "part");
    }

    print() {
        window.print();
    }

    // ---- flechas ----------------------------------------------------
    _boxRect(key) {
        const root = this.root.el;
        const box = root.querySelector(`[data-key="${CSS.escape(key)}"]`);
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
        // De borde a borde: horizontal entre carriles (columnas), vertical entre bandas.
        const cax = a.left + a.width / 2, cay = a.top + a.height / 2;
        const cbx = b.left + b.width / 2, cby = b.top + b.height / 2;
        const dx = cbx - cax, dy = cby - cay;
        const horizontal = this.state.data.layout === "columns"
            ? Math.abs(dx) > a.width * 0.6
            : Math.abs(dx) > Math.abs(dy) && Math.abs(dy) < a.height;
        if (!horizontal) {
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

    drawEdges() {
        const svg = this.svg.el, root = this.root.el;
        if (!svg || !root || !this.state.data) {
            return;
        }
        while (svg.lastChild) {
            svg.removeChild(svg.lastChild);
        }
        if (this.state.data.layout === "matrix") {
            return;
        }
        svg.setAttribute("width", root.scrollWidth);
        svg.setAttribute("height", root.scrollHeight);
        const defs = document.createElementNS(SVG_NS, "defs");
        for (const [id, cls] of [["sgi_dg_arrow", "o_sgi_dg_arrow"], ["sgi_dg_arrow_active", "o_sgi_dg_arrow_active"]]) {
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
        const focus = this.focusKey;
        for (const edge of this.visibleEdges) {
            const a = this._boxRect(edge.from), b = this._boxRect(edge.to);
            if (!a || !b) {
                continue;
            }
            const p = this._anchors(a, b);
            const active = focus && (edge.from === focus || edge.to === focus);
            const path = document.createElementNS(SVG_NS, "path");
            path.setAttribute("d", `M ${p.x1} ${p.y1} C ${p.c1x} ${p.c1y}, ${p.c2x} ${p.c2y}, ${p.x2} ${p.y2}`);
            path.setAttribute("class", "o_sgi_dg_edge" + (active ? " o_sgi_dg_edge_active" : ""));
            path.setAttribute("marker-end", `url(#${active ? "sgi_dg_arrow_active" : "sgi_dg_arrow"})`);
            if (edge.label) {
                const title = document.createElementNS(SVG_NS, "title");
                title.textContent = edge.label;
                path.appendChild(title);
            }
            svg.appendChild(path);
            if (edge.label && active) {
                const text = document.createElementNS(SVG_NS, "text");
                text.setAttribute("x", (p.x1 + p.x2) / 2);
                text.setAttribute("y", (p.y1 + p.y2) / 2 - 6);
                text.setAttribute("class", "o_sgi_dg_edge_label");
                text.setAttribute("text-anchor", "middle");
                text.textContent = edge.label.length > 40 ? edge.label.slice(0, 38) + "…" : edge.label;
                svg.appendChild(text);
            }
        }
    }
}

registry.category("actions").add("sgi_diagram", SgiDiagram);
