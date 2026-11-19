/** @odoo-module ignore */
/* Sitio B2B de Quimibond: animaciones e interacción.
   JS plano, sin dependencias: solo actúa si la página trae html[data-qb]. */
(function () {
    "use strict";

    var raiz = document.documentElement;
    var sinMovimiento = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var TAU = Math.PI * 2;
    var HILOS = ["#2f54c8", "#e8590c", "#0f9d7a"];

    function listo(fn) {
        if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", fn);
        } else {
            fn();
        }
    }
    function todos(sel, ctx) {
        return Array.prototype.slice.call((ctx || document).querySelectorAll(sel));
    }
    function normal(s) {
        return (s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
    }

    /* Lienzo que se ajusta al tamaño y solo corre cuando está a la vista */
    function lienzo(canvas, dibujar) {
        var ctx = canvas.getContext("2d");
        var estado = { w: 0, h: 0, visible: false, t0: 0, raf: 0 };
        function medir() {
            var r = canvas.getBoundingClientRect();
            var dpr = Math.min(window.devicePixelRatio || 1, 2);
            estado.w = r.width;
            estado.h = r.height;
            canvas.width = Math.max(1, Math.round(r.width * dpr));
            canvas.height = Math.max(1, Math.round(r.height * dpr));
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            if (sinMovimiento || !estado.visible) {
                dibujar(ctx, estado.w, estado.h, 0, 0);
            }
        }
        function cuadro(t) {
            if (!estado.visible) {
                estado.raf = 0;
                return;
            }
            var dt = Math.min(0.05, (t - (estado.t0 || t)) / 1000);
            estado.t0 = t;
            dibujar(ctx, estado.w, estado.h, t / 1000, dt);
            estado.raf = requestAnimationFrame(cuadro);
        }
        if ("ResizeObserver" in window) {
            new ResizeObserver(medir).observe(canvas);
        } else {
            window.addEventListener("resize", medir);
        }
        medir();
        if (sinMovimiento || !("IntersectionObserver" in window)) {
            estado.visible = !sinMovimiento;
            if (!sinMovimiento) {
                estado.raf = requestAnimationFrame(cuadro);
            }
            return;
        }
        new IntersectionObserver(function (entradas) {
            estado.visible = entradas[0].isIntersecting;
            if (estado.visible && !estado.raf) {
                estado.t0 = 0;
                estado.raf = requestAnimationFrame(cuadro);
            }
        }).observe(canvas);
    }

    /* ---------------------------------------------------------------------
       Máquina de tejido circular, vista desde arriba.
       Las bobinas alimentan el hilo; el cilindro gira; las agujas suben al
       pasar por cada alimentador (leva fija) y la tela baja en espiral por
       el centro, con una pasada por alimentador.
       ------------------------------------------------------------------ */
    function maquinaCircular(canvas) {
        var N = 96;            // agujas
        var F = 9;             // alimentadores
        var PASADAS = 15;      // anillos de tela visibles
        var colores = [];
        for (var f = 0; f < F; f++) {
            colores.push(f % 3 === 0 ? HILOS[(f / 3) % 3] : "#a1a1aa");
        }
        var rot = 0;
        var vel = 0.32;
        var objetivo = 0.32;
        var mallas = 0;
        var cursor = null;
        var elRpm = document.querySelector("[data-qb-rpm]");
        var elMallas = document.querySelector("[data-qb-mallas]");
        var formato = new Intl.NumberFormat("es-MX");

        canvas.parentElement.addEventListener("pointerenter", function () { objetivo = 0.85; });
        canvas.parentElement.addEventListener("pointerleave", function () { objetivo = 0.32; cursor = null; });
        canvas.parentElement.addEventListener("pointermove", function (e) {
            var r = canvas.getBoundingClientRect();
            cursor = { x: e.clientX - r.left, y: e.clientY - r.top };
        });

        function leva(a) {
            // Altura de la aguja según su posición: sube al acercarse a cada alimentador
            var paso = TAU / F;
            var d = ((a % paso) + paso) % paso / paso;      // 0..1 entre alimentadores
            var x = Math.abs(d - 0.5) * 2;                  // 1 en el alimentador
            return Math.pow(x, 3);
        }

        lienzo(canvas, function (ctx, w, h, t, dt) {
            var c = Math.min(w, h);
            var cx = w / 2;
            var cy = h / 2;
            var R = c * 0.47;
            var Rc = R * 0.60;          // cilindro de agujas
            var Rb = R * 0.90;          // bobinas
            vel += (objetivo - vel) * Math.min(1, dt * 2.5);
            rot += vel * dt;
            mallas += (vel * dt / TAU) * N * F;
            ctx.clearRect(0, 0, w, h);

            // Base y aros guía
            var g = ctx.createRadialGradient(cx, cy, Rc * 0.2, cx, cy, R * 1.05);
            g.addColorStop(0, "rgba(244,244,245,0)");
            g.addColorStop(0.62, "rgba(244,244,245,.9)");
            g.addColorStop(1, "rgba(244,244,245,0)");
            ctx.fillStyle = g;
            ctx.beginPath();
            ctx.arc(cx, cy, R * 1.05, 0, TAU);
            ctx.fill();
            ctx.lineWidth = 1;
            ctx.strokeStyle = "#e4e4e7";
            ctx.setLineDash([3, 5]);
            ctx.beginPath();
            ctx.arc(cx, cy, Rb, 0, TAU);
            ctx.stroke();
            ctx.setLineDash([]);

            // Tela: anillos de mallas que bajan hacia el centro
            var avance = (rot * F / TAU) % 1;               // fracción de pasada
            var base = Math.floor(rot * F / TAU);
            var porColor = {};
            for (var j = 0; j < PASADAS; j++) {
                var k = (j + avance) / PASADAS;             // 0 borde → 1 centro
                var r1 = Rc * (1 - k * 0.86);
                var r2 = Rc * (1 - (j + avance + 0.78) / PASADAS * 0.86);
                var color = colores[(((base - j) % F) + F) % F];
                var alfa = Math.max(0, 1 - k * 1.05);
                var clave = color + "|" + alfa.toFixed(2);
                var lista = porColor[clave] || (porColor[clave] = []);
                lista.push(r1, r2);
            }
            ctx.lineCap = "round";
            Object.keys(porColor).forEach(function (clave) {
                var partes = clave.split("|");
                var radios = porColor[clave];
                ctx.strokeStyle = partes[0];
                ctx.globalAlpha = parseFloat(partes[1]) * (partes[0] === "#a1a1aa" ? 0.75 : 1);
                ctx.beginPath();
                for (var q = 0; q < radios.length; q += 2) {
                    var ra = radios[q];
                    var rb = radios[q + 1];
                    ctx.lineWidth = Math.max(0.6, ra / Rc * 1.7);
                    for (var i = 0; i < N; i++) {
                        var a0 = rot + i / N * TAU;
                        var am = a0 + 0.5 / N * TAU;
                        var a1 = a0 + 1 / N * TAU;
                        // Malla en V: dos patas que se juntan hacia el centro
                        ctx.moveTo(cx + Math.cos(a0 + 0.1 / N * TAU) * ra, cy + Math.sin(a0 + 0.1 / N * TAU) * ra);
                        ctx.lineTo(cx + Math.cos(am) * rb, cy + Math.sin(am) * rb);
                        ctx.lineTo(cx + Math.cos(a1 - 0.1 / N * TAU) * ra, cy + Math.sin(a1 - 0.1 / N * TAU) * ra);
                    }
                }
                ctx.stroke();
            });
            ctx.globalAlpha = 1;

            // Centro: el tubo de tela que baja
            var gc = ctx.createRadialGradient(cx, cy, 0, cx, cy, Rc * 0.3);
            gc.addColorStop(0, "#18181b");
            gc.addColorStop(0.55, "rgba(24,24,27,.55)");
            gc.addColorStop(1, "rgba(24,24,27,0)");
            ctx.fillStyle = gc;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc * 0.3, 0, TAU);
            ctx.fill();

            // Hilos: de cada bobina a su alimentador
            for (var f = 0; f < F; f++) {
                var af = f / F * TAU - Math.PI / 2;
                var ab = af - 0.34;
                var bx = cx + Math.cos(ab) * Rb;
                var by = cy + Math.sin(ab) * Rb;
                var fx = cx + Math.cos(af) * (Rc + c * 0.045);
                var fy = cy + Math.sin(af) * (Rc + c * 0.045);
                var tiembla = Math.sin(t * 9 + f * 1.7) * c * 0.004 * (vel / 0.32);
                var mx = cx + Math.cos((af + ab) / 2) * (R * 0.80) + tiembla;
                var my = cy + Math.sin((af + ab) / 2) * (R * 0.80) + tiembla;
                ctx.strokeStyle = colores[f];
                ctx.globalAlpha = colores[f] === "#a1a1aa" ? 0.7 : 0.95;
                ctx.lineWidth = 1.3;
                ctx.setLineDash([5, 3]);
                ctx.lineDashOffset = -t * 40 * (vel / 0.32);
                ctx.beginPath();
                ctx.moveTo(bx, by);
                ctx.quadraticCurveTo(mx, my, fx, fy);
                ctx.stroke();
                ctx.setLineDash([]);
                ctx.globalAlpha = 1;

                // Alimentador
                ctx.fillStyle = "#fff";
                ctx.strokeStyle = "#18181b";
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.arc(fx, fy, c * 0.011, 0, TAU);
                ctx.fill();
                ctx.stroke();

                // Bobina: cono de hilo que gira al desenrollar
                var rbob = c * 0.043;
                var cerca = cursor ? Math.max(0, 1 - Math.hypot(cursor.x - bx, cursor.y - by) / (c * 0.18)) : 0;
                rbob *= 1 + cerca * 0.18;
                ctx.fillStyle = "#fff";
                ctx.strokeStyle = "#d4d4d8";
                ctx.lineWidth = 1;
                ctx.shadowColor = "rgba(9,9,11,.12)";
                ctx.shadowBlur = 10;
                ctx.shadowOffsetY = 3;
                ctx.beginPath();
                ctx.arc(bx, by, rbob, 0, TAU);
                ctx.fill();
                ctx.shadowColor = "transparent";
                ctx.shadowBlur = 0;
                ctx.shadowOffsetY = 0;
                ctx.stroke();
                ctx.strokeStyle = colores[f];
                ctx.lineWidth = 1.4;
                var giro = -t * 2.2 * (vel / 0.32) + f;
                for (var v = 0; v < 3; v++) {
                    ctx.globalAlpha = 0.9 - v * 0.22;
                    ctx.beginPath();
                    ctx.arc(bx, by, rbob * (0.78 - v * 0.2), giro + v * 1.3, giro + v * 1.3 + 4.4);
                    ctx.stroke();
                }
                ctx.globalAlpha = 1;
                ctx.fillStyle = "#18181b";
                ctx.beginPath();
                ctx.arc(bx, by, rbob * 0.16, 0, TAU);
                ctx.fill();
            }

            // Cilindro y agujas
            ctx.strokeStyle = "#18181b";
            ctx.lineWidth = 1.6;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc, 0, TAU);
            ctx.stroke();
            ctx.strokeStyle = "#d4d4d8";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.arc(cx, cy, Rc + c * 0.062, 0, TAU);
            ctx.stroke();
            for (var n = 0; n < N; n++) {
                var an = rot + n / N * TAU;
                var sube = leva(an + Math.PI / 2);
                var largo = c * (0.012 + sube * 0.04);
                var x0 = cx + Math.cos(an) * Rc;
                var y0 = cy + Math.sin(an) * Rc;
                var x1 = cx + Math.cos(an) * (Rc + largo);
                var y1 = cy + Math.sin(an) * (Rc + largo);
                ctx.strokeStyle = sube > 0.6 ? "#18181b" : "#71717a";
                ctx.lineWidth = sube > 0.6 ? 1.5 : 1;
                ctx.beginPath();
                ctx.moveTo(x0, y0);
                ctx.lineTo(x1, y1);
                ctx.stroke();
                if (sube > 0.75) {
                    ctx.fillStyle = "#18181b";
                    ctx.beginPath();
                    ctx.arc(x1, y1, 1.5, 0, TAU);
                    ctx.fill();
                }
            }

            if (elRpm) {
                elRpm.textContent = Math.round(vel / 0.32 * 26);
            }
            if (elMallas) {
                elMallas.textContent = formato.format(Math.floor(mallas));
            }
        });
    }

    /* ---------------------------------------------------------------------
       No tejido: fibras al azar y puntos de adhesivo que se encienden en ola.
       ------------------------------------------------------------------ */
    function fibras(canvas) {
        var semilla = 7;
        function azar() {
            semilla = (semilla * 16807) % 2147483647;
            return (semilla - 1) / 2147483646;
        }
        var lista = [];
        for (var i = 0; i < 110; i++) {
            lista.push({ x: azar(), y: azar(), a: azar() * Math.PI, l: 0.12 + azar() * 0.3, c: (azar() - 0.5) * 0.5, f: azar() * TAU, o: 0.05 + azar() * 0.13 });
        }
        var cursor = null;
        var seccion = canvas.parentElement;
        seccion.addEventListener("pointermove", function (e) {
            var r = canvas.getBoundingClientRect();
            cursor = { x: e.clientX - r.left, y: e.clientY - r.top };
        });
        seccion.addEventListener("pointerleave", function () { cursor = null; });

        lienzo(canvas, function (ctx, w, h, t) {
            ctx.clearRect(0, 0, w, h);
            var m = Math.max(w, h);
            ctx.lineCap = "round";
            lista.forEach(function (fb) {
                var x = fb.x * w + Math.sin(t * 0.12 + fb.f) * 14;
                var y = fb.y * h + Math.cos(t * 0.1 + fb.f) * 10;
                var a = fb.a + Math.sin(t * 0.07 + fb.f) * 0.08;
                var dx = Math.cos(a) * fb.l * m / 2;
                var dy = Math.sin(a) * fb.l * m / 2;
                ctx.strokeStyle = "rgba(255,255,255," + fb.o + ")";
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(x - dx, y - dy);
                ctx.quadraticCurveTo(x - dy * fb.c, y + dx * fb.c, x + dx, y + dy);
                ctx.stroke();
            });
            // Puntos de adhesivo
            var paso = 26;
            for (var py = paso / 2, fila = 0; py < h; py += paso * 0.866, fila++) {
                for (var px = (fila % 2 ? paso : paso / 2); px < w; px += paso) {
                    var ola = Math.sin(px * 0.012 + py * 0.006 - t * 0.9) * 0.5 + 0.5;
                    var luz = Math.pow(ola, 4);
                    if (cursor) {
                        luz = Math.max(luz, Math.max(0, 1 - Math.hypot(cursor.x - px, cursor.y - py) / 150));
                    }
                    if (luz < 0.03) {
                        continue;
                    }
                    ctx.fillStyle = "rgba(125,150,255," + (luz * 0.75).toFixed(3) + ")";
                    ctx.beginPath();
                    ctx.arc(px, py, 1 + luz * 2.2, 0, TAU);
                    ctx.fill();
                }
            }
        });
    }

    /* ---------------------------------------------------------------------
       Tela diseñada: dibuja la construcción elegida (malla de una o doble
       fontura, no tejido, tramado) con su color, peso y punto de adhesivo, y
       la mueve como un lienzo según el tacto.
       --------------------------------------------------------------------- */
    var TELA_DEMO = [
        { construccion: "una_fontura", galga: 24, peso: 150, color: "#f5f5f2", tacto: "Suave", fusionable: false },
        { construccion: "doble_fontura", galga: 18, peso: 260, color: "#1f2a4a", tacto: "Semirrígido", fusionable: false },
        { construccion: "no_tejido", galga: 24, peso: 60, color: "#e9e2d0", tacto: "Rígido", fusionable: true },
        { construccion: "tramado", galga: 28, peso: 110, color: "#9a9aa2", tacto: "Suave", fusionable: true }
    ];
    var TELA_ONDA = { "Suave": 9, "Semirrígido": 4.5, "Rígido": 1.5 };

    function rgb(hex) {
        var m = /^#?([0-9a-f]{6})$/i.exec(hex || "");
        var n = parseInt(m ? m[1] : "f5f5f2", 16);
        return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
    }
    function mezcla(a, b, p) {
        return "rgb(" + [0, 1, 2].map(function (i) {
            return Math.round(a[i] + (b[i] - a[i]) * p);
        }).join(",") + ")";
    }
    function esClaro(c) {
        return (c[0] * 299 + c[1] * 587 + c[2] * 114) / 1000 > 140;
    }
    function azar(semilla) {
        var s = semilla || 1;
        return function () {
            s = (s * 16807) % 2147483647;
            return (s - 1) / 2147483646;
        };
    }

    function pintarTela(ctx, w, h, d) {
        var base = rgb(d.color);
        var claro = esClaro(base);
        var hilo = mezcla(base, claro ? [20, 20, 28] : [255, 255, 255], claro ? 0.42 : 0.38);
        var hilo2 = mezcla(base, claro ? [47, 84, 200] : [150, 175, 255], 0.6);
        var peso = Math.max(20, Math.min(400, d.peso || 150));
        var carga = (peso - 20) / 380;
        ctx.fillStyle = mezcla(base, [255, 255, 255], 0);
        ctx.fillRect(0, 0, w, h);
        ctx.lineCap = "round";
        ctx.lineJoin = "round";

        function mallas(paso, dx, dy, color, alfa, grosor) {
            var k = paso / 20;
            var fila = 11 * k;
            ctx.strokeStyle = color;
            ctx.globalAlpha = alfa;
            ctx.lineWidth = grosor;
            ctx.beginPath();
            for (var y = dy - fila; y < h + fila; y += fila) {
                for (var x = dx - paso; x < w + paso; x += paso) {
                    ctx.moveTo(x, y + 10 * k);
                    ctx.bezierCurveTo(x + 5 * k, y + 10 * k, x + 7 * k, y + 7 * k, x + 6 * k, y + 4 * k);
                    ctx.bezierCurveTo(x + 5 * k, y - k, x + 15 * k, y - k, x + 14 * k, y + 4 * k);
                    ctx.bezierCurveTo(x + 13 * k, y + 7 * k, x + 15 * k, y + 10 * k, x + 20 * k, y + 10 * k);
                }
            }
            ctx.stroke();
            ctx.globalAlpha = 1;
            return fila;
        }

        var paso = Math.max(9, Math.min(26, 416 / (d.galga || 24)));
        var grosor = (1 + carga * 2.4) * (paso / 17);
        var fila;
        if (d.construccion === "no_tejido") {
            var r = azar(7 + Math.round(peso));
            var n = Math.round(90 + carga * 520);
            ctx.strokeStyle = hilo;
            ctx.lineWidth = 0.9;
            ctx.globalAlpha = 0.34 + carga * 0.2;
            ctx.beginPath();
            for (var i = 0; i < n; i++) {
                var x0 = r() * w, y0 = r() * h, ang = r() * TAU, largo = 50 + r() * 150;
                var x1 = x0 + Math.cos(ang) * largo, y1 = y0 + Math.sin(ang) * largo;
                ctx.moveTo(x0, y0);
                ctx.quadraticCurveTo((x0 + x1) / 2 + (r() - 0.5) * 70, (y0 + y1) / 2 + (r() - 0.5) * 70, x1, y1);
            }
            ctx.stroke();
            ctx.globalAlpha = 1;
        } else if (d.construccion === "doble_fontura") {
            fila = mallas(paso, paso / 2, 11 * paso / 40, hilo2, 0.5, grosor);
            mallas(paso, 0, 0, hilo, 0.95, grosor);
        } else if (d.construccion === "tramado") {
            fila = mallas(paso, 0, 0, hilo, 0.5, grosor * 0.8);
            ctx.strokeStyle = hilo2;
            ctx.lineWidth = grosor * 1.5;
            ctx.globalAlpha = 0.85;
            ctx.beginPath();
            for (var yt = fila * 0.85; yt < h + fila; yt += fila * 2) {
                ctx.moveTo(-4, yt);
                ctx.lineTo(w + 4, yt);
            }
            ctx.stroke();
            ctx.globalAlpha = 1;
        } else {
            mallas(paso, 0, 0, hilo, 0.95, grosor);
        }
        if (d.fusionable) {
            ctx.fillStyle = claro ? "rgba(47, 84, 200, .55)" : "rgba(255, 255, 255, .6)";
            for (var py = 9; py < h; py += 18) {
                for (var px = ((py / 18) % 2 ? 18 : 9); px < w; px += 18) {
                    ctx.beginPath();
                    ctx.arc(px, py, 2.1, 0, TAU);
                    ctx.fill();
                }
            }
        }
    }

    function tela(canvas) {
        var demo = canvas.hasAttribute("data-qb-demo");
        var diseno = Object.assign({}, TELA_DEMO[0]);
        var fuera = document.createElement("canvas");
        var margen = 14;
        var sucio = true;
        var ultimo = null;
        var turno = 0;

        function preparar(w, h) {
            var dpr = Math.min(window.devicePixelRatio || 1, 2);
            fuera.width = Math.max(1, Math.round(w * dpr));
            fuera.height = Math.max(1, Math.round((h + margen * 2) * dpr));
            var c = fuera.getContext("2d");
            c.setTransform(dpr, 0, 0, dpr, 0, 0);
            pintarTela(c, w, h + margen * 2, diseno);
            sucio = false;
        }

        function dibujar(ctx, w, h, t) {
            if (!w || !h) {
                return;
            }
            if (demo && !sinMovimiento) {
                var ahora = Math.floor(t / 3.4) % TELA_DEMO.length;
                if (ahora !== turno) {
                    turno = ahora;
                    diseno = Object.assign({}, TELA_DEMO[turno]);
                    sucio = true;
                }
            }
            if (sucio || !ultimo || ultimo.w !== w || ultimo.h !== h) {
                preparar(w, h);
            }
            ultimo = { ctx: ctx, w: w, h: h };
            var amp = Math.min(margen - 2, TELA_ONDA[diseno.tacto] === undefined ? 6 : TELA_ONDA[diseno.tacto]);
            var tira = 3;
            var esc = fuera.width / w;
            ctx.clearRect(0, 0, w, h);
            for (var x = 0; x < w; x += tira) {
                var f = x * 0.012 + t * 0.9;
                var g = x * 0.027 - t * 0.6;
                var dy = amp * (Math.sin(f) * 0.7 + Math.sin(g) * 0.3);
                var pend = Math.cos(f) * 0.7 + Math.cos(g) * 0.3;
                ctx.drawImage(fuera, x * esc, 0, tira * esc, fuera.height, x, dy - margen, tira, h + margen * 2);
                var sombra = pend * amp * 0.011;
                if (sombra > 0.004) {
                    ctx.fillStyle = "rgba(9, 9, 11, " + Math.min(0.2, sombra).toFixed(3) + ")";
                    ctx.fillRect(x, 0, tira, h);
                } else if (sombra < -0.004) {
                    ctx.fillStyle = "rgba(255, 255, 255, " + Math.min(0.2, -sombra).toFixed(3) + ")";
                    ctx.fillRect(x, 0, tira, h);
                }
            }
        }

        canvas.qbTela = function (nuevo) {
            diseno = Object.assign({}, diseno, nuevo);
            sucio = true;
            if (sinMovimiento && ultimo) {
                dibujar(ultimo.ctx, ultimo.w, ultimo.h, 0);
            }
        };
        lienzo(canvas, dibujar);
    }

    /* --------------------------------------------------------------------- */
    function disenador() {
        var raizDis = document.querySelector("[data-qb-disenador]");
        if (!raizDis) {
            return;
        }
        var canvas = raizDis.querySelector('canvas[data-qb-anim="tela"]');
        var ficha = raizDis.querySelector("[data-qb-ficha]");
        var detalle = document.getElementById("qb_f_detalle");
        var asunto = document.getElementById("qb_f_asunto");
        var solicitud = document.getElementById("qb_f_solicitud");
        var bloqueAnterior = "";
        var asuntoAnterior = "";
        var colorLibre = false;

        function elegido(nombre) {
            return raizDis.querySelector('input[name="' + nombre + '"]:checked');
        }
        function campo(nombre) {
            return raizDis.querySelector('input[name="' + nombre + '"]');
        }

        function leer() {
            var cons = elegido("qb_construccion");
            var col = elegido("qb_color");
            var libre = campo("qb_color_libre");
            var ancho = parseInt(campo("qb_ancho").value, 10);
            var acabados = [];
            if (campo("qb_fusionable").checked) {
                acabados.push("Fusionable");
            }
            if (campo("qb_termofijado").checked) {
                acabados.push("Termofijada");
            }
            return {
                construccion: cons ? cons.value : "una_fontura",
                construccionNombre: cons ? cons.getAttribute("data-nombre") : "",
                galga: parseInt((elegido("qb_galga") || {}).value, 10) || 24,
                fibra: (elegido("qb_fibra") || {}).value || "",
                peso: parseInt(campo("qb_peso").value, 10) || 150,
                color: colorLibre || !col ? libre.value : col.value,
                colorNombre: colorLibre || !col ? "Personalizado " + libre.value.toUpperCase() : col.getAttribute("data-nombre"),
                tacto: (elegido("qb_tacto") || {}).value || "Suave",
                fusionable: campo("qb_fusionable").checked,
                acabados: acabados,
                presentacion: (elegido("qb_presentacion") || {}).value || "",
                ancho: ancho > 0 ? ancho : 0
            };
        }

        function filas(d) {
            var punto = d.construccion === "una_fontura" || d.construccion === "doble_fontura";
            var f = [["Construcción", d.construccionNombre]];
            if (punto) {
                f.push(["Galga", String(d.galga)]);
            }
            f.push(["Fibra", d.fibra], ["Peso", d.peso + " g/m²"], ["Color", d.colorNombre], ["Tacto", d.tacto]);
            if (d.acabados.length) {
                f.push(["Acabado", d.acabados.join(", ")]);
            }
            f.push(["Presentación", d.presentacion + (d.ancho ? ", " + d.ancho + " cm de ancho" : "")]);
            return f;
        }

        function aplicar() {
            var d = leer();
            var punto = d.construccion === "una_fontura" || d.construccion === "doble_fontura";
            todos('[data-qb-solo="punto"]', raizDis).forEach(function (g) {
                g.hidden = !punto;
            });
            var salidaPeso = raizDis.querySelector('[data-qb-salida="peso"]');
            var salidaColor = raizDis.querySelector('[data-qb-salida="color"]');
            salidaPeso.textContent = d.peso + " g/m²";
            salidaColor.textContent = d.colorNombre;
            raizDis.querySelector(".qb-dis__color--libre").classList.toggle("activo", colorLibre);
            var lista = filas(d);
            ficha.textContent = "";
            lista.forEach(function (par) {
                var div = document.createElement("div");
                var dt = document.createElement("dt");
                var dd = document.createElement("dd");
                dt.textContent = par[0];
                dd.textContent = par[1];
                div.appendChild(dt);
                div.appendChild(dd);
                ficha.appendChild(div);
            });
            if (canvas && canvas.qbTela) {
                canvas.qbTela(d);
            }
            // El diseño viaja a Ventas dentro de la especificación del formulario.
            var bloque = "Diseño de tela (diseñador en línea)\n" + lista.map(function (par) {
                return "- " + par[0] + ": " + par[1];
            }).join("\n");
            if (detalle) {
                var resto = detalle.value;
                if (bloqueAnterior && resto.indexOf(bloqueAnterior) === 0) {
                    resto = resto.slice(bloqueAnterior.length).replace(/^\n+/, "");
                }
                detalle.value = bloque + "\n\n" + resto;
                bloqueAnterior = bloque;
            }
            if (asunto) {
                var nuevoAsunto = "Diseño de tela: " + d.construccionNombre + ", " + d.peso + " g/m²";
                if (!asunto.value || asunto.value === asuntoAnterior) {
                    asunto.value = nuevoAsunto;
                }
                asuntoAnterior = nuevoAsunto;
            }
        }

        raizDis.addEventListener("input", function (e) {
            if (e.target.name === "qb_color_libre") {
                colorLibre = true;
                todos('input[name="qb_color"]', raizDis).forEach(function (c) {
                    c.checked = false;
                });
            } else if (e.target.name === "qb_color") {
                colorLibre = false;
            }
            aplicar();
        });
        var botonAzar = raizDis.querySelector("[data-qb-azar]");
        if (botonAzar) {
            botonAzar.addEventListener("click", function () {
                ["qb_construccion", "qb_galga", "qb_fibra", "qb_color", "qb_tacto"].forEach(function (nombre) {
                    var ops = todos('input[name="' + nombre + '"]', raizDis);
                    ops[Math.floor(Math.random() * ops.length)].checked = true;
                });
                colorLibre = false;
                campo("qb_peso").value = 20 + Math.round(Math.random() * 76) * 5;
                campo("qb_fusionable").checked = Math.random() < 0.4;
                aplicar();
            });
        }
        // El formulario de Odoo rellena sus campos al cargar: la solicitud se fija después,
        // y solo mientras la persona no la cambie.
        var solicitudTocada = false;
        if (solicitud) {
            solicitud.addEventListener("change", function () {
                solicitudTocada = true;
            });
            var fijar = function () {
                if (!solicitudTocada) {
                    solicitud.value = "Desarrollo de producto";
                }
            };
            fijar();
            window.addEventListener("load", function () {
                setTimeout(fijar, 400);
            });
            raizDis.addEventListener("input", fijar);
        }
        aplicar();
    }

    /* --------------------------------------------------------------------- */
    function aparicion() {
        var els = todos(".qb-reveal");
        if (!els.length) {
            return;
        }
        if (sinMovimiento || !("IntersectionObserver" in window)) {
            els.forEach(function (el) { el.classList.add("qb-in"); });
            return;
        }
        raiz.classList.add("qb-js");
        var io = new IntersectionObserver(function (entradas) {
            entradas.forEach(function (en) {
                if (!en.isIntersecting) {
                    return;
                }
                var el = en.target;
                var hermanos = todos(":scope > .qb-reveal", el.parentElement);
                var i = Math.max(0, hermanos.indexOf(el));
                el.style.setProperty("--qb-d", Math.min(i, 8) * 70 + "ms");
                el.classList.add("qb-in");
                io.unobserve(el);
            });
        }, { rootMargin: "0px 0px -8% 0px", threshold: 0.05 });
        els.forEach(function (el) { io.observe(el); });
    }

    function cuentas() {
        var els = todos("[data-qb-cuenta]");
        if (!els.length || sinMovimiento || !("IntersectionObserver" in window)) {
            return;
        }
        var formato = new Intl.NumberFormat("es-MX");
        var io = new IntersectionObserver(function (entradas) {
            entradas.forEach(function (en) {
                if (!en.isIntersecting) {
                    return;
                }
                var el = en.target;
                io.unobserve(el);
                var fin = parseFloat(el.dataset.valor) || 0;
                var sinMiles = !!el.dataset.sinMiles;
                var desde = sinMiles ? fin - 60 : 0;
                var inicio = performance.now();
                (function paso(ahora) {
                    var p = Math.min(1, (ahora - inicio) / 1400);
                    var v = Math.round(desde + (fin - desde) * (1 - Math.pow(1 - p, 4)));
                    el.textContent = sinMiles ? String(v) : formato.format(v);
                    if (p < 1) {
                        requestAnimationFrame(paso);
                    }
                })(inicio);
            });
        }, { threshold: 0.6 });
        els.forEach(function (el) { io.observe(el); });
    }

    function desplazamiento() {
        var header = document.querySelector(".qb-header");
        var procesos = todos("[data-qb-proceso]");
        var contenedor = document.getElementById("wrapwrap");
        var pendiente = false;
        function leer() {
            pendiente = false;
            var y = window.scrollY || (contenedor && contenedor.scrollTop) || 0;
            var alto = Math.max(document.documentElement.scrollHeight, contenedor ? contenedor.scrollHeight : 0) - window.innerHeight;
            raiz.style.setProperty("--qb-avance", alto > 0 ? Math.min(1, y / alto).toFixed(4) : "0");
            if (header) {
                header.classList.toggle("qb-header--scroll", y > 8);
            }
            procesos.forEach(function (ol) {
                var r = ol.getBoundingClientRect();
                var p = Math.max(0, Math.min(1, (window.innerHeight * 0.72 - r.top) / Math.max(1, r.height + window.innerHeight * 0.15)));
                if (window.innerWidth > 960) {
                    p = Math.max(0, Math.min(1, (window.innerHeight * 0.85 - r.top) / (window.innerHeight * 0.55)));
                }
                ol.style.setProperty("--qb-proceso", p.toFixed(3));
                var pasos = todos(".qb-proceso__paso", ol);
                pasos.forEach(function (li, i) {
                    li.classList.toggle("qb-hecho", p >= (i + 0.15) / pasos.length);
                });
            });
        }
        function alMover() {
            if (!pendiente) {
                pendiente = true;
                requestAnimationFrame(leer);
            }
        }
        window.addEventListener("scroll", alMover, { passive: true });
        if (contenedor) {
            contenedor.addEventListener("scroll", alMover, { passive: true });
        }
        window.addEventListener("resize", alMover);
        leer();
    }

    function foco() {
        document.addEventListener("pointermove", function (e) {
            var tarjeta = e.target.closest && e.target.closest(".qb-spot");
            if (!tarjeta) {
                return;
            }
            var r = tarjeta.getBoundingClientRect();
            tarjeta.style.setProperty("--mx", (e.clientX - r.left) + "px");
            tarjeta.style.setProperty("--my", (e.clientY - r.top) + "px");
        }, { passive: true });
    }

    function menuMovil() {
        var boton = document.querySelector("[data-qb-menu]");
        var panel = document.querySelector("[data-qb-movil]");
        if (!boton || !panel) {
            return;
        }
        boton.addEventListener("click", function () {
            var abierto = panel.classList.toggle("abierto");
            boton.setAttribute("aria-expanded", abierto ? "true" : "false");
        });
    }

    /* Buscador ⌘K: productos del catálogo, mercados y páginas */
    function buscador() {
        var caja = document.querySelector("[data-qb-cmd]");
        if (!caja) {
            return;
        }
        var entrada = caja.querySelector("input");
        var lista = caja.querySelector(".qb-cmd__lista");
        var datos = null;
        var visibles = [];
        var activo = 0;

        function pintar() {
            var q = normal(entrada.value).trim();
            var terminos = q.split(/\s+/).filter(Boolean);
            visibles = (datos || []).filter(function (d) {
                if (!terminos.length) {
                    return !d.codigo;
                }
                return terminos.every(function (tm) { return d._txt.indexOf(tm) !== -1; });
            }).slice(0, 30);
            activo = 0;
            lista.textContent = "";
            if (!datos) {
                lista.innerHTML = '<div class="qb-cmd__nada">Cargando catálogo…</div>';
                return;
            }
            if (!visibles.length) {
                var nada = document.createElement("div");
                nada.className = "qb-cmd__nada";
                nada.textContent = "Sin resultados. Fabricamos sobre especificación: pida una cotización.";
                lista.appendChild(nada);
                return;
            }
            var grupo = null;
            visibles.forEach(function (d, i) {
                var nombre = d.codigo ? "Productos" : d.categoria;
                if (nombre !== grupo) {
                    grupo = nombre;
                    var g = document.createElement("div");
                    g.className = "qb-cmd__grupo";
                    g.textContent = nombre;
                    lista.appendChild(g);
                }
                var a = document.createElement("a");
                a.className = "qb-cmd__op" + (i === 0 ? " activo" : "");
                a.href = d.url;
                a.setAttribute("role", "option");
                if (d.codigo) {
                    var c = document.createElement("code");
                    c.textContent = d.codigo;
                    a.appendChild(c);
                }
                var s = document.createElement("span");
                s.textContent = d.titulo;
                a.appendChild(s);
                var sm = document.createElement("small");
                sm.textContent = d.detalle || "";
                a.appendChild(sm);
                a.addEventListener("mousemove", function () { marcar(i); });
                lista.appendChild(a);
            });
        }
        function marcar(i) {
            var ops = todos(".qb-cmd__op", lista);
            if (!ops.length) {
                return;
            }
            activo = (i + ops.length) % ops.length;
            ops.forEach(function (o, k) { o.classList.toggle("activo", k === activo); });
            ops[activo].scrollIntoView({ block: "nearest" });
        }
        function abrir() {
            caja.hidden = false;
            entrada.value = "";
            pintar();
            entrada.focus();
            if (!datos) {
                fetch("/catalogo/buscar.json", { credentials: "same-origin" })
                    .then(function (r) { return r.ok ? r.json() : []; })
                    .catch(function () { return []; })
                    .then(function (json) {
                        datos = json.map(function (d) {
                            d._txt = normal([d.codigo, d.titulo, d.detalle, d.categoria].join(" "));
                            return d;
                        });
                        pintar();
                    });
            }
        }
        function cerrar() {
            caja.hidden = true;
        }
        todos("[data-qb-buscar]").forEach(function (b) { b.addEventListener("click", abrir); });
        caja.addEventListener("mousedown", function (e) {
            if (e.target === caja) {
                cerrar();
            }
        });
        entrada.addEventListener("input", pintar);
        document.addEventListener("keydown", function (e) {
            if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
                e.preventDefault();
                if (caja.hidden) {
                    abrir();
                } else {
                    cerrar();
                }
                return;
            }
            if (caja.hidden) {
                return;
            }
            if (e.key === "Escape") {
                cerrar();
            } else if (e.key === "ArrowDown") {
                e.preventDefault();
                marcar(activo + 1);
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                marcar(activo - 1);
            } else if (e.key === "Enter") {
                var op = todos(".qb-cmd__op", lista)[activo];
                if (op) {
                    window.location.href = op.href;
                }
            }
        });
        if (!/Mac|iPhone|iPad/.test(navigator.platform || "")) {
            todos(".qb-kbd-btn kbd").forEach(function (k) { k.textContent = "Ctrl K"; });
        }
    }

    /* Filtro del catálogo: texto y fichas, sin recargar */
    function filtroCatalogo() {
        var rejilla = document.querySelector("[data-qb-rejilla]");
        if (!rejilla) {
            return;
        }
        var entrada = document.querySelector("[data-qb-filtro]");
        var vacio = document.querySelector("[data-qb-vacio]");
        var cuenta = document.querySelector("[data-qb-cuenta-filtro]");
        var tarjetas = todos(".qb-prod", rejilla);
        var fichas = todos("[data-qb-chips] .qb-chip");
        function aplicar() {
            var terminos = normal(entrada ? entrada.value : "").split(/\s+/).filter(Boolean);
            var activas = {};
            fichas.forEach(function (f) {
                if (f.classList.contains("activo")) {
                    (activas[f.dataset.campo] = activas[f.dataset.campo] || []).push(f.dataset.valor);
                }
            });
            var n = 0;
            tarjetas.forEach(function (tj) {
                var texto = normal(tj.dataset.qbTexto);
                var ok = terminos.every(function (tm) { return texto.indexOf(tm) !== -1; });
                Object.keys(activas).forEach(function (campo) {
                    var valor = tj.dataset["qb" + campo.charAt(0).toUpperCase() + campo.slice(1)];
                    if (activas[campo].indexOf(valor) === -1) {
                        ok = false;
                    }
                });
                tj.hidden = !ok;
                if (ok) {
                    n++;
                    tj.classList.add("qb-in");
                }
            });
            if (cuenta) {
                cuenta.textContent = n;
            }
            if (vacio) {
                vacio.hidden = n > 0;
            }
        }
        if (entrada) {
            entrada.addEventListener("input", aplicar);
            if (entrada.value) {
                aplicar();
            }
        }
        fichas.forEach(function (f) {
            f.addEventListener("click", function () {
                f.classList.toggle("activo");
                aplicar();
            });
        });
    }

    listo(function () {
        if (!raiz.hasAttribute("data-qb")) {
            return;
        }
        todos('canvas[data-qb-anim="circular"]').forEach(maquinaCircular);
        todos('canvas[data-qb-anim="fibras"]').forEach(fibras);
        todos('canvas[data-qb-anim="tela"]').forEach(tela);
        disenador();
        aparicion();
        cuentas();
        desplazamiento();
        foco();
        menuMovil();
        buscador();
        filtroCatalogo();
    });
})();
