/* global Chart */
(function () {
    const data = window.__TSSI_CHART__ || {};
    const canvas = document.getElementById("tssi-chart");
    if (!canvas || !data.labels) return;

    const ctx = canvas.getContext("2d");

    // Editorial palette. Single red accent on the composite; the five
    // asset lines use muted functional colors so the eye lands on the
    // headline index first but the per-asset drift is still legible.
    const COLOR = {
        accent: "#B00020",
        rice: "#9A7B34",
        ink: "#262A33",
        ezygo: "#10B981",
        water: "#2E8BCC",
        muted: "#66707A",
        rule: "#D9D0C4",
        ruleStrong: "#B8A88F",
        paper: "#FFFFFF",
        ticks: "#66707A",
    };

    const FONT = {
        cond:
            "'Archivo Narrow', 'Roboto Condensed', 'Inter', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', Arial, sans-serif",
        serif:
            "'Source Serif 4', 'Source Serif Pro', Charter, Georgia, serif",
        mono:
            "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace",
    };

    // Economist convention: draw a hairline dashed baseline across the chart
    // at the normalization anchor (index = 100) so the eye always knows where
    // "no change" sits. Keeps the data payload untouched.
    const baselinePlugin = {
        id: "tssiBaseline",
        beforeDatasetsDraw(chart) {
            const yScale = chart.scales.y;
            if (!yScale) return;
            const y = yScale.getPixelForValue(100);
            if (!isFinite(y)) return;
            const { ctx: c, chartArea } = chart;
            if (y < chartArea.top || y > chartArea.bottom) return;
            c.save();
            c.strokeStyle = COLOR.ruleStrong;
            c.lineWidth = 1;
            c.setLineDash([3, 3]);
            c.beginPath();
            c.moveTo(chartArea.left, y);
            c.lineTo(chartArea.right, y);
            c.stroke();
            c.restore();
        },
    };

    const common = {
        borderWidth: 1.25,
        pointRadius: 0,
        pointHitRadius: 12,
        tension: 0.2,
        spanGaps: false,
    };

    const chart = new Chart(ctx, {
        type: "line",
        data: {
            labels: data.labels,
            datasets: [
                {
                    ...common,
                    label: "TSSI composite",
                    data: data.tssi,
                    borderColor: COLOR.accent,
                    backgroundColor: "rgba(176, 0, 32, 0.03)",
                    fill: true,
                    borderWidth: 2,
                    tension: 0.22,
                    z: 10,
                },
                {
                    ...common,
                    label: "Mahboonkrong Rice 5kg",
                    data: data.rice,
                    borderColor: COLOR.rice,
                    borderDash: [1, 0],
                },
                {
                    ...common,
                    label: "Mama Tom Yum Koong",
                    data: data.mama,
                    borderColor: COLOR.ink,
                    borderDash: [1, 0],
                },
                {
                    ...common,
                    label: "EZYGO Kaphrao",
                    data: data.ezygo,
                    borderColor: COLOR.ezygo,
                    borderDash: [5, 3],
                },
                {
                    ...common,
                    label: "Crystal Water 600ml",
                    data: data.water,
                    borderColor: COLOR.water,
                    borderDash: [2, 3],
                },
                {
                    ...common,
                    label: "M-150",
                    data: data.m150,
                    borderColor: COLOR.muted,
                    borderDash: [3, 3],
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            layout: { padding: { top: 24, right: 8, bottom: 0, left: 4 } },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: COLOR.paper,
                    borderColor: COLOR.rule,
                    borderWidth: 1,
                    titleColor: "#0D0D0D",
                    titleFont: {
                        family: FONT.serif,
                        size: 14,
                        weight: "700",
                    },
                    bodyColor: COLOR.ink,
                    bodyFont: {
                        family: FONT.mono,
                        size: 12,
                    },
                    padding: 12,
                    boxPadding: 6,
                    cornerRadius: 2,
                    displayColors: true,
                    callbacks: {
                        label: (ctx) => {
                            const v = ctx.parsed.y;
                            if (v === null || v === undefined) return `${ctx.dataset.label}  —`;
                            return `${ctx.dataset.label}  ${v.toFixed(2)}`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        color: COLOR.ticks,
                        font: {
                            family: FONT.cond,
                            size: 11,
                            weight: "500",
                        },
                        maxTicksLimit: data.long_range ? 10 : 8,
                        maxRotation: 0,
                        autoSkip: true,
                        // When the window spans more than a year, collapse
                        // tick labels to "YYYY-MM" so they stay readable.
                        // Raw ISO labels are still used for tooltip titles.
                        callback: function (value) {
                            const lbl = this.getLabelForValue(value);
                            if (typeof lbl !== "string") return lbl;
                            return data.long_range ? lbl.substring(0, 7) : lbl;
                        },
                    },
                    border: { display: false },
                },
                y: {
                    grid: {
                        color: COLOR.rule,
                        lineWidth: 1,
                        drawTicks: false,
                    },
                    ticks: {
                        color: COLOR.ticks,
                        font: {
                            family: FONT.cond,
                            size: 11,
                            weight: "500",
                        },
                        padding: 10,
                        callback: (v) => v.toFixed(0),
                    },
                    border: { display: false },
                    title: { display: false },
                },
            },
        },
        plugins: [baselinePlugin],
    });

    // Keep chart crisp on window resize.
    window.addEventListener("resize", () => chart.resize());
})();
