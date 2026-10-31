import { useState } from "react";
import type { Mode, Session } from "../api";
import { Card, ErrorLine, PageHead, Pill, Stat, Table, ago, fmt, pct, usePoll } from "../ui";

type Row = Record<string, unknown>;

/** Reliability diagram: mean predicted vs observed frequency per bin; dot area ∝ sample size. */
function Calibration({ bins }: { bins: Row[] }) {
  const [hover, setHover] = useState<Row | null>(null);
  const pts = bins.filter((b) => b.mean_p != null && b.observed_up != null);
  if (!pts.length) return <p className="empty">No settled predictions yet.</p>;
  const S = 320, P = 34;
  const x = (v: number) => P + v * (S - P - 8);
  const y = (v: number) => S - P - v * (S - P - 8);
  const maxN = Math.max(...pts.map((b) => Number(b.n)));
  return (
    <div className="chart-wrap" style={{ maxWidth: S }}>
      <svg viewBox={`0 0 ${S} ${S}`} className="chart" role="img" aria-label="Calibration: predicted vs observed probability of Up">
        {[0, 0.25, 0.5, 0.75, 1].map((v) => (
          <g key={v}>
            <line x1={x(0)} x2={x(1)} y1={y(v)} y2={y(v)} className="gridline" />
            <text x={P - 6} y={y(v) + 4} textAnchor="end">{v * 100}</text>
            <text x={x(v)} y={S - P + 16} textAnchor="middle">{v * 100}</text>
          </g>
        ))}
        <line x1={x(0)} y1={y(0)} x2={x(1)} y2={y(1)} className="diag" />
        <polyline points={pts.map((b) => `${x(Number(b.mean_p))},${y(Number(b.observed_up))}`).join(" ")} className="series" />
        {pts.map((b) => (
          <circle key={String(b.bin)} cx={x(Number(b.mean_p))} cy={y(Number(b.observed_up))} r={4 + 5 * Math.sqrt(Number(b.n) / maxN)} className="end"
            onPointerEnter={() => setHover(b)} onPointerLeave={() => setHover(null)} />
        ))}
      </svg>
      {hover && (
        <div className="tip" style={{ left: `${(x(Number(hover.mean_p)) / S) * 100}%`, top: `${(y(Number(hover.observed_up)) / S) * 100}%` }}>
          <strong>{pct(hover.observed_up)} Up</strong>
          <span>predicted {pct(hover.mean_p)} · n {fmt(hover.n, 0)}</span>
        </div>
      )}
      <p className="muted small">Predicted P(Up) % across, observed Up % up. On the diagonal = calibrated.</p>
    </div>
  );
}

export default function Predictions({ s, mode }: { s: Session; mode: Mode }) {
  const { data, error } = usePoll<{ history: Row[]; calibration: Row[]; brier_settlement: number | null; n_settled: number; latency: Row[]; models: Row[] }>(s, `/api/predictions?mode=${mode}&hours=24`, 10000);
  return (
    <>
      <PageHead title="Predictions" sub="Last 24 hours" />
      <ErrorLine error={error} />
      <div className="grid">
        <Card title="Settlement calibration" meta={`${fmt(data?.n_settled ?? 0, 0)} settled predictions`} span={7}>
          <div className="stats">
            <Stat label="Brier score" value={fmt(data?.brier_settlement, 4)} sub="lower is better · 0.25 = coin flip" />
            {(data?.latency ?? []).slice(0, 2).map((l) => (
              <Stat key={String(l.horizon)} label={`Inference · ${String(l.horizon)}`} value={`${fmt(l.p50_us, 0)} µs`} sub={`p99 ${fmt(l.p99_us, 0)} µs · n ${fmt(l.n, 0)}`} />
            ))}
          </div>
          <Calibration bins={data?.calibration ?? []} />
        </Card>
        <Card title="Calibration bins" span={5} flush>
          <Table rows={data?.calibration ?? []} empty="No settled predictions yet." cols={[["P(Up) bin", (r) => String(r.bin)], ["n", (r) => fmt(r.n, 0)], ["Predicted", (r) => pct(r.mean_p)], ["Observed", (r) => pct(r.observed_up)]]} />
        </Card>
        <Card title="Models" flush>
          <Table rows={data?.models ?? []} empty="No models registered. Train with python -m polygemy.research." cols={[["Version", (r) => <span className="dim">{String(r.version)}</span>], ["Horizon", (r) => String(r.horizon)], ["Kind", (r) => String(r.kind)], ["Status", (r) => <Pill text={String(r.status)} />], ["Approved", (r) => ago(r.approved_at as string)]]} />
        </Card>
        <Card title="Recent predictions" meta="latest 100" flush>
          <Table maxHeight={480} rows={(data?.history ?? []).slice(0, 100)} cols={[["Time", (r) => ago(String(r.ts))], ["Horizon", (r) => String(r.horizon)], ["P(Up)", (r) => pct(r.p_up)], ["±", (r) => pct(r.uncertainty)], ["Quality", (r) => <Pill text={String(r.quality)} />], ["Outcome", (r) => (r.official_outcome ? <Pill text={String(r.official_outcome)} /> : <span className="muted">pending</span>)]]} />
        </Card>
      </div>
    </>
  );
}
