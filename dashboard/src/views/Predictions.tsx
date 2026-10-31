import type { Mode, Session } from "../api";
import { Card, ErrorLine, Pill, Table, ago, fmt, pct, usePoll } from "../ui";

type Row = Record<string, unknown>;
export default function Predictions({ s, mode }: { s: Session; mode: Mode }) {
  const { data, error } = usePoll<{ history: Row[]; calibration: Row[]; brier_settlement: number | null; n_settled: number; latency: Row[]; models: Row[] }>(s, `/api/predictions?mode=${mode}&hours=24`, 10000);
  return (
    <>
      <ErrorLine error={error} />
      <div className="grid">
        <Card title="Settlement calibration (last 24 h)">
          <p>Brier score {fmt(data?.brier_settlement, 4)} on {data?.n_settled ?? 0} predictions with an official outcome.</p>
          <Table rows={data?.calibration ?? []} empty="No settled predictions yet." cols={[["P(Up) bin", (r) => String(r.bin)], ["n", (r) => fmt(r.n, 0)], ["Mean predicted", (r) => pct(r.mean_p)], ["Observed Up", (r) => pct(r.observed_up)]]} />
        </Card>
        <Card title="Prediction latency">
          <Table rows={data?.latency ?? []} cols={[["Horizon", (r) => String(r.horizon)], ["n", (r) => fmt(r.n, 0)], ["p50", (r) => `${fmt(r.p50_us, 0)} µs`], ["p99", (r) => `${fmt(r.p99_us, 0)} µs`]]} />
        </Card>
      </div>
      <Card title="Models" wide>
        <Table rows={data?.models ?? []} empty="No models registered. Train with python -m polygemy.research." cols={[["Version", (r) => <span className="small">{String(r.version)}</span>], ["Horizon", (r) => String(r.horizon)], ["Kind", (r) => String(r.kind)], ["Status", (r) => <Pill text={String(r.status)} />], ["Approved", (r) => ago(r.approved_at as string)]]} />
      </Card>
      <Card title="Recent predictions" wide>
        <Table rows={(data?.history ?? []).slice(0, 100)} cols={[["Time", (r) => ago(String(r.ts))], ["Horizon", (r) => String(r.horizon)], ["P(Up)", (r) => pct(r.p_up)], ["±", (r) => pct(r.uncertainty)], ["Quality", (r) => <Pill text={String(r.quality)} />], ["Outcome", (r) => String(r.official_outcome ?? "pending")]]} />
      </Card>
    </>
  );
}
