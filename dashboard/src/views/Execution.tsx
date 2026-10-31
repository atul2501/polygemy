import type { Mode, Session } from "../api";
import { Card, ErrorLine, Pill, Stat, Table, ago, fmt, usePoll } from "../ui";

type Row = Record<string, unknown>;
export default function Execution({ s, mode }: { s: Session; mode: Mode }) {
  const { data, error } = usePoll<{ orders: Row[]; fills: Row[]; positions: Row[]; by_status: Row[]; summary: Row }>(s, `/api/execution?mode=${mode}`, 5000);
  const lat = (r: Row) => {
    const t = r.timings as Record<string, number> | null;
    return t?.signal && t?.final ? `${fmt((t.final - t.signal) / 1000, 0)} ms` : "—";
  };
  return (
    <>
      <ErrorLine error={error} />
      <Card title={`Orders (${mode})`} wide>
        <div className="stats">
          {(data?.by_status ?? []).map((b) => <Stat key={String(b.status)} label={String(b.status)} value={fmt(b.n, 0)} />)}
          <Stat label="Avg slippage vs decision" value={fmt(data?.summary.avg_slippage, 4)} />
          <Stat label="Fees" value={fmt(data?.summary.fees, 4)} />
        </div>
        <Table rows={data?.orders ?? []} empty="No orders in this mode." cols={[["Time", (r) => ago(String(r.created_at))], ["Side", (r) => `${String(r.side)} ${String(r.outcome)}`], ["Shares", (r) => fmt(r.shares)], ["Limit", (r) => fmt(r.limit_price, 3)], ["Status", (r) => <Pill text={String(r.status)} />], ["Filled", (r) => fmt(r.filled_shares)], ["Avg price", (r) => fmt(r.avg_price, 4)], ["Fee", (r) => fmt(r.fee, 4)], ["Signal→final", lat], ["Note", (r) => <span className="small">{String(r.error ?? "")}</span>]]} />
      </Card>
      <div className="grid">
        <Card title="Open positions">
          <Table rows={data?.positions ?? []} empty="Flat." cols={[["Market", (r) => <span className="small">{String(r.condition_id).slice(0, 12)}…</span>], ["Outcome", (r) => String(r.outcome)], ["Shares", (r) => fmt(r.shares)], ["Cost", (r) => fmt(r.cost)], ["Run", (r) => String(r.run_id || "live")]]} />
        </Card>
        <Card title="Fills">
          <Table rows={data?.fills ?? []} empty="No fills." cols={[["Time", (r) => ago(String(r.ts))], ["Shares", (r) => fmt(r.shares)], ["Price", (r) => fmt(r.price, 4)], ["Fee", (r) => fmt(r.fee, 4)]]} />
        </Card>
      </div>
    </>
  );
}
