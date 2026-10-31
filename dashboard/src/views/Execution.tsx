import type { Mode, Session } from "../api";
import { Card, ErrorLine, PageHead, Pill, Stat, Table, ago, fmt, short, usd, usePoll } from "../ui";

type Row = Record<string, unknown>;
export default function Execution({ s, mode }: { s: Session; mode: Mode }) {
  const { data, error } = usePoll<{ orders: Row[]; fills: Row[]; positions: Row[]; by_status: Row[]; summary: Row }>(s, `/api/execution?mode=${mode}`, 5000);
  const lat = (r: Row) => {
    const t = r.timings as Record<string, number> | null;
    return t?.signal && t?.final ? `${fmt((t.final - t.signal) / 1000, 0)} ms` : "—";
  };
  return (
    <>
      <PageHead title="Execution" sub={`${mode} orders, fills and open positions`} />
      <ErrorLine error={error} />
      <div className="stats">
        {(data?.by_status ?? []).map((b) => <Stat key={String(b.status)} label={`Orders · ${String(b.status).replace(/_/g, " ")}`} value={fmt(b.n, 0)} />)}
        <Stat label="Avg slippage vs decision" value={fmt(data?.summary.avg_slippage, 4)} sub="price units per share" />
        <Stat label="Fees paid" value={usd(data?.summary.fees)} />
      </div>
      <div className="grid">
        <Card title="Orders" meta={mode} flush>
          <Table maxHeight={420} rows={data?.orders ?? []} empty="No orders in this mode." cols={[["Time", (r) => ago(String(r.created_at))], ["Side", (r) => `${String(r.side)} ${String(r.outcome)}`], ["Shares", (r) => fmt(r.shares)], ["Limit", (r) => fmt(r.limit_price, 3)], ["Status", (r) => <Pill text={String(r.status)} />], ["Filled", (r) => fmt(r.filled_shares)], ["Avg price", (r) => fmt(r.avg_price, 4)], ["Fee", (r) => usd(r.fee, { dp: 4 })], ["Signal → final", lat], ["Note", (r) => <span className="wrap">{String(r.error ?? "")}</span>]]} />
        </Card>
        <Card title="Open positions" span={6} flush>
          <Table rows={data?.positions ?? []} empty="Flat: no open positions." cols={[["Market", (r) => <span className="dim">{short(r.condition_id, 12)}</span>], ["Outcome", (r) => <Pill text={String(r.outcome)} />], ["Shares", (r) => fmt(r.shares)], ["Cost", (r) => usd(r.cost)], ["Run", (r) => String(r.run_id || mode)]]} />
        </Card>
        <Card title="Fills" span={6} flush>
          <Table maxHeight={360} rows={data?.fills ?? []} empty="No fills." cols={[["Time", (r) => ago(String(r.ts))], ["Shares", (r) => fmt(r.shares)], ["Price", (r) => fmt(r.price, 4)], ["Fee", (r) => usd(r.fee, { dp: 4 })]]} />
        </Card>
      </div>
    </>
  );
}
