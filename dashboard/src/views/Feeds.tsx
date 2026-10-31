import type { Session } from "../api";
import { Card, ErrorLine, Pill, Table, ago, fmt, usePoll } from "../ui";

type Row = Record<string, unknown>;
export default function Feeds({ s }: { s: Session }) {
  const { data, error } = usePoll<{ health: Row[]; latency: Row[]; prices: Row[]; chainlink: Row[] }>(s, "/api/feeds", 3000);
  const lat = (f: unknown) => data?.latency.find((l) => l.feed === f);
  return (
    <>
      <ErrorLine error={error} />
      <Card title="Feed health" wide>
        <Table
          rows={data?.health ?? []}
          cols={[
            ["Feed", (r) => `${String(r.feed)}${r.required ? " *" : ""}`],
            ["Status", (r) => <Pill text={String(r.status)} />],
            ["Data age", (r) => (r.age_s == null ? "—" : `${fmt(r.age_s, 1)} s`)],
            ["Latency p50 / p99", (r) => (lat(r.feed) ? `${fmt(lat(r.feed)!.feed_latency_p50, 0)} / ${fmt(lat(r.feed)!.feed_latency_p99, 0)} ms` : "—")],
            ["msg/min", (r) => fmt(lat(r.feed)?.messages, 0)],
            ["Reconnects", (r) => fmt(r.reconnects, 0)],
            ["Gaps", (r) => fmt(r.sequence_gaps, 0)],
            ["Dropped", (r) => fmt(r.dropped_records, 0)],
            ["Last error", (r) => <span className="small">{String(r.last_error ?? "")}</span>],
          ]}
        />
        <p className="muted small">* required for trading. Latency includes the local clock offset (see Infrastructure).</p>
      </Card>
      <div className="grid">
        <Card title="Latest prices">
          <Table rows={data?.prices ?? []} cols={[["Source", (r) => String(r.source)], ["Symbol", (r) => <span className="small">{String(r.symbol).slice(0, 18)}</span>], ["Kind", (r) => String(r.kind)], ["Price", (r) => fmt(r.price ?? r.bid, 4)], ["Spread", (r) => (r.bid != null && r.ask != null ? fmt(Number(r.ask) - Number(r.bid), 4) : "—")], ["When", (r) => ago(String(r.received_ts))]]} />
        </Card>
        <Card title="Chainlink BTC/USD">
          <Table rows={data?.chainlink ?? []} cols={[["Feed", (r) => String(r.feed)], ["Value", (r) => fmt(r.value)], ["Source time", (r) => ago(String(r.observed_ts))], ["Delay", (r) => `${fmt((new Date(String(r.received_ts)).getTime() - new Date(String(r.observed_ts)).getTime()) / 1000, 2)} s`]]} />
        </Card>
      </div>
    </>
  );
}
