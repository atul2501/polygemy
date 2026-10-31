import type { Session } from "../api";
import { Card, ErrorLine, PageHead, Pill, Table, ago, fmt, unit, usePoll } from "../ui";

type Row = Record<string, unknown>;
export default function Feeds({ s }: { s: Session }) {
  const { data, error } = usePoll<{ health: Row[]; latency: Row[]; prices: Row[]; chainlink: Row[] }>(s, "/api/feeds", 3000);
  const lat = (f: unknown) => data?.latency.find((l) => l.feed === f);
  const up = data?.health.filter((h) => h.status === "CONNECTED").length ?? 0;
  return (
    <>
      <PageHead title="Feeds" sub={data ? `${up} of ${data.health.length} connected · refreshes every 3 s` : undefined} />
      <ErrorLine error={error} />
      <div className="grid">
        <Card title="Feed health" meta="* required for trading" flush>
          <Table
            rows={data?.health ?? []}
            cols={[
              ["Feed", (r) => `${String(r.feed)}${r.required ? " *" : ""}`],
              ["Status", (r) => <Pill text={String(r.status)} />],
              ["Data age", (r) => unit(r.age_s, "s", 1)],
              ["Latency p50", (r) => unit(lat(r.feed)?.feed_latency_p50, "ms")],
              ["p99", (r) => unit(lat(r.feed)?.feed_latency_p99, "ms")],
              ["Msg / min", (r) => fmt(lat(r.feed)?.messages, 0)],
              ["Reconnects", (r) => fmt(r.reconnects, 0)],
              ["Gaps", (r) => fmt(r.sequence_gaps, 0)],
              ["Dropped", (r) => fmt(r.dropped_records, 0)],
              ["Last error", (r) => <span className="wrap">{String(r.last_error ?? "")}</span>],
            ]}
          />
          <p className="muted small" style={{ padding: "8px 18px 6px" }}>Latency includes the local clock offset (see Infrastructure). Negative values mean the source clock runs ahead of ours.</p>
        </Card>
        <Card title="Latest prices" span={7} flush>
          <Table rows={data?.prices ?? []} cols={[["Source", (r) => String(r.source)], ["Symbol", (r) => <span className="dim">{String(r.symbol).slice(0, 18)}</span>], ["Kind", (r) => String(r.kind)], ["Price", (r) => fmt(r.price ?? r.bid, 2)], ["Spread", (r) => (r.bid != null && r.ask != null ? fmt(Number(r.ask) - Number(r.bid), 2) : "—")], ["Received", (r) => ago(String(r.received_ts))]]} />
        </Card>
        <Card title="Chainlink BTC/USD" span={5} flush>
          <Table rows={data?.chainlink ?? []} cols={[["Feed", (r) => String(r.feed)], ["Value", (r) => fmt(r.value)], ["Source time", (r) => ago(String(r.observed_ts))], ["Delay", (r) => `${fmt((new Date(String(r.received_ts)).getTime() - new Date(String(r.observed_ts)).getTime()) / 1000, 2)} s`]]} />
        </Card>
      </div>
    </>
  );
}
