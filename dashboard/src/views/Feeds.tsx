import type { Session } from "../api";
import { Card, ErrorLine, PageHead, Pill, Table, ago, fmt, unit, usePoll } from "../ui";

type Row = Record<string, unknown>;

const CAUSES: [RegExp, string][] = [
  [/tls handshake/i, "TLS handshake failed"],
  [/missing level|sequence gap/i, "Order book resync"],
  [/connection reset|reset by peer/i, "Connection reset"],
  [/connection refused/i, "Connection refused"],
  [/timed? ?out/i, "Timed out"],
  [/dns|resolve|name or service/i, "DNS lookup failed"],
  [/(?:http|status)\D{0,3}([45]\d\d)\b/i, "HTTP $1"],
];
/** Root cause of a raw transport error, without URLs and wrapper prefixes. */
function errorSummary(msg: unknown): string {
  const m = String(msg ?? "").trim();
  if (!m || m === "null") return "—";
  const hit = CAUSES.find(([re]) => re.test(m));
  if (hit) return hit[1].replace("$1", m.match(hit[0])?.[1] ?? "");
  const tail = m.replace(/https?:\/\/\S+/g, "").split(": ").filter(Boolean).pop() ?? m;
  return tail.charAt(0).toUpperCase() + tail.slice(1, 60);
}

export default function Feeds({ s }: { s: Session }) {
  const { data, error } = usePoll<{ health: Row[]; latency: Row[]; prices: Row[]; chainlink: Row[] }>(s, "/api/feeds", 3000);
  const lat = (f: unknown) => data?.latency.find((l) => l.feed === f);
  const up = data?.health.filter((h) => h.status === "CONNECTED").length ?? 0;
  const errored = data?.health.filter((h) => h.last_error) ?? [];
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
              ["Last error", (r) => (r.last_error ? <span className="err-cell" title={String(r.last_error)}><i className="dot warn" />{errorSummary(r.last_error)}</span> : <span className="muted">—</span>)],
            ]}
          />
          <p className="muted small" style={{ padding: "8px 18px 6px" }}>Latency includes the local clock offset (see Infrastructure). Negative values mean the source clock runs ahead of ours.</p>
        </Card>
        {errored.length > 0 && (
          <Card title="Feed errors" meta="latest error per feed · feeds reconnect automatically" flush>
            <Table
              rows={errored}
              cols={[
                ["Feed", (r) => String(r.feed)],
                ["Status", (r) => <Pill text={String(r.status)} />],
                ["Cause", (r) => errorSummary(r.last_error)],
                ["Reconnects", (r) => fmt(r.reconnects, 0)],
                ["Full message", (r) => <code className="err-msg">{String(r.last_error)}</code>],
              ]}
            />
          </Card>
        )}
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
