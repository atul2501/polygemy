import { useEffect, useState } from "react";
import { liveSocket, type Mode, type Session } from "../api";
import { Card, ErrorLine, Pill, Stat, Table, fmt, pct } from "../ui";

type Row = Record<string, unknown>;
interface Overview {
  market: Row | null;
  features: Record<string, number | string | null>;
  predictions?: Row[];
  decision?: Row | null;
  position?: Row | null;
  exposure: number;
  kill_switch: Row;
}

export default function OverviewView({ s, mode }: { s: Session; mode: Mode }) {
  const [o, setO] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    const ws = liveSocket(s, mode);
    ws.onmessage = (e) => (setO(JSON.parse(e.data)), setError(null));
    ws.onclose = (e) => setError(e.code === 4401 ? "session expired: log in again" : "live connection closed");
    return () => ws.close();
  }, [s, mode]);
  if (!o) return <ErrorLine error={error ?? "connecting…"} />;
  const f = o.features;
  const m = o.market;
  const settle = o.predictions?.find((p) => p.horizon === "settlement");
  const edge = settle && f.implied_p_up != null ? Number(settle.p_up) - Number(f.implied_p_up) : null;
  return (
    <>
      <ErrorLine error={error} />
      {Boolean(o.kill_switch.engaged) && <p className="banner bad">Kill switch ENGAGED: {String(o.kill_switch.reason)} ({String(o.kill_switch.changed_by)})</p>}
      <div className="grid">
        <Card title="Current market" wide>
          {m ? (
            <div className="stats">
              <Stat label="Market" value={String(m.slug)} sub={<Pill text={String(m.state)} />} />
              <Stat label="Countdown" value={`${fmt(m.seconds_left, 0)} s`} sub={String(m.methodology ?? "")} />
              <Stat label="BTC (Binance mid)" value={fmt(f.bn_mid)} sub={`Coinbase ${fmt(f.cb_mid)}`} />
              <Stat label="Chainlink TWAP 60 s" value={fmt(f.cl_twap)} sub={`spot ${fmt(f.cl_spot)} · age ${fmt(f.cl_twap_age_ms, 0)} ms`} />
              <Stat label="Price to beat" value={fmt(f.price_to_beat)} sub={`distance ${fmt(f.dist_twap_ptb_bps)} bps`} />
              <Stat label="Feature quality" value={<Pill text={String(f._quality ?? "—")} />} />
            </div>
          ) : (
            <p className="muted">No BTC 5-minute market is open right now.</p>
          )}
        </Card>
        <Card title="Probability of Up">
          <div className="stats">
            <Stat label="Model (settlement)" value={settle ? pct(settle.p_up) : "no approved model"} sub={settle ? `± ${pct(settle.uncertainty)}` : undefined} />
            <Stat label="Market implied" value={pct(f.implied_p_up)} sub={`Up ${fmt(f.up_bid)}/${fmt(f.up_ask)} · Down ${fmt(f.down_bid)}/${fmt(f.down_ask)}`} />
            <Stat label="Analytic baseline" value={pct(f.p_analytic)} />
            <Stat label="Edge vs market" value={edge === null ? "—" : pct(edge)} tone={edge === null ? undefined : edge > 0 ? "good" : "warn"} />
          </div>
          <Table
            rows={o.predictions ?? []}
            empty="No predictions: none of the horizons has an approved model."
            cols={[["Horizon", (r) => String(r.horizon)], ["P(Up)", (r) => pct(r.p_up)], ["Vol (bps)", (r) => fmt(r.volatility)], ["Model", (r) => String(r.model_version)], ["Latency", (r) => `${fmt(r.latency_us, 0)} µs`]]}
          />
        </Card>
        <Card title="Strategy and position">
          {o.decision ? (
            <div className="stats">
              <Stat label="Decision" value={String(o.decision.action)} sub={String(o.decision.explanation)} />
              <Stat label="Expected net" value={fmt(o.decision.expected_net, 4)} sub={`fair ${fmt(o.decision.fair_value, 4)}`} />
            </div>
          ) : (
            <p className="muted">No decision recorded for this market in {mode} mode.</p>
          )}
          <div className="stats">
            <Stat label="Position" value={o.position ? `${fmt(o.position.shares)} ${String(o.position.outcome)}` : "flat"} sub={o.position ? `cost ${fmt(o.position.cost)}` : undefined} />
            <Stat label="Total exposure" value={fmt(o.exposure)} />
          </div>
        </Card>
      </div>
    </>
  );
}
