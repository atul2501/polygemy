import { useEffect, useState } from "react";
import { liveSocket, type Mode, type Session } from "../api";
import { Card, ErrorLine, Meter, PageHead, Pill, Skeleton, Stat, Table, fmt, pct, short, unit, usd } from "../ui";

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

const WINDOW_S = 300;
const clock = (s: unknown) => {
  const n = Math.max(0, Math.round(Number(s)));
  return Number.isFinite(n) ? `${Math.floor(n / 60)}:${String(n % 60).padStart(2, "0")}` : "—";
};

/** Model, market and baseline P(Up) on one 0–100 % track, so the disagreement is visible at a glance. */
function ProbScale({ model, market, base }: { model: number | null; market: number | null; base: number | null }) {
  const at = (v: number) => ({ left: `${Math.max(0, Math.min(1, v)) * 100}%` });
  return (
    <div className="pscale">
      <div className="pscale-track" role="img" aria-label={`P(Up): model ${pct(model)}, market ${pct(market)}, baseline ${pct(base)}`}>
        <div className="pscale-bar" />
        <div className="pscale-mid" />
        {base !== null && <div className="pscale-mark base" style={at(base)}><i /></div>}
        {market !== null && <div className="pscale-mark market" style={at(market)}><i /></div>}
        {model !== null && <div className="pscale-mark model" style={at(model)}><i /></div>}
      </div>
      <div className="pscale-axis"><span>0% · Down</span><span>50%</span><span>Up · 100%</span></div>
      <div className="legend">
        <span><i style={{ background: "var(--accent)" }} /> Model {pct(model)}</span>
        <span><i style={{ background: "var(--ink)", borderRadius: 2 }} /> Market {pct(market)}</span>
        <span><i style={{ background: "var(--ink-3)" }} /> Baseline {pct(base)}</span>
      </div>
    </div>
  );
}

export default function OverviewView({ s, mode }: { s: Session; mode: Mode }) {
  const [o, setO] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    const ws = liveSocket(s, mode);
    ws.onmessage = (e) => (setO(JSON.parse(e.data)), setError(null));
    ws.onclose = (e) => setError(e.code === 4401 ? "session expired, log in again" : "live connection closed");
    return () => ws.close();
  }, [s, mode]);
  if (!o)
    return (
      <>
        <ErrorLine error={error} />
        <div className="grid">
          <Card title="Current market"><Skeleton rows={3} height={18} /></Card>
        </div>
      </>
    );
  const f = o.features;
  const m = o.market;
  const settle = o.predictions?.find((p) => p.horizon === "settlement");
  const n = (v: unknown) => (v === null || v === undefined ? null : Number(v));
  const pModel = settle ? n(settle.p_up) : null;
  const pMarket = n(f.implied_p_up);
  const edge = pModel !== null && pMarket !== null ? pModel - pMarket : null;
  const dist = n(f.dist_twap_ptb_bps);
  return (
    <>
      <PageHead title="Overview" sub={`Live · ${mode} data`} />
      <ErrorLine error={error} />
      {Boolean(o.kill_switch.engaged) && (
        <p className="banner" role="alert"><i className="dot bad pulse" /> Kill switch engaged: {String(o.kill_switch.reason)} (by {String(o.kill_switch.changed_by)})</p>
      )}
      <div className="grid">
        <Card title="Current market" meta={m ? <Pill text={String(f._quality ?? "")} /> : undefined}>
          {m ? (
            <div className="hero">
              <div className="clock">
                <span className="slug"><Pill text={String(m.state)} /> {String(m.slug)}</span>
                <span className="t" aria-label={`${fmt(m.seconds_left, 0)} seconds left`}>{clock(m.seconds_left)}<small>left</small></span>
                <Meter value={1 - Number(m.seconds_left) / WINDOW_S} label="Window elapsed" />
              </div>
              <div className="stats">
                <Stat label="Price to beat" value={fmt(f.price_to_beat)} sub="Chainlink TWAP at window start" />
                <Stat label="Chainlink TWAP 60 s" value={fmt(f.cl_twap)} sub={`spot ${fmt(f.cl_spot)} · ${fmt(f.cl_twap_age_ms, 0)} ms old`} />
                <Stat label="Distance" value={dist === null ? "—" : `${dist > 0 ? "+" : ""}${fmt(dist)} bps`} tone={dist === null || dist === 0 ? undefined : dist > 0 ? "good" : "bad"} sub={dist === null ? undefined : dist >= 0 ? "Up side of the line" : "Down side of the line"} />
                <Stat label="BTC · Binance" value={fmt(f.bn_mid)} sub={`Coinbase ${fmt(f.cb_mid)}`} />
              </div>
            </div>
          ) : (
            <p className="empty">No BTC 5-minute market is open right now. The next one is picked up automatically.</p>
          )}
        </Card>

        <Card title="Probability of Up" meta={settle ? short(settle.model_version, 34) : "no approved model"} span={7}>
          <ProbScale model={pModel} market={pMarket} base={n(f.p_analytic)} />
          <div className="stats">
            <Stat label="Edge vs market" value={edge === null ? "—" : `${edge > 0 ? "+" : ""}${pct(edge)}`} tone={edge === null ? undefined : Math.abs(edge) < 0.02 ? undefined : edge > 0 ? "good" : "bad"} sub="model − market" />
            <Stat label="Up book" value={`${fmt(f.up_bid)} / ${fmt(f.up_ask)}`} sub="bid / ask" />
            <Stat label="Down book" value={`${fmt(f.down_bid)} / ${fmt(f.down_ask)}`} sub="bid / ask" />
          </div>
        </Card>

        <Card title="Strategy and position" meta={`${mode} mode`} span={5}>
          {o.decision ? (
            <div className="stats plain">
              <Stat label="Decision" value={String(o.decision.action)} sub={String(o.decision.explanation)} />
              <Stat label="Expected net" value={usd(o.decision.expected_net, { dp: 4, signed: true })} sub={`fair value ${fmt(o.decision.fair_value, 4)}`} />
            </div>
          ) : (
            <p className="muted">No decision for this market in {mode} mode.</p>
          )}
          <div className="stats">
            <Stat label="Position" value={o.position ? `${fmt(o.position.shares)} ${String(o.position.outcome)}` : "Flat"} sub={o.position ? `cost ${usd(o.position.cost)}` : undefined} />
            <Stat label="Total exposure" value={usd(o.exposure)} />
          </div>
        </Card>

        <Card title="Predictions by horizon" flush>
          <Table
            rows={o.predictions ?? []}
            empty="No predictions: none of the horizons has an approved model."
            cols={[["Horizon", (r) => String(r.horizon)], ["P(Up)", (r) => pct(r.p_up)], ["Volatility", (r) => unit(r.volatility, "bps", 2)], ["Inference", (r) => unit(r.latency_us, "µs")], ["Model", (r) => <span className="dim">{String(r.model_version)}</span>]]}
          />
        </Card>
      </div>
    </>
  );
}
