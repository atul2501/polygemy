import type { Session } from "../api";
import { Card, ErrorLine, Meter, PageHead, Pill, ago, fmt, usePoll } from "../ui";

// [key, label, meter]: "0-100" / "0-1" fill a bar; "±1" draws a tick around a centre line.
type Ind = [string, string, ("0-100" | "0-1" | "±1")?];
const GROUPS: [string, Ind[]][] = [
  ["Momentum", [["rsi7", "RSI 7", "0-100"], ["rsi14", "RSI 14", "0-100"], ["stoch_rsi", "Stoch RSI", "0-1"], ["macd_bps", "MACD (bps)"], ["macd_signal_bps", "MACD signal (bps)"], ["macd_hist_bps", "MACD histogram (bps)"]]],
  ["Trend", [["ema9_dev_bps", "Price vs EMA 9 (bps)"], ["ema21_dev_bps", "Price vs EMA 21 (bps)"], ["ema50_dev_bps", "Price vs EMA 50 (bps)"], ["vwap_dev_bps", "Price vs VWAP (bps)"], ["adx14", "ADX 14", "0-100"], ["bb_pctb", "Bollinger %B", "0-1"]]],
  ["Order book · Binance", [["bn_imb5", "Imbalance, 5 levels", "±1"], ["bn_spread_bps", "Spread (bps)"], ["bn_depth5_bid", "Bid depth 5 (BTC)"], ["bn_depth5_ask", "Ask depth 5 (BTC)"], ["bn_micro_bps", "Microprice vs mid (bps)"]]],
  ["Flow and cross-exchange", [["bn_cvd60", "Binance CVD 60 s"], ["cb_cvd60", "Coinbase CVD 60 s"], ["div_bn_cb_bps", "Binance − Coinbase (bps)"], ["div_bn_cl_bps", "Binance − Chainlink (bps)"], ["leadlag_bn_cl_1", "Lead-lag Binance → Chainlink (1 s)", "±1"]]],
  ["Volatility", [["atr14_bps", "ATR 14 (bps)"], ["bn_vol_60s", "Binance vol 60 s (bps)"], ["cl_sigma_1s_bps", "Chainlink σ per second (bps)"]]],
];

export default function Technical({ s }: { s: Session }) {
  const { data, error } = usePoll<{ ts: string | null; quality: string | null; values: Record<string, number | null> }>(s, "/api/technical", 2000);
  const v = (k: string) => data?.values[k] ?? null;
  const scaled = (k: string, m?: string) => {
    const x = v(k);
    if (x === null || !m) return null;
    return m === "0-100" ? x / 100 : m === "0-1" ? x : (x + 1) / 2;
  };
  return (
    <>
      <PageHead title="Technical and microstructure" sub={<>Binance 5 s bars · updated {data?.ts ? ago(data.ts) : "—"} · <Pill text={data?.quality} /></>} />
      <ErrorLine error={error} />
      <p className="banner info">Indicators are model inputs, not trading signals on their own.</p>
      <div className="groups">
        {GROUPS.map(([g, inds]) => (
          <Card key={g} title={g}>
            <div>
              {inds.map(([k, label, m]) => (
                <div className="ind" key={k}>
                  <span className="n">{label}</span>
                  <span className="v">{fmt(v(k), k.startsWith("bn_depth") ? 3 : 2)}</span>
                  {m && <Meter value={scaled(k, m)} centered={m === "±1"} label={label} />}
                </div>
              ))}
            </div>
          </Card>
        ))}
      </div>
    </>
  );
}
