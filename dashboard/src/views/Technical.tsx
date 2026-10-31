import type { Session } from "../api";
import { Card, ErrorLine, Pill, Stat, ago, fmt, usePoll } from "../ui";

const LABELS: Record<string, string> = {
  rsi7: "RSI 7", rsi14: "RSI 14", atr14_bps: "ATR 14 (bps)", ema9_dev_bps: "Price vs EMA 9 (bps)", ema21_dev_bps: "Price vs EMA 21 (bps)",
  ema50_dev_bps: "Price vs EMA 50 (bps)", macd_bps: "MACD (bps)", macd_signal_bps: "MACD signal (bps)", macd_hist_bps: "MACD hist (bps)",
  vwap_dev_bps: "Price vs VWAP (bps)", adx14: "ADX 14", bb_pctb: "Bollinger %B", stoch_rsi: "Stoch RSI", bn_imb5: "Book imbalance (5 lvls)",
  bn_spread_bps: "Spread (bps)", bn_depth5_bid: "Bid depth 5 (BTC)", bn_depth5_ask: "Ask depth 5 (BTC)", bn_micro_bps: "Microprice vs mid (bps)",
  bn_cvd60: "Binance CVD 60 s", cb_cvd60: "Coinbase CVD 60 s", div_bn_cb_bps: "Binance − Coinbase (bps)", div_bn_cl_bps: "Binance − Chainlink (bps)",
  cl_sigma_1s_bps: "Chainlink σ per s (bps)", bn_vol_60s: "Binance vol 60 s (bps)", leadlag_bn_cl_1: "Lead-lag Binance→Chainlink (1 s)",
};

export default function Technical({ s }: { s: Session }) {
  const { data, error } = usePoll<{ ts: string | null; quality: string | null; values: Record<string, number | null> }>(s, "/api/technical", 2000);
  return (
    <Card title={`Technical and microstructure (Binance 5 s bars) · ${data?.ts ? ago(data.ts) : "—"}`} wide>
      <ErrorLine error={error} />
      <p>Snapshot quality: <Pill text={data?.quality ?? "—"} /> Indicators are model inputs, not signals on their own.</p>
      <div className="stats">
        {Object.entries(LABELS).map(([k, label]) => <Stat key={k} label={label} value={fmt(data?.values[k], k.startsWith("bn_depth") ? 3 : 2)} />)}
      </div>
    </Card>
  );
}
