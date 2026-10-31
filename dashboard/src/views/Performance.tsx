import type { Mode, Session } from "../api";
import { Card, ErrorLine, LineChart, Stat, Table, fmt, pct, usePoll } from "../ui";

type Row = Record<string, unknown>;
interface Perf { markets: number; net_pnl: number; fees: number; win_rate: number | null; profit_factor: number | null; expectancy: number | null; max_drawdown: number; open_exposure: number; curve: { equity: number }[]; periods: Record<string, Row[]>; by_model: Record<string, Row>; note: string }

export default function Performance({ s, mode }: { s: Session; mode: Mode }) {
  const { data, error } = usePoll<Perf>(s, `/api/performance?mode=${mode}`, 10000);
  const d = data;
  return (
    <>
      <ErrorLine error={error} />
      <Card title={`Performance · ${mode}`} wide>
        <div className="stats">
          <Stat label="Net PnL" value={fmt(d?.net_pnl, 4)} tone={d && d.net_pnl > 0 ? "good" : d && d.net_pnl < 0 ? "bad" : undefined} />
          <Stat label="Markets traded" value={fmt(d?.markets, 0)} />
          <Stat label="Win rate" value={pct(d?.win_rate)} />
          <Stat label="Profit factor" value={fmt(d?.profit_factor)} />
          <Stat label="Expectancy / market" value={fmt(d?.expectancy, 4)} />
          <Stat label="Max drawdown" value={fmt(d?.max_drawdown, 4)} />
          <Stat label="Fees" value={fmt(d?.fees, 4)} />
          <Stat label="Open exposure" value={fmt(d?.open_exposure)} />
        </div>
        <LineChart points={(d?.curve ?? []).map((p) => p.equity)} label="Cumulative net PnL" />
        <p className="muted small">{d?.note}</p>
      </Card>
      <div className="grid">
        {(["day", "week", "month"] as const).map((p) => (
          <Card key={p} title={`By ${p}`}>
            <Table rows={d?.periods[p] ?? []} empty="No closed markets." cols={[[p, (r) => new Date(String(r.period)).toLocaleDateString()], ["Net", (r) => fmt(r.net, 4)], ["Markets", (r) => fmt(r.markets, 0)]]} />
          </Card>
        ))}
        <Card title="By model">
          <Table rows={Object.entries(d?.by_model ?? {}).map(([k, v]) => ({ model: k, ...v }))} cols={[["Model", (r) => <span className="small">{String(r.model)}</span>], ["Markets", (r) => fmt(r.markets, 0)], ["Net", (r) => fmt(r.net_pnl, 4)]]} />
        </Card>
      </div>
    </>
  );
}
