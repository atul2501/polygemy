import type { Mode, Session } from "../api";
import { Card, ErrorLine, LineChart, PageHead, Skeleton, Stat, Table, fmt, pct, short, tone, usd, usePoll } from "../ui";

type Row = Record<string, unknown>;
interface Perf { markets: number; net_pnl: number; fees: number; win_rate: number | null; profit_factor: number | null; expectancy: number | null; max_drawdown: number; open_exposure: number; curve: { t: string; equity: number }[]; periods: Record<string, Row[]>; by_model: Record<string, Row>; note: string }

const signed = (v: unknown) => usd(v, { signed: true });
const DATE: Record<string, Intl.DateTimeFormatOptions> = { day: { month: "short", day: "numeric" }, week: { month: "short", day: "numeric" }, month: { month: "long", year: "numeric" } };

export default function Performance({ s, mode }: { s: Session; mode: Mode }) {
  const { data: d, error } = usePoll<Perf>(s, `/api/performance?mode=${mode}`, 10000);
  return (
    <>
      <PageHead title="Performance" sub={`${mode} · net of fees`} />
      <ErrorLine error={error} />
      <div className="grid">
        <Card title="Cumulative net PnL" meta={d ? `${fmt(d.markets, 0)} settled markets` : undefined}>
          {d ? (
            <>
              <div className="stats">
                <Stat label="Net PnL" value={signed(d.net_pnl)} tone={tone(d.net_pnl)} sub={`after ${usd(d.fees)} fees`} />
                <Stat label="Win rate" value={pct(d.win_rate)} sub={`${fmt(d.markets, 0)} markets`} />
                <Stat label="Profit factor" value={fmt(d.profit_factor)} tone={d.profit_factor === null ? undefined : d.profit_factor >= 1 ? "good" : "bad"} sub="gross win ÷ gross loss" />
                <Stat label="Expectancy" value={signed(d.expectancy)} tone={tone(d.expectancy)} sub="per market" />
                <Stat label="Max drawdown" value={usd(d.max_drawdown)} />
                <Stat label="Open exposure" value={usd(d.open_exposure)} />
              </div>
              <LineChart points={d.curve.map((p) => ({ t: p.t, v: p.equity }))} label="Cumulative net PnL" format={(v) => usd(v, { dp: 0 })} />
              <p className="muted small">{d.note}</p>
            </>
          ) : (
            <Skeleton rows={4} height={20} />
          )}
        </Card>
        {(["day", "week", "month"] as const).map((p) => (
          <Card key={p} title={`By ${p}`} span={4} flush>
            <Table rows={d?.periods[p] ?? []} empty="No settled markets." cols={[[p[0].toUpperCase() + p.slice(1), (r) => new Date(String(r.period)).toLocaleDateString([], DATE[p])], ["Markets", (r) => fmt(r.markets, 0)], ["Net", (r) => <span className={tone(r.net)}>{signed(r.net)}</span>]]} />
          </Card>
        ))}
        <Card title="By model" flush>
          <Table rows={Object.entries(d?.by_model ?? {}).map(([k, v]) => ({ model: k, ...v }))} empty="No settled markets." cols={[["Model", (r) => <span className="dim">{short(r.model, 40)}</span>], ["Markets", (r) => fmt(r.markets, 0)], ["Net", (r) => signed(r.net_pnl)]]} />
        </Card>
      </div>
    </>
  );
}

