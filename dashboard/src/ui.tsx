import { isValidElement, useEffect, useRef, useState, type CSSProperties, type PointerEvent, type ReactNode } from "react";
import { get, type Session } from "./api";

/** Polls an endpoint; keeps the last good value and shows errors instead of hiding them. */
export function usePoll<T>(s: Session, path: string, everyMs = 5000): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const load = () =>
      get<T>(s, path)
        .then((d) => alive && (setData(d), setError(null)))
        .catch((e: Error) => alive && setError(e.message));
    load();
    const t = setInterval(load, everyMs);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [s, path, everyMs]);
  return { data, error };
}

const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
};
// Typographic minus so negative numbers line up with positive ones in tabular figures.
const minus = (s: string) => s.replace(/^-/, "−");

export const fmt = (v: unknown, dp = 2): string => {
  const n = num(v);
  if (n === null) return v === null || v === undefined || v === "" ? "—" : String(v);
  return minus(n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp }));
};
/** Value with a unit, or a bare dash when there is no value. */
export const unit = (v: unknown, u: string, dp = 0) => (num(v) === null ? "—" : `${fmt(v, dp)} ${u}`);
export const pct = (v: unknown, dp = 1) => (num(v) === null ? "—" : `${fmt(Number(v) * 100, dp)}%`);
/** US dollars; `signed` adds + to gains (for PnL). */
export const usd = (v: unknown, { dp = 2, signed = false } = {}) => {
  const n = num(v);
  if (n === null) return "—";
  const body = Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
  return `${n < 0 ? "−" : signed && n > 0 ? "+" : ""}$${body}`;
};
export const tone = (v: unknown): "good" | "bad" | undefined => {
  const n = num(v);
  return n === null || n === 0 ? undefined : n > 0 ? "good" : "bad";
};
export const ago = (iso: string | null | undefined) => {
  if (!iso || iso === "null") return "—";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 0) return `in ${s > -90 ? `${(-s).toFixed(0)} s` : `${(-s / 60).toFixed(0)} min`}`;
  return s < 90 ? `${s.toFixed(0)} s ago` : s < 5400 ? `${(s / 60).toFixed(0)} min ago` : s < 129600 ? `${(s / 3600).toFixed(1)} h ago` : new Date(iso).toLocaleDateString();
};
export const short = (id: unknown, n = 10) => {
  const s = String(id ?? "");
  return s.length > n + 2 ? `${s.slice(0, n)}…` : s;
};

export function Card({ title, meta, children, span = 12, flush }: { title?: ReactNode; meta?: ReactNode; children: ReactNode; span?: 4 | 5 | 6 | 7 | 8 | 12; flush?: boolean }) {
  return (
    <section className={`card span-${span}`}>
      {title !== undefined && (
        <div className="card-head">
          <h2>{title}</h2>
          {meta !== undefined && <span className="meta">{meta}</span>}
        </div>
      )}
      <div className={flush ? "card-body flush" : "card-body"}>{children}</div>
    </section>
  );
}

export function PageHead({ title, sub }: { title: string; sub?: ReactNode }) {
  return (
    <div className="page-head">
      <h1>{title}</h1>
      {sub !== undefined && <p>{sub}</p>}
    </div>
  );
}

export function Stat({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: "good" | "warn" | "bad" }) {
  return (
    <div className={`stat ${tone ?? ""}`}>
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {sub !== undefined && <span className="sub">{sub}</span>}
    </div>
  );
}

const TONE: Record<string, string> = {
  CONNECTED: "good", DEGRADED: "warn", STALE: "warn", RECOVERING: "warn", DISCONNECTED: "bad",
  filled: "good", acked: "warn", partially_filled: "warn", cancelled: "muted", rejected: "bad", failed: "bad", unknown: "bad",
  ACTIVE: "good", READY: "good", PRELOADING: "warn", POSITION_MANAGEMENT: "warn", INVALID: "bad",
  valid: "good", degraded: "warn", invalid: "bad", approved: "good", candidate: "warn", retired: "muted",
  ENGAGED: "bad", off: "good", Up: "good", Down: "bad", error: "bad", warn: "warn", warning: "warn", success: "good",
};
const label = (t: string) => {
  const s = t.replace(/_/g, " ");
  return s === s.toUpperCase() || s === s.toLowerCase() ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : s;
};
export function Pill({ text }: { text: string | null | undefined }) {
  if (!text || text === "null" || text === "undefined") return <span className="pill">—</span>;
  return (
    <span className="pill">
      <i className={`dot ${TONE[text] ?? ""}`} />
      {label(text)}
    </span>
  );
}

const NUMERIC = /^[−+-]?\$?[\d,.]+(\s?(%|ms|µs|s|bps|GB|MB))?$/;
type Col = [string, (r: Record<string, unknown>) => ReactNode];
/** Numeric columns (detected from the first rows) are right-aligned so digits line up. */
export function Table({ cols, rows, empty = "Nothing yet.", maxHeight }: { cols: Col[]; rows: Record<string, unknown>[]; empty?: string; maxHeight?: number }) {
  if (!rows.length) return <p className="empty">{empty}</p>;
  const text = (v: ReactNode) => (isValidElement(v) ? (v.props as { children?: unknown }).children : v);
  const right = cols.map(([, f]) => {
    const vs = rows.slice(0, 8).map((r) => text(f(r))).filter((v) => v !== "—");
    return vs.length > 0 && vs.every((v) => typeof v === "string" && NUMERIC.test(v));
  });
  return (
    <div className="scroll" style={maxHeight ? ({ "--max-h": `${maxHeight}px` } as CSSProperties) : undefined}>
      <table>
        <thead>
          <tr>{cols.map(([h], j) => <th key={h} className={right[j] ? "r" : undefined}>{h}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{cols.map(([h, f], j) => <td key={h} className={right[j] ? "r" : undefined}>{f(r)}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Clean axis ticks: 1, 2 or 5 × 10^k steps. */
function ticks(min: number, max: number, n = 4): number[] {
  const raw = (max - min) / n || 1;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? raw;
  const out = [];
  for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) out.push(+v.toFixed(10));
  return out;
}

/** Rendered pixel width of an element, so SVG text stays at its CSS size instead of scaling. */
function useWidth(initial: number): [(el: HTMLDivElement | null) => void, number] {
  const [w, setW] = useState(initial);
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!el) return;
    const ro = new ResizeObserver(([e]) => setW(Math.max(320, Math.round(e.contentRect.width))));
    ro.observe(el);
    return () => ro.disconnect();
  }, [el]);
  return [setEl, w];
}

/** One series over time: recessive grid, labelled zero line, end dot, crosshair tooltip. */
export function LineChart({ points, label, format = (v) => fmt(v), height = 220 }: { points: { t: string; v: number }[]; label: string; format?: (v: number) => string; height?: number }) {
  const ref = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<number | null>(null);
  const [wrap, W] = useWidth(900);
  if (points.length < 2) return <p className="empty">Not enough data for a chart yet.</p>;
  const H = height, L = 64, R = 16, T = 12, B = 26;
  const vs = points.map((p) => p.v);
  const ys = ticks(Math.min(...vs, 0), Math.max(...vs, 0));
  const lo = Math.min(ys[0], ...vs), hi = Math.max(ys[ys.length - 1], ...vs);
  const t0 = new Date(points[0].t).getTime(), t1 = new Date(points[points.length - 1].t).getTime();
  const x = (t: string) => L + ((new Date(t).getTime() - t0) / (t1 - t0 || 1)) * (W - L - R);
  const y = (v: number) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - T - B);
  const d = points.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join("");
  const last = points[points.length - 1];
  const hm = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const onMove = (e: PointerEvent) => {
    const box = ref.current!.getBoundingClientRect();
    const px = ((e.clientX - box.left) / box.width) * W;
    let best = 0;
    points.forEach((p, i) => Math.abs(x(p.t) - px) < Math.abs(x(points[best].t) - px) && (best = i));
    setHover(best);
  };
  const hp = hover === null ? null : points[hover];
  return (
    <div className="chart-wrap" ref={wrap}>
      <svg ref={ref} viewBox={`0 0 ${W} ${H}`} className="chart" role="img" aria-label={`${label}: ${format(last.v)} at ${hm(last.t)}`} onPointerMove={onMove} onPointerLeave={() => setHover(null)}>
        {ys.map((v) => (
          <g key={v}>
            <line x1={L} x2={W - R} y1={y(v)} y2={y(v)} className={v === 0 ? "zero" : "gridline"} />
            <text x={L - 10} y={y(v) + 4} textAnchor="end">{format(v)}</text>
          </g>
        ))}
        <text x={L} y={H - 6}>{hm(points[0].t)}</text>
        <text x={W - R} y={H - 6} textAnchor="end">{hm(last.t)}</text>
        <path d={`${d}L${x(last.t)},${y(Math.max(lo, Math.min(0, hi)))}L${x(points[0].t)},${y(Math.max(lo, Math.min(0, hi)))}Z`} className="wash" />
        <path d={d} className="series" />
        {hp ? (
          <>
            <line x1={x(hp.t)} x2={x(hp.t)} y1={T} y2={H - B} className="hair" />
            <circle cx={x(hp.t)} cy={y(hp.v)} r="4.5" className="end" />
          </>
        ) : (
          <circle cx={x(last.t)} cy={y(last.v)} r="4.5" className="end" />
        )}
        <rect x={L} y={T} width={W - L - R} height={H - T - B} fill="transparent" />
      </svg>
      {hp && (
        <div className="tip" style={{ left: `${(x(hp.t) / W) * 100}%`, top: `${(y(hp.v) / H) * 100}%` }}>
          <strong>{format(hp.v)}</strong>
          <span>{new Date(hp.t).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}</span>
        </div>
      )}
    </div>
  );
}

/** 0–1 bar. `centered` draws a tick at the value instead of a fill (for signed or two-sided values). */
export function Meter({ value, tone, centered, label }: { value: number | null; tone?: string; centered?: boolean; label: string }) {
  const v = value === null || !Number.isFinite(value) ? null : Math.max(0, Math.min(1, value));
  return (
    <div className={`meter ${centered ? "tick" : ""} ${tone ?? ""}`} role="meter" aria-label={label} aria-valuemin={0} aria-valuemax={1} aria-valuenow={v ?? undefined}>
      {v !== null && <b style={centered ? ({ "--at": `calc(${v * 100}% - 1.5px)` } as CSSProperties) : { width: `${v * 100}%` }} />}
    </div>
  );
}

export function Skeleton({ rows = 3, height = 14 }: { rows?: number; height?: number }) {
  return (
    <div style={{ display: "grid", gap: 10 }} aria-busy="true" aria-label="Loading">
      {Array.from({ length: rows }, (_, i) => <div key={i} className="sk" style={{ height, width: `${92 - i * 14}%` }} />)}
    </div>
  );
}

export function ErrorLine({ error }: { error: string | null }) {
  return error ? (
    <p className="banner" role="alert">
      <i className="dot bad" /> Backend unreachable: {error}. Showing the last values received.
    </p>
  ) : null;
}
