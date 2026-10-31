import { useEffect, useState, type ReactNode } from "react";
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

export const fmt = (v: unknown, dp = 2): string => {
  if (v === null || v === undefined || v === "") return "—";
  const n = typeof v === "number" ? v : Number(v);
  if (!Number.isFinite(n)) return String(v);
  return n.toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });
};
export const pct = (v: unknown, dp = 1) => (v === null || v === undefined ? "—" : `${fmt(Number(v) * 100, dp)}%`);
export const ago = (iso: string | null | undefined) => {
  if (!iso) return "—";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  return s < 90 ? `${s.toFixed(0)} s ago` : s < 5400 ? `${(s / 60).toFixed(0)} min ago` : new Date(iso).toLocaleString();
};

export function Card({ title, children, wide }: { title: string; children: ReactNode; wide?: boolean }) {
  return (
    <section className={wide ? "card wide" : "card"}>
      <h2>{title}</h2>
      {children}
    </section>
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
};
export function Pill({ text }: { text: string | null | undefined }) {
  return <span className={`pill ${TONE[text ?? ""] ?? "muted"}`}>{text ?? "—"}</span>;
}

export function Table({ cols, rows, empty = "Nothing yet." }: { cols: [string, (r: Record<string, unknown>) => ReactNode][]; rows: Record<string, unknown>[]; empty?: string }) {
  if (!rows.length) return <p className="muted">{empty}</p>;
  return (
    <div className="scroll">
      <table>
        <thead>
          <tr>{cols.map(([h]) => <th key={h}>{h}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{cols.map(([h, f]) => <td key={h}>{f(r)}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Minimal line chart: one series, faint grid, emphasised last point. */
export function LineChart({ points, height = 140, label }: { points: number[]; height?: number; label: string }) {
  if (points.length < 2) return <p className="muted">Not enough data for a chart yet.</p>;
  const w = 600;
  const min = Math.min(...points, 0);
  const max = Math.max(...points, 0);
  const span = max - min || 1;
  const xy = points.map((p, i) => [(i / (points.length - 1)) * w, height - ((p - min) / span) * (height - 10) - 5]);
  const d = xy.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const zero = height - ((0 - min) / span) * (height - 10) - 5;
  const last = xy[xy.length - 1];
  return (
    <svg viewBox={`0 0 ${w} ${height}`} className="chart" role="img" aria-label={label}>
      <line x1="0" x2={w} y1={zero} y2={zero} className="grid" />
      <path d={`${d} L${w},${zero} L0,${zero} Z`} className="area" />
      <path d={d} className="line" />
      <circle cx={last[0]} cy={last[1]} r="4" className="dot" />
    </svg>
  );
}

export function ErrorLine({ error }: { error: string | null }) {
  return error ? <p className="error">Backend: {error}</p> : null;
}
