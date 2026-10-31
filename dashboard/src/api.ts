// Talks to one backend at a time: this host (local PC) or Stockholm through an SSH tunnel.
export type Mode = "paper" | "shadow" | "live" | "backtest";
export const MODES: Mode[] = ["paper", "shadow", "live", "backtest"];

export interface Session {
  base: string; // "" = same origin
  token: string;
  role: string;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export async function login(base: string, username: string, password: string): Promise<Session> {
  const r = await fetch(`${base}/api/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!r.ok) throw new ApiError(r.status, r.status === 401 ? "Wrong username or password." : `Login failed (${r.status}).`);
  const j = await r.json();
  return { base, token: j.token, role: j.role };
}

export async function get<T>(s: Session, path: string): Promise<T> {
  const r = await fetch(`${s.base}${path}`, { headers: { Authorization: `Bearer ${s.token}` } });
  if (!r.ok) throw new ApiError(r.status, `${path}: HTTP ${r.status}`);
  return r.json() as Promise<T>;
}

export async function post<T>(s: Session, path: string, body: unknown): Promise<T> {
  const r = await fetch(`${s.base}${path}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${s.token}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new ApiError(r.status, `${path}: HTTP ${r.status}`);
  return r.json() as Promise<T>;
}

export function liveSocket(s: Session, mode: Mode): WebSocket {
  const origin = s.base || window.location.origin;
  const url = origin.replace(/^http/, "ws") + `/api/live?mode=${mode}&token=${encodeURIComponent(s.token)}`;
  return new WebSocket(url);
}
