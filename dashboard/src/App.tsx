import { useEffect, useRef, useState, type FormEvent } from "react";
import { MODES, login, post, type Mode, type Session } from "./api";
import { usePoll } from "./ui";
import Execution from "./views/Execution";
import Feeds from "./views/Feeds";
import Infrastructure from "./views/Infrastructure";
import Overview from "./views/Overview";
import Performance from "./views/Performance";
import Predictions from "./views/Predictions";
import Technical from "./views/Technical";

const TABS = ["Overview", "Feeds", "Technical", "Predictions", "Execution", "Performance", "Infrastructure"] as const;
type Tab = (typeof TABS)[number];
const BACKENDS: Record<string, string> = { "This host": "", "Stockholm (SSH tunnel :18000)": "http://127.0.0.1:18000" };

function Mark() {
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
      <path d="M2 13 L7 7 L10 10 L16 3" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="16" cy="3" r="1.6" fill="currentColor" />
    </svg>
  );
}

function Login({ onLogin }: { onLogin: (s: Session) => void }) {
  const [backend, setBackend] = useState("This host");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const f = new FormData(e.currentTarget);
    setBusy(true);
    try {
      onLogin(await login(BACKENDS[backend], String(f.get("username")), String(f.get("password"))));
    } catch (err) {
      setError((err as Error).message);
      setBusy(false);
    }
  };
  return (
    <div className="login-page mode-paper">
      <form className="login" onSubmit={submit}>
        <header>
          <span className="brand"><Mark /> Polygemy</span>
          <h1>Sign in to the console</h1>
          <p className="muted">BTC 5-minute markets: feeds, predictions and paper execution.</p>
        </header>
        <section className="card">
          <div className="card-body">
            <div className="field">
              <label htmlFor="backend">Backend</label>
              <select id="backend" value={backend} onChange={(e) => setBackend(e.target.value)}>
                {Object.keys(BACKENDS).map((b) => <option key={b}>{b}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="username">Username</label>
              <input id="username" name="username" autoComplete="username" required autoFocus />
            </div>
            <div className="field">
              <label htmlFor="password">Password</label>
              <input id="password" name="password" type="password" autoComplete="current-password" required />
            </div>
            {error && <p className="form-error" role="alert">{error}</p>}
            <button type="submit" className="primary" disabled={busy}>{busy ? "Signing in…" : "Sign in"}</button>
          </div>
        </section>
        <p className="foot">Live trading is disabled in this release.</p>
      </form>
    </div>
  );
}

/** Kill-switch state for everyone; admins get an inline control that asks for a reason (audited). */
function KillSwitch({ s }: { s: Session }) {
  const { data } = usePoll<{ kill_switch: { engaged: boolean; reason: string | null } }>(s, "/api/infrastructure", 10000);
  const [open, setOpen] = useState(false);
  const [reason, setReason] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const wrap = useRef<HTMLDivElement>(null);
  const engaged = Boolean(data?.kill_switch.engaged);
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent | KeyboardEvent) => {
      if (e instanceof KeyboardEvent ? e.key === "Escape" : !wrap.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", close);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", close);
    };
  }, [open]);
  const flip = async (e: FormEvent) => {
    e.preventDefault();
    try {
      await post(s, "/api/kill-switch", { engaged: !engaged, reason: reason.trim() });
      setMsg(null);
      setReason("");
      setOpen(false);
    } catch (err) {
      setMsg((err as Error).message);
    }
  };
  const state = (
    <span className="status">
      <i className={`dot ${engaged ? "bad pulse" : "good"}`} />
      {engaged ? "Kill switch engaged" : "Kill switch off"}
    </span>
  );
  if (s.role !== "admin") return state;
  return (
    <div className="popover-wrap" ref={wrap}>
      <button className="ghost" aria-expanded={open} aria-haspopup="dialog" onClick={() => setOpen(!open)}>{state}</button>
      {open && (
        <form className="popover" role="dialog" aria-label="Kill switch" onSubmit={flip}>
          <h3>{engaged ? "Release the kill switch" : "Engage the kill switch"}</h3>
          <p className="muted small">{engaged ? "New orders will be allowed again." : "Stops all new orders immediately, in every mode."} The change is audited.</p>
          <input placeholder="Reason (required)" value={reason} onChange={(e) => setReason(e.target.value)} autoFocus required />
          {msg && <p className="form-error">{msg}</p>}
          <div className="row">
            <button type="button" className="ghost" onClick={() => setOpen(false)}>Cancel</button>
            <button type="submit" className={engaged ? "primary" : "danger"} disabled={!reason.trim()}>{engaged ? "Release" : "Engage"}</button>
          </div>
        </form>
      )}
    </div>
  );
}

export default function App() {
  const [s, setS] = useState<Session | null>(null);
  const [tab, setTab] = useState<Tab>("Overview");
  const [mode, setMode] = useState<Mode>("paper");
  if (!s) return <Login onLogin={setS} />;
  return (
    <div className={`app mode-${mode}`}>
      <a className="skip" href="#main">Skip to content</a>
      <header className="topbar">
        <div className="topbar-inner">
          <span className="brand"><Mark /> Polygemy</span>
          <nav className="tabs" role="tablist" aria-label="Sections">
            {TABS.map((t) => (
              <button key={t} role="tab" aria-selected={t === tab} onClick={() => setTab(t)}>{t}</button>
            ))}
          </nav>
          <div className="tools">
            <div className="segmented" role="radiogroup" aria-label="Data mode">
              {MODES.map((m) => (
                <button key={m} role="radio" aria-checked={m === mode} className={`mode-${m}`} onClick={() => setMode(m)}>
                  <i className="swatch" /> {m}
                </button>
              ))}
            </div>
            <KillSwitch s={s} />
            <button className="ghost" onClick={() => setS(null)}>Log out</button>
          </div>
        </div>
      </header>
      <main id="main">
        {tab === "Overview" && <Overview s={s} mode={mode} />}
        {tab === "Feeds" && <Feeds s={s} />}
        {tab === "Technical" && <Technical s={s} />}
        {tab === "Predictions" && <Predictions s={s} mode={mode} />}
        {tab === "Execution" && <Execution s={s} mode={mode} />}
        {tab === "Performance" && <Performance s={s} mode={mode} />}
        {tab === "Infrastructure" && <Infrastructure s={s} />}
      </main>
    </div>
  );
}
