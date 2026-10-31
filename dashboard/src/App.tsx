import { useState, type FormEvent } from "react";
import { MODES, login, post, type Mode, type Session } from "./api";
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

function Login({ onLogin }: { onLogin: (s: Session) => void }) {
  const [backend, setBackend] = useState("This host");
  const [error, setError] = useState<string | null>(null);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const f = new FormData(e.currentTarget);
    try {
      onLogin(await login(BACKENDS[backend], String(f.get("username")), String(f.get("password"))));
    } catch (err) {
      setError((err as Error).message);
    }
  };
  return (
    <form className="login" onSubmit={submit}>
      <h1>Polygemy</h1>
      <label htmlFor="backend">Backend</label>
      <select id="backend" value={backend} onChange={(e) => setBackend(e.target.value)}>
        {Object.keys(BACKENDS).map((b) => <option key={b}>{b}</option>)}
      </select>
      <label htmlFor="username">Username</label>
      <input id="username" name="username" autoComplete="username" required />
      <label htmlFor="password">Password</label>
      <input id="password" name="password" type="password" autoComplete="current-password" required />
      <button type="submit">Log in</button>
      {error && <p className="error">{error}</p>}
    </form>
  );
}

function KillSwitch({ s }: { s: Session }) {
  const [msg, setMsg] = useState<string | null>(null);
  const flip = async (engaged: boolean) => {
    const reason = window.prompt(engaged ? "Reason for engaging the kill switch" : "Reason for releasing the kill switch");
    if (!reason?.trim()) return;
    try {
      await post(s, "/api/kill-switch", { engaged, reason });
      setMsg(engaged ? "Kill switch engaged." : "Kill switch released.");
    } catch (e) {
      setMsg((e as Error).message);
    }
  };
  return (
    <span className="kill">
      <button className="danger" onClick={() => flip(true)}>Engage kill switch</button>
      <button onClick={() => flip(false)}>Release</button>
      {msg && <span className="small">{msg}</span>}
    </span>
  );
}

export default function App() {
  const [s, setS] = useState<Session | null>(null);
  const [tab, setTab] = useState<Tab>("Overview");
  const [mode, setMode] = useState<Mode>("paper");
  if (!s) return <Login onLogin={setS} />;
  return (
    <div className={`app mode-${mode}`}>
      <header>
        <strong>Polygemy</strong>
        <nav>
          {TABS.map((t) => (
            <button key={t} className={t === tab ? "active" : ""} onClick={() => setTab(t)}>{t}</button>
          ))}
        </nav>
        <label className="mode">
          Data
          <select value={mode} onChange={(e) => setMode(e.target.value as Mode)}>
            {MODES.map((m) => <option key={m}>{m}</option>)}
          </select>
        </label>
        <span className={`badge mode-${mode}`}>{mode.toUpperCase()}</span>
        {s.role === "admin" && <KillSwitch s={s} />}
        <button onClick={() => setS(null)}>Log out</button>
      </header>
      <main>
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
