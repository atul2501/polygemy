import type { Session } from "../api";
import { Card, ErrorLine, Meter, PageHead, Pill, Stat, Table, ago, fmt, usePoll } from "../ui";

type Row = Record<string, unknown>;
interface Infra { host: Row; database: Row; engine: Row[]; deployments: Row[]; errors: Row[]; kill_switch: Row; leases: Row[]; readiness: Row[]; models: Row[] }

const GB = 1073741824;
const level = (p: number | null) => (p === null || !Number.isFinite(p) ? undefined : p >= 90 ? "bad" : p >= 70 ? "warn" : "good");

function Gauge({ label, used, sub }: { label: string; used: number | null; sub: string }) {
  return (
    <div className="stat">
      <span className="label">{label}</span>
      <span className="value">{fmt(used, 0)}%</span>
      <Meter value={used === null ? null : used / 100} tone={level(used)} label={`${label} used`} />
      <span className="sub">{sub}</span>
    </div>
  );
}

export default function Infrastructure({ s }: { s: Session }) {
  const { data, error } = usePoll<Infra>(s, "/api/infrastructure", 10000);
  const h = data?.host;
  const db = data?.database;
  const dbPct = db ? (Number(db.bytes) / (Number(db.budget_gb) * GB)) * 100 : null;
  const signed = data?.readiness.filter((r) => r.signed_at).length ?? 0;
  const total = data?.readiness.length ?? 0;
  return (
    <>
      <PageHead title="Infrastructure" sub={h ? `${String(h.name)} · ${String(h.app_env)} · ${String(h.cpus)} vCPU` : undefined} />
      <ErrorLine error={error} />
      <div className="stats">
        <Gauge label="CPU" used={h ? Number(h.cpu_percent) : null} sub={`load ${(h?.load_avg as number[] | undefined)?.map((x) => x.toFixed(2)).join(" · ") ?? "—"}`} />
        <Gauge label="Memory" used={h ? Number(h.ram_used_percent) : null} sub={`of ${fmt(Number(h?.ram_total) / GB, 1)} GB`} />
        <Gauge label="Disk" used={h ? Number(h.disk_used_percent) : null} sub={`of ${fmt(Number(h?.disk_total) / GB, 0)} GB`} />
        <Gauge label="Database budget" used={dbPct} sub={`${fmt(Number(db?.bytes) / GB, 2)} of ${String(db?.budget_gb ?? "—")} GB · PostgreSQL ${String(db?.version ?? "").split(" ")[0]}`} />
        <Stat label="Kill switch" value={<Pill text={data ? (data.kill_switch.engaged ? "ENGAGED" : "off") : null} />} sub={data ? `changed ${ago(String(data.kill_switch.changed_at))}` : undefined} />
        <div className="stat">
          <span className="label">Live readiness</span>
          <span className="value">{signed} / {total}</span>
          <Meter value={total ? signed / total : null} tone={signed === total && total ? "good" : "warn"} label="Readiness items signed" />
          <span className="sub">items signed off</span>
        </div>
      </div>
      <div className="grid">
        <Card title="Engine heartbeats" span={6} flush>
          <Table rows={data?.engine ?? []} cols={[["When", (r) => ago(String(r.ts))], ["Instance", (r) => <span className="dim">{String(r.instance_id).slice(0, 18)}…</span>], ["Version", (r) => String(r.version)], ["Mode", (r) => String(r.mode ?? "stopped")]]} />
        </Card>
        <Card title="Approved models" span={6} flush>
          <Table rows={data?.models ?? []} empty="None approved." cols={[["Version", (r) => <span className="dim">{String(r.version)}</span>], ["Horizon", (r) => String(r.horizon)], ["Approved", (r) => ago(String(r.approved_at))]]} />
        </Card>
        <Card title="Execution leader" span={6} flush>
          <Table rows={data?.leases ?? []} empty="No lease held, so no instance may submit live orders." cols={[["Account", (r) => String(r.account)], ["Holder", (r) => String(r.holder)], ["Expires", (r) => ago(String(r.expires_at))]]} />
        </Card>
        <Card title="Deployments" span={6} flush>
          <Table rows={data?.deployments ?? []} empty="No deployments recorded." cols={[["When", (r) => ago(String(r.deployed_at))], ["Env", (r) => String(r.app_env)], ["Version", (r) => String(r.version)], ["Status", (r) => <Pill text={String(r.status)} />]]} />
        </Card>
        <Card title="Recent warnings and errors" meta={`${data?.errors.length ?? 0} shown`} flush>
          <Table maxHeight={420} rows={data?.errors ?? []} empty="None." cols={[["When", (r) => ago(String(r.ts))], ["Level", (r) => <Pill text={String(r.level)} />], ["Kind", (r) => String(r.kind)], ["Message", (r) => <span className="wrap">{String(r.message)}</span>]]} />
        </Card>
      </div>
    </>
  );
}
