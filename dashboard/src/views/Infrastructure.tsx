import type { Session } from "../api";
import { Card, ErrorLine, Pill, Stat, Table, ago, fmt, usePoll } from "../ui";

type Row = Record<string, unknown>;
interface Infra { host: Row; database: Row; engine: Row[]; deployments: Row[]; errors: Row[]; kill_switch: Row; leases: Row[]; readiness: Row[]; models: Row[] }

export default function Infrastructure({ s }: { s: Session }) {
  const { data, error } = usePoll<Infra>(s, "/api/infrastructure", 10000);
  const h = data?.host;
  const db = data?.database;
  const dbPct = db ? (Number(db.bytes) / (Number(db.budget_gb) * 1073741824)) * 100 : null;
  const tone = (p: number | null) => (p === null ? undefined : p >= 90 ? "bad" : p >= 70 ? "warn" : "good");
  const signed = data?.readiness.filter((r) => r.signed_at).length ?? 0;
  return (
    <>
      <ErrorLine error={error} />
      <Card title={`Host ${String(h?.name ?? "")} · ${String(h?.app_env ?? "")}`} wide>
        <div className="stats">
          <Stat label="CPU" value={`${fmt(h?.cpu_percent, 0)}%`} sub={`${String(h?.cpus ?? "")} vCPU · load ${(h?.load_avg as number[] | undefined)?.map((x) => x.toFixed(2)).join(" ") ?? ""}`} />
          <Stat label="RAM" value={`${fmt(h?.ram_used_percent, 0)}%`} sub={`of ${fmt(Number(h?.ram_total) / 1073741824, 1)} GB`} tone={tone(Number(h?.ram_used_percent))} />
          <Stat label="Disk" value={`${fmt(h?.disk_used_percent, 0)}%`} sub={`of ${fmt(Number(h?.disk_total) / 1073741824, 0)} GB`} tone={tone(Number(h?.disk_used_percent))} />
          <Stat label="PostgreSQL" value={`${fmt(Number(db?.bytes) / 1073741824, 2)} GB`} sub={`${fmt(dbPct, 0)}% of ${String(db?.budget_gb ?? "")} GB budget · v${String(db?.version ?? "")}`} tone={tone(dbPct)} />
          <Stat label="Kill switch" value={<Pill text={data?.kill_switch.engaged ? "ENGAGED" : "off"} />} />
          <Stat label="Live readiness" value={`${signed} / ${data?.readiness.length ?? 0} signed`} tone={signed === data?.readiness.length ? "good" : "warn"} />
        </div>
      </Card>
      <div className="grid">
        <Card title="Engine">
          <Table rows={data?.engine ?? []} cols={[["When", (r) => ago(String(r.ts))], ["Instance", (r) => <span className="small">{String(r.instance_id)}</span>], ["Version", (r) => String(r.version)], ["Mode", (r) => String(r.mode ?? "stop")]]} />
        </Card>
        <Card title="Execution leader">
          <Table rows={data?.leases ?? []} empty="No lease held: no instance may submit live orders." cols={[["Account", (r) => String(r.account)], ["Holder", (r) => String(r.holder)], ["Expires", (r) => ago(String(r.expires_at))]]} />
        </Card>
        <Card title="Approved models">
          <Table rows={data?.models ?? []} empty="None approved." cols={[["Version", (r) => <span className="small">{String(r.version)}</span>], ["Horizon", (r) => String(r.horizon)]]} />
        </Card>
        <Card title="Deployments">
          <Table rows={data?.deployments ?? []} empty="No deployments recorded." cols={[["When", (r) => ago(String(r.deployed_at))], ["Env", (r) => String(r.app_env)], ["Version", (r) => String(r.version)], ["Status", (r) => <Pill text={String(r.status)} />]]} />
        </Card>
      </div>
      <Card title="Recent warnings and errors" wide>
        <Table rows={data?.errors ?? []} empty="None." cols={[["When", (r) => ago(String(r.ts))], ["Level", (r) => String(r.level)], ["Kind", (r) => String(r.kind)], ["Message", (r) => <span className="small">{String(r.message)}</span>]]} />
      </Card>
    </>
  );
}
