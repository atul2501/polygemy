//! Host resource alerts: disk, RAM and database budget at 70 / 80 / 90 %. An event is written
//! when a resource crosses a level (up or down), not on every sample.

use std::collections::HashMap;

/// Percent of the filesystem holding `path` that is used.
pub fn disk_used_pct(path: &str) -> Option<f64> {
    let c = std::ffi::CString::new(path).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: valid NUL-terminated path and a zeroed out-struct of the right type.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 || s.f_blocks == 0 {
        return None;
    }
    let total = s.f_blocks as f64 * s.f_frsize as f64;
    let avail = s.f_bavail as f64 * s.f_frsize as f64;
    Some((1.0 - avail / total) * 100.0)
}

/// Percent of RAM in use (Linux /proc/meminfo; None elsewhere).
pub fn ram_used_pct() -> Option<f64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let get = |k: &str| {
        text.lines()
            .find(|l| l.starts_with(k))?
            .split_whitespace()
            .nth(1)?
            .parse::<f64>()
            .ok()
    };
    let (total, avail) = (get("MemTotal:")?, get("MemAvailable:")?);
    Some((1.0 - avail / total) * 100.0)
}

/// 0 below 70 %, then 1, 2, 3 for 70 / 80 / 90 %.
pub fn level(pct: f64) -> u8 {
    match pct {
        p if p >= 90.0 => 3,
        p if p >= 80.0 => 2,
        p if p >= 70.0 => 1,
        _ => 0,
    }
}

#[derive(Default)]
pub struct Alerts {
    last: HashMap<&'static str, u8>,
}

impl Alerts {
    /// Returns (event level, message) when `resource` moved to a different alert level.
    pub fn observe(&mut self, resource: &'static str, pct: f64) -> Option<(&'static str, String)> {
        let lv = level(pct);
        let prev = self.last.insert(resource, lv).unwrap_or(0);
        if lv == prev {
            return None;
        }
        let sev = ["info", "warn", "error", "critical"][lv as usize];
        Some((
            sev,
            format!(
                "{resource} at {pct:.1}% ({})",
                if lv > prev { "rising" } else { "recovered" }
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alerts_fire_on_crossings_only() {
        let mut a = Alerts::default();
        assert!(a.observe("disk", 50.0).is_none());
        assert_eq!(a.observe("disk", 71.0).unwrap().0, "warn");
        assert!(a.observe("disk", 75.0).is_none(), "same level: no repeat");
        assert_eq!(a.observe("disk", 85.0).unwrap().0, "error");
        assert_eq!(a.observe("disk", 95.0).unwrap().0, "critical");
        assert!(a.observe("disk", 60.0).unwrap().1.contains("recovered"));
        assert!(disk_used_pct("/").is_some_and(|p| (0.0..=100.0).contains(&p)));
        assert!(disk_used_pct("/definitely/not/here").is_none());
    }
}
