//! Model inference in Rust (no Python on the hot path). Artifacts are produced by
//! `python -m polygemy.research` and stored in `model_versions.artifact`:
//!
//! ```json
//! {"format": 1, "kind": "lightgbm" | "logistic", "horizon": "settlement", "feature_version": 1,
//!  "features": ["name", ...],                  // model input order (subset of the engine's)
//!  "lightgbm": <Booster.dump_model()>,         // when kind = lightgbm
//!  "logistic": {"coef": [], "intercept": 0.0, "impute": [], "mean": [], "scale": []},
//!  "calibration": {"x": [], "y": [], "n": []}} // isotonic thresholds; n = training rows per step
//! ```

use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::features::{FeatureEngine, FEATURE_VERSION};
use crate::persist::{FeatureRow, PredictionRow};

pub const HORIZONS: [(&str, Option<f64>); 4] = [
    ("10s", Some(10.0)),
    ("30s", Some(30.0)),
    ("60s", Some(60.0)),
    ("settlement", None),
];

#[derive(Debug)]
enum Node {
    Split {
        feature: usize,
        threshold: f64,
        default_left: bool,
        missing: Missing,
        left: Box<Node>,
        right: Box<Node>,
    },
    Leaf(f64),
}

#[derive(Debug, Clone, Copy)]
enum Missing {
    None,
    Zero,
    NaN,
}

fn parse_node(v: &Value) -> Result<Node> {
    if let Some(leaf) = v.get("leaf_value") {
        return Ok(Node::Leaf(leaf.as_f64().context("leaf_value")?));
    }
    if v["decision_type"].as_str() != Some("<=") {
        bail!("unsupported decision_type {}", v["decision_type"]);
    }
    Ok(Node::Split {
        feature: v["split_feature"].as_u64().context("split_feature")? as usize,
        threshold: v["threshold"].as_f64().context("threshold")?,
        default_left: v["default_left"].as_bool().unwrap_or(true),
        missing: match v["missing_type"].as_str() {
            Some("NaN") => Missing::NaN,
            Some("Zero") => Missing::Zero,
            _ => Missing::None,
        },
        left: Box::new(parse_node(&v["left_child"])?),
        right: Box::new(parse_node(&v["right_child"])?),
    })
}

/// LightGBM's numerical decision rule, including its missing-value handling.
fn eval(node: &Node, x: &[f64]) -> f64 {
    let mut n = node;
    loop {
        match n {
            Node::Leaf(v) => return *v,
            Node::Split {
                feature,
                threshold,
                default_left,
                missing,
                left,
                right,
            } => {
                let v = x[*feature];
                let go_left = match missing {
                    Missing::NaN if v.is_nan() => *default_left,
                    Missing::Zero if v.is_nan() || v.abs() <= 1e-35 => *default_left,
                    _ => (if v.is_nan() { 0.0 } else { v }) <= *threshold,
                };
                n = if go_left { left } else { right };
            }
        }
    }
}

#[derive(Debug)]
enum Kind {
    Lightgbm {
        trees: Vec<Node>,
        sigmoid: f64,
    },
    Logistic {
        coef: Vec<f64>,
        intercept: f64,
        impute: Vec<f64>,
        mean: Vec<f64>,
        scale: Vec<f64>,
    },
}

#[derive(Debug)]
pub struct Model {
    pub version: String,
    pub horizon: String,
    /// Model input i reads engine feature `index[i]`.
    index: Vec<usize>,
    kind: Kind,
    calibration: Option<(Vec<f64>, Vec<f64>, Vec<f64>)>,
}

fn floats(v: &Value) -> Result<Vec<f64>> {
    v.as_array()
        .context("expected array")?
        .iter()
        .map(|x| x.as_f64().context("expected number"))
        .collect()
}

impl Model {
    pub fn from_artifact(version: &str, a: &Value) -> Result<Self> {
        if a["format"].as_i64() != Some(1) {
            bail!("unsupported artifact format {}", a["format"]);
        }
        if a["feature_version"].as_i64() != Some(FEATURE_VERSION as i64) {
            bail!(
                "model built for feature version {}, engine has {FEATURE_VERSION}",
                a["feature_version"]
            );
        }
        let names = FeatureEngine::names();
        let index = a["features"]
            .as_array()
            .context("features")?
            .iter()
            .map(|n| {
                let n = n.as_str().context("feature name")?;
                names
                    .iter()
                    .position(|x| *x == n)
                    .with_context(|| format!("model needs unknown feature {n}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let kind = match a["kind"].as_str() {
            Some("lightgbm") => {
                let d = &a["lightgbm"];
                let objective = d["objective"].as_str().unwrap_or_default();
                if !objective.starts_with("binary") {
                    bail!("lightgbm objective {objective:?} is not binary");
                }
                let sigmoid = objective
                    .split_whitespace()
                    .find_map(|p| p.strip_prefix("sigmoid:"))
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1.0);
                let trees = d["tree_info"]
                    .as_array()
                    .context("tree_info")?
                    .iter()
                    .map(|t| parse_node(&t["tree_structure"]))
                    .collect::<Result<Vec<_>>>()?;
                let model_features = d["feature_names"]
                    .as_array()
                    .map(|x| x.len())
                    .unwrap_or(index.len());
                if model_features != index.len() {
                    bail!(
                        "lightgbm has {model_features} features, artifact lists {}",
                        index.len()
                    );
                }
                Kind::Lightgbm { trees, sigmoid }
            }
            Some("logistic") => {
                let l = &a["logistic"];
                let k = Kind::Logistic {
                    coef: floats(&l["coef"])?,
                    intercept: l["intercept"].as_f64().context("intercept")?,
                    impute: floats(&l["impute"])?,
                    mean: floats(&l["mean"])?,
                    scale: floats(&l["scale"])?,
                };
                if let Kind::Logistic {
                    coef,
                    impute,
                    mean,
                    scale,
                    ..
                } = &k
                {
                    if [coef.len(), impute.len(), mean.len(), scale.len()]
                        .iter()
                        .any(|n| *n != index.len())
                    {
                        bail!("logistic vectors do not match {} features", index.len());
                    }
                }
                k
            }
            other => bail!("unknown model kind {other:?}"),
        };
        let calibration = match &a["calibration"] {
            Value::Null => None,
            c => {
                let (x, y, n) = (floats(&c["x"])?, floats(&c["y"])?, floats(&c["n"])?);
                if x.is_empty()
                    || x.len() != y.len()
                    || x.len() != n.len()
                    || x.windows(2).any(|w| w[0] > w[1])
                {
                    bail!("invalid calibration table");
                }
                Some((x, y, n))
            }
        };
        Ok(Model {
            version: version.to_string(),
            horizon: a["horizon"].as_str().unwrap_or_default().to_string(),
            index,
            kind,
            calibration,
        })
    }

    /// Raw model probability of Up, before calibration.
    pub fn raw(&self, features: &[f64]) -> f64 {
        let x: Vec<f64> = self.index.iter().map(|i| features[*i]).collect();
        let z = match &self.kind {
            Kind::Lightgbm { trees, sigmoid } => {
                sigmoid * trees.iter().map(|t| eval(t, &x)).sum::<f64>()
            }
            Kind::Logistic {
                coef,
                intercept,
                impute,
                mean,
                scale,
            } => {
                intercept
                    + x.iter()
                        .enumerate()
                        .map(|(i, v)| {
                            coef[i] * ((if v.is_nan() { impute[i] } else { *v }) - mean[i])
                                / scale[i]
                        })
                        .sum::<f64>()
            }
        };
        1.0 / (1.0 + (-z).exp())
    }

    /// (calibrated probability, standard error from the calibration step's training count).
    pub fn predict(&self, features: &[f64]) -> (f64, f64) {
        let p = self.raw(features);
        let Some((x, y, n)) = &self.calibration else {
            return (p, f64::NAN);
        };
        // Linear interpolation, clipped at the ends (sklearn IsotonicRegression out_of_bounds="clip").
        let i = x.partition_point(|t| *t <= p);
        let (pc, k) = if i == 0 {
            (y[0], 0)
        } else if i == x.len() {
            (y[x.len() - 1], x.len() - 1)
        } else {
            let w = if x[i] > x[i - 1] {
                (p - x[i - 1]) / (x[i] - x[i - 1])
            } else {
                0.0
            };
            (
                y[i - 1] + w * (y[i] - y[i - 1]),
                if w < 0.5 { i - 1 } else { i },
            )
        };
        let pc = pc.clamp(0.0, 1.0);
        (pc, (pc * (1.0 - pc) / n[k].max(1.0)).sqrt())
    }
}

/// Runs the approved model of every horizon on a feature row.
pub struct Predictor {
    pub models: Vec<Model>,
    pub mode: &'static str,
    sigma_idx: usize,
    t_rem_idx: usize,
}

impl Predictor {
    pub fn new(models: Vec<Model>, mode: &'static str) -> Self {
        let names = FeatureEngine::names();
        let pos = |n: &str| names.iter().position(|x| *x == n).expect("engine feature");
        Predictor {
            models,
            mode,
            sigma_idx: pos("cl_sigma_1s_bps"),
            t_rem_idx: pos("time_remaining_s"),
        }
    }

    pub fn predict(&self, row: &FeatureRow, data_age_ms: Option<i32>) -> Vec<PredictionRow> {
        let sigma = row.features[self.sigma_idx];
        self.models
            .iter()
            .map(|m| {
                let started = Instant::now();
                let (p, se) = m.predict(&row.features);
                let latency_us = started.elapsed().as_micros() as i32;
                let h = HORIZONS
                    .iter()
                    .find(|x| x.0 == m.horizon)
                    .and_then(|x| x.1)
                    .unwrap_or(row.features[self.t_rem_idx].max(1.0));
                // Volatility over the horizon (bps) and the implied mean move of a normal with
                // that spread and this direction probability.
                let vol = sigma * h.sqrt();
                PredictionRow {
                    ts_us: row.ts_us,
                    condition_id: row.condition_id.clone(),
                    horizon: HORIZONS
                        .iter()
                        .find(|x| x.0 == m.horizon)
                        .map(|x| x.0)
                        .unwrap_or("settlement"),
                    model_version: m.version.clone(),
                    feature_version: row.feature_version,
                    p_up: p,
                    expected_move: Some(
                        (2.0 * p - 1.0) * vol * (2.0 / std::f64::consts::PI).sqrt(),
                    )
                    .filter(|x| x.is_finite()),
                    volatility: Some(vol).filter(|x| x.is_finite()),
                    uncertainty: Some(se).filter(|x| x.is_finite()),
                    degraded: row.degraded,
                    data_age_ms,
                    latency_us,
                    mode: self.mode,
                }
            })
            .collect()
    }
}

/// Approved models for the engine's feature version (none approved => no predictions, not fakes).
pub async fn load_approved(pool: &sqlx::PgPool) -> Result<Vec<Model>> {
    let rows: Vec<(String, Value)> = sqlx::query_as("SELECT version, artifact FROM model_versions WHERE status = 'approved' AND feature_version = $1 ORDER BY horizon")
        .bind(FEATURE_VERSION)
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|(v, a)| Model::from_artifact(v, a))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names() -> Vec<&'static str> {
        FeatureEngine::names()
    }

    #[test]
    fn lightgbm_rules_and_missing_values() {
        let names = names();
        let art = json!({"format": 1, "kind": "lightgbm", "horizon": "settlement", "feature_version": FEATURE_VERSION,
            "features": [names[0], names[1]],
            "lightgbm": {"objective": "binary sigmoid:1", "feature_names": ["a", "b"], "tree_info": [
                {"tree_structure": {"split_feature": 0, "threshold": 10.0, "decision_type": "<=", "default_left": false, "missing_type": "NaN",
                    "left_child": {"leaf_value": -1.0}, "right_child": {"leaf_value": 1.0}}},
                {"tree_structure": {"split_feature": 1, "threshold": 0.5, "decision_type": "<=", "default_left": true, "missing_type": "None",
                    "left_child": {"leaf_value": 0.25}, "right_child": {"leaf_value": -0.25}}}]},
            "calibration": null});
        let m = Model::from_artifact("v", &art).unwrap();
        let mut x = vec![f64::NAN; names.len()];
        let sig = |z: f64| 1.0 / (1.0 + (-z).exp());
        x[0] = 5.0;
        x[1] = 1.0;
        assert!((m.raw(&x) - sig(-1.25)).abs() < 1e-15);
        x[0] = f64::NAN; // NaN with missing_type NaN -> default (right)
        x[1] = f64::NAN; // NaN with missing_type None -> treated as 0 -> left
        assert!((m.raw(&x) - sig(1.25)).abs() < 1e-15);
    }

    #[test]
    fn logistic_imputes_and_calibration_interpolates() {
        let names = names();
        let art = json!({"format": 1, "kind": "logistic", "horizon": "10s", "feature_version": FEATURE_VERSION,
            "features": [names[0]],
            "logistic": {"coef": [2.0], "intercept": 0.0, "impute": [3.0], "mean": [1.0], "scale": [2.0]},
            "calibration": {"x": [0.0, 0.5, 1.0], "y": [0.1, 0.4, 0.9], "n": [100.0, 400.0, 100.0]}});
        let m = Model::from_artifact("v", &art).unwrap();
        let mut x = vec![f64::NAN; names.len()];
        // Missing -> impute 3 -> z = 2 * (3 - 1) / 2 = 2.
        assert!((m.raw(&x) - 1.0 / (1.0 + (-2.0f64).exp())).abs() < 1e-15);
        x[0] = 1.0; // z = 0 -> raw 0.5 -> calibrated exactly 0.4
        let (p, se) = m.predict(&x);
        assert!((p - 0.4).abs() < 1e-12);
        assert!((se - (0.4 * 0.6 / 400.0f64).sqrt()).abs() < 1e-12);
    }

    #[test]
    fn rejects_mismatched_artifacts() {
        let names = names();
        let base = json!({"format": 1, "kind": "logistic", "horizon": "10s", "feature_version": FEATURE_VERSION, "features": [names[0]],
            "logistic": {"coef": [1.0], "intercept": 0.0, "impute": [0.0], "mean": [0.0], "scale": [1.0]}, "calibration": null});
        assert!(Model::from_artifact("v", &base).is_ok());
        let mut a = base.clone();
        a["feature_version"] = json!(999);
        assert!(Model::from_artifact("v", &a).is_err());
        let mut a = base.clone();
        a["features"] = json!(["no_such_feature"]);
        assert!(Model::from_artifact("v", &a).is_err());
        let mut a = base;
        a["logistic"]["coef"] = json!([1.0, 2.0]);
        assert!(Model::from_artifact("v", &a).is_err());
    }
}
