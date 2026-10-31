//! Rust inference must reproduce Python's (scikit-learn / LightGBM) predictions exactly.
//! Fixture: `python -m polygemy.research.parity crates/engine/tests/fixtures/models_parity.json`.

use polygemy_engine::features::FeatureEngine;
use polygemy_engine::predict::Model;
use serde_json::Value;

#[test]
fn rust_matches_python_lightgbm_and_logistic() {
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/models_parity.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let names: Vec<&str> = fixture["feature_names"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        FeatureEngine::names(),
        "fixture was generated for a different feature version; regenerate it"
    );
    for case in fixture["cases"].as_array().unwrap() {
        let model = Model::from_artifact("parity", &case["artifact"]).unwrap();
        let kind = case["artifact"]["kind"].as_str().unwrap();
        for (i, (input, (exp, exp_raw))) in case["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .zip(
                case["expected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(case["expected_raw"].as_array().unwrap()),
            )
            .enumerate()
        {
            let x: Vec<f64> = input
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap_or(f64::NAN))
                .collect();
            let raw = model.raw(&x);
            let (p, _) = model.predict(&x);
            assert!(
                (raw - exp_raw.as_f64().unwrap()).abs() < 1e-9,
                "{kind} case {i}: raw {raw} vs {exp_raw}"
            );
            assert!(
                (p - exp.as_f64().unwrap()).abs() < 1e-9,
                "{kind} case {i}: calibrated {p} vs {exp}"
            );
        }
    }
}
