//! Incremental technical indicators on OHLCV bars. Bounded memory; NaN until warmed up.
//! Wilder smoothing (RSI, ATR, ADX) and standard EMA (alpha = 2 / (n + 1)).

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Fixed-capacity window.
#[derive(Debug, Clone)]
pub struct Ring {
    cap: usize,
    v: VecDeque<f64>,
}

impl Ring {
    pub fn new(cap: usize) -> Self {
        Ring {
            cap,
            v: VecDeque::with_capacity(cap),
        }
    }
    pub fn push(&mut self, x: f64) {
        if self.v.len() == self.cap {
            self.v.pop_front();
        }
        self.v.push_back(x);
    }
    pub fn full(&self) -> bool {
        self.v.len() == self.cap
    }
    pub fn len(&self) -> usize {
        self.v.len()
    }
    pub fn is_empty(&self) -> bool {
        self.v.is_empty()
    }
    /// i = 0 is the newest value.
    pub fn back(&self, i: usize) -> f64 {
        if i < self.v.len() {
            self.v[self.v.len() - 1 - i]
        } else {
            f64::NAN
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = &f64> {
        self.v.iter()
    }
    pub fn mean(&self) -> f64 {
        if self.v.is_empty() {
            return f64::NAN;
        }
        self.v.iter().sum::<f64>() / self.v.len() as f64
    }
    /// Population standard deviation.
    pub fn std(&self) -> f64 {
        if self.v.len() < 2 {
            return f64::NAN;
        }
        let m = self.mean();
        (self.v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / self.v.len() as f64).sqrt()
    }
    pub fn min(&self) -> f64 {
        self.v.iter().copied().fold(f64::NAN, f64::min)
    }
    pub fn max(&self) -> f64 {
        self.v.iter().copied().fold(f64::NAN, f64::max)
    }
}

#[derive(Debug, Clone)]
pub struct Ema {
    alpha: f64,
    pub value: f64,
}

impl Ema {
    pub fn new(n: usize) -> Self {
        Ema {
            alpha: 2.0 / (n as f64 + 1.0),
            value: f64::NAN,
        }
    }
    pub fn update(&mut self, x: f64) -> f64 {
        self.value = if self.value.is_nan() {
            x
        } else {
            self.value + self.alpha * (x - self.value)
        };
        self.value
    }
}

/// Wilder RSI: simple average over the first n changes, then (prev * (n-1) + x) / n.
#[derive(Debug, Clone)]
pub struct Rsi {
    n: usize,
    prev: f64,
    count: usize,
    gain: f64,
    loss: f64,
    pub value: f64,
}

impl Rsi {
    pub fn new(n: usize) -> Self {
        Rsi {
            n,
            prev: f64::NAN,
            count: 0,
            gain: 0.0,
            loss: 0.0,
            value: f64::NAN,
        }
    }
    pub fn update(&mut self, close: f64) -> f64 {
        if !self.prev.is_nan() {
            let d = close - self.prev;
            let (g, l) = (d.max(0.0), (-d).max(0.0));
            let n = self.n as f64;
            self.count += 1;
            if self.count <= self.n {
                self.gain += g / n;
                self.loss += l / n;
            } else {
                self.gain = (self.gain * (n - 1.0) + g) / n;
                self.loss = (self.loss * (n - 1.0) + l) / n;
            }
            if self.count >= self.n {
                self.value = if self.loss == 0.0 {
                    if self.gain == 0.0 {
                        50.0
                    } else {
                        100.0
                    }
                } else {
                    100.0 - 100.0 / (1.0 + self.gain / self.loss)
                };
            }
        }
        self.prev = close;
        self.value
    }
}

fn true_range(b: &Bar, prev_close: f64) -> f64 {
    if prev_close.is_nan() {
        b.high - b.low
    } else {
        (b.high - b.low)
            .max((b.high - prev_close).abs())
            .max((b.low - prev_close).abs())
    }
}

/// Wilder ATR.
#[derive(Debug, Clone)]
pub struct Atr {
    n: usize,
    prev_close: f64,
    count: usize,
    pub value: f64,
}

impl Atr {
    pub fn new(n: usize) -> Self {
        Atr {
            n,
            prev_close: f64::NAN,
            count: 0,
            value: f64::NAN,
        }
    }
    pub fn update(&mut self, b: &Bar) -> f64 {
        let tr = true_range(b, self.prev_close);
        let n = self.n as f64;
        self.count += 1;
        self.value = if self.count <= self.n {
            let acc = if self.value.is_nan() {
                0.0
            } else {
                self.value * (self.count - 1) as f64
            };
            (acc + tr) / self.count as f64
        } else {
            (self.value * (n - 1.0) + tr) / n
        };
        self.prev_close = b.close;
        if self.count >= self.n {
            self.value
        } else {
            f64::NAN
        }
    }
    pub fn current(&self) -> f64 {
        if self.count >= self.n {
            self.value
        } else {
            f64::NAN
        }
    }
}

/// Wilder ADX(n): smoothed +DM/-DM/TR -> DX -> ADX (average of first n DX, then Wilder).
#[derive(Debug, Clone)]
pub struct Adx {
    n: usize,
    prev: Option<Bar>,
    count: usize,
    tr: f64,
    pdm: f64,
    mdm: f64,
    dx_count: usize,
    pub value: f64,
}

impl Adx {
    pub fn new(n: usize) -> Self {
        Adx {
            n,
            prev: None,
            count: 0,
            tr: 0.0,
            pdm: 0.0,
            mdm: 0.0,
            dx_count: 0,
            value: f64::NAN,
        }
    }
    pub fn update(&mut self, b: &Bar) -> f64 {
        let Some(p) = self.prev.replace(*b) else {
            return self.value;
        };
        let up = b.high - p.high;
        let down = p.low - b.low;
        let pdm = if up > down && up > 0.0 { up } else { 0.0 };
        let mdm = if down > up && down > 0.0 { down } else { 0.0 };
        let tr = true_range(b, p.close);
        let n = self.n as f64;
        self.count += 1;
        if self.count <= self.n {
            self.tr += tr;
            self.pdm += pdm;
            self.mdm += mdm;
        } else {
            self.tr = self.tr - self.tr / n + tr;
            self.pdm = self.pdm - self.pdm / n + pdm;
            self.mdm = self.mdm - self.mdm / n + mdm;
        }
        if self.count < self.n || self.tr == 0.0 {
            return self.value;
        }
        let pdi = 100.0 * self.pdm / self.tr;
        let mdi = 100.0 * self.mdm / self.tr;
        let dx = if pdi + mdi == 0.0 {
            0.0
        } else {
            100.0 * (pdi - mdi).abs() / (pdi + mdi)
        };
        self.dx_count += 1;
        self.value = if self.dx_count == 1 {
            dx
        } else if self.dx_count <= self.n {
            (self.value * (self.dx_count - 1) as f64 + dx) / self.dx_count as f64
        } else {
            (self.value * (n - 1.0) + dx) / n
        };
        if self.dx_count >= self.n {
            self.value
        } else {
            f64::NAN
        }
    }
    pub fn current(&self) -> f64 {
        if self.dx_count >= self.n {
            self.value
        } else {
            f64::NAN
        }
    }
}

/// Every bar-based indicator for one price series.
#[derive(Debug, Clone)]
pub struct Indicators {
    pub rsi7: Rsi,
    pub rsi14: Rsi,
    pub atr7: Atr,
    pub atr14: Atr,
    pub ema9: Ema,
    pub ema21: Ema,
    pub ema50: Ema,
    ema12: Ema,
    ema26: Ema,
    macd_signal: Ema,
    pub adx14: Adx,
    closes: Ring,
    rsi_hist: Ring,
    log_rets: Ring,
    pub last: Option<Bar>,
    bars: usize,
}

impl Default for Indicators {
    fn default() -> Self {
        Indicators {
            rsi7: Rsi::new(7),
            rsi14: Rsi::new(14),
            atr7: Atr::new(7),
            atr14: Atr::new(14),
            ema9: Ema::new(9),
            ema21: Ema::new(21),
            ema50: Ema::new(50),
            ema12: Ema::new(12),
            ema26: Ema::new(26),
            macd_signal: Ema::new(9),
            adx14: Adx::new(14),
            closes: Ring::new(60),
            rsi_hist: Ring::new(14),
            log_rets: Ring::new(60),
            last: None,
            bars: 0,
        }
    }
}

pub struct IndicatorValues {
    pub rsi7: f64,
    pub rsi14: f64,
    pub atr7: f64,
    pub atr14: f64,
    pub ema9: f64,
    pub ema21: f64,
    pub ema50: f64,
    pub macd: f64,
    pub macd_signal: f64,
    pub macd_hist: f64,
    pub bb_pctb: f64,
    pub bb_width: f64,
    pub adx14: f64,
    pub stoch_rsi: f64,
    pub roc6: f64,
    pub momentum6: f64,
    /// Std of log returns per bar over the last 12 bars / 60 bars.
    pub realized_vol: f64,
    pub historical_vol: f64,
    pub body_ratio: f64,
    pub upper_wick: f64,
    pub lower_wick: f64,
    pub acceleration: f64,
}

impl Indicators {
    pub fn update(&mut self, b: &Bar) {
        if let Some(prev) = self.last {
            if prev.close > 0.0 && b.close > 0.0 {
                self.log_rets.push((b.close / prev.close).ln());
            }
        }
        let rsi14 = self.rsi14.update(b.close);
        if !rsi14.is_nan() {
            self.rsi_hist.push(rsi14);
        }
        self.rsi7.update(b.close);
        self.atr7.update(b);
        self.atr14.update(b);
        self.ema9.update(b.close);
        self.ema21.update(b.close);
        self.ema50.update(b.close);
        let macd = self.ema12.update(b.close) - self.ema26.update(b.close);
        self.macd_signal.update(macd);
        self.adx14.update(b);
        self.closes.push(b.close);
        self.last = Some(*b);
        self.bars += 1;
    }

    pub fn values(&self) -> IndicatorValues {
        let warm = |min: usize, v: f64| if self.bars >= min { v } else { f64::NAN };
        let macd = self.ema12.value - self.ema26.value;
        // Bollinger(20, 2) on the newest 20 closes.
        let (bb_pctb, bb_width) = if self.closes.len() >= 20 {
            let last20: Vec<f64> = (0..20).map(|i| self.closes.back(i)).collect();
            let m = last20.iter().sum::<f64>() / 20.0;
            let sd = (last20.iter().map(|x| (x - m).powi(2)).sum::<f64>() / 20.0).sqrt();
            let (lo, hi) = (m - 2.0 * sd, m + 2.0 * sd);
            (
                if hi > lo {
                    (self.closes.back(0) - lo) / (hi - lo)
                } else {
                    0.5
                },
                if m != 0.0 { (hi - lo) / m } else { f64::NAN },
            )
        } else {
            (f64::NAN, f64::NAN)
        };
        let stoch_rsi = if self.rsi_hist.full() {
            let (lo, hi) = (self.rsi_hist.min(), self.rsi_hist.max());
            if hi > lo {
                (self.rsi_hist.back(0) - lo) / (hi - lo)
            } else {
                0.5
            }
        } else {
            f64::NAN
        };
        let c = self.closes.back(0);
        let c6 = self.closes.back(6);
        let recent_std = |n: usize| -> f64 {
            if self.log_rets.len() < n {
                return f64::NAN;
            }
            let v: Vec<f64> = (0..n).map(|i| self.log_rets.back(i)).collect();
            let m = v.iter().sum::<f64>() / n as f64;
            (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64).sqrt()
        };
        let (body, upper, lower) = match self.last {
            Some(b) if b.high > b.low => {
                let range = b.high - b.low;
                (
                    (b.close - b.open).abs() / range,
                    (b.high - b.close.max(b.open)) / range,
                    (b.close.min(b.open) - b.low) / range,
                )
            }
            Some(_) => (0.0, 0.0, 0.0),
            None => (f64::NAN, f64::NAN, f64::NAN),
        };
        let acceleration = self.log_rets.back(0) - self.log_rets.back(1);
        IndicatorValues {
            rsi7: self.rsi7.value,
            rsi14: self.rsi14.value,
            atr7: self.atr7.current(),
            atr14: self.atr14.current(),
            ema9: warm(9, self.ema9.value),
            ema21: warm(21, self.ema21.value),
            ema50: warm(50, self.ema50.value),
            macd: warm(26, macd),
            macd_signal: warm(34, self.macd_signal.value),
            macd_hist: warm(34, macd - self.macd_signal.value),
            bb_pctb,
            bb_width,
            adx14: self.adx14.current(),
            stoch_rsi,
            roc6: if c6 > 0.0 {
                (c / c6 - 1.0) * 100.0
            } else {
                f64::NAN
            },
            momentum6: c - c6,
            realized_vol: recent_std(12),
            historical_vol: recent_std(60),
            body_ratio: body,
            upper_wick: upper,
            lower_wick: lower,
            acceleration,
        }
    }
}

/// Standard normal CDF (Abramowitz–Stegun 7.1.26, |error| < 1.5e-7).
pub fn norm_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.327_591_1 * x.abs() / std::f64::consts::SQRT_2);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736
                + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    let erf = 1.0 - poly * (-(x * x) / 2.0).exp();
    if x >= 0.0 {
        0.5 * (1.0 + erf)
    } else {
        0.5 * (1.0 - erf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(c: f64) -> Bar {
        Bar {
            open: c,
            high: c,
            low: c,
            close: c,
            volume: 1.0,
        }
    }

    #[test]
    fn ema_matches_closed_form() {
        let mut e = Ema::new(3); // alpha 0.5
        assert_eq!(e.update(10.0), 10.0);
        assert_eq!(e.update(20.0), 15.0);
        assert_eq!(e.update(20.0), 17.5);
    }

    #[test]
    fn rsi_wilder_hand_computed() {
        // Changes: +1 +1 -1 +1 (n=3). First avg: gain 2/3, loss 1/3 -> RSI 66.667.
        // Then +1: gain (2/3*2+1)/3 = 7/9, loss (1/3*2)/3 = 2/9 -> RS 3.5 -> RSI 77.778.
        let mut r = Rsi::new(3);
        for c in [10.0, 11.0, 12.0, 11.0] {
            r.update(c);
        }
        assert!((r.value - 66.666_666_7).abs() < 1e-6, "{}", r.value);
        r.update(12.0);
        assert!((r.value - 77.777_777_8).abs() < 1e-6, "{}", r.value);
        let mut flat = Rsi::new(3);
        for _ in 0..5 {
            flat.update(5.0);
        }
        assert_eq!(flat.value, 50.0);
    }

    #[test]
    fn atr_wilder_hand_computed() {
        let bars = [
            Bar {
                open: 10.0,
                high: 11.0,
                low: 9.0,
                close: 10.0,
                volume: 1.0,
            }, // TR 2
            Bar {
                open: 10.0,
                high: 12.0,
                low: 10.0,
                close: 11.0,
                volume: 1.0,
            }, // TR 2
            Bar {
                open: 11.0,
                high: 11.0,
                low: 8.0,
                close: 9.0,
                volume: 1.0,
            }, // TR 3
            Bar {
                open: 9.0,
                high: 13.0,
                low: 9.0,
                close: 12.0,
                volume: 1.0,
            }, // TR 4
        ];
        let mut a = Atr::new(3);
        let v: Vec<f64> = bars.iter().map(|b| a.update(b)).collect();
        assert!(v[0].is_nan() && v[1].is_nan());
        assert!((v[2] - 7.0 / 3.0).abs() < 1e-12);
        assert!((v[3] - (7.0 / 3.0 * 2.0 + 4.0) / 3.0).abs() < 1e-12);
    }

    #[test]
    fn adx_strong_trend_is_high_and_flat_is_low() {
        let mut up = Adx::new(5);
        let mut flat = Adx::new(5);
        for i in 0..40 {
            let x = 100.0 + i as f64;
            up.update(&Bar {
                open: x,
                high: x + 1.0,
                low: x - 0.5,
                close: x + 0.8,
                volume: 1.0,
            });
            let y = 100.0 + if i % 2 == 0 { 1.0 } else { -1.0 };
            flat.update(&Bar {
                open: y,
                high: y + 1.0,
                low: y - 1.0,
                close: y,
                volume: 1.0,
            });
        }
        assert!(up.current() > 90.0, "{}", up.current());
        assert!(flat.current() < 20.0, "{}", flat.current());
    }

    #[test]
    fn indicator_bundle_warms_up_and_bounds_hold() {
        let mut ind = Indicators::default();
        assert!(ind.values().rsi14.is_nan());
        for i in 0..80 {
            let c = 100.0 + (i as f64 * 0.7).sin() * 3.0 + i as f64 * 0.05;
            ind.update(&Bar {
                open: c - 0.2,
                high: c + 0.5,
                low: c - 0.6,
                close: c,
                volume: 2.0,
            });
        }
        let v = ind.values();
        for (name, x) in [
            ("rsi7", v.rsi7),
            ("rsi14", v.rsi14),
            ("stoch", v.stoch_rsi * 100.0),
            ("adx", v.adx14),
        ] {
            assert!((0.0..=100.0).contains(&x), "{name} {x}");
        }
        assert!(v.atr14 > 0.0 && v.bb_width > 0.0 && v.historical_vol > 0.0);
        assert!((v.macd_hist - (v.macd - v.macd_signal)).abs() < 1e-12);
        assert!((0.0..=1.0).contains(&v.body_ratio));
        assert!((v.body_ratio + v.upper_wick + v.lower_wick - 1.0).abs() < 1e-9);
        // ROC over 6 bars from the closes themselves.
        let mut flat = Indicators::default();
        for c in [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0] {
            flat.update(&close(c));
        }
        assert!((flat.values().roc6 - 6.0).abs() < 1e-12);
        assert_eq!(flat.values().momentum6, 6.0);
    }

    #[test]
    fn normal_cdf() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-7);
        assert!((norm_cdf(1.96) - 0.975_002_1).abs() < 1e-6);
        assert!((norm_cdf(-1.0) - 0.158_655_3).abs() < 1e-6);
    }
}
