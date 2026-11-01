-- End-game 0.99 maker dry run: bids, fills, wins and profit per asset and phase.
--   psql "$DATABASE_URL" -v hours=24 -f scripts/endgame_report.sql
-- Phase: "early" = joined before the window ended (some reversal risk); "after end" = joined once
-- the deciding Chainlink TWAP report was in (should never lose). Profit = settled filled shares
-- x (1 if our side won else 0) - 0.99, no fee (maker).
\if :{?hours} \else \set hours 24 \endif
WITH b AS (
    SELECT split_part(m.series_slug, '-', 1) AS asset,
           CASE WHEN s.explanation LIKE 'decided after end%' THEN 'after end' ELSE 'early' END AS phase,
           o.filled_shares, o.limit_price, o.outcome, l.official_outcome
    FROM orders o
    JOIN markets m ON m.condition_id = o.condition_id
    LEFT JOIN strategy_signals s ON s.id = o.signal_id
    LEFT JOIN market_lifecycles l ON l.condition_id = o.condition_id
    WHERE o.time_in_force = 'POST_ONLY' AND o.mode = 'paper'
      AND o.created_at > now() - make_interval(hours => :hours)
)
SELECT asset, phase,
       count(*) AS bids,
       count(*) FILTER (WHERE filled_shares > 0) AS filled,
       round(100.0 * count(*) FILTER (WHERE filled_shares > 0) / count(*), 1) AS fill_pct,
       count(*) FILTER (WHERE filled_shares > 0 AND official_outcome = outcome) AS wins,
       count(*) FILTER (WHERE filled_shares > 0 AND official_outcome IS NOT NULL AND official_outcome <> outcome) AS losses,
       count(*) FILTER (WHERE filled_shares > 0 AND official_outcome IS NULL) AS unsettled,
       round(sum(filled_shares * ((official_outcome = outcome)::int - limit_price))
             FILTER (WHERE filled_shares > 0 AND official_outcome IS NOT NULL), 2) AS profit_usd
FROM b
GROUP BY 1, 2
ORDER BY 1, 2;
-- Break-even: a loss costs ~99 wins at 0.99, so the win rate must stay above 99 %.
