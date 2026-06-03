-- DeepSeek / fleet analytics schema (MySQL 8+)
-- Canonical source: db/migrations/*.sql (applied via scripts/migrate_db.py)
-- This file is a snapshot for manual reference only.

CREATE TABLE IF NOT EXISTS symbol_config (
    symbol VARCHAR(20) NOT NULL PRIMARY KEY,
    config_json JSON NOT NULL,
    config_version INT NOT NULL DEFAULT 1,
    active TINYINT(1) NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by VARCHAR(50) NOT NULL DEFAULT 'manual'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS symbol_config_history (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    config_json JSON NOT NULL,
    config_version INT NOT NULL,
    previous_version INT NULL,
    updated_by VARCHAR(50) NOT NULL DEFAULT 'manual',
    reason TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_config_history_symbol (symbol),
    INDEX idx_symbol_config_history_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS decision_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    outcome VARCHAR(40) NULL,
    block_reason VARCHAR(80) NULL,
    config_snapshot JSON NOT NULL,
    market_snapshot JSON NULL,
    config_version INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_decision_events_symbol (symbol),
    INDEX idx_decision_events_type (event_type),
    INDEX idx_decision_events_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS trade_outcomes (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    entry_price DECIMAL(24, 8) NULL,
    exit_price DECIMAL(24, 8) NOT NULL,
    exit_qty DECIMAL(24, 8) NULL,
    realized_pnl DECIMAL(18, 8) NOT NULL,
    pnl_pct DECIMAL(12, 4) NULL,
    exit_type VARCHAR(32) NOT NULL,
    outcome VARCHAR(16) NOT NULL,
    sl_price DECIMAL(24, 8) NULL,
    tp_price DECIMAL(24, 8) NULL,
    tp_type VARCHAR(24) NULL,
    be_applied TINYINT(1) NOT NULL DEFAULT 0,
    dca_legs_placed INT UNSIGNED NULL,
    entry_opened_at TIMESTAMP NULL,
    entry_market_snapshot JSON NULL,
    entry_decision_event_id BIGINT UNSIGNED NULL,
    config_snapshot JSON NOT NULL,
    trade_context JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trade_outcomes_symbol (symbol),
    INDEX idx_trade_outcomes_outcome (outcome),
    INDEX idx_trade_outcomes_exit_type (exit_type),
    INDEX idx_trade_outcomes_created (created_at),
    INDEX idx_trade_outcomes_entry_event (entry_decision_event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
