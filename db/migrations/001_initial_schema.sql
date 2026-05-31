-- Initial schema: symbol config, history, and decision audit tables.

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
