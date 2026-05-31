-- Example: local-only MySQL user (run as root on the server)
-- Replace YOUR_PASSWORD before executing.

CREATE DATABASE IF NOT EXISTS deepseek
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'deepseek'@'localhost' IDENTIFIED BY 'YOUR_PASSWORD';

GRANT SELECT, INSERT, UPDATE, DELETE ON deepseek.* TO 'deepseek'@'localhost';

FLUSH PRIVILEGES;
