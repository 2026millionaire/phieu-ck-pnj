# -*- coding: utf-8 -*-
"""Khởi tạo bảng dữ liệu riêng cho phân hệ ĐỀ XUẤT."""

import json

from .definitions import TEMPLATE_DEFINITIONS


def initialize_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS de_xuat_templates (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            slug             TEXT NOT NULL UNIQUE,
            name             TEXT NOT NULL,
            group_name       TEXT NOT NULL,
            description      TEXT DEFAULT '',
            icon             TEXT DEFAULT 'bi-file-earmark-text',
            fields_json      TEXT NOT NULL DEFAULT '[]',
            title_template   TEXT NOT NULL DEFAULT '',
            content_template TEXT NOT NULL DEFAULT '',
            reason_template  TEXT NOT NULL DEFAULT '',
            active           INTEGER NOT NULL DEFAULT 1,
            sort_order       INTEGER NOT NULL DEFAULT 0,
            created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS de_xuat (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            user_id             INTEGER NOT NULL DEFAULT 1,
            template_id         INTEGER NOT NULL,
            status              TEXT NOT NULL DEFAULT 'draft',
            title               TEXT NOT NULL DEFAULT '',
            proposal_content    TEXT NOT NULL DEFAULT '',
            reason_content      TEXT NOT NULL DEFAULT '',
            amount              REAL NOT NULL DEFAULT 0,
            approval_level      TEXT NOT NULL DEFAULT '',
            organization        TEXT NOT NULL DEFAULT '',
            attachments         TEXT NOT NULL DEFAULT '',
            form_data_json      TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(template_id) REFERENCES de_xuat_templates(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS de_xuat_versions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            de_xuat_id    INTEGER NOT NULL,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            user_id       INTEGER NOT NULL DEFAULT 1,
            source        TEXT NOT NULL DEFAULT 'manual',
            snapshot_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(de_xuat_id) REFERENCES de_xuat(id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_de_xuat_user_updated
        ON de_xuat (user_id, updated_at DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_de_xuat_versions_parent
        ON de_xuat_versions (de_xuat_id, created_at DESC)
    """)

    for definition in TEMPLATE_DEFINITIONS:
        conn.execute("""
            INSERT OR IGNORE INTO de_xuat_templates
                (slug, name, group_name, description, icon, fields_json,
                 title_template, content_template, reason_template, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            definition["slug"],
            definition["name"],
            definition["group_name"],
            definition["description"],
            definition["icon"],
            json.dumps(definition["fields"], ensure_ascii=False),
            definition["title_template"],
            definition["content_template"],
            definition["reason_template"],
            definition["sort_order"],
        ))
