# -*- coding: utf-8 -*-
"""Phân hệ ĐỀ XUẤT dùng chung trong ứng dụng PNJ 1305."""

from .routes import create_de_xuat_blueprint
from .schema import initialize_schema

__all__ = ["create_de_xuat_blueprint", "initialize_schema"]
