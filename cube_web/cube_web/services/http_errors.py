from __future__ import annotations

from typing import Any

try:
    from fastapi import HTTPException as HTTPException
except ModuleNotFoundError:
    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: Any = None) -> None:
            self.status_code = int(status_code)
            self.detail = detail
            super().__init__(str(detail) if detail is not None else "")
