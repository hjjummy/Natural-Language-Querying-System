from __future__ import annotations
import os
import shutil
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from datetime import datetime

@dataclass
class Paths:
    # cache
    cache_dir: Path
    cache_md_path: Path
    cache_schema_path: Path
    # session
    session_root: Path
    session_dir: Path
    md_path: Path           # 세션용 MD "파일" 경로
    schema_path: Path       # 세션용 schema.json "파일" 경로

class SessionManager:
    def __init__(self, workspace_root: str = "./workspace"):
        self.workspace_root = Path(workspace_root)
        self.cache_root = self.workspace_root / "cache"
        self.sessions_root = self.workspace_root / "sessions"
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.sessions_root.mkdir(parents=True, exist_ok=True)

    # ---------- 내부 유틸 ----------
    def _make_cache_key(self, input_path: str, sheet_name: Optional[str]) -> str:
        p = Path(input_path).resolve()
        h = hashlib.sha256()
        h.update(str(p).encode("utf-8"))
        h.update(b"||")
        h.update((sheet_name or "").encode("utf-8"))
        return h.hexdigest()[:12]

    def _safe_sheet(self, sheet_name: Optional[str]) -> str:
        if not sheet_name:
            return "CSV"
        s = sheet_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
        return s or "SHEET"

    def _cache_file_names(self, input_path: str, sheet_name: Optional[str]) -> tuple[Path, Path, Path]:
        key = self._make_cache_key(input_path, sheet_name)
        date_str = datetime.now().strftime("%Y%m%d")   # ✅ 날짜 추가
        cache_dir = self.cache_root / f"{date_str}__{key}"   # ✅ 날짜__해시 형태로 폴더 생성
        cache_dir.mkdir(parents=True, exist_ok=True)

        stem = Path(input_path).stem
        safe_sheet = self._safe_sheet(sheet_name)
        cache_md = cache_dir / f"{stem}__{safe_sheet}.md"
        cache_schema = cache_dir / "schema.json"
        return cache_dir, cache_md, cache_schema

    def _session_paths(self, thread_id: str, input_path: str, sheet_name: Optional[str]) -> tuple[Path, Path, Path]:
        session_dir = self.sessions_root / thread_id
        session_dir.mkdir(parents=True, exist_ok=True)

        stem = Path(input_path).stem
        safe_sheet = self._safe_sheet(sheet_name)
        md_path = session_dir / f"{stem}__{safe_sheet}.md"   # ✅ 파일 경로
        schema_path = session_dir / "schema.json"            # ✅ 파일 경로
        return self.sessions_root, session_dir, md_path, schema_path

    # ---------- 공개 API ----------
    def prepare_workspace(self, thread_id: str, input_path: str, sheet_name: Optional[str]) -> Paths:
        cache_dir, cache_md_path, cache_schema_path = self._cache_file_names(input_path, sheet_name)
        sessions_root, session_dir, md_path, schema_path = self._session_paths(thread_id, input_path, sheet_name)

        return Paths(
            cache_dir=cache_dir,
            cache_md_path=cache_md_path,
            cache_schema_path=cache_schema_path,
            session_root=sessions_root,
            session_dir=session_dir,
            md_path=md_path,
            schema_path=schema_path,
        )

    def ensure_link_or_copy(self, src: Path, dst: Path):
        src = Path(src); dst = Path(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not src.exists():
            raise FileNotFoundError(f"source not found for linking: {src}")

        # 기존 것 제거
        try:
            if dst.is_symlink() or dst.exists():
                dst.unlink()
        except Exception:
            pass

        # ✅ 임시: 항상 복사로 고정 (문제 해결 후 symlink/hardlink 재도입)
        shutil.copy2(src, dst)

        # 사후 검증
        assert dst.exists() and dst.stat().st_size > 0, f"copy failed: {dst}"

    def remove_session(self, thread_id: str):
        """특정 스레드 세션 전체 삭제"""
        session_dir = self.sessions_root / thread_id
        try:
            if session_dir.exists():
                shutil.rmtree(session_dir)
        except Exception:
            pass

    def remove_all_sessions(self):
        """모든 세션 삭제 (앱 부팅 1회 시 호출)"""
        try:
            if self.sessions_root.exists():
                shutil.rmtree(self.sessions_root)
            self.sessions_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
