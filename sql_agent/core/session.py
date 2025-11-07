# core/session.py
from __future__ import annotations
import os
import shutil
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from .config import DB_PATH as DEFAULT_DB_PATH
from .sql_executor import introspect_table  # DuckDB → 컬럼 메타 추출

@dataclass
class Paths:
    # cache
    cache_dir: Path
    cache_schema_path: Path      # DuckDB 인트로스펙션 캐시(JSON)
    # session
    session_root: Path
    session_dir: Path
    schema_path: Path            # 세션용 schema.json (LLM 주입용)
    # db
    db_path: Path                # DuckDB 파일 경로
    table: str                   # 주 테이블명

class SessionManager:
    def __init__(self, workspace_root: str = "./workspace"):
        self.workspace_root = Path(workspace_root)
        self.cache_root = self.workspace_root / "cache"
        self.sessions_root = self.workspace_root / "sessions"
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.sessions_root.mkdir(parents=True, exist_ok=True)

    # ---------- 내부 유틸 ----------
    def _make_cache_key(self, db_path: str | os.PathLike, table: Optional[str]) -> str:
        """
        DB 파일 경로 + 테이블명을 해시하여 캐시 키 생성.
        """
        p = Path(db_path).resolve()
        h = hashlib.sha256()
        h.update(str(p).encode("utf-8"))
        h.update(b"||")
        h.update((table or "").encode("utf-8"))
        return h.hexdigest()[:12]

    def _cache_file_names(self, db_path: str | os.PathLike, table: Optional[str]) -> Tuple[Path, Path]:
        key = self._make_cache_key(db_path, table)
        cache_dir = self.cache_root / key
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_schema = cache_dir / "schema.json"
        return cache_dir, cache_schema

    def _session_paths(self, thread_id: str) -> Tuple[Path, Path, Path]:
        session_dir = self.sessions_root / thread_id
        session_dir.mkdir(parents=True, exist_ok=True)
        schema_path = session_dir / "schema.json"
        return self.sessions_root, session_dir, schema_path

    # ---------- 공개 API ----------
    def prepare_workspace(
        self,
        thread_id: str,
        db_path: str | os.PathLike = DEFAULT_DB_PATH,
        table: str = "fact_manufacturing",
    ) -> Paths:
        """
        - DuckDB 파일 존재 검증
        - (캐시 없을 시) PRAGMA table_info 로 스키마 인트로스펙션 → cache/schema.json 저장
        - 세션 폴더에 schema.json 복사
        """
        db_path = Path(db_path)
        if not db_path.exists():
            raise FileNotFoundError(f"DuckDB 파일이 존재하지 않습니다: {db_path}")

        cache_dir, cache_schema_path = self._cache_file_names(db_path, table)
        sessions_root, session_dir, schema_path = self._session_paths(thread_id)

        # 1) 캐시 스키마 생성(최초 1회)
        if not cache_schema_path.exists():
            schema = introspect_table(table=table, db_path=str(db_path))
            cache_schema_path.write_text(
                __import__("json").dumps(schema, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        # 2) 세션 폴더에 복사(항상 최신으로 교체)
        self.ensure_link_or_copy(cache_schema_path, schema_path)

        return Paths(
            cache_dir=cache_dir,
            cache_schema_path=cache_schema_path,
            session_root=sessions_root,
            session_dir=session_dir,
            schema_path=schema_path,
            db_path=db_path,
            table=table,
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

        # OS/권한 이슈 회피: 항상 복사
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
