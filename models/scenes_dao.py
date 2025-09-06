import sqlite3
from typing import Optional, List, Any, Tuple
from datetime import datetime


class Scenes:
    """
    封装 scenes 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 title: Optional[str] = None,
                 details: Optional[str] = None,
                 date: Optional[str] = None,
                 rating: Optional[int] = None,
                 studio_id: Optional[int] = None,
                 organized: bool = False,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 code: Optional[str] = None,
                 director: Optional[str] = None,
                 resume_time: float = 0.0,
                 play_duration: float = 0.0,
                 cover_blob: Optional[str] = None):
        self.id = id
        self.title = title
        self.details = details
        self.date = date
        self.rating = rating
        self.studio_id = studio_id
        self.organized = organized
        self.created_at = created_at
        self.updated_at = updated_at
        self.code = code
        self.director = director
        self.resume_time = resume_time
        self.play_duration = play_duration
        self.cover_blob = cover_blob

    def __repr__(self) -> str:
        return f"Scenes(id={self.id}, title='{self.title}', code='{self.code}')"


class ScenesDAO:
    """
    用于操作 scenes 表的 DAO 类。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self):
        self._conn = sqlite3.connect(self.db_path)
        self._cursor = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.commit()
            self._conn.close()

    def _execute(self, query: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """
        内部方法，用于执行 SQL 查询。
        """
        if not self._cursor:
            raise RuntimeError("Database connection not open. Use with a 'with' statement.")
        return self._cursor.execute(query, params)

    def _row_to_scenes(self, row: Tuple[Any, ...]) -> Scenes:
        """
        将数据库行数据转换为 Scenes 对象。
        """
        if not row:
            return None

        # 匹配数据库列的顺序
        return Scenes(
            id=row[0],
            title=row[1],
            details=row[2],
            date=row[3],
            rating=row[4],
            studio_id=row[5],
            organized=bool(row[6]),
            created_at=row[7],
            updated_at=row[8],
            code=row[9],
            director=row[10],
            resume_time=row[11],
            play_duration=row[12],
            cover_blob=row[13]
        )

    def create(self, scenes: Scenes) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO scenes (
            title, details, date, rating, studio_id, organized, created_at, updated_at,
            code, director, resume_time, play_duration, cover_blob
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            scenes.title, scenes.details, scenes.date, scenes.rating, scenes.studio_id,
            scenes.organized, now, now, scenes.code, scenes.director,
            scenes.resume_time, scenes.play_duration, scenes.cover_blob
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, scenes_id: int) -> Optional[Scenes]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM scenes WHERE id = ?"
        self._execute(query, (scenes_id,))
        row = self._cursor.fetchone()
        return self._row_to_scenes(row) if row else None

    def get_all(self) -> List[Scenes]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM scenes"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_scenes(row) for row in rows]

    def update(self, scenes: Scenes) -> int:
        """
        更新一条现有记录。
        """
        if scenes.id is None:
            raise ValueError("Scenes ID is required for update.")

        now = datetime.now().isoformat()
        query = """
        UPDATE scenes SET
            title = ?, details = ?, date = ?, rating = ?, studio_id = ?, organized = ?,
            updated_at = ?, code = ?, director = ?, resume_time = ?, play_duration = ?,
            cover_blob = ?
        WHERE id = ?
        """
        params = (
            scenes.title, scenes.details, scenes.date, scenes.rating, scenes.studio_id,
            scenes.organized, now, scenes.code, scenes.director, scenes.resume_time,
            scenes.play_duration, scenes.cover_blob, scenes.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, scenes_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM scenes WHERE id = ?"
        self._execute(query, (scenes_id,))
        return self._cursor.rowcount