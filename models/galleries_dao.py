import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Galleries:
    """
    封装 galleries 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 folder_id: Optional[int] = None,
                 title: Optional[str] = None,
                 date: Optional[str] = None,
                 details: Optional[str] = None,
                 studio_id: Optional[int] = None,
                 rating: Optional[int] = None,
                 organized: bool = False,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 code: Optional[str] = None,
                 photographer: Optional[str] = None):
        self.id = id
        self.folder_id = folder_id
        self.title = title
        self.date = date
        self.details = details
        self.studio_id = studio_id
        self.rating = rating
        self.organized = organized
        self.created_at = created_at
        self.updated_at = updated_at
        self.code = code
        self.photographer = photographer

    def __repr__(self) -> str:
        return f"Galleries(id={self.id}, title='{self.title}')"


class GalleriesDAO:
    """
    用于操作 galleries 表的 DAO 类。
    """

    def __init__(self, db_conn: sqlite3.Connection):
        """
        初始化 DAO。
        :param db_conn: SQLite 数据库链接。
        """
        self._conn: Optional[sqlite3.Connection] = db_conn
        self._cursor: Optional[sqlite3.Cursor] = db_conn.cursor()

    def _execute(self, query: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """
        内部方法，用于执行 SQL 查询。
        """
        if not self._cursor:
            raise RuntimeError("Database connection not open.")
        return self._cursor.execute(query, params)

    def _row_to_galleries(self, row: Tuple[Any, ...]) -> Optional[Galleries]:
        """
        将数据库行数据转换为 Galleries 对象。
        """
        if not row:
            return None

        # 匹配数据库列的顺序: id, folder_id, title, date, details, studio_id, rating,
        # organized, created_at, updated_at, code, photographer
        return Galleries(
            id=row[0],
            folder_id=row[1],
            title=row[2],
            date=row[3],
            details=row[4],
            studio_id=row[5],
            rating=row[6],
            organized=bool(row[7]),
            created_at=row[8],
            updated_at=row[9],
            code=row[10],
            photographer=row[11]
        )

    def create(self, gallery: Galleries) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO galleries (
            folder_id, title, date, details, studio_id, rating, organized,
            created_at, updated_at, code, photographer
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            gallery.folder_id, gallery.title, gallery.date, gallery.details,
            gallery.studio_id, gallery.rating, gallery.organized,
            now, now, gallery.code, gallery.photographer
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, gallery_id: int) -> Optional[Galleries]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM galleries WHERE id = ?"
        self._execute(query, (gallery_id,))
        row = self._cursor.fetchone()
        return self._row_to_galleries(row) if row else None

    def get_by_folder_id(self, folder_id: int) -> List[Galleries]:
        """
        根据 Folder ID 查询单条记录。
        """
        query = "SELECT * FROM galleries WHERE folder_id = ?"
        self._execute(query, (folder_id,))
        rows = self._cursor.fetchall()
        return [self._row_to_galleries(row) for row in rows]

    def get_all(self) -> List[Galleries]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM galleries"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_galleries(row) for row in rows]

    def update(self, gallery: Galleries) -> int:
        """
        更新一条现有记录。
        """
        if gallery.id is None:
            raise ValueError("Gallery ID is required for update.")

        now = datetime.now().isoformat()
        query = """
        UPDATE galleries SET
            folder_id = ?, title = ?, date = ?, details = ?, studio_id = ?,
            rating = ?, organized = ?, updated_at = ?, code = ?, photographer = ?
        WHERE id = ?
        """
        params = (
            gallery.folder_id, gallery.title, gallery.date, gallery.details,
            gallery.studio_id, gallery.rating, gallery.organized,
            now, gallery.code, gallery.photographer, gallery.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, gallery_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM galleries WHERE id = ?"
        self._execute(query, (gallery_id,))
        return self._cursor.rowcount

    def commit(self) -> bool:
        if self._conn:
            self._conn.commit()
            return True
        else:
            return False
