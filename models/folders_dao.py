import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Folders:
    """
    封装 folders 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 path: Optional[str] = None,
                 parent_folder_id: Optional[int] = None,
                 mod_time: Optional[str] = None,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 zip_file_id: Optional[int] = None):
        self.id = id
        self.path = path
        self.parent_folder_id = parent_folder_id
        self.mod_time = mod_time
        self.created_at = created_at
        self.updated_at = updated_at
        self.zip_file_id = zip_file_id

    def __repr__(self) -> str:
        return f"Folders(id={self.id}, path='{self.path}')"


class FoldersDAO:
    """
    用于操作 folders 表的 DAO 类。
    """

    def __init__(self, db_conn: sqlite3.Connection):
        """
        初始化 DAO，但不立即连接数据库。
        :param db_conn: SQLite 数据库链接。
        """
        self._conn: Optional[sqlite3.Connection] = db_conn
        self._cursor: Optional[sqlite3.Cursor] = db_conn.cursor()

    def _execute(self, query: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """
        内部方法，用于执行 SQL 查询。
        """
        if not self._cursor:
            raise RuntimeError("Database connection not open. Use with a 'with' statement.")
        return self._cursor.execute(query, params)

    def _row_to_folders(self, row: Tuple[Any, ...]) -> Optional[Folders]:
        """
        将数据库行数据转换为 Folders 对象。
        """
        if not row:
            return None

        # 匹配数据库列的顺序: id, path, parent_folder_id, mod_time, created_at, updated_at, zip_file_id
        return Folders(
            id=row[0],
            path=row[1],
            parent_folder_id=row[2],
            mod_time=row[3],
            created_at=row[4],
            updated_at=row[5],
            zip_file_id=row[6]
        )

    def create(self, folder: Folders) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO folders (
            path, parent_folder_id, mod_time, created_at, updated_at, zip_file_id
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            folder.path, folder.parent_folder_id, folder.mod_time,
            now, now, folder.zip_file_id
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, folder_id: int) -> Optional[Folders]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM folders WHERE id = ?"
        self._execute(query, (folder_id,))
        row = self._cursor.fetchone()
        return self._row_to_folders(row) if row else None

    def get_by_path_pattern(self, path_pattern: str) -> List[Folders]:
        """
        根据模板匹配，查询查询path字段符合条件的所有记录。
        """
        # 查询不区分大小写
        query = "SELECT * FROM folders WHERE upper(NORMALIZE(path)) LIKE upper(NORMALIZE(?))"
        self._execute(query, (path_pattern,))
        rows = self._cursor.fetchall()
        return [self._row_to_folders(row) for row in rows]

    def get_all(self) -> List[Folders]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM folders"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_folders(row) for row in rows]

    def update(self, folder: Folders) -> int:
        """
        更新一条现有记录。
        """
        if folder.id is None:
            raise ValueError("Folder ID is required for update.")

        now = datetime.now().isoformat()
        query = """
        UPDATE folders SET
            path = ?, parent_folder_id = ?, mod_time = ?,
            updated_at = ?, zip_file_id = ?
        WHERE id = ?
        """
        params = (
            folder.path, folder.parent_folder_id, folder.mod_time,
            now, folder.zip_file_id, folder.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, folder_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM folders WHERE id = ?"
        self._execute(query, (folder_id,))
        return self._cursor.rowcount

    def commit(self) -> bool:
        if self._conn:
            self._conn.commit()
            return True
        else:
            return False
