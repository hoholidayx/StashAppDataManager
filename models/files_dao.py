import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Files:
    """
    封装 files 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 basename: Optional[str] = None,
                 zip_file_id: Optional[int] = None,
                 parent_folder_id: Optional[int] = None,
                 size: Optional[int] = None,
                 mod_time: Optional[str] = None,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None):
        self.id = id
        self.basename = basename
        self.zip_file_id = zip_file_id
        self.parent_folder_id = parent_folder_id
        self.size = size
        self.mod_time = mod_time
        self.created_at = created_at
        self.updated_at = updated_at

    def __repr__(self) -> str:
        return f"Files(id={self.id}, basename='{self.basename}', size={self.size})"


class FilesDAO:
    """
    用于操作 files 表的 DAO 类。
    """

    def __init__(self, db_conn: sqlite3.Connection):
        """
        初始化 DAO，但不立即连接数据库。
        :param db_conn: SQLite 数据库链接。
        """
        # self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = db_conn
        self._cursor: Optional[sqlite3.Cursor] = db_conn.cursor()

    def _execute(self, query: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """
        内部方法，用于执行 SQL 查询。
        """
        if not self._cursor:
            raise RuntimeError("数据库连接未打开。请配合 'with' 语句使用。")
        return self._cursor.execute(query, params)

    def _row_to_files(self, row: Tuple[Any, ...]) -> Files:
        """
        将数据库行数据转换为 Files 对象。
        """
        if not row:
            return None

        # 匹配数据库列的顺序
        return Files(
            id=row[0],
            basename=row[1],
            zip_file_id=row[2],
            parent_folder_id=row[3],
            size=row[4],
            mod_time=row[5],
            created_at=row[6],
            updated_at=row[7]
        )

    def create_tables(self):
        """
        如果表不存在，则创建 'folders' 和 'files' 表。
        'folders' 表是必需的，以满足外键约束。
        """
        self._execute("""
            CREATE TABLE IF NOT EXISTS `folders` (
                `id` integer NOT NULL primary key autoincrement,
                `name` varchar(255) NOT NULL
            );
        """)
        self._execute("""
            CREATE TABLE IF NOT EXISTS `files` (
                `id` integer NOT NULL primary key autoincrement,
                `basename` varchar(255) NOT NULL,
                `zip_file_id` integer,
                `parent_folder_id` integer NOT NULL,
                `size` integer NOT NULL,
                `mod_time` datetime NOT NULL,
                `created_at` datetime NOT NULL,
                `updated_at` datetime NOT NULL,
                foreign key(`parent_folder_id`) references `folders`(`id`),
                foreign key(`zip_file_id`) references `files`(`id`),
                CHECK (`basename` != '')
            );
        """)

    def insert(self, file_obj: Files) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO files (
            basename, zip_file_id, parent_folder_id, size, mod_time, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            file_obj.basename, file_obj.zip_file_id, file_obj.parent_folder_id,
            file_obj.size, now, now, now
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, file_id: int) -> Optional[Files]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM files WHERE id = ?"
        self._execute(query, (file_id,))
        row = self._cursor.fetchone()
        return self._row_to_files(row) if row else None

    def get_by_movie_code(self, movie_code: str) -> List[Files]:
        """
        根据电影代码（movie_code）查询所有包含该代码的记录。
        """
        # 使用 LIKE 和通配符 (%) 来查找包含指定字符串的记录
        query = "SELECT * FROM files WHERE basename LIKE ?"
        self._execute(query, (f"%[{movie_code}]%",))
        rows = self._cursor.fetchall()
        return [self._row_to_files(row) for row in rows]

    def get_by_basename(self, basename: str) -> Optional[Files]:
        """
        根据文件称，查询basename等于该名称的记录
        """
        # 使用 LIKE 和通配符 (%) 来查找包含指定字符串的记录
        query = "SELECT * FROM files WHERE NORMALIZE(files.basename) is NORMALIZE(?)"
        self._execute(query, (basename,))
        row = self._cursor.fetchone()
        return self._row_to_files(row) if row else None

    def get_all(self) -> List[Files]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM files"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_files(row) for row in rows]

    def update(self, file_obj: Files) -> int:
        """
        更新一条现有记录。
        """
        if file_obj.id is None:
            raise ValueError("Files ID 对于更新是必需的。")

        now = datetime.now().isoformat()
        query = """
        UPDATE files SET
            basename = ?, zip_file_id = ?, parent_folder_id = ?, size = ?,
            mod_time = ?, updated_at = ?
        WHERE id = ?
        """
        params = (
            file_obj.basename, file_obj.zip_file_id, file_obj.parent_folder_id,
            file_obj.size, file_obj.mod_time, now, file_obj.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, file_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM files WHERE id = ?"
        self._execute(query, (file_id,))
        return self._cursor.rowcount
