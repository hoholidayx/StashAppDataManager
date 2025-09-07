import sqlite3
from typing import Optional, List, Any, Tuple


class ScenesFiles:
    """
    封装 scenes_files 表数据的数据类。
    """

    def __init__(self,
                 scene_id: Optional[int] = None,
                 file_id: Optional[int] = None,
                 primary: Optional[bool] = None):
        self.scene_id = scene_id
        self.file_id = file_id
        self.primary = primary

    def __repr__(self) -> str:
        return f"ScenesFiles(scene_id={self.scene_id}, file_id={self.file_id}, primary={self.primary})"


class ScenesFilesDAO:
    """
    用于操作 scenes_files 表的 DAO 类。
    该类依赖于外部提供的数据库连接。
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
        return self._cursor.execute(query, params)

    def _row_to_scenes_files(self, row: Tuple[Any, ...]) -> ScenesFiles:
        """
        将数据库行数据转换为 ScenesFiles 对象。
        """
        if not row:
            return None
        return ScenesFiles(scene_id=row[0], file_id=row[1], primary=bool(row[2]))

    def create_table(self):
        """
        如果表不存在，则创建 `scenes_files` 表。
        """
        self._execute("""
            CREATE TABLE IF NOT EXISTS `scenes_files` (
                `scene_id` integer NOT NULL,
                `file_id` integer NOT NULL,
                `primary` boolean NOT NULL,
                foreign key(`scene_id`) references `scenes`(`id`) on delete CASCADE,
                foreign key(`file_id`) references `files`(`id`) on delete CASCADE,
                PRIMARY KEY(`scene_id`, `file_id`)
            )
        """)

    def insert(self, scenes_files_obj: ScenesFiles) -> int:
        """
        向数据库中添加一条新记录。
        由于是复合主键，成功插入后返回 1。
        """
        query = "INSERT INTO scenes_files (scene_id, file_id, `primary`) VALUES (?, ?, ?)"
        params = (scenes_files_obj.scene_id, scenes_files_obj.file_id, scenes_files_obj.primary)
        self._execute(query, params)
        return self._cursor.rowcount

    def get_by_ids(self, scene_id: int, file_id: int) -> Optional[ScenesFiles]:
        """
        根据复合主键查询单条记录。
        """
        query = "SELECT * FROM scenes_files WHERE scene_id = ? AND file_id = ?"
        self._execute(query, (scene_id, file_id))
        row = self._cursor.fetchone()
        return self._row_to_scenes_files(row)

    def get_by_file_id(self, file_id: int) -> Optional[ScenesFiles]:
        """
        查询单条记录。
        """
        query = "SELECT * FROM scenes_files WHERE file_id = ?"
        self._execute(query, (file_id,))
        # scene与文件1:1对应
        row = self._cursor.fetchone()
        return self._row_to_scenes_files(row)

    def get_all(self) -> List[ScenesFiles]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM scenes_files"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_scenes_files(row) for row in rows]

    def update(self, scenes_files_obj: ScenesFiles) -> int:
        """
        更新一条现有记录。
        """
        query = """
        UPDATE scenes_files SET
            `primary` = ?
        WHERE scene_id = ? AND file_id = ?
        """
        params = (scenes_files_obj.primary, scenes_files_obj.scene_id, scenes_files_obj.file_id)
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, scene_id: int, file_id: int) -> int:
        """
        根据复合主键删除一条记录。
        """
        query = "DELETE FROM scenes_files WHERE scene_id = ? AND file_id = ?"
        self._execute(query, (scene_id, file_id))
        return self._cursor.rowcount
