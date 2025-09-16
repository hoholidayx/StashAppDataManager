import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Studios:
    """
    封装 studios 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 name: Optional[str] = None,
                 url: Optional[str] = None,
                 parent_id: Optional[int] = None,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 details: Optional[str] = None,
                 rating: Optional[int] = None,
                 ignore_auto_tag: bool = False,
                 image_blob: Optional[str] = None,
                 favorite: bool = False):
        """
        初始化 Studios 数据对象。
        """
        self.id = id
        self.name = name
        self.url = url
        self.parent_id = parent_id
        self.created_at = created_at
        self.updated_at = updated_at
        self.details = details
        self.rating = rating
        self.ignore_auto_tag = ignore_auto_tag
        self.image_blob = image_blob
        self.favorite = favorite

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"Studios(id={self.id}, name='{self.name}', parent_id={self.parent_id})"


class StudiosDAO:
    """
    用于操作 studios 表的 DAO 类。
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
            raise RuntimeError("数据库连接未打开。请使用 'with' 语句。")
        return self._cursor.execute(query, params)

    def _row_to_studios(self, row: Tuple[Any, ...]) -> Optional[Studios]:
        """
        将数据库行数据转换为 Studios 对象。
        """
        if not row:
            return None
        return Studios(
            id=row[0],
            name=row[1],
            url=row[2],
            parent_id=row[3],
            created_at=row[4],
            updated_at=row[5],
            details=row[6],
            rating=row[7],
            ignore_auto_tag=bool(row[8]),
            image_blob=row[9],
            favorite=bool(row[10])
        )

    def create_table(self):
        """
        创建 studios 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS "studios" (
                    `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    `name` VARCHAR(255) NOT NULL,
                    `url` VARCHAR(255),
                    `parent_id` INTEGER DEFAULT NULL REFERENCES `studios`(`id`) ON DELETE SET NULL,
                    `created_at` DATETIME NOT NULL,
                    `updated_at` DATETIME NOT NULL,
                    `details` TEXT,
                    `rating` TINYINT,
                    `ignore_auto_tag` BOOLEAN NOT NULL DEFAULT FALSE,
                    `image_blob` VARCHAR(255) REFERENCES `blobs`(`checksum`),
                    `favorite` BOOLEAN NOT NULL DEFAULT FALSE
                )
            """
            self._execute(query)
            print("studios 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, studio_obj: Studios) -> Optional[int]:
        """
        向 studios 表插入一个新记录。
        :param studio_obj: 包含工作室数据的 Studios 对象。
        :return: 新记录的 ID，如果插入失败则返回 None。
        """
        now = datetime.now().isoformat()
        try:
            query = """
                INSERT INTO studios (
                    name, url, parent_id, created_at, updated_at, details,
                    rating, ignore_auto_tag, image_blob, favorite
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                studio_obj.name, studio_obj.url, studio_obj.parent_id, now, now,
                studio_obj.details, studio_obj.rating, studio_obj.ignore_auto_tag,
                studio_obj.image_blob, studio_obj.favorite
            )
            self._execute(query, params)
            print(f"成功插入工作室: {studio_obj.name}")
            return self._cursor.lastrowid
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return None

    def get_by_id(self, studio_id: int) -> Optional[Studios]:
        """
        根据 ID 查询单条记录。
        """
        try:
            query = "SELECT * FROM studios WHERE id = ?"
            self._execute(query, (studio_id,))
            row = self._cursor.fetchone()
            return self._row_to_studios(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def get_by_name(self, studio_name: str) -> Optional[Studios]:
        """
        根据 ID 查询单条记录。
        """
        try:
            query = "SELECT * FROM studios WHERE name = ?"
            self._execute(query, (studio_name,))
            row = self._cursor.fetchone()
            return self._row_to_studios(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def get_all(self) -> List[Studios]:
        """
        查询所有记录。
        """
        try:
            query = "SELECT * FROM studios"
            self._execute(query)
            rows = self._cursor.fetchall()
            return [self._row_to_studios(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索所有记录时出错: {e}")
            return []

    def update(self, studio_obj: Studios) -> int:
        """
        更新一条现有记录。
        :param studio_obj: 包含要更新的数据的 Studios 对象。
        :return: 更新的行数。
        """
        if studio_obj.id is None:
            raise ValueError("Studios ID is required for update.")

        now = datetime.now().isoformat()
        try:
            query = """
                UPDATE studios SET
                    name = ?, url = ?, parent_id = ?, updated_at = ?, details = ?,
                    rating = ?, ignore_auto_tag = ?, image_blob = ?, favorite = ?
                WHERE id = ?
            """
            params = (
                studio_obj.name, studio_obj.url, studio_obj.parent_id, now,
                studio_obj.details, studio_obj.rating, studio_obj.ignore_auto_tag,
                studio_obj.image_blob, studio_obj.favorite, studio_obj.id
            )
            self._execute(query, params)
            row_count = self._cursor.rowcount
            print(f"成功更新 {row_count} 条记录，ID: {studio_obj.id}")
            return row_count
        except sqlite3.Error as e:
            print(f"更新时出错: {e}")
            return 0

    def delete(self, studio_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        try:
            query = "DELETE FROM studios WHERE id = ?"
            self._execute(query, (studio_id,))
            row_count = self._cursor.rowcount
            print(f"成功删除 {row_count} 条记录，ID: {studio_id}")
            return row_count
        except sqlite3.Error as e:
            print(f"删除时出错: {e}")
            return 0

    def commit(selfs) -> bool:
        if selfs._conn:
            selfs._conn.commit()
            return True
        else:
            return False
