import sqlite3
from typing import Optional, List, Any, Tuple
from datetime import datetime


class ScenesGalleries:
    """
    封装 scenes_galleries 表数据的数据类。
    """

    def __init__(self,
                 scene_id: Optional[int] = None,
                 gallery_id: Optional[int] = None):
        """
        初始化 ScenesGalleries 数据对象。
        """
        self.scene_id = scene_id
        self.gallery_id = gallery_id

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"ScenesGalleries(scene_id={self.scene_id}, gallery_id={self.gallery_id})"


class ScenesGalleriesDAO:
    """
    用于操作 scenes_galleries 关联表的 DAO 类。
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

    def _row_to_scenes_galleries(self, row: Tuple[Any, ...]) -> Optional[ScenesGalleries]:
        """
        将数据库行数据转换为 ScenesGalleries 对象。
        """
        if not row:
            return None
        return ScenesGalleries(
            scene_id=row[0],
            gallery_id=row[1]
        )

    def create_table(self):
        """
        创建 scenes_galleries 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS "scenes_galleries" (
                    `scene_id` INTEGER NOT NULL,
                    `gallery_id` INTEGER NOT NULL,
                    foreign key(`scene_id`) references `scenes`(`id`) on delete CASCADE,
                    foreign key(`gallery_id`) references `galleries`(`id`) on delete CASCADE,
                    PRIMARY KEY(`scene_id`, `gallery_id`)
                )
            """
            self._execute(query)
            print("scenes_galleries 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, scene_id: int, gallery_id: int) -> int:
        """
        向 scenes_galleries 关联表插入一条新记录。
        :param scene_id: 场景 ID。
        :param gallery_id: 画廊 ID。
        :return: 插入的行数，如果插入失败则返回 0。
        """
        try:
            query = """
                INSERT OR IGNORE INTO scenes_galleries (scene_id, gallery_id) VALUES (?, ?)
            """
            self._execute(query, (scene_id, gallery_id))
            row_count = self._cursor.rowcount
            print(f"成功插入 {row_count} 条记录: (scene_id={scene_id}, gallery_id={gallery_id})")
            return row_count
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return 0

    def get_by_scene_id(self, scene_id: int) -> List[ScenesGalleries]:
        """
        根据 scene_id 查询所有关联记录。
        """
        try:
            query = "SELECT * FROM scenes_galleries WHERE scene_id = ?"
            self._execute(query, (scene_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_scenes_galleries(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return []

    def get_by_gallery_id(self, gallery_id: int) -> List[ScenesGalleries]:
        """
        根据 gallery_id 查询所有关联记录。
        """
        try:
            query = "SELECT * FROM scenes_galleries WHERE gallery_id = ?"
            self._execute(query, (gallery_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_scenes_galleries(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return []

    def delete(self, scene_id: int, gallery_id: int) -> int:
        """
        根据 scene_id 和 gallery_id 删除一条关联记录。
        """
        try:
            query = "DELETE FROM scenes_galleries WHERE scene_id = ? AND gallery_id = ?"
            self._execute(query, (scene_id, gallery_id))
            row_count = self._cursor.rowcount
            print(f"成功删除 {row_count} 条记录: (scene_id={scene_id}, gallery_id={gallery_id})")
            return row_count
        except sqlite3.Error as e:
            print(f"删除时出错: {e}")
            return 0
