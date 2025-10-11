import sqlite3
from typing import Optional, List, Any, Tuple


class PerformersScenes:
    """
    封装 performers_scenes 表数据的数据类。
    """

    def __init__(self,
                 performer_id: Optional[int] = None,
                 scene_id: Optional[int] = None):
        """
        初始化 PerformersScenes 数据对象。
        """
        self.performer_id = performer_id
        self.scene_id = scene_id

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"PerformersScenes(performer_id={self.performer_id}, scene_id={self.scene_id})"


class PerformersScenesDAO:
    """
    用于操作 performers_scenes 关联表的 DAO 类。
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

    def _row_to_performers_scenes(self, row: Tuple[Any, ...]) -> Optional[PerformersScenes]:
        """
        将数据库行数据转换为 PerformersScenes 对象。
        """
        if not row:
            return None
        return PerformersScenes(
            performer_id=row[0],
            scene_id=row[1]
        )

    def create_table(self):
        """
        创建 performers_scenes 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS "performers_scenes" (
                    `performer_id` integer,
                    `scene_id` integer,
                    foreign key(`performer_id`) references `performers`(`id`) on delete CASCADE,
                    foreign key(`scene_id`) references `scenes`(`id`) on delete CASCADE,
                    PRIMARY KEY (`scene_id`, `performer_id`)
                )
            """
            self._execute(query)
            print("performers_scenes 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, performer_id: int, scene_id: int) -> int:
        """
        向 performers_scenes 关联表插入一条新记录。
        :param performer_id: 表演者 ID。
        :param scene_id: 场景 ID。
        :return: 插入的行数，如果插入失败则返回 0。
        """
        try:
            query = """
                INSERT OR IGNORE INTO performers_scenes (performer_id, scene_id) VALUES (?, ?)
            """
            self._execute(query, (performer_id, scene_id))
            row_count = self._cursor.rowcount
            print(f"成功插入 {row_count} 条记录: (performer_id={performer_id}, scene_id={scene_id})")
            return row_count
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return 0

    def get_by_performer_id(self, performer_id: int) -> List[PerformersScenes]:
        """
        根据 performer_id 查询所有关联记录。
        """
        try:
            query = "SELECT * FROM performers_scenes WHERE performer_id = ?"
            self._execute(query, (performer_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_performers_scenes(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return []

    def get_by_scene_id(self, scene_id: int) -> List[PerformersScenes]:
        """
        根据 scene_id 查询所有关联记录。
        """
        try:
            query = "SELECT * FROM performers_scenes WHERE scene_id = ?"
            self._execute(query, (scene_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_performers_scenes(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return []

    def get_by_ids(self, scene_id: int, performer_id: int) -> Optional[PerformersScenes]:
        """
        根据 scene_id 和 performer_id 联合主键查询单条记录。
        """
        try:
            query = "SELECT * FROM performers_scenes WHERE scene_id = ? AND performer_id = ?"
            self._execute(query, (scene_id, performer_id))
            row = self._cursor.fetchone()
            return self._row_to_performers_scenes(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def delete(self, performer_id: int, scene_id: int) -> int:
        """
        根据 performer_id 和 scene_id 删除一条关联记录。
        """
        try:
            query = "DELETE FROM performers_scenes WHERE performer_id = ? AND scene_id = ?"
            self._execute(query, (performer_id, scene_id))
            row_count = self._cursor.rowcount
            print(f"成功删除 {row_count} 条记录: (performer_id={performer_id}, scene_id={scene_id})")
            return row_count
        except sqlite3.Error as e:
            print(f"删除时出错: {e}")
            return 0
