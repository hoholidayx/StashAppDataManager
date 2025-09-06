import os
import sqlite3
from typing import Optional, List, Any, Tuple


class ScenesTags:
    """
    封装 scenes_tags 关联表数据的数据类。
    """

    def __init__(self, scene_id: Optional[int] = None, tag_id: Optional[int] = None):
        """
        初始化 ScenesTags 数据对象。
        :param scene_id: 场景的 ID。
        :param tag_id: 标签的 ID。
        """
        self.scene_id = scene_id
        self.tag_id = tag_id

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"ScenesTags(scene_id={self.scene_id}, tag_id={self.tag_id})"


class ScenesTagsDAO:
    """
    用于操作 scenes_tags 关联表的 DAO 类。
    """

    def __init__(self, db_path: str):
        """
        初始化 DAO，但不立即连接数据库。
        :param db_path: SQLite 数据库文件的路径。
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self):
        """
        进入上下文，建立数据库连接。
        """
        # 确保数据库文件所在的目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._cursor = self._conn.cursor()
        print(f"成功连接到数据库: {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文，提交事务并关闭连接。
        """
        if self._conn:
            self._conn.commit()
            self._conn.close()
            print("数据库连接已关闭。")
        if exc_val:
            print(f"操作中发生错误: {exc_val}")

    def _execute(self, query: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """
        内部方法，用于执行 SQL 查询。
        """
        if not self._cursor:
            raise RuntimeError("数据库连接未打开。请使用 'with' 语句。")
        return self._cursor.execute(query, params)

    def _row_to_scenes_tags(self, row: Tuple[Any, ...]) -> Optional[ScenesTags]:
        """
        将数据库行数据转换为 ScenesTags 对象。
        """
        if not row:
            return None
        return ScenesTags(scene_id=row[0], tag_id=row[1])

    def create_table(self):
        """
        创建 scenes_tags 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS scenes_tags (
                    scene_id integer,
                    tag_id integer,
                    foreign key(`scene_id`) references `scenes`(`id`) on delete CASCADE,
                    foreign key(`tag_id`) references `tags`(`id`) on delete CASCADE,
                    PRIMARY KEY(`scene_id`, `tag_id`)
                )
            """
            self._execute(query)
            print("scenes_tags 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, scenes_tags_obj: ScenesTags) -> bool:
        """
        向 scenes_tags 表插入一条新记录。
        :param scenes_tags_obj: 包含场景 ID 和标签 ID 的 ScenesTags 对象。
        :return: 如果插入成功，返回 True；否则返回 False。
        """
        try:
            query = "INSERT INTO scenes_tags (scene_id, tag_id) VALUES (?, ?)"
            self._execute(query, (scenes_tags_obj.scene_id, scenes_tags_obj.tag_id))
            print(f"成功插入关联记录: scene_id={scenes_tags_obj.scene_id}, tag_id={scenes_tags_obj.tag_id}")
            return True
        except sqlite3.IntegrityError:
            print(f"插入失败: 关联记录已存在。")
            return False
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return False

    def get_by_ids(self, scene_id: int, tag_id: int) -> Optional[ScenesTags]:
        """
        根据 scene_id 和 tag_id 联合主键查询单条记录。
        """
        try:
            query = "SELECT * FROM scenes_tags WHERE scene_id = ? AND tag_id = ?"
            self._execute(query, (scene_id, tag_id))
            row = self._cursor.fetchone()
            return self._row_to_scenes_tags(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def get_all_by_scene_id(self, scene_id: int) -> List[ScenesTags]:
        """
        查询与给定场景 ID 相关的所有标签关联记录。
        """
        try:
            query = "SELECT * FROM scenes_tags WHERE scene_id = ?"
            self._execute(query, (scene_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_scenes_tags(row) for row in rows]
        except sqlite3.Error as e:
            print(f"根据场景 ID 检索时出错: {e}")
            return []

    def get_all_by_tag_id(self, tag_id: int) -> List[ScenesTags]:
        """
        查询与给定标签 ID 相关的所有场景关联记录。
        """
        try:
            query = "SELECT * FROM scenes_tags WHERE tag_id = ?"
            self._execute(query, (tag_id,))
            rows = self._cursor.fetchall()
            return [self._row_to_scenes_tags(row) for row in rows]
        except sqlite3.Error as e:
            print(f"根据标签 ID 检索时出错: {e}")
            return []

    def delete(self, scene_id: int, tag_id: int) -> int:
        """
        根据 scene_id 和 tag_id 删除一条记录。
        """
        try:
            query = "DELETE FROM scenes_tags WHERE scene_id = ? AND tag_id = ?"
            self._execute(query, (scene_id, tag_id))
            row_count = self._cursor.rowcount
            print(f"成功删除 {row_count} 条记录，scene_id={scene_id}, tag_id={tag_id}")
            return row_count
        except sqlite3.Error as e:
            print(f"删除时出错: {e}")
            return 0
