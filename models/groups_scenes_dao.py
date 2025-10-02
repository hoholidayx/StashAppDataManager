## GroupsScenesDAO 类
import sqlite3
from typing import Optional, Tuple, Any, List


class GroupsScenes:
    """
    封装 groups_scenes 关联表数据的数据类。
    """

    def __init__(self, scene_id: Optional[int] = None, group_id: Optional[int] = None):
        """
        初始化 ScenesTags 数据对象。
        :param scene_id: 场景的 ID。
        :param tag_id: 标签的 ID。
        """
        self.scene_id = scene_id
        self.group_id = group_id

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"GroupsScenes(scene_id={self.scene_id}, group_id={self.group_id})"


class GroupsScenesDAO:
    """
    用于操作 groups_scenes 联结表的 DAO 类。
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
            raise RuntimeError("Database connection not open. Use with a 'with' statement.")
        return self._cursor.execute(query, params)

    def add_scene_to_group(self, group_id: int, scene_id: int, scene_index: int) -> int:
        """
        将一个 scene 添加到一个 group 中。
        :return: 影响的行数 (1 或 0)
        """
        query = """
        INSERT OR REPLACE INTO groups_scenes (group_id, scene_id, scene_index)
        VALUES (?, ?, ?)
        """
        params = (group_id, scene_id, scene_index)
        self._execute(query, params)
        return self._cursor.rowcount

    def insert(self, scene_id: int, group_id: int, ) -> int:
        """
        将一个 scene 添加到一个 group 中。
        :return: 影响的行数 (1 或 0)
        """
        query = """
           INSERT OR REPLACE INTO groups_scenes (group_id, scene_id, scene_index)
           VALUES (?, ?, ?)
           """
        params = (group_id, scene_id, None)
        self._execute(query, params)
        return self._cursor.rowcount

    def remove_scene_from_group(self, group_id: int, scene_id: int) -> int:
        """
        从一个 group 中移除一个 scene 关联。
        :return: 影响的行数
        """
        query = "DELETE FROM groups_scenes WHERE group_id = ? AND scene_id = ?"
        params = (group_id, scene_id)
        self._execute(query, params)
        return self._cursor.rowcount

    def get_scenes_for_group(self, group_id: int) -> List[Tuple[int, int]]:
        """
        查询某个 group 下的所有 scene ID 和索引。
        :return: [(scene_id, scene_index), ...] 列表
        """
        query = "SELECT scene_id, scene_index FROM groups_scenes WHERE group_id = ? ORDER BY scene_index"
        self._execute(query, (group_id,))
        return self._cursor.fetchall()

    def get_by_ids(self, scene_id: int, group_id: int) -> Optional[GroupsScenes]:
        """
            根据 scene_id 和 tag_id 联合主键查询单条记录。
        """
        try:
            query = "SELECT * FROM groups_scenes WHERE scene_id = ? AND group_id = ?"
            self._execute(query, (scene_id, group_id))
            row = self._cursor.fetchone()
            return self._row_to_groups_scenes(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def commit(self) -> bool:
        """提交当前连接上的事务。"""
        if self._conn:
            self._conn.commit()
            return True
        return False

    def _row_to_groups_scenes(self, row: Tuple[Any, ...]) -> Optional[GroupsScenes]:
        """
        将数据库行数据转换为 GroupsScenes 对象。
        """
        if not row:
            return None
        return GroupsScenes(scene_id=row[0], group_id=row[1])
