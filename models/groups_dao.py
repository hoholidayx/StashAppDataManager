import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


## Groups 数据类
class Groups:
    """
    封装 groups 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 name: Optional[str] = None,
                 aliases: Optional[str] = None,
                 duration: Optional[int] = None,
                 date: Optional[str] = None,  # 使用 str 存储 date 类型
                 rating: Optional[int] = None,
                 studio_id: Optional[int] = None,
                 director: Optional[str] = None,
                 description: Optional[str] = None,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 front_image_blob: Optional[str] = None,
                 back_image_blob: Optional[str] = None):
        self.id = id
        self.name = name
        self.aliases = aliases
        self.duration = duration
        self.date = date
        self.rating = rating
        self.studio_id = studio_id
        self.director = director
        self.description = description
        self.created_at = created_at
        self.updated_at = updated_at
        self.front_image_blob = front_image_blob
        self.back_image_blob = back_image_blob

    def __repr__(self) -> str:
        return f"Groups(id={self.id}, name='{self.name}', date='{self.date}')"


# ---

## GroupsDAO 类
class GroupsDAO:
    """
    用于操作 groups 表的 DAO 类。
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

    def _row_to_groups(self, row: Tuple[Any, ...]) -> Optional[Groups]:
        """
        将数据库行数据转换为 Groups 对象。
        """
        if not row:
            return None

        # 匹配 groups 表列的顺序
        return Groups(
            id=row[0],
            name=row[1],
            aliases=row[2],
            duration=row[3],
            date=row[4],
            rating=row[5],
            studio_id=row[6],
            director=row[7],
            description=row[8],
            created_at=row[9],
            updated_at=row[10],
            front_image_blob=row[11],
            back_image_blob=row[12]
        )

    def insert(self, group: Groups) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO groups (
            name, aliases, duration, date, rating, studio_id, director, description,
            created_at, updated_at, front_image_blob, back_image_blob
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            group.name, group.aliases, group.duration, group.date, group.rating,
            group.studio_id, group.director, group.description, now, now,
            group.front_image_blob, group.back_image_blob
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, group_id: int) -> Optional[Groups]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM groups WHERE id = ?"
        self._execute(query, (group_id,))
        row = self._cursor.fetchone()
        return self._row_to_groups(row) if row else None

    def get_by_name(self, group_name: str) -> Optional[Groups]:
        """
        根据 name 查询单条记录。
        """
        query = "SELECT * FROM groups WHERE name = ?"
        self._execute(query, (group_name,))
        row = self._cursor.fetchone()
        return self._row_to_groups(row) if row else None

    def get_all(self) -> List[Groups]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM groups"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_groups(row) for row in rows]

    def update(self, group: Groups) -> int:
        """
        更新一条现有记录。
        """
        if group.id is None:
            raise ValueError("Groups ID is required for update.")

        now = datetime.now().isoformat()
        query = """
        UPDATE groups SET
            name = ?, aliases = ?, duration = ?, date = ?, rating = ?, studio_id = ?,
            director = ?, description = ?, updated_at = ?, front_image_blob = ?,
            back_image_blob = ?
        WHERE id = ?
        """
        params = (
            group.name, group.aliases, group.duration, group.date, group.rating,
            group.studio_id, group.director, group.description, now,
            group.front_image_blob, group.back_image_blob, group.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, group_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM groups WHERE id = ?"
        self._execute(query, (group_id,))
        return self._cursor.rowcount

    def commit(self) -> bool:
        """提交当前连接上的事务。"""
        if self._conn:
            self._conn.commit()
            return True
        return False
