import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Tags:
    """
    封装 tags 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 name: Optional[str] = None,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 ignore_auto_tag: bool = False,
                 description: Optional[str] = None,
                 image_blob: Optional[str] = None,
                 favorite: bool = False,
                 sort_name: Optional[str] = None):
        """
        初始化 Tags 数据对象。
        """
        self.id = id
        self.name = name
        self.created_at = created_at
        self.updated_at = updated_at
        self.ignore_auto_tag = ignore_auto_tag
        self.description = description
        self.image_blob = image_blob
        self.favorite = favorite
        self.sort_name = sort_name

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"Tags(id={self.id}, name='{self.name}', favorite={self.favorite})"


class TagsDAO:
    """
    用于操作 tags 表的 DAO 类。
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

    def _row_to_tags(self, row: Tuple[Any, ...]) -> Optional[Tags]:
        """
        将数据库行数据转换为 Tags 对象。
        """
        if not row:
            return None
        return Tags(
            id=row[0],
            name=row[1],
            created_at=row[2],
            updated_at=row[3],
            ignore_auto_tag=bool(row[4]),
            description=row[5],
            image_blob=row[6],
            favorite=bool(row[7]),
            sort_name=row[8]
        )

    def create_table(self):
        """
        创建 tags 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS tags (
                    id integer not null primary key autoincrement,
                    name varchar(255) not null,
                    created_at datetime not null,
                    updated_at datetime not null,
                    ignore_auto_tag boolean not null default '0',
                    description text,
                    image_blob varchar(255),
                    favorite boolean not null default '0',
                    sort_name varchar(255)
                )
            """
            self._execute(query)
            print("tags 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, tag_obj: Tags) -> Optional[int]:
        """
        向 tags 表插入一个新记录。
        :param tag_obj: 包含标签数据的 Tags 对象。
        :return: 新记录的 ID，如果插入失败则返回 None。
        """
        now = datetime.now().isoformat()
        try:
            query = """
                INSERT INTO tags (
                    name, created_at, updated_at, ignore_auto_tag,
                    description, image_blob, favorite, sort_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                tag_obj.name, now, now, tag_obj.ignore_auto_tag,
                tag_obj.description, tag_obj.image_blob, tag_obj.favorite,
                tag_obj.sort_name
            )
            self._execute(query, params)
            print(f"成功插入标签: {tag_obj.name}")
            return self._cursor.lastrowid
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return None

    def get_by_id(self, tag_id: int) -> Optional[Tags]:
        """
        根据 ID 查询单条记录。
        """
        try:
            query = "SELECT * FROM tags WHERE id = ?"
            self._execute(query, (tag_id,))
            row = self._cursor.fetchone()
            return self._row_to_tags(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def get_all(self) -> List[Tags]:
        """
        查询所有记录。
        """
        try:
            query = "SELECT * FROM tags"
            self._execute(query)
            rows = self._cursor.fetchall()
            return [self._row_to_tags(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索所有记录时出错: {e}")
            return []

    def update(self, tag_obj: Tags) -> int:
        """
        更新一条现有记录。
        :param tag_obj: 包含要更新的数据的 Tags 对象。
        :return: 更新的行数。
        """
        if tag_obj.id is None:
            raise ValueError("Tags ID is required for update.")

        now = datetime.now().isoformat()
        try:
            query = """
                UPDATE tags SET
                    name = ?, updated_at = ?, ignore_auto_tag = ?,
                    description = ?, image_blob = ?, favorite = ?,
                    sort_name = ?
                WHERE id = ?
            """
            params = (
                tag_obj.name, now, tag_obj.ignore_auto_tag,
                tag_obj.description, tag_obj.image_blob, tag_obj.favorite,
                tag_obj.sort_name, tag_obj.id
            )
            self._execute(query, params)
            row_count = self._cursor.rowcount
            print(f"成功更新 {row_count} 条记录，ID: {tag_obj.id}")
            return row_count
        except sqlite3.Error as e:
            print(f"更新时出错: {e}")
            return 0

    def delete(self, tag_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        try:
            query = "DELETE FROM tags WHERE id = ?"
            self._execute(query, (tag_id,))
            row_count = self._cursor.rowcount
            print(f"成功删除 {row_count} 条记录，ID: {tag_id}")
            return row_count
        except sqlite3.Error as e:
            print(f"删除时出错: {e}")
            return 0
