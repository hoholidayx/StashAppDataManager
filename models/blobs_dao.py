import sqlite3
from typing import Optional, List, Any, Tuple


class Blobs:
    """
    封装 blobs 表数据的数据类。
    """

    def __init__(self, checksum: Optional[str] = None, blob: Optional[bytes] = None):
        """
        初始化 Blobs 数据对象。
        :param checksum: blob 的唯一校验和。
        :param blob: 二进制数据。
        """
        self.checksum = checksum
        self.blob = blob

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        blob_size = len(self.blob) if self.blob else 0
        return f"Blobs(checksum='{self.checksum}', blob_size={blob_size})"


class BlobsDAO:
    """
    用于操作 blobs 表的 DAO 类。
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

    def _row_to_blobs(self, row: Tuple[Any, ...]) -> Optional[Blobs]:
        """
        将数据库行数据转换为 Blobs 对象。
        """
        if not row:
            return None
        return Blobs(checksum=row[0], blob=row[1])

    def create_table(self):
        """
        创建 blobs 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS blobs (
                    checksum varchar(255) NOT NULL PRIMARY KEY,
                    blob blob
                )
            """
            self._execute(query)
            print("blobs 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, blob_obj: Blobs) -> bool:
        """
        向 blobs 表插入一个新记录。
        :param blob_obj: 包含校验和和二进制数据的 Blobs 对象。
        :return: 如果插入成功，返回 True；否则返回 False。
        """
        try:
            query = "INSERT INTO blobs (checksum, blob) VALUES (?, ?)"
            self._execute(query, (blob_obj.checksum, blob_obj.blob))
            print(f"成功插入 blob，校验和: {blob_obj.checksum}")
            return True
        except sqlite3.IntegrityError:
            print(f"插入失败: 校验和 '{blob_obj.checksum}' 已存在。")
            return False
        except sqlite3.Error as e:
            print(f"插入时出错: {e}")
            return False

    def get_by_checksum(self, checksum: str) -> Optional[Blobs]:
        """
        根据校验和检索 blob。
        :param checksum: 要检索的 blob 的校验和。
        :return: 如果找到，返回 Blobs 对象；否则返回 None。
        """
        try:
            query = "SELECT checksum, blob FROM blobs WHERE checksum = ?"
            self._execute(query, (checksum,))
            row = self._cursor.fetchone()
            if row:
                print(f"成功检索到 blob，校验和: {checksum}")
            else:
                print(f"未找到校验和为 '{checksum}' 的 blob。")
            return self._row_to_blobs(row)
        except sqlite3.Error as e:
            print(f"检索时出错: {e}")
            return None

    def get_all(self) -> List[Blobs]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM blobs"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_blobs(row) for row in rows]

    def update_blob(self, blob_obj: Blobs) -> int:
        """
        更新一条现有记录的 blob 数据。
        :param blob_obj: 包含要更新的数据的 Blobs 对象。
        :return: 更新的行数。
        """
        if not blob_obj.checksum:
            raise ValueError("Checksum is required for update.")
        query = "UPDATE blobs SET blob = ? WHERE checksum = ?"
        self._execute(query, (blob_obj.blob, blob_obj.checksum))
        row_count = self._cursor.rowcount
        print(f"成功更新 {row_count} 条记录，校验和: {blob_obj.checksum}")
        return row_count

    def delete_by_checksum(self, checksum: str) -> int:
        """
        根据校验和删除一条记录。
        :param checksum: 要删除的 blob 的校验和。
        :return: 删除的行数。
        """
        query = "DELETE FROM blobs WHERE checksum = ?"
        self._execute(query, (checksum,))
        row_count = self._cursor.rowcount
        print(f"成功删除 {row_count} 条记录，校验和: {checksum}")
        return row_count
