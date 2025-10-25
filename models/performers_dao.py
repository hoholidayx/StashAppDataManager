import sqlite3
from datetime import datetime
from typing import Optional, List, Any, Tuple


class Performers:
    """
    封装 performers 表数据的数据类。
    """

    def __init__(self,
                 id: Optional[int] = None,
                 name: Optional[str] = None,
                 disambiguation: Optional[str] = None,
                 gender: Optional[str] = None,
                 birthdate: Optional[str] = None,
                 ethnicity: Optional[str] = None,
                 country: Optional[str] = None,
                 eye_color: Optional[str] = None,
                 height: Optional[int] = None,
                 measurements: Optional[str] = None,
                 fake_tits: Optional[str] = None,
                 career_length: Optional[str] = None,
                 tattoos: Optional[str] = None,
                 piercings: Optional[str] = None,
                 favorite: bool = False,
                 created_at: Optional[str] = None,
                 updated_at: Optional[str] = None,
                 details: Optional[str] = None,
                 death_date: Optional[str] = None,
                 hair_color: Optional[str] = None,
                 weight: Optional[int] = None,
                 rating: Optional[int] = None,
                 ignore_auto_tag: bool = False,
                 image_blob: Optional[str] = None,
                 penis_length: Optional[float] = None,
                 circumcised: Optional[str] = None):
        """
        初始化 Performers 数据对象。
        """
        self.id = id
        self.name = name
        self.disambiguation = disambiguation
        self.gender = gender
        self.birthdate = birthdate
        self.ethnicity = ethnicity
        self.country = country
        self.eye_color = eye_color
        self.height = height
        self.measurements = measurements
        self.fake_tits = fake_tits
        self.career_length = career_length
        self.tattoos = tattoos
        self.piercings = piercings
        self.favorite = favorite
        self.created_at = created_at
        self.updated_at = updated_at
        self.details = details
        self.death_date = death_date
        self.hair_color = hair_color
        self.weight = weight
        self.rating = rating
        self.ignore_auto_tag = ignore_auto_tag
        self.image_blob = image_blob
        self.penis_length = penis_length
        self.circumcised = circumcised

    def __repr__(self) -> str:
        """
        提供一个友好的字符串表示，便于调试。
        """
        return f"Performers(id={self.id}, name='{self.name}', gender='{self.gender}')"


class PerformersDAO:
    """
    用于操作 performers 表的 DAO 类。
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

    def _row_to_performer(self, row: Tuple[Any, ...]) -> Optional[Performers]:
        """
        将数据库行数据转换为 Performers 对象。
        """
        if not row:
            return None
        return Performers(
            id=row[0],
            name=row[1],
            disambiguation=row[2],
            gender=row[3],
            birthdate=row[4],
            ethnicity=row[5],
            country=row[6],
            eye_color=row[7],
            height=row[8],
            measurements=row[9],
            fake_tits=row[10],
            career_length=row[11],
            tattoos=row[12],
            piercings=row[13],
            favorite=bool(row[14]),
            created_at=row[15],
            updated_at=row[16],
            details=row[17],
            death_date=row[18],
            hair_color=row[19],
            weight=row[20],
            rating=row[21],
            ignore_auto_tag=bool(row[22]),
            image_blob=row[23],
            penis_length=row[24],
            circumcised=row[25]
        )

    def create_table(self):
        """
        创建 performers 表，如果它尚不存在。
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS "performers" (
                    `id` integer not null primary key autoincrement,
                    `name` varchar(255) not null,
                    `disambiguation` varchar(255),
                    `gender` varchar(20),
                    `birthdate` date,
                    `ethnicity` varchar(255),
                    `country` varchar(255),
                    `eye_color` varchar(255),
                    `height` int,
                    `measurements` varchar(255),
                    `fake_tits` varchar(255),
                    `career_length` varchar(255),
                    `tattoos` varchar(255),
                    `piercings` varchar(255),
                    `favorite` boolean not null default '0',
                    `created_at` datetime not null,
                    `updated_at` datetime not null,
                    `details` text,
                    `death_date` date,
                    `hair_color` varchar(255),
                    `weight` integer,
                    `rating` tinyint,
                    `ignore_auto_tag` boolean not null default '0',
                    `image_blob` varchar(255) REFERENCES `blobs`(`checksum`),
                    `penis_length` float,
                    `circumcised` varchar[10]
                )
            """
            self._execute(query)
            print("performers 表已成功创建或已存在。")
        except sqlite3.Error as e:
            print(f"创建表时出错: {e}")

    def insert(self, performer: Performers) -> Optional[int]:
        """
        向数据库中添加一条新记录。
        """
        now = datetime.now().isoformat()
        query = """
        INSERT INTO performers (
            name, disambiguation, gender, birthdate, ethnicity, country, eye_color,
            height, measurements, fake_tits, career_length, tattoos, piercings,
            favorite, created_at, updated_at, details, death_date, hair_color,
            weight, rating, ignore_auto_tag, image_blob, penis_length, circumcised
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            performer.name, performer.disambiguation, performer.gender, performer.birthdate,
            performer.ethnicity, performer.country, performer.eye_color, performer.height,
            performer.measurements, performer.fake_tits, performer.career_length,
            performer.tattoos, performer.piercings, performer.favorite, now, now,
            performer.details, performer.death_date, performer.hair_color, performer.weight,
            performer.rating, performer.ignore_auto_tag, performer.image_blob,
            performer.penis_length, performer.circumcised
        )
        self._execute(query, params)
        return self._cursor.lastrowid

    def get_by_id(self, performer_id: int) -> Optional[Performers]:
        """
        根据 ID 查询单条记录。
        """
        query = "SELECT * FROM performers WHERE id = ?"
        self._execute(query, (performer_id,))
        row = self._cursor.fetchone()
        return self._row_to_performer(row) if row else None

    def get_by_name(self, performers_name: str) -> List[Performers]:
        """
        根据 name 查询单条记录。
        """
        try:
            query = "SELECT * FROM performers WHERE NORMALIZE(name) = NORMALIZE(?)"
            self._execute(query, (performers_name,))
            rows = self._cursor.fetchall()
            return [self._row_to_performer(row) for row in rows]
        except sqlite3.Error as e:
            print(f"检索 name 时出错: {e}")
            return list()

    def get_all(self) -> List[Performers]:
        """
        查询所有记录。
        """
        query = "SELECT * FROM performers"
        self._execute(query)
        rows = self._cursor.fetchall()
        return [self._row_to_performer(row) for row in rows]

    def update(self, performer: Performers) -> int:
        """
        更新一条现有记录。
        """
        if performer.id is None:
            raise ValueError("Performer ID is required for update.")

        now = datetime.now().isoformat()
        query = """
        UPDATE performers SET
            name = ?, disambiguation = ?, gender = ?, birthdate = ?, ethnicity = ?,
            country = ?, eye_color = ?, height = ?, measurements = ?, fake_tits = ?,
            career_length = ?, tattoos = ?, piercings = ?, favorite = ?, updated_at = ?,
            details = ?, death_date = ?, hair_color = ?, weight = ?, rating = ?,
            ignore_auto_tag = ?, image_blob = ?, penis_length = ?, circumcised = ?
        WHERE id = ?
        """
        params = (
            performer.name, performer.disambiguation, performer.gender, performer.birthdate,
            performer.ethnicity, performer.country, performer.eye_color, performer.height,
            performer.measurements, performer.fake_tits, performer.career_length,
            performer.tattoos, performer.piercings, performer.favorite, now,
            performer.details, performer.death_date, performer.hair_color, performer.weight,
            performer.rating, performer.ignore_auto_tag, performer.image_blob,
            performer.penis_length, performer.circumcised, performer.id
        )
        self._execute(query, params)
        return self._cursor.rowcount

    def delete(self, performer_id: int) -> int:
        """
        根据 ID 删除一条记录。
        """
        query = "DELETE FROM performers WHERE id = ?"
        self._execute(query, (performer_id,))
        return self._cursor.rowcount
