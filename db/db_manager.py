import sqlite3
from typing import Optional

from models.blobs_dao import BlobsDAO
from models.files_dao import FilesDAO
from models.folders_dao import FoldersDAO
from models.galleries_dao import GalleriesDAO
from models.performers_dao import PerformersDAO
from models.performers_scenes_dao import PerformersScenesDAO
from models.scenes_dao import ScenesDAO
from models.scenes_files_dao import ScenesFilesDAO
from models.scenes_galleries_dao import ScenesGalleriesDAO
from models.scenes_tags_dao import ScenesTagsDAO
from models.studios_dao import StudiosDAO
from models.tags_dao import TagsDAO


class Database:
    """
    一个用于管理数据库连接和所有 DAO 的上下文管理器。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None

        # 定义所有的 DAO 属性
        self.blobs: Optional[BlobsDAO] = None
        self.files: Optional[FilesDAO] = None
        self.performers: Optional[PerformersDAO] = None
        self.performers_scenes: Optional[PerformersScenesDAO] = None
        self.scenes: Optional[ScenesDAO] = None
        self.scenes_files: Optional[ScenesFilesDAO] = None
        self.scenes_galleries: Optional[ScenesGalleriesDAO] = None
        self.scenes_tags: Optional[ScenesTagsDAO] = None
        self.studios: Optional[StudiosDAO] = None
        self.tags: Optional[TagsDAO] = None

    def __enter__(self):
        """
        进入 'with' 语句块时被调用。
        建立数据库连接并实例化所有 DAO。
        """
        self._conn = sqlite3.connect(self.db_path)

        # 实例化所有 DAO，并将相同的连接传递给它们
        self.blobs = BlobsDAO(self._conn)
        self.files = FilesDAO(self._conn)
        self.performers = PerformersDAO(self._conn)
        self.performers_scenes = PerformersScenesDAO(self._conn)
        self.scenes = ScenesDAO(self._conn)
        self.scenes_files = ScenesFilesDAO(self._conn)
        self.scenes_galleries = ScenesGalleriesDAO(self._conn)
        self.scenes_tags = ScenesTagsDAO(self._conn)
        self.studios = StudiosDAO(self._conn)
        self.tags = TagsDAO(self._conn)
        self.folders = FoldersDAO(self._conn)
        self.galleries = GalleriesDAO(self._conn)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出 'with' 语句块时被调用。
        处理事务和关闭连接。
        """
        if exc_type is None:
            # 如果没有异常，提交事务
            self._conn.commit()
        else:
            # 如果有异常，回滚事务
            self._conn.rollback()
            print(f"检测到异常 ({exc_type.__name__})，事务已回滚。")

        # 关闭连接
        self._conn.close()
