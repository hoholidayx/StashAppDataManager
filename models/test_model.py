# 假设您有一个名为 'database.db' 的数据库文件，并且其中已存在 scenes 表。
import sqlite3

from models.blobs_dao import BlobsDAO, Blobs
from models.performers_dao import PerformersDAO, Performers
from models.performers_scenes_dao import PerformersScenesDAO
from models.scenes_dao import ScenesDAO, Scenes
from models.scenes_galleries_dao import ScenesGalleriesDAO
from models.scenes_tags_dao import ScenesTags, ScenesTagsDAO
from models.studios_dao import StudiosDAO, Studios
from models.tags_dao import TagsDAO, Tags

DB_PATH = 'stash-go-dev.sqlite'


def test_scenes():
    # 为了测试，我们先创建一个空的数据库和表
    def setup_database(db_path: str):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "scenes" (
            `id` integer not null primary key autoincrement,
            `title` varchar(255),
            `details` text,
            `date` date,
            `rating` tinyint,
            `studio_id` integer,
            `organized` boolean not null default '0',
            `created_at` datetime not null,
            `updated_at` datetime not null,
            `code` text,
            `director` text,
            `resume_time` float not null default 0,
            `play_duration` float not null default 0,
            `cover_blob` varchar(255)
        )
        """)
        conn.commit()
        conn.close()

    setup_database(DB_PATH)

    # 使用 with 语句来确保数据库连接的正确关闭
    with ScenesDAO(DB_PATH) as dao:
        # 1. 创建新场景
        new_scenes = Scenes(
            title="Test Scenes 1",
            code="A-123",
            rating=5,
            organized=True
        )
        new_id = dao.create(new_scenes)
        print(f"新场景创建成功，ID为: {new_id}")

        # 2. 根据 ID 读取场景
        retrieved_scenes = dao.get_by_id(new_id)
        print(f"读取到的场景: {retrieved_scenes}")
        print(f"标题: {retrieved_scenes.title}, 编码: {retrieved_scenes.code}")

        # 3. 更新场景信息
        retrieved_scenes.title = "Updated Title"
        rows_updated = dao.update(retrieved_scenes)
        print(f"更新了 {rows_updated} 条记录。")

        # 4. 读取所有场景
        all_scenes = dao.get_all()
        print("\n所有场景:")
        for scenes in all_scenes:
            print(scenes)

        # 5. 删除场景
        rows_deleted = dao.delete(new_id)
        print(f"\n删除了 {rows_deleted} 条记录。")

        # 再次读取所有场景，确认已删除
        remaining_scenes = dao.get_all()
        print(f"剩余场景数量: {len(remaining_scenes)}")


def test_blobs():
    # 1. 使用 with 语句来自动管理连接
    with BlobsDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        test_checksum = "d41d8cd98f00b204e9800998ecf8427e"
        test_data = "这是一条测试字符串，会被转换为二进制数据。".encode("utf-8")
        test_blob_obj = Blobs(checksum=test_checksum, blob=test_data)

        # 4. 插入测试数据
        print("\n--- 正在插入数据... ---")
        dao.insert(test_blob_obj)

        # 5. 尝试再次插入相同的数据，应该会失败
        print("\n--- 正在尝试插入重复数据... ---")
        dao.insert(test_blob_obj)

        # 6. 检索刚刚插入的数据
        print("\n--- 正在检索数据... ---")
        retrieved_blob_obj = dao.get_by_checksum(test_checksum)

        # 7. 验证数据
        if retrieved_blob_obj:
            print(f"检索到的数据对象: {retrieved_blob_obj}")
            if retrieved_blob_obj.blob == test_data:
                print("✅ 数据匹配，测试通过！")
            else:
                print("❌ 数据不匹配，测试失败。")
        else:
            print("❌ 检索失败，无法进行数据验证。")

        # 8. 更新数据
        print("\n--- 正在更新数据... ---")
        updated_data = "这是一条更新后的字符串".encode("utf-8")
        updated_blob_obj = Blobs(checksum=test_checksum, blob=updated_data)
        dao.update_blob(updated_blob_obj)

        # 9. 检索更新后的数据并验证
        print("\n--- 正在检索更新后的数据... ---")
        retrieved_updated_blob = dao.get_by_checksum(test_checksum)
        if retrieved_updated_blob and retrieved_updated_blob.blob == updated_data:
            print("✅ 数据更新并检索成功，测试通过！")
        else:
            print("❌ 数据更新或检索失败。")

        # 10. 检索一个不存在的 blob
        print("\n--- 正在检索一个不存在的 blob... ---")
        dao.get_by_checksum("non_existent_checksum")

        # 11. 删除数据
        print("\n--- 正在删除数据... ---")
        dao.delete_by_checksum(test_checksum)

        # 12. 验证删除
        print("\n--- 正在验证删除... ---")
        deleted_blob = dao.get_by_checksum(test_checksum)
        if not deleted_blob:
            print("✅ 数据已成功删除，测试通过！")
        else:
            print("❌ 数据删除失败。")


def test_tags():
    # 1. 使用 with 语句来自动管理连接
    with TagsDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        test_tag = Tags(
            name="Test Tag",
            description="A test tag for demonstration purposes.",
            favorite=True,
            sort_name="testtag"
        )

        # 4. 插入测试数据
        print("\n--- 正在插入数据... ---")
        tag_id = dao.insert(test_tag)
        if tag_id:
            print(f"新插入标签的 ID: {tag_id}")
            test_tag.id = tag_id
        else:
            print("❌ 插入失败。")

        # 5. 检索刚刚插入的数据
        print("\n--- 正在检索数据... ---")
        retrieved_tag = dao.get_by_id(test_tag.id)
        if retrieved_tag:
            print(f"检索到的数据对象: {retrieved_tag}")
            if retrieved_tag.name == "Test Tag" and retrieved_tag.favorite:
                print("✅ 数据匹配，测试通过！")
            else:
                print("❌ 数据不匹配，测试失败。")
        else:
            print("❌ 检索失败。")

        # 6. 更新数据
        print("\n--- 正在更新数据... ---")
        retrieved_tag.name = "Updated Tag Name"
        retrieved_tag.favorite = False
        row_count = dao.update(retrieved_tag)
        if row_count > 0:
            print("✅ 数据更新成功！")
        else:
            print("❌ 数据更新失败。")

        # 7. 检索更新后的数据并验证
        print("\n--- 正在检索更新后的数据... ---")
        retrieved_updated_tag = dao.get_by_id(retrieved_tag.id)
        if retrieved_updated_tag and retrieved_updated_tag.name == "Updated Tag Name":
            print("✅ 数据更新并检索成功，测试通过！")
        else:
            print("❌ 数据更新或检索失败。")

        # 8. 删除数据
        print("\n--- 正在删除数据... ---")
        row_count = dao.delete(retrieved_tag.id)
        if row_count > 0:
            print("✅ 数据已成功删除！")
        else:
            print("❌ 数据删除失败。")

        # 9. 验证删除
        print("\n--- 正在验证删除... ---")
        deleted_tag = dao.get_by_id(retrieved_tag.id)
        if not deleted_tag:
            print("✅ 数据已成功删除，测试通过！")
        else:
            print("❌ 数据删除失败。")

    print("\n所有测试已完成。")


def test_scenes_tags():
    # 1. 使用 with 语句来自动管理连接
    with ScenesTagsDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        # 假设存在 ID 为 101, 102 的场景和 ID 为 201, 202 的标签
        test_relations = [
            ScenesTags(scene_id=101, tag_id=201),
            ScenesTags(scene_id=101, tag_id=202),
            ScenesTags(scene_id=102, tag_id=201)
        ]

        # 4. 批量插入测试数据
        print("\n--- 正在插入数据... ---")
        for relation in test_relations:
            dao.insert(relation)

        # 5. 检索特定关系
        print("\n--- 正在检索特定关系 (101, 201)... ---")
        retrieved_relation = dao.get_by_ids(101, 201)
        if retrieved_relation:
            print(f"检索到的关系: {retrieved_relation}")
            print("✅ 关系检索成功，测试通过！")
        else:
            print("❌ 关系检索失败。")

        # 6. 根据场景 ID 检索所有关系
        print("\n--- 正在根据场景 ID 检索所有关系 (101)... ---")
        relations_by_scene = dao.get_all_by_scene_id(101)
        if len(relations_by_scene) == 2:
            print(f"找到 {len(relations_by_scene)} 条关系: {relations_by_scene}")
            print("✅ 根据场景 ID 检索成功，测试通过！")
        else:
            print("❌ 根据场景 ID 检索失败。")

        # 7. 根据标签 ID 检索所有关系
        print("\n--- 正在根据标签 ID 检索所有关系 (201)... ---")
        relations_by_tag = dao.get_all_by_tag_id(201)
        if len(relations_by_tag) == 2:
            print(f"找到 {len(relations_by_tag)} 条关系: {relations_by_tag}")
            print("✅ 根据标签 ID 检索成功，测试通过！")
        else:
            print("❌ 根据标签 ID 检索失败。")

        # 8. 删除一条关系
        print("\n--- 正在删除一条关系 (101, 202)... ---")
        row_count = dao.delete(101, 202)
        if row_count > 0:
            print("✅ 删除成功！")
        else:
            print("❌ 删除失败。")

        # 9. 验证删除
        print("\n--- 正在验证删除... ---")
        deleted_relation = dao.get_by_ids(101, 202)
        if not deleted_relation:
            print("✅ 关系已成功删除，测试通过！")
        else:
            print("❌ 关系删除失败。")

    print("\n所有测试已完成。")


def test_studios_dao():
    """
    测试 StudiosDAO 类的所有功能。
    """
    print("--- 正在开始 StudiosDAO 测试... ---")

    # 1. 使用 with 语句来自动管理连接
    with StudiosDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        test_studio = Studios(
            name="Test Studio",
            url="http://test.com",
            details="A test studio for demonstration.",
            rating=5,
            favorite=True
        )

        # 4. 插入测试数据
        print("\n--- 正在插入数据... ---")
        studio_id = dao.insert(test_studio)
        if studio_id:
            print(f"新插入工作室的 ID: {studio_id}")
            test_studio.id = studio_id
        else:
            print("❌ 插入失败。")
            return

        # 5. 检索刚刚插入的数据
        print("\n--- 正在检索数据... ---")
        retrieved_studio = dao.get_by_id(test_studio.id)
        if retrieved_studio and retrieved_studio.name == "Test Studio":
            print(f"检索到的数据对象: {retrieved_studio}")
            print("✅ 数据匹配，测试通过！")
        else:
            print("❌ 数据不匹配，测试失败。")
            return

        # 6. 更新数据
        print("\n--- 正在更新数据... ---")
        retrieved_studio.name = "Updated Studio Name"
        retrieved_studio.rating = 4
        row_count = dao.update(retrieved_studio)
        if row_count > 0:
            print("✅ 数据更新成功！")
        else:
            print("❌ 数据更新失败。")
            return

        # 7. 检索更新后的数据并验证
        print("\n--- 正在检索更新后的数据... ---")
        retrieved_updated_studio = dao.get_by_id(retrieved_studio.id)
        if retrieved_updated_studio and retrieved_updated_studio.name == "Updated Studio Name":
            print("✅ 数据更新并检索成功，测试通过！")
        else:
            print("❌ 数据更新或检索失败。")
            return

        # 8. 删除数据
        print("\n--- 正在删除数据... ---")
        row_count = dao.delete(retrieved_studio.id)
        if row_count > 0:
            print("✅ 数据已成功删除！")
        else:
            print("❌ 数据删除失败。")
            return

        # 9. 验证删除
        print("\n--- 正在验证删除... ---")
        deleted_studio = dao.get_by_id(retrieved_studio.id)
        if not deleted_studio:
            print("✅ 数据已成功删除，测试通过！")
        else:
            print("❌ 数据删除失败。")
            return

    print("\n所有测试已完成。")


def test_scenes_galleries_dao():
    """
    测试 ScenesGalleriesDAO 类的所有功能。
    """
    print("--- 正在开始 ScenesGalleriesDAO 测试... ---")

    # 1. 使用 with 语句来自动管理连接
    with ScenesGalleriesDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        scene_id = 1
        gallery_id_1 = 1
        gallery_id_2 = 2

        # 4. 插入测试数据
        print("\n--- 正在插入关联记录... ---")
        row_count_1 = dao.insert(scene_id, gallery_id_1)
        row_count_2 = dao.insert(scene_id, gallery_id_2)

        if row_count_1 > 0 and row_count_2 > 0:
            print("✅ 关联记录插入成功！")
        else:
            print("❌ 关联记录插入失败。")
            return

        # 5. 检索关联数据
        print("\n--- 正在检索 scene_id 为 1 的关联记录... ---")
        retrieved_records = dao.get_by_scene_id(scene_id)
        if len(retrieved_records) == 2:
            print(f"检索到的记录数: {len(retrieved_records)}")
            print("✅ 记录数匹配，测试通过！")
        else:
            print(f"❌ 记录数不匹配。预期 2，得到 {len(retrieved_records)}。")
            return

        # 6. 删除一条记录
        print("\n--- 正在删除 scene_id 为 1, gallery_id 为 1 的记录... ---")
        row_count = dao.delete(scene_id, gallery_id_1)
        if row_count > 0:
            print("✅ 记录已成功删除！")
        else:
            print("❌ 记录删除失败。")
            return

        # 7. 验证删除
        print("\n--- 正在验证删除... ---")
        remaining_records = dao.get_by_scene_id(scene_id)
        if len(remaining_records) == 1 and remaining_records[0].gallery_id == gallery_id_2:
            print("✅ 记录已成功删除，并且只剩下一条记录，测试通过！")
        else:
            print("❌ 验证删除失败。")
            return

    print("\n所有测试已完成。")


def test_performers_scenes_dao():
    """
    测试 PerformersScenesDAO 类的所有功能。
    """
    print("--- 正在开始 PerformersScenesDAO 测试... ---")

    # 1. 使用 with 语句来自动管理连接
    with PerformersScenesDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        performer_id_1 = 1
        performer_id_2 = 2
        scene_id_1 = 1
        scene_id_2 = 2

        # 4. 插入测试数据
        print("\n--- 正在插入关联记录... ---")
        row_count_1 = dao.insert(performer_id_1, scene_id_1)
        row_count_2 = dao.insert(performer_id_2, scene_id_1)
        row_count_3 = dao.insert(performer_id_1, scene_id_2)

        if row_count_1 > 0 and row_count_2 > 0 and row_count_3 > 0:
            print("✅ 关联记录插入成功！")
        else:
            print("❌ 关联记录插入失败。")
            return

        # 5. 检索关联数据
        print("\n--- 正在检索 scene_id 为 1 的关联记录... ---")
        retrieved_by_scene = dao.get_by_scene_id(scene_id_1)
        if len(retrieved_by_scene) == 2:
            print(f"检索到的记录数: {len(retrieved_by_scene)}")
            print("✅ 记录数匹配，测试通过！")
        else:
            print(f"❌ 记录数不匹配。预期 2，得到 {len(retrieved_by_scene)}。")
            return

        print("\n--- 正在检索 performer_id 为 1 的关联记录... ---")
        retrieved_by_performer = dao.get_by_performer_id(performer_id_1)
        if len(retrieved_by_performer) == 2:
            print(f"检索到的记录数: {len(retrieved_by_performer)}")
            print("✅ 记录数匹配，测试通过！")
        else:
            print(f"❌ 记录数不匹配。预期 2，得到 {len(retrieved_by_performer)}。")
            return

        # 6. 删除一条记录
        print("\n--- 正在删除 performer_id 为 1, scene_id 为 1 的记录... ---")
        row_count = dao.delete(performer_id_1, scene_id_1)
        if row_count > 0:
            print("✅ 记录已成功删除！")
        else:
            print("❌ 记录删除失败。")
            return

        # 7. 验证删除
        print("\n--- 正在验证删除... ---")
        remaining_records = dao.get_by_performer_id(performer_id_1)
        if len(remaining_records) == 1 and remaining_records[0].scene_id == scene_id_2:
            print("✅ 记录已成功删除，并且只剩下一条记录，测试通过！")
        else:
            print("❌ 验证删除失败。")
            return

    print("\n所有测试已完成。")


def test_performers_dao():
    """
    测试 PerformersDAO 类的所有功能。
    """
    print("--- 正在开始 PerformersDAO 测试... ---")

    # 1. 使用 with 语句来自动管理连接
    with PerformersDAO(DB_PATH) as dao:
        # 2. 创建表
        dao.create_table()

        # 3. 准备测试数据
        test_performer = Performers(
            name="Jane Doe",
            gender="female",
            birthdate="1990-01-01",
            rating=5,
            favorite=True,
            details="A test performer for demonstration."
        )

        # 4. 插入测试数据
        print("\n--- 正在插入数据... ---")
        performer_id = dao.insert(test_performer)
        if performer_id:
            print(f"新插入表演者的 ID: {performer_id}")
            test_performer.id = performer_id
        else:
            print("❌ 插入失败。")
            return

        # 5. 检索刚刚插入的数据
        print("\n--- 正在检索数据... ---")
        retrieved_performer = dao.get_by_id(test_performer.id)
        if retrieved_performer and retrieved_performer.name == "Jane Doe":
            print(f"检索到的数据对象: {retrieved_performer}")
            print("✅ 数据匹配，测试通过！")
        else:
            print("❌ 数据不匹配，测试失败。")
            return

        # 6. 更新数据
        print("\n--- 正在更新数据... ---")
        retrieved_performer.name = "Jane Smith"
        retrieved_performer.rating = 4
        row_count = dao.update(retrieved_performer)
        if row_count > 0:
            print("✅ 数据更新成功！")
        else:
            print("❌ 数据更新失败。")
            return

        # 7. 检索更新后的数据并验证
        print("\n--- 正在检索更新后的数据... ---")
        retrieved_updated_performer = dao.get_by_id(retrieved_performer.id)
        if retrieved_updated_performer and retrieved_updated_performer.name == "Jane Smith":
            print("✅ 数据更新并检索成功，测试通过！")
        else:
            print("❌ 数据更新或检索失败。")
            return

        # 8. 删除数据
        print("\n--- 正在删除数据... ---")
        row_count = dao.delete(retrieved_performer.id)
        if row_count > 0:
            print("✅ 数据已成功删除！")
        else:
            print("❌ 数据删除失败。")
            return

        # 9. 验证删除
        print("\n--- 正在验证删除... ---")
        deleted_performer = dao.get_by_id(retrieved_performer.id)
        if not deleted_performer:
            print("✅ 数据已成功删除，测试通过！")
        else:
            print("❌ 数据删除失败。")
            return

    print("\n所有测试已完成。")


if __name__ == '__main__':
    # test_scenes()
    # test_blobs()
    # test_tags()
    # test_scenes_tags()
    # test_studios_dao()
    # test_scenes_galleries_dao()
    # test_performers_scenes_dao()
    test_performers_dao()
