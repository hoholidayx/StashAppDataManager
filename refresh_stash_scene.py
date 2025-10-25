import argparse
import hashlib
import os
import shutil
from collections import deque
from datetime import datetime
from typing import List

from MyLogger import CustomLogger
from db.db_manager import Database
from models.blobs_dao import Blobs
from models.groups_dao import Groups
from models.performers_dao import Performers
from models.scenes_dao import ScenesDAO, Scenes
from models.scenes_tags_dao import ScenesTags
from models.studios_dao import StudiosDAO, Studios
from models.tags_dao import Tags
from nfo.nfo_parser import parse_nfo_to_movie, Movie

# 初始化日志记录器
logger = CustomLogger(__name__)

# StashApp 存储 blobs 的路径
STASH_APP_BLOBS_DIRS = '/Users/hoholiday/Downloads/stashapp/blobs'
# StashApp 数据库文件路径
STASH_APP_SQLITE_DB_PATH = '/Users/hoholiday/Projects/Python/StashAppManager/stash-go-dev.sqlite'
# 电影文件后缀
SCENE_FILE_SUBFIX = ['.mp4', '.mkv', '.avi', '.mov']
# 影片的封面图片文件名
SCENE_COVER_FILE_NAME = 'poster.jpg'
# 影片的nfo文件名
SCENE_NFO_FILE_NAME = 'movie.nfo'


def update_scene_title(scene_dao: ScenesDAO, scene_record: Scenes, info: Movie):
    """
    更新短片标题。
    使用 nfo 的 title 更新到 scenes 表 title 字段
    :param scene_dao:
    :param scene_record:
    :param info:
    :return:更新是否成功
    """
    scene_record.title = info.title
    try:
        scene_dao.update(scene_record)
        scene_dao.commit()
    except Exception as ex:
        logger.error(f"更新短片标题失败: {ex}", exc_info=True)
        return False
    return True


def find_files_by_code(folder_path: str, movie_code: str, file_suffixes: List[str]) -> List[str]:
    """
    在指定文件夹中，根据文件名和后缀查找匹配的文件。

    参数:
    folder_path (str): 要搜索的文件夹路径。
    movie_code (str): 电影代码，用于匹配文件名中的 [代码] 部分。
    file_suffixes (List[str]): 包含有效文件后缀的列表。

    返回:
    List[str]: 匹配到的文件路径列表。
    """
    found_files: List[str] = []

    if not os.path.isdir(folder_path):
        logger.warning(f"错误: 路径 '{folder_path}' 不是一个有效的文件夹。")
        return found_files

    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            # 确保是文件且后缀匹配
            if os.path.isfile(file_path) and filename.lower().endswith(tuple(s.lower() for s in file_suffixes)):
                # 检查文件名是否包含指定的电影代码
                if movie_code in filename:
                    found_files.append(file_path)

    except OSError as e:
        logger.error(f"处理文件夹时出错: {e}")

    return found_files


def update_scene_cover(dbm: Database, scene_record: Scenes, img_path: str):
    """
    更新短片封面。
    :param img_path: 封面图路径
    :return: 更新结果
    """
    # 1. 检查文件是否存在
    if not os.path.isfile(img_path):
        logger.warning(f"错误：文件不存在或不是有效文件 -> {img_path}")
        return False

    # 2. 解析文件 md5, 32 位
    file_md5 = ''
    try:
        # 创建一个 MD5 哈希对象
        hash_md5 = hashlib.md5()

        # 以二进制模式分块读取文件，防止大文件占用过多内存
        with open(img_path, "rb") as f:
            # 循环读取文件，每次读取 4096 字节
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        file_md5 = hash_md5.hexdigest()
        logger.info(f"文件 MD5 值为: {file_md5}")
    except Exception as e:
        logger.error(f"计算文件 MD5 失败: {e}", exc_info=True)
        return False

    # 3. 以 hash_md5 的1-2位为名创建文件夹A，3-4位为名创建文件夹B，将 img_path 复制到路径 STASH_APP_BLOBS_DIRS/A/B/hash_md5.{原后缀名}
    try:
        # 获取 MD5 字符串的前两位和第三四位
        dir_a = file_md5[0:2]
        dir_b = file_md5[2:4]

        # 拼接目标文件夹的完整路径
        target_dir = os.path.join(STASH_APP_BLOBS_DIRS, dir_a, dir_b)

        # 检查并创建文件夹（如果不存在）
        os.makedirs(target_dir, exist_ok=True)

        # 拼接目标文件的完整路径，不包含后缀名
        target_path = os.path.join(target_dir, file_md5)

        # 复制文件
        shutil.copy2(img_path, target_path)
        logger.info(f"文件已成功复制到 -> {target_path}")
    except Exception as e:
        logger.error(f"创建文件夹或复制文件失败: {e}", exc_info=True)
        return False

    # 4. 用file_md5 更新 scenes 表中的 cover 字段，与 blobs 表
    try:
        scene_record.cover_blob = file_md5
        dbm.scenes.update(scene_record)
        blobs = Blobs(checksum=file_md5, blob=None)
        dbm.blobs.insert(blobs)
        dbm.commit()
    except Exception as e:
        logger.error(f"blob 数据记录写入失败: {e}", exc_info=True)

    return True


def update_scene_date(scene_dao: ScenesDAO, scene_record: Scenes, info: Movie):
    """
        更新短片发行日期。
        使用 nfo 的 premiered 更新到 scenes 表 date 字段
        :param scene_dao:
        :param scene_record:
        :param info:
        :return:更新是否成功
    """
    scene_record.date = info.premiered
    try:
        scene_dao.update(scene_record)
        scene_dao.commit()
    except Exception as ex:
        logger.error(f"更新短片发行日期失败: {ex}", exc_info=True)
        return False
    return True


def update_scene_studio(scene_dao: ScenesDAO, studio_dao: StudiosDAO, scene_record: Scenes, info: Movie):
    """
        更新短片工作室。
        1. 根据名movie studio字段，查询 studios 表当前工作室记录是否存在
        2. 如存记录存在，直接更新 scene studio id
        3. 记录不存在，先在 studios 创建记录
        :param scene_dao:
        :param scene_record:
        :param studio_dao:
        :param info:
        :return:更新是否成功
    """
    studio_name = info.studio
    studio_record = studio_dao.get_by_name(studio_name)
    if studio_record is None:
        studio_record = Studios(name=studio_name)
        studio_id = studio_dao.insert(studio_record)
        studio_dao.commit()
        if studio_id is None:
            logger.error(f"工作室:{studio_name} 记录创建失败!")
            return False
        logger.info(f"找不到工作室:{studio_name}，已创建新工作室记录:{studio_id}")
    else:
        studio_id = studio_record.id
        logger.info(f"找到已存在工作室记录:{studio_record}")

    scene_record.studio_id = studio_id
    try:
        scene_dao.update(scene_record)
        scene_dao.commit()
    except Exception as ex:
        logger.error(f"更新短片工作室失败: {ex}", exc_info=True)
        return False
    return True


def update_scene_tags(dbm, scene_record, info):
    """
        更新短片标签。
        1. 遍历 info 中的 genres 标签列表
        2. 检查 tags 表对应名称的标签是否已存在
        3. 记录不存在，先在 tags 创建记录
        4. 最后更新 scenes_tags 表建立映射关系
        :return:更新是否成功
    """
    tags = info.genres
    if tags is None or len(tags) == 0:
        logger.warning(f"影片信息无任何标签!")
        return False
    logger.info(f"准备更新 {len(tags)} 个标签:[{','.join(tags)}]")

    # 检查标签是否都存在
    for tag in tags:
        db_result = dbm.tags.get_by_name(tag)
        if db_result is None or len(db_result) == 0:
            logger.info(f"标签[{tag}]不存在,准备创建...")
            tag_obj = Tags(name=tag)
            tag_obj_id = dbm.tags.insert(tag_obj)
            if tag_obj_id is None:
                logger.error(f"标签[{tag}]创建失败！")
                # 标签创建异常，尝试下一个
                continue
            logger.info(f"标签[{tag}]创建成功, id:{tag_obj_id}！")

    # 重新查询数据库
    tag_obj_list = list()
    for tag in tags:
        tag_objs = dbm.tags.get_by_name(tag)
        if tag_objs is None or len(tag_objs) == 0:
            # 查询不到标签，跳过
            logger.warning(f"标签[{tag}]不存在，跳过")
            continue
        # 取匹配的第一个就行
        tag_obj_list.append(tag_objs[0])

    # 建立映射
    new_scene_tag_record_count = 0
    old_scene_tag_record_count = 0
    for tag_obj in tag_obj_list:
        # 检查映射记录是否已存在
        scene_tag_record = dbm.scenes_tags.get_by_ids(scene_id=scene_record.id, tag_id=tag_obj.id)
        if scene_tag_record is not None:
            old_scene_tag_record_count += 1
            continue
        # 无映射记录，新建
        scene_tag_record = ScenesTags(scene_id=scene_record.id, tag_id=tag_obj.id)
        row_count = dbm.scenes_tags.insert(scene_tag_record)
        new_scene_tag_record_count += row_count
    logger.info(f"已完成标签更新,新增数量:{new_scene_tag_record_count}"
                f"，已有数量:{old_scene_tag_record_count}")
    return True


def update_scene_gallery(dbm, scene_record, info: Movie):
    """
    更新短片剧照(画廊)。
    1、从 dbm.folders_dao 查询 path 字段，包含格式 "fanart#{info.uniqueid}",不区分大小写
    2. 找到匹配的 folders 记录后，再去 dbm.galleries_dao，查询 folder_id 匹配的记录
    3. 更新 scenes_galleries 表，更新 scene-gallery的映射关系
    :param dbm:
    :param scene_record:
    :param info:
    :return:
    """
    search_pattern = f"%fanart#{info.uniqueid}"
    logger.info(f"正在查询对应番号的剧照源文件夹: {search_pattern}")
    folder_record_list = dbm.folders.get_by_path_pattern(search_pattern)
    if folder_record_list is None or len(folder_record_list) == 0:
        logger.warning("找不到对应的剧照源文件夹！")
        return False
    # 默认取第一个就行
    fanart_folder_record = folder_record_list[0]
    logger.info(f"找到对应的剧照源文件夹:{fanart_folder_record.path}")

    gallery_record_list = dbm.galleries.get_by_folder_id(fanart_folder_record.id)
    if gallery_record_list is None or len(gallery_record_list) == 0:
        logger.warning("找不到对应的画廊！")
        return False
    # 默认取第一个就行
    gallery_record = gallery_record_list[0]
    logger.info(f"找到对应的画廊:{gallery_record}")

    # 检查是否已有相同映射记录
    exists_scene_gallery_record = dbm.scenes_galleries.get_by_ids(scene_id=scene_record.id,
                                                                  gallery_id=gallery_record.id)
    if exists_scene_gallery_record is not None and len(exists_scene_gallery_record) > 0:
        logger.info(f"scene gallery 映射表已存在相同映射记录！")
        return True

    scene_update_ret = dbm.scenes_galleries.update_or_insert(scene_id=scene_record.id, gallery_id=gallery_record.id)
    logger.info(f"已更新 scene gallery 映射表！{scene_update_ret}")
    if scene_update_ret == 0:
        return False
    return True


def update_scene_director(dbm, scene_record, movie_info):
    """
        更新短片导演。
        使用 nfo 的 director 更新到 scenes 表 director 字段
        :param scene_dao:
        :param scene_record:
        :param info:
        :return:更新是否成功
    """
    if not movie_info.director:
        logger.warning(f"nfo 未解析出合法的[导演]字段:{movie_info.director}")
        return False
    scene_record.director = movie_info.director
    try:
        dbm.scenes.update(scene_record)
        dbm.scenes.commit()
        logger.info(f"已更新[导演]:{scene_record.director}")
    except Exception as ex:
        logger.error(f"更新短片导演失败: {ex}", exc_info=True)
        return False
    return True


def update_scene_code(dbm, scene_record, info):
    """
      更新短片番号。
      使用 nfo 的 uniqueid 更新到 scenes 表 code 字段
      :param dbm:
      :param scene_record:
      :param info:
      :return:更新是否成功
    """
    scene_record.code = info.uniqueid
    try:
        dbm.scenes.update(scene_record)
        dbm.scenes.commit()
    except Exception as ex:
        logger.error(f"更新短片番号失败: {ex}", exc_info=True)
        return False
    return True


def update_scene_groups(dbm, scene_record, info):
    """
    更新集合（影片系列）
    1、解析 info.movie_set 集合，从 groups 查询 name 字段是否已存在对应的集合
    2、如不存在创建新集合
    3、更新 groups_scenes 表，添加映射关系（如不存在）
    :param dbm:
    :param scene_record:
    :param info: MovieInfo
    :return:
    """

    movie_set = info.movie_set
    if movie_set is None:
        logger.warning(f"影片信息无任何集合信息!")
        return False
    logger.info(f"准备更新影片集合：{movie_set}")

    # 检查影片集合是否已存在
    db_result = dbm.groups.get_by_name(movie_set.name)
    group_id = None
    if db_result is None:
        logger.info(f"影片集合[{movie_set.name}]不存在,准备创建...")
        group_obj = Groups(name=movie_set.name)
        group_id = dbm.groups.insert(group_obj)
        if group_id is None:
            logger.error(f"影片集合[{group_obj}]创建失败！")
            return False
        logger.info(f"影片集合[{group_obj}]创建成功！")

    # 重新查询数据库
    group_obj = dbm.groups.get_by_name(movie_set.name)
    if group_obj is None:
        logger.error(f"影片集合[{movie_set.name}]查询数据库失败！")
        return False

    # 建立映射
    # 检查映射记录是否已存在
    groups_scenes_record = dbm.groups_scenes.get_by_ids(scene_id=scene_record.id, group_id=group_obj.id)
    if groups_scenes_record is not None:
        # 记录已存在，直接返回成功
        logger.info(f"影片集合-影片映射已存在！{groups_scenes_record}")
        return True
    # 无映射记录，新建
    row_count = dbm.groups_scenes.insert(scene_id=scene_record.id, group_id=group_obj.id)
    return row_count > 0


def update_scene_performers(dbm, scene_record, movie_info):
    """
     更新短片演员。
     1. 遍历 info 中的 actors 演员列表
     2. 检查 performers 表对应名称 name 的演员是否已存在
     3. 演员记录不存在，先在 performers 创建记录
     4. 最后更新 performers_scenes 表建立映射关系
     :return:更新是否成功
    """
    performers = movie_info.actors
    if performers is None or len(performers) == 0:
        logger.warning(f"影片信息无任何演员!")
        return False
    logger.info(f"准备更新 {len(performers)} 个演员")

    # 检查演员是否都存在
    for performer in performers:
        db_result = dbm.performers.get_by_name(performer.name)
        if db_result is None or len(db_result) == 0:
            logger.info(f"演员[{performer}]不存在,准备创建...")
            performer_obj = Performers(name=performer.name)
            performer_obj_id = dbm.performers.insert(performer_obj)
            if performer_obj_id is None:
                logger.error(f"演员[{performer}]创建失败！")
                # 标签创建异常，尝试下一个
                continue
            logger.info(f"演员[{performer}]创建成功, id:{performer_obj_id}！")

    # 重新查询数据库
    performer_obj_list = list()
    for performer in performers:
        performer_objs = dbm.performers.get_by_name(performer.name)
        if performer_objs is None or len(performer_objs) == 0:
            # 查询不到演员，跳过
            logger.warning(f"演员[{performer}]不存在，跳过")
            continue
        # 取匹配的第一个就行
        performer_obj_list.append(performer_objs[0])

    # 建立映射
    new_scene_performer_record_count = 0
    old_scene_performer_record_count = 0
    for performer_obj in performer_obj_list:
        # 检查映射记录是否已存在
        scene_performer_record = dbm.performers_scenes.get_by_ids(scene_id=scene_record.id,
                                                                  performer_id=performer_obj.id)
        if scene_performer_record is not None:
            old_scene_performer_record_count += 1
            continue
        # 无映射记录，新建
        row_count = dbm.performers_scenes.insert(performer_id=performer_obj.id, scene_id=scene_record.id)
        new_scene_performer_record_count += row_count
    logger.info(f"已完成演员更新,新增数量:{new_scene_performer_record_count}"
                f"，已有数量:{old_scene_performer_record_count}")
    return True


def process_folder(folder_path):
    """
    一个处理文件夹的示例函数。
    """
    if not os.path.isdir(folder_path):
        logger.warning(f"错误：路径 '{folder_path}' 不是一个有效的文件夹。")
        return

    logger.info(f"正在处理文件夹：{folder_path}")

    # 1. 解析 nfo 文件
    nfo_file_path = os.path.join(folder_path, SCENE_NFO_FILE_NAME)
    logger.info(f"正在解析 nfo 文件 {SCENE_NFO_FILE_NAME}")
    try:
        movie_info = parse_nfo_to_movie(nfo_file_path)
        logger.info(f"nfo 文件解析成功: {movie_info}")
    except Exception as e:
        logger.error(f"nfo 文件解析失败: {e}", exc_info=True)
        return
    movie_code = movie_info.uniqueid

    # 1.2 查找影片源文件
    movie_src_files = find_files_by_code(folder_path, movie_code, SCENE_FILE_SUBFIX)
    if movie_src_files.__len__() == 0:
        logger.warning(f"文件夹内找不到电影源文件!")
        return
    else:
        logger.info(f"已找到电影源文件:{movie_src_files}")
    # 取第一个匹配的文件即可，目前不会有多个
    movie_src_file = movie_src_files[0]

    # 2. 链接数据库
    with Database(STASH_APP_SQLITE_DB_PATH) as dbm:
        failed_item_count = 0
        logger.info(f"已链接数据库 {STASH_APP_SQLITE_DB_PATH}...")
        # 3. 更新标题：根据番号，从 files 表查找匹配的文件记录
        movie_src_file_basename = os.path.basename(movie_src_file)
        logger.info(f"根据文件名[{movie_src_file_basename}],从 files 表查找匹配的文件记录")
        matched_file = dbm.files.get_by_basename(movie_src_file_basename)
        if matched_file is None:
            logger.warning(f"files 数据表无匹配记录，StashApp 可能没扫描文件！")
            return
        logger.info(f"已从数据表中找到匹配文件记录：{matched_file}")

        # 4. 根据之前匹配到的 file 记录，从 scenes-files 映射表查找目标 scene 的记录更新
        scene_file_record = dbm.scenes_files.get_by_file_id(matched_file.id)
        if scene_file_record is None:
            logger.warning(f"scenes_files 表无匹配记录,找不到对应 scene id！")
            return
        else:
            logger.info(f"已从 scenes_files 表找到对应的 scene 记录，scene_id:{scene_file_record.scene_id}")

        # 5. 查找对应的 Scene 记录
        scene_record = dbm.scenes.get_by_id(scene_file_record.scene_id)
        if scene_record is None:
            logger.error(f"scenes 表无匹配记录,找不到对应 scene！")
            return

        # 6. 更新短片标题
        old_title = scene_record.title
        title_update_ret = update_scene_title(dbm.scenes, scene_record, movie_info)
        if title_update_ret:
            logger.info(f"标题更新成功:{old_title} ==> {scene_record.title}")
        else:
            failed_item_count += 1
            logger.warning(f"标题更新失败")

        # 7. 更新短片发布时间
        old_date = scene_record.date
        date_update_ret = update_scene_date(dbm.scenes, scene_record, movie_info)
        if date_update_ret:
            logger.info(f"发行日期更新成功:{old_date} ==> {scene_record.date}")
        else:
            failed_item_count += 1
            logger.warning(f"发行日期更新失败")

        # 8. 更新短片工作室
        old_studio_id = scene_record.studio_id
        studio_update_ret = update_scene_studio(dbm.scenes, dbm.studios, scene_record, movie_info)
        if studio_update_ret:
            logger.info(f"短片工作室更新成功:{old_studio_id} ==> {scene_record.studio_id}")
        else:
            failed_item_count += 1
            logger.warning(f"短片工作室更新失败")

        # 10. 更新标签
        tags_update_ret = update_scene_tags(dbm, scene_record, movie_info)
        if tags_update_ret:
            logger.info(f"短片标签更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"短片标签更新失败")

        # 11. 更新短片剧照(画廊)
        gallery_update_ret = update_scene_gallery(dbm, scene_record, movie_info)
        if gallery_update_ret:
            logger.info(f"短片剧照(画廊)更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"短片剧照(画廊)更新失败")

        # 12. 更新短片导演
        director_update_ret = update_scene_director(dbm, scene_record, movie_info)
        if director_update_ret:
            logger.info(f"导演更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"导演更新失败！")

        # 13. 更新封面图
        cover_img = os.path.join(folder_path, SCENE_COVER_FILE_NAME)
        if os.path.exists(cover_img):
            # 更新短片封面
            logger.info(f"准备使用图片 {cover_img} 更新短片封面...")
            update_ret = update_scene_cover(dbm, scene_record, cover_img)
            logger.info(f"封面更新结果:{update_ret}")

        # 14. 更新工作室代码（番号）
        scene_code_ret = update_scene_code(dbm, scene_record, movie_info)
        if scene_code_ret:
            logger.info(f"更新工作室代码（番号:{movie_info.uniqueid}）更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"更新工作室代码（番号）更新失败")

        # 15. 更新集合（影片系列）
        scene_group_ret = update_scene_groups(dbm, scene_record, movie_info)
        if scene_group_ret:
            logger.info(f"更新集合（影片系列）更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"更新集合（影片系列）更新失败")

        # 16. 更新演员信息
        scene_performer_ret = update_scene_performers(dbm, scene_record, movie_info)
        if scene_performer_ret:
            logger.info(f"演员信息更新成功")
        else:
            failed_item_count += 1
            logger.warning(f"演员信息更新失败")

    logger.info(f"===== 所有工作完成，其中失败项目 {failed_item_count} 件 =====")


def main():
    parser = argparse.ArgumentParser(description='一个处理文件夹内容的命令行工具。')

    # 添加一个必选的位置参数，用于指定文件夹路径
    parser.add_argument(
        'folder_path',
        type=str,
        help='需要处理的文件夹的路径。'
    )

    args = parser.parse_args()

    # 打印解析出的参数
    logger.info(f"接收到的参数：{args}")

    # 调用处理函数
    process_folder(args.folder_path)


def find_nfo_folders_by_time_bfs(start_dir, start_time, end_time=None):
    """
    使用广度优先搜索遍历所有子文件夹。
    若文件夹内包含 .nfo 文件，且该文件的修改时间在指定的区间内，
    则记录该文件夹的完整路径。

    Args:
        start_dir (str): 开始遍历的根目录路径。
        start_time (datetime): 更新时间区间的起始时间 (包含)。
                                必须是 datetime 对象，建议使用时区信息 (如 UTC 或本地时区)。
        end_time (datetime, optional): 更新时间区间的结束时间 (不包含)。
                                       如果为 None，则区间截止到当前时间。默认为 None。

    Returns:
        list: 包含符合时间要求的 .nfo 文件的所有文件夹的完整路径列表。
    """
    # 确保起始路径存在
    if not os.path.isdir(start_dir):
        print(f"错误: 路径 '{start_dir}' 不存在或不是一个目录。")
        return []

    # 统一处理时间区间
    # 如果 end_time 为 None，则设置为当前时间 (并添加时区信息以便比较)
    if end_time is None:
        # 使用 timezone.utc 或 datetime.now(timezone.utc) 是更安全的做法
        # 但为了兼容 getmtime 返回的本地时间戳，我们在这里保持一致性，如果输入没有时区，则假设是本地时间
        end_time = datetime.now()
        if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
            # 假设没有时区信息的输入时间是本地时间
            pass
        else:
            # 如果有明确的时区信息，需要转换为秒级时间戳，os.path.getmtime返回的是本地时间戳
            pass

    # 将 datetime 对象转换为 Unix 时间戳 (秒)
    # 确保所有时间都转换为可比较的 UTC 时间戳，以便与 os.path.getmtime 返回的秒数进行比较
    # 注意：os.path.getmtime 返回的是文件最后修改时间的“时间戳”，它通常是基于操作系统的本地时间。
    # 为了简化，我们直接将输入的 datetime 对象也转换为时间戳进行比较。
    # 注意：如果输入时间带有不同时区，可能需要额外的时区转换步骤来保证精确性。
    try:
        start_timestamp = start_time.timestamp()
        end_timestamp = end_time.timestamp()
    except ValueError as e:
        print(f"时间参数转换错误，请确保 datetime 对象正确: {e}")
        return []

    if start_timestamp >= end_timestamp:
        print("警告: 起始时间必须早于结束时间。")
        return []

    # 结果列表
    result_folders = []

    # 使用双端队列 (deque) 实现 BFS 队列
    queue = deque([start_dir])

    while queue:
        # 1. 弹出当前要处理的文件夹路径（广度优先）
        current_dir = queue.popleft()

        # 2. 检查当前文件夹是否包含符合时间要求的 .nfo 文件
        found_nfo_in_time = False
        try:
            for item in os.listdir(current_dir):
                if item.lower().endswith(".nfo"):
                    item_path = os.path.join(current_dir, item)

                    # 获取【文件夹】的最后修改时间戳
                    mod_timestamp = os.path.getmtime(current_dir)

                    # 检查修改时间是否在区间内： [start_time, end_time)
                    if start_timestamp <= mod_timestamp < end_timestamp:
                        # 找到了符合时间要求的 .nfo 文件
                        result_folders.append(current_dir)
                        found_nfo_in_time = True
                        # 找到后记录路径，并继续遍历下一个节点
                        break  # 跳出内层循环，检查下一个要处理的文件夹

            # 3. 如果当前文件夹没有找到符合要求的 .nfo 文件，则遍历其子文件夹并加入队列
            if not found_nfo_in_time:
                for item in os.listdir(current_dir):
                    item_path = os.path.join(current_dir, item)
                    # 检查是否是目录
                    if os.path.isdir(item_path):
                        # 将子目录加入队列，等待后续处理
                        queue.append(item_path)

        except PermissionError:
            print(f"警告: 没有权限访问 '{current_dir}'，已跳过。")
        except FileNotFoundError:
            # 路径在检查时可能被删除
            pass
        except Exception as e:
            print(f"处理 '{current_dir}' 时发生错误: {e}")

    return result_folders


if __name__ == '__main__':
    # main()
    process_folder(
        '/Users/hoholiday/Downloads/outputs/#整理完成/八掛うみ/[ABF-193] 今日も清楚ぶって看護師してます。 八掛うみ【限定特典映像30分付き】')
