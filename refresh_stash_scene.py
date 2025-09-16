import argparse
import hashlib
import os
import shutil
from typing import List

from MyLogger import CustomLogger
from db.db_manager import Database
from models.scenes_dao import ScenesDAO, Scenes
from models.studios_dao import StudiosDAO, Studios
from nfo.nfo_parser import parse_nfo_to_movie, Movie

# 初始化日志记录器
logger = CustomLogger(__name__)

# StashApp 存储 blobs 的路径
STASH_APP_BLOBS_DIRS = '/Users/hoholiday/Downloads/stashapp/blobs'
# StashApp 数据库文件路径
STASH_APP_SQLITE_DB_PATH = '/Users/hoholiday/Projects/Python/StashAppManager/models/stash-go-dev.sqlite'
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


def update_cover(scenes_dao: ScenesDAO, img_path: str):
    """
    更新短片封面
    :param img_path: 封面图路径
    :return: 更新结果
    """
    # 1. 检查文件是否存在
    if not os.path.isfile(img_path):
        logger.warning(f"错误：文件不存在或不是有效文件 -> {img_path}")
        return False

    # 2. 解析文件 md5, 32 位
    hash_md5 = ''
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

    # 4. 用file_md5 更新 scenes 表中的 cover 字段

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

def update_scene_gallery():
    pass

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
            logger.warning(f"标题更新失败")

        # 7. 更新短片发布时间
        old_date = scene_record.date
        date_update_ret = update_scene_date(dbm.scenes, scene_record, movie_info)
        if date_update_ret:
            logger.info(f"发行日期更新成功:{old_date} ==> {scene_record.date}")
        else:
            logger.warning(f"发行日期更新失败")

        # 8. 更新短片工作室
        old_studio_id = scene_record.studio_id
        studio_update_ret = update_scene_studio(dbm.scenes, dbm.studios, scene_record, movie_info)
        if studio_update_ret:
            logger.info(f"短片工作室更新成功:{old_studio_id} ==> {scene_record.studio_id}")
        else:
            logger.warning(f"短片工作室更新失败")


        # 9. 更新短片剧照(画廊)
        scene_gallery_update_ret = update_scene_gallery()

        # 2. 更新封面图
        cover_img = os.path.join(folder_path, SCENE_COVER_FILE_NAME)
        if os.path.exists(cover_img):
            # 更新短片封面
            logger.info(f"准备使用图片 {cover_img} 更新短片封面...")
            update_ret = update_cover(dbm.scenes, cover_img)
            logger.info(f"封面更新结果:{update_ret}")


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


if __name__ == '__main__':
    # main()
    process_folder(
        '/Users/hoholiday/Downloads/outputs/#整理完成/八掛うみ/[ABF-193] 今日も清楚ぶって看護師してます。 八掛うみ【限定特典映像30分付き】')
