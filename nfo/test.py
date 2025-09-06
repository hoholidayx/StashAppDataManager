from nfo.nfo_parser import parse_nfo_to_movie

if __name__ == '__main__':
    # 假设您的NFO文件名为 movie.nfo
    nfo_file_path = 'movie.nfo'

    try:
        movie_info = parse_nfo_to_movie(nfo_file_path)

        # 访问明确的字段
        print("--- 电影信息 ---")
        print(f"标题: {movie_info.title}")
        print(f"片长: {movie_info.runtime} 分钟")
        print(f"唯一ID: {movie_info.uniqueid}")
        print(f"上映日期: {movie_info.premiered}")
        print(f"制片商: {movie_info.studio}")
        print(f"国家: {movie_info.country}")
        print(f"分级: {movie_info.mpaa}")
        print(f"流派: {', '.join(movie_info.genres)}")
        print(f"标签: {', '.join(movie_info.tags)}")

        print("\n--- 演员列表 ---")
        for actor in movie_info.actors:
            print(f"姓名: {actor.name}")
            print(f"头像: {actor.thumb}")

    except Exception as e:
        print(f"程序执行失败: {e}")
