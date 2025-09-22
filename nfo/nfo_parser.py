import xml.etree.ElementTree as ET
from typing import List, Optional


class Actor:
    """
    封装演员信息的结构体。
    """

    def __init__(self, name: str, thumb: Optional[str] = None):
        self.name = name
        self.thumb = thumb

    def __repr__(self) -> str:
        return f"Actor(name='{self.name}', thumb='{self.thumb}')"


class Movie:
    """
    封装电影信息的结构体，所有字段都已明确定义。
    """

    def __init__(self,
                 title: Optional[str],
                 runtime: int,
                 mpaa: Optional[str],
                 uniqueid: Optional[str],
                 genres: List[str],
                 tags: List[str],
                 country: Optional[str],
                 premiered: Optional[str],
                 studio: Optional[str],
                 actors: List[Actor],
                 director: Optional[str]):
        self.title = title
        self.runtime = runtime
        self.mpaa = mpaa
        self.uniqueid = uniqueid
        self.genres = genres
        self.tags = tags
        self.country = country
        self.premiered = premiered
        self.studio = studio
        self.actors = actors
        self.director = director

    def __repr__(self) -> str:
        return (f"Movie(title='{self.title}', uniqueid='{self.uniqueid}'"
                f", actors='{self.actors}')")


class MovieBuilder:
    """
    电影对象的构建器，用于逐步设置属性并构建Movie实例。
    """

    def __init__(self):
        self._title: Optional[str] = None
        self._runtime: int = 0
        self._mpaa: Optional[str] = None
        self._uniqueid: Optional[str] = None
        self._genres: List[str] = []
        self._tags: List[str] = []
        self._country: Optional[str] = None
        self._premiered: Optional[str] = None
        self._studio: Optional[str] = None
        self._actors: List[Actor] = []
        self.director: Optional[str] = None

    def set_title(self, value: Optional[str]) -> 'MovieBuilder':
        self._title = value
        return self

    def set_runtime(self, value: int) -> 'MovieBuilder':
        self._runtime = value
        return self

    def set_mpaa(self, value: Optional[str]) -> 'MovieBuilder':
        self._mpaa = value
        return self

    def set_uniqueid(self, value: Optional[str]) -> 'MovieBuilder':
        self._uniqueid = value
        return self

    def set_genres(self, value: List[str]) -> 'MovieBuilder':
        self._genres = value
        return self

    def set_tags(self, value: List[str]) -> 'MovieBuilder':
        self._tags = value
        return self

    def set_country(self, value: Optional[str]) -> 'MovieBuilder':
        self._country = value
        return self

    def set_premiered(self, value: Optional[str]) -> 'MovieBuilder':
        self._premiered = value
        return self

    def set_studio(self, value: Optional[str]) -> 'MovieBuilder':
        self._studio = value
        return self

    def set_actors(self, value: List[Actor]) -> 'MovieBuilder':
        self._actors = value
        return self

    def set_director(self, value: Optional[str]) -> 'MovieBuilder':
        self.director = value
        return self

    def build(self) -> Movie:
        """
        构建并返回一个Movie实例。
        """
        return Movie(
            title=self._title,
            runtime=self._runtime,
            mpaa=self._mpaa,
            uniqueid=self._uniqueid,
            genres=self._genres,
            tags=self._tags,
            country=self._country,
            premiered=self._premiered,
            studio=self._studio,
            actors=self._actors,
            director=self.director
        )


def parse_nfo_to_movie(file_path: str) -> Movie:
    """
    从NFO文件路径中读取、解析并构建Movie对象。
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        builder = MovieBuilder()

        # 逐个解析字段并使用构建器设置
        title_elem = root.find('title')
        builder.set_title(title_elem.text if title_elem is not None else None)

        runtime_elem = root.find('runtime')
        try:
            runtime = int(runtime_elem.text) if runtime_elem is not None and runtime_elem.text else 0
        except (ValueError, TypeError):
            runtime = 0
        builder.set_runtime(runtime)

        uniqueid_elem = root.find('uniqueid')
        builder.set_uniqueid(uniqueid_elem.text if uniqueid_elem is not None else None)

        # 解析列表字段
        genres = [genre.text for genre in root.findall('genre') if genre.text is not None]
        builder.set_genres(genres)

        tags = [tag.text for tag in root.findall('tag') if tag.text is not None]
        builder.set_tags(tags)

        # 解析演员信息
        actors = []
        for actor_elem in root.findall('actor'):
            name_elem = actor_elem.find('name')
            thumb_elem = actor_elem.find('thumb')
            name = name_elem.text if name_elem is not None else 'Unknown Actor'
            thumb = thumb_elem.text if thumb_elem is not None else None
            actors.append(Actor(name=name, thumb=thumb))
        builder.set_actors(actors)

        # 解析其他简单字段
        builder.set_mpaa(root.find('mpaa').text if root.find('mpaa') is not None else None)
        builder.set_country(root.find('country').text if root.find('country') is not None else None)
        builder.set_premiered(root.find('premiered').text if root.find('premiered') is not None else None)
        builder.set_studio(root.find('studio').text if root.find('studio') is not None else None)
        builder.set_director(root.find('director').text if root.find('director') is not None else None)

        return builder.build()

    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The file '{file_path}' was not found.")
    except ET.ParseError:
        raise ValueError(f"Error: Failed to parse XML from '{file_path}'.")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred: {e}")
