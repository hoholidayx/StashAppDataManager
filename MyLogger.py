import logging


class CustomLogger:
    """
    一个用于简化日志配置和使用的自定义日志类。
    """

    def __init__(self, name, level=logging.INFO, log_file=None):
        """
        初始化 CustomLogger 实例。

        参数:
            name (str): 日志记录器的名称，通常是模块名或应用名。
            level (int): 日志级别 (e.g., logging.DEBUG, logging.INFO)。
            log_file (str): 可选，日志文件的路径。如果指定，日志将同时写入文件。
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()  # 防止重复添加处理器

        # 创建并配置控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 如果指定了日志文件，创建并配置文件处理器
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, msg, *args, **kwargs):
        """
        记录 DEBUG 级别的日志。
        """
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        记录 INFO 级别的日志。
        """
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        记录 WARNING 级别的日志。
        """
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        记录 ERROR 级别的日志。
        """
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        记录 CRITICAL 级别的日志。
        """
        self.logger.critical(msg, *args, **kwargs)
