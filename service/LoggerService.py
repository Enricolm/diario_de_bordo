import logging

class LoggerService:
    def __init__(self, logger_name: str, console_level=logging.INFO):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(console_level)
            handler.setFormatter(logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            self.logger.addHandler(handler)

    def info(self, mensagem: str):
        self.logger.info(mensagem)

    def warning(self, mensagem: str):
        self.logger.warning(mensagem)

    def error(self, mensagem: str, exc_info=False):
        self.logger.error(mensagem, exc_info=exc_info)

    def debug(self, mensagem: str):
        self.logger.debug(mensagem)
