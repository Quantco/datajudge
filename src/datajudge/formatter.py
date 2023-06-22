import abc
import re

from colorama import Fore, Style

COLOR_REGEX = r"\[(red|green|yellow|blue|magenta|cyan|white|black)\](.*?)\[/\1\]"
ALL_BB_REGEX = r"\[([a-zA-Z]*)\](.*)\[/\1\]"


class Formatter(abc.ABC):
    @abc.abstractmethod
    def fmt_str(self, result: str) -> str:
        pass


class DefaultFormatter(Formatter):
    def __init__(self):
        self.bb_pattern = re.compile(ALL_BB_REGEX)

    def strip_bb(self, string: str) -> str:
        return self.bb_pattern.sub(lambda m: m.group(2), string)

    def fmt_str(self, string: str) -> str:
        return self.strip_bb(string)


class AnsiColorFormatter(DefaultFormatter):
    def __init__(self):
        self.color_patterns = re.compile(COLOR_REGEX)
        super().__init__()

    def fmt_str(self, string: str) -> str:
        # Replace bb color codes with colorama codes
        string = self.color_patterns.sub(
            lambda m: getattr(Fore, m.group(1).upper()) + m.group(2) + Style.RESET_ALL,
            string,
        )

        # Stip all remaining bb code
        return self.strip_bb(string)


class HtmlFormatter(DefaultFormatter):
    def __init__(self):
        self.color_patterns = re.compile(COLOR_REGEX)
        super().__init__()

    def fmt_str(self, string: str) -> str:
        # Replace bb color codes with html color tags
        string = self.color_patterns.sub(
            lambda m: f"<span style='color:{m.group(1)}'>{m.group(2)}</span>",
            string,
        )

        # Stip all remaining bb code
        return self.strip_bb(string)
