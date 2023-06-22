import abc
import re

from colorama import Back

STYLING_CODES = r"\[(numMatch|numDiff)\](.*?)\[/\1\]"


class Formatter(abc.ABC):
    @abc.abstractmethod
    def fmt_str(self, result: str) -> str:
        pass


class DefaultFormatter(Formatter):
    def __init__(self):
        self.known_bb_pattern = re.compile(STYLING_CODES)

    # Just ignore styling in the default formatter
    def apply_formatting(self, _: str, inner: str) -> str:
        return inner

    def fmt_str(self, string: str) -> str:
        # Replace codes with platform specific styling
        string = self.known_bb_pattern.sub(
            lambda m: self.apply_formatting(m.group(1), m.group(2)), string
        )

        return string


class AnsiColorFormatter(DefaultFormatter):
    def apply_formatting(self, code: str, inner: str) -> str:
        if code == "numDiff":
            return f"{Back.CYAN}{inner}{Back.RESET}"
        else:
            return inner


class HtmlFormatter(DefaultFormatter):
    def apply_formatting(self, code: str, inner: str) -> str:
        if code == "numDiff":
            return f"<span style='background-color: #FF0000; color: #FFFFFF'>{inner}</span>"
        elif code == "numMatch":
            return f"<span style='background-color: #00FF00; color: #FFFFFF'>{inner}</span>"
        else:
            return inner
