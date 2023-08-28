import abc
import re

from colorama import Back, just_fix_windows_console

# example: match = [numMatch]...[/numMatch]
STYLING_CODES = r"\[(numMatch|numDiff)\](.*?)\[/\1\]"


class Formatter(abc.ABC):
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


class AnsiColorFormatter(Formatter):
    def __init__(self):
        super().__init__()
        just_fix_windows_console()

    def apply_formatting(self, code: str, inner: str) -> str:
        if code == "numDiff":
            return f"{Back.CYAN}{inner}{Back.RESET}"
        else:
            return inner
