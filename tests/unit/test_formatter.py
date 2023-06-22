from colorama import Back

from datajudge.formatter import AnsiColorFormatter, DefaultFormatter, HtmlFormatter


def test_default_formatter():
    formatter = DefaultFormatter()

    assert formatter.fmt_str("[numDiff]Hello[/numDiff]") == "Hello"
    assert formatter.fmt_str("[numMatch]Hello[/numMatch]") == "Hello"
    assert formatter.fmt_str("[b]Hello[/b]") == "[b]Hello[/b]"
    assert formatter.fmt_str("[numDiff]Hello[/numMatch]") == "[numDiff]Hello[/numMatch]"


def test_ansi_color_formatter():
    formatter = AnsiColorFormatter()

    assert (
        formatter.fmt_str("[numDiff]Hello[/numDiff]") == f"{Back.CYAN}Hello{Back.RESET}"
    )
    assert formatter.fmt_str("[numMatch]Hello[/numMatch]") == "Hello"
    assert formatter.fmt_str("[b]Hello[/b]") == "[b]Hello[/b]"
    assert formatter.fmt_str("[numDiff]Hello[/numMatch]") == "[numDiff]Hello[/numMatch]"


def test_html_formatter():
    formatter = HtmlFormatter()

    assert formatter.fmt_str("[numDiff]Hello[/numDiff]") == (
        "<span style='background-color: #FF0000; color: #FFFFFF'>Hello</span>"
    )
    assert formatter.fmt_str("[numMatch]Hello[/numMatch]") == (
        "<span style='background-color: #00FF00; color: #FFFFFF'>Hello</span>"
    )
    assert formatter.fmt_str("[b]Hello[/b]") == "[b]Hello[/b]"
    assert formatter.fmt_str("[numDiff]Hello[/numMatch]") == "[numDiff]Hello[/numMatch]"
