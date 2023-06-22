from colorama import Fore, Style

from datajudge.formatter import AnsiColorFormatter, DefaultFormatter, HtmlFormatter


def test_default_formatter():
    formatter = DefaultFormatter()

    assert formatter.fmt_str("[b]Hello[/b]") == "Hello"
    assert formatter.fmt_str("[red]Hello[/red]") == "Hello"
    assert formatter.fmt_str("[red]Hello[/blue]") == "[red]Hello[/blue]"
    assert formatter.fmt_str("[red]Hello[/red] [blue]World[/blue]") == "Hello World"
    assert formatter.fmt_str("[]hello[/]") == "hello"


def test_ansi_color_formatter():
    formatter = AnsiColorFormatter()

    # Test color formatting
    assert formatter.fmt_str("[red]Hello[/red]") == f"{Fore.RED}Hello{Style.RESET_ALL}"
    assert formatter.fmt_str("[red]Hello[/blue]") == "[red]Hello[/blue]"

    # Test BB strip functionality
    assert formatter.fmt_str("[invalid]Hello[/invalid]") == "Hello"


def test_html_formatter():
    formatter = HtmlFormatter()

    assert (
        formatter.fmt_str("[red]Hello[/red]") == "<span style='color:red'>Hello</span>"
    )
    assert formatter.fmt_str("[red]Hello[/blue]") == "[red]Hello[/blue]"

    # Test BB strip functionality
    assert formatter.fmt_str("[invalid]Hello[/invalid]") == "Hello"
