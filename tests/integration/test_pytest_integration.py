def test_formatter():
    import os
    import subprocess

    cwd = os.path.dirname(os.path.abspath(__file__))
    output = subprocess.run(
        ["pytest", "-s", "pytest_testfile.py", "--color", "yes"],
        cwd=cwd,
        stdout=subprocess.PIPE,
    )
    assert (
        "AssertionError: This is a \x1b[46mstylized\x1b[49m failure message"
        in output.stdout.decode("utf-8")
    )

    output = subprocess.run(
        ["pytest", "-s", "pytest_testfile.py", "--color", "no"],
        cwd=cwd,
        stdout=subprocess.PIPE,
    )
    assert "AssertionError: This is a stylized failure message" in output.stdout.decode(
        "utf-8"
    )
