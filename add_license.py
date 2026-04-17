# Copyright (C) 2026  Noah Ross
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from pathlib import Path

HEADER_LINES = [
    "Copyright (C) 2026  Noah Ross",
    "",
    "This program is free software: you can redistribute it and/or modify",
    "it under the terms of the GNU General Public License as published by",
    "the Free Software Foundation, either version 3 of the License, or",
    "(at your option) any later version.",
    "",
    "This program is distributed in the hope that it will be useful,",
    "but WITHOUT ANY WARRANTY; without even the implied warranty of",
    "MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the",
    "GNU General Public License for more details.",
    "",
    "You should have received a copy of the GNU General Public License",
    "along with this program.  If not, see <https://www.gnu.org/licenses/>.",
]

RUST_HEADER = "\n".join(f"// {line}".rstrip() for line in HEADER_LINES) + "\n\n"
MARKER = "Copyright (C) 2026  Noah Ross"


def main() -> None:
    src = Path(__file__).parent / "src"
    for path in sorted(src.rglob("*.rs")):
        original = path.read_text()
        if MARKER in original.splitlines()[0:20] or MARKER in original[:2000]:
            print(f"skip (already licensed): {path}")
            continue
        path.write_text(RUST_HEADER + original)
        print(f"licensed: {path}")


if __name__ == "__main__":
    main()
