from __future__ import annotations

import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"


class LandingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.duplicates: set[str] = set()
        self.hrefs: list[str] = []
        self.config_keys: list[str] = []
        self.in_code_panel = False
        self.in_hero_code = False
        self.hero_code: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        element_id = values.get("id")
        if element_id is not None:
            if element_id in self.ids:
                self.duplicates.add(element_id)
            self.ids.add(element_id)
        href = values.get("href")
        if tag == "a" and href is not None:
            self.hrefs.append(href)
        config_key = values.get("data-config-link")
        if config_key is not None:
            self.config_keys.append(config_key)
        classes = (values.get("class") or "").split()
        if "code-panel" in classes and not self.hero_code:
            self.in_code_panel = True
        if tag == "code" and self.in_code_panel:
            self.in_hero_code = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "code" and self.in_hero_code:
            self.in_hero_code = False
            self.in_code_panel = False

    def handle_data(self, data: str) -> None:
        if self.in_hero_code:
            self.hero_code.append(data)


class LandingContractTest(unittest.TestCase):
    def test_page_uses_working_sdk_and_cli_surface(self) -> None:
        html = (LANDING / "index.html").read_text()
        parser = LandingParser()
        parser.feed(html)

        self.assertFalse(parser.duplicates)
        for href in parser.hrefs:
            if href.startswith("#") and href != "#":
                self.assertIn(href[1:], parser.ids)

        config = (LANDING / "config.js").read_text()
        for key in parser.config_keys:
            self.assertIn(f"{key}:", config)

        hero_code = "".join(parser.hero_code)
        compile(hero_code, "landing/shopping.py", "exec")
        self.assertIn('streaming.versioned("catalog", key="product_id")', hero_code)
        self.assertIn("await catalog.get", hero_code)
        self.assertIn('@streaming.process(key="user_id")', hero_code)

        self.assertNotIn("streaming.wait_until", html)
        self.assertNotIn("highwater dev", html)
        self.assertNotIn("highwater send", html)
        self.assertIn("pip install highwater", html)
        self.assertIn("Book a technical review", html)
        self.assertIn('data-config-link="earlyAccess"', html)
        self.assertIn("10,000 products", html)
        self.assertIn("53,638 events per second", html)


if __name__ == "__main__":
    unittest.main()
