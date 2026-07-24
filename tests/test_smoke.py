"""Browser smoke test — the one thing the unit and integration suites can't cover.

Everything else tests Kafka logic against the broker directly. This drives a real
browser through the rendered HTML and HTMX/SSE flows, catching the class of bug that
never shows up server-side: a broken hx-post target, a template variable that renders
empty, a JavaScript error that stops the scan stream.

It needs a *running app* (not just a broker), so it is gated the same way the
integration tests are gated on a broker: if BASE_URL is unreachable, the whole module
skips. It is also excluded from the default `pytest` run (marker `smoke`) because it
needs a browser; run it explicitly with ./run-smoke.sh.

Safety: it only ever touches its own throwaway topic (playwright-smoke-tmp) and deletes
it again over HTTP in a finally, so a mid-test failure can't leave litter and it never
goes near a real topic.
"""

import os
import time

import httpx
import pytest

pytest.importorskip("playwright", reason="playwright not installed; see run-smoke.sh")
from playwright.sync_api import Page, expect  # noqa: E402

BASE_URL = os.environ.get("KAFKASCOPE_BASE_URL", "http://localhost:8080").rstrip("/")
# The cluster to drive; pages live under /c/<cluster>/. Defaults to the bundled stack's.
CLUSTER = os.environ.get("KAFKASCOPE_CLUSTER", "local")
BASE = f"{BASE_URL}/c/{CLUSTER}"
TOPIC = "playwright-smoke-tmp"
# A value unique to this run so the search assertion can't match a stale message.
MARKER = f"smoke-{os.getpid()}-{int(time.time())}"


def _app_reachable() -> bool:
    try:
        return httpx.get(f"{BASE_URL}/healthz", timeout=3).status_code == 200
    except Exception:
        return False


APP_UP = _app_reachable()

pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(not APP_UP, reason=f"no kafkascope app reachable at {BASE_URL}"),
]


def _delete_topic_over_http() -> None:
    """Best-effort teardown that doesn't need broker access — post the delete form."""
    try:
        httpx.post(
            f"{BASE}/topic/{TOPIC}/delete", data={"confirm": TOPIC}, timeout=10
        )
    except Exception:
        pass


@pytest.fixture
def clean_topic():
    _delete_topic_over_http()
    yield
    _delete_topic_over_http()


def test_golden_path(page: Page, clean_topic):
    """Overview → create → detail → produce → search(SSE) → groups → delete.

    One ordered walk rather than many tests: each step is a precondition for the next,
    and a smoke test wants to prove the whole path holds together, not isolated units.
    """

    # --- root redirects to the default cluster's overview ---
    page.goto(f"{BASE_URL}/")
    expect(page.get_by_role("heading", name="Cluster")).to_be_visible()
    expect(page.locator("#topics")).to_be_visible()

    # --- theme toggle: flips light/dark, restyles, and persists the choice ---
    initial_theme = page.get_attribute("html", "data-theme")
    bg_before = page.evaluate("getComputedStyle(document.body).backgroundColor")
    page.click("#theme-toggle")
    flipped_theme = page.get_attribute("html", "data-theme")
    assert flipped_theme and flipped_theme != initial_theme, "theme did not flip"
    assert page.evaluate("getComputedStyle(document.body).backgroundColor") != bg_before, (
        "background colour did not change with the theme"
    )
    assert page.evaluate("localStorage.getItem('kafkascope-theme')") == flipped_theme
    page.click("#theme-toggle")  # back to the original theme for the rest of the run

    # --- create the throwaway topic through the UI ---
    page.goto(f"{BASE}/topics/new")
    page.fill("input[name=name]", TOPIC)
    page.fill("input[name=partitions]", "1")
    page.click("button:has-text('Create topic')")
    expect(page.locator("#result .status.ok")).to_contain_text("Created", timeout=15000)

    # --- topic detail shows the partition table ---
    page.goto(f"{BASE}/topic/{TOPIC}")
    expect(page.get_by_role("heading", name=TOPIC)).to_be_visible()
    expect(page.get_by_role("heading", name="Partitions", exact=True)).to_be_visible()

    # --- produce a message carrying the unique marker; wait for the delivery report ---
    page.goto(f"{BASE}/topic/{TOPIC}/produce")
    page.fill("input[name=key]", "smoke-key")
    page.fill("textarea[name=value]", f'{{"marker": "{MARKER}"}}')
    page.click("button:has-text('Send')")
    delivered = page.locator("#result .status.ok")
    expect(delivered).to_contain_text("Delivered", timeout=15000)
    expect(delivered).to_contain_text("@")  # a real partition@offset coordinate

    # --- the message browser auto-scans on load; the marker must stream back via SSE ---
    page.goto(f"{BASE}/topic/{TOPIC}/messages")
    expect(page.locator("article.msg").filter(has_text=MARKER)).to_be_visible(
        timeout=20000
    )
    # the status line settles on a completed scan, not a spinner
    expect(page.locator("#status")).to_contain_text("Scanned", timeout=20000)

    # --- live tail: new messages stream in and the view auto-scrolls to the newest ---
    page.click("#tail-btn")
    page.wait_for_timeout(2500)  # let the tail consumer assign at the current end offset
    tail_marker = f"{MARKER}-tail"
    for i in range(20):  # enough to make the page taller than the viewport
        page.request.post(
            f"{BASE}/topic/{TOPIC}/produce",
            form={"key": f"t{i}", "value": f"seq-{i}-{tail_marker}"},
        )
    # the newest message must arrive via the tail stream...
    expect(page.locator("article.msg").filter(has_text=f"seq-19-{tail_marker}")).to_be_visible(
        timeout=20000
    )
    # ...and the page must have auto-scrolled down to keep it in view
    scrolled = page.evaluate(
        "window.scrollY > 0 && (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 200"
    )
    assert scrolled, "live tail did not auto-scroll to the newest message"
    page.click("#stop-btn")

    # --- consumer groups page renders (no group required to exist) ---
    page.goto(f"{BASE}/groups")
    expect(page.get_by_role("heading", name="Consumer groups")).to_be_visible()

    # --- the delete guardrail: a wrong confirmation is refused, the right one works ---
    page.goto(f"{BASE}/topic/{TOPIC}")
    delete_form = page.locator("form.danger", has=page.get_by_role("heading", name="Delete topic"))
    delete_form.locator("input[name=confirm]").fill("WRONG")
    delete_form.get_by_role("button", name="Delete topic").click()
    expect(page.locator("#result .status.error")).to_contain_text(
        "Type the topic name", timeout=15000
    )

    delete_form.locator("input[name=confirm]").fill(TOPIC)
    delete_form.get_by_role("button", name="Delete topic").click()
    expect(page.locator("#result .status.ok")).to_contain_text(
        "Deleted topic", timeout=15000
    )
