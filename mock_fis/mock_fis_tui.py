#!/usr/bin/env python3
"""Retro terminal control center for Mock FIS.

Launches mock_fis.py as a subprocess, streams its output, and provides
admin action buttons and live file-state monitoring.

Usage:
    python mock_fis/mock_fis_tui.py
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import httpx
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.events import Click, Key
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Button, DataTable, Footer, Header, RichLog, Static, Switch

# ── Config (mirrors mock_fis.py env var defaults) ────────────────────────────
PORT = int(os.getenv("MOCK_FIS_PORT", "8000"))
BASE_URL = f"http://localhost:{PORT}"
CHAOS_MODE = os.getenv("MOCK_FIS_CHAOS", "true").lower() in ("1", "true", "yes")
CHAOS_CORRUPT = os.getenv("MOCK_FIS_CHAOS_CORRUPT", "true").lower() in (
    "1",
    "true",
    "yes",
)
CHAOS_PROBABILITY = float(os.getenv("MOCK_FIS_CHAOS_PROBABILITY", "0.5"))
AUTO_ARCHIVE = os.getenv("MOCK_FIS_AUTO_ARCHIVE", "false").lower() in (
    "1",
    "true",
    "yes",
)
S3_ENDPOINT = os.getenv("MOCK_FIS_S3_ENDPOINT", "http://localstack:4566")
INBOX_BUCKET = os.getenv("MOCK_FIS_INBOX_BUCKET", "inbox1")
STORAGE_ALIAS = os.getenv("MOCK_FIS_STORAGE_ALIAS", "TESTHUB")
FILE_SIZE_MB = int(os.getenv("MOCK_FIS_FILE_SIZE_MB", "6"))
PART_SIZE_MB = int(os.getenv("MOCK_FIS_PART_SIZE_MB", "5"))

SCRIPT_DIR = Path(__file__).parent
MOCK_FIS_SCRIPT = SCRIPT_DIR / "mock_fis.py"


# ── Log line colorizer ────────────────────────────────────────────────────────

_LEVEL_COLORS_UVICORN = {
    "INFO": "bold bright_cyan",
    "WARNING": "bold bright_yellow",
    "ERROR": "bold bright_red",
    "DEBUG": "dim #777788",
    "CRITICAL": "bold white on red",
}

_LEVEL_COLORS_LOGGING = {
    "INFO": "bold bright_green",
    "WARNING": "bold bright_yellow",
    "WARN": "bold bright_yellow",
    "ERROR": "bold bright_red",
    "DEBUG": "dim #777788",
    "CRITICAL": "bold white on red",
}


def _colorize(line: str) -> Text:
    """Parse a uvicorn/logging output line and apply retro terminal colors."""
    stripped = line.strip()
    if not stripped:
        return Text("")

    text = Text(overflow="fold")
    colon_pos = stripped.find(":")

    # uvicorn format: "LEVEL:     message"
    if 0 < colon_pos <= 8 and stripped[:colon_pos] in _LEVEL_COLORS_UVICORN:
        level = stripped[:colon_pos]
        rest = stripped[colon_pos + 1 :].strip()
        text.append(f"{level:<8}", style=_LEVEL_COLORS_UVICORN[level])
        text.append("  ")
        _colorize_message(text, rest)
        return text

    # Standard logging format: "YYYY-MM-DD HH:MM:SS,mmm LEVEL    name: message"
    parts = stripped.split(" ", 3)
    if len(parts) == 4 and len(parts[0]) == 10 and parts[0][4] == "-":
        date_str, time_str, level_raw, rest = parts
        level = level_raw.strip()
        text.append(f"{date_str} {time_str}", style="dim #4a5568")
        text.append(" ")
        text.append(f"{level:<8}", style=_LEVEL_COLORS_LOGGING.get(level, "white"))
        text.append(" ")
        _colorize_message(text, rest)
        return text

    # Fallback
    _colorize_message(text, stripped)
    return text


def _colorize_message(text: Text, msg: str) -> None:
    mu = msg.upper()
    if "CHAOS" in mu:
        text.append(msg, style="bold #cc66ff")
    elif any(k in mu for k in ("ERROR", "FAIL", "EXCEPTION", "TRACEBACK")):
        text.append(msg, style="bright_red")
    elif "WARN" in mu:
        text.append(msg, style="bright_yellow")
    elif any(
        k in mu for k in ("READY", "SEED", "SUCCESS", "STARTED", "COMPLETE", "ARCHIV")
    ):
        text.append(msg, style="bright_green")
    elif any(k in mu for k in ("SHUTDOWN", "STOPPING", "TERMINAT", "EXIT")):
        text.append(msg, style="dim yellow")
    else:
        text.append(msg, style="#b0c4d8")


# ── CSS ───────────────────────────────────────────────────────────────────────

APP_CSS = """
Screen {
    background: #07071a;
}

Header {
    background: #0d0d30;
    color: #00ffcc;
    text-style: bold;
    height: 1;
}

Footer {
    background: #0d0d30;
    color: #007799;
    height: 1;
}

Footer > .footer--highlight {
    background: #003355;
    color: #00aacc;
    text-style: bold;
}

Footer > .footer--key {
    color: #00aacc;
    text-style: bold;
}

/* ── Top row ── */
#top-row {
    height: auto;
    min-height: 14;
}

/* ── Left column (settings + chaos stacked) ── */
#left-col {
    width: 44;
    height: auto;
}

/* ── Settings panel ── */
SettingsPanel {
    border: heavy #005577;
    background: #08081e;
    width: 100%;
    height: auto;
    padding: 0 1 1 1;
}

#settings-title {
    background: #0d0d2e;
    color: #ffcc00;
    text-style: bold;
    text-align: center;
    padding: 0 1;
    width: 100%;
    margin-bottom: 1;
}

#settings-body {
    color: #88aabb;
    padding: 0 1;
}

/* ── Chaos panel ── */
ChaosPanel {
    border: heavy #440055;
    background: #0e0818;
    width: 100%;
    height: auto;
    padding: 0 1 1 1;
}

#chaos-title {
    background: #1a0a2e;
    color: #cc66ff;
    text-style: bold;
    text-align: center;
    padding: 0 1;
    width: 100%;
    margin-bottom: 0;
}

.chaos-row {
    height: 3;
    align: left middle;
    padding: 0 1;
}

.chaos-label {
    width: 16;
    color: #997799;
    content-align: left middle;
}

Switch {
    margin: 0 1;
    background: #1a0022;
    border: round #550066;
    border-top: round;
    border-left: round;
    border-right: round;
    border-bottom: round;
}

Switch:hover {
    border: round #9900cc;
}

Switch:focus {
    border: round #cc44ff;
}

Switch.-on {
    background: #0a001a;
}

/* ── Probability slider row ── */
#prob-row {
    height: 3;
    align: left middle;
    padding: 0 1;
}

HorizontalSlider {
    height: 1;
    width: 1fr;
    background: #0e0818;
    padding: 0 1;
    margin: 0 1;
}

HorizontalSlider:focus {
    background: #150d24;
}

#prob-value {
    width: 5;
    color: #ffcc00;
    text-style: bold;
    content-align: right middle;
}

/* ── Admin panel ── */
AdminPanel {
    border: heavy #005577;
    background: #08081e;
    height: auto;
    padding: 0 1 1 1;
}

#admin-title {
    background: #0d0d2e;
    color: #ffcc00;
    text-style: bold;
    text-align: center;
    padding: 0 1;
    width: 100%;
    margin-bottom: 0;
}

#btn-row-1, #btn-row-2 {
    height: 5;
    align: left middle;
    padding: 0 1;
}

Button {
    margin: 0 1;
    min-width: 20;
    height: 3;
    text-style: bold;
}

.btn-reseed {
    background: #002244;
    color: #33ccff;
    border: heavy #0055aa;
}

.btn-reseed:hover {
    background: #003366;
    color: #ffffff;
    border: heavy #0088ee;
}

.btn-archive {
    background: #002211;
    color: #33ff88;
    border: heavy #007733;
}

.btn-archive:hover {
    background: #003322;
    color: #ffffff;
    border: heavy #00aa55;
}

.btn-clear {
    background: #220011;
    color: #ff4455;
    border: heavy #880022;
}

.btn-clear:hover {
    background: #330022;
    color: #ffffff;
    border: heavy #bb0033;
}

.btn-refresh {
    background: #1a1a00;
    color: #cccc33;
    border: heavy #666600;
}

.btn-refresh:hover {
    background: #2a2a00;
    color: #ffffff;
    border: heavy #999900;
}

/* ── Status bar ── */
ServerStatusBar {
    height: 1;
    background: #0a0a22;
    border-top: solid #003344;
    border-bottom: solid #003344;
    padding: 0 2;
}

/* ── Inbox panel ── */
InboxPanel {
    border: heavy #005577;
    background: #08081e;
    height: 12;
    padding: 0 1 1 1;
}

#inbox-title {
    background: #0d0d2e;
    color: #ffcc00;
    text-style: bold;
    text-align: center;
    padding: 0 1;
    width: 100%;
}

DataTable {
    background: #08081e;
    color: #aaccdd;
    height: 1fr;
}

DataTable > .datatable--header {
    background: #001133;
    color: #00ddff;
    text-style: bold;
}

DataTable > .datatable--cursor {
    background: #003355;
    color: #ffffff;
}

DataTable > .datatable--even-row {
    background: #090919;
}

DataTable > .datatable--odd-row {
    background: #08081e;
}

/* ── Log panel ── */
#log-panel {
    border: heavy #005577;
    background: #08081e;
    height: 1fr;
    min-height: 10;
    padding: 0 1 0 1;
}

#log-title {
    background: #0d0d2e;
    color: #ffcc00;
    text-style: bold;
    text-align: center;
    padding: 0 1;
    width: 100%;
}

RichLog {
    background: #08081e;
    color: #aaccdd;
    height: 1fr;
    scrollbar-color: #005577;
    scrollbar-background: #001122;
}
"""


# ── Widgets ───────────────────────────────────────────────────────────────────


class SettingsPanel(Static):
    """Read-only display of current mock_fis configuration (non-chaos settings)."""

    def compose(self) -> ComposeResult:
        yield Static("[ CONFIGURATION ]", id="settings-title")

        def flag(val: bool, on_style: str = "bold bright_green") -> str:
            return f"[{on_style}]ON[/]" if val else "[bold red]OFF[/]"

        body = (
            f"  [dim]PORT           [/dim]  [bright_cyan]{PORT}[/]\n"
            f"  [dim]S3 ENDPOINT    [/dim]  [#6a8fa8]{S3_ENDPOINT[:28]}[/]\n"
            f"  [dim]INBOX BUCKET   [/dim]  [#aabbcc]{INBOX_BUCKET}[/]\n"
            f"  [dim]STORAGE ALIAS  [/dim]  [#aabbcc]{STORAGE_ALIAS}[/]\n"
            f"  [dim]FILE SIZE      [/dim]  [#aabbcc]{FILE_SIZE_MB} MB[/]\n"
            f"  [dim]PART SIZE      [/dim]  [#aabbcc]{PART_SIZE_MB} MB[/]\n"
            f"  [dim]AUTO ARCHIVE   [/dim]  {flag(AUTO_ARCHIVE)}"
        )
        yield Static(body, id="settings-body", markup=True)


class HorizontalSlider(Widget):
    """Single-row interactive slider for a float in [0.0, 1.0].

    Keyboard: Left / Right arrows move by step.
    Mouse: click anywhere on the track to snap the thumb.
    """

    can_focus = True

    class Changed(Message):
        def __init__(self, slider: "HorizontalSlider", value: float) -> None:
            super().__init__()
            self.slider = slider
            self.value = value

        @property
        def control(self) -> "HorizontalSlider":
            return self.slider

    value: reactive[float] = reactive(0.5)

    def __init__(
        self,
        value: float = 0.5,
        *,
        min_val: float = 0.0,
        max_val: float = 1.0,
        step: float = 0.05,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._min = min_val
        self._max = max_val
        self._step = step
        self.value = max(min_val, min(max_val, value))

    def render(self) -> Text:
        width = self.size.width
        if width <= 0:
            return Text("─")

        frac = (self.value - self._min) / (self._max - self._min)
        thumb = int(round(frac * (width - 1)))

        track = Text(overflow="fold")
        thumb_style = "bold bright_cyan" if self.has_focus else "bold #006688"
        for i in range(width):
            if i == thumb:
                track.append("█", style=thumb_style)
            elif i < thumb:
                track.append("━", style="#005577")
            else:
                track.append("─", style="dim #1a2233")
        return track

    def on_key(self, event: Key) -> None:
        if event.key == "left":
            self._move(-self._step)
            event.stop()
        elif event.key == "right":
            self._move(self._step)
            event.stop()

    def on_click(self, event: Click) -> None:
        width = self.size.width
        if width > 1:
            frac = event.x / (width - 1)
            new_val = self._min + frac * (self._max - self._min)
            self._set(new_val)

    def _move(self, delta: float) -> None:
        self._set(self.value + delta)

    def _set(self, new_val: float) -> None:
        clamped = max(self._min, min(self._max, new_val))
        if clamped != self.value:
            self.value = clamped
            self.post_message(self.Changed(self, self.value))


class ChaosPanel(Widget):
    """Interactive live controls: chaos mode toggle, chaos corrupt toggle, probability slider."""

    def compose(self) -> ComposeResult:
        yield Static("[ CHAOS CONTROLS ]", id="chaos-title")
        with Horizontal(classes="chaos-row"):
            yield Static("CHAOS MODE", classes="chaos-label")
            yield Switch(value=CHAOS_MODE, id="sw-chaos-mode")
        with Horizontal(classes="chaos-row"):
            yield Static("CHAOS CORRUPT", classes="chaos-label")
            yield Switch(value=CHAOS_CORRUPT, id="sw-chaos-corrupt")
        with Horizontal(id="prob-row"):
            yield Static("PROBABILITY", classes="chaos-label")
            yield HorizontalSlider(
                value=CHAOS_PROBABILITY,
                id="slider-prob",
            )
            yield Static(f"{CHAOS_PROBABILITY:.0%}", id="prob-value")

    @on(HorizontalSlider.Changed, "#slider-prob")
    def _sync_prob_label(self, event: HorizontalSlider.Changed) -> None:
        """Keep the percentage label in sync as the thumb moves."""
        self.query_one("#prob-value", Static).update(f"{event.value:.0%}")
        # Event intentionally not stopped — App also handles it for the HTTP update.


class AdminPanel(Vertical):
    """Admin action buttons."""

    def compose(self) -> ComposeResult:
        yield Static("[ ADMIN ACTIONS ]", id="admin-title")
        with Horizontal(id="btn-row-1"):
            yield Button("⊕  RESEED FILE", id="btn-reseed", classes="btn-reseed")
            yield Button("✓  ARCHIVE ALL", id="btn-archive", classes="btn-archive")
        with Horizontal(id="btn-row-2"):
            yield Button("✗  CLEAR INBOX", id="btn-clear", classes="btn-clear")
            yield Button("↺  REFRESH NOW", id="btn-refresh", classes="btn-refresh")


class ServerStatusBar(Widget):
    """One-line strip showing server liveness, file count, and last sync time."""

    status_text: reactive[str] = reactive("STARTING…")
    file_count: reactive[int] = reactive(0)
    last_refresh: reactive[str] = reactive("--:--:--")

    def render(self) -> Text:
        t = Text()
        t.append("  SERVER  ", style="dim #446677")
        if "RUNNING" in self.status_text:
            t.append("● RUNNING", style="bold bright_green")
        elif self.status_text in ("STOPPED", "FAILED"):
            t.append(f"● {self.status_text}", style="bold bright_red")
        else:
            t.append(f"● {self.status_text}", style="bold bright_yellow")
        t.append("    FILES  ", style="dim #446677")
        t.append(str(self.file_count), style="bold bright_cyan")
        t.append("    LAST SYNC  ", style="dim #446677")
        t.append(self.last_refresh, style="#445566")
        return t


class InboxPanel(Container):
    """DataTable showing all tracked files and their states."""

    _COLUMNS = [
        ("FILE ID", 16),
        ("OBJECT ID", 16),
        ("STATE", 13),
        ("CAN REMOVE", 10),
        ("INT. OBJ ID", 16),
    ]

    def compose(self) -> ComposeResult:
        yield Static("[ INBOX FILES ]", id="inbox-title")
        yield DataTable(id="file-table", zebra_stripes=True, cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        for label, width in self._COLUMNS:
            table.add_column(label, width=width)

    def update_files(self, files: list[dict]) -> None:
        table = self.query_one(DataTable)
        table.clear()
        for entry in files:
            file_id = str(entry.get("file_id", ""))
            upload = entry.get("upload", {})
            obj_id = str(upload.get("object_id", ""))
            state = entry.get("state", "?")
            can_remove = entry.get("can_remove", False)
            int_obj = entry.get("interrogation_object_id") or "—"

            state_cell = {
                "inbox": Text("INBOX", style="bold bright_yellow"),
                "interrogated": Text("INTERROGATED", style="bold bright_green"),
                "failed": Text("FAILED", style="bold bright_red"),
            }.get(state, Text(state.upper()))

            table.add_row(
                file_id[:15] + "…",
                obj_id[:15] + "…",
                state_cell,
                Text(
                    "YES" if can_remove else "no",
                    style="bold bright_green" if can_remove else "dim #445566",
                ),
                (int_obj[:15] + "…")
                if int_obj != "—" and len(int_obj) > 15
                else int_obj,
            )


# ── App ───────────────────────────────────────────────────────────────────────


class MockFisApp(App):
    CSS = APP_CSS
    TITLE = "MOCK FIS CONTROL CENTER"
    SUB_TITLE = f"localhost:{PORT}"

    BINDINGS = [
        Binding("r", "reseed", "Reseed"),
        Binding("a", "archive_all", "Archive All"),
        Binding("c", "clear_inbox", "Clear Inbox"),
        Binding("f5", "refresh_state", "Refresh"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._server_proc: asyncio.subprocess.Process | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="top-row"):
            with Vertical(id="left-col"):
                yield SettingsPanel()
                yield ChaosPanel()
            yield AdminPanel()
        yield ServerStatusBar(id="status-bar")
        yield InboxPanel(id="inbox-panel")
        with Container(id="log-panel"):
            yield Static("[ SERVER LOGS ]", id="log-title")
            yield RichLog(
                id="log-view",
                highlight=False,
                markup=False,
                auto_scroll=True,
                wrap=True,
            )
        yield Footer()

    async def on_mount(self) -> None:
        self._run_server()
        self.set_interval(3.0, self._poll_state)

    # ── Server subprocess ──────────────────────────────────────────────────────

    @work(exclusive=True, group="server")
    async def _run_server(self) -> None:
        log = self.query_one("#log-view", RichLog)
        status_bar = self.query_one(ServerStatusBar)

        self._tui_log(
            log, f"Launching {MOCK_FIS_SCRIPT.name} on port {PORT}…", "bright_cyan"
        )

        try:
            self._server_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(MOCK_FIS_SCRIPT),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=os.environ.copy(),
            )
        except Exception as exc:
            self._tui_log(log, f"Failed to start server: {exc}", "bright_red")
            status_bar.status_text = "FAILED"
            return

        self._tui_log(
            log, f"PID {self._server_proc.pid} — streaming output below", "dim #556677"
        )
        status_bar.status_text = "RUNNING"

        assert self._server_proc.stdout is not None
        async for raw_line in self._server_proc.stdout:
            log.write(_colorize(raw_line.decode("utf-8", errors="replace")))

        status_bar.status_text = "STOPPED"
        self._tui_log(log, "Server process exited.", "bright_yellow")

    @staticmethod
    def _tui_log(log: RichLog, msg: str, style: str = "white") -> None:
        t = Text()
        t.append("[TUI] ", style="bold #334455")
        t.append(msg, style=style)
        log.write(t)

    # ── State polling ──────────────────────────────────────────────────────────

    @work(exclusive=True, group="poll")
    async def _poll_state(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"{BASE_URL}/admin/state")
        except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError):
            return

        if resp.status_code != 200:
            return

        files = resp.json()
        self.query_one(InboxPanel).update_files(files)

        status_bar = self.query_one(ServerStatusBar)
        status_bar.file_count = len(files)
        status_bar.status_text = "RUNNING"
        status_bar.last_refresh = datetime.now().strftime("%H:%M:%S")

    # ── Admin actions ──────────────────────────────────────────────────────────

    @work(group="admin")
    async def _do_admin(self, method: str, path: str, label: str) -> None:
        log = self.query_one("#log-view", RichLog)

        header = Text()
        header.append(">>> ", style="bold #334455")
        header.append(label.upper(), style="bold bright_cyan")
        log.write(header)

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.request(method, f"{BASE_URL}{path}")
        except Exception as exc:
            log.write(Text(f"    ✗ {exc}", style="bright_red"))
            return

        preview = resp.text[:200].replace("\n", " ")
        if resp.status_code < 300:
            log.write(
                Text(f"    ✓ HTTP {resp.status_code}  {preview}", style="bright_green")
            )
        else:
            log.write(
                Text(f"    ✗ HTTP {resp.status_code}  {preview}", style="bright_red")
            )

        self._poll_state()

    # ── Chaos config updates ───────────────────────────────────────────────────

    @on(Switch.Changed, "#sw-chaos-mode")
    def _on_chaos_mode(self, event: Switch.Changed) -> None:
        self._patch_switch("chaos", event.value)

    @on(Switch.Changed, "#sw-chaos-corrupt")
    def _on_chaos_corrupt(self, event: Switch.Changed) -> None:
        self._patch_switch("chaos_corrupt", event.value)

    @on(HorizontalSlider.Changed, "#slider-prob")
    def _on_prob_slider(self, event: HorizontalSlider.Changed) -> None:
        self._patch_prob(event.value)

    @work(group="config-switch")
    async def _patch_switch(self, key: str, value: bool) -> None:
        """Send an immediate PATCH for a toggle change."""
        log = self.query_one("#log-view", RichLog)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.patch(f"{BASE_URL}/admin/config", json={key: value})
        except Exception as exc:
            self._tui_log(log, f"Config update failed: {exc}", "bright_red")
            return
        if resp.status_code == 200:
            cfg = resp.json()
            self._tui_log(
                log,
                f"chaos={cfg['chaos']}  corrupt={cfg['chaos_corrupt']}  prob={cfg['chaos_probability']:.0%}",
                "bold #cc66ff",
            )

    @work(exclusive=True, group="config-slider")
    async def _patch_prob(self, value: float) -> None:
        """Debounced PATCH for probability slider — rapid drags coalesce into one request."""
        await asyncio.sleep(0.2)
        log = self.query_one("#log-view", RichLog)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.patch(
                    f"{BASE_URL}/admin/config", json={"chaos_probability": value}
                )
        except Exception as exc:
            self._tui_log(log, f"Config update failed: {exc}", "bright_red")
            return
        if resp.status_code == 200:
            cfg = resp.json()
            self._tui_log(
                log,
                f"chaos={cfg['chaos']}  corrupt={cfg['chaos_corrupt']}  prob={cfg['chaos_probability']:.0%}",
                "bold #cc66ff",
            )

    # ── Button → action wiring ─────────────────────────────────────────────────

    @on(Button.Pressed, "#btn-reseed")
    def _on_btn_reseed(self) -> None:
        self.action_reseed()

    @on(Button.Pressed, "#btn-archive")
    def _on_btn_archive(self) -> None:
        self.action_archive_all()

    @on(Button.Pressed, "#btn-clear")
    def _on_btn_clear(self) -> None:
        self.action_clear_inbox()

    @on(Button.Pressed, "#btn-refresh")
    def _on_btn_refresh(self) -> None:
        self.action_refresh_state()

    # ── Keyboard actions ───────────────────────────────────────────────────────

    def action_reseed(self) -> None:
        self._do_admin("POST", "/admin/reseed", "Reseed")

    def action_archive_all(self) -> None:
        self._do_admin("POST", "/admin/archive_all", "Archive All")

    def action_clear_inbox(self) -> None:
        self._do_admin("DELETE", "/admin/inbox", "Clear Inbox")

    def action_refresh_state(self) -> None:
        self._poll_state()

    # ── Cleanup ────────────────────────────────────────────────────────────────

    async def on_unmount(self) -> None:
        if self._server_proc and self._server_proc.returncode is None:
            self._server_proc.terminate()
            try:
                await asyncio.wait_for(self._server_proc.wait(), timeout=3.0)
            except TimeoutError:
                self._server_proc.kill()


if __name__ == "__main__":
    MockFisApp().run()
