import os
import sys
import sqlite3
import locale
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# =========================================================
# НАСТРОЙКИ
# =========================================================

POSSIBLE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "latin-1"]

PREFIX_CATEGORY = "Категория:"
PREFIX_AUTHOR = "Автор:"
PREFIX_TITLE = "Название:"

STATUS_NONE = 0
STATUS_DONE = 1
STATUS_PRIORITY = 2

STATUS_LABEL = {
    STATUS_NONE: "",
    STATUS_DONE: "✅",
    STATUS_PRIORITY: "⭐",
}

STATUS_SORT_KEY = {"": 0, "✅": 1, "⭐": 2}

CAT_SEP = "\u001f"
IID_SEP = "\u001e"

# =========================================================
# УТИЛИТЫ
# =========================================================

def open_in_default_app(path: str):
    if sys.platform.startswith("win"):
        os.startfile(path)


def base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def db_path() -> Path:
    d = base_dir() / "data"
    d.mkdir(exist_ok=True)
    return d / "index.db"


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    migrate_schema(conn)
    return conn


def migrate_schema(conn: sqlite3.Connection):
    """
    Корректная миграция старых БД:
    - category -> categories
    - гарантирует наличие всех нужных колонок
    """
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            path TEXT PRIMARY KEY
        )
    """)

    cur.execute("PRAGMA table_info(articles)")
    cols = {row[1] for row in cur.fetchall()}

    if "category" in cols and "categories" not in cols:
        cur.execute("ALTER TABLE articles ADD COLUMN categories TEXT DEFAULT ''")
        cur.execute("UPDATE articles SET categories = category")

    for col, ddl in [
        ("categories", "TEXT DEFAULT ''"),
        ("author", "TEXT DEFAULT ''"),
        ("title", "TEXT DEFAULT ''"),
        ("mtime", "REAL DEFAULT 0"),
        ("size", "INTEGER DEFAULT 0"),
        ("status", "INTEGER DEFAULT 0"),
    ]:
        if col not in cols:
            cur.execute(f"ALTER TABLE articles ADD COLUMN {col} {ddl}")

    conn.commit()


def pack_categories(cats):
    return CAT_SEP.join(cats)


def unpack_categories(s):
    if not s:
        return []
    return [c for c in s.split(CAT_SEP) if c]


def read_header_fields(path: str):
    lines = None
    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                lines = []
                for _ in range(10):
                    s = f.readline()
                    if not s:
                        break
                    s = s.strip()
                    if s:
                        lines.append(s)
                    if len(lines) >= 3:
                        break
            break
        except Exception:
            lines = None

    if not lines:
        with open(path, "r", encoding="cp1251", errors="replace") as f:
            lines = [f.readline().strip() for _ in range(3)]

    category_raw = ""
    author = ""
    title = ""

    for ln in lines:
        if ln.startswith(PREFIX_CATEGORY):
            category_raw = ln[len(PREFIX_CATEGORY):].strip()
        elif ln.startswith(PREFIX_AUTHOR):
            author = ln[len(PREFIX_AUTHOR):].strip()
        elif ln.startswith(PREFIX_TITLE):
            title = ln[len(PREFIX_TITLE):].strip()

    categories = [c.strip() for c in category_raw.split(",") if c.strip()]
    return categories, author, title

# =========================================================
# GUI
# =========================================================

class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TXT Indexer")
        self.geometry("1300x700")

        locale.setlocale(locale.LC_COLLATE, "")

        self.folder = tk.StringVar()
        self.search = tk.StringVar()
        self.status_text = tk.StringVar(value="Выберите папку")

        self.records = []
        self.path_iids = {}
        self.sort_col = None
        self.sort_reverse = False

        self._build_ui()
        self.load_from_db()
        self.render()

    # ---------- UI ----------

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Entry(top, textvariable=self.folder, width=60).pack(side="left")
        ttk.Button(top, text="Обзор", command=self.choose_folder).pack(side="left", padx=5)
        ttk.Button(top, text="Индексировать", command=self.reindex).pack(side="left")

        ttk.Label(top, text="Поиск:").pack(side="left", padx=10)
        e = ttk.Entry(top, textvariable=self.search, width=30)
        e.pack(side="left")
        self.search.trace_add("write", lambda *_: self.render())

        columns = ("status", "category", "author", "title")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")

        headers = {
            "status": "Статус",
            "category": "Категория",
            "author": "Автор",
            "title": "Название",
        }
        for c in columns:
            self.tree.heading(c, text=headers[c], command=lambda col=c: self.sort(col))

        self.tree.column("status", width=80, anchor="center")
        self.tree.column("category", width=300)
        self.tree.column("author", width=240)
        self.tree.column("title", width=600)

        self.tree.pack(fill="both", expand=True, padx=10)
        self.tree.bind("<Button-1>", self.on_click)

        ttk.Label(self, textvariable=self.status_text).pack(anchor="w", padx=10)

    # ---------- DB ----------

    def load_from_db(self):
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT path, categories, author, title, status FROM articles")
            self.records = []
            for p, c, a, t, s in cur.fetchall():
                self.records.append({
                    "path": p,
                    "categories": unpack_categories(c),
                    "author": a,
                    "title": t,
                    "status": s,
                })
        finally:
            conn.close()

    # ---------- Render ----------

    def render(self):
        q = self.search.get().lower().strip()
        self.tree.delete(*self.tree.get_children())
        self.path_iids = {}

        rows = []
        for r in self.records:
            hay = " ".join([
                r["title"],
                r["author"],
                " ".join(r["categories"]),
                os.path.basename(r["path"]),
            ]).lower()
            if q and q not in hay:
                continue

            cats = r["categories"] or [""]
            for cat in cats:
                rows.append({
                    "path": r["path"],
                    "category": cat,
                    "author": r["author"],
                    "title": r["title"],
                    "status": r["status"],
                })

        if self.sort_col:
            col = self.sort_col
            if col == "status":
                keyfunc = lambda row: STATUS_SORT_KEY[STATUS_LABEL[row["status"]]]
            else:
                keyfunc = lambda row: locale.strxfrm(str(row[col]).lower())
            rows.sort(key=keyfunc, reverse=self.sort_reverse)

        for row in rows:
            iid = row["path"] + IID_SEP + row["category"]
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    STATUS_LABEL[row["status"]],
                    row["category"],
                    row["author"],
                    row["title"],
                ),
            )
            self.path_iids.setdefault(row["path"], []).append(iid)

    # ---------- Actions ----------

    def choose_folder(self):
        d = filedialog.askdirectory()
        if d:
            self.folder.set(d)
            self.reindex()

    def reindex(self):
        folder = self.folder.get()
        if not os.path.isdir(folder):
            return

        conn = db_connect()
        try:
            cur = conn.cursor()
            for name in os.listdir(folder):
                if not name.lower().endswith(".txt"):
                    continue
                path = os.path.join(folder, name)
                st = os.stat(path)
                cats, a, t = read_header_fields(path)

                cur.execute("""
                    INSERT INTO articles(path, categories, author, title, mtime, size)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(path) DO UPDATE SET
                        categories=excluded.categories,
                        author=excluded.author,
                        title=excluded.title,
                        mtime=excluded.mtime,
                        size=excluded.size
                """, (path, pack_categories(cats), a, t, st.st_mtime, st.st_size))
            conn.commit()
        finally:
            conn.close()

        self.load_from_db()
        self.render()

    def on_click(self, e):
        region = self.tree.identify_region(e.x, e.y)
        if region != "cell":
            return

        column = self.tree.identify_column(e.x)
        iid = self.tree.identify_row(e.y)
        if not iid:
            return

        path = iid.split(IID_SEP)[0]

        # клик по колонке статуса — переключение статуса
        if column == "#1":
            self.toggle_status(path)
            return "break"

        # клик по названию — открыть файл в программе по умолчанию
        if column == "#4":
            open_in_default_app(path)
            return "break"

    def toggle_status(self, path):
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM articles WHERE path=?", (path,))
            s = (cur.fetchone()[0] + 1) % 3
            cur.execute("UPDATE articles SET status=? WHERE path=?", (s, path))
            conn.commit()
        finally:
            conn.close()
        self.load_from_db()
        self.render()

    def sort(self, col):
        if self.sort_col == col:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_col = col
            self.sort_reverse = False
        self.render()


if __name__ == "__main__":
    TxtIndexerApp().mainloop()