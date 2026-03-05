import os
import sys
import threading
import queue
import sqlite3
import locale
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ================== НАСТРОЙКИ ==================

POSSIBLE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "latin-1"]

PREFIX_CATEGORY = "Категория:"
PREFIX_AUTHOR = "Автор:"
PREFIX_TITLE = "Название:"

STATUS_NONE = 0
STATUS_DONE = 1
STATUS_PRIORITY = 2

STATUS_LABELS = {
    STATUS_NONE: "",
    STATUS_DONE: "✅",
    STATUS_PRIORITY: "⭐",
}

# ================== УТИЛИТЫ ==================

def open_in_default_app(path: str):
    if sys.platform.startswith("win"):
        os.startfile(path)


def base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).parent


def db_path() -> Path:
    d = base_dir() / "data"
    d.mkdir(exist_ok=True)
    return d / "index.db"


def db_connect():
    conn = sqlite3.connect(db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            path TEXT PRIMARY KEY,
            categories TEXT,
            author TEXT,
            title TEXT,
            mtime REAL,
            size INTEGER,
            status INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def read_first_three_fields(path: str):
    lines = None
    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc) as f:
                lines = [f.readline().strip() for _ in range(5)]
            break
        except Exception:
            pass

    if not lines:
        return [], "", ""

    category = author = title = ""

    for ln in lines:
        if ln.startswith(PREFIX_CATEGORY):
            category = ln[len(PREFIX_CATEGORY):].strip()
        elif ln.startswith(PREFIX_AUTHOR):
            author = ln[len(PREFIX_AUTHOR):].strip()
        elif ln.startswith(PREFIX_TITLE):
            title = ln[len(PREFIX_TITLE):].strip()

    categories = [c.strip() for c in category.split(",") if c.strip()]
    return categories, author, title


# ================== GUI ==================

class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TXT Indexer")
        self.geometry("1200x650")

        locale.setlocale(locale.LC_COLLATE, "")

        self.folder = tk.StringVar()
        self.status = tk.StringVar(value="Выберите папку")

        self.queue = queue.Queue()
        self.worker = None

        self._build_ui()
        self.load_from_db()

        self.after(100, self._poll)

    # ---------- UI ----------

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=5)

        ttk.Entry(top, textvariable=self.folder, width=80).pack(side="left")
        ttk.Button(top, text="Обзор", command=self.choose_folder).pack(side="left", padx=5)
        ttk.Button(top, text="Индексировать", command=self.reindex).pack(side="left")

        columns = ("status", "category", "author", "title")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")

        self.tree.heading("status", text="Статус", command=lambda: self.sort_by("status"))
        self.tree.heading("category", text="Категория", command=lambda: self.sort_by("category"))
        self.tree.heading("author", text="Автор", command=lambda: self.sort_by("author"))
        self.tree.heading("title", text="Название", command=lambda: self.sort_by("title"))

        self.tree.column("status", width=70, anchor="center")
        self.tree.column("category", width=250)
        self.tree.column("author", width=200)
        self.tree.column("title", width=550)

        self.tree.pack(fill="both", expand=True, padx=10)
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Button-1>", self.on_click)

        ttk.Label(self, textvariable=self.status).pack(anchor="w", padx=10, pady=5)

    # ---------- ЗАГРУЗКА ----------

    def load_from_db(self):
        self.tree.delete(*self.tree.get_children())
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT path, categories, author, title, status FROM articles")
            for path, cats, auth, title, status in cur.fetchall():
                for cat in cats.split("|"):
                    iid = f"{path}|{cat}"
                    self.tree.insert("", "end", iid=iid,
                        values=(STATUS_LABELS[status], cat, auth, title))
        finally:
            conn.close()

    # ---------- ИНДЕКСАЦИЯ ----------

    def reindex(self):
        folder = self.folder.get().strip()
        if not os.path.isdir(folder):
            messagebox.showerror("Ошибка", "Неверная папка")
            return

        if self.worker and self.worker.is_alive():
            return

        self.worker = threading.Thread(target=self._worker, args=(folder,), daemon=True)
        self.worker.start()
        self.status.set("Индексация...")

    def _worker(self, folder):
        conn = db_connect()
        try:
            cur = conn.cursor()
            seen = set()

            for name in os.listdir(folder):
                if not name.lower().endswith(".txt"):
                    continue

                path = os.path.join(folder, name)
                seen.add(path)
                st = os.stat(path)

                cur.execute("SELECT mtime, size FROM articles WHERE path=?", (path,))
                row = cur.fetchone()
                if row and row == (st.st_mtime, st.st_size):
                    continue

                cats, auth, title = read_first_three_fields(path)
                cats_joined = "|".join(cats)

                cur.execute("""
                    INSERT INTO articles(path, categories, author, title, mtime, size)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(path) DO UPDATE SET
                        categories=excluded.categories,
                        author=excluded.author,
                        title=excluded.title,
                        mtime=excluded.mtime,
                        size=excluded.size
                """, (path, cats_joined, auth, title, st.st_mtime, st.st_size))

            cur.execute("SELECT path FROM articles")
            for (p,) in cur.fetchall():
                if p not in seen:
                    cur.execute("DELETE FROM articles WHERE path=?", (p,))

            conn.commit()
        finally:
            conn.close()

        self.queue.put("done")

    # ---------- СОБЫТИЯ ----------

    def _poll(self):
        try:
            while True:
                msg = self.queue.get_nowait()
                if msg == "done":
                    self.load_from_db()
                    self.status.set("Готово")
        except queue.Empty:
            pass
        self.after(100, self._poll)

    def on_double_click(self, event):
        item = self.tree.focus()
        if not item:
            return
        path = item.split("|")[0]
        if os.path.exists(path):
            open_in_default_app(path)

    def on_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        col = self.tree.identify_column(event.x)
        if col != "#1":
            return

        item = self.tree.identify_row(event.y)
        if not item:
            return

        path = item.split("|")[0]
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM articles WHERE path=?", (path,))
            status = cur.fetchone()[0]
            status = (status + 1) % 3
            cur.execute("UPDATE articles SET status=? WHERE path=?", (status, path))
            conn.commit()
        finally:
            conn.close()

        self.load_from_db()

    def sort_by(self, col):
        idx = {"status": 0, "category": 1, "author": 2, "title": 3}[col]
        items = [(self.tree.item(i)["values"][idx], i) for i in self.tree.get_children("")]
        items.sort(key=lambda x: locale.strxfrm(str(x[0])))
        for i, (_, iid) in enumerate(items):
            self.tree.move(iid, "", i)


if __name__ == "__main__":
    TxtIndexerApp().mainloop()