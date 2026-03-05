import os
import sys
import sqlite3
import threading
import queue
import locale
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# -------------------- Настройки чтения --------------------

POSSIBLE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "latin-1"]

PREFIX_CATEGORY = "Категория:"
PREFIX_AUTHOR = "Автор:"
PREFIX_TITLE = "Название:"

STATUS_NONE = 0       # без выделения
STATUS_DONE = 1       # выполнено
STATUS_PRIORITY = 2   # приоритет

STATUS_LABEL = {
    STATUS_NONE: "",
    STATUS_DONE: "✅",
    STATUS_PRIORITY: "⭐",
}

# Для сортировки статуса (по возрастанию). Можно поменять порядок как вам удобнее.
STATUS_SORT_KEY = {
    "": 0,
    "✅": 1,
    "⭐": 2,
}


# -------------------- Утилиты --------------------

def open_in_default_app(path: str):
    """Открыть файл в программе по умолчанию."""
    if sys.platform.startswith("win"):
        os.startfile(path)  # noqa
    elif sys.platform == "darwin":
        import subprocess
        subprocess.Popen(["open", path])
    else:
        import subprocess
        subprocess.Popen(["xdg-open", path])


def base_dir() -> Path:
    """Папка рядом с exe (если frozen) или рядом со скриптом."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def data_dir() -> Path:
    d = base_dir() / "data"
    d.mkdir(exist_ok=True)
    return d


def db_file() -> Path:
    return data_dir() / "index.db"


def db_connect() -> sqlite3.Connection:
    """Соединение SQLite. Не шарим между потоками."""
    conn = sqlite3.connect(db_file(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author ON articles(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title  ON articles(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON articles(status)")
    conn.commit()
    return conn


def read_header_fields(path: str):
    """
    Возвращает (categories_list, author, title).
    categories_list — список категорий (может быть пустым).
    """
    lines = None
    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                lines = []
                for _ in range(10):  # берем чуть больше строк на случай пустых
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
        # Фоллбек: читаем как получится
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

    # Категорий может быть несколько через запятую
    categories = [c.strip() for c in category_raw.split(",") if c.strip()]
    return categories, author, title


def pack_categories(categories: list[str]) -> str:
    """Сохраняем категории в БД одной строкой."""
    # Используем разделитель, которого не бывает в обычных названиях категорий
    return "|".join(categories)


def unpack_categories(s: str) -> list[str]:
    if not s:
        return []
    return [x for x in s.split("|") if x]


# -------------------- GUI приложение --------------------

class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TXT Indexer (multi-category + status)")
        self.geometry("1250x680")

        try:
            locale.setlocale(locale.LC_COLLATE, "")
        except Exception:
            pass

        self.folder = tk.StringVar(value="")
        self.status_text = tk.StringVar(value="Выберите папку с TXT файлами")

        self._q = queue.Queue()
        self._worker = None
        self._stop_flag = False

        self._sort_state = {"status": True, "category": True, "author": True, "title": True}

        self._build_ui()
        self.load_from_cache()

        self.after(150, self._poll_queue)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- UI ----------

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Папка:").pack(side="left")
        ttk.Entry(top, textvariable=self.folder, width=75).pack(side="left", padx=8)
        ttk.Button(top, text="Выбрать…", command=self.choose_folder).pack(side="left")
        ttk.Button(top, text="Переиндексировать", command=self.reindex).pack(side="left", padx=8)

        # Таблица
        columns = ("status", "category", "author", "title")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")

        self.tree.heading("status", text="Статус", command=lambda: self.sort_by("status"))
        self.tree.heading("category", text="Категория", command=lambda: self.sort_by("category"))
        self.tree.heading("author", text="Автор", command=lambda: self.sort_by("author"))
        self.tree.heading("title", text="Название", command=lambda: self.sort_by("title"))

        self.tree.column("status", width=80, anchor="center")
        self.tree.column("category", width=280, anchor="w")
        self.tree.column("author", width=220, anchor="w")
        self.tree.column("title", width=600, anchor="w")

        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(fill="both", expand=True, padx=10, pady=(0, 0))
        vsb.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
        hsb.pack(fill="x", padx=10, pady=(0, 0))

        # Открытие файла по двойному клику
        self.tree.bind("<Double-1>", self.on_double_click)
        # Переключение статуса по клику в колонке Статус
        self.tree.bind("<Button-1>", self.on_click)

        bottom = ttk.Frame(self, padding=10)
        bottom.pack(fill="x")
        ttk.Label(bottom, textvariable=self.status_text).pack(side="left")

    # ---------- Выбор папки ----------

    def choose_folder(self):
        folder = filedialog.askdirectory(title="Выберите папку с TXT файлами")
        if folder:
            self.folder.set(folder)
            self.reindex()

    # ---------- Загрузка из кэша (БД) ----------

    def load_from_cache(self):
        self.tree.delete(*self.tree.get_children())

        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT path, categories, author, title, status FROM articles")
            rows = cur.fetchall()
        finally:
            conn.close()

        inserted = 0
        for path, cats_s, author, title, status in rows:
            cats = unpack_categories(cats_s)
            if not cats:
                cats = [""]  # если категорий нет — показываем одной строкой

            for cat in cats:
                # iid должен быть уникален -> path + категория
                iid = f"{path}||{cat}"
                self.tree.insert(
                    "", "end", iid=iid,
                    values=(STATUS_LABEL.get(status, ""), cat, author, title)
                )
                inserted += 1

        self.status_text.set(f"Загружено из кэша: файлов {len(rows)}, строк в таблице {inserted}.")

    # ---------- Индексация ----------

    def reindex(self):
        folder = self.folder.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("Папка не выбрана", "Пожалуйста, выберите существующую папку.")
            return

        if self._worker and self._worker.is_alive():
            return

        self.load_from_cache()
        self.status_text.set("Синхронизация с папкой…")

        self._stop_flag = False
        self._worker = threading.Thread(target=self._worker_func, args=(folder,), daemon=True)
        self._worker.start()

    def _worker_func(self, folder: str):
        total = 0
        updated = 0
        deleted = 0
        errors = 0

        try:
            names = [n for n in os.listdir(folder) if n.lower().endswith(".txt")]
            names.sort()
        except Exception as e:
            self._q.put(("error", f"Не удалось прочитать папку: {e}"))
            return

        existing_paths = set()

        conn = db_connect()
        try:
            cur = conn.cursor()

            for fname in names:
                if self._stop_flag:
                    break

                path = os.path.join(folder, fname)
                existing_paths.add(path)

                try:
                    st = os.stat(path)
                    mtime, size = st.st_mtime, st.st_size

                    cur.execute("SELECT mtime, size FROM articles WHERE path=?", (path,))
                    row = cur.fetchone()
                    if row and row[0] == mtime and row[1] == size:
                        total += 1
                        continue

                    categories, author, title = read_header_fields(path)
                    cats_s = pack_categories(categories)

                    # статус сохраняем старый, если запись уже была
                    cur.execute("SELECT status FROM articles WHERE path=?", (path,))
                    old = cur.fetchone()
                    old_status = old[0] if old else STATUS_NONE

                    cur.execute("""
                        INSERT INTO articles(path, categories, author, title, mtime, size, status)
                        VALUES(?,?,?,?,?,?,?)
                        ON CONFLICT(path) DO UPDATE SET
                            categories=excluded.categories,
                            author=excluded.author,
                            title=excluded.title,
                            mtime=excluded.mtime,
                            size=excluded.size,
                            status=excluded.status
                    """, (path, cats_s, author, title, mtime, size, old_status))

                    updated += 1
                    total += 1

                except Exception:
                    errors += 1

            # Удаляем записи, которых нет на диске
            cur.execute("SELECT path FROM articles")
            all_paths = [r[0] for r in cur.fetchall()]
            for p in all_paths:
                if p not in existing_paths:
                    cur.execute("DELETE FROM articles WHERE path=?", (p,))
                    deleted += 1

            conn.commit()
        finally:
            conn.close()

        self._q.put(("done", (total, updated, deleted, errors)))

    # ---------- Queue polling ----------

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self._q.get_nowait()
                if kind == "done":
                    total, updated, deleted, errors = payload
                    self.load_from_cache()
                    msg = f"Синхронизация: файлов {total}, обновлено {updated}, удалено {deleted}"
                    if errors:
                        msg += f", ошибок {errors}"
                    self.status_text.set(msg)
                elif kind == "error":
                    self.status_text.set(payload)
        except queue.Empty:
            pass

        self.after(150, self._poll_queue)

    # ---------- События UI ----------

    def on_double_click(self, event):
        iid = self.tree.focus()
        if not iid:
            return
        path = iid.split("||", 1)[0]
        if os.path.exists(path):
            open_in_default_app(path)
        else:
            messagebox.showwarning("Файл не найден", "Файл был удалён или перемещён. Переиндексируйте папку.")

    def on_click(self, event):
        # Если кликнули по ячейке в колонке "Статус" — переключаем
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return

        col = self.tree.identify_column(event.x)
        # #1 = первая колонка из show="headings", т.е. status
        if col != "#1":
            return

        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return

        path = row_id.split("||", 1)[0]
        self.toggle_status(path)

        # Обновляем таблицу (статус один на файл — обновится во всех категориях)
        self.load_from_cache()

    # ---------- Статус ----------

    def toggle_status(self, path: str):
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM articles WHERE path=?", (path,))
            row = cur.fetchone()
            if not row:
                return
            status = row[0]
            status = (status + 1) % 3
            cur.execute("UPDATE articles SET status=? WHERE path=?", (status, path))
            conn.commit()
        finally:
            conn.close()

    # ---------- Сортировка ----------

    def sort_by(self, col: str):
        ascending = self._sort_state.get(col, True)

        def key_func(iid: str):
            vals = self.tree.item(iid, "values")
            # vals = (status_symbol, category, author, title)
            if col == "status":
                return STATUS_SORT_KEY.get(vals[0], 0)
            if col == "category":
                return locale.strxfrm((vals[1] or "").lower())
            if col == "author":
                return locale.strxfrm((vals[2] or "").lower())
            if col == "title":
                return locale.strxfrm((vals[3] or "").lower())
            return ""

        items = list(self.tree.get_children(""))
        items.sort(key=key_func, reverse=not ascending)

        for idx, iid in enumerate(items):
            self.tree.move(iid, "", idx)

        self._sort_state[col] = not ascending

    # ---------- Выход ----------

    def on_close(self):
        self._stop_flag = True
        self.destroy()


if __name__ == "__main__":
    TxtIndexerApp().mainloop()