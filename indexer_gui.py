import os
import sys
import sqlite3
import threading
import queue
import locale
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# -------------------- Настройки --------------------

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

# порядок сортировки статуса (можете поменять, если нужно)
STATUS_SORT_KEY = {"": 0, "✅": 1, "⭐": 2}

# разделители, которые почти никогда не встретятся в тексте
CAT_SEP = "\u001f"   # unit separator (для хранения категорий в SQLite)
IID_SEP = "\u001e"   # record separator (для iid строк Treeview)


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


def _ensure_schema(conn: sqlite3.Connection):
    """Мягкая миграция схемы под новые колонки."""
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS articles (path TEXT PRIMARY KEY)")
    cur.execute("PRAGMA table_info(articles)")
    cols = {r[1] for r in cur.fetchall()}

    # старые версии могли иметь "category" вместо "categories"
    if "categories" not in cols and "category" in cols:
        try:
            cur.execute("ALTER TABLE articles ADD COLUMN categories TEXT DEFAULT ''")
            cur.execute("UPDATE articles SET categories = category WHERE categories = ''")
        except Exception:
            pass

    if "categories" not in cols and "category" not in cols:
        try:
            cur.execute("ALTER TABLE articles ADD COLUMN categories TEXT DEFAULT ''")
        except Exception:
            pass

    for col, ddl in [
        ("author", "TEXT DEFAULT ''"),
        ("title", "TEXT DEFAULT ''"),
        ("mtime", "REAL DEFAULT 0"),
        ("size", "INTEGER DEFAULT 0"),
        ("status", "INTEGER DEFAULT 0"),
    ]:
        if col not in cols:
            try:
                cur.execute(f"ALTER TABLE articles ADD COLUMN {col} {ddl}")
            except Exception:
                pass

    conn.commit()


def db_connect() -> sqlite3.Connection:
    """Соединение SQLite. Не шарим между потоками."""
    conn = sqlite3.connect(db_file(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    _ensure_schema(conn)

    # если таблицы не было, создадим нормальную (безопасно повторять)
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


def pack_categories(categories: list[str]) -> str:
    return CAT_SEP.join(categories)


def unpack_categories(s: str) -> list[str]:
    if not s:
        return []
    return [x for x in s.split(CAT_SEP) if x]


def read_header_fields(path: str):
    """
    Возвращает (categories_list, author, title)
    categories_list — список категорий из первой строки (через запятую)
    """
    lines = None
    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="strict", newline="") as f:
                raw = []
                for _ in range(10):  # на случай пустых строк
                    s = f.readline()
                    if not s:
                        break
                    s = s.strip()
                    if s:
                        raw.append(s)
                    if len(raw) >= 3:
                        break
            lines = raw
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


# -------------------- GUI --------------------

class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TXT Indexer (multi-category + status + search)")
        self.geometry("1300x700")

        try:
            locale.setlocale(locale.LC_COLLATE, "")
        except Exception:
            pass

        self.folder = tk.StringVar(value="")
        self.search_var = tk.StringVar(value="")
        self.status_text = tk.StringVar(value="Выберите папку с TXT файлами")

        self._q = queue.Queue()
        self._worker = None
        self._stop_flag = False

        self._sort_state = {"status": True, "category": True, "author": True, "title": True}

        # кэш записей из БД: список dict по файлам
        self._file_records = []  # каждый: {"path","categories","author","title","status"}
        # индексы для обновления статуса
        self._path_to_row_iids = {}  # path -> [iid,...]

        self._search_after_id = None

        self._build_ui()
        self.reload_cache_from_db()
        self.render_table()

        self.after(150, self._poll_queue)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- UI ----------

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Папка:").pack(side="left")
        ttk.Entry(top, textvariable=self.folder, width=60).pack(side="left", padx=8)
        ttk.Button(top, text="Выбрать…", command=self.choose_folder).pack(side="left")
        ttk.Button(top, text="Переиндексировать", command=self.reindex).pack(side="left", padx=8)

        ttk.Label(top, text="Поиск:").pack(side="left", padx=(20, 5))
        search_entry = ttk.Entry(top, textvariable=self.search_var, width=35)
        search_entry.pack(side="left")
        ttk.Button(top, text="Очистить", command=self.clear_search).pack(side="left", padx=6)

        # реагируем на ввод в поиске (debounce)
        self.search_var.trace_add("write", lambda *_: self.on_search_changed())

        columns = ("status", "category", "author", "title")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")

        # сортировка только по клику на шапке (heading command)
        self.tree.heading("status", text="Статус", command=lambda: self.sort_by("status"))
        self.tree.heading("category", text="Категория", command=lambda: self.sort_by("category"))
        self.tree.heading("author", text="Автор", command=lambda: self.sort_by("author"))
        self.tree.heading("title", text="Название", command=lambda: self.sort_by("title"))

        self.tree.column("status", width=80, anchor="center")
        self.tree.column("category", width=300, anchor="w")
        self.tree.column("author", width=240, anchor="w")
        self.tree.column("title", width=620, anchor="w")

        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(fill="both", expand=True, padx=10, pady=(0, 0))
        vsb.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
        hsb.pack(fill="x", padx=10, pady=(0, 0))

        # Двойной клик — открыть файл
        self.tree.bind("<Double-1>", self.on_double_click)

        # ОДИНАРНЫЙ клик по строке — смена статуса (а шапка — сортировка)
        # Важно: отличаем region "heading" и "cell"
        self.tree.bind("<Button-1>", self.on_single_click)

        bottom = ttk.Frame(self, padding=10)
        bottom.pack(fill="x")
        ttk.Label(bottom, textvariable=self.status_text).pack(side="left")

    # ---------- Поиск ----------

    def on_search_changed(self):
        # debounce: чтобы не перерисовывать на каждый символ мгновенно
        if self._search_after_id is not None:
            try:
                self.after_cancel(self._search_after_id)
            except Exception:
                pass
        self._search_after_id = self.after(200, self.render_table)

    def clear_search(self):
        self.search_var.set("")
        self.render_table()

    # ---------- Папка ----------

    def choose_folder(self):
        folder = filedialog.askdirectory(title="Выберите папку с TXT файлами")
        if folder:
            self.folder.set(folder)
            self.reindex()

    # ---------- DB cache ----------

    def reload_cache_from_db(self):
        conn = db_connect()
        try:
            cur = conn.cursor()
            # COALESCE на случай старого поля category
            cur.execute("""
                SELECT
                    path,
                    COALESCE(categories, category, '') AS categories,
                    author,
                    title,
                    status
                FROM articles
            """)
            rows = cur.fetchall()
        finally:
            conn.close()

        self._file_records = []
        for path, cats_s, author, title, status in rows:
            cats = unpack_categories(cats_s)
            self._file_records.append({
                "path": path,
                "categories": cats,
                "author": author or "",
                "title": title or "",
                "status": int(status) if status is not None else 0
            })

    # ---------- Table render ----------

    def render_table(self):
        """Перерисовать таблицу из self._file_records с учётом поиска."""
        q = (self.search_var.get() or "").strip().lower()

        # очищаем
        self.tree.delete(*self.tree.get_children())
        self._path_to_row_iids = {}

        shown_files = 0
        shown_rows = 0

        for rec in self._file_records:
            path = rec["path"]
            categories = rec["categories"][:] if rec["categories"] else [""]  # одна строка если нет категорий
            author = rec["author"]
            title = rec["title"]
            status_symbol = STATUS_LABEL.get(rec["status"], "")

            base_name = os.path.basename(path)

            # фильтр по любому полю (самый стабильный подход)
            if q:
                hay = " ".join([
                    " ".join(categories),
                    author,
                    title,
                    base_name,
                    path
                ]).lower()
                if q not in hay:
                    continue

            shown_files += 1

            for cat in categories:
                iid = f"{path}{IID_SEP}{cat}"
                self.tree.insert(
                    "", "end", iid=iid,
                    values=(status_symbol, cat, author, title)
                )
                self._path_to_row_iids.setdefault(path, []).append(iid)
                shown_rows += 1

        self.status_text.set(f"Показано файлов: {shown_files}, строк: {shown_rows}. Поиск: '{self.search_var.get().strip()}'")

    # ---------- Индексация ----------

    def reindex(self):
        folder = self.folder.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("Папка не выбрана", "Пожалуйста, выберите существующую папку.")
            return

        if self._worker and self._worker.is_alive():
            return

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

                    cur.execute("SELECT mtime, size, status FROM articles WHERE path=?", (path,))
                    row = cur.fetchone()
                    if row and row[0] == mtime and row[1] == size:
                        total += 1
                        continue

                    categories, author, title = read_header_fields(path)
                    cats_s = pack_categories(categories)

                    old_status = row[2] if row else STATUS_NONE

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
                    """, (path, cats_s, author, title, mtime, size, int(old_status)))

                    updated += 1
                    total += 1

                except Exception:
                    errors += 1

            # удалить исчезнувшие файлы
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

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self._q.get_nowait()
                if kind == "done":
                    total, updated, deleted, errors = payload
                    self.reload_cache_from_db()
                    self.render_table()
                    msg = f"Синхронизация: файлов {total}, обновлено {updated}, удалено {deleted}"
                    if errors:
                        msg += f", ошибок {errors}"
                    self.status_text.set(msg)
                elif kind == "error":
                    self.status_text.set(payload)
        except queue.Empty:
            pass

        self.after(150, self._poll_queue)

    # ---------- Кликовые действия ----------

    def on_double_click(self, event):
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return
        path = row_id.split(IID_SEP, 1)[0]
        if os.path.exists(path):
            open_in_default_app(path)
        else:
            messagebox.showwarning("Файл не найден", "Файл был удалён или перемещён. Переиндексируйте папку.")

    def on_single_click(self, event):
        region = self.tree.identify_region(event.x, event.y)

        # Клик по шапке -> не трогаем, сортировка уже на heading command
        if region == "heading":
            return

        # Клик по строке (ячейке) -> переключаем статус
        if region == "cell":
            row_id = self.tree.identify_row(event.y)
            if not row_id:
                return

            path = row_id.split(IID_SEP, 1)[0]
            self.toggle_status(path)

            # Обновляем символ статуса во всех строках этого файла без полной перерисовки
            self.update_status_in_view(path)

            # Останавливаем дальнейшую обработку клика Treeview (чтобы не было побочных эффектов)
            return "break"

    # ---------- Статус ----------

    def toggle_status(self, path: str):
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM articles WHERE path=?", (path,))
            row = cur.fetchone()
            if not row:
                return
            status = int(row[0]) if row[0] is not None else 0
            status = (status + 1) % 3
            cur.execute("UPDATE articles SET status=? WHERE path=?", (status, path))
            conn.commit()
        finally:
            conn.close()

        # обновим в кэше self._file_records
        for rec in self._file_records:
            if rec["path"] == path:
                rec["status"] = status
                break

    def update_status_in_view(self, path: str):
        # Найдём новый символ
        new_symbol = ""
        for rec in self._file_records:
            if rec["path"] == path:
                new_symbol = STATUS_LABEL.get(rec["status"], "")
                break

        # Обновим все строки этого файла в Treeview
        for iid in self._path_to_row_iids.get(path, []):
            if self.tree.exists(iid):
                vals = list(self.tree.item(iid, "values"))
                vals[0] = new_symbol
                self.tree.item(iid, values=vals)

    # ---------- Сортировка ----------

    def sort_by(self, col: str):
        ascending = self._sort_state.get(col, True)

        def key_func(iid: str):
            vals = self.tree.item(iid, "values")  # (status, category, author, title)
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