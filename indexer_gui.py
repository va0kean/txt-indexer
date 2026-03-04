import os
import sys
import time
import queue
import locale
import sqlite3
import threading
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# -------------------- Парсинг заголовка --------------------

POSSIBLE_ENCODINGS = ["cp1251", "utf-8", "utf-8-sig", "latin-1"]

PREFIX_CATEGORY = "Категория:"
PREFIX_AUTHOR = "Автор:"
PREFIX_TITLE = "Название:"


def read_first_three_fields(path: str):
    """
    Читает первые строки и извлекает:
      Категория: ...
      Автор: ...
      Название: ...
    Возвращает (category, author, title). Если что-то отсутствует - пустая строка.
    """
    lines = None

    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="strict", newline="") as f:
                raw = []
                for _ in range(10):  # берём немного больше, на случай пустых строк
                    s = f.readline()
                    if not s:
                        break
                    raw.append(s.rstrip("\n").rstrip("\r"))
            lines = raw
            break
        except Exception:
            pass

    if lines is None:
        # Фоллбек: читаем "как получится"
        with open(path, "r", encoding="cp1251", errors="replace", newline="") as f:
            lines = [f.readline().rstrip("\n").rstrip("\r") for _ in range(3)]

    first = [ln.strip() for ln in lines if ln.strip()][:3]

    category = author = title = ""

    for ln in first:
        if ln.startswith(PREFIX_CATEGORY):
            category = ln[len(PREFIX_CATEGORY):].strip()
        elif ln.startswith(PREFIX_AUTHOR):
            author = ln[len(PREFIX_AUTHOR):].strip()
        elif ln.startswith(PREFIX_TITLE):
            title = ln[len(PREFIX_TITLE):].strip()

    # если префиксов нет, но строки есть — подставим по порядку
    if not category and len(first) >= 1:
        category = first[0].replace(PREFIX_CATEGORY, "").strip()
    if not author and len(first) >= 2:
        author = first[1].replace(PREFIX_AUTHOR, "").strip()
    if not title and len(first) >= 3:
        title = first[2].replace(PREFIX_TITLE, "").strip()

    return category, author, title


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


# -------------------- Пути и БД --------------------

def base_dir() -> Path:
    """
    Папка рядом с exe (если frozen) или рядом со скриптом (если .py).
    """
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
    """
    ВАЖНО: соединение НЕ шарим между потоками.
    Каждый поток/операция создаёт своё соединение — так избегаем ProgrammingError
    'SQLite objects created in a thread...' [1](http://pyinstaller.org/)[2](https://github.com/orgs/pyinstaller/discussions/6948)
    """
    conn = sqlite3.connect(db_file(), timeout=30)  # подождать, если база занята
    conn.execute("PRAGMA journal_mode=WAL")        # лучше для чтения+записи параллельно
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            path TEXT PRIMARY KEY,
            category TEXT,
            author TEXT,
            title TEXT,
            mtime REAL,
            size INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON articles(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_author   ON articles(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title    ON articles(title)")
    conn.commit()
    return conn


# -------------------- GUI-приложение --------------------

class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Индексатор TXT статей (с кэшем)")
        self.geometry("1100x650")

        # локаль для сортировки (А-Я)
        try:
            locale.setlocale(locale.LC_COLLATE, "")
        except Exception:
            pass

        self.folder = tk.StringVar(value="")
        self.status = tk.StringVar(value="Выберите папку с .txt файлами")
        self.auto_refresh = tk.BooleanVar(value=True)

        self._q = queue.Queue()
        self._index_thread = None
        self._stop_flag = False
        self._snapshot = {}  # path -> (mtime, size)

        self._sort_state = {"category": True, "author": True, "title": True}  # True = A->Я

        self._build_ui()

        # мгновенно загрузим то, что есть в кэше
        self.load_from_cache()

        self.after(150, self._poll_queue)
        self.after(2000, self._auto_refresh_tick)

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # -------------------- UI --------------------

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Папка:").pack(side="left")
        ttk.Entry(top, textvariable=self.folder, width=70).pack(side="left", padx=8)

        ttk.Button(top, text="Выбрать…", command=self.choose_folder).pack(side="left")
        ttk.Button(top, text="Переиндексировать", command=self.reindex).pack(side="left", padx=8)
        ttk.Button(top, text="Очистить кэш", command=self.clear_cache).pack(side="left", padx=8)

        ttk.Checkbutton(top, text="Автообновление", variable=self.auto_refresh).pack(side="left", padx=8)

        # Таблица
        columns = ("category", "author", "title")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")

        self.tree.heading("category", text="Категория", command=lambda: self.sort_by("category"))
        self.tree.heading("author", text="Автор", command=lambda: self.sort_by("author"))
        self.tree.heading("title", text="Название", command=lambda: self.sort_by("title"))

        self.tree.column("category", width=260, anchor="w")
        self.tree.column("author", width=220, anchor="w")
        self.tree.column("title", width=560, anchor="w")

        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(fill="both", expand=True, padx=10, pady=(0, 0))
        vsb.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
        hsb.pack(fill="x", padx=10)

        self.tree.bind("<Double-1>", lambda e: self.open_selected())

        bottom = ttk.Frame(self, padding=10)
        bottom.pack(fill="x")

        ttk.Label(bottom, textvariable=self.status).pack(side="left")
        ttk.Button(bottom, text="Открыть выбранный", command=self.open_selected).pack(side="right")

    # -------------------- Кэш (SQLite -> Treeview) --------------------

    def load_from_cache(self):
        """Загрузить таблицу из кэша БД (быстро)."""
        self.tree.delete(*self.tree.get_children())

        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT category, author, title, path FROM articles")
            rows = cur.fetchall()
        finally:
            conn.close()

        for (cat, auth, title, path) in rows:
            if path:
                self.tree.insert("", "end", iid=path, values=(cat, auth, title))

        self.status.set(f"Загружено из кэша: {len(rows)} записей.")

    def clear_cache(self):
        """Полностью очистить кэш индекса."""
        if messagebox.askyesno("Очистить кэш", "Удалить все записи индекса из базы?"):
            conn = db_connect()
            try:
                conn.execute("DELETE FROM articles")
                conn.commit()
            finally:
                conn.close()
            self.load_from_cache()
            self.status.set("Кэш очищен. Нажмите 'Переиндексировать'.")

    # -------------------- Выбор папки / автообновление --------------------

    def choose_folder(self):
        folder = filedialog.askdirectory(title="Выберите папку с TXT файлами")
        if folder:
            self.folder.set(folder)
            self.reindex()

    def _make_snapshot(self, folder: str):
        snap = {}
        try:
            for name in os.listdir(folder):
                if not name.lower().endswith(".txt"):
                    continue
                path = os.path.join(folder, name)
                try:
                    st = os.stat(path)
                    snap[path] = (st.st_mtime, st.st_size)
                except OSError:
                    continue
        except FileNotFoundError:
            pass
        return snap

    def _has_changes(self, folder: str):
        new_snap = self._make_snapshot(folder)
        if new_snap != self._snapshot:
            self._snapshot = new_snap
            return True
        return False

    def _auto_refresh_tick(self):
        if self.auto_refresh.get():
            folder = self.folder.get().strip()
            if folder and os.path.isdir(folder):
                if self._has_changes(folder):
                    self.reindex()
        self.after(2000, self._auto_refresh_tick)

    # -------------------- Индексация (кэш + поток) --------------------

    def reindex(self):
        folder = self.folder.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("Папка не выбрана", "Пожалуйста, выберите существующую папку.")
            return

        if self._index_thread and self._index_thread.is_alive():
            return  # уже идёт

        # сразу показываем кэш
        self.load_from_cache()
        self.status.set("Синхронизация с папкой…")

        self._stop_flag = False
        self._snapshot = self._make_snapshot(folder)

        self._index_thread = threading.Thread(target=self._index_worker, args=(folder,), daemon=True)
        self._index_thread.start()

    def _index_worker(self, folder: str):
        """
        В отдельном потоке:
        - открываем СВОЁ соединение к sqlite
        - читаем только изменённые/новые файлы по (mtime,size)
        - upsert в БД
        - удаляем из БД записи для удалённых файлов
        """
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

                    cat, auth, title = read_first_three_fields(path)

                    cur.execute("""
                        INSERT INTO articles(path, category, author, title, mtime, size)
                        VALUES(?,?,?,?,?,?)
                        ON CONFLICT(path) DO UPDATE SET
                            category=excluded.category,
                            author=excluded.author,
                            title=excluded.title,
                            mtime=excluded.mtime,
                            size=excluded.size
                    """, (path, cat, auth, title, mtime, size))

                    self._q.put(("upsert", (path, cat, auth, title)))
                    updated += 1
                    total += 1

                    if updated % 200 == 0:
                        conn.commit()

                except Exception:
                    errors += 1

            # удаляем из БД то, чего уже нет на диске
            cur.execute("SELECT path FROM articles")
            all_paths = [r[0] for r in cur.fetchall()]
            for p in all_paths:
                if p not in existing_paths:
                    cur.execute("DELETE FROM articles WHERE path=?", (p,))
                    self._q.put(("delete", p))
                    deleted += 1

            conn.commit()

        finally:
            conn.close()

        self._q.put(("done", (total, updated, deleted, errors)))

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self._q.get_nowait()

                if kind == "upsert":
                    path, cat, auth, title = payload
                    if self.tree.exists(path):
                        self.tree.item(path, values=(cat, auth, title))
                    else:
                        self.tree.insert("", "end", iid=path, values=(cat, auth, title))

                elif kind == "delete":
                    path = payload
                    if self.tree.exists(path):
                        self.tree.delete(path)

                elif kind == "done":
                    total, updated, deleted, errors = payload
                    msg = f"Готово: файлов {total}, обновлено {updated}, удалено {deleted}"
                    if errors:
                        msg += f", ошибок {errors}"
                    self.status.set(msg)

                elif kind == "error":
                    self.status.set(payload)

        except queue.Empty:
            pass

        self.after(150, self._poll_queue)

    # -------------------- Сортировка --------------------

    def sort_by(self, col: str):
        ascending = self._sort_state.get(col, True)

        items = [(self.tree.set(iid, col), iid) for iid in self.tree.get_children("")]
        items.sort(key=lambda t: locale.strxfrm((t[0] or "").lower()), reverse=not ascending)

        for idx, (_, iid) in enumerate(items):
            self.tree.move(iid, "", idx)

        self._sort_state[col] = not ascending

    # -------------------- Открытие файла --------------------

    def get_selected_path(self):
        sel = self.tree.selection()
        if not sel:
            return None
        return sel[0]  # iid = path

    def open_selected(self):
        path = self.get_selected_path()
        if not path:
            messagebox.showinfo("Не выбрано", "Выберите строку в таблице.")
            return
        if os.path.exists(path):
            open_in_default_app(path)
        else:
            messagebox.showwarning("Файл не найден", "Файл был удалён или перемещён. Нажмите 'Переиндексировать'.")

    # -------------------- Выход --------------------

    def on_close(self):
        self._stop_flag = True
        self.destroy()


if __name__ == "__main__":
    TxtIndexerApp().mainloop()