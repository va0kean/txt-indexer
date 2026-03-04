import os
import sys
import threading
import queue
import time
import locale
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# --- Настройки чтения ---
POSSIBLE_ENCODINGS = ["cp1251", "utf-8", "utf-8-sig", "latin-1"]

PREFIX_CATEGORY = "Категория:"
PREFIX_AUTHOR = "Автор:"
PREFIX_TITLE = "Название:"

def read_first_three_fields(path: str):
    """
    Возвращает (category, author, title) из первых трех строк.
    Если формат нарушен — пытается извлечь максимально возможное.
    """
    last_err = None
    lines = None

    for enc in POSSIBLE_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="strict", newline="") as f:
                # читаем первые 10 строк на всякий случай (иногда бывают пустые строки)
                raw = []
                for _ in range(10):
                    s = f.readline()
                    if not s:
                        break
                    raw.append(s.rstrip("\n").rstrip("\r"))
            lines = raw
            break
        except Exception as e:
            last_err = e

    if lines is None:
        # если вообще не смогли декодировать — читаем “как получится”
        with open(path, "r", encoding="cp1251", errors="replace", newline="") as f:
            lines = [f.readline().rstrip("\n").rstrip("\r") for _ in range(3)]

    # Берем первые непустые строки (до 3)
    first = [ln.strip() for ln in lines if ln.strip()][:3]

    # По умолчанию пусто
    category = author = title = ""

    # Нормальный ожидаемый формат: ровно 3 строки с нужными префиксами
    # Но на практике могут быть отклонения — поэтому делаем “умный” парсинг.
    for ln in first:
        if ln.startswith(PREFIX_CATEGORY):
            category = ln[len(PREFIX_CATEGORY):].strip()
        elif ln.startswith(PREFIX_AUTHOR):
            author = ln[len(PREFIX_AUTHOR):].strip()
        elif ln.startswith(PREFIX_TITLE):
            title = ln[len(PREFIX_TITLE):].strip()

    # Если префиксы не нашли, но строки есть — подставим по порядку
    if not category and len(first) >= 1 and not first[0].startswith((PREFIX_AUTHOR, PREFIX_TITLE)):
        if first[0].startswith(PREFIX_CATEGORY):
            category = first[0][len(PREFIX_CATEGORY):].strip()
        else:
            # возможно файлы без префикса — тогда первая строка = категория
            category = first[0]

    if not author and len(first) >= 2:
        if first[1].startswith(PREFIX_AUTHOR):
            author = first[1][len(PREFIX_AUTHOR):].strip()
        elif not first[1].startswith((PREFIX_CATEGORY, PREFIX_TITLE)):
            author = first[1]

    if not title and len(first) >= 3:
        if first[2].startswith(PREFIX_TITLE):
            title = first[2][len(PREFIX_TITLE):].strip()
        elif not first[2].startswith((PREFIX_CATEGORY, PREFIX_AUTHOR)):
            title = first[2]

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


class TxtIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Индексатор TXT статей")
        self.geometry("1100x650")

        # Для сортировки “А-Я” используем системную локаль
        try:
            locale.setlocale(locale.LC_COLLATE, "")
        except Exception:
            pass

        self.folder = tk.StringVar(value="")
        self.status = tk.StringVar(value="Выберите папку с .txt файлами")
        self.auto_refresh = tk.BooleanVar(value=True)

        self._index_thread = None
        self._q = queue.Queue()
        self._stop_flag = False
        self._snapshot = {}  # path -> (mtime, size)

        self._build_ui()
        self.after(200, self._poll_queue)
        self.after(2000, self._auto_refresh_tick)

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Папка:").pack(side="left")
        self.entry = ttk.Entry(top, textvariable=self.folder, width=70)
        self.entry.pack(side="left", padx=8)

        ttk.Button(top, text="Выбрать…", command=self.choose_folder).pack(side="left")
        ttk.Button(top, text="Переиндексировать", command=self.reindex).pack(side="left", padx=8)

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
        hsb.pack(fill="x", padx=10, pady=(0, 0))

        # Двойной клик — открыть файл
        self.tree.bind("<Double-1>", self.on_double_click)

        bottom = ttk.Frame(self, padding=10)
        bottom.pack(fill="x")
        ttk.Label(bottom, textvariable=self.status).pack(side="left")

        ttk.Button(bottom, text="Открыть выбранный", command=self.open_selected).pack(side="right")

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

    def reindex(self):
        folder = self.folder.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("Папка не выбрана", "Пожалуйста, выберите существующую папку.")
            return

        if self._index_thread and self._index_thread.is_alive():
            # уже индексируем — не стартуем второй раз
            return

        # очистить таблицу
        for item in self.tree.get_children():
            self.tree.delete(item)

        self.status.set("Индексация…")
        self._stop_flag = False

        # обновить snapshot сразу
        self._snapshot = self._make_snapshot(folder)

        self._index_thread = threading.Thread(target=self._index_worker, args=(folder,), daemon=True)
        self._index_thread.start()

    def _index_worker(self, folder: str):
        rows = []
        total = 0
        errors = 0

        try:
            files = [f for f in os.listdir(folder) if f.lower().endswith(".txt")]
        except Exception as e:
            self._q.put(("error", f"Не удалось прочитать папку: {e}"))
            return

        files.sort()  # 0001..8500 и т.п.

        for fname in files:
            if self._stop_flag:
                break
            path = os.path.join(folder, fname)
            try:
                cat, auth, title = read_first_three_fields(path)
                rows.append((cat, auth, title, path))
                total += 1
            except Exception:
                errors += 1

            # порционная отправка в UI (каждые 200 строк)
            if len(rows) >= 200:
                self._q.put(("rows", rows))
                rows = []

        if rows:
            self._q.put(("rows", rows))

        self._q.put(("done", (total, errors)))

    def _poll_queue(self):
        try:
            while True:
                msg = self._q.get_nowait()
                kind = msg[0]

                if kind == "rows":
                    rows = msg[1]
                    for (cat, auth, title, path) in rows:
                        # path сохраняем как "values + tags" через iid:
                        iid = path  # уникально
                        self.tree.insert("", "end", iid=iid, values=(cat, auth, title))

                elif kind == "done":
                    total, errors = msg[1]
                    if errors:
                        self.status.set(f"Готово: {total} файлов. Ошибок чтения: {errors}.")
                    else:
                        self.status.set(f"Готово: {total} файлов проиндексировано.")
                elif kind == "error":
                    self.status.set(msg[1])

        except queue.Empty:
            pass

        self.after(200, self._poll_queue)

    def sort_by(self, col: str):
        # Забираем все строки и сортируем по выбранной колонке
        items = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        # Русская сортировка через locale.strxfrm (если локаль настроена в системе)
        items.sort(key=lambda t: locale.strxfrm(t[0].lower()))
        for index, (_, k) in enumerate(items):
            self.tree.move(k, "", index)

    def get_selected_path(self):
        sel = self.tree.selection()
        if not sel:
            return None
        # iid у нас = path
        return sel[0]

    def open_selected(self):
        path = self.get_selected_path()
        if not path:
            messagebox.showinfo("Не выбрано", "Выберите строку в таблице.")
            return
        if os.path.exists(path):
            open_in_default_app(path)
        else:
            messagebox.showwarning("Файл не найден", "Похоже, файл был удалён или перемещён.")

    def on_double_click(self, event):
        # Открываем по двойному клику (включая по названию)
        self.open_selected()

    def _auto_refresh_tick(self):
        if self.auto_refresh.get():
            folder = self.folder.get().strip()
            if folder and os.path.isdir(folder):
                if self._has_changes(folder):
                    # изменения есть — переиндексируем
                    self.reindex()
        self.after(2000, self._auto_refresh_tick)


if __name__ == "__main__":
    app = TxtIndexerApp()
    app.mainloop()
