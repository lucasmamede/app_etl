import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import sqlalchemy
import urllib
import re

class ETLApp:
    def __init__(self, master):
        self.master = master
        master.title("ETL")
        master.geometry("1200x700")

        # Conexão com SQL Server
        self.server = 'BHSBI-H5XW643\\SQLEXPRESS'
        self.database = 'BHSTST'
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection=yes;"
        )
        self.engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        self.df = None
        self.transformation_widgets = []

        tk.Label(master, text="Nome da Tabela:").pack(pady=5)
        self.table_var = tk.StringVar()
        self.table_entry = tk.Entry(master, textvariable=self.table_var)
        self.table_entry.pack(pady=5)

        self.btn_extract = tk.Button(master, text="Extract - Carregar dados", command=self.extract_data)
        self.btn_extract.pack(pady=5)

        # Frame com Canvas e Scrollbar para os campos de transformação
        self.transformation_container = tk.Frame(master)
        self.transformation_container.pack(pady=10, fill='both', expand=True)

        self.transformation_canvas = tk.Canvas(self.transformation_container)
        self.transformation_scrollbar = ttk.Scrollbar(self.transformation_container, orient="vertical", command=self.transformation_canvas.yview)
        self.transformation_scrollbar.pack(side="right", fill="y")

        self.transformation_canvas.configure(yscrollcommand=self.transformation_scrollbar.set)
        self.transformation_canvas.pack(side="left", fill="both", expand=True)

        self.transformation_frame = tk.Frame(self.transformation_canvas)
        self.transformation_canvas.create_window((0, 0), window=self.transformation_frame, anchor="nw")

        def update_scrollregion(event):
            self.transformation_canvas.configure(scrollregion=self.transformation_canvas.bbox("all"))

        self.transformation_frame.bind("<Configure>", update_scrollregion)

        self.btn_apply_transform = tk.Button(master, text="Aplicar Transformações", command=self.apply_transformations)
        self.btn_apply_transform.pack(pady=5)

        self.btn_apply_conversion = tk.Button(master, text="Aplicar Conversões", command=self.apply_conversions)
        self.btn_apply_conversion.pack(pady=5)

        self.btn_load = tk.Button(master, text="Load - Salvar alterações", command=self.load_data)
        self.btn_load.pack(pady=5)

        self.tree_frame = tk.Frame(master)
        self.tree_frame.pack(expand=True, fill='both')

        self.tree = ttk.Treeview(self.tree_frame, show="headings")
        self.tree.grid(row=0, column=0, sticky='nsew')

        self.tree_scroll_y = ttk.Scrollbar(self.tree_frame, orient='vertical', command=self.tree.yview)
        self.tree_scroll_y.grid(row=0, column=1, sticky='ns')

        self.tree_scroll_x = ttk.Scrollbar(self.tree_frame, orient='horizontal', command=self.tree.xview)
        self.tree_scroll_x.grid(row=1, column=0, sticky='ew')

        self.tree.configure(yscrollcommand=self.tree_scroll_y.set, xscrollcommand=self.tree_scroll_x.set)

        self.tree_frame.rowconfigure(0, weight=1)
        self.tree_frame.columnconfigure(0, weight=1)

    def extract_data(self):
        table_name = self.table_var.get().strip()
        if not table_name:
            messagebox.showerror("Erro", "Por favor, informe o nome da tabela.")
            return

        try:
            query = f"SELECT * FROM {table_name}"
            self.df = pd.read_sql_query(query, self.engine)
            self.update_treeview()
            self.generate_transformation_fields()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao carregar dados: {e}")

    def generate_transformation_fields(self):
        for widget in self.transformation_frame.winfo_children():
            widget.destroy()
        self.transformation_widgets.clear()

        tk.Label(self.transformation_frame, text="Transformações e Conversões por Coluna:", font=("Arial", 10, "bold")).pack(anchor='w')

        for col in self.df.columns:
            frame = tk.Frame(self.transformation_frame)
            frame.pack(fill='x', pady=2)

            tk.Label(frame, text=col, width=20, anchor='w').pack(side='left')

            trans_var = tk.StringVar()
            if pd.api.types.is_string_dtype(self.df[col]):
                trans_options = ["Nenhum", "Maiúsculas", "Remover caracteres especiais", "Formatar CPF"]
            else:
                trans_options = ["Nenhum"]

            trans_combo = ttk.Combobox(frame, textvariable=trans_var, values=trans_options, width=25)
            trans_combo.set("Nenhum")
            trans_combo.pack(side='left', padx=5)

            conv_var = tk.StringVar()
            conv_options = ["STRING", "VARCHAR", "INT", "DECIMAL"]

            col_data = self.df[col]
            if pd.api.types.is_string_dtype(col_data):
                max_len = col_data.dropna().map(len).max() if not col_data.dropna().empty else 50
                conv_default = "VARCHAR"
            elif pd.api.types.is_integer_dtype(col_data):
                conv_default = "INT"
                max_len = None
            elif pd.api.types.is_float_dtype(col_data):
                conv_default = "DECIMAL"
                max_len = None
            else:
                conv_default = "STRING"
                max_len = None

            conv_combo = ttk.Combobox(frame, textvariable=conv_var, values=conv_options, width=15)
            conv_combo.set(conv_default)
            conv_combo.pack(side='left', padx=5)

            param_frame = tk.Frame(frame)
            param_frame.pack(side='left')

            param1_var = tk.StringVar()
            param2_var = tk.StringVar()

            if conv_default == "VARCHAR" and max_len:
                param1_var.set(str(max_len))

            def on_conv_change(event=None, param_frame=param_frame, conv_var=conv_var, param1_var=param1_var, param2_var=param2_var):
                for widget in param_frame.winfo_children():
                    widget.destroy()

                sel = conv_var.get()

                if sel == "VARCHAR":
                    tk.Label(param_frame, text="Tamanho:").pack(side='left')
                    tk.Entry(param_frame, textvariable=param1_var, width=5).pack(side='left')

                elif sel == "DECIMAL":
                    tk.Label(param_frame, text="Tamanho:").pack(side='left')
                    tk.Entry(param_frame, textvariable=param1_var, width=5).pack(side='left')

                    tk.Label(param_frame, text="Casas Decimais:").pack(side='left')
                    tk.Entry(param_frame, textvariable=param2_var, width=5).pack(side='left')

            conv_combo.bind("<<ComboboxSelected>>", on_conv_change)
            on_conv_change()

            self.transformation_widgets.append((col, trans_var, conv_var, param1_var, param2_var))

    def update_treeview(self):
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = list(self.df.columns)
        for col in self.df.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, anchor='w')

        for _, row in self.df.iterrows():
            self.tree.insert("", "end", values=list(row))

    def apply_transformations(self):
        if self.df is None:
            messagebox.showerror("Erro", "Nenhum dado carregado para transformação.")
            return

        for col, trans_var, _, _, _ in self.transformation_widgets:
            trans = trans_var.get()

            if trans == "Maiúsculas":
                self.df[col] = self.df[col].astype(str).str.upper()
            elif trans == "Remover caracteres especiais":
                self.df[col] = self.df[col].astype(str).apply(lambda x: re.sub(r'[^A-Za-z0-9 ]+', '', x))
            elif trans == "Formatar CPF":
                self.df[col] = self.df[col].astype(str).apply(self.formatar_cpf)

        messagebox.showinfo("Sucesso", "Transformações aplicadas com sucesso.")
        self.update_treeview()

    def apply_conversions(self):
        if self.df is None:
            messagebox.showerror("Erro", "Nenhum dado carregado para conversão.")
            return

        for col, _, conv_var, param1_var, param2_var in self.transformation_widgets:
            conv = conv_var.get()
            param1 = param1_var.get()
            param2 = param2_var.get()

            try:
                if conv in ["Texto", "STRING"]:
                    self.df[col] = self.df[col].astype(str)
                elif conv in ["Numérico", "INT"]:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
                elif conv == "VARCHAR":
                    max_len = int(param1) if param1.isdigit() else None
                    if max_len:
                        self.df[col] = self.df[col].astype(str).str.slice(0, max_len)
                elif conv == "DECIMAL":
                    scale = int(param2) if param2.isdigit() else None
                    if scale is not None:
                        self.df[col] = pd.to_numeric(self.df[col], errors='coerce').round(scale)
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao converter coluna '{col}': {e}")

        messagebox.showinfo("Sucesso", "Conversões aplicadas com sucesso.")
        self.update_treeview()

    def formatar_cpf(self, cpf):
        nums = re.sub(r'\D', '', cpf)
        if len(nums) == 11:
            return f"{nums[:3]}.{nums[3:6]}.{nums[6:9]}-{nums[9:]}"
        else:
            return cpf

    def load_data(self):
        table_name = self.table_var.get().strip()
        if not table_name:
            messagebox.showerror("Erro", "Por favor, informe o nome da tabela.")
            return

        if self.df is not None:
            try:
                self.df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                messagebox.showinfo("Carga", f"Dados salvos com sucesso na tabela '{table_name}'.")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao salvar dados: {e}")
        else:
            messagebox.showerror("Erro", "Nenhum dado para carregar.")

if __name__ == "__main__":
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()
