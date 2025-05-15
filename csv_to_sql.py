#!/usr/bin/env python3

# Biblioteca usadas
import os                       # os — Operações com o sistema de arquivos, como caminhos e verificação de existência
import sys                      # sys — Permite acesso aos argumentos da linha de comando e outras funções do sistema
import csv                      # csv — Leitura e escrita de arquivos CSV com suporte a diferentes delimitadores
import re                       # re — Lida com expressões regulares, como normalização de strings
from datetime import datetime   # datetime — Manipulação de datas e horas, gera prefixos e timestamps
from dateutil.parser import parse as parse_date  # dateutil.parser — Submódulo da dateutil (para manipulação de datas completas) que contém a função parse (identifica strings como datas)
import time                     # time — Mede duração de execução e fornece funções de tempo em segundos




# COMO USAR: 
# source ~/.bashrc; python csv_to_sql.py (de forma interativa) ou
# source ~/.bashrc; python csv_to_sql.py "seu_arquivo.csv" (conversão direta)
# source ~/.bashrc; python csv_to_sql.py "seu_arquivo.csv" "nome_da_tabela" (direta, com nome de tabela explícito)

# CONFIGURAÇÕES: 
SGBD=           "postgres"         # "postgres" | "mysql"
rView=          5                  # Linhas em preview
batchSize=      500                # Blocos de insert
outputPath=     "/app/backups/"    # pasta de saída





def normalize_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'[\s\-\.]+', '_', name)
    return name

def detect_delimiter(file_path):
    delimiters = [",", ";", "\t", "|"]
    with open(file_path, 'r', encoding='utf-8') as f:
        line = f.readline()
        results = {d: len(line.split(d)) for d in delimiters}
    return max(results, key=results.get)

def detect_column_type(values, sgbd):
    bool_set = {'true', 'false', 'yes', 'no', 'sim', 'não', '0', '1'}
    non_empty_values = [v.strip().lower() for v in values if v.strip()]
    type_balance = {'boolean': 0, 'date': 0, 'integer': 0, 'float': 0, 'text': 0}
    for v in non_empty_values:
        if v in bool_set:
            type_balance['boolean'] += 1
        elif v.lstrip('0') != v and len(v) > 1:  # Provável zeros à esquerda
            type_balance['text'] += 1
        elif v.replace(',', '.').replace('.', '', 1).isdigit():
            type_balance['float' if '.' in v or ',' in v else 'integer'] += 1
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', v):  # Date (ISO)
            type_balance['date'] += 1
        else:
            try:
                parse_date(v, dayfirst=True)
                type_balance['date'] += 1
            except:
                type_balance['text'] += 1
    best_type = max(type_balance, key=type_balance.get) if len(non_empty_values) == 0 or type_balance[max(type_balance, key=type_balance.get)] / len(non_empty_values) >= 0.8 else 'text'
    return {
        'mysql': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INT', 'float': 'FLOAT', 'text': 'TEXT'},
        'postgres': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INTEGER', 'float': 'REAL', 'text': 'TEXT'}
    }.get(sgbd, {}).get(best_type, 'TEXT') # fallback adicional para segurança de tipos (previne perda de dados)

def prompt(message):
    return input(message).strip()

def preview_csv(file_path, delimiter, limit):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        print(f"\n\n\033[37;44m PRÉ-VISUALIZAÇÃO DOS DADOS \033[0m (máximo {limit} linhas):")
        for i, row in enumerate(reader):
            print(" | ".join(row))
            if i + 1 >= limit:
                break

def main():
    args = sys.argv
    if len(args) == 1:
        csv_file = prompt("Digite o caminho do arquivo CSV: ")
        if not os.path.exists(csv_file):
            print("Erro: Arquivo não encontrado.")
            return
        delimiter = detect_delimiter(csv_file)
        preview_csv(csv_file, delimiter, rView)
        table_name_input = prompt("\n\033[37;44m DIGITE O NOME DA TABELA SQL \033[0m \033[37m\n(ou pressione Enter para usar o nome do arquivo):\033[0m ")
        table_name = normalize_name(table_name_input) if table_name_input else normalize_name(os.path.splitext(os.path.basename(csv_file))[0])
    elif len(args) == 2:
        csv_file = args[1]
        table_name = normalize_name(os.path.splitext(os.path.basename(csv_file))[0])
        delimiter = detect_delimiter(csv_file)
    elif len(args) == 3:
        csv_file = args[1]
        table_name = normalize_name(args[2])
        delimiter = detect_delimiter(csv_file)
    else:
        print("USO: python csv_to_sql.py \"caminho/para/arquivo.csv\" \"nome da tabela\"")
        return

    if not os.path.isfile(csv_file):
        print(f"Erro: Arquivo não encontrado ou não legível: {csv_file}")
        return

    start_time = time.time()
    sgbd = SGBD.lower()
    date_prefix = datetime.now().strftime('%d.%m.%Y')
    output_file = os.path.join(outputPath, f"{date_prefix}_{table_name}.sql")
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)
        columns = [normalize_name(h) for h in headers]
        preview_rows = [row for _, row in zip(range(10), reader)]
    column_types = {}
    for idx, col in enumerate(columns):
        values = [row[idx] for row in preview_rows if idx < len(row)]
        column_types[col] = detect_column_type(values, sgbd)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write(f"-- Gerado a partir de {csv_file} \n-- Padrão: {sgbd}\n\n")

        # CREATE TABLE
        if sgbd == "mysql":
            out.write(f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n")
            out.write("  `id` INT AUTO_INCREMENT PRIMARY KEY,\n")
            for col in columns:
                if col != 'id':
                    out.write(f"  `{col}` {column_types[col]},\n")
        elif sgbd == "postgres":
            out.write(f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n")
            out.write("  \"id\" SERIAL PRIMARY KEY,\n")
            for col in columns:
                if col != 'id':
                    out.write(f"  \"{col}\" {column_types[col]},\n")
        out.seek(out.tell() - 2)
        out.write("\n);\n\n")

        # INSERTs
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader)  # skip header
            insert_batch = []
            record_count = 0
            for row in reader:
                if len(row) != len(columns):
                    continue
                def escape(val):
                    val = val.strip()
                    if val.replace('.', '', 1).isdigit():
                        return val
                    if sgbd == 'mysql':
                        return "'" + val.replace("'", "\\'") + "'"
                    elif sgbd == "postgres":
                        return "'" + val.replace("'", "''") + "'"
                escaped = [escape(v) for v in row]
                insert_batch.append(f"({', '.join(escaped)})")
                record_count += 1
                if len(insert_batch) >= batchSize:
                    if sgbd == "mysql":
                        out.write("BEGIN;\nINSERT INTO `{}` (`{}`) VALUES\n".format(table_name, '`, `'.join(columns)))
                    elif sgbd == "postgres":
                        out.write("BEGIN;\nINSERT INTO \"{}\" (\"{}\") VALUES\n".format(table_name, "\", \"".join(columns)))
                    out.write(",\n".join(insert_batch) + ";\nCOMMIT;\n\n")
                    insert_batch = []

            if insert_batch:
                if sgbd == "mysql":
                    out.write("BEGIN;\nINSERT INTO `{}` (`{}`) VALUES\n".format(table_name, '`, `'.join(columns)))
                elif sgbd == "postgres":
                    out.write("BEGIN;\nINSERT INTO \"{}\" (\"{}\") VALUES\n".format(table_name, "\", \"".join(columns)))
                out.write(",\n".join(insert_batch) + ";\nCOMMIT;\n\n")

    duration = time.time() - start_time
    print("\n\033[37;44m ✅ CONVERSÃO CONCLUÍDA \033[0m")
    print(f"Script {sgbd} gerado em: \033[37m{output_file}\033[0m")
    print(f"Linhas processadas (dados): {record_count}")
    print(f"Todo o processo levou: {datetime.utcfromtimestamp(duration).strftime('%H:%M:%S')} ({duration:.2f} segundos)\n")






if __name__ == "__main__":
    main()
