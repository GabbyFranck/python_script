import json
import pandas as pd
import numpy as np
import os
import math

def search_new_files(path: str, file_to_search: str):
    file_arr = []

    for file in os.listdir(path):
        file_name = file.split(".")[0]
        extension = file.split(".")[-1]

        if file_name.startswith(file_to_search) and extension == "txt":
            file_arr.append(file)

    return file_arr

def save_original_files(original_path: str, original_files: list):
    for arq in original_files:
        os.rename(arq, f"./{original_path}/{arq}")

def generate_json_parser(column_parser: dict):
    try:
        with open('column_parser.json', 'w', encoding='utf-8') as f:
            json.dump(column_parser, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print("Error trying generate json parser:"+str(e))

def find_f1(expression: str)->int:
    if expression.find("(f1,") >= 0:
        return expression.find("(f1,")
    elif expression.find("(F1,") >= 0:
        return expression.find("(F1,")
    else:
        raise "Erro ao tentar encontrar F1"

def read_de_para(de_para_path: str):
    with open(de_para_path, "r") as de_para:
        column_parser = {
            f[0:f.find("=")].strip("\n").strip("\t\xa0"): f[find_f1(f):f.find(")")].strip("\n").strip("\t").replace("(f1,", "").replace("(F1,", "")
            for f in de_para.readlines()
        }

    return column_parser

def verify_row(column:str, readed_row:str):
    add_row_response = None
    column = column.strip()

    if column == "REPORT_IDTFIR" and "S" in readed_row:
        add_row_response = None

    elif column == "TRS_DATE" and not readed_row.strip():
        add_row_response = None

    else:
        add_row_response = "yes"

    return add_row_response    

def make_rows(column_parser:dict, line:str, final_columns:list, final_rows:list):

    for column, numbers in column_parser.items():
        if column not in final_columns:
            final_columns.append(column)

        start_line = int(numbers.split(",")[0])
        end_line = int(numbers.split(",")[1])
        readed_row = line[start_line-1:(start_line+end_line)-1]
        add_row = verify_row(column, readed_row)
        if not add_row:
            return None

        final_rows.append(readed_row)
    
    return final_rows

def main():
    de_para = "./depara/depara_visa.txt"
    files_path = "./"
    original_path = "./original/"
    processados_path = "./processados/"
    size = 40
    file_to_search = "New_System"

    column_parser = read_de_para(de_para)
    
    new_files = search_new_files(files_path, file_to_search)

    for readed_file in new_files:
        files_to_chunk = int(math.ceil(os.path.getsize(readed_file) / ((size * 1024)*1024)))

        with open(readed_file, "r") as file:
            final_columns = []
            final_values = []

            for line in file.readlines():
                final_rows = []

                final_rows = make_rows(column_parser, line, final_columns, final_rows)
                
                final_values.append(final_rows) if final_rows else None

            for idx, row in enumerate(final_values):
                final_values[idx][-1] = final_values[idx][-1].replace("\n", "")

        df = pd.DataFrame(data=final_values, columns=final_columns)

        print(df.head(10))
        print(len(df))

        if len(df) > 0:
            for idx, chunk in enumerate(np.array_split(df, files_to_chunk)):
                final_name = readed_file.split(".")[0]
                cont = idx + 1
                chunk.to_csv(f'{processados_path}{final_name}_{cont}.txt', index=False, sep=";")

        save_original_files(original_path, new_files)

    #input_df = spark.createDataFrame(df)
    #input_df.write.mode("overwrite").saveAsTable(f"sap_raw.AVNK")

if __name__ == '__main__':
    main()