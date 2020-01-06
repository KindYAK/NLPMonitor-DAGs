def return_cleaned_array(documents):  # комбинирует наши веhхние функции выдает очищенный массив
    import re
    array = []
    for document in documents:
        cleaned_doc = " ".join(x.lower().strip() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', document).split()).split())
        array.append(cleaned_doc)
    return array


def txt_writer(data, filename):  # ЗАПИСЫВАЕТ НАШИ МАССИВЫ В TXT FILE
    # saving collection in txt file
    out_file = open(filename, "w", encoding="utf-8")
    for i in data:
        # write line to output file
        out_file.write(str(i))
        out_file.write("\n")
    out_file.close()
