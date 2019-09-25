def lemmatizer_func(sentence):
    import pymorphy2
    from textblob import TextBlob

    morph = pymorphy2.MorphAnalyzer()
    sentence = sentence.replace(',', '')\
        .replace('.', '').replace(')', '')\
        .replace('(', '').replace(':', '')\
        .replace('/', '').replace('\\', '')\
        .replace('?', '').replace('!', '')\
        .replace(';', '').replace('-', '')
    return [morph.parse(w)[0].normal_form for w in TextBlob(sentence).words]


def return_cleaned_array(array):  # комбинирует наши веhхние функции выдает очищенный массив
    import re
    array1 = []
    for i in array:
        lemma = lemmatizer_func(i)
        Lemma = [word.lower() for word in lemma]
        array1.append(Lemma)
    # *********************************************
    array2 = []
    for i in array1:
        array2.append(str(i).replace(']', '').replace('[', '').replace("'", '').replace(',', ''))
    return array2


def txt_writer(data, filename):  # ЗАПИСЫВАЕТ НАШИ МАССИВЫ В TXT FILE
    # saving collection in txt file
    out_file = open(filename, "w", encoding="utf-8")
    for i in data:
        # write line to output file
        out_file.write(str(i))
        out_file.write("\n")
    out_file.close()
