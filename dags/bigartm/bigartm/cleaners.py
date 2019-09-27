def lemmatizer_func(sentence):
    import pymorphy2
    from textblob import TextBlob
    from nltk.corpus import stopwords

    stopwords = stopwords.words('russian')

    morph = pymorphy2.MorphAnalyzer()
    sentence = sentence.replace(',', '')\
        .replace('.', '').replace(')', '')\
        .replace('(', '').replace(':', '')\
        .replace('/', '').replace('\\', '')\
        .replace('?', '').replace('!', '')\
        .replace(';', '').replace('-', '').replace('|', '')\
        .replace(']', '').replace('[', '')\
        .replace("'", '').replace('"', '')
    return [morph.parse(w)[0].normal_form.lower() for w in TextBlob(sentence).words if morph.parse(w)[0].normal_form.lower() not in stopwords]


def return_cleaned_array(documents):  # комбинирует наши веhхние функции выдает очищенный массив
    array = []
    for i in documents:
        lemma = lemmatizer_func(i)
        array.append(" ".join(lemma))
    return array


def txt_writer(data, filename):  # ЗАПИСЫВАЕТ НАШИ МАССИВЫ В TXT FILE
    # saving collection in txt file
    out_file = open(filename, "w", encoding="utf-8")
    for i in data:
        # write line to output file
        out_file.write(str(i))
        out_file.write("\n")
    out_file.close()
