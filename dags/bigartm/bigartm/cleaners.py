def get_wordnet_pos(word):
    import nltk
    from nltk.corpus import wordnet

    """Map POS tag to first character lemmatize() accepts"""
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}
    return tag_dict.get(tag, wordnet.NOUN)


def lemmatizer_func(sentence):
    from nltk.stem.wordnet import WordNetLemmatizer
    import nltk
    lemmatizer = WordNetLemmatizer()  # лемматизатор из WOrdNet
    sentence = sentence.replace(',', '').replace('.', '').replace(')', '').replace('(', '').replace(':', '')
    sentence = sentence.replace(';', '').replace('-', '')
    return [lemmatizer.lemmatize(w, get_wordnet_pos(w)) for w in nltk.word_tokenize(sentence)]


def stop_words_remover(sentence):  # удаляет стоп слова
    from nltk.corpus import stopwords
    stop_words = stopwords.words('english')  # получим словарь стоп-слов
    return [word for word in sentence if word not in stop_words]


def return_cleaned_array(array):  # комбинирует наши веhхние функции выдает очищенный массив
    import re
    array1 = []
    for i in array:
        lemma = lemmatizer_func(i)
        Lemma = [word.lower() for word in lemma]
        array1.append(stop_words_remover(Lemma))
    # *********************************************
    array2 = []
    for i in array1:
        array2.append(str(i).replace(']', '').replace('[', '').replace("'", '').replace(',', ''))
    # *********************************************
    # import re
    regex = re.compile('[^a-zA-Z]')
    final_array = []
    for i in array2:
        final_array.append(regex.sub(' ', i))
    return final_array


def txt_writer(data, filename):  # ЗАПИСЫВАЕТ НАШИ МАССИВЫ В TXT FILE
    # saving collection in txt file
    out_file = open(filename, "w")
    for i in data:
        # write line to output file
        out_file.write(str(i))
        out_file.write("\n")
    out_file.close()
