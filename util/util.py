import pickle


def not_implemented():
    raise Exception("Not implemented")


def is_kazakh(text):
    return sum([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in text]) / len(text) > 0.07


def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


def save_obj(obj, name):
    pickle.dump(obj, open(name + '.pkl', 'wb'), protocol=4)
