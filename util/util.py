def not_implemented():
    raise Exception("Not implemented")


def is_kazakh(text):
    return sum([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in text]) / len(text) > 0.04
