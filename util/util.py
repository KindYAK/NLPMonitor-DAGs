def not_implemented():
    raise Exception("Not implemented")


def is_kazakh(text):
    return sum([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in text]) > 0.04
