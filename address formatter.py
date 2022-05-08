import re
import nltk

x = '3/1 Northfield grove Northfield Grove'

pat = re.compile(r'[^a-zA-Z ]+')
y=re.sub(pat, '', x)

nltk_tokens = nltk.word_tokenize(y)

ordered_tokens = set()
result = []
for word in nltk_tokens:
    if word not in ordered_tokens:
        ordered_tokens.add(word)
        result.append(word)

print(result)