import numpy as np
import pandas as pd
import elasticsearch as es
import requests
import json
import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split

# Import LabelEncoder
from sklearn import preprocessing

#Import scikit-learn metrics module for accuracy calculation
from sklearn import metrics

df = pd.read_csv("C:\\bouncetest.csv")

# Split dataset into training set and test set
X_train, X_test, y_train, y_test = train_test_split(df['Desc1'],df['Bounce'] , test_size=0.3,random_state=109) # 70% training and 30% test

# Creating labelEncoder
le = preprocessing.LabelEncoder()

# Converting string labels into numbers.
bounce_encoded = le.fit_transform(y_train)

# Creating Model
vectorizer = CountVectorizer()
counts = vectorizer.fit_transform(X_train)

classifier = MultinomialNB()
classifier.fit(counts, bounce_encoded)

counts_test = vectorizer.transform(X_test)
y_pred = classifier.predict(counts_test)


# Model Accuracy, how often is the classifier correct?
y_test_encoded = le.fit_transform(y_test)
print("Accuracy:",metrics.accuracy_score(y_test_encoded, y_pred))


# Test new bounce data
df2 = pd.read_csv("C:\\Users\\Peter\\Downloads\\dataclean 09022019\\bounceall.csv")

#df2 = df2.replace({r'\\':''},regex=True)
#df2 = df2.replace('"','',regex=True)

examples = df2['diagnostic'].values.astype('U')

example_counts = vectorizer.transform(examples)

df2['decision'] = classifier.predict(example_counts)
df2['decision'] = df2['decision'].replace([0,1],['HARD','SOFT'])

# Output to CSV file
df2.to_csv('c:\\Users\\Peter\\Downloads\\decision.csv')
