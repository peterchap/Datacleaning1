
import pandas as pd

import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib,pickle
# Import LabelEncoder
from sklearn import preprocessing

#Import scikit-learn metrics module for accuracy calculation
from sklearn import metrics

directory = "c:/Users/Peter/Downloads/graduation work/"

df = pd.read_csv("C:\\new bounce model data.csv")


print(df.shape)

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

# Output a pickle file for the model
pickle.dump(vectorizer, open("vectorizer.pickle", "wb"))
joblib.dump(classifier, 'C:\\bounce_model.pkl') 
