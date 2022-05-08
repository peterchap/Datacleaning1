
import pandas as pd
listOfNumbers = [1, 2, 3, 4, 5, 6]

x =  ('test1', 90)
x1 = list(x)
print(x1)
y = ['test']
y.extend(x)
print(y)
df3 = pd.DataFrame([y], columns= ['company', 'match', 'score'])
print(df3)
print(x)
for number in listOfNumbers:
    print(number)
    if (number % 2 == 0):
        print("is even")
    else:
        print("is odd")
        
print ("Hooray All done.")