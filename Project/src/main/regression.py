
# coding: utf-8

# In[1]:

import graphlab as gl


# In[2]:


#data =  gl.SFrame('Output_BooksAndElectroncs.csv').csv

data = gl.SFrame.read_csv('BooksTable.csv')


# In[3]:

# Split the data as train and test set in the ratio 80:20
train_set, test_set = data.random_split(0.8, seed=1)


# In[4]:

#train_set = train_set.head(800000)
#test_set = test_set.head(200000)


# In[5]:

# Book_Id User_Id Avg_Rating_For_Book_By_AllUsers Avg_Rating_For_All_Books_By_User No_Of_Books_Read_By_User
# No_Of_Reviewers,  User_Rating_For_Book
model = gl.regression.create(train_set, target='User_Rating_For_Book', features=['Avg_Rating_For_Book_By_AllUsers', 'Avg_Rating_For_All_Books_By_User', 'No_Of_Books_Read_By_User', 'No_Of_Reviewers'])


# In[6]:

# Make predictions and evaluate results.
predictions = model.predict(test_set)


# In[7]:

predictions.head(5)


# In[8]:

#results = model.evaluate(test_set)


# In[37]:

sf = gl.SFrame()
sf['Predicted-Rating'] = [ round(value) for value in predictions]
sf['Actual-Rating'] = [ round(value) for value in test_set['User_Rating_For_Book'] ] 

predict_count = sf.groupby('Actual-Rating', [gl.aggregate.COUNT('Actual-Rating'), gl.aggregate.AVG('Predicted-Rating')])


# In[38]:

predict_count.topk('Actual-Rating', k=5, reverse=True)   


# In[39]:

total_correct_predictions_one = 0
total_correct_predictions_two = 0
total_correct_predictions_three = 0
total_correct_predictions_four = 0
total_correct_predictions_five = 0
total_actual_one = 0
total_actual_two = 0
total_actual_three = 0
total_actual_four = 0
total_actual_five = 0


# In[40]:

for frame in sf:
    #print frame['Actual-Rating']
    #print frame['Predicted-Rating']
    #break;
    if frame['Actual-Rating'] == 1:
        #print "Block 1"
        total_actual_one += 1
        #print total_actual_one
        if frame['Predicted-Rating'] == 1:
            #print "Block 1 preduction"
            total_correct_predictions_one += 1
            #print total_correct_predictions_one
            #break;
    elif frame['Actual-Rating'] == 2:
        total_actual_two += 1
        if frame['Predicted-Rating'] == 2:
            total_correct_predictions_two += 1
    elif frame['Actual-Rating'] == 3:
        total_actual_three += 1
        if frame['Predicted-Rating'] == 3:
            total_correct_predictions_three += 1
    elif frame['Actual-Rating'] == 4:
        total_actual_four += 1
        if frame['Predicted-Rating'] == 4:
            total_correct_predictions_four += 1
    elif frame['Actual-Rating'] == 5:
        total_actual_five += 1
        if frame['Predicted-Rating'] == 5:
            total_correct_predictions_five += 1
        


# In[41]:

print total_correct_predictions_one
print total_actual_one


# In[43]:

print "Accuracy of rating 1: " + str(total_correct_predictions_one*1.0/total_actual_one*100)
print "Accuracy of rating 2: " + str(total_correct_predictions_two*1.0/total_actual_two*100)
print "Accuracy of rating 3: " + str(total_correct_predictions_three*1.0/total_actual_three*100)
print "Accuracy of rating 4: " + str(total_correct_predictions_four*1.0/total_actual_four*100)
print "Accuracy of rating 5: " + str(total_correct_predictions_five*1.0/total_actual_five*100)
print "Total Accuracy of ratings :" + str((total_correct_predictions_one + total_correct_predictions_two + total_correct_predictions_three
                                        + total_correct_predictions_four + total_correct_predictions_five)*1.0/
                                        (total_actual_one + total_actual_two + total_actual_three + total_actual_four +
                                         total_actual_five)*100)


# In[33]:

model.show()

