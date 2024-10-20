#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    DB_NAME = "cs4250_project2"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try: 
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        print("Database connected succesfully")

        return db
    
    except:
        print("Error connecting to database")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.

    # splitting doctext into words array by space
    words = docText.split(" ")
    # making each word lowercase
    lowercase_words = [word.lower() for word in words]
    # create a dictionary to count occurrences
    word_count = {}
    for word in lowercase_words:
        # remove punctuation
        word = word.strip('?!,.')
        # count number of occurrences of each word
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    term_data = []
    for word, count in word_count.items():
        term_data.append({
            'term': word,
            'count': count,
            'num_chars': len(word)
        })

    #Producing a final document as a dictionary including all the required fields
    document = {
        "_id": docId,
        "title": docTitle,
        "text": docText,
        "num_chars": len(docText.strip('?!,. ')),
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": term_data 
    }

    # Insert the document
    col.insert_one(document)

    # print a message if successful
    print("Created document successfully")

def deleteDocument(col, docId):
    
    # Delete the document from the database
    col.delete_one({"_id": docId})

    # print a message if successful
    print(f"Deleted document %s successfully" % (docId))    

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    print("update document")
    # Delete the document
    col.delete_one({"_id": docId})

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    print("get index")
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    
    pipeline = [
        {"$unwind": {'$terms'}},
        {"$group": {"_id": null, "total": {"$sum": "$terms.count"}}},
    ]

    # print("index")