

def getitems(collection, sortterm=False):
    '''Deze functie haalt alle items op uit een MongoDB collection'''
    if sortterm:
        return collection.find().sort(sortterm)
    return collection.find()