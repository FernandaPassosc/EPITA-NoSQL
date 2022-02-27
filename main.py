import pymongo as pymongo
from bson import ObjectId
from pymongo.server_api import ServerApi

def connectDB():

    def printCountries():
        for country in countries.find():
            print(country)

    def createCountry(name, isoCode, continent, population):
        country = {
            "name": name,
            "IsoCode": isoCode,
            "continent": continent,
            "population": population
        }
        newCountry = countries.insert_one(country)
        if continent is not None:
            continents.update_one({"_id": ObjectId(continent)}, {
                '$addToSet': {
                    'countries': newCountry['id']
                }
            })

        print(newCountry)

    def updateCountry(id, country):
        updatedCountry = countries.update_one({"_id": ObjectId(id)}, {"$set": country})
        if country.get('continent') is not None:
            continents.update_one({"_id": ObjectId(country['continent'])}, {
                '$addToSet': {
                    'countries': id
                }
            })
        print(updatedCountry)

    def printContinents():
        for continent in continents.find():
            print(continent)

    def createContinent(name):
        continent = {
            "name": name,
            "countries": []
        }
        newContinent = continents.insert_one(continent)

        print(newContinent)

    def updateContinent(id, continent):
        newContinent = continents.update_one({"_id": ObjectId(id)}, {"$set": continent})

        print(newContinent)


    # QUESTION 01
    def findCountryByLetters():
        letters = input()
        for country in countries.find({
           "name": {"$regex": letters}
        }):
            print(country)


    # QUESTION 03
    def findContinentWithCount():
        for continent in continents.aggregate([
            {
                "$addFields": {
                    "count": { "$size": { "$ifNull": ["$countries", []]} }
                }
            }
        ]):
            print(continent)


     # QUESTION 04
    def findSortedContinentCountries(id):
        for continent in continents.aggregate([
        {
             "$match": {"_id": ObjectId(id)}
        },
        {
            "$lookup": {
                "from": "countries",
                "let": {"countries": "$countries"},
                "pipeline": [
                    {
                        "$limit": 4
                    },
                    {
                        "$sort": {"name": 1}
                    }
                ],
                "as": "countries"
            }
        }]):
            print(continent['countries'])


    # QUESTION 6
    def findCountrySortedByPopulation():
        for country in countries.aggregate([{
           "$sort": {"population": 1}
        }]):
            print(country)

    # QUESTION 7
    def findCountryBiggerThan():
        for country in countries.find({
            "name": {"$regex": 'u'},
            "population": {"$gt": 100000}
        }):
            print(country)

    # CONECTION TO THE DB
    client = pymongo.MongoClient(
        "mongodb+srv://FernandaCirino:MegLinda@cluster0.6zm6l.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", server_api=ServerApi('1'))
    db = client['test']
    print(db.list_collection_names())

    countries = db.countries
    continents = db.continents
    printCountries()
    printContinents()


if __name__ == '__main__':
    connectDB()






