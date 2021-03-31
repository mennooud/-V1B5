from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import PGAdmin

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop

connection = PGAdmin.makeconnection('localhost', 'huwebshop', 'postgres', '1234')
cursor = PGAdmin.makecursor(connection)
weights = {'doelgroep': 0.25, 'bestcategory': 0.2, 'bestsubcategory': 0.25, 'bestbrand': 0.1, 'herhaalpreference': 0.1,
           'pricepreference': 0.1}

class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self, profileid, cat1, cat2, product, productids, page, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        if cat2 == "dierverzorging":
            return self.simple_recom(), 200
        elif page == 3:
            return self.boughtbyothers(weights, profileid)
        else:
            randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])
            prodids = list(map(lambda x: x['_id'], list(randcursor)))
            return prodids, 200

    def simple_recom(self):
        data = PGAdmin.getdata(cursor, "SELECT productid FROM topSold LIMIT 4", '', False)
        top4 = []
        for productid in data:
            top4.append(productid[0])
        return top4


    def boughtbyothers(self, weight, profileid):
        alreadybought = PGAdmin.getdata(cursor, f"select orderedproductid from orderedprofiles "
                                                f"where profilesprofileid='{profileid}' ", fetchone=False)
        print(alreadybought)
        if alreadybought != []:
            alreadybought = [item[0] for item in alreadybought]
            allbought = PGAdmin.getdata(cursor, f"select * from orderedprofiles where not profilesprofileid='{profileid}'",
                                        fetchone=False)
            bought = {}
            for profile in allbought:
                if profile[0] in bought.keys():
                    bought[profile[0]].append(profile[1])
                else:
                    bought[profile[0]] = [profile[1]]
            loggedin = PGAdmin.getdata(cursor, f"select * from profileproperties where profilesprofileid='{profileid}'")
            otherprofiles = PGAdmin.getdata(cursor, f"select * from profileproperties "
                                                    f"where not profilesprofileid='{profileid}'",
                                            fetchone=False)
            result = []
            for profile in otherprofiles:
                score = 0
                for hostprop, prop, weigh in zip(loggedin[1:], profile[1:], weight.values()):
                    if hostprop is not None and prop is not None:
                        score += (hostprop == prop) * weigh
                    if score >= 0.2:
                        result.append((profile[0], round(score, 2)))
            result = sorted(result, key=lambda x: (x[1], x[0]), reverse=True)
            ind = 0
            recommendedproducts = []
            while len(recommendedproducts) < 4 and ind <= len(result):
                [recommendedproducts.append(product) for product in bought[result[ind][0]]
                 if product not in alreadybought and product not in recommendedproducts
                 and len(recommendedproducts) < 4]
                ind += 1
            return recommendedproducts
        else:
            return self.simple_recom()


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:cat1>/<string:cat2>/<string:product>/<string:productids>/<int:page>/<int:count>")
