from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pgadmin
import random

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

connection = pgadmin.makeconnection('localhost', 'huwebshop', 'postgres', '1234')
cursor = pgadmin.makecursor(connection)

weights = {'doelgroep': 0.25, 'bestcategory': 0.2, 'bestsubcategory': 0.25, 'bestbrand': 0.1, 'herhaalpreference': 0.1,
           'pricepreference': 0.1}

class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. Depending on the information it gets, it returns a
    different recommendation """

    def get(self, profileid, cat1, cat2, product, productids, page, count):
        """ This function represents the handler for GET requests coming in
        through the API. Depending on the information it gets, it returns a
        different recommendation """
        if page == 2:
            return self.similar_products(weights, product, count), 200
        elif page == 3:
            return self.boughtbyothers(weights, profileid, count), 200
        elif page == 0:
            rand = random.randint(0, 2)
            if rand == 0:
                return self.top('topviewed', count), 200
            else:
                return self.top('popular', count), 200
        elif page == 1:
            rand = random.randint(0, 2)
            if rand == 0:
                return self.same_categorie(count, cat1, cat2), 200
            else:
                return self.top('topsold', count), 200

    def top(self, table, count):
        """ This function takes the top products from the specified table out of the database
        and returns them as a list, count defines how many products it will take """
        data = pgadmin.getdata(cursor, "SELECT productid FROM " + table + " LIMIT " + str(count), False)
        top4 = []
        for productid in data:
            top4.append(productid[0])
        return top4

    def similar_products(self, weights, productid, count):
        """ This function makes a list of product-ids which are similar to the productid it gets,
        count defines how many products it will take """
        combid = pgadmin.getdata(cursor, "SELECT combid FROM prodcomb WHERE productid = '" + productid + "'")
        combprods = pgadmin.getdata(cursor, "SELECT productid FROM prodcomb WHERE combid = " + str(combid[0]) + \
                                            "AND productid != '" + productid + "'", False)
        recommendations = []
        for i in range(2):
            try:
                recommendations.append(combprods[i][0])
            except:
                break
        combinfo = pgadmin.getdata(cursor, "SELECT * FROM prodcombinations WHERE combid = " + str(combid[0]))
        allcombs = pgadmin.getdata(cursor, "SELECT * FROM prodcombinations WHERE combid != " + str(combid[0]), False)
        cap = 0.8
        while len(recommendations) < count:
            for combination in allcombs:
                total = 0
                for i in range(1, 7):
                    if combination[i] == combinfo[i]:
                        total += list(weights.values())[i-1]
                if total > cap:
                    recs = pgadmin.getdata(cursor, "SELECT productid FROM prodcomb WHERE combid = " +
                                           str(combination[0]), False)
                    if recs[0][0] not in recommendations:
                        recommendations.append(recs[0][0])
                    if len(recommendations) == count:
                        break
            cap -= 0.1
        return recommendations

    def boughtbyothers(self, weight, profileid, count):
        """ This function makes a list of product-ids which are taken from products bought by other profiles
        that look similar to the current one, count defines how many products it will take """
        alreadybought = pgadmin.getdata(cursor, f"select orderedproductid from orderedprofiles "
                                                f"where profilesprofileid='{profileid}' ", fetchone=False)
        if alreadybought != []:
            alreadybought = [item[0] for item in alreadybought]
            allbought = pgadmin.getdata(cursor, f"select * from orderedprofiles where not profilesprofileid='{profileid}'",
                                        fetchone=False)
            bought = {}
            for profile in allbought:
                if profile[0] in bought.keys():
                    bought[profile[0]].append(profile[1])
                else:
                    bought[profile[0]] = [profile[1]]
            loggedin = pgadmin.getdata(cursor, f"select * from profileproperties where profilesprofileid='{profileid}'")
            otherprofiles = pgadmin.getdata(cursor, f"select * from profileproperties "
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
            while len(recommendedproducts) < count and ind <= len(result):
                [recommendedproducts.append(product) for product in bought[result[ind][0]]
                 if product not in alreadybought and product not in recommendedproducts
                 and len(recommendedproducts) < count]
                ind += 1
            return recommendedproducts
        else:
            return self.simple_recom()

    def same_categorie(self, count, categorie, sub_categorie):
        """ This function makes a list of product-ids which are the most popular products from the specified
        (sub)category, count defines how many products it will take. It will only look at the category when there
        is no sub_category available """
        query = "SELECT popular.productid FROM popular, products, "
        ssub_categories = {'elektronica-en-media': 'Elektronica & media', 'scheren-en-ontharen': 'Scheren & ontharen',
                           'boeken-en-tijdschriften': 'Boeken & tijdschriften',  'make-up': 'Make-up',
                           'sieraden-en-bijoux': 'Sieraden & bijoux', 'make-up-accessoires': 'Make-up accessoires'}

        if sub_categorie == 'none2':
            query += "categories WHERE products.productid = popular.productid AND products.categoriescategoryid = " \
                     "categories.categoryid AND categories.category = '"
            if categorie == 'make-up-en-geuren':
                query += "Make-up & geuren' LIMIT " + str(count)
            else:
                query += categorie.capitalize().replace("-", " ").replace(" en ", " & ") + "' LIMIT " + str(count)
            data = pgadmin.getdata(cursor, query, False)

        else:
            query += "sub_categories WHERE products.productid = popular.productid AND " \
                    "products.sub_categoriessub_categoryid = sub_categories.sub_categoryid AND " \
                    "sub_categories.sub_category = '"
            if sub_categorie in ssub_categories:
                query += ssub_categories[sub_categorie] + "' LIMIT " + str(count)
            else:
                query += sub_categorie.capitalize().replace("-", " ") + "' LIMIT " + str(count)
            data = pgadmin.getdata(cursor, query, False)

        recommendations = []
        for product in data:
            recommendations.append(product[0])
        return recommendations


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:cat1>/<string:cat2>/<string:product>/<string:productids>/<int:page>/<int:count>")
