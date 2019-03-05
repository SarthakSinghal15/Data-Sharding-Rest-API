from flask import Flask
from flask import request
from flask import Response
from flask_restful import reqparse, abort, Api, Resource
import collections
import json
import sys

app = Flask(__name__)
api = Api(app)
num=0
datastore = {}
list1=[]


class TodoSimple(Resource):
    def get(self):        
        out = ""
        '''list1=[]    
        for key, value in datastore.items():
            output={}
            output[key]=value
            list1.append(output)
            out+= str(output)
            out+=','
            '''
        final = {}
        final["num_of_entries"] = num
        final["entries"] = list1
        final = json.dumps(final,indent=2)
        return Response("{}\n".format(final), status = 200)

    def post(self):
        global num
        num = num+1
        val = request.get_json()
        list1.append(val)
        '''for key, value in val.items():
            datastore[key]=value'''
        final="201 created\n"
        return Response(final,status = 201)

api.add_resource(TodoSimple, '/api/v1/entries')

if __name__ == "__main__":
    port = sys.argv[1]
    host="0.0.0.0"
    app.run(host, port, debug=True)