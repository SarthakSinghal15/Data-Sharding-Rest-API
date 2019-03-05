import requests
import copy
import sys
import csv
import hashlib
import json
class Hashring():

    def __init__(self,servers):
        self.servers=servers
        self.server_key_pair=[]
        self.serverdict={}
        print("Servers have been added to the ring")
    
    def add_data(self, row):
        year=row[0]
        causes=row[2]
        state=row[3]
        data_b=",".join(row)
        key=""
        key= ":".join([year,causes,state])
        server_key_pair=[]
        for i in self.servers:
            renkey=key+","+i
            hashobj = hashlib.sha256(renkey.encode('utf-8'))
            val_hex = hashobj.hexdigest()                
            hashkey= int(val_hex, 16)
            server_key_pair.append(hashkey)
            self.serverdict[hashkey]=i
        final_key=max(server_key_pair)
        link  = self.serverdict[final_key]
        link += '/api/v1/entries'
        resobj=requests.post(link, json = {final_key : data_b})
        x = resobj.content.decode()
        #print(x)

    
    def retrieve_data(self):
        g=[]
        for i in self.servers:
            link=i
            link+='/api/v1/entries'
            r = requests.get(link)
            x = r.content.decode()
            g.append(eval(x))
            print("GET {}".format(i))
            print(json.dumps(eval(x), indent=2))
            print("\n\n")
        #print(g[0]['num_of_entries'])
        #print(g[1]['num_of_entries'])
        #print(g[2]['num_of_entries'])
        #print(g[3]['num_of_entries'])
        

def test(filename):
    servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']
    ring = Hashring(servers)
   
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = -1
        for row in csv_reader:
            if line_count == -1:
                line_count += 1
            else:
                ring.add_data(row)
                line_count += 1
        print("Uploaded all {} entries.".format(line_count))
        print("Verifying the data.")
        ring.retrieve_data()

if __name__ == "__main__":
    filename=sys.argv[1]
    test(filename)