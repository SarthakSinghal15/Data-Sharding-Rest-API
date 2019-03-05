import requests
import copy
import sys
import csv
import hashlib
import json
class Hashring():

    def __init__(self,servers):
        self.servers=servers
        self.serverkeys=[]
        self.serverdict={}
        if servers:
            for i in servers:
                ser_h_key=self.add_server(i)
                self.serverdict[ser_h_key]=i
        print("Servers have been added to the ring")
    def add_server(self, server):
        hashobj2 = hashlib.sha256(server.encode('utf-8'))
        val_hex2 = hashobj2.hexdigest()                
        hserver= int(val_hex2, 16)%10000000000000000
        self.serverkeys.append(hserver)
        self.serverkeys.sort()
        return hserver
    def add_data(self, row):
        year=row[0]
        causes=row[2]
        state=row[3]
        data_b=",".join(row)
        key=""
        key= ":".join([year,causes,state])
        hashobj = hashlib.sha256(key.encode('utf-8'))
        val_hex = hashobj.hexdigest()                
        hashkey= int(val_hex, 16)%10000000000000000
        if (hashkey > self.serverkeys[-1]):
            link  = self.serverdict[self.serverkeys[0]]
            link += '/api/v1/entries'
            requests.post(link, json = {hashkey : data_b})
            
        else:
            for i in self.serverkeys:
                if hashkey<i:
                    link  = self.serverdict[i]
                    link += '/api/v1/entries'
                    requests.post(link, json = {hashkey : data_b})
                    break
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