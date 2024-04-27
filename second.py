import csv
from math import radians, cos, sin, asin, sqrt

def main():
    with open("CSV per Month/2009-01-31.csv","r") as file:
        reader = csv.reader(file)
        next(reader, None)
        output = []
        for row in reader:
            if float(row[1])>=10 and int(row[7])>=2 and haversine(float(row[3]),float(row[4]),float(row[5]),float(row[6]))>=1:
                output.append(row)
                print(haversine(float(row[3]),float(row[4]),float(row[5]),float(row[6])))
        with open('output1.csv','w',newline='') as file2:
            writer = csv.writer(file2)
            for i in output:
                writer.writerow(i)
            file2.close()
        file.close()            
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r
main()