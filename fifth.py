import csv
from math import *
def main():
    latitude = 40.667851
    longtitude = -73.984291
    with open("CSV per Month/2009-01-31.csv","r") as file:
        reader = csv.reader(file)
        next(reader, None)
        output = []
        for row in reader:
            if (float(row[1])>=10 and haversine(longtitude,latitude,float(row[3]),float(row[4]))>=4.8 and haversine(longtitude,latitude,float(row[3]),float(row[4]))<=5.2):
                output.append(row[1])
        print(output)
#todo Thelei doyleia den einai etoimo
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
