import csv
from datetime import datetime

def main():
    with open("output1.csv","r") as file:
        csv_reader = csv.reader(file)
        rows= list(csv_reader)
        
        print(findPopularHours(rows))
        print(findPopularQuarter(rows))
    file.close()

def findPopularHours(rows):
    hoursCount = {}
    datetimes = []
    for x in range(0,25):
        hoursCount[x] = 0
    for row in rows:
        row[0] = row[0].rsplit(".",1)[0]
        datetimes.append(datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'))
    for x in datetimes:
        for h in range(0,25):
            if(x.hour == h):
                hoursCount[h] +=1
    return [k for k, v in sorted(hoursCount.items(), key=lambda item: item[1], reverse=True)[:5]]

def findPopularQuarter(rows):
    quarterCount = {"Q1":0, "Q2":0, "Q3":0, "Q4":0}

    for i in rows:
        if(float(i[4])>=40.735923 and float(i[3])>=-73.990294):
            quarterCount["Q1"] += 1
        elif (float(i[4])<40.735923 and float(i[3])>=-73.990294):
            quarterCount["Q2"] += 1
        elif (float(i[4])<40.735923 and float(i[3])<-73.990294):
            quarterCount["Q3"] += 1
        else:
            quarterCount["Q4"] += 1
    return quarterCount


main()