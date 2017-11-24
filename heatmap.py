import plotly.plotly as py
import plotly.graph_objs as go
import re

#only run this on the "part-r-00000" file in results/p4
filename="part-r-00000"
py.sign_in('alvtdev', '8gxrmKsZbc3C68uk60Pu')

#parses through the input file and generates a list of lists
#data[][0] = location
#data[][1] = year
#data[][-1] = conference count
def populateList():
    data = []
    file = open(filename, "r")
    for line in file:
        dat = line.split("\t")
        dat[-1] = dat[-1].strip()
        #current dataset has some non-numeric characters group in with year
        #remove all of these non-numeric characters
        a = re.sub('[^0-9]','',dat[1]) 
        dat[1] = a
        data.append(dat)
    return data

#helper function to determinr minimum year and maximum year
def getYearRange(data): 
    minYear = 2015
    maxYear = 2015
    for row in data:
        if int(row[1]) > maxYear: 
            maxYear = int(row[1])
        if int(row[1]) < minYear:
            minYear = int(row[1])
    #print str(minYear) + " " + str(maxYear)
    return minYear

def main():
    data = populateList()
    currcity = data[0][0]
    yLabel = []
    minYear = getYearRange(data)
    #getYearRange(data)
    #NOTE: year range for p4 dataset is 2011-2018
    yearCount = [0, 0, 0, 0, 0, 0, 0, 0]
    heatmap = []
    #populate heatmap
    for row in data:
        #first check if city is different, if different - append to heatmap
        if row[0] != currcity:
            yLabel.append(currcity)
            heatmap.append(yearCount)
            yearCount = [0, 0, 0, 0, 0, 0, 0, 0]
            currcity = row[0]
        #now check years and set yearCount values accordingly
        pos = int(row[1]) - minYear
        yearCount[pos] = row[-1]
    yLabel.append(currcity)
        
    #now that the heatmap has been populated, set x and y axes labels
    #(y label actually set in above loop
    xLabel = ["2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"]

    #transpose heatmap to get flipped heatmap image
    hmtransposed = [[x[i] for x in heatmap] for i in range(len(heatmap[0]))]
    #trace = go.Heatmap(z = heatmap, x = xLabel, y = yLabel)
    trace = go.Heatmap(z = hmtransposed, x = yLabel, y = xLabel)
    dat = [trace]
    py.iplot(dat, filename='p4-heatmap')
    return
    
        


if __name__ == "__main__":
    main()
