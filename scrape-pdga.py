from bs4 import BeautifulSoup
import urllib2

def scrapePlayerNameFromRow(row):
	player = row.find("td", {"class": "player"})

def scrapePlayerPdgaNumberFromRow(row):
	pdgaNumberElement = row.find("td", {"class": "pdga-number"})
	print pdgaNumberElement

def scrapeResultsFromRow(row):
	scrapePlayerNameFromRow(row)
	scrapePlayerPdgaNumberFromRow(row)

def rowContainsResults(row):
	return row.find("td", {"class": "pdga-number"}) is not None

def scrapeResultsFromTable(table):
	for row in table.find_all('tr'):
		if (rowContainsResults(row)):
			scrapeResultsFromRow(row)

def getSoupFromUrl(url):
	header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
	req = urllib2.Request(url, headers=header)
	page = urllib2.urlopen(req)
	return BeautifulSoup(page)

def scrapeResultsFromUrl(url):
	soup = getSoupFromUrl(url)
	tables = soup.findAll("table", { "class" : "results" })
	for table in tables:
		scrapeResultsFromTable(table)


scrapeResultsFromUrl("http://www.pdga.com/tour/event/10003")

