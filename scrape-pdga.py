from bs4 import BeautifulSoup
import urllib2
import mysql.connector
import time
import random

fetchDelayIntervalMinSeconds = 5
fetchDelayIntervalMaxSeconds = 15

# Path to the location where we will cache a copy of every single page we fetch.
CACHE_PATH = "/Volumes/ninja/programming/PdgaDataScraperCache/"

###############################################################################

# Connect to DB

database = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='pdga')
cursor = database.cursor()


###############################################################################


def scrapePlayerNameFromRow(row):
	playerElement = row.find("td", {"class": "player"})
	return playerElement.getText()


def scrapePlayerPdgaNumberFromRow(row):
	pdgaNumberElement = row.find("td", {"class": "pdga-number"})
	return pdgaNumberElement.getText()


def scrapePlayerRatingFromRow(row):
	playerRatingElement = row.find("td", {"class": "player-rating propagator"})
	if playerRatingElement is None:
		return ""
	return playerRatingElement.getText()


def scrapeTdBodyFromRowByClass(row, className):
	elt = row.find("td", {"class": className})
	if elt is None:
		return ""
	return elt.getText()

def scrapePlaceFromRow(row):
	return scrapeTdBodyFromRowByClass(row, "place")


def scrapePrizeFromRow(row):
	return scrapeTdBodyFromRowByClass(row, "prize")


def scrapePointsFromRow(row):
	return scrapeTdBodyFromRowByClass(row, "points")


def scrapeTotalScoreFromRow(row):
	return scrapeTdBodyFromRowByClass(row, "total")


def getScoreArrayFromRow(row):
	roundScoreElements = row.findAll("td", {"class": "round"})
	scores = []
	for scoreElt in roundScoreElements:
		scores.append(scoreElt.getText())
	return scores


def getRatingArrayFromRow(row):
	roundRatingElements = row.findAll("td", {"class": "round-rating"})
	ratings = []
	for ratingElt in roundRatingElements:
		ratings.append(ratingElt.getText())
	return ratings


def scrapeScoresAndRatingsFromRow(row):
	scores = getScoreArrayFromRow(row)
	ratings = getRatingArrayFromRow(row)
	return zip(scores, ratings)


def insertRound(roundDataMap):
	insertStatement = ("INSERT INTO rounds SET "
		"event_id=%(eventId)s, "
		"player_pdga_number=%(playerPdgaNumber)s, "
		"player_name=%(playerName)s, "
		"player_rating=%(playerRating)s, "
		"score=%(score)s, "
		"round_rating=%(roundRating)s, "
		"round_number=%(roundNumber)s, "
		"division=%(division)s "
		";")
	cursor.execute(insertStatement, roundDataMap)


def insertTournamentResult(tournamentResultDataMap):
	insertStatement = ("INSERT INTO tournament_results SET "
		"player_pdga_number=%(playerPdgaNumber)s, "
		"player_name=%(playerName)s, "
		"event_id=%(eventId)s, "
		"place=%(place)s, "
		"division=%(division)s, "
		"player_rating=%(playerRating)s, "
		"total_score=%(totalScore)s, "
		"points=%(points)s, "
		"prize=%(prize)s "
		";")
	cursor.execute(insertStatement, tournamentResultDataMap)


def scrapeFinalResultFromRow(row):
	place = scrapePlaceFromRow(row)
	totalScore = scrapeTotalScoreFromRow(row)
	points = scrapePointsFromRow(row)
	prize = scrapePrizeFromRow(row)

	return {
		'place': place,
		'totalScore': totalScore,
		'points': points,
		'prize': prize
	}

def parseIntOrNull(string):
	if string is None:
		return "null"
	try:
		x = int(string)
		return x
	except ValueError:
		return None


def scrapeRoundResultsFromRow(row, eventId, division):
	# Get player info
	playerPdgaNumber = scrapePlayerPdgaNumberFromRow(row)
	playerName = scrapePlayerNameFromRow(row)
	playerRating = scrapePlayerRatingFromRow(row)

	# Get scores and ratings
	scoresAndRatings = scrapeScoresAndRatingsFromRow(row)

	# Get final results
	finalResultsMap = scrapeFinalResultFromRow(row)

	# Create a `rounds` entry for every round appearing in this row
	roundNumber = 0
	for scoreRating in scoresAndRatings:
		roundNumber = roundNumber + 1
		# Make an object containing all data for the round
		roundData = {
			'eventId': eventId,
			'playerPdgaNumber': parseIntOrNull(playerPdgaNumber),
			'playerName': playerName,
			'playerRating': parseIntOrNull(playerRating),
			'score': parseIntOrNull(scoreRating[0]),
			'roundRating': parseIntOrNull(scoreRating[1]),
			'roundNumber': roundNumber,
			'division': division
		}
		insertRound(roundData)

	# Create a `tournament_results` entry for this row
	# Args to the dao method contain the results
	finalResultsData = finalResultsMap.copy()
	# Also need to add in event and player info
	finalResultsData.update({
			'eventId': eventId,
			'playerPdgaNumber': parseIntOrNull(playerPdgaNumber),
			'playerName': playerName,
			'playerRating': parseIntOrNull(playerRating),
			'division': division
		})
	insertTournamentResult(finalResultsData)


def rowContainsResults(row):
	# Note: simply checking there's a cell with this class is enought to
	# determine this is a real row (not a header row).
	return row.find("td", {"class": "pdga-number"}) is not None


def scrapeResultsFromTable(table, eventId, division):
	for row in table.find_all('tr'):
		if (rowContainsResults(row)):
			scrapeRoundResultsFromRow(row, eventId, division)

def getCachedPagePath(eventId):
	return CACHE_PATH + str(eventId) + ".html"

def cachePage(pageSource, eventId):
	path = getCachedPagePath(eventId)
	cacheFile = open(path, "w")
	cacheFile.write(pageSource)
	cacheFile.close()

def getSoupFromUrl(url, eventId):
	header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
	req = urllib2.Request(url, headers=header)
	page = urllib2.urlopen(req)
	soup = BeautifulSoup(page)
	cachePage(str(soup), eventId)
	return soup


def getIdFromHtmlElement(elt):
	return elt.get('id')


def scrapeResultsFromUrl(url, eventId):
	try:
		soup = getSoupFromUrl(url, eventId)
	except urllib2.HTTPError:
		print "Failed to fetch event " + str(eventId) + "."
		return

	tables = list(soup.findAll("table", { "class" : "results" }))
	tableDivisions = map(getIdFromHtmlElement, list(soup.findAll("h3", { "class": "division"})))

	# if tables and tableDivisions aren't the same length we are in big trouble
	if (len(tables) != len(tableDivisions)):
		raise Exception('Event with id ' + eventId + ' has different number of divisions than tables!')

	dividedTables = zip(tables, list(tableDivisions))

	TABLE = 0
	DIVISION = 1

	for divisionTable in dividedTables:
		scrapeResultsFromTable(divisionTable[TABLE], eventId, divisionTable[DIVISION])

	# Commit the entire tournament at once.  That we we never have partial tournaments
	database.commit()


def scrapeResultsFromEventNumber(eventId):
	scrapeResultsFromUrl("http://www.pdga.com/tour/event/" + str(eventId), eventId)

def getRandomSleepTime():
	return random.randrange(fetchDelayIntervalMinSeconds, fetchDelayIntervalMaxSeconds)

def scrapeResultsFromEventNumberSet(eventIds):
	for eventId in eventIds:
		print "Fetching results for event " + str(eventId) + "..."
		scrapeResultsFromEventNumber(eventId)
		sleepTime = getRandomSleepTime()
		print "Sleeping for " + str(sleepTime) + " seconds."
		time.sleep(sleepTime)


###############################################################################

startId = 8438
stopId = 8485

print "Expected duration: " + str((stopId - startId) * (fetchDelayIntervalMaxSeconds + fetchDelayIntervalMinSeconds)*0.5 / 60 / 60) + " hours."
scrapeResultsFromEventNumberSet(range(startId, stopId))

database.commit()
cursor.close()
database.close()

# 4-11-16  First scrape attempt
#	Using Toronto proxy.  5-15 sec delay.  Event range 1-391

# 4-11-16  Second scrape attempt (to run over night)
# 	Using Toronto proxy.  5-15 sec delay.  392-4158

# 4-11-17
# 	Using Toronto proxy.  5-15 sec delay.  4159-4232

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  4233-4539

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  4540-5091

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  5092, 5316

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  5317, 7333

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  7334, 7564

# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  7565, 8437

# 4-11-17
# 	At The Laughing Goat.  5-15 sec delay.  8438, 8749

############ MISTAKE #####################################################
# 4-11-17
# 	Using Texas proxy + prod VPN.  5-15 sec delay.  8438, 8485

# delete all entries from this time range using SQL directly
# DELETE FROM pdga.tournament_results
# 	WHERE
# 		event_id >= 8438
# 	AND
# 		event_id <= 8485
# ;
# DELETE FROM pdga.rounds
# 	WHERE
# 		event_id >= 8438
# 	AND
# 		event_id <= 8485
# ;

# Now fill back int he hole we left:
# 4-11-16  Second scrape attempt (to run over night)
# 	Using Toronto proxy.  5-15 sec delay.  8438, 8485

