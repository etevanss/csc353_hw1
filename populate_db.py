'''
CSC 353: Homework 1
Ethan Evans and Calvin Spencer
'''

import mysql.connector
import os
import glob
import lxml
import lxml.etree
import datetime

# I want the votes stored as 'Y', 'N', or 'A'. This might be useful.
def convertVoteCast(vote_cast):
	if vote_cast == 'Yea':
		return 'Y'

	if vote_cast == 'Nay':
		return 'N'
	
	return 'A'

# This converts the date obtained from the XML files into a SQL date string. This might be useful.
def convertDate(date):
	date_split = date.split(',')
	day = date_split[0] + date_split[1]

	return datetime.datetime.strptime(day, '%B %d %Y').strftime('%Y-%m-%d')

# Use helper functions. Add other parameters as appropriate.

def insertVoteCast(cursor, vote_cast_tup):
	data_string = "INSERT INTO VoteCast VALUES" + vote_cast_tup
	try:
		cursor.execute(data_string)
	except mysql.connector.Error as error_descriptor:
		print("Failed inserting tuple: {}".format(error_descriptor))

def insertVote(cursor, vote_tup):
	data_string = "INSERT INTO Votes VALUES" + vote_tup
	try:
		cursor.execute(data_string)
	except mysql.connector.Error as error_descriptor:
		print("Failed inserting tuple: {}".format(error_descriptor))

def insertSenator(cursor, senator_tup):
	data_string = "INSERT INTO Senators VALUES" + senator_tup
	try:
		cursor.execute(data_string)
	except mysql.connector.Error as error_descriptor:
		print("Failed inserting tuple: {}".format(error_descriptor))






# Read the 'Schema.sql' file into the 'schema_string' variable
# See 'Schema.sql' for comments on what that file should contain
schema_string_senators = (
"CREATE TABLE Senators"
	"(lis_member_id  VARCHAR(4),"
	 "first_name		VARCHAR(50), "
	 "last_name		VARCHAR(50), "
	 "party	        VARCHAR(1),"
	 "state          VARCHAR(2),"
	 "PRIMARY KEY (lis_member_id)"
	");" )

schema_string_votes = (
"CREATE TABLE Votes"
	"(cong_num		INT,"
	 "cong_session	INT,"
	 "vote_num		INT,"
	 "year			INT,"
	 "date			DATE,"
	 "PRIMARY KEY (cong_num, cong_session, vote_num)"
	");")

schema_string_votecast = (
"CREATE TABLE VoteCast"
	"(id			VARCHAR(4), "
	 "cong_num		INT,"
	 "cong_session	INT,"
	 "vote_num 		INT,"
	 "vote_cast	VARCHAR(1),"
	 "FOREIGN KEY (id) REFERENCES Senators(lis_member_id),"
	 "FOREIGN KEY (cong_num, cong_session, vote_num) REFERENCES Votes(cong_num, cong_session, vote_num)"
	");"
)

# Data definition

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='r0ckies2', host='localhost')

cursor = connection.cursor()

databaseName = "SenatorVotes"

# Remove previously existing DB
try:
	cursor.execute("DROP DATABASE IF EXISTS {}".format(databaseName))
except mysql.connector.Error as error_descriptor:
	print("Failed dropping database: {}".format(error_descriptor))
	exit(1)

# Create DB
try:
	cursor.execute("CREATE DATABASE {}".format(databaseName))
except mysql.connector.Error as error_descriptor:
	print("Failed creating database: {}".format(error_descriptor))
	exit(1)

# Use DB
try:
	cursor.execute("USE {}".format(databaseName))
except mysql.connector.Error as error_descriptor:
	print("Failed using database: {}".format(error_descriptor))
	exit(1)


# Execute schema strings one at a time
try:
	cursor.execute(schema_string_senators, multi=False)
except mysql.connector.Error as error_descriptor:
	if error_descriptor.errno == mysql.connector.errorcode.ER_TABLE_EXISTS_ERROR:
		print("Table already exists: {}".format(error_descriptor))
	else:
		print("Failed creating schema: {}".format(error_descriptor))
	exit(1)

try:
	cursor.execute(schema_string_votes, multi=False)
except mysql.connector.Error as error_descriptor:
	if error_descriptor.errno == mysql.connector.errorcode.ER_TABLE_EXISTS_ERROR:
		print("Table already exists: {}".format(error_descriptor))
	else:
		print("Failed creating schema: {}".format(error_descriptor))
	exit(1)

try:
	cursor.execute(schema_string_votecast, multi=False)
except mysql.connector.Error as error_descriptor:
	if error_descriptor.errno == mysql.connector.errorcode.ER_TABLE_EXISTS_ERROR:
		print("Table already exists: {}".format(error_descriptor))
	else:
		print("Failed creating schema: {}".format(error_descriptor))
	exit(1)

# Run the contents of 'Schema.sql', creating a schema (deleting previous incarnations),
# and creating the three relations mentioned in the handout.

connection.commit()
cursor.close()

# Data manipulation
cursor = connection.cursor()

# After running the contents of 'Schema.sql', you have to do again
# a USE SenatorVotes in your connection before adding the tuples.

try:
	cursor.execute("USE {}".format(databaseName))
except mysql.connector.Error as error_descriptor:
	print("Failed using database: {}".format(error_descriptor))
	exit(1)

for filename in glob.glob("XML/*.xml"):
	# Create the parsing tree using lxml (we did it in class and you have example files)
	tree = lxml.etree.parse(filename)
	# Extract the attributes of Vote 

	cong_num = tree.xpath("congress")[0].text
	cong_session = tree.xpath("session")[0].text
	vote_num = tree.xpath("vote_number")[0].text
	year = tree.xpath("congress_year")[0].text
	date = tree.xpath("vote_date")[0].text


	vote_tup = "(" + cong_num + "," + cong_session + "," + vote_num + "," + year + ",'" + convertDate(date) + "')"
	insertVote(cursor, vote_tup)

	# Find all members
	members = tree.xpath("//member")
	senators = []

	for member in members:

		lis_member_id = member.xpath("lis_member_id")[0].text

		# Identify a senator
		# senators = getCol(cursor, "Senators", "lis_member_id")
		if lis_member_id not in senators:
			senators.append(lis_member_id)

			# Parse other attributes of senator
			last_name = member.xpath("last_name")[0].text
			first_name = member.xpath("first_name")[0].text
			party = member.xpath("party")[0].text
			state = member.xpath("state")[0].text
			vote_cast = member.xpath("vote_cast")[0].text

			senator_tup = "('" + lis_member_id + "','" + first_name + "','" + last_name + "','" + party + "','" + state + "')"
			# print(senator_tup)
			insertSenator(cursor, senator_tup)



		vote_cast_tup = "('" + lis_member_id + "'," + cong_num + "," + cong_session + "," + vote_num + ",'" + convertVoteCast(vote_cast) + "')"
		insertVoteCast(cursor, vote_cast_tup)


# Don't forget to commit, close connections, etc
connection.commit()
cursor.close()
connection.close()