from random import randint
from time import sleep
from os import system

prefix="https://www.senate.gov/legislative/LIS/roll_call_votes/vote1171/vote_117_1_"

for i in range(1, 358):
	url = prefix + "{:05d}".format(i) + ".xml"
	print(url)
	
	system('wget ' + url)
	sleep(randint(10,30))
