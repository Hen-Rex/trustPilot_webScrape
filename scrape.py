# Original skeleton code downloaded and improved from: https://www.reddit.com/r/Python/comments/jjm9tb/trustpilot_review_scraper (20. sep. 2021) and https://github.com/Verdifer-26/trustPilot_webScrape (15. oktober 2021).

# coding: utf-8

import pandas as pd
import numpy as np
import json
import time
import requests
from bs4 import BeautifulSoup

##### SET DATA HERE #####

#Set CSV filename
trustpilot_csv = 'TrustPilot_ultimate.csv'

#####

#start time
then = time.time()

#Define empty series for all data to be collected
reviewers = []
reviewstatuses = []
#thumbsups = [] # does not work, because Beautifulsoup does not load Javascript or CSS
#locations = [] # does not work, because Trustpilot hides location data if not signed in and/or due to URL parameters
headings = []
reviews = []
stars = []
dates = []

# TrustPilot rate limiting. 3.0 seconds timeout works best. Or a random step interval.
# Trustpilot only shows first 500 pages. Stop at 501 (not included).
# Please keep an open eye on th HTTP GET Status Code. It should return <200>, and if it does not, re-index those pages. I have not had time or need to make the code automatically wait and reindex if e.g. error <403> or !<200> was returned.
#Set number of pages to scrape (start, end-not-included, increment)

pages = np.arange(1, 501, 1)

#BEGIN: Prints message to user
print("\n This process could take 30 minutes. \n")

#Create a loop to go over the reviews
for page in pages:
    
    #HTTP rate limit timeout can be set here if needed. Seconds between each HTTP GET request. 3.0 seconds recommended with Trustpilot for >100 requests.
    timeout = time.sleep(3.0)
    
    url = "https://dk.trustpilot.com/review/stofa.dk" + "?languages=da" + "&page=" + str(page) + "&stars=1&stars=2&stars=3&stars=4&stars=5"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'} # some elements requires a user agent string to render/load correctly.
    page_http_get = requests.get(url, headers=headers)
    
    print("Page " + str(page) + " of 500 \t\t HTTP Response Code: " + str(page_http_get)) # prints a line for each page (20 reviews) scraped correctly. Remember to multiply number of pages by 20 to know how many lines your final CSV file should have. 500 pages of 20 reviews per page should return 10000 lines.
    
    soup = BeautifulSoup(page_http_get.text, "html.parser")
    #Set the tag we wish to start at
    html_div_consumerinformation = soup.find_all('div', class_="consumer-information__details") # it's important to define where find_all should start. Otherwise we will receive either too little or too many divs/data.
    html_div_reviewcontent = soup.find_all('div', class_="review-content")
    html_div_reviewlabels = soup.find_all('div', class_="review-content-header__review-labels")

    #loop to iterate through each review
    for container in html_div_consumerinformation:

        #Get reviewers' names
        reviewer_index = container.find_all('div', attrs={'class': "consumer-information__name"})
        reviewer = container.div.text if len(reviewer_index) == True else 'ERROR IN CODE!'
        reviewers.append(reviewer)
    
    for container in html_div_reviewcontent:
       
        #Get the text body of the review
        body_index = container.find_all('p', attrs={'class': "review-content__text"})
        review = container.p.text if len(body_index) == True else '-'
        reviews.append(review)

        #Get the heading/title of the review
        heading_index = container.find_all('h2', attrs={'class': "review-content__title"})
        heading = container.a.text if len(heading_index) == True else '-'
        headings.append(heading)

        #Get the star rating review given
        star = container.find("div", {'class': "star-rating star-rating--medium"}).find('img').get('alt')
        stars.append(star)

        #Get the date
        #NOTE: beautifulsoup does NOT support javascript loading, thus 'Date' and 'ThumbsUps', among others, CANNOT be scraped. It will return an empty series. Instead use the json table just above to scrape the DateTime data. However, json does not exist every time because sometimes object is empty and then the proces fails with 'keyerror'.
        date_json = json.loads(container.find('script').string)
        date = date_json['publishedDate']
        dates.append(date)
        
    for container in html_div_reviewlabels:
        
        #Get customer invitation & verification status
        reviewstatus_index = container.find_all('script', attrs={'data-initial-state': "verification-level-label"})
        reviewstatus = container.script.text if len(reviewstatus_index) == True else 'organic' #A review that's not labelled is an organic review. This indicates that a reviewer logged into Trustpilot on their own initiative to write a review about their experience with a business. The reviewer was not invited by the business. The review was not yet verified.
        reviewstatuses.append(reviewstatus)
        
#Create a DataFrame using the data
TrustPilot = pd.DataFrame({'Reviewer': reviewers, 'Review Status': reviewstatuses, 'Title': headings, 'Body': reviews, 'Rating': stars, 'Date': dates})

#Setting column data type to ensure robustness whenever TrustPilot decides to send "403" error messages instead of HTML content.
TrustPilot['Reviewer'] = TrustPilot['Reviewer'].astype(str)
TrustPilot["Review Status"] = TrustPilot["Review Status"].astype(str)
TrustPilot['Title'] = TrustPilot['Title'].astype(str)
TrustPilot['Body'] = TrustPilot['Body'].astype(str)
TrustPilot['Rating'] = TrustPilot['Rating'].astype(str)
TrustPilot['Date'] = TrustPilot['Date'].astype(str)

#Clean the white space from data
TrustPilot['Reviewer'] = TrustPilot['Reviewer'].str.strip()
TrustPilot['Title'] = TrustPilot['Title'].str.strip()
TrustPilot['Body'] = TrustPilot['Body'].str.strip()

#Saves to CSV
TrustPilot.to_csv(trustpilot_csv, index = False)

#Read the CSV file
data = pd.read_csv(trustpilot_csv)

#Split date and time into separate columns
new = data["Date"].str.split("T", n = 1, expand = True)
data["Date Posted"]= new[0] # NOTE: Prompts a 'KeyError' if no data at all received from TrustPilot. 
data["Time Posted"]= new[1]
data.drop(columns =["Date"], inplace = True)
new = data["Rating"].str.split(":", n = 1, expand = True)

#Do what we done with date to stars - part of the original code from Verdifer. It splits the alt text image data that is received as a single series of text into two separate columns.
data["Stars"]= new[0]
data["Rated"]= new[1]
data.drop(columns =["Rating"], inplace = True)
data['Time Posted'] = data['Time Posted'].map(lambda x: str(x)[:-4])
data['Stars'] = data['Stars'].map(lambda x: str(x)[0:1])

#Remove prepended text from 'Review Source'. Note that Trustpilot has changed its data structure regarding this data sometime in october 2020. That is why 4 and not 2 checks are required. From Oct 2020 onwards, the value is always 'true', which I presume is for compatibility reasons.
data["Review Status"] = data["Review Status"].apply(lambda x: 'invited' if ('true' and 'invited') in x else x)
data["Review Status"] = data["Review Status"].apply(lambda x: 'invited' if ('false' and 'null') in x else x)
data["Review Status"] = data["Review Status"].apply(lambda x: 'verified' if ('true' and 'verified') in x else x)
data["Review Status"] = data["Review Status"].apply(lambda x: 'verified' if ('true' and 'null') in x else x)

#Arrange the columns order and save it as a CSV
data = data[['Reviewer', 'Review Status', 'Title', 'Body', 'Stars', 'Rated', 'Date Posted', 'Time Posted']]
data.to_csv(trustpilot_csv, index = False)

#Show how long the code took to complete
now = time.time()
totalSeconds = now-then
totalMins = totalSeconds / 60
print("It took: ", round(totalSeconds,2), "seconds", "|", round(totalMins,2), "minutes")
