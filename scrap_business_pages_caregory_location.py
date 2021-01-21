# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19

Scrap Places data by using Facebook's nearby places API.

@author: @eyadkht
"""

import urllib
from urllib.request import urlopen
import json
import datetime
import csv
import time

  
"""
INPUTS:
    url: a request url
OUTPUTS: 
    the data returned by calling that url
"""
def request_data_from_url(url):
    req = urllib.request.Request(url)
    success = False
    while success is False:
        try: 
            #open the url
            response = urlopen(req)
            #200 is the success code for http
            if response.getcode() == 200:
                success = True
        except ValueError:
            #if we didn't get a success, then print the error and wait 5 seconds before trying again
            print ( response.getcode())
            time.sleep(5)
            print ("Error for URL",url,": ",datetime.datetime.now())
            print ("Retrying...")

    #return the contents of the response
    return response.read()

"""
INPUTS: 
    access_token: authentication proving that you have a valid facebook account
OUTPUTS:
    a python dictionary of the data on the requested nearby places
"""
def get_nearby_places_data(access_token,location_lat,location_long,radius_of_search,category):
    print("Facebook Place data")

    website = "https://graph.facebook.com/v3.1/"
    location = "search"
    options="?center="+location_lat+","+location_long+"&distance="+radius_of_search+"&"
    fields = "fields=name,checkins,about,link,overall_star_rating,rating_count," + \
            "phone,location,category_list,engagement," + \
            "price_range,restaurant_services,restaurant_specialties,single_line_address,website," + \
            "hours,is_always_open,is_permanently_closed,is_verified"
            
    authentication = "&limit=100&categories=[\""+category+"\"]&type=place&access_token=%s" % (access_token)
    
    request_url = website + location + options + fields + authentication

    #converts facebook's response to a python dictionary to easier manipulate later
    data = json.loads(request_data_from_url(request_url))
    return data
"""
INPUTS: 
    post: information about a single post on the facebook page
    access_token: authentication proving that you have a valid facebook account
OUTPUTS: 
    a list with the requested fields for this post
"""
def process_post(place):
    
    place_name = place['name']
    
    place_phone = '' if 'phone' not in place.keys() else \
          place['phone']
            
    place_checkins = '' if 'checkins' not in place.keys() else \
            place['checkins']
    
    place_id= place['id']
    
    place_link = '' if 'link' not in place.keys() else \
            place['link']
    
    place_about = '' if 'about' not in place.keys() else \
            place['about']
    
    place_rates_count = '' if 'rating_count' not in place.keys() else \
           place['rating_count']
    
    place_overall_rating = '' if 'overall_star_rating' not in place.keys() else \
          place['overall_star_rating']
    
    place_city = '' if 'city' not in place['location'] else \
           place['location']['city']
    
    place_country = '' if 'country' not in place['location'] else \
            place['location']['country']
            
    place_lat = '' if 'location' not in place.keys() else \
         place['location']['latitude']
            
    place_long = '' if 'location' not in place.keys() else \
           place['location']['longitude']
    
    place_street = '' if 'street' not in place['location'] else \
            place['location']['street']
            
    place_likes = '' if 'engagement' not in place.keys() else \
          place['engagement']['count']
    
    price_range = '' if 'price_range' not in place.keys() else \
            place['price_range']
    
    restaurant_services = '' if 'restaurant_services' not in place.keys() else \
            place['restaurant_services']

    restaurant_specialties = '' if 'restaurant_specialties' not in place.keys() else \
            place['restaurant_specialties']
    
    single_line_address = '' if 'single_line_address' not in place.keys() else \
            place['single_line_address']
    
    website = '' if 'website' not in place.keys() else \
            place['website']
    
    hours = '' if 'hours' not in place.keys() else \
            place['hours']

    is_always_open = '' if 'is_always_open' not in place.keys() else \
            place['is_always_open']

    is_permanently_closed = '' if 'is_permanently_closed' not in place.keys() else \
            place['is_permanently_closed']
    
    is_verified = '' if 'is_verified' not in place.keys() else \
            place['is_verified']
              
    #return a list of all the fields we asked for
    return (place_id,place_name,place_link,place_phone,place_country,place_city,place_street
            ,place_rates_count, place_overall_rating,place_checkins,place_likes,place_about,
            place_lat, place_long,price_range,restaurant_services,restaurant_specialties,
            single_line_address,website,hours,is_always_open,is_permanently_closed,is_permanently_closed,is_verified)

"""
INPUTS:
    access_token: authentication proving that you have a valid facebook account
OUTPUTS:
    nothing, simply prints how many places were processed and how long it took
"""
def scrape_nearby_places(access_token,location_name,location_lat,location_long,radius_of_search,category):
    #open up a csv (comma separated values) file to write data to

    with open(location_name+"_"+category+".csv",'w',newline='', encoding='utf-8-sig') as file:
        #let w represent our files
        
        w = csv.writer(file)
        
        #write the header row
        w.writerow(["place_id","place_name","place_link","place_phone","place_country", "place_city","place_street",
                    "place_rates_count","place_overall_rating","place_checkins","place_likes","place_about",
                    "place_lat","place_long","price_range","services","specialties","single_line_address","website","hours",
                    "is_always_open","is_permanently_closed","is_verified"]
                    )

        has_next_page = True
        num_processed = 0  
        scrape_starttime = datetime.datetime.now()

        print ("Scraping Facebook Nearby Places start time: ",scrape_starttime)

        #get first batch of places
        places = get_nearby_places_data(access_token,location_lat,location_long,radius_of_search,category)

        #while there is another page of posts to process
        while has_next_page:
            #for each individual post in our retrieved posts ...
            for place in places['data']:
                
                """if num_processed == 1000:
                    break"""
                #...get post info and write to our spreadsheet
                w.writerow(process_post(place))
                print(num_processed)
                num_processed += 1

            #if there is a next page of places to get, then get next page to process
            if 'paging' in places.keys():
                places = json.loads(request_data_from_url(
                                        places['paging']['next']))
            #otherwise, we are done!
            else:
                has_next_page = False

        print ("Completed!\n%s Places Processed in %s" % \
                (num_processed, datetime.datetime.now() - scrape_starttime))
        
access_token = "1696785713876555|tGsDow1IpAkzEgVpY9UwQOkjdz4"

if __name__ == '__main__':
  radius_of_search="40000"
#  Syria_locations={"Eindhoven":["51.4485569","5.4501225"],
#                   "Homs":["34.7324273","36.713696"],
#                   "Aleppo":["36.2021047","37.13426"],
#                   "Lattakia":["35.5407103","35.795267"],
#                   "Tartous":["34.8959276","35.886652"],
#                   "Hama":["35.1408881","36.755199"],
#                   "Sweida":["32.712897","36.566286"],
#                   "Daraa":["32.626434","36.1033061"],
#                   "Hasakeh":["36.507867","40.746267"],
#                   "Deir-ez-zor":["35.3296518","40.1350340"],
#                   "Raqqa":["35.9594106","38.998105"]
#          }
  
  Syria_locations={"Oslo":["59.9133301","10.7389701"]
          }
#  
#  Syria_locations={"Seattle":["47.6038321","-122.3300624"],
#                   "Philadelphia":["39.9527237","-75.1635262"],
#                   "Oslo":["59.9133301","10.7389701"],
#                   "Cleavland":["41.5052","-81.6934"]
#          }
  categories={"FOOD_BEVERAGE"}
 # categories={"FOOD_BEVERAGE","ARTS_ENTERTAINMENT",
             # "EDUCATION","FITNESS_RECREATION","HOTEL_LODGING",
              #"SHOPPING_RETAIL","TRAVEL_TRANSPORTATION"}#
  
  #"MEDICAL_HEALTH",
for category in categories:
    for key, value in Syria_locations.items():
       scrape_nearby_places(access_token,key,value[0],value[1],radius_of_search,category) 

 