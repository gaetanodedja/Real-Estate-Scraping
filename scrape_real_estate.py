import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import hashlib

def generate_hash_key(row):
    row_string = f"{row['Location']}{row['Description']}{row['Price']}{row['Surface']}{row['RoomN']}{row['Listing']}{row['Company']}"
    return hashlib.md5(row_string.encode()).hexdigest()

def scrape_century21(pages):
    dataset = pd.DataFrame(columns=['Location', 'Description', 'Price', 'Surface', 'RoomN', 'Listing', 'Company']) 
    
    for i in range(1, pages + 1):
        url = f'https://www.century21albania.com/en/properties?display=grid&city=Tirana&page={i}'
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'} 
        c21Web = requests.get(url, headers=header)
        soup = BeautifulSoup(c21Web.content, "html.parser")
        results = soup.find_all('div', {'class': 'card card-list'})

        for result in results:
            location = result.find('h6', {'class': 'card-subtitle mt-1 mb-0 text-muted'})
            description = result.find('h5', {'class': 'card-title'})
            price = result.find('h2', {'class': 'text-primary mb-2'})     
            surface = result.find('div', {'class': 'col-xs-3 col-sm-3 col-md-3 col-lg-3 FutureInfo col-3'})
            iconroom = result.find('i', {'class': 'mdi mdi-hotel'})   
            if iconroom:        
                room_div = iconroom.find_parent('div')
                roomn = room_div.find('div', {'class': 'col-xs-3 col-sm-3 col-md-3 col-lg-3 FutureInfo col-3'})
            else:
                roomn = None                    
            saleORrent = result.find('div', {'class': 'card-img'})

            location_text = location.get_text() if location else None
            description_text = description.get_text() if description else None
            price_text = price.get_text() if price else None
            surface_text = surface.get_text() if surface else None
            roomn_text = roomn.get_text() if roomn else None
            saleORrent_text = saleORrent.get_text() if saleORrent else None

            dataset = dataset.append({
                'Location': location_text,
                'Description': description_text,
                'Price': price_text,
                'Surface': surface_text,
                'RoomN': roomn_text,
                'Listing': saleORrent_text,
                'Company': 'Century21'
            }, ignore_index=True)
    
    dataset['Price'] = dataset['Price'].apply(lambda line: line.replace('\n', '') if line else line)
    dataset['Surface'] = dataset['Surface'].apply(lambda line: line.replace('\n', '') if line else line)
    dataset['Location'] = dataset['Location'].apply(lambda line: line.replace('\n', '') if line else line)
    dataset['Listing'] = dataset['Listing'].apply(lambda line: line.replace('\n', '') if line else line)
    dataset['HashKey'] = dataset.apply(generate_hash_key, axis=1)
    
    return dataset

def scrape_futurehome(pages):
    dataset = pd.DataFrame(columns=['Location', 'Description', 'Price', 'Surface', 'RoomN', 'Listing', 'Company']) 
    
    for j in range(1, pages + 1):
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'}
        url = f'https://futurehome.al/en/properties?viewMode=list&city=Tirana&page={j}'
        fhWeb = requests.get(url, headers=header)
        soup = BeautifulSoup(fhWeb.content, "html.parser")
        results = soup.find_all('div', {'class': 'card property-card property-card-2 border-0'})

        for result in results:
            location = result.find('span', {'class': 'ps-3 fh-text-1 fh-font-2 fw-500'})
            description = result.find('p', {'class': 'card-title m-0 fh-text-2 fh-font-2 fw-bold truncate-property'})
            price = result.find('h5', {'class': 'price m-0 fh-font-1 fh-heading-1 fw-600 ls-none text-md-end dark-primary-orange'})
            iconsurf = result.find('i', {'class': 'icon-move rounded-circle'})
            if iconsurf:        
                surface_div = iconsurf.find_parent('div')
                surface = surface_div.find('span', {'class': 'ps-2 fh-text-1 fh-font-2 fw-bold ls-none'})
            else:
                surface = None
            iconroom = result.find('i', {'class': 'icon-hotel-bed rounded-circle'})   
            if iconroom:        
                room_div = iconroom.find_parent('div')
                roomn = room_div.find('span', {'class': 'ps-2 fh-text-1 fh-font-2 fw-bold ls-none'})
            else:
                roomn = None            
            saleORrent = result.find('span', {'class': 'type fh-font-2 fh-text-2 fw-bold'})
            
            location_text = location.get_text() if location else None
            description_text = description.get_text() if description else None
            price_text = price.get_text() if price else None
            surface_text = surface.get_text() if surface else None
            roomn_text = roomn.get_text() if roomn else None                            
            saleORrent_text = saleORrent.get_text() if saleORrent else None

            dataset = dataset.append({
                'Location': location_text,
                'Description': description_text,
                'Price': price_text,
                'Surface': surface_text,
                'RoomN': roomn_text,
                'Listing': saleORrent_text,
                'Company': 'Future Home'
            }, ignore_index=True)      
    
    dataset['Price'] = dataset['Price'].apply(lambda line: line.replace('/month', '') if line else line)
    dataset['HashKey'] = dataset.apply(generate_hash_key, axis=1)
    
    return dataset

def main(pages):
    century21_data = scrape_century21(pages)
    futurehome_data = scrape_futurehome(pages)
    
    RE_dataset = pd.concat([century21_data, futurehome_data], ignore_index=True)
    
    RE_dataset.to_csv('C:/Users/User/Desktop/Real Estate project/RE scraping/data/real_estate.csv', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape real estate data from Future Home and Cetury21')
    parser.add_argument('pages', type=int, help='Insert number of pages to scrape from webites')
    args = parser.parse_args()
    
    main(args.pages)
