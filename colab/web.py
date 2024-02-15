import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import os
import re

async def fetch_website_data(session, url, all_website_data):
    website_data = {}
    try:
        async with session.get(url, timeout=30) as response:
            if response.status == 200:
                content = await response.text()
                # Parse the HTML content of the page
                soup = BeautifulSoup(content, 'html.parser')
                # Extract the data you need
                seo_description_element = soup.find('meta', attrs={'name': 'description'})
                seo_description = seo_description_element['content'] if seo_description_element else "Not found"

                # Check if the 'about-section' element exists
                about_element = soup.find('div', class_='about-section')
                about = about_element.text.strip() if about_element else "Not found"

                # Check if the 'news-section' element exists
                news_element = soup.find('div', class_='news-section')
                news = news_element.text.strip() if news_element else "Not found"

                # Capture the full content of the website and remove leading/trailing white spaces and tabs
                content = soup.get_text()
                content = re.sub(r'\s+', ' ', content)  # Replace multiple spaces with a single space
                content = content.strip()

                # Create a dictionary with the extracted data
                website_data = {
                    'url': url,
                    'seo_description': seo_description,
                    'about': about,
                    'news': news,
                    'content': content,
                    'status': response.status
                }
                # print(f'Successfully retrieve data from {url}. Status code: {response.status}')
                # Append the website data to the list
                all_website_data.append(website_data)
            else:
                # print(f'Failed to retrieve data from {url}. Status code: {response.status}')
                website_data = {
                    'url': url,
                    'seo_description': "Not found",
                    'about': "Not found",
                    'news': "Not found",
                    'content': "Not found",
                    'status': response.status
                }
                all_website_data.append(website_data)
    except Exception as e:
        # print(f'Error occurred while requesting {url}: {e}')
        website_data = {
                    'url': url,
                    'seo_description': "Not found",
                    'about': "Not found",
                    'news': "Not found",
                    'content': "Not found",
                    'status': '500'
                }
        all_website_data.append(website_data)

    return all_website_data

async def scrape_websites(all_urls):
    all_website_data = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in all_urls:
            tasks.append(fetch_website_data(session, url, all_website_data))

        website_data = await asyncio.gather(*tasks)
        return website_data

def url_match(url1, url2):
  # Convert input arguments to strings
  url1 = str(url1)
  url2 = str(url2)
  if url1.startswith('http://'):
      url1_cleaned = url1[len('http://'):]
  elif url1.startswith('https://'):
      url1_cleaned = url1[len('https://'):]
  else:
      url1_cleaned = url1

  if url2.startswith('http://'):
      url2_cleaned = url2[len('http://'):]
  elif url2.startswith('https://'):
      url2_cleaned = url2[len('https://'):]
  else:
      url2_cleaned = url2

  return url1_cleaned == url2_cleaned

def csv_input_part ():
    print('*****************************   Instruction   *******************************************')
    print('Please input the exact csv name only. you dont need to provide .csv extension')
    print('Then please input the exact column name where the website url available in your csv')
    print('Then please input the exact column name where you want to put the scrapped data')
    print('At last please input the number of website that you want to scrapp\n')
    print('Please careful about uppercase and lowercase while you are giving input. It is case sensative.')
    print('You can get all web scrapping from the csv at a same time but error rate will be increase')
    print('My suggestion for number of web scrap at a single exection should be Maximum 100')
    print('*****************************   Instruction   *******************************************\n\n')
    # Taking a string input
    csv_name = input("Now Enter The csv name: ")
    # Check if the input is empty or null
    while len(csv_name) == 0:
        print("CSV name cannot be empty.")
        csv_name = input("Please enter the CSV name: ")

    csv_name = csv_name+'.csv'
    print("The CSV is: ", csv_name)

    path = csv_name
    if not os.path.isfile(path):
        print("No CSV file found with the provided name.")
        exit()

    input_col = input("Now enter the exact column name of the website's URL in your CSV: ")

    # Check if the input is empty or null
    while len(input_col) == 0:
        print("Column name cannot be empty.")
        input_col = input("Please enter the column name of the website's URL: ")

    output_col = input("Now enter the exact column name where you want to put the scraped data in your CSV: ")

    # Check if the input is empty or null
    while len(output_col) == 0:
        print("Column name cannot be empty.")
        output_col = input("Please enter the column name where you want to put the scraped data: ")

    # Taking an integer input
    number_str = input("How many websites do you want to scrape in one execution: ")

    try:
        number = int(number_str)
        print(number)
    except ValueError:
        print("Invalid input for execution number. Setting the number to default: 100")
        number = 100

    df = pd.read_csv(path, dtype={output_col: str})

    # Check if the input column names are available
    if input_col not in df.columns:
        print(f"Input column '{input_col}' is not available in the CSV.")
        exit()

    if output_col not in df.columns:
        print(f"Output column '{output_col}' is not available in the CSV.")
        exit()

    total_len = len(df)
    total_len = total_len - 1

    if number > total_len:
        print('Your provided number of execution is higher than the csv lenth. \nSo we are setting the total execution number 100')
        number = 100
    else:
        number = number
    print(f"Execution started for '{number}' Website")
    return df, path, input_col, output_col, number, total_len

if __name__ == '__main__':
    df, path, input_col, output_col, number, total_len = csv_input_part()

    filtered_df = df[df[output_col].isnull()]
    selected_rows = filtered_df.head(number)
    null_count = len(selected_rows)

    if null_count < number:
        # Calculate the difference between 'number' and null_count
        remaining_count = number - null_count
        specific_string_rows = df[df[output_col] == '{"seo_description": "Not found", "about": "Not found", "news": "Not found", "content": "Not found"}']
        # Limit the number of specific string rows to append by the remaining_count
        if (remaining_count == number):
            print('There is no url left to check')
            print('So we are trying to scrap the urls for which we got error before')
            specific_string_rows = specific_string_rows.head(total_len)
        else:
            print('There are few amount of url left to scrap')
            specific_string_rows = specific_string_rows.head(remaining_count)
        # Concatenate the selected null rows and specific string rows
        all_selected_rows = pd.concat([selected_rows, specific_string_rows])
    else:
        all_selected_rows = selected_rows
    all_urls = all_selected_rows[input_col]

    # Concatenate "https://" or "http://" to URLs if missing
    all_urls = [url if url.startswith('http') else 'https://' + url for url in all_urls]

    print('Execution Starts Now')

    all_website_data = asyncio.run(scrape_websites(all_urls))
    print('Execution Ends Now')
    print('Started Putting Data to CSV ')
    # Update the DataFrame based on URL matches
    for website_data in all_website_data[0]:
        for index, row in df.iterrows():
            website_url = website_data['url'].replace('http://', '').replace('https://', '')
            if url_match(website_data['url'], row[input_col]):
                df.at[index, output_col] = json.dumps({
                    'seo_description': website_data['seo_description'],
                    'about': website_data['about'],
                    'news': website_data['news'],
                    'content': website_data['content'],
                })
    df.drop(df.columns[df.columns.str.contains('Unnamed', na=False)], axis=1, inplace=True)
    df.to_csv(path, index=False)
    print('CSV updated successfully with scrapped Data')


