import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO  # This will help address the FutureWarning

# List of years to scrape data for, starting from 2024 to 2023
years = list(range(2024, 2022, -1))

# URL for Premier League Stats (starting point)
standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats"

# Empty list to store match data for all teams and seasons
all_matches = []

# Loop over the seasons (starting from 2024 and going backwards)
for year in years:
    # Request the page for the current season
    data = requests.get(standings_url)
    
    # Parse the page data using BeautifulSoup
    soup = BeautifulSoup(data.text, features="html.parser")
    
    # Select the table containing the team stats
    standings_table = soup.select('table.stats_table')[0]
    
    # Find all links for teams
    links = [l.get("href") for l in standings_table.find_all('a')]
    links = [l for l in links if '/squads/' in l]  # Only keep team URLs
    
    # Construct full URLs for each team
    team_urls = [f"https://fbref.com{l}" for l in links]
    
    # Find the link for the previous season and update the standings URL
    previous_season = soup.select("a.prev")[0].get("href")
    standings_url = f"https://fbref.com{previous_season}"
    
    # Loop over each team's URL for the current season
    for team_url in team_urls:
        # Extract the team name from the URL by removing extra parts
        team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        
        # Request data for the current team's page
        data = requests.get(team_url)
        
        # Wrap the HTML content in StringIO to avoid the FutureWarning
        matches_html = StringIO(data.text)
        
        # Use pandas to read the "Scores & Fixtures" table for the current team
        matches = pd.read_html(matches_html, match="Scores & Fixtures")[0]
        
        # Parse the team page for more detailed stats (like shooting data)
        soup = BeautifulSoup(data.text, features="html.parser")
        links = [l.get("href") for l in soup.find_all('a')]
        links = [l for l in links if l and 'all_comps/shooting/' in l]  # Shooting data links
        
        # Request the shooting data for the team
        data = requests.get(f"https://fbref.com{links[0]}")
        
        # Wrap the shooting data in StringIO to avoid FutureWarning
        shooting_html = StringIO(data.text)
        
        # Use pandas to read the "Shooting" table for the current team
        shooting = pd.read_html(shooting_html, match="Shooting")[0]
        
        # Clean the shooting data columns (remove unnecessary multi-level index)
        shooting.columns = shooting.columns.droplevel()
        
        # Merge the match data with the shooting data on the "Date" column
        try:
            team_data = matches.merge(shooting[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        except ValueError:
            # If there's an issue with merging (likely missing data), skip this team
            continue
        
        # Filter the matches to include only Premier League matches
        team_data = team_data[team_data["Comp"] == "Premier League"]
        
        # Add the season and team information to the dataset
        team_data["Season"] = year
        team_data["Team"] = team_name
        
        # Append the processed team data to the overall matches list
        all_matches.append(team_data)
        
        # Sleep for 1 second to avoid overwhelming the server with requests
        time.sleep(1)

# Check the total number of dataframes (matches for all teams and seasons)
print(f"Number of team-season dataframes: {len(all_matches)}")

# Concatenate all team-season dataframes into a single dataframe
match_df = pd.concat(all_matches, ignore_index=True)

# Convert all column names to lowercase for consistency
match_df.columns = [c.lower() for c in match_df.columns]

# Save the data frame as a csv
match_df.to_csv("matches.csv")