# song-catalog-data

## Description
Scrapping data from youtube and spotify based on your songs catalog!

## How to Use
1. Makesure you have Airflow and Clickhouse running
2. In airflow set environment variables:
    ```
    api-key-secret: {
        "spotify_client_id": "...",
        "spotify_client_secret": "...",
        "google_api_key": "..."
    }

    bronze-config: {
        "gsheet_songs_catalog": "https://sheets.googleapis.com/v4/spreadsheets/...",
        "gsheet_sheet_name": "Data",
        "gsheet_start_cell": "A1",
        "gsheet_end_cell": "...",
        "limit_scrapping_songs": ...
    }

    dwh-creds-secret: {
        "dwh_host": "...",
        "dwh_port": ...,
        "dwh_username": "...",
        "dwh_password": "..."
    }
    ```

## Presentation slides
Technical Test:
https://docs.google.com/presentation/d/19f1gQg41YWHCwRpq6pO_S75j0CFXUYAKQFn68BqP3UU/edit?usp=sharing

C Level Test:
https://docs.google.com/presentation/d/19rSkFh_H4lcJokp-seEe42r_hp__RwLNWHrGWNqLS5s/edit?usp=sharing
