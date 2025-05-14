import requests
from googleapiclient.discovery import build

def get_metadata_from_spotify(
        search_track_url: str, 
        keyword: str, 
        access_token: str, 
        limit_results: int = 5
) -> list[dict]:
    """
    Extract metadata from spotify api
    Params:
        - search_track_url: endpoint to search tracks
        - keyword: song title
        - access_token: credentials to hit endpoint
        - limit_results: count tracks will be showed
    Return:
        list of metadata - 
        [{"title": ..., "artist": ..., "album": ..., "release_date": ..., "isrc": ...}, ...]
    """
    # Search tracks based on keyword
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "q": f"track:{keyword}",
        "type": "track",
        "limit": limit_results
    }
    response = requests.get(search_track_url, headers=headers, params=params)
    tracks = response.json().get("tracks", {}).get("items", [])
    # Extract tracks metadata
    results = []
    for track in tracks:
        track_info = {
            "title": track.get("name", None),
            "artist": track.get("artists", [{}])[0].get("name", None),
            "album": track.get("album", {}).get("name", None),
            "release_date": track.get("album", {}).get("release_date", None),
            "isrc": track.get("external_ids", {}).get("isrc", None)
        }
        results.append(track_info)
    return results

def get_metadata_from_youtube(
        keyword: str, 
        auth: build, 
        limit_results: int = 5
) -> list[dict]:
    """
    Get videos metadata from youtube api
    Params:
        - keyword: song title
        - auth: authentication method
        - limit_results: count tracks will be showed
    Return:
        list of metadata - 
        [{"video_id": ..., "channel_id": ..., "song_title": ..., "artist": ..., "video_title": ...}, ...]
    """
    # Search videos based on keyword
    response = auth.search().list(
        q=keyword,
        type="video",
        part="id",
        maxResults=limit_results
    ).execute()
    # Extract videos metadata
    results = []
    videos = [item["id"]["videoId"] for item in response["items"]]
    for video_id in videos:
        video_response = auth.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        ).execute()
        snippet = video_response['items'][0].get('snippet', None)
        if snippet:
            results.append({
                "video_id": video_id,
                "channel_id": snippet.get('channelId', None),
                "song_title": snippet.get('title', None),
                "artist": snippet.get('channelTitle', None),
                "video_title": snippet.get('title', None)
            })
    return results

def get_data_from_gsheet(
        url: str,
        api_key: str,
        sheet_name: str,
        start_cell: str,
        end_cell: str
):
    """
    Get songs catalog from google sheet
    Params:
        - url: public gsheet url. example: https://sheets.googleapis.com/v4/spreadsheets/1OkDM1miCXh48M23n_C4AOGPl_DW1QtIXFSdHW6Grzxg
        - api_key: google api key
        - sheet_name: sheet name want to scrapping
        - start_cell: start cell want to scrap. example: A1
        - end_cell: end cell want to scrap. example: C10
    Return:
        list of metadata - 
        [{"header_1": ..., "header_2": ..., "header_n": ...}, ...]
    """
    gsheet_url = f"{url}/values/{sheet_name}!{start_cell}:{end_cell}?key={api_key}"
    response = requests.get(gsheet_url)
    if response.status_code == 200:
        results = []
        rows = response.json().get("values", [])
        columns = [x.replace(' ', '_').lower().strip() for x in rows[0]]
        for row in rows[1:]:
            results.append(dict(zip(columns, row)))
    return results
