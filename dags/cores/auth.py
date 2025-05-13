import requests
from googleapiclient.discovery import build

def spotify_auth(auth_url: str, spotify_client_id: str, spotify_client_secret: str) -> dict:
    """
    Get access token from spotify api
    Params:
        auth_url: spotify endpoint to get access token 
    Return:
        dictionary - {"access_token": "...", "token_type": "...", "expires_in": "..."} 
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "client_id": spotify_client_id,
        "client_secret": spotify_client_secret
    }
    response = requests.post(auth_url, data=payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    raise Exception(f"ERR authentication failed! \nDetails: {response.text}")

def youtube_auth(api_key: str) -> build:
    auth = build("youtube", "v3", developerKey=api_key)
    return auth