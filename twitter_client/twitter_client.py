import requests

def get_auth(token):
        headers = {'Authorization':f'Bearer {token}'}
        return headers

class TwitterClient:
    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.base_url = 'https://api.twitter.com/2'

    def get_user_id(self, username, token):
        url = f'{self.base_url}/users/by/username/{username}'
        req = requests.get(url, headers=get_auth(token))
        return req

    def get_user_timeline(self, user_id, token):
        url = f'{self.base_url}/users/{user_id}/tweets'
        return requests.get(url, headers=get_auth(token))
