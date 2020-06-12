import json
import datetime
import hashlib
import hmac
import base64


def generate_header(is_simulated:bool=True) -> dict:
    utc_now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    with open('confidential.json', 'r') as f:
        info = json.load(f)
    signature = sign(utc_now, 'GET', '/api/margin/v3/accounts', '', info['secretkey'])
    header = {
        'Content-Type': 'application/json',
        'OK-ACCESS-KEY': info['apikey'],
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-PASSPHRASE': info['passwd'],
        "OK-ACCESS-TIMESTAMP": utc_now
    }
    if is_simulated:
        header['x-simulated-trading'] = '1'
    return header


def sign(timestamp:str, method:str, endpoint:str, params:str, secret:str) -> str:
    if method != 'POST' or method != 'post':
        params = ''
    message = timestamp + str.upper(method) + endpoint + params
    h = hmac.new(bytes(secret, 'utf-8'), bytes(message, 'utf-8'), digestmod='sha256')
    d = h.digest()
    return base64.b64encode(d)


