import json
import datetime
import hashlib
import hmac
import base64
from bson.decimal128 import Decimal128
from decimal import Decimal
import time


def safe_decimal128(x):
    if isinstance(x, str):
        return Decimal128(x)
    if isinstance(x, float):
        return Decimal128(str(x))


def generate_header(info: dict, requestPath: str, body: dict=None, method: str="GET", is_simulated:bool=True) -> dict:
    utc_now = datetime.datetime.utcnow()
    timestamp = str(utc_now.isoformat("T", "milliseconds") + "Z")

    signature = sign(timestamp, method, requestPath, info['secretkey'], body)
    header = {
        'Content-Type': 'application/json',
        'OK-ACCESS-KEY': info['apikey'],
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-PASSPHRASE': info['passwd'],
        "OK-ACCESS-TIMESTAMP": timestamp
    }
    if is_simulated:
        header['x-simulated-trading'] = '1'
    return header

def decode(string):
    return string.decode('latin-1')

def encode(string):
    return string.encode('latin-1')

def binary_to_base64(s):
    return decode(base64.standard_b64encode(s))

def binary_to_base16(s):
    return decode(base64.b16encode(s)).lower()

def iso8601(timestamp=None):
    if timestamp is None:
        return timestamp
    if not isinstance(timestamp, (int, float)):
        return None
    if int(timestamp) < 0:
        return None
    try:
        utc = datetime.datetime.utcfromtimestamp(timestamp // 1000)
        return utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-6] + "{:03d}".format(int(timestamp) % 1000) + 'Z'
    except (TypeError, OverflowError, OSError):
        return None

def ok_hmac(request, secret, algorithm=hashlib.sha256, digest='hex'):
    h = hmac.new(secret.encode("utf-8"), request.encode("utf-8"), digestmod=algorithm)
    binary = h.digest()
    if digest == 'hex':
        return base64.b16encode(binary)
    elif digest == 'base64':
        return base64.standard_b64encode(binary)
    return binary

def sign(timestamp:str, method:str, endpoint:str, secret:str, params:dict=None):
    method = str.upper(method)
    if method != 'POST' or not params:
        str_params = ''
    else:
        str_params = json.dumps(params, ensure_ascii=False)
    auth = timestamp + method + endpoint + str_params
    print(auth)
    mac = hmac.new(
        secret.encode("utf8"), auth.encode("utf8"), digestmod="sha256"
    )
    binary = mac.digest()
    sign = base64.b64encode(binary)
    return sign
