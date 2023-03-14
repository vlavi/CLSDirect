"""
Class to access CLSdirect api to request metadata or data series.
To request any data via CLS direct a token must be generated which will be active
for an hour. After the token is generated you can then request information

If password is out of date, email ucpqa@cls-bank.com

Examples on how to use the class
#pd.set_option("max_columns", None)

# *** Find all meta dataset information ***
dataset = CLSDirect.all_dataset_metadata()

#Query for dataset code
dataset.loc[(dataset['Dataset'] == "FX Spot Flow"), ["Dataset_Code", "Dataset_Name", "Update_Frequency"]]

# *** Extract data series ***
CLSDirect.product_series("FXSPTOF01D:DAIL", "2021-01-01", "2021-12-31", "EURUSD")


@author: simonchan
"""

import requests
import json
import pandas as pd
from requests.utils import quote


class CLSDirect():
    """
    Using CLSDirect API to generate token and download metadata and time series
    """


    def _get_access_token(self):
        """
        Generate CLS direct token to access server.
        """
        response = requests.request("POST", self._token_url, data=self._payload, headers=self._headers, verify=False)
        token = None
    
        try:
            response.raise_for_status()
            response_json = json.loads(response.text)
            token = response_json["access_token"]
    
        except requests.exceptions.HTTPError as e:
            # it wasn't a 200; Check error code & retry if token expired
            print("Error: " + str(e))
            #print(response.json())
            print(json.dumps(e.response.json())) 
            
            if response.status_code == 404:
                #exit
                raise SystemExit('Not Found %s' % response.url)
    
            if response.status_code == 401:
                error_json = json.loads(response.text)
                error_code = error_json["code"]
    
                if error_code == '10016':
                   #token expired
                   print(error_code)
                   #request token
                elif error_code == '10012':
                   #token not valid
                   print(error_code)
                elif error_code == '10010':
                   #invalid credentials
                    print(error_code)
                    raise SystemExit('Credentials Not Valid')
                else:
                   #unexpected
                   print(error_code)

            #bad request
            if response.status_code == 400:
                error_json = json.loads(response.text)
                error_code = error_json["code"]

                if error_code == '10013':
                    #invalid request. exit
                    #print (error_json["message"])
                    print(error_code)
                    raise SystemExit('Invalid Request: %s' % error_json["message"])
            #server error
            if response.status_code == 500:
                #error_code == '10000' or error_code == '10001':
                print(response.text)

        return token


    def __init__(self):
        """Initialize CLS credentials and generate token"""
        self._clsdirect_dataset = "https://api.clsdirect.com/services/marketdata/v1/datasets"
        self._token_url = "https://api.clsdirect.com/services/general/getToken"
        self._username = "CLSDirectTestOrgUserMDAPI@cls-services.com"
        self._password = 'UcpR10123!'
        self._payload = str("username=") + quote(self._username) + str("&password=") + quote(self._password)
        self._headers = {'content-type': "application/x-www-form-urlencoded",
                         'cache-control': "no-cache"}
        self._bearer_token = self._get_access_token();
        self._auth_headers = {'authorization': "Bearer " + self._bearer_token,
                              'cache-control': "no-cache",}


    def _refresh_token(self):
        """Refresh token - Tokens are active for an hour"""
        self._bearer_token = self._get_access_token();
        self._auth_headers = {'authorization': "Bearer " + self._bearer_token,
                              'cache-control': "no-cache",}


    def extract(self, code, querystring):
        "Request information from CLSDirect via code and parameter string"

        url = self._clsdirect_dataset
        url += f"/{code}" if code else ""
        url += "/data" if not "instruments" in querystring.keys() and len(querystring) > 0 else ""
        print(url)
        print(querystring)

        try:
            response = requests.request("GET", url, headers=self._auth_headers, params=querystring)
            response.raise_for_status()
            data = json.loads(response.text)
            return data
        except requests.exceptions.HTTPError as err:
            if response.status_code == 401 and not response.ok:
                print("Token expired! Refresh token, please try again.")
                self._refresh_token()
            print(err)
            print(json.dumps(err.response.json()))


    def all_dataset_metadata(self):
        "Extract all metadata and combine into one dataframe"
    
        all_metadata = self.extract(code="", querystring={"instruments": False})
        meta = pd.DataFrame({})
    
        for i in all_metadata:
            for j in i["datasets"]:
                tmp = {
                    "Dataset": i["name"],
                    "Dataset_Name": j["name"],
                    "Dataset_Code": j["datasetCode"],
                    "Dataset_URL": j["url"],
                    "Dataset_StartDate": j["subscribedDataRangeStart"],
                    "Update_Frequency": j["frequency"],
                    "Last_Data_Update": j["lastUpdatedAt"],
                    "Status": j["status"]
                    }
                meta = meta.append(tmp, ignore_index=True)

        return meta


    def product_series(self, product, start_date, end_date, currency):
        "Extract data series and return a dataframe time series"

        param = {"format":"json", "startDate": start_date, "endDate": end_date, "currency": currency}
        return pd.DataFrame(self.extract(code=product, querystring=param))
