import os
import datetime
import json, os, logging
import pandas as pd

from google.cloud import bigquery

class BigQuerryManager:
    def __init__(self):
        LOGGER = logging.getLogger(__name__)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/gcloud.json"
        print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

        LOGGER.info("google sdk is being authenticated...")
        os.system("gcloud auth activate-service-account {} --key-file={}".format(os.environ["google_service_account"], os.environ["GOOGLE_APPLICATION_CREDENTIALS"]))

        LOGGER.info("google sdk is being configured...")
        os.system("gcloud config set account {}".format(os.environ["google_service_account"]))

        LOGGER.info("setting default project...")
        os.system("gcloud config set project {}".format(os.environ["project_id"]))
        # initialize bigquery client
        self.client = bigquery.Client()
        # self.client = bigquery.Client.from_service_account_json("../config/gcloud.json")
        self.PROJECT_ID  = os.environ["project_id"]
        self.DB_NAME     = os.environ['db_name']



    def push_to_bq(self, df, schema, table_name):
        dataset_and_table = f"{self.DB_NAME}.{table_name}"
        df.to_gbq( dataset_and_table, 
                   self.PROJECT_ID,
                   chunksize=None, 
                   if_exists='append',
                   table_schema=schema
                 )

        print("succesfully pushed !!!")