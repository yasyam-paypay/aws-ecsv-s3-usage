# aws-ecsv-s3-usage

Extract S3 usage data from billing csv file.


Download the billing csv file from the AWS billing console and save it as `ecsv_{month}_{year}.csv` in the `files` directory.

After that, run the following commands:

```
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ python ./main.py
```

You will get the result.csv file with the extracted data.
