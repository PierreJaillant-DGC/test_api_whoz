
# Whoz exploitation
## API Documentation
https://whoz.stoplight.io/docs/whoz-api/b2edb775739e6-whoz-api-documentation



## Prepare env (venv + pip)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


## Run

```bash
python main.py
```

## Modifying the schema
- Update the schema in the corresponding table in the dataset in BQ and also in the schemas/schemas.py file accordingly to your needs
- In the file app/data_processing/data_filtering.py update the ProcessForBigQuery class appropariate functions to add the new field(s) to the lines