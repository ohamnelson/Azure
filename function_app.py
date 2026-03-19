import azure.functions as func
import logging
import requests

from etl import extract_data, transform_data, load_data

app = func.FunctionApp()


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")


@app.timer_trigger(schedule="0 0 */1 * * *", arg_name="etlTimer", run_on_startup=True)
def etl_function(etlTimer: func.TimerRequest) -> None:
    """ETL: Extract from API, Transform (aggregate), Load to Blob Storage."""
    logging.info("ETL function started.")

    try:
        # --- EXTRACT ---
        raw_data = extract_data()
        logging.info(f"Extracted {len(raw_data)} records.")

        # --- TRANSFORM ---
        transformed_data = transform_data(raw_data)
        logging.info(f"Transformation complete. {len(transformed_data)} aggregated records.")

        # --- LOAD ---
        blob_name = load_data(transformed_data)
        logging.info(f"Data loaded to blob: {blob_name}")

    except Exception as e:
        logging.error(f"ETL failed: {e}")
        raise

    return blob_name


@app.route(route="check-ip")
def check_ip(req: func.HttpRequest) -> func.HttpResponse:
    #This service returns the IP address it sees your request coming from.
    response = requests.get("https://ifconfig.me/ip")
    return func.HttpResponse(f"Outbound IP: {response.text.strip}")