import datetime
import logging
import azure.functions as func
from function_app2 import update_all

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f'Python timer trigger function started at {utc_timestamp}')
    
    try:
        # Update all markets by default
        logging.info("Starting scheduled stock update...")
        update_all()
        logging.info("Scheduled stock update completed successfully")
        
    except Exception as e:
        logging.error(f"Error in scheduled update: {str(e)}")

    logging.info(f'Python timer trigger function completed at {utc_timestamp}')